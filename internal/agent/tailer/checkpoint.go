package tailer

// Offset persistence through the shared positions store.

import (
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/positions"
)

// checkpoint is one file's persisted position (shared shape with the
// unified positions store).
type checkpoint = positions.LogPos

func (t *Tailer) loadCheckpoints() map[string]checkpoint {
	if t.cfg.Positions == nil {
		return nil
	}
	return t.cfg.Positions.Logs()
}

// checkpointing reports whether any checkpoint store is configured.
func (t *Tailer) checkpointing() bool {
	return t.cfg.Positions != nil
}

func (t *Tailer) saveCheckpoints() {
	t.lastCheckpoint = time.Now()
	if !t.checkpointing() {
		return
	}
	// Entries for files we no longer track are dropped by rebuilding the map
	// from t.files. That is only safe when the last listing SUCCEEDED: a failed
	// glob (a log dir not yet mounted, a transient EIO, a mistyped include)
	// leaves t.files empty or short, and this save would then destroy every
	// persisted offset — after which the next start treats those files as
	// history and skips them to the end. Keep the stored entries we cannot
	// currently see, and let a successful listing prune them.
	cps := make(map[string]checkpoint, len(t.files))
	if !t.lastListingOK {
		for path, cp := range t.cfg.Positions.Logs() {
			cps[path] = cp
		}
	}
	// Stored entries not yet matched to a discovered file survive the rebuild.
	// claimPath keeps a path whose stat failed with a non-ENOENT error (EIO,
	// EACCES, ELOOP) out of the in-memory prune precisely because its absence
	// is unproven — but this rebuild used to consult only t.files, so the very
	// next save (including the immediate one scanDir triggers on discovery)
	// destroyed the entry ON DISK while the in-memory map still protected it: a
	// crash before the path recovered lost the offset and its Pending segments.
	// The two prunes must agree; t.checkpoints IS the surviving set. (Disjoint
	// from t.files by construction — initFile consumes an entry on discovery —
	// and the t.files loop below would win anyway.)
	for path, cp := range t.checkpoints {
		// Clone Pending: cps is handed to the store by SetLogsOwned, and these
		// slices belong to t.checkpoints, which outlives the call. Neither side
		// appends to them today; aliasing them across an ownership transfer is
		// the kind of thing that stays true only until it does not.
		if len(cp.Pending) > 0 {
			cp.Pending = append([]positions.Prefix(nil), cp.Pending...)
		}
		cps[path] = cp
	}
	for path, f := range t.files {
		t.extendFingerprint(f)
		cp := checkpoint{
			Offset: f.committed, Inode: f.inode,
			FingerprintLen: f.fp.Len, FingerprintHash: f.fp.Hash,
		}
		for _, c := range f.segments {
			// From is the segment's commit PROGRESS: a restart re-reads only
			// the owed [From, To) remainder.
			cp.Pending = append(cp.Pending, positions.Prefix{
				Inode:           c.inode,
				FingerprintLen:  c.fp.Len,
				FingerprintHash: c.fp.Hash,
				From:            c.committed,
				To:              c.to,
			})
		}
		cps[path] = cp
	}
	// SetLogsOwned, not SetLogs: cps is built fresh here on every save and
	// dropped on return, so the defensive copy SetLogs makes duplicated every
	// entry and every Pending slice — ~340 KB and ~300 allocations at 3000
	// tracked files — on the single sweep goroutine. Nothing below reads cps.
	if err := t.cfg.Positions.SetLogsOwned(cps); err != nil {
		// The hop flags stay ARMED: clearing them before the write forfeited
		// the "one file never carries two unsaved hops" invariant on a failed
		// save (full disk, read-only mount) — the next rotation of the same
		// file then no longer forced a save, and a crash lost the
		// intermediate inode the flag protocol exists to protect.
		//
		// Throttled: saveCheckpoints runs on a 10s cadence, so a persisting
		// condition (a read-only mount, a full disk — the two ways this
		// actually fails) is six identical lines a minute from every node in
		// the fleet for one unchanging fact, while
		// kubescrape_positions_save_errors_total already carries the rate. The
		// FIRST failure always logs (the throttle's window has not opened) and
		// the recovery logs once at Info, so the pair reads as a transition
		// rather than as a flood with no end marker.
		if t.positionsWarn.Allow(time.Minute) {
			t.log.Warn("writing positions file", "error", err)
		}
		t.positionsFailing = true
		return
	}
	if t.positionsFailing {
		t.positionsFailing = false
		t.log.Info("positions file write recovered")
	}
	t.hopsUnsaved = false
	t.discoveryUnsaved = false
	for _, f := range t.files {
		f.hopUnsaved = false
	}
}

// discoverySaveWindow coalesces the per-discovery saves of a BURST. One save
// marshals the whole positions document and fsyncs twice (the file, then the
// directory), which is ~11 ms at 100 tracked files and ~15 ms at 3000 on
// ext4/nvme — of which ~11 ms is the two fsyncs — and it runs on the single
// sweep goroutine that serves every log file on the node. scanDir is called
// once per fsnotify event (handleEvent), so a rolling update that recreates a
// node's pods was one whole-node stall per new container log file.
//
// The window bounds nothing in the ordinary case: a discovery that follows a
// quiet period saves IMMEDIATELY, exactly as before, because the last save is
// already older than the window. Only discoveries arriving while a save from
// less than a window ago is still fresh are deferred, and the deferral is what
// caps positions I/O at one save per window while the churn lasts.
//
// What is deliberately NOT coalesced is the ROTATION half — reopen's and
// readFile's "one file never carries two unsaved hops" saves, and the sweep's
// closing hopsUnsaved save. A missing hop is the case with no route back
// (nothing on disk names the intermediate inode), so those stay synchronous
// and immediate; a missing DISCOVERY entry resolves under the default
// -logs-unknown-files=auto to re-reading the file whole, i.e. duplicates,
// which at-least-once already tolerates.
const discoverySaveWindow = 250 * time.Millisecond

// saveDiscovery persists a discovery pass, coalescing bursts (see
// discoverySaveWindow). A deferred save is picked up by housekeeping, which
// runs after every sweep — and a discovery always schedules one — so the
// exposure is the window plus one housekeeping pass, never the 10-second
// cadence.
func (t *Tailer) saveDiscovery() {
	t.discoveryUnsaved = true
	if time.Since(t.lastCheckpoint) >= discoverySaveWindow {
		t.saveCheckpoints()
	}
}

// discoverySaveDue reports that a coalesced discovery save is waiting and its
// window has elapsed.
//
// Not while the store is FAILING. A save that cannot write leaves the flag
// armed, and re-offering it once per window would turn a read-only mount or a
// full disk into four failed saves a second — a
// kubescrape_positions_save_errors_total rate two orders of magnitude above the
// condition it describes, and a syscall storm on the sweep goroutine. Under a
// failure the flag simply waits for the 10-second cadence, which is exactly
// what a failed discovery save did before this window existed; the first save
// that succeeds clears the flag with it.
func (t *Tailer) discoverySaveDue() bool {
	return t.discoveryUnsaved && !t.positionsFailing && time.Since(t.lastCheckpoint) >= discoverySaveWindow
}
