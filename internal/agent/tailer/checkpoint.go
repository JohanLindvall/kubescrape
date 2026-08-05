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
			cps[path] = checkpoint(cp)
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
	if err := t.cfg.Positions.SetLogs(cps); err != nil {
		t.log.Warn("writing positions file", "error", err)
	}
}
