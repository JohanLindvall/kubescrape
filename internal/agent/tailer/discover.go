package tailer

// File discovery and change notification: source scanning, checkpoint
// seeding for newly discovered files, and the fsnotify watch plumbing over
// the symlink dir and resolved target dirs.

import (
	"context"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/fsnotify/fsnotify"
)

// handleEvent processes one fsnotify event; it reports whether a dirty sweep
// should be scheduled.
func (t *Tailer) handleEvent(ev fsnotify.Event) bool {
	dir := filepath.Dir(ev.Name)
	if _, isScanDir := t.scanDirs[dir]; isScanDir {
		// A file (or symlink) appeared/disappeared in a discovery directory:
		// rediscover immediately. A recreated symlink names an already-tracked
		// path — mark that file dirty too, or a RETARGETED link (new target
		// dir, no events from the old one ever again) waits a full poll
		// interval before the rotation is even noticed.
		if ev.Op&(fsnotify.Create|fsnotify.Remove|fsnotify.Rename) != 0 {
			if f, ok := t.files[ev.Name]; ok {
				f.dirty = true
			}
			t.scanDir(nil, false)
			return true
		}
		// The log file may live directly in the watched directory (no symlink
		// indirection): treat writes like target-dir events.
		if f, ok := t.files[ev.Name]; ok && ev.Op&fsnotify.Write != 0 {
			f.dirty = true
			return true
		}
		return false
	}
	// A write/create in a watched target directory: mark the files tailing
	// that directory (rotation creates a new file there, too).
	dirty := false
	for f := range t.byTargetDir[dir] {
		f.dirty = true
		dirty = true
	}
	return dirty
}

// retryScanWatches (re-)registers every discovery-directory watch on every
// discovery pass — UNCONDITIONALLY, never skipping dirs it believes watched.
// The kernel auto-removes an inotify watch when the watched directory itself
// is deleted, moved or unmounted, and fsnotify drops it from its bookkeeping
// without any event this side could key an invalidation on (the event's Name
// is the dir itself, so handleEvent attributes it to the parent). A skip list
// therefore turned one dir recreation into a permanent degradation to poll
// cadence — under which sub-poll-interval rename rotations lose segments.
// Add is idempotent on a live watch, so the steady state is one cheap
// inotify_add_watch per dir per pass; watchedScan remains only to log
// transitions once and to gate the startup "nothing watched" warning.
// Failures stay at Debug — the startup Warn already named the directory once.
func (t *Tailer) retryScanWatches() {
	if t.watcher == nil {
		return
	}
	for dir := range t.scanDirs {
		if err := t.watcher.Add(dir); err != nil {
			delete(t.watchedScan, dir)
			t.log.Debug("watching log directory still failing", "dir", dir, "error", err)
			continue
		}
		if _, ok := t.watchedScan[dir]; !ok {
			t.watchedScan[dir] = struct{}{}
			t.log.Info("log directory watch established", "dir", dir)
		}
	}
}

// watchTarget resolves the file's log directory, caches it on the file and
// registers it with the watcher.
func (t *Tailer) watchTarget(f *file) {
	target, err := filepath.EvalSymlinks(f.path)
	if err != nil {
		return // next open retries; any existing watch stays
	}
	dir := filepath.Dir(target)
	if t.watcher == nil {
		// No watcher (-logs-watch=false, or a failed fsnotify init) — but the
		// RESOLVED DIRECTORY is still needed: findRotated locates a rotated
		// segment's file by name in it, and by the time that runs the live path
		// is frequently gone (a container GC'd after a CrashLoop restart takes
		// the /var/log/containers symlink while its rotated files remain), so
		// its EvalSymlinks fallback fails. Leaving targetDir empty for the
		// file's whole life declared still-on-disk segments unrecoverable —
		// counted obs.LogPrefixLost and retired. releaseDir/unwatchTarget are
		// refcount no-ops without a watcher, so the cache costs nothing else.
		f.targetDir = dir
		return
	}
	if dir == f.targetDir {
		return // unchanged (the common case for every reopen)
	}
	// Acquire the new directory's watch BEFORE releasing the old one: a
	// rotation that retargets the symlink must never leave a window with no
	// OS watch, or a second rotation inside one poll interval goes unseen
	// and its segment is lost.
	if t.watchRefs[dir] == 0 {
		if err := t.watcher.Add(dir); err != nil {
			t.log.Debug("watching log target directory", "dir", dir, "error", err)
			return
		}
	}
	t.watchRefs[dir]++
	if t.byTargetDir == nil {
		t.byTargetDir = make(map[string]map[*file]struct{})
	}
	set := t.byTargetDir[dir]
	if set == nil {
		set = make(map[*file]struct{})
		t.byTargetDir[dir] = set
	}
	set[f] = struct{}{}
	old := f.targetDir
	f.targetDir = dir
	t.releaseDir(f, old) // release the previous dir (refcounted; "" is a no-op)
}

// unwatchTarget releases the file's directory watch.
func (t *Tailer) unwatchTarget(f *file) {
	t.releaseDir(f, f.targetDir)
	f.targetDir = ""
}

// releaseDir drops one reference on a watched target directory and removes
// f from its dirty-marking index.
func (t *Tailer) releaseDir(f *file, dir string) {
	if t.watcher == nil || dir == "" {
		return
	}
	if t.watchRefs[dir]--; t.watchRefs[dir] <= 0 {
		delete(t.watchRefs, dir)
		// Never remove the watch on a discovery directory: those are watched
		// unconditionally from Run and both discovery and same-dir tailing
		// depend on their events. Under a rotation storm every file sharing
		// the dir can be momentarily unregistered (between reopen and the
		// next sweep's ensureOpen); dropping the OS watch then silences all
		// events until a poll tick re-adds it — and the resulting event gap
		// widens the unregistered windows, cascading into whole rotated
		// segments being lost.
		if _, isScanDir := t.scanDirs[dir]; !isScanDir {
			_ = t.watcher.Remove(dir)
		}
	}
	if set := t.byTargetDir[dir]; set != nil {
		delete(set, f)
		if len(set) == 0 {
			delete(t.byTargetDir, dir)
		}
	}
}

// parseFileName extracts the container ID and namespace from a
// <pod>_<namespace>_<container>-<containerID>.log name.
func parseFileName(name string) (containerID, namespace string, ok bool) {
	name, found := strings.CutSuffix(name, ".log")
	if !found {
		return "", "", false
	}
	i := strings.LastIndexByte(name, '-')
	if i < 0 || i == len(name)-1 {
		return "", "", false
	}
	// <pod>_<namespace>_<container>: exactly two underscores. Two Cuts rather
	// than strings.Split, which allocates a three-element slice for every
	// globbed file on every discovery pass.
	//
	// The structure is part of the VERDICT, not just of the namespace: a name
	// without it is not a CRI name, and reporting ok with an empty namespace
	// made claimPath's "unparseable CRI name" claim-and-skip miss exactly those
	// files. Each was then tracked with a bogus container ID (the tail after
	// the last '-'), never read — nothing is read before it resolves — and
	// blocking on a metadata lookup that can never succeed once a minute
	// forever, on the single sweep goroutine.
	_, rest, ok := strings.Cut(name[:i], "_")
	if !ok {
		return "", "", false
	}
	ns, container, ok := strings.Cut(rest, "_")
	if !ok || strings.IndexByte(container, '_') >= 0 {
		return "", "", false
	}
	return name[i+1:], ns, true
}

// scanSets are the two INDEPENDENT proofs one discovery pass accumulates.
//
// seen is presence: the path was listed AND stat succeeded (or the file was
// deliberately claimed-and-skipped). It is what clears a tracked file of the
// gone flag and what keeps it from being set.
//
// unproven is "listed, but the stat failed" — an ENOENT (a rename rotation
// caught between the readdir and the stat), an EIO, an EACCES. It protects the
// STORED checkpoint from being pruned, and nothing else.
//
// They are separate because the two consumers need different proofs. Pruning a
// checkpoint entry is destructive and irreversible (a recreated path would read
// from zero and its Pending prefixes — initFile's only route back to a rotated
// inode's unshipped tail — are gone), so it demands proven absence. Gone-marking
// a TRACKED file is neither: the sweep re-stats the path and resurrects it if it
// is back, and until then the file's remaining bytes are drained from the fd we
// already hold. Conflating them into one set meant an unstattable path suppressed
// gone-marking too — and for the primary shape this tailer tracks that is not a
// transient at all: /var/log/containers/*.log are SYMLINKS, the readdir-based
// glob lists a dangling one forever while os.Stat follows it and returns ENOENT
// forever, so such a file was never released — fd, unlinked inode, files-map
// entry and checkpoint entry pinned for the process lifetime with nothing logged
// and no counter moving.
//
// skipped is the third, and it is a REPORTING set rather than a proof: path ->
// the reason discovery declined to track it. Every one of those decisions used
// to be silent by design, which made "why is this pod's log missing?" — the
// operator's first question about a log pipeline — unanswerable from the logs
// and the metrics, the only two things a first live run has. It is diffed
// against the previous scan's (Tailer.skipped) so a file counts and logs ONCE
// per reason instead of once per discovery pass, and an entry that ends up
// tracked anyway (a later source claimed what an earlier one declined) is
// dropped before the diff.
type scanSets struct {
	seen     map[string]struct{}
	unproven map[string]struct{}
	skipped  map[string]string
	// detail carries the ERROR behind a skip whose reason is a failure rather
	// than a selection (today only skipStatError). Kept beside skipped rather
	// than folded into it because skipped's value is the metric label value
	// AND the change-detection key: an errno that changes while the reason
	// does not must not re-log.
	detail map[string]string
}

// skip records why this path is not being tracked. Last writer wins: sources
// are consulted in order and a later source's verdict is the more final one.
// The map is allocated on demand so a zero-value scanSets is usable — the
// other two sets are proofs every caller must supply, this one is reporting.
func (s *scanSets) skip(path, reason string) {
	if s.skipped == nil {
		s.skipped = make(map[string]string)
	}
	s.skipped[path] = reason
}

// skipErr is skip for a reason that is a FAILURE, recording the error so the
// report can name it. A counter can say a file was not collected; only the
// errno says whether to fix a mount, a permission or a disk.
func (s *scanSets) skipErr(path, reason string, err error) {
	s.skip(path, reason)
	if s.detail == nil {
		s.detail = make(map[string]string)
	}
	s.detail[path] = err.Error()
}

// Discovery skip reasons. They are metric LABEL VALUES
// (kubescrape_log_files_skipped_total{reason}) as well as log values, so they
// are spelled once, here, and the metric's help text enumerates exactly these.
const (
	skipSourceExclude   = "source_exclude"
	skipExcludedNS      = "excluded_namespace"
	skipNamespaceNotSel = "namespace_not_selected"
	skipUnparseableName = "unparseable_name"
	skipTooOld          = "too_old"
	skipNonRegular      = "non_regular"
	skipStatError       = "stat_error"
)

// scanDir discovers new and removed log files across all sources by globbing
// their include patterns. initial marks the startup scan, which seeds the
// stored positions (checkpoints) every later scan also consults — see
// Tailer.checkpoints and compiledSource.startingUp for why the seeding and the
// startup-only -logs-unknown-files policy outlive this one call.
func (t *Tailer) scanDir(checkpoints map[string]checkpoint, initial bool) {
	t.retryScanWatches()
	if initial {
		t.checkpoints = checkpoints
		t.hadStoredCheckpoints = len(checkpoints) > 0
		// A source's startup is not over until ITS listing SUCCEEDS: with the
		// very first glob failing, nothing is discovered, and the files a later
		// pass finds are that source's startup set arriving late — not new files.
		for _, src := range t.sources {
			src.startingUp = true
		}
	}
	sets := scanSets{seen: make(map[string]struct{}), unproven: make(map[string]struct{}), skipped: make(map[string]string)}
	discovered := false
	listingOK := true
	defer func() {
		if !listingOK && !t.warnedListing {
			t.warnedListing = true
			t.log.Warn("a source glob failed; gone-detection is disabled until listings succeed")
		}
		if listingOK {
			t.warnedListing = false
		}
	}()
	for _, src := range t.sources {
		paths, ok := src.glob()
		if !ok {
			listingOK = false // a failed glob proves nothing about absent files
		}
		for _, path := range paths {
			if _, done := sets.seen[path]; done {
				continue // an earlier source already claimed this file
			}
			if src.excluded(path) {
				// The include match is implied: path came from src.glob().
				// Recorded rather than dropped silently — an over-broad
				// exclude glob is the cheapest way to lose a workload's logs
				// and the hardest to spot, since nothing anywhere names the
				// file. A later source may still claim it, and reportSkips
				// drops the entry when one does.
				sets.skip(path, skipSourceExclude)
				continue
			}
			if t.claimPath(src, path, &sets) {
				discovered = true
			}
		}
		// This source's startup ends here — AFTER its own paths were claimed,
		// so the files this very pass discovered are still governed by
		// -logs-unknown-files, and anything appearing under it later is
		// genuinely new. Per source: a sibling stuck on a failing glob must not
		// keep this one's new files being skipped as history.
		//
		// It ends on a PARTIAL listing too, which is the whole distinction: a
		// permanently unreadable subdirectory under a `**` include keeps ok
		// false for the life of the process, and gating on ok therefore left
		// the source in startup forever — so every file it discovered an hour
		// later was treated as history, seeked to EOF under
		// -logs-unknown-files=end (also what `auto` resolves to on a first run)
		// and CHECKPOINTED there. That silently skips a new container's first
		// lines, uncounted. "Has this source produced a listing?" and "was the
		// listing complete?" are different questions; ok answers only the
		// second, and it still gates gone-detection and checkpoint pruning
		// below, which are the things that genuinely need absence proven.
		if ok || len(paths) > 0 {
			src.startingUp = false
		}
	}
	// listingOK is the conjunction over EVERY source, deliberately unlike the
	// per-source startup flag above: what follows rests on "absence is proven",
	// and a path is not attributable to a source before some source globs it —
	// so only a scan in which every source listed may declare a file gone or
	// drop its stored offset.
	if listingOK {
		for path, f := range t.files {
			if _, ok := sets.seen[path]; !ok {
				f.gone = true
			}
		}
		// The same proof that lets saveCheckpoints prune the STORE applies to
		// the pending map: a listing that saw the sources and did not see this
		// path means the file is gone, so its stored offset must not later be
		// applied to a recreated path — that would skip its first bytes as
		// though they had shipped. (Keeping the two in lockstep is what makes a
		// scan-then-save sequence idempotent.)
		//
		// This is the ONE consumer the unproven set feeds: a path the glob
		// listed and the stat could not answer for is not proven absent, and
		// pruning is the destructive, irreversible half (see scanSets).
		for path := range t.checkpoints {
			if _, ok := sets.seen[path]; ok {
				continue
			}
			if _, ok := sets.unproven[path]; ok {
				continue
			}
			delete(t.checkpoints, path)
		}
	}
	// Publish THIS scan's verdict before anything can save. It used to be set
	// in the defer above, which runs only after the body — so the
	// saveCheckpoints below ran against the PREVIOUS scan's verdict, and a scan
	// where one source globbed fine (setting discovered) while another failed
	// pruned the store as though absence had been proven. That destroys the
	// persisted offsets of every file the failed listing could not see, after
	// which the next start treats them as history and skips them to the end —
	// exactly the loss the flag exists to prevent (see saveCheckpoints).
	t.lastListingOK = listingOK
	t.reportSkips(sets.skipped, sets.detail, listingOK)
	obs.LogFiles.Set(float64(len(t.files)))
	if discovered && t.checkpointing() {
		// Persist immediately: until a file has a checkpoint entry, a crash
		// makes the restart treat it as pre-existing history and skip to its
		// end — the 10s periodic save left every new file a window in which
		// kill -9 lost its unread lines (and everything written while down).
		//
		// Through saveDiscovery, which is still immediate whenever the last
		// save is older than discoverySaveWindow — i.e. for every ordinary
		// discovery — and coalesces only the passes of a BURST. scanDir runs
		// once per fsnotify event, so without that a rolling update paid one
		// ~15 ms whole-node stall per new container log file.
		t.saveDiscovery()
	}
}

// reportSkips makes this pass's declined files visible, exactly once each.
//
// Every skip decision in claimPath is re-taken on EVERY discovery pass — the
// 2s dirTicker plus every fsnotify event in a scan dir — so counting or
// logging at the decision site would report one file hundreds of times an
// hour. The diff against the previous pass is what turns "this file is
// skipped" (a state, and the thing an operator actually wants) into one count
// and one line: a newly skipped file, or one whose reason CHANGED, is
// reported; a file skipped for the same reason as last pass is not; a file
// that stopped being skipped is forgotten, so it reports again if it comes
// back.
//
// A path some source declined and another then CLAIMED is not a skip at all —
// that is `namespaces` doing its routing job — so tracked paths are dropped
// before the diff.
//
// The log line is Debug: on a healthy node most of these are the operator's own
// configuration working (an excluded namespace, an ignoreOlder cutoff), and
// only the counter belongs in the steady-state signal. It is guarded because
// slog evaluates arguments eagerly and this loop runs per pass.
//
// skipStatError is the ONE exception, and it gets a throttled Warn. Every other
// reason here is a SELECTION — the operator asked for it, so the counter alone
// is the right weight. A stat failure is a file the operator meant to collect
// and is not collecting, and the counter cannot say WHICH file or WHY: an
// EACCES from a hostPath mounted with the wrong ownership and an EIO from a
// failing disk are the same number and different jobs. That is precisely the
// context a counter cannot carry.
//
// Two bounds keep it from becoming the flood the Debug default was avoiding.
// A vanished path is EXCLUDED: an ENOENT here is a rename rotation caught
// between the readdir and the stat, which is both benign and constant on a busy
// node — it still COUNTS (the metric's help enumerates stat_error as any
// unstattable path), it just does not warn. And the surviving cases are gated
// by one keyless throttle naming a single example plus how many others share
// the pass, because a wrongly-mounted log directory fails every file in it at
// once and the remedy is the same for all of them.
func (t *Tailer) reportSkips(skipped, detail map[string]string, listingOK bool) {
	if !listingOK {
		// A failed glob proves nothing about the paths it did not list, and
		// FORGETTING one is what makes it count again — so a directory
		// flapping between readable and not would report every file it holds
		// on every recovery, breaking the "once per file" claim the counter's
		// help text makes. Carry the undecided ones forward; a later
		// successful listing drops whatever genuinely stopped being skipped.
		for path, reason := range t.skipped {
			if _, decided := skipped[path]; !decided {
				skipped[path] = reason
			}
		}
	}
	for path := range skipped {
		if _, tracked := t.files[path]; tracked {
			delete(skipped, path)
		}
	}
	debug := t.log.Enabled(context.Background(), slog.LevelDebug)
	// One example plus a count, rather than a line per file: see the throttle
	// note above. Collected in the diff loop so an unchanged skip stays silent.
	var statErrPath, statErrMsg string
	statErrs := 0
	for path, reason := range skipped {
		if was, ok := t.skipped[path]; ok && was == reason {
			continue
		}
		obs.LogFilesSkipped.WithLabelValues(reason).Inc()
		if reason == skipStatError && !strings.Contains(detail[path], "no such file or directory") {
			statErrs++
			if statErrPath == "" {
				statErrPath, statErrMsg = path, detail[path]
			}
		}
		if debug {
			t.log.Debug("log file not tracked", "path", path, "reason", reason)
		}
	}
	if statErrs > 0 && t.statErrWarn.Allow(statErrWarnEvery) {
		t.log.Warn("log file could not be stat'd and is not being collected",
			"path", statErrPath, "error", statErrMsg, "files", statErrs,
			"note", "the path was listed but cannot be read: check the ownership and mode of the log hostPath mount, "+
				"or the node's disk. Further reports are suppressed for "+statErrWarnEvery.String())
	}
	t.skipped = skipped
}

// statErrWarnEvery re-warns about unstattable log files at this cadence. The
// condition is a STATE (a mount is wrong, a disk is failing), so it persists
// across every pass of a sweep that runs every two seconds.
const statErrWarnEvery = 5 * time.Minute

// tooOld reports whether a source's ignoreOlder cutoff excludes this file.
//
// It applies ONLY to files we have never read: one already tracked, or one
// carrying a stored offset, is read however stale it looks. Ignoring those
// would abandon the bytes between the committed offset and EOF, and — worse —
// re-ingest the file from scratch if it were ever written to again, since
// dropping it also drops its checkpoint. A cutoff is a discovery-time cost
// lever ("don't start on last week's rotated logs"), not a retention policy.
func (t *Tailer) tooOld(st os.FileInfo, path string, cutoff time.Duration) bool {
	if time.Since(st.ModTime()) <= cutoff {
		return false
	}
	if _, known := t.files[path]; known {
		return false
	}
	_, hasCheckpoint := t.checkpoints[path]
	return !hasCheckpoint
}

// deniesNamespace is the DENY half of compiledSource.wantNamespace (sources.go)
// on its own: the source's excludeNamespaces globs, without the allowlist.
// claimPath needs the two apart because they have opposite claim semantics — a
// deny claims the file so no later source can resurrect it, an allowlist miss
// leaves it for one. Patterns are validated at startup (ValidateSources), so a
// path.Match error here cannot happen and reads as "no match".
func (s *compiledSource) deniesNamespace(ns string) bool {
	for _, pat := range s.excludeNamespaces {
		if ok, _ := path.Match(pat, ns); ok {
			return true
		}
	}
	return false
}

// claimPath decides one globbed path's fate for its source: skip (non-regular
// or transiently unstattable), claim-though-skipped (excluded namespace or
// unparseable CRI name — a later catch-all source must not resurrect it),
// already tracked (unmark a raced gone flag), or newly discovered. It reports
// whether a NEW file was tracked.
func (t *Tailer) claimPath(src *compiledSource, path string, sets *scanSets) bool {
	if known, ok := t.files[path]; ok && known.swept() {
		// Tracked, open and read by EVERY sweep, which stats this same path
		// itself and marks the file gone when that stat says ENOENT — so the
		// stat below would buy nothing here and cost one symlink resolution per
		// tracked file per pass (~7µs against ~2µs for an fstat), on a pass that
		// also runs synchronously from every create/remove event in a scan dir,
		// where its duration is time the fsnotify channel is not drained.
		// Everything the sweep does NOT stat — unresolved, annotation-excluded,
		// idle-closed, compressed, already gone — falls through and is stat'd.
		sets.seen[path] = struct{}{}
		return false
	}
	st, err := os.Stat(path)
	if err != nil || !st.Mode().IsRegular() {
		// An unstattable path is not a proven-absent one: an ENOENT is a path
		// the glob JUST LISTED vanishing between the two syscalls, i.e. a
		// rename rotation caught mid-scan, and EIO/EACCES/ELOOP prove nothing
		// either. Record it as unproven so the checkpoint prune spares it —
		// pruning a not-yet-consumed entry would make a recreated path read
		// from zero AND destroy the Pending prefixes that are initFile's only
		// route back to the rotated inode's unshipped tail. Gone-marking a
		// TRACKED file is a separate, recoverable decision and deliberately NOT
		// suppressed here (see scanSets).
		if err != nil {
			sets.unproven[path] = struct{}{}
			// A stat failure is the one entry in this set that is not a
			// SELECTION: an EACCES (a hostPath mounted with the wrong
			// ownership) or an EIO is a file the operator meant to collect
			// and is not collecting. It is reported for exactly that reason,
			// and a rename rotation caught between the readdir and the stat
			// reports it once too — the diff means one line, not one per pass.
			sets.skipErr(path, skipStatError, err)
		} else {
			// Non-regular files (FIFOs, sockets, devices) are never tracked:
			// open(2)/read(2) on a FIFO block indefinitely and would wedge the
			// single sweep goroutine node-wide.
			sets.skip(path, skipNonRegular)
		}
		return false
	}
	var id string
	if src.containerd {
		cid, namespace, ok := parseFileName(filepath.Base(path))
		if ok && src.deniesNamespace(namespace) {
			// A source's OWN excludeNamespaces is a PROHIBITION, exactly like
			// the global list below — so it claims-and-skips: without the claim
			// a later catch-all source resurrects the file the operator just
			// said not to tail, and the case that matters is the observability
			// feedback loop (the agent tails the collector's namespace and
			// amplifies precisely when the collector is struggling).
			sets.seen[path] = struct{}{}
			sets.skip(path, skipExcludedNS)
			return false
		}
		if ok && !src.wantNamespace(namespace) {
			// An allowlist MISS is the opposite of a prohibition: this SOURCE
			// does not want the namespace, but a later one may. Deliberately
			// NOT claimed — "prod through source A, the rest through source B"
			// is the allowlist's most obvious use, and claiming here made
			// source B collect nothing. (Routing is what `namespaces` is for;
			// `excludeNamespaces` denies, and denying cannot be undone by a
			// later source. wantNamespace merges both, so testing it alone
			// silently took the deny half along and lost the guard above.)
			//
			// Reported anyway: an allowlist that matches nothing looks
			// exactly like a source that is working, and a typo'd namespace
			// glob is silent otherwise. reportSkips drops the entry when a
			// later source does claim the file, so the routing case stays
			// quiet.
			sets.skip(path, skipNamespaceNotSel)
			return false
		}
		if !ok || slices.Contains(t.cfg.ExcludeNamespaces, namespace) {
			// The file is CLAIMED by this source even though it is skipped:
			// an excluded namespace (or an unparseable CRI name) must not
			// fall through to a later catch-all source — ExcludeNamespaces is
			// global tailer config (the observability feedback-loop guard),
			// and a later source exporting the raw CRI lines would defeat it.
			// A source's own excludeNamespaces claims-and-skips above for the
			// same reason. Either way the file is never opened, tracked or read.
			sets.seen[path] = struct{}{}
			if !ok {
				// A file in the containerd log directory whose name is not a
				// CRI name is a "should not happen" branch that happens — a
				// stray file, a different runtime's layout, an operator's
				// backup copy. It reads as a silent selection today.
				sets.skip(path, skipUnparseableName)
			} else {
				sets.skip(path, skipExcludedNS)
			}
			return false
		}
		id = cid
	}
	// AFTER the prohibitions above, never before them. A stale file in an
	// excluded namespace is still PROHIBITED, and skipping it here without the
	// claim left it for the next source — which, being a catch-all, has no
	// namespace logic at all and tailed the excluded namespace's raw CRI lines
	// for the process lifetime (once tracked, tooOld never fires again). The
	// cutoff itself stays a per-source SELECTION, like the namespaces
	// allowlist: unclaimed, so a later source with its own (or no) cutoff may
	// take the file.
	if src.ignoreOlder > 0 && t.tooOld(st, path, src.ignoreOlder) {
		sets.skip(path, skipTooOld)
		return false
	}
	sets.seen[path] = struct{}{}
	if known, ok := t.files[path]; ok {
		// A previous listing may have raced a rename+recreate rotation (the
		// path momentarily absent between the two syscalls) and marked the
		// file gone; this listing proves it is back — unmark it before a
		// sweep drops it. Through file.resurrect, the one home for that
		// decision: this branch cleared two of the three fields, and the
		// goneEnd it left behind made a replaced archive report a lost
		// remainder it had in fact delivered (see resurrect).
		known.resurrect()
		return false
	}
	f := &file{
		path:        path,
		source:      src,
		containerID: id,
		discovered:  time.Now(),
		compressed:  src.compressed || strings.HasSuffix(path, ".gz"),
		dirty:       true, // read on the first (event-driven) sweep
	}
	t.newPipeline(f)
	t.initFile(f)
	t.files[path] = f
	return true
}

// initFile seeds a newly discovered file's checkpoint/starting offset.
//
// The stored position is applied on WHATEVER pass discovers the file, not only
// the first: a startup glob that fails discovers nothing, and the files the 2s
// dirTicker then finds would each have started at byte 0 — a node-wide
// re-ingest with every Pending prefix destroyed, and no counter moving. Where a
// file with NO stored position starts still depends on the pass:
// -logs-unknown-files governs the startup set (f.source.startingUp, which lasts
// until THAT SOURCE's listing succeeds), while a file appearing after that is
// new and is read whole.
func (t *Tailer) initFile(f *file) {
	if cp, ok := t.checkpoints[f.path]; ok {
		// Consumed: the tailer's own offset is authoritative from here, and a
		// re-application after the file is dropped and its path recreated would
		// skip the new file's first bytes.
		delete(t.checkpoints, f.path)
		f.committed = cp.Offset
		f.inode = cp.Inode
		f.fp = fingerprint{Len: cp.FingerprintLen, Hash: cp.FingerprintHash}
		// Segments belong to the incremental path only, exactly like the
		// open-ended synthesis below: readArchive never calls feedSegments, so a
		// segment restored onto a COMPRESSED file is owed forever — settledGone
		// can never be true and saveCheckpoints rewrites the stale Pending list
		// on every save for the life of the process. (Reachable when a path with
		// Pending entries is later matched by a `compressed: true` source.)
		if !f.compressed {
			for _, pp := range cp.Pending {
				// Uncommitted rotated-away ranges at shutdown/crash: re-read from
				// the rotated files (oldest first) before this (new) inode is
				// consumed. segmentsFed is already false. Ids are per-process:
				// issue them in list order, below the tail id issued afterwards.
				f.segSeq++
				f.segments = append(f.segments, &segment{
					id:        f.segSeq,
					inode:     pp.Inode,
					fp:        fingerprint{Len: pp.FingerprintLen, Hash: pp.FingerprintHash},
					committed: pp.From,
					to:        pp.To,
				})
			}
		}
		// A rotation that happened while the agent was DOWN: the path now
		// names a DIFFERENT incarnation than the checkpoint. The checkpointed
		// identity + offset are everything needed to recover the old tail's
		// remainder from the rotated file — synthesize an open-ended segment
		// (to = -1: feedSegments reads it to EOF via findRotated and pins the
		// range, or counts obs.LogPrefixLost and retires it if the runtime
		// already pruned the file). Previously this remainder was lost
		// silently and uncounted.
		// There is deliberately no `f.committed > 0` guard: committed == 0
		// means NOTHING was ever shipped from that incarnation — a checkpoint
		// is persisted as soon as a file is discovered, so an export failing
		// since startup leaves exactly this state — which is the MAXIMUM-loss
		// case, not a no-op. (A file skipped as history under
		// -logs-unknown-files=end checkpoints at its SIZE, not 0, so it is
		// unaffected.) Identity is pinned by the exact inode in findRotated,
		// so an empty fingerprint (a file discovered at size 0) cannot
		// mismatch onto some other file.
		if !f.compressed && f.inode != 0 {
			if st, err := os.Stat(f.path); err == nil && inodeOf(st) != f.inode {
				f.segSeq++
				f.segments = append(f.segments, &segment{
					id:        f.segSeq,
					inode:     f.inode,
					fp:        f.fp,
					committed: f.committed,
					to:        -1,
				})
				f.committed = 0
				f.inode = 0
				f.fp = fingerprint{}
			}
		}
		f.newTail()
	} else if f.source.startingUp && !f.compressed {
		// Present at startup with no checkpoint entry. Where to start is
		// configurable (Config.UnknownFiles): "end" skips it as pre-existing
		// history; "start" reads it whole; "auto" (default) reads from the
		// start when the checkpoint store already has entries — the agent ran
		// before, so this file appeared while it was down and its content is
		// unshipped, not history. Compressed archives are always read whole.
		//
		// Only while ITS SOURCE IS STARTING UP: a file that appears once that
		// source's listing has succeeded was created under our eyes, so it is
		// read from the beginning whatever this setting says — "end" is a
		// history policy for the backlog present before the agent, never a
		// licence to skip a container's first lines. Another source's glob
		// still failing says nothing about this one (see
		// compiledSource.startingUp).
		mode := t.cfg.UnknownFiles
		if mode == "" || mode == "auto" {
			// A CORRUPT positions file decodes to nothing, so the store looks
			// empty and exactly like a first run — which would skip every
			// existing file to its end and lose the whole unshipped window.
			// Prefer re-reading: at-least-once already tolerates duplicates,
			// and losing data to a bad byte in a state file does not.
			corrupt := t.cfg.Positions != nil && t.cfg.Positions.Corrupt()
			if t.hadStoredCheckpoints || corrupt {
				mode = "start"
			} else {
				mode = "end"
			}
		}
		if mode == "end" {
			if st, err := os.Stat(f.path); err == nil {
				f.committed = st.Size()
				// The single most consequential silent decision in this
				// package: everything already in this file is declared
				// history and will never be exported. It is correct — that is
				// what the mode means — and it is also the most common "the
				// agent is collecting nothing" report on a first live run,
				// because `auto` resolves HERE whenever the positions store is
				// empty. No counter: skipping history is not a failure and a
				// rate would page on every fresh volume. The size is what says
				// how much was skipped. Arguments are field reads and a stat
				// already taken, so no Enabled guard is warranted.
				t.log.Debug("existing log file skipped to its end as history",
					"path", f.path, "bytes", st.Size(), "reason", mode,
					"flag", "-logs-unknown-files")
			}
		}
	}
}
