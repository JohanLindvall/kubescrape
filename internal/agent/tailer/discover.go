package tailer

// File discovery and change notification: source scanning, checkpoint
// seeding for newly discovered files, and the fsnotify watch plumbing over
// the symlink dir and resolved target dirs.

import (
	"os"
	"path/filepath"
	"slices"
	"strings"

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

// watchTarget registers the file's resolved log directory with the watcher.
func (t *Tailer) watchTarget(f *file) {
	if t.watcher == nil {
		return
	}
	target, err := filepath.EvalSymlinks(f.path)
	if err != nil {
		return // next open retries; any existing watch stays
	}
	dir := filepath.Dir(target)
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
	containerID = name[i+1:]
	if parts := strings.Split(name[:i], "_"); len(parts) == 3 {
		namespace = parts[1]
	}
	return containerID, namespace, true
}

// scanDir discovers new and removed log files across all sources by globbing
// their include patterns. checkpoints is non-nil only on the initial scan.
func (t *Tailer) scanDir(checkpoints map[string]checkpoint, initial bool) {
	seen := make(map[string]struct{})
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
			if _, done := seen[path]; done {
				continue // an earlier source already claimed this file
			}
			if src.excluded(path) {
				continue // the include match is implied: path came from src.glob()
			}
			if t.claimPath(src, path, seen, checkpoints, initial) {
				discovered = true
			}
		}
	}
	if listingOK {
		for path, f := range t.files {
			if _, ok := seen[path]; !ok {
				f.gone = true
			}
		}
	}
	obs.LogFiles.Set(float64(len(t.files)))
	if discovered && t.checkpointing() {
		// Persist immediately: until a file has a checkpoint entry, a crash
		// makes the restart treat it as pre-existing history and skip to its
		// end — the 10s periodic save left every new file a window in which
		// kill -9 lost its unread lines (and everything written while down).
		t.saveCheckpoints()
	}
}

// claimPath decides one globbed path's fate for its source: skip (non-regular
// or transiently unstattable), claim-though-skipped (excluded namespace or
// unparseable CRI name — a later catch-all source must not resurrect it),
// already tracked (unmark a raced gone flag), or newly discovered. It reports
// whether a NEW file was tracked.
func (t *Tailer) claimPath(src *compiledSource, path string, seen map[string]struct{}, checkpoints map[string]checkpoint, initial bool) bool {
	if st, err := os.Stat(path); err != nil || !st.Mode().IsRegular() {
		// A transient stat failure on a file we already track must not mark
		// it gone (drop would delete its checkpoint and a rediscovery would
		// re-ingest the whole file); only genuine absence may.
		if err != nil && !os.IsNotExist(err) {
			if _, known := t.files[path]; known {
				seen[path] = struct{}{}
			}
		}
		// Non-regular files (FIFOs, sockets, devices) are never tracked:
		// open(2)/read(2) on a FIFO block indefinitely and would wedge the
		// single sweep goroutine node-wide.
		return false
	}
	var id string
	if src.containerd {
		cid, namespace, ok := parseFileName(filepath.Base(path))
		if !ok || slices.Contains(t.cfg.ExcludeNamespaces, namespace) {
			// The file is CLAIMED by this source even though it is skipped:
			// an excluded namespace (or an unparseable CRI name) must not
			// fall through to a later catch-all source — ExcludeNamespaces is
			// global tailer config (the observability feedback-loop guard),
			// and a later source exporting the raw CRI lines would defeat it.
			seen[path] = struct{}{}
			return false
		}
		id = cid
	}
	seen[path] = struct{}{}
	if known, ok := t.files[path]; ok {
		// A previous listing may have raced a rename+recreate rotation (the
		// path momentarily absent between the two syscalls) and marked the
		// file gone; this listing proves it is back — unmark it before a
		// sweep drops it.
		known.gone = false
		return false
	}
	f := &file{
		path:        path,
		source:      src,
		containerID: id,
		compressed:  src.compressed || strings.HasSuffix(path, ".gz"),
		dirty:       true, // read on the first (event-driven) sweep
	}
	t.newPipeline(f)
	t.initFile(f, checkpoints, initial)
	t.files[path] = f
	return true
}

// initFile seeds a newly discovered file's checkpoint/starting offset.
func (t *Tailer) initFile(f *file, checkpoints map[string]checkpoint, initial bool) {
	if !initial {
		return
	}
	if cp, ok := checkpoints[f.path]; ok {
		f.committed = cp.Offset
		f.inode = cp.Inode
		f.fp = fingerprint{Len: cp.FingerprintLen, Hash: cp.FingerprintHash}
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
		// A rotation that happened while the agent was DOWN: the path now
		// names a DIFFERENT incarnation than the checkpoint. The checkpointed
		// identity + offset are everything needed to recover the old tail's
		// remainder from the rotated file — synthesize an open-ended segment
		// (to = -1: feedSegments reads it to EOF via findRotated and pins the
		// range, or counts obs.LogPrefixLost and retires it if the runtime
		// already pruned the file). Previously this remainder was lost
		// silently and uncounted.
		if !f.compressed && f.inode != 0 && f.committed > 0 {
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
	} else if !f.compressed {
		// Present at startup with no checkpoint entry. Where to start is
		// configurable (Config.UnknownFiles): "end" skips it as pre-existing
		// history; "start" reads it whole; "auto" (default) reads from the
		// start when the checkpoint store already has entries — the agent ran
		// before, so this file appeared while it was down and its content is
		// unshipped, not history. Compressed archives are always read whole.
		mode := t.cfg.UnknownFiles
		if mode == "" || mode == "auto" {
			if len(checkpoints) > 0 {
				mode = "start"
			} else {
				mode = "end"
			}
		}
		if mode == "end" {
			if st, err := os.Stat(f.path); err == nil {
				f.committed = st.Size()
			}
		}
	}
}
