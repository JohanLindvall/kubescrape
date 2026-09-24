package tailer

// fsnotify plumbing: event dispatch, the per-file watch on the resolved
// symlink target directory (acquire-before-release, reference-counted), and
// the retry of scan-directory watches that failed to register.

import (
	"path/filepath"

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
	// Cache the RESOLVED DIRECTORY unconditionally, before anything that can
	// fail or return: findRotated locates a rotated segment's file by name in
	// it, and by the time that runs the live path is frequently gone (a
	// container GC'd after a CrashLoop restart takes the /var/log/containers
	// symlink while its rotated files remain), so its EvalSymlinks fallback
	// fails. Leaving targetDir empty for the file's whole life declared
	// still-on-disk segments unrecoverable — counted obs.LogPrefixLost and
	// retired. The nil-watcher branch was fixed for exactly that, and the
	// watcher.Add FAILURE below reinstated the same state for as long as the
	// failure lasts: a node that has exhausted fs.inotify.max_user_watches
	// fails every Add, so EVERY newly opened file kept targetDir empty.
	//
	// Hence the cache is separate from f.watchedDir, which names the directory
	// this file holds a watch REFERENCE on. They are usually equal; when the
	// Add fails they are not, and conflating them would make the short-circuit
	// below skip the retry the next open exists to make.
	f.targetDir = dir
	if t.watcher == nil {
		// releaseDir/unwatchTarget are refcount no-ops without a watcher, so
		// the cache costs nothing else.
		return
	}
	if dir == f.watchedDir {
		return // unchanged (the common case for every reopen)
	}
	// Acquire the new directory's watch BEFORE releasing the old one: a
	// rotation that retargets the symlink must never leave a window with no
	// OS watch, or a second rotation inside one poll interval goes unseen
	// and its segment is lost.
	if t.watchRefs[dir] == 0 {
		if err := t.watcher.Add(dir); err != nil {
			// The file keeps whatever watch it already held (if any) and
			// degrades to the poll cadence for this one; the next open retries.
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
	old := f.watchedDir
	f.watchedDir = dir
	t.releaseDir(f, old) // release the previous dir (refcounted; "" is a no-op)
}

// unwatchTarget releases the file's directory watch.
func (t *Tailer) unwatchTarget(f *file) {
	t.releaseDir(f, f.watchedDir)
	f.watchedDir = ""
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
