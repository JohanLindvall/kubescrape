package transform

// Hot reload: the transforms file lives in its OWN ConfigMap (mounted as a
// directory, never subPath — subPath mounts never update) and is watched for
// changes. Kubernetes updates ConfigMap volumes atomically by swapping the
// ..data symlink, so the watch covers the DIRECTORY and any event triggers a
// re-read. Reloads compile-then-commit: a broken edit keeps the last good
// program running, counted and warned; convergence is observable via the
// program hash on /debug/transforms.

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// debounce is how long a fsnotify burst is coalesced before the file is
// re-read: a ConfigMap update replaces several directory entries around the
// ..data symlink swap, and re-reading on the first of them can catch the
// half-swapped state.
const debounce = 100 * time.Millisecond

// Reload watches path and swaps recompiled programs into w until ctx ends.
// The poll interval is a fallback for filesystems without inotify (and for
// missed events); 0 defaults to 30s.
func Reload(ctx context.Context, w *Wrapper, path string, poll time.Duration, log *slog.Logger) {
	if poll <= 0 {
		poll = 30 * time.Second
	}
	watcher, err := fsnotify.NewWatcher()
	if err == nil {
		// Watch the directory: ConfigMap updates replace the file via the
		// ..data symlink swap, which never fires a Write on the file itself.
		if err := watcher.Add(filepath.Dir(path)); err != nil {
			log.Warn("watching transforms dir; falling back to polling", "error", err)
		}
		defer func() { _ = watcher.Close() }()
	} else {
		log.Warn("fsnotify unavailable for transforms; polling only", "error", err)
	}

	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	r := &reloader{w: w, path: path, log: log}
	if current := w.Active(); current != nil {
		r.currentHash = current.Hash
	}
	apply := r.apply

	var events chan fsnotify.Event
	// errs must be drained too. fsnotify's reader goroutine SENDS on Errors,
	// so an undrained channel wedges it and file events stop arriving
	// altogether — silently, because the poll fallback keeps reloading and
	// nothing distinguishes "watch is dead" from "nothing changed". The tailer
	// already drains its watcher's errors for the same reason.
	var errs chan error
	if watcher != nil {
		events, errs = watcher.Events, watcher.Errors
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			apply()
		case err, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			// The poll ticker is the fallback, so a watch error degrades the
			// reload latency rather than stopping reloads.
			log.Warn("transforms watch error; falling back to polling", "path", path, "error", err)
		case _, ok := <-events:
			if !ok {
				events = nil
				continue
			}
			// Debounce the symlink-swap event burst. Selectable, not a bare
			// Sleep: this loop holds a live ctx, and a shutdown arriving inside
			// the window would otherwise wait it out and then reload the file
			// on the way to returning.
			t := time.NewTimer(debounce)
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
			drainEvents(events)
			apply()
		}
	}
}

// reloader is Reload's per-file state. It is a value rather than a pair of
// closures so the dedupe below can be driven directly by a test: the property
// is "one report per distinct file", which a goroutine woken by a ticker can
// only ever be asked about with a sleep.
type reloader struct {
	w    *Wrapper
	path string
	log  *slog.Logger
	// currentHash is the active program's content hash, and failedHash the
	// content hash of the last file that would NOT compile. Both exist for the
	// same reason: a re-read of unchanged bytes must be silent. A broken
	// transforms file is a PERSISTING CONDITION — this runs every poll period
	// on every node, and the agent's own log stream is itself collected, so an
	// unthrottled complaint feeds the pipeline it complains about. Keyed by
	// CONTENT rather than throttled by time, so a new broken edit is reported
	// (and counted) immediately while the thousandth re-read of the same
	// broken bytes is not, and the counter reads as distinct broken edits
	// rather than as the poll rate.
	currentHash string
	failedHash  string
}

// fail reports a reload failure once per distinct cause.
func (r *reloader) fail(hash, msg string, err error) {
	if hash == r.failedHash {
		return
	}
	r.failedHash = hash
	obs.TransformReloads.WithLabelValues("failed").Inc()
	r.log.Warn(msg, "path", r.path, "active", r.currentHash, "error", err)
}

func (r *reloader) apply() {
	raw, err := os.ReadFile(r.path)
	if err != nil {
		// An unreadable file is the same persisting condition; with no bytes
		// to hash, the error text is the identity.
		r.fail(contentHash([]byte("read:"+err.Error())), "transforms file unreadable; keeping the last good program", err)
		return
	}
	p, err := Compile(raw)
	if err != nil {
		r.fail(contentHash(raw), "transforms reload failed; keeping the last good program", err)
		return
	}
	r.failedHash = ""
	if p.Hash == r.currentHash {
		return // unchanged content (duplicate event / poll tick)
	}
	r.w.Swap(p)
	r.currentHash = p.Hash
	obs.TransformReloads.WithLabelValues("applied").Inc()
	r.log.Info("transforms reloaded", "path", r.path, "hash", p.Hash)
}

func drainEvents(events chan fsnotify.Event) {
	for {
		select {
		case <-events:
		default:
			return
		}
	}
}
