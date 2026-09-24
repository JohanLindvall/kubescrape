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
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// debounce is how long a fsnotify burst is coalesced before the file is
// re-read: a ConfigMap update replaces several directory entries around the
// ..data symlink swap, and re-reading on the first of them can catch the
// half-swapped state.
const debounce = 100 * time.Millisecond

// reloadsFailing counts the watched files whose CURRENT content does not
// compile (or cannot be read), published as kubescrape_transform_reload_failing
// — the state the per-edit counter cannot carry: the failed counter is deduped
// by content hash, so it moves once per broken edit and an alert on its rate
// resolves while the file on disk is still broken. That matters beyond the
// edit, because the startup compile is fatal: a node restarted while this is
// nonzero CrashLoops. A count rather than a flag so that more than one reloader
// in a process (tests) cannot clear each other's state; the agent runs one.
var reloadsFailing atomic.Int64

// registerReloadGauge registers the gauge the first time a reloader RUNS, so a
// process without -transforms-file publishes nothing and a 0 means "watching,
// and the file compiles".
var registerReloadGauge sync.Once

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
	registerReloadGauge.Do(func() {
		obs.RegisterTransformReloadFailing(func() float64 { return float64(reloadsFailing.Load()) })
	})
	r := &reloader{w: w, path: path, log: log}
	// A watcher that stops watching stops vouching for the file either way.
	defer r.setFailing(false)
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
	// failing is whether the file as last read is broken — this reloader's
	// share of reloadsFailing. Unlike the report, it is not deduped: it is set
	// on every failed read and cleared by every compiling one.
	failing bool
}

// setFailing moves this reloader's share of the failing gauge on a change.
func (r *reloader) setFailing(broken bool) {
	if broken == r.failing {
		return
	}
	r.failing = broken
	if broken {
		reloadsFailing.Add(1)
	} else {
		reloadsFailing.Add(-1)
	}
}

// fail reports a reload failure once per distinct cause, and marks the file
// broken every time.
func (r *reloader) fail(hash, msg string, err error) {
	r.setFailing(true)
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
	// Hash FIRST. Unchanged content (a duplicate event, every poll tick) is
	// the active program's own source, compiled already, so there is nothing
	// to compile — and compiling is not free of effects: it runs every
	// section's module top level again, under its own wall-clock budget, so a
	// module-level print() logged a line on every node every poll period and
	// the fresh program was then thrown away. A hash equal to failedHash is
	// still recompiled: a module can fail on the clock alone on a CPU-starved
	// node, and re-reading the same bytes is the only retry it gets (fail()
	// keeps that silent).
	h := contentHash(raw)
	if h == r.currentHash {
		r.failedHash = ""
		r.setFailing(false) // the file on disk is the active program: it compiles
		return
	}
	p, err := Compile(raw)
	if err != nil {
		r.fail(h, "transforms reload failed; keeping the last good program", err)
		return
	}
	r.failedHash = ""
	r.setFailing(false)
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
