package main

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// capturingHandler collects records so a test can assert on the LEVEL of what
// was logged: the campaign's finding was not a missing metric alone, it was a
// 60-second outage whose entire log was INFO.
type capturingHandler struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (h *capturingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.recs = append(h.recs, r.Clone())
	return nil
}

func (h *capturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capturingHandler) WithGroup(string) slog.Handler      { return h }

// at returns the messages logged at one level.
func (h *capturingHandler) at(level slog.Level) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, r := range h.recs {
		if r.Level == level {
			out = append(out, r.Message)
		}
	}
	return out
}

// attr returns the value of one attribute of the first record at level, or "".
func (h *capturingHandler) attr(level slog.Level, key string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.recs {
		if r.Level != level {
			continue
		}
		var out string
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == key {
				out = a.Value.String()
				return false
			}
			return true
		})
		return out
	}
	return ""
}

func (h *capturingHandler) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.recs = nil
}

func newCapturingLogger() (*slog.Logger, *capturingHandler) {
	h := &capturingHandler{}
	return slog.New(h), h
}

// The watchdog's whole job: turn an unreachable API server into a 0 gauge, a
// rising counter and a WARN — none of which the passive signals produce.
func TestAPIServerWatchdogReportsAnOutageAndItsRecovery(t *testing.T) {
	var fail atomic.Bool
	probeErr := errors.New("dial tcp 10.96.0.1:443: connect: connection refused")
	log, h := newCapturingLogger()
	w := newAPIServerWatchdog(func(context.Context) error {
		if fail.Load() {
			return probeErr
		}
		return nil
	}, time.Minute, log)

	ctx := context.Background()

	// Healthy: reachable, nothing counted, nothing said.
	w.probeOnce(ctx)
	if !w.reachable.Load() || w.failures.Load() != 0 {
		t.Fatalf("after a healthy probe: reachable=%v failures=%d", w.reachable.Load(), w.failures.Load())
	}
	if got := len(h.recs); got != 0 {
		t.Fatalf("healthy probe logged %d records, want silence", got)
	}

	// The transition to unreachable warns immediately and names the cause.
	fail.Store(true)
	w.probeOnce(ctx)
	if w.reachable.Load() {
		t.Fatal("gauge still reads reachable after a failed probe")
	}
	if got := w.failures.Load(); got != 1 {
		t.Fatalf("failures = %d, want 1", got)
	}
	warns := h.at(slog.LevelWarn)
	if len(warns) != 1 || !strings.Contains(warns[0], "API server unreachable") {
		t.Fatalf("transition warns = %v, want one 'API server unreachable' line", warns)
	}

	// A PERSISTING outage is throttled: it still counts every probe, but the
	// condition is unchanged and one line per probe would bury the recovery.
	h.reset()
	w.probeOnce(ctx)
	w.probeOnce(ctx)
	if got := w.failures.Load(); got != 3 {
		t.Fatalf("failures = %d, want 3 (every probe counts)", got)
	}
	if warns := h.at(slog.LevelWarn); len(warns) != 0 {
		t.Fatalf("persisting outage re-warned per probe: %v", warns)
	}

	// Recovery flips the gauge back and says so, at INFO.
	h.reset()
	fail.Store(false)
	w.probeOnce(ctx)
	if !w.reachable.Load() {
		t.Fatal("gauge still reads unreachable after a successful probe")
	}
	infos := h.at(slog.LevelInfo)
	if len(infos) != 1 || !strings.Contains(infos[0], "reachable again") {
		t.Fatalf("recovery infos = %v, want one 'reachable again' line", infos)
	}

	// And the next outage warns again immediately — the throttle governs a
	// persisting state, not the transition into one.
	h.reset()
	fail.Store(true)
	w.probeOnce(ctx)
	if warns := h.at(slog.LevelWarn); len(warns) != 1 {
		t.Fatalf("second outage warns = %v, want one line", warns)
	}
}

// Both outage lines put a COUNT beside a per-outage DURATION, so the count has
// to have the same scope. It used to be the process-lifetime total, which reads
// off one line as "57 probes failed in the 12 seconds this outage lasted"; the
// lifetime total is what kubescrape_apiserver_probe_failures_total is for.
func TestOutageLogLinesCountOnlyTheCurrentOutage(t *testing.T) {
	var fail atomic.Bool
	log, h := newCapturingLogger()
	w := newAPIServerWatchdog(func(context.Context) error {
		if fail.Load() {
			return errors.New("connection refused")
		}
		return nil
	}, time.Minute, log)
	ctx := context.Background()

	// First outage: three failed probes, then recovery.
	fail.Store(true)
	w.probeOnce(ctx)
	w.probeOnce(ctx)
	w.probeOnce(ctx)
	fail.Store(false)
	w.probeOnce(ctx)

	// Second outage: two failed probes, then recovery.
	h.reset()
	fail.Store(true)
	w.probeOnce(ctx)
	if got := h.attr(slog.LevelWarn, "failedProbes"); got != "1" {
		t.Errorf("transition warn failedProbes = %q, want 1 (this outage), not the lifetime total", got)
	}
	w.probeOnce(ctx)
	h.reset()
	fail.Store(false)
	w.probeOnce(ctx)
	if got := h.attr(slog.LevelInfo, "failedProbes"); got != "2" {
		t.Errorf("recovery failedProbes = %q, want 2 (this outage), not the lifetime total", got)
	}

	// The lifetime total is still complete — it is just not what the log says.
	if got := w.failures.Load(); got != 5 {
		t.Errorf("failures = %d, want 5 across both outages", got)
	}
}

// Our own shutdown cancels the probe's context. Reporting that as an outage
// would stamp a 0 on the gauge and a WARN in the log on every clean
// termination — the failure would be the process, not the cluster.
func TestAPIServerWatchdogIgnoresItsOwnCancellation(t *testing.T) {
	log, h := newCapturingLogger()
	w := newAPIServerWatchdog(func(ctx context.Context) error {
		return ctx.Err()
	}, time.Minute, log)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w.probeOnce(ctx)

	if !w.reachable.Load() {
		t.Error("a cancelled probe was reported as an outage")
	}
	if got := w.failures.Load(); got != 0 {
		t.Errorf("failures = %d, want 0 for our own cancellation", got)
	}
	if got := len(h.recs); got != 0 {
		t.Errorf("cancelled probe logged %d records, want silence", got)
	}
}

// run probes IMMEDIATELY rather than after one interval: at the default 30s an
// outage present at startup would otherwise go unreported for half a minute,
// and the interval is an operator's knob that may be much larger.
func TestAPIServerWatchdogProbesImmediately(t *testing.T) {
	probed := make(chan struct{}, 1)
	log, _ := newCapturingLogger()
	w := newAPIServerWatchdog(func(context.Context) error {
		select {
		case probed <- struct{}{}:
		default:
		}
		return nil
	}, time.Hour, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); w.run(ctx) }()

	select {
	case <-probed:
	case <-time.After(5 * time.Second):
		t.Fatal("run did not probe before the first tick")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return when its context was cancelled")
	}
}

// The two metrics are published exactly when the probe RUNS. Disabled, the
// families must stay ABSENT: a published 0 would say "unreachable" and a
// published 1 would be a claim the process never checked.
//
// The assertion is a DELTA over obs.Registry, not an absolute absence, because
// that registry is a process GLOBAL and registrations are permanent: under
// `go test -count=2` the second run shares the process with the first, so
// "nothing named this is registered" is true exactly once and the test used to
// fail on its own second execution. What the disabled path must guarantee — it
// registers nothing NEW — holds on every run, and on the first one it is
// literally the absence assertion.
func TestAPIServerProbeMetricsArePublishedOnlyWhenItRuns(t *testing.T) {
	const (
		gauge    = "kubescrape_apiserver_reachable"
		failures = "kubescrape_apiserver_probe_failures_total"
	)
	names := func() []string {
		var out []string
		for _, s := range obs.Registry.Dump() {
			out = append(out, s.Name)
		}
		slices.Sort(out)
		return out
	}
	registered := func(name string) bool { return slices.Contains(names(), name) }

	before := names()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // nothing must actually run
	if w := startAPIServerWatchdog(ctx, func(context.Context) error { return nil }, 0, slog.Default()); w != nil {
		t.Fatal("startAPIServerWatchdog returned a watchdog for interval 0")
	}
	if after := names(); !slices.Equal(before, after) {
		t.Fatalf("-apiserver-probe-interval=0 registered %v; an absent family is what says nobody is looking",
			added(before, after))
	}

	if w := startAPIServerWatchdog(ctx, func(context.Context) error { return nil }, time.Hour, slog.Default()); w == nil {
		t.Fatal("startAPIServerWatchdog returned nil for a positive interval")
	}
	if !registered(gauge) || !registered(failures) {
		t.Fatal("a running probe did not publish its metrics")
	}
}

// added lists the names present in after but not in before (both sorted).
func added(before, after []string) []string {
	var out []string
	for _, name := range after {
		if !slices.Contains(before, name) {
			out = append(out, name)
		}
	}
	return out
}
