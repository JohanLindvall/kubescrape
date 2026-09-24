package events

// What the operator can SEE when the events pipeline degrades. Every case here
// is a path that used to move a counter (or nothing at all) with no line
// carrying the object, the error or the remedy.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// capturedLog is a logger writing into a buffer, at Debug so both halves of a
// counter+context pair are visible to a test.
func capturedLog() (*slog.Logger, func() string) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(h), buf.String
}

// failingMeta is a metadata source that cannot answer.
type failingMeta struct{ err error }

func (f failingMeta) PodByName(context.Context, string, string) (*kubemeta.Pod, error) {
	return nil, f.err
}

// The whole reason events are collected here rather than as a flat stream is
// that an event about a pod lands on that pod's resource attributes. When that
// resolution fails the event still exports — so nothing else can report it.
func TestUnresolvedInvolvedPodIsCountedAndLogged(t *testing.T) {
	for _, tc := range []struct {
		name   string
		meta   MetadataSource
		reason string
	}{
		{"lookup", failingMeta{err: errors.New("metadata service unreachable")}, reasonLookup},
		// A pod of that name exists but is a different incarnation: a
		// SUCCESSFUL lookup, so no request counter can show it either.
		{"uid mismatch", fakeMeta{pod: &kubemeta.Pod{UID: "someone-else", Name: "web-abc"}}, reasonUIDMismatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log, dump := capturedLog()
			before := obs.EventsUnresolved.WithLabelValues(tc.reason).Value()
			r, _, _ := newReader(t, Config{Meta: tc.meta, Logger: log, BatchSize: 100})
			ctx := context.Background()
			r.ingest(ctx, event("a", "OOMKilled", "boom", "Warning", "10", 1, time.Now()))

			if got := obs.EventsUnresolved.WithLabelValues(tc.reason).Value(); got <= before {
				t.Errorf("kubescrape_events_unresolved_total{reason=%q} did not move (%v -> %v)", tc.reason, before, got)
			}
			out := dump()
			if !strings.Contains(out, "reason="+tc.reason) || !strings.Contains(out, "pod=web-abc") {
				t.Errorf("no line naming the object and the reason:\n%s", out)
			}
			// The condition is a state, so it is warned once and then throttled
			// — but it must be warned at least once, or the counter is the only
			// evidence and it cannot say which pod.
			if !strings.Contains(out, "level=WARN") {
				t.Errorf("the condition was never warned about:\n%s", out)
			}
		})
	}
}

// hangingMeta is a metadata service that accepted the connection and never
// answers — partitioned, blackholed — until the caller's context gives up (or,
// so an unbounded caller still finishes, `fallback` elapses). It counts calls.
type hangingMeta struct {
	calls    int
	fallback time.Duration
}

func (m *hangingMeta) PodByName(ctx context.Context, _, _ string) (*kubemeta.Pod, error) {
	m.calls++
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(m.fallback):
		return nil, errors.New("hung metadata service finally answered")
	}
}

// A metadata service that HANGS must cost attribution, never the stream. The
// lookup runs on the reader's only goroutine, and bounded only by the shared
// client's ~15s timeout every distinct involved pod cost that much: the watch
// fell to a few events a minute, and a relist lap's first page outlived its
// continue token and exported nothing. One lookup that runs out of its own
// bound pauses the rest, and the events export under the identity they carry.
func TestHangingMetadataServiceCostsAttributionNotTheStream(t *testing.T) {
	log, dump := capturedLog()
	meta := &hangingMeta{fallback: 300 * time.Millisecond}
	r, _, _ := newReader(t, Config{Meta: meta, Logger: log, BatchSize: 1000})
	r.lookupTimeout = 20 * time.Millisecond
	now := time.Now()
	r.now = func() time.Time { return now }

	before := obs.EventsUnresolved.WithLabelValues(reasonLookup).Value()
	const pods = 10
	for i := range pods {
		e := event(fmt.Sprintf("e%d", i), "BackOff", "m", "Warning", strconv.Itoa(10+i), 1, now)
		e.InvolvedObject.Name = fmt.Sprintf("web-%d", i)
		e.InvolvedObject.UID = types.UID(fmt.Sprintf("pod-uid-%d", i))
		r.ingest(context.Background(), e)
	}
	if meta.calls != 1 {
		t.Fatalf("issued %d lookups for %d distinct pods against a hung metadata service, want 1: "+
			"the first that runs out of its bound must pause the rest", meta.calls, pods)
	}
	if len(r.batch) != pods {
		t.Fatalf("batched %d of %d events; an unresolvable pod must still export", len(r.batch), pods)
	}
	if got := obs.EventsUnresolved.WithLabelValues(reasonLookup).Value() - before; got != pods {
		t.Errorf("kubescrape_events_unresolved_total{reason=lookup} delta = %v, want %d: every event the "+
			"pause skipped lost its pod identity and must be counted", got, pods)
	}
	for _, e := range r.batch {
		if v, ok := e.res.Attributes().Get("k8s.pod.name"); !ok || !strings.HasPrefix(v.Str(), "web-") {
			t.Fatalf("an unresolved event lost even the identity it carries: %v", e.res.Attributes().AsRaw())
		}
	}
	if out := dump(); !strings.Contains(out, "pausing pod lookups") {
		t.Errorf("the pause was not announced:\n%s", out)
	}

	// The pause ENDS: past it, the next distinct pod is looked up again.
	now = now.Add(podLookupPause + time.Second)
	e := event("late", "BackOff", "m", "Warning", "99", 1, now)
	e.InvolvedObject.Name, e.InvolvedObject.UID = "web-late", "pod-uid-late"
	r.ingest(context.Background(), e)
	if meta.calls != 2 {
		t.Fatalf("lookups = %d after the pause expired, want 2", meta.calls)
	}
}

// Only a lookup that HUNG pauses the rest. A refused connection — the service's
// pod down, no endpoints — fails in microseconds and costs nothing to repeat,
// and pausing on it would discard attribution the moment the service is back.
func TestFastFailingLookupDoesNotPauseLookups(t *testing.T) {
	meta := &countingFailMeta{}
	r, _, _ := newReader(t, Config{Meta: meta, BatchSize: 1000})
	for i := range 3 {
		e := event(fmt.Sprintf("e%d", i), "BackOff", "m", "Warning", strconv.Itoa(10+i), 1, time.Now())
		e.InvolvedObject.Name = fmt.Sprintf("web-%d", i)
		e.InvolvedObject.UID = types.UID(fmt.Sprintf("pod-uid-%d", i))
		r.ingest(context.Background(), e)
	}
	if meta.calls != 3 {
		t.Fatalf("lookups = %d for 3 distinct pods against a REFUSING service, want 3 (no pause)", meta.calls)
	}
}

// switchMeta resolves pod while up and refuses while down, counting lookups.
type switchMeta struct {
	pod   *kubemeta.Pod
	down  bool
	calls int
}

func (m *switchMeta) PodByName(context.Context, string, string) (*kubemeta.Pod, error) {
	m.calls++
	if m.down {
		return nil, errors.New("connection refused")
	}
	return m.pod, nil
}

// A FAILED pod resolution is memoized only briefly. The memo lives as long as
// the batch, and a collector outage settles nothing — so a lookup that failed
// once (the metadata service down at the same time) went on answering
// "unresolved" for every event about that pod for the rest of the outage,
// including the ones ingested long after the service came back: they exported
// without k8s.pod.uid, node or owner labels.
func TestFailedPodResolutionIsRetriedOnceTheMetadataServiceIsBack(t *testing.T) {
	meta := &switchMeta{pod: &kubemeta.Pod{Name: "web-abc", Namespace: "default", UID: "pod-uid"}, down: true}
	exp := &captureExporter{failN: 1, err: errors.New("collector down")}
	r, _, _ := newReader(t, Config{Meta: meta, Exporter: exp, BatchSize: 1000})
	now := time.Unix(1_700_000_000, 0)
	r.now = func() time.Time { return now }
	ctx := context.Background()
	handle := func(name, rv string) {
		t.Helper()
		if err := r.handle(ctx, watch.Event{Type: watch.Added, Object: event(name, "BackOff", "m", "Warning", rv, 1, now)}); err != nil {
			t.Fatal(err)
		}
	}

	handle("e1", "10") // metadata down: exported under the identity it carries
	if err := r.flush(ctx); err == nil {
		t.Fatal("precondition: the collector is down, the flush must fail")
	}
	handle("e2", "11") // still inside the window: answered from the memo
	if meta.calls != 1 {
		t.Fatalf("lookups = %d inside the retry window, want 1: a failure is still memoized briefly", meta.calls)
	}

	// The metadata service comes back while the collector is still down.
	meta.down = false
	now = now.Add(unresolvedRetryAfter)
	handle("e3", "12")
	handle("e4", "13")
	if meta.calls != 2 {
		t.Fatalf("lookups = %d once the window passed, want 2 (one retry, then memoized for the batch)", meta.calls)
	}

	// The collector recovers: the retained payload ([e1]) ships, then the tail.
	for len(r.batch) > 0 {
		if err := r.flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	resolved := map[string]bool{}
	for _, ld := range exp.sent {
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			rl := ld.ResourceLogs().At(i)
			_, hasUID := rl.Resource().Attributes().Get("k8s.pod.uid")
			lrs := rl.ScopeLogs().At(0).LogRecords()
			for j := 0; j < lrs.Len(); j++ {
				name, _ := lrs.At(j).Attributes().Get("k8s.event.name")
				resolved[name.Str()] = hasUID
			}
		}
	}
	want := map[string]bool{"e1": false, "e2": false, "e3": true, "e4": true}
	for name, w := range want {
		if got, ok := resolved[name]; !ok || got != w {
			t.Errorf("%s exported with the pod's resolved identity = %v (present %v), want %v — e2 shares a "+
				"render with e3/e4, so a shared group would also hand them e2's name-only resource", name, got, ok, w)
		}
	}
}

// ctxMeta resolves pod unless the caller's context is done, as the real client
// does for any lookup its cache cannot answer.
type ctxMeta struct{ pod *kubemeta.Pod }

func (m ctxMeta) PodByName(ctx context.Context, _, _ string) (*kubemeta.Pod, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return m.pod, nil
}

// A shutdown or lost lease that lands mid-way through a backlog page is not a
// metadata failure. The walk checks its context once per PAGE, so the rest of
// the page (up to replayPageSize items) used to be ingested under a dead
// context: every uncached lookup failed with context.Canceled, was counted as
// kubescrape_events_unresolved_total{reason="lookup"} with a Warn, was memoized,
// and exported by the final flush under the name-only identity — while the
// successor, re-listing the unsecured replay, shipped the same events again
// resolved.
func TestShutdownMidReplayIsNotAMetadataFailure(t *testing.T) {
	log, dump := capturedLog()
	meta := ctxMeta{pod: &kubemeta.Pod{Name: "web-abc", Namespace: "default", UID: "pod-uid"}}
	r, _, _ := newReader(t, Config{Meta: meta, Logger: log, BatchSize: 1000})
	r.replaying, r.replaySecured = true, false
	before := obs.EventsUnresolved.WithLabelValues(reasonLookup).Value()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	kept, err := r.replayItem(ctx, event("a", "BackOff", "m", "Warning", "10", 1, time.Now()))
	if !errors.Is(err, context.Canceled) || kept {
		t.Fatalf("replayItem under a cancelled context = (%v, %v), want (false, context.Canceled): the walk must stop", kept, err)
	}
	if len(r.batch) != 0 || r.replayOwed != 0 {
		t.Fatalf("batch = %d, owed = %d: nothing may be ingested once the walk's context is done", len(r.batch), r.replayOwed)
	}

	// The same cancelled lookup reached from the watch path (its last in-flight
	// event) exports, but is neither reported nor memoized.
	r.ingest(ctx, event("b", "BackOff", "m", "Warning", "11", 1, time.Now()))
	if got := obs.EventsUnresolved.WithLabelValues(reasonLookup).Value() - before; got != 0 {
		t.Errorf("kubescrape_events_unresolved_total{reason=lookup} moved by %v for a lookup our own shutdown cancelled", got)
	}
	if len(r.resCache) != 0 {
		t.Errorf("a lookup cut short by shutdown was memoized: %d entries", len(r.resCache))
	}
	if out := dump(); strings.Contains(out, "did not resolve") || strings.Contains(out, "without the pod's resolved identity") {
		t.Errorf("a shutdown was reported as a metadata failure:\n%s", out)
	}
}

// countingFailMeta refuses every lookup at once, counting them.
type countingFailMeta struct{ calls int }

func (m *countingFailMeta) PodByName(context.Context, string, string) (*kubemeta.Pod, error) {
	m.calls++
	return nil, errors.New("connection refused")
}

// A resolvable pod logs nothing: this pipeline's Debug must not become a line
// per event on a healthy cluster.
func TestResolvedInvolvedPodIsSilent(t *testing.T) {
	log, dump := capturedLog()
	r, _, _ := newReader(t, Config{
		Meta:   fakeMeta{pod: &kubemeta.Pod{UID: "pod-uid", Name: "web-abc", Namespace: "default"}},
		Logger: log, BatchSize: 100,
	})
	r.ingest(context.Background(), event("a", "Started", "ok", "Normal", "10", 1, time.Now()))
	if out := dump(); out != "" {
		t.Errorf("a resolved event logged something:\n%s", out)
	}
}

// failingStore fails every Save; Load reports a cold start.
type failingStore struct{ fail bool }

func (s *failingStore) Load(context.Context) (Position, bool, error) { return Position{}, false, nil }

func (s *failingStore) Save(context.Context, Position) error {
	if s.fail {
		return errors.New("configmaps is forbidden")
	}
	return nil
}

// The position is the one piece of state that outlives the process, and an
// unwritable one is silent loss of the RESUME POINT: the pipeline keeps
// exporting and a handover then replays (or, past the watch window, discards).
// One Warn on the transition, one Info on the recovery, and a Debug per write.
func TestPositionWriteFailureWarnsOnceAndRecovers(t *testing.T) {
	log, dump := capturedLog()
	store := &failingStore{fail: true}
	r, _, _ := newReader(t, Config{Positions: store, Logger: log, PersistInterval: time.Nanosecond})
	r.committed.ResourceVersion = "42"
	ctx := context.Background()

	r.persist(ctx, true)
	r.persist(ctx, true) // the repeat must be throttled, not a second line
	out := dump()
	if n := strings.Count(out, "writing the event position failed"); n != 1 {
		t.Errorf("want exactly one transition warning, got %d:\n%s", n, out)
	}
	if !strings.Contains(out, "resourceVersion=42") {
		t.Errorf("the warning does not say where the position stood:\n%s", out)
	}

	store.fail = false
	r.persist(ctx, true)
	out = dump()
	if !strings.Contains(out, "writing the event position recovered") {
		t.Errorf("the recovery is silent, which reads the same as a still-broken write:\n%s", out)
	}
	if !strings.Contains(out, "event position written") {
		t.Errorf("a successful write leaves no Debug trace:\n%s", out)
	}
}

// An export outage warns on its FIRST failure, stays quiet while it persists
// inside flushWarnEvery, re-warns once that has elapsed and says so when it
// recovers — and the NEXT outage warns at once, even inside the interval the
// throttle last claimed: the transition speaks whatever the throttle says.
// Driven on r.now, so the cadence is pinned without a sleep.
func TestExportFailureWarnsOnTheTransitionAndThrottlesTheRest(t *testing.T) {
	log, dump := capturedLog()
	exp := &captureExporter{failN: 1 << 30, err: context.DeadlineExceeded}
	r, _, _ := newReader(t, Config{Exporter: exp, Logger: log, BatchSize: 100})
	now := time.Now()
	r.now = func() time.Time { return now }
	ctx := context.Background()
	r.ingest(ctx, event("a", "R", "m", "Normal", "10", 1, now))

	const failed = "event export failed"
	fail := func() {
		t.Helper()
		if r.tryFlush(ctx) {
			t.Fatal("a flush landed against a failing collector")
		}
	}
	warnings := func(want int, when string) {
		t.Helper()
		if n := strings.Count(dump(), failed); n != want {
			t.Fatalf("%s: %d export-failure warnings, want %d:\n%s", when, n, want, dump())
		}
	}

	fail()
	fail()
	now = now.Add(flushWarnEvery / 2)
	fail()
	warnings(1, "three failures inside one interval")
	now = now.Add(flushWarnEvery / 2)
	fail()
	warnings(2, "a failure once the interval elapsed")

	exp.mu.Lock()
	exp.failN = 0
	exp.mu.Unlock()
	if !r.tryFlush(ctx) {
		t.Fatal("the flush did not land once the collector recovered")
	}
	if !strings.Contains(dump(), "event export recovered") {
		t.Fatalf("the recovery is silent:\n%s", dump())
	}
	// The recovery sizes the outage the way journald's and the tailer's do:
	// four failed attempts, the first a minute before the one that landed.
	if out := dump(); !strings.Contains(out, "failures=4") || !strings.Contains(out, "outage=1m0s") {
		t.Fatalf("the recovery line does not say how long the collector refused or how often:\n%s", out)
	}

	exp.mu.Lock()
	exp.failN = 1 << 30
	exp.mu.Unlock()
	r.ingest(ctx, event("b", "R", "m", "Normal", "11", 1, now))
	now = now.Add(time.Second)
	fail()
	warnings(3, "the first failure of a new outage, a second after the last re-warn")
}

// The same transition shape for the position write: the first failure of each
// outage warns, the rest re-warn at most every positionWarnEvery.
func TestPositionWriteFailureRewarnsOnlyOnceTheIntervalElapses(t *testing.T) {
	log, dump := capturedLog()
	store := &failingStore{fail: true}
	r, _, _ := newReader(t, Config{Positions: store, Logger: log})
	now := time.Now()
	r.now = func() time.Time { return now }
	r.committed.ResourceVersion = "42"
	ctx := context.Background()

	const failed = "writing the event position failed"
	warnings := func(want int, when string) {
		t.Helper()
		if n := strings.Count(dump(), failed); n != want {
			t.Fatalf("%s: %d position-write warnings, want %d:\n%s", when, n, want, dump())
		}
	}
	r.persist(ctx, true)
	now = now.Add(positionWarnEvery - time.Second)
	r.persist(ctx, true)
	warnings(1, "two failures inside one interval")
	now = now.Add(time.Second)
	r.persist(ctx, true)
	warnings(2, "a failure once the interval elapsed")

	store.fail = false
	r.persist(ctx, true)
	if out := dump(); !strings.Contains(out, "writing the event position recovered") ||
		!strings.Contains(out, "failures=3") || !strings.Contains(out, "outage=5m0s") {
		t.Fatalf("the recovery line does not size the outage (3 failed writes over 5m):\n%s", out)
	}
	store.fail = true
	now = now.Add(time.Second)
	r.persist(ctx, true)
	warnings(3, "the first failure after a recovery")
}

// The overflow warning used to latch for the process' life, so a singleton
// running for weeks reported only its FIRST outage. It re-arms on the flush
// that recovers.
func TestOverflowWarningReArmsAfterRecovery(t *testing.T) {
	r, _, _ := newReader(t, Config{BatchSize: 1})
	r.overflowWarned = true
	failedAt := time.Now().Add(-time.Minute)
	r.flushFailedAt = failedAt
	r.exportOutage.Fail(failedAt, flushWarnEvery)
	r.tryFlush(context.Background()) // an empty batch flushes cleanly
	if r.overflowWarned {
		t.Error("the overflow warning stayed latched after the export recovered")
	}
}

// A watch that decodes into something other than an Event exports nothing while
// looking perfectly healthy. Throttled, because it would be one line per
// delivery.
func TestNonEventObjectIsWarnedAndThrottled(t *testing.T) {
	log, dump := capturedLog()
	r, _, _ := newReader(t, Config{Logger: log, BatchSize: 100})
	ctx := context.Background()
	odd := watch.Event{Type: watch.Added, Object: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "not-an-event"}}}
	for range 3 {
		if err := r.handle(ctx, odd); err != nil {
			t.Fatal(err)
		}
	}
	out := dump()
	if n := strings.Count(out, "is not a core/v1 Event"); n != 1 {
		t.Errorf("want exactly one (throttled) warning, got %d:\n%s", n, out)
	}
	if !strings.Contains(out, "type=*v1.Pod") {
		t.Errorf("the warning does not name what arrived:\n%s", out)
	}
}

// uidEvent is `event` with a chosen involved-object UID.
func uidEvent(uid types.UID) *corev1.Event {
	e := event("a", "Started", "ok", "Normal", "10", 1, time.Now())
	e.InvolvedObject.UID = uid
	return e
}

// An event whose involved object carries NO uid cannot be mismatched, so it
// must resolve rather than be counted unresolved — the cross-check is a guard
// against adopting the wrong pod, not a requirement that every event carry a
// uid.
func TestInvolvedObjectWithoutUIDStillResolves(t *testing.T) {
	log, dump := capturedLog()
	r, _, _ := newReader(t, Config{
		Meta:   fakeMeta{pod: &kubemeta.Pod{UID: "pod-uid", Name: "web-abc", Namespace: "default"}},
		Logger: log, BatchSize: 100,
	})
	r.ingest(context.Background(), uidEvent(""))
	if out := dump(); strings.Contains(out, "did not resolve") {
		t.Errorf("a uid-less involved object was reported unresolved:\n%s", out)
	}
}
