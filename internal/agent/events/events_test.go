package events

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/leader"
	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// captureExporter records exported payloads; failN fails the first N sends.
// calls counts every attempt, failed ones included.
type captureExporter struct {
	mu    sync.Mutex
	sent  []plog.Logs
	calls int
	failN int
	err   error
}

func (c *captureExporter) attempts() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (c *captureExporter) ExportLogs(_ context.Context, ld plog.Logs) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.failN > 0 {
		c.failN--
		return c.err
	}
	out := plog.NewLogs()
	ld.CopyTo(out)
	c.sent = append(c.sent, out)
	return nil
}

func (c *captureExporter) records() []plog.LogRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []plog.LogRecord
	for _, ld := range c.sent {
		rls := ld.ResourceLogs()
		for i := 0; i < rls.Len(); i++ {
			sls := rls.At(i).ScopeLogs()
			for j := 0; j < sls.Len(); j++ {
				lrs := sls.At(j).LogRecords()
				for k := 0; k < lrs.Len(); k++ {
					out = append(out, lrs.At(k))
				}
			}
		}
	}
	return out
}

// fakeMeta resolves one pod.
type fakeMeta struct{ pod *kubemeta.Pod }

func (f fakeMeta) PodByName(context.Context, string, string) (*kubemeta.Pod, error) {
	if f.pod == nil {
		return nil, context.Canceled
	}
	return f.pod, nil
}

// countingMeta is fakeMeta with a lookup counter; the count is also the number
// of resources built, since the resolve is the first thing a build does.
type countingMeta struct {
	pod   *kubemeta.Pod
	calls int
}

func (m *countingMeta) PodByName(context.Context, string, string) (*kubemeta.Pod, error) {
	m.calls++
	return m.pod, nil
}

func event(name, reason, msg, typ string, rv string, count int32, when time.Time) *corev1.Event {
	return &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "default", ResourceVersion: rv, UID: types.UID("e-" + name),
		},
		InvolvedObject: corev1.ObjectReference{
			Kind: "Pod", Name: "web-abc", Namespace: "default", UID: "pod-uid",
		},
		Reason: reason, Message: msg, Type: typ, Count: count,
		LastTimestamp: metav1.NewTime(when),
		Source:        corev1.EventSource{Component: "kubelet"},
	}
}

// newReader wires a Reader against a fake clientset with an injectable watch.
func newReader(t *testing.T, cfg Config) (*Reader, *fake.Clientset, *watch.FakeWatcher) {
	t.Helper()
	client := fake.NewSimpleClientset()
	w := watch.NewFake()
	client.PrependWatchReactor("events", k8stesting.DefaultWatchReactor(w, nil))
	cfg.Client = client
	if cfg.Exporter == nil {
		cfg.Exporter = &captureExporter{}
	}
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = 10 * time.Millisecond
	}
	return New(cfg), client, w
}

// A Modified event is a NEW occurrence: Kubernetes aggregates repeats into
// one object with a growing count, so handling only Added would lose
// "BackOff x47" — the most diagnostically valuable event there is.
func TestModifiedIsAnOccurrence(t *testing.T) {
	exp := &captureExporter{}
	r, _, _ := newReader(t, Config{Exporter: exp, BatchSize: 1})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	now := time.Now()
	if err := r.handle(ctx, watch.Event{Type: watch.Added, Object: event("a", "BackOff", "back-off", "Warning", "10", 1, now)}); err != nil {
		t.Fatal(err)
	}
	if err := r.handle(ctx, watch.Event{Type: watch.Modified, Object: event("a", "BackOff", "back-off", "Warning", "11", 47, now)}); err != nil {
		t.Fatal(err)
	}
	// Deleted is TTL expiry, not an occurrence.
	if err := r.handle(ctx, watch.Event{Type: watch.Deleted, Object: event("a", "BackOff", "back-off", "Warning", "12", 47, now)}); err != nil {
		t.Fatal(err)
	}
	recs := exp.records()
	if len(recs) != 2 {
		t.Fatalf("records = %d, want 2 (Added + Modified, never Deleted)", len(recs))
	}
	if v, ok := recs[1].Attributes().Get("k8s.event.count"); !ok || v.Int() != 47 {
		t.Fatalf("the repeat must carry its count, got %v", v.AsString())
	}
	if recs[0].SeverityNumber() != plog.SeverityNumberWarn {
		t.Fatalf("Warning must map to WARN, got %v", recs[0].SeverityNumber())
	}
	if r.committed.ResourceVersion != "12" && r.committed.ResourceVersion != "11" {
		t.Fatalf("position did not advance: %q", r.committed.ResourceVersion)
	}
}

// A bookmark carries only a position. Applying it while a batch is pending
// would put the committed position AHEAD of unexported records — a gap on the
// next resume. It must wait for the flush that covers them.
func TestBookmarkNeverOvertakesUnexportedRecords(t *testing.T) {
	exp := &captureExporter{}
	r, _, _ := newReader(t, Config{Exporter: exp, BatchSize: 1000}) // never auto-flushes
	ctx := context.Background()
	now := time.Now()

	if err := r.handle(ctx, watch.Event{Type: watch.Added, Object: event("a", "Started", "m", "Normal", "10", 1, now)}); err != nil {
		t.Fatal(err)
	}
	bookmark := &corev1.Event{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "99"}}
	if err := r.handle(ctx, watch.Event{Type: watch.Bookmark, Object: bookmark}); err != nil {
		t.Fatal(err)
	}
	if r.committed.ResourceVersion == "99" {
		t.Fatal("bookmark advanced the position past an unexported record")
	}
	if err := r.flush(ctx); err != nil {
		t.Fatal(err)
	}
	if r.committed.ResourceVersion != "99" {
		t.Fatalf("after the flush the bookmark must apply, got %q", r.committed.ResourceVersion)
	}

	// With nothing buffered, a bookmark applies immediately — that is what
	// keeps an idle cluster's position inside the API server's watch window.
	bookmark2 := &corev1.Event{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "120"}}
	if err := r.handle(ctx, watch.Event{Type: watch.Bookmark, Object: bookmark2}); err != nil {
		t.Fatal(err)
	}
	if r.committed.ResourceVersion != "120" {
		t.Fatalf("idle bookmark not applied, got %q", r.committed.ResourceVersion)
	}
}

// A failed export must NOT advance the position: the batch is retried, and a
// process death replays from where the collector last acknowledged.
func TestPositionOnlyAdvancesAfterAck(t *testing.T) {
	exp := &captureExporter{failN: 1, err: context.DeadlineExceeded}
	r, _, _ := newReader(t, Config{Exporter: exp, BatchSize: 1000})
	ctx := context.Background()

	if err := r.handle(ctx, watch.Event{Type: watch.Added, Object: event("a", "Started", "m", "Normal", "10", 1, time.Now())}); err != nil {
		t.Fatal(err)
	}
	if err := r.flush(ctx); err == nil {
		t.Fatal("a transient export failure must surface")
	}
	if r.committed.ResourceVersion != "" {
		t.Fatalf("position advanced past an unacknowledged batch: %q", r.committed.ResourceVersion)
	}
	if len(r.batch) != 1 {
		t.Fatalf("batch must be retained for the retry, got %d", len(r.batch))
	}
	if err := r.flush(ctx); err != nil {
		t.Fatal(err)
	}
	if r.committed.ResourceVersion != "10" {
		t.Fatalf("position after ack = %q, want 10", r.committed.ResourceVersion)
	}
}

// After a relist the whole TTL window arrives again; the watermark drops what
// was already exported, biased toward re-emitting (at-least-once).
func TestWatermarkFiltersReplay(t *testing.T) {
	r, _, _ := newReader(t, Config{})
	base := time.Now()
	r.committed.Watermark = base
	r.replaying = true  // the state a relist establishes (stream started from "")
	r.replayFrom = base // frozen by stream() from the committed watermark

	if r.wanted(event("old", "R", "m", "Normal", "1", 1, base.Add(-time.Minute))) {
		t.Fatal("an event older than the watermark must be filtered")
	}
	if !r.wanted(event("same", "R", "m", "Normal", "2", 1, base)) {
		t.Fatal("an event exactly at the watermark must be kept (bias to duplicates)")
	}
	if !r.wanted(event("new", "R", "m", "Normal", "3", 1, base.Add(time.Minute))) {
		t.Fatal("a newer event must be kept")
	}
}

// The involved pod's identity becomes the RESOURCE, so events correlate with
// that pod's logs and metrics; an unresolvable pod still correlates by name.
func TestResourceCarriesInvolvedPodIdentity(t *testing.T) {
	pod := &kubemeta.Pod{
		Name: "web-abc", Namespace: "default", UID: "pod-uid", NodeName: "node1",
		Owners: []kubemeta.Owner{{Kind: "Deployment", Name: "web"}},
	}
	exp := &captureExporter{}
	r, _, _ := newReader(t, Config{Exporter: exp, BatchSize: 1, Meta: fakeMeta{pod: pod}})
	if err := r.handle(context.Background(), watch.Event{
		Type: watch.Added, Object: event("a", "Killing", "Stopping container", "Normal", "10", 1, time.Now()),
	}); err != nil {
		t.Fatal(err)
	}
	rl := exp.sent[0].ResourceLogs().At(0).Resource().Attributes()
	for k, want := range map[string]string{
		"k8s.pod.name": "web-abc", "k8s.namespace.name": "default",
		"k8s.deployment.name": "web", "service.name": "web",
	} {
		v, ok := rl.Get(k)
		if !ok || v.Str() != want {
			t.Errorf("resource[%s] = %q (present=%v), want %q", k, v.AsString(), ok, want)
		}
	}
	// The singleton's OWN node must never leak onto records about other nodes.
	if _, ok := rl.Get("k8s.node.name"); ok {
		if v, _ := rl.Get("k8s.node.name"); v.Str() != "node1" {
			t.Errorf("k8s.node.name = %q, want the involved pod's node", v.Str())
		}
	}
}

// A UID mismatch means a recreated pod of the same name: it must not lend its
// identity to an event about its predecessor.
func TestStalePodIdentityRejected(t *testing.T) {
	pod := &kubemeta.Pod{Name: "web-abc", Namespace: "default", UID: "different-uid"}
	exp := &captureExporter{}
	r, _, _ := newReader(t, Config{Exporter: exp, BatchSize: 1, Meta: fakeMeta{pod: pod}})
	if err := r.handle(context.Background(), watch.Event{
		Type: watch.Added, Object: event("a", "R", "m", "Normal", "10", 1, time.Now()),
	}); err != nil {
		t.Fatal(err)
	}
	a := exp.sent[0].ResourceLogs().At(0).Resource().Attributes()
	if v, ok := a.Get("k8s.pod.uid"); ok && v.Str() == "different-uid" {
		t.Fatal("a recreated pod's identity was applied to its predecessor's event")
	}
	if v, ok := a.Get("k8s.pod.name"); !ok || v.Str() != "web-abc" {
		t.Fatal("an unresolved pod must still correlate by name")
	}
}

// logs.rules apply to events exactly as they do to container logs and journal
// entries, including the synthetic __severity__ key.
func TestRulesDropEvents(t *testing.T) {
	rules, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "drop", Match: []string{"__severity__=info"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	exp := &captureExporter{}
	r, _, _ := newReader(t, Config{Exporter: exp, BatchSize: 2, Rules: rules})
	ctx := context.Background()
	now := time.Now()
	if err := r.handle(ctx, watch.Event{Type: watch.Added, Object: event("n", "Started", "normal", "Normal", "10", 1, now)}); err != nil {
		t.Fatal(err)
	}
	if err := r.handle(ctx, watch.Event{Type: watch.Added, Object: event("w", "Failed", "warning", "Warning", "11", 1, now)}); err != nil {
		t.Fatal(err)
	}
	recs := exp.records()
	if len(recs) != 1 || recs[0].Body().Str() != "warning" {
		t.Fatalf("records = %d, want only the Warning kept", len(recs))
	}
	// Dropped records still advance the position — the batch is settled.
	if r.committed.ResourceVersion != "11" {
		t.Fatalf("position = %q, want 11 (a dropped record still advances)", r.committed.ResourceVersion)
	}
}

// An expired resourceVersion clears the resume point so the next stream
// relists, while keeping the watermark that filters the replay.
func TestExpiryClearsResourceVersionButKeepsWatermark(t *testing.T) {
	r, _, _ := newReader(t, Config{})
	mark := time.Now()
	r.committed = Position{ResourceVersion: "10", Watermark: mark}
	r.expire("watch")
	if r.committed.ResourceVersion != "" {
		t.Fatal("an expired position must be cleared so the next stream relists")
	}
	if !r.committed.Watermark.Equal(mark) {
		t.Fatal("the watermark must survive to filter the replay")
	}
}

func TestValidateStartMode(t *testing.T) {
	for _, ok := range []string{"", "auto", "end", "start"} {
		if err := ValidateStartMode(ok); err != nil {
			t.Errorf("%q: %v", ok, err)
		}
	}
	if err := ValidateStartMode("middle"); err == nil || !strings.Contains(err.Error(), "invalid") {
		t.Fatalf("unknown mode must be rejected, got %v", err)
	}
}

// A cold start in "end" mode takes a resourceVersion without ingesting the
// backlog; "start" replays whatever the API server still holds.
func TestColdStartModes(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("list", "events", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.EventList{ListMeta: metav1.ListMeta{ResourceVersion: "424242"}}, nil
	})
	r := New(Config{Client: client, StartMode: StartEnd})
	rv, redelivers, err := r.startResourceVersion(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rv != "424242" || redelivers {
		t.Fatalf("end mode must start at the current revision without redelivery, got %q/%v", rv, redelivers)
	}

	r = New(Config{Client: client, StartMode: StartBeginning})
	if rv, redelivers, err = r.startResourceVersion(context.Background()); err != nil || rv != "" || !redelivers {
		t.Fatalf("start mode must replay from the oldest available (rv=%q, redelivers=%v, err=%v)", rv, redelivers, err)
	}

	// A stored position always wins over the start mode.
	r = New(Config{Client: client, StartMode: StartEnd})
	r.committed.ResourceVersion = "77"
	if rv, redelivers, err = r.startResourceVersion(context.Background()); err != nil || rv != "77" || !redelivers {
		t.Fatalf("a stored position must win and redeliver (rv=%q, redelivers=%v, err=%v)", rv, redelivers, err)
	}
}

// A 410 Gone must REPLAY the watch window, not skip to the current revision:
// the whole point of the watermark is that a relist can re-receive what the
// expired watch missed. Taking the cold-start policy there would drop every
// event between the expired version and now.
func TestExpiryRelistsRatherThanSkippingAhead(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("list", "events", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.EventList{ListMeta: metav1.ListMeta{ResourceVersion: "999"}}, nil
	})
	r := New(Config{Client: client, StartMode: StartEnd})
	r.committed.ResourceVersion = "77"
	r.committed.Watermark = time.Now().Add(-time.Minute) // something was exported

	r.expire("watch")
	rv, _, err := r.startResourceVersion(context.Background())
	if err != nil || rv != "" {
		t.Fatalf("after Gone the reader must relist from the oldest available (rv=%q, err=%v)", rv, err)
	}
	// The relist SURVIVES a failed attempt: a watch that dies before anything
	// commits must relist again, or the gap is lost after all.
	if rv, _, err = r.startResourceVersion(context.Background()); err != nil || rv != "" {
		t.Fatalf("an uncommitted relist must persist (rv=%q, err=%v)", rv, err)
	}
	// A commit secures the replay and disarms it.
	r.settle(entry{rv: "1001", when: time.Now()}, len(r.batch))
	if rv, _, err = r.startResourceVersion(context.Background()); err != nil || rv != "1001" {
		t.Fatalf("after a commit the reader resumes from it (rv=%q, err=%v)", rv, err)
	}
	if r.relist {
		t.Fatal("a commit must disarm the relist")
	}

	// Nothing exported yet: with a zero watermark a replay is unfiltered, so
	// -events-start=end must not turn its first expiry into the full backlog.
	r = New(Config{Client: client, StartMode: StartEnd})
	r.committed.ResourceVersion = "77"
	r.expire("watch")
	if rv, _, err = r.startResourceVersion(context.Background()); err != nil || rv != "999" {
		t.Fatalf("a watermark-less expiry must honour the start mode (rv=%q, err=%v)", rv, err)
	}
}

// A restarted stream that resumes from the committed position re-delivers
// everything already buffered — retaining the batch would export the whole
// backlog once per restart and grow memory without bound while the collector
// is down. Only the cold skip-the-backlog start, whose revision is past the
// buffered entries, may keep them.
func TestRestartDropsRedeliveredBatch(t *testing.T) {
	newStopped := func() *fake.Clientset {
		client := fake.NewSimpleClientset()
		client.PrependReactor("list", "events", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, &corev1.EventList{ListMeta: metav1.ListMeta{ResourceVersion: "500"}}, nil
		})
		w := watch.NewFake()
		w.Stop() // the stream sees a closed channel and returns right away
		client.PrependWatchReactor("events", k8stesting.DefaultWatchReactor(w, nil))
		return client
	}

	// Redelivering restart (a committed position): batch and stale bookmark go.
	r := New(Config{Client: newStopped(), Exporter: &captureExporter{}})
	r.committed.ResourceVersion = "77"
	r.batch = []entry{{rv: "80"}, {rv: "81"}}
	r.pendingRV = "99"
	if err := r.stream(context.Background()); err == nil {
		t.Fatal("the pre-stopped watch must end the stream")
	}
	if len(r.batch) != 0 || r.pendingRV != "" {
		t.Fatalf("batch=%d pendingRV=%q, want the redelivered backlog dropped", len(r.batch), r.pendingRV)
	}

	// Cold skip-the-backlog restart: the new revision is PAST the buffered
	// entries — they are not re-delivered and must be retained.
	r = New(Config{Client: newStopped(), Exporter: &captureExporter{}, StartMode: StartEnd})
	r.batch = []entry{{rv: "80"}}
	if err := r.stream(context.Background()); err == nil {
		t.Fatal("the pre-stopped watch must end the stream")
	}
	if len(r.batch) != 1 {
		t.Fatalf("batch=%d, want the un-redelivered entry retained", len(r.batch))
	}
}

// The watermark must NOT filter a live or position-resumed stream. Event
// timestamps come from whichever component reported the event — kubelet events
// are second-truncated metav1.Time, scheduler and controller-manager events are
// microsecond MicroTime — so filtering there dropped every event whose
// reporter's clock trailed the watermark. One fast clock latched a future
// watermark, and since the watermark is persisted in the position ConfigMap,
// the blackout survived restarts and leader handover.
func TestWatermarkDoesNotFilterLiveStream(t *testing.T) {
	r, _, _ := newReader(t, Config{})
	base := time.Now()
	r.committed.Watermark = base
	r.replaying = false // positioned by a committed resourceVersion, or live

	for _, when := range []time.Time{
		base.Add(-time.Minute),    // a lagging reporter's clock
		base.Add(-time.Second),    // second-truncated kubelet timestamp
		base.Add(-24 * time.Hour), // a badly skewed node
	} {
		if !r.wanted(event("live", "R", "m", "Normal", "9", 1, when)) {
			t.Fatalf("a live event at %v was dropped: the watermark only applies to a replay", when)
		}
	}
}

// The replay filter is per-STREAM and survives a commit. Clearing it on the
// first acked batch disarmed it seconds into a replay that delivers the whole
// event TTL, so the rest of the backlog re-exported as duplicates — each Pod
// event costing a metadata lookup. Only the NEXT stream, positioned by the
// committed resourceVersion, starts unfiltered. And while the replay is
// UNSECURED (no bookmark yet), the commit itself is HELD: the backlog arrives
// in store order, so an advanced position would strand its undelivered
// remainder on a restart — the exported high-water accumulates aside and
// folds in when a bookmark secures the replay.
func TestReplayFilterSurvivesACommit(t *testing.T) {
	r, _, _ := newReader(t, Config{Exporter: &captureExporter{}, BatchSize: 1})
	r.replaying = true
	base := time.Now()
	r.committed.Watermark = base

	ctx := context.Background()
	if err := r.handle(ctx, watch.Event{Type: watch.Added, Object: event("new", "R", "m", "Normal", "11", 1, base.Add(time.Second))}); err != nil {
		t.Fatal(err)
	}
	if !r.replaying {
		t.Fatal("replay disarmed by a commit; the rest of the backlog would re-export")
	}
	if r.committed.Watermark.After(base) {
		t.Fatal("watermark advanced mid-replay; a subsequent replay would drop the undelivered older-timestamp remainder")
	}
	if r.committed.ResourceVersion != "" {
		t.Fatalf("position %q committed mid-replay; a restarted stream would skip the undelivered lower-RV backlog", r.committed.ResourceVersion)
	}
	if !r.heldWatermark.After(base) {
		t.Fatal("exported high-water not held for the fold on secure")
	}

	// A bookmark secures the replay: the held watermark folds in and later
	// flushes commit directly.
	bm := &corev1.Event{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "12"}}
	if err := r.handle(ctx, watch.Event{Type: watch.Bookmark, Object: bm}); err != nil {
		t.Fatal(err)
	}
	if !r.committed.Watermark.After(base) {
		t.Fatal("held watermark not folded in when the bookmark secured the replay")
	}
	if r.committed.ResourceVersion != "12" {
		t.Fatalf("committed %q, want the securing bookmark's revision", r.committed.ResourceVersion)
	}
}

// A stream death mid-replay must resume as a RELIST, never positioned: the
// backlog arrives in store order, so any position committed from a rendered
// prefix (its maximum included) lies PAST lower-RV backlog entries the dead
// stream never delivered — a positioned resume silently loses them. The
// commits are held while the replay is unsecured, so the next attempt
// relists and the watermark (also held) still filters what was exported.
func TestStreamDeathMidReplayRelistsAgain(t *testing.T) {
	exp := &captureExporter{}
	r, _, _ := newReader(t, Config{Exporter: exp, BatchSize: 1000})
	ctx := context.Background()
	base := time.Now().Add(-time.Hour)
	r.committed.Watermark = base // something was exported before the Gone
	r.expire("test")             // the API server dropped our resourceVersion
	if !r.relist {
		t.Fatal("precondition: relist armed")
	}
	rv, redelivers, err := r.startResourceVersion(ctx)
	if err != nil || rv != "" || !redelivers {
		t.Fatalf("relist start = (%q, %v, %v), want a full replay", rv, redelivers, err)
	}
	r.replaying, r.replaySecured = true, false

	// The replay delivers in STORE order: [100, 900] flush and ack while 200
	// is still undelivered.
	for _, ev := range []string{"9900100", "9900900"} {
		if err := r.handle(ctx, watch.Event{Type: watch.Added,
			Object: event("e"+ev, "R", "m", "Normal", ev, 1, base.Add(time.Minute))}); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.flush(ctx); err != nil {
		t.Fatal(err)
	}

	// The stream dies here. The next attempt must relist, not resume at 900.
	rv2, _, err := r.startResourceVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rv2 != "" {
		t.Fatalf("resumed positioned at %q after a mid-replay flush; the undelivered lower-RV backlog is lost", rv2)
	}
	if !r.relist {
		t.Fatal("relist disarmed by a mid-replay commit")
	}
}

// settle commits the batch's HIGHEST resourceVersion, not its last. A relist
// delivers the backlog in store order, so the last entry is routinely older
// than one earlier in the batch.
func TestSettleCommitsTheBatchMaximum(t *testing.T) {
	exp := &captureExporter{}
	r, _, _ := newReader(t, Config{Exporter: exp, BatchSize: 1000})
	ctx := context.Background()
	base := time.Now()
	for _, rv := range []string{"9880001", "9900412", "9885000"} {
		if err := r.handle(ctx, watch.Event{Type: watch.Added,
			Object: event("e"+rv, "R", "m", "Normal", rv, 1, base)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got := r.committed.ResourceVersion; got != "9900412" {
		t.Fatalf("committed %q; want the batch maximum 9900412 — committing the last entry walks the position backwards", got)
	}
}

// A bookmark seen while a batch was pending must never walk the committed
// position BACKWARDS. Bookmarks arrive interleaved with events, so one observed
// before the last few entries of a flush window carries an OLDER
// resourceVersion than the batch does; applying it unconditionally rolled the
// position back by up to a flush window, and the next restart or leader
// handover then redelivered every event in between.
func TestBookmarkNeverWalksThePositionBackwards(t *testing.T) {
	exp := &captureExporter{}
	r, _, _ := newReader(t, Config{Exporter: exp, BatchSize: 1000}) // never auto-flushes
	ctx := context.Background()
	now := time.Now()

	// A bookmark arrives mid-window, then two events with HIGHER revisions.
	if err := r.handle(ctx, watch.Event{Type: watch.Added, Object: event("a", "Started", "m", "Normal", "9900100", 1, now)}); err != nil {
		t.Fatal(err)
	}
	stale := &corev1.Event{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "9900150"}}
	if err := r.handle(ctx, watch.Event{Type: watch.Bookmark, Object: stale}); err != nil {
		t.Fatal(err)
	}
	if err := r.handle(ctx, watch.Event{Type: watch.Added, Object: event("b", "Started", "m", "Normal", "9900400", 1, now)}); err != nil {
		t.Fatal(err)
	}
	if err := r.flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got := r.committed.ResourceVersion; got != "9900400" {
		t.Fatalf("committed %q; want 9900400 — a bookmark older than the batch it was interleaved with must not roll the position back, or the next restart redelivers everything after it", got)
	}
	if r.pendingRV != "" {
		t.Errorf("pendingRV = %q; a consumed bookmark must be cleared either way", r.pendingRV)
	}
}

// The same guard on the batch itself: a relist delivers the TTL backlog in
// store order, so a later flush can carry entries OLDER than what an earlier
// one already committed. settle must keep the maximum.
func TestSettleNeverRegressesTheCommittedPosition(t *testing.T) {
	r, _, _ := newReader(t, Config{})
	r.committed.ResourceVersion = "9900400"
	mark := time.Now()
	r.committed.Watermark = mark

	r.settle(entry{rv: "9880001", when: mark.Add(-time.Minute)}, len(r.batch))
	if got := r.committed.ResourceVersion; got != "9900400" {
		t.Errorf("committed %q after settling an older batch; want 9900400 held — a backwards position redelivers every event in between on the next resume", got)
	}
	if !r.committed.Watermark.Equal(mark) {
		t.Errorf("watermark moved backwards to %v (was %v); the replay filter would stop dropping what was already exported",
			r.committed.Watermark, mark)
	}

	// A non-numeric resourceVersion is not comparable, so it must be REFUSED
	// rather than guessed at — the conservative direction is keeping what we
	// have, since a wrong guess forward is outright loss.
	r.settle(entry{rv: "not-a-number", when: mark}, len(r.batch))
	if got := r.committed.ResourceVersion; got != "9900400" {
		t.Errorf("committed %q from an uncomparable resourceVersion; want the known-good 9900400 kept", got)
	}
}

// The shutdown budget is DERIVED from the lease renew deadline the caller stops
// this work within, so the two cannot drift apart. A hard-coded budget above
// the deadline made a slow final flush report "leader work did not stop", which
// startEvents treats as fatal — the process exited non-zero and took the
// co-located -azure-diagnostics consumer with it.
func TestShutdownBudgetStaysUnderTheRenewDeadline(t *testing.T) {
	r, _, _ := newReader(t, Config{})
	def := r.shutdownBudget()
	if def != leader.DefaultRenewDeadline/2 {
		t.Errorf("default budget = %v, want half the leader renew deadline (%v)", def, leader.DefaultRenewDeadline/2)
	}
	if def >= leader.DefaultRenewDeadline {
		t.Errorf("default budget %v is not below the renew deadline %v: a slow final flush is reported as a stuck leader and fails the process",
			def, leader.DefaultRenewDeadline)
	}
	// An explicit budget is honoured verbatim — it is how a caller with a
	// longer lease widens the window.
	r, _, _ = newReader(t, Config{StopBudget: 3 * time.Second})
	if got := r.shutdownBudget(); got != 3*time.Second {
		t.Errorf("explicit budget = %v, want 3s", got)
	}
	// ...including a negative one, which must fall back rather than produce an
	// already-expired context that cancels the final flush before it starts.
	r, _, _ = newReader(t, Config{StopBudget: -time.Second})
	if got := r.shutdownBudget(); got <= 0 {
		t.Errorf("budget = %v for a negative StopBudget; the final flush would run on an already-expired context and the last events would be lost", got)
	}
}

// replaying is derived per STREAM, from the resourceVersion that stream starts
// at, and nothing inside the stream may change it. It gates the watermark
// filter: armed on a stream that re-receives the whole TTL backlog, disarmed on
// one the API server positioned exactly. Getting it wrong in either direction
// is a data bug — a stuck-on flag drops live events whose reporter's clock
// trails the watermark, a cleared one re-exports the rest of a backlog.
func TestReplayingIsDerivedPerStream(t *testing.T) {
	newStopped := func() *fake.Clientset {
		client := fake.NewSimpleClientset()
		client.PrependReactor("list", "events", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, &corev1.EventList{ListMeta: metav1.ListMeta{ResourceVersion: "500"}}, nil
		})
		w := watch.NewFake()
		w.Stop() // the stream sees a closed channel and returns right away
		client.PrependWatchReactor("events", k8stesting.DefaultWatchReactor(w, nil))
		return client
	}

	for _, tc := range []struct {
		name  string
		setup func(*Reader)
		mode  string
		want  bool
	}{
		// Cold start, skip the backlog: positioned at the List revision.
		{"cold-end", func(*Reader) {}, StartEnd, false},
		// Cold start from the beginning: "" replays the whole TTL.
		{"cold-start", func(*Reader) {}, StartBeginning, true},
		// A committed position: the API server delivers only what follows.
		{"committed-resume", func(r *Reader) { r.committed.ResourceVersion = "77" }, StartEnd, false},
		// A relist after Gone: "" again, and the watermark is what keeps the
		// replay from re-exporting.
		{"relist-after-gone", func(r *Reader) {
			r.committed.ResourceVersion = "77"
			r.committed.Watermark = time.Now().Add(-time.Minute)
			r.expire("watch")
		}, StartEnd, true},
		// An expiry with nothing ever exported honours the start mode instead
		// (a zero watermark cannot filter anything).
		{"expiry-without-watermark", func(r *Reader) {
			r.committed.ResourceVersion = "77"
			r.expire("watch")
		}, StartEnd, false},
		// A stale flag from the PREVIOUS stream must be re-derived, not
		// inherited: the stream that follows a relist is positioned exactly.
		{"stale-flag-recomputed", func(r *Reader) {
			r.replaying = true
			r.committed.ResourceVersion = "77"
		}, StartEnd, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := New(Config{Client: newStopped(), Exporter: &captureExporter{}, StartMode: tc.mode})
			tc.setup(r)
			if err := r.stream(context.Background()); err == nil {
				t.Fatal("the pre-stopped watch must end the stream")
			}
			if r.replaying != tc.want {
				t.Fatalf("replaying = %v after stream(), want %v", r.replaying, tc.want)
			}
			// The filter's boundary is snapshotted by the SAME call. Without
			// this assertion the only production writer of replayFrom could be
			// deleted with the suite still green — and a zero boundary
			// disables the filter, re-exporting a whole TTL window.
			//
			// It is the watermark MINUS replaySlack: the watermark is a maximum
			// over timestamps from unsynchronised clocks at two precisions, so a
			// strict boundary drops never-exported events on the one path that
			// exists to recover them. A zero watermark stays zero (the filter is
			// off, which the replaying flag above governs).
			want := r.committed.Watermark
			if !want.IsZero() {
				want = want.Add(-replaySlack)
			}
			if !r.replayFrom.Equal(want) {
				t.Fatalf("replayFrom = %v after stream(), want %v (watermark %v less the replay slack)",
					r.replayFrom, want, r.committed.Watermark)
			}
		})
	}
}

// Nothing WITHIN a stream may disarm the replay filter, and a mid-replay
// COMMIT may not disarm the relist either: the store-order backlog is not
// covered by any prefix's position, so an acked flush holds the position and
// keeps the relist armed. Only a BOOKMARK — which the API server sends once
// the watcher is caught up — proves the backlog fully delivered; it secures
// the relist and moves the position, while the replay filter still survives
// for the stream's lifetime (it is per-stream by definition).
func TestReplayingSurvivesCommitsAndBookmarks(t *testing.T) {
	r, _, _ := newReader(t, Config{Exporter: &captureExporter{}, BatchSize: 1})
	r.replaying = true
	r.relist = true
	base := time.Now()
	r.committed.Watermark = base
	ctx := context.Background()

	// A committed batch (BatchSize 1 flushes inline).
	if err := r.handle(ctx, watch.Event{Type: watch.Added,
		Object: event("new", "R", "m", "Normal", "11", 1, base.Add(time.Second))}); err != nil {
		t.Fatal(err)
	}
	if !r.replaying {
		t.Fatal("a commit disarmed the replay filter; the rest of the backlog would re-export as duplicates")
	}
	if !r.relist {
		t.Error("a mid-replay commit disarmed the relist; a death before the backlog finishes then resumes positioned and loses the remainder")
	}
	if r.committed.ResourceVersion != "" {
		t.Errorf("a mid-replay commit advanced the position to %q; a restarted stream would skip undelivered lower-RV backlog", r.committed.ResourceVersion)
	}

	// A bookmark, applied immediately (nothing buffered): it secures the
	// relist and the position.
	if err := r.handle(ctx, watch.Event{Type: watch.Bookmark,
		Object: &corev1.Event{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "120"}}}); err != nil {
		t.Fatal(err)
	}
	if !r.replaying {
		t.Fatal("a bookmark disarmed the replay filter; a bookmark carries a position, not the start of a new stream")
	}
	if r.relist {
		t.Error("the securing bookmark must disarm the relist")
	}
	if r.committed.ResourceVersion != "120" {
		t.Fatalf("idle bookmark not applied, got %q", r.committed.ResourceVersion)
	}
}

// A relist delivers the backlog in STORE order, not time order, so a batch
// committed mid-replay routinely carries a timestamp NEWER than events still
// to come. Filtering against the live committed watermark therefore dropped
// those as "already exported" when they never were — destroying exactly the
// gap the relist exists to recover, silently, and unrecoverably (settle
// commits the batch maximum, so a restart cannot reach back either).
func TestReplayFilterIsFrozenAtStreamStart(t *testing.T) {
	r, _, _ := newReader(t, Config{})
	base := time.Now()
	r.committed.Watermark = base.Add(-time.Hour) // everything since is unexported
	r.replaying = true
	r.replayFrom = r.committed.Watermark

	// A batch commits mid-replay carrying the newest event in the backlog.
	r.committed.Watermark = base

	// An event OLDER than that commit but newer than where the replay started
	// is still owed: it must survive the filter.
	owed := event("owed", "R", "m", "Normal", "7", 1, base.Add(-30*time.Minute))
	if !r.wanted(owed) {
		t.Fatal("an unexported event was dropped because a mid-replay commit advanced the live watermark past it")
	}

	// The frozen boundary still filters what really was exported before.
	if r.wanted(event("done", "R", "m", "Normal", "8", 1, base.Add(-2*time.Hour))) {
		t.Fatal("an event exported before this replay began was re-emitted")
	}
}

// Severity TEXT casing is a cross-producer contract, not a local style choice:
// convert runs logenrich.Apply with overwrite semantics over every record, and
// enrich writes its six level names in lowercase. Uppercase here meant one
// event shipped "WARN" and the next shipped "warn" purely because the second
// message happened to parse. journald and azurediag carry the same assertion.
func TestSeverityTextIsLowercase(t *testing.T) {
	for _, typ := range []string{"Warning", "Normal", "", "Something"} {
		_, text := severityOf(typ)
		if text == "" || text != strings.ToLower(text) {
			t.Errorf("severityOf(%q) text = %q, want a lowercase level name", typ, text)
		}
	}
}

// A permanently rejected batch is skipped past — real, deliberate loss. Count
// the RECORDS, not just the batch: an events batch is up to BatchSize entries,
// so the batch counter answers whether a loss happened and never how big it
// was.
func TestPermanentRejectionCountsRecords(t *testing.T) {
	exp := &captureExporter{failN: 1, err: &otlpexport.HTTPStatusError{Code: 400, Body: "bad payload"}}
	r, _, _ := newReader(t, Config{Exporter: exp, BatchSize: 1000})
	ctx := context.Background()
	now := time.Now()
	for _, rv := range []string{"1", "2", "3", "4"} {
		if err := r.handle(ctx, watch.Event{Type: watch.Added,
			Object: event("e"+rv, "R", "m", "Normal", rv, 1, now)}); err != nil {
			t.Fatal(err)
		}
	}

	beforeBatches := obs.EventsDropped.Value()
	beforeRecords := obs.EventsDroppedRecords.Value()
	if err := r.flush(ctx); err != nil {
		t.Fatalf("a permanent rejection must be skipped past, not returned: %v", err)
	}
	if got := obs.EventsDropped.Value() - beforeBatches; got != 1 {
		t.Fatalf("dropped batches = %v, want 1", got)
	}
	if got := obs.EventsDroppedRecords.Value() - beforeRecords; got != 4 {
		t.Fatalf("dropped records = %v, want 4 — the batch counter alone cannot size the loss", got)
	}
}

// Before the first ACKED export there is no committed resourceVersion, and the
// cold-start branch took a fresh List — whose revision is the CURRENT store
// revision. So every stream restart in that window began AFTER everything that
// had happened since the previous watch died, losing it silently.
//
// The window is not narrow: any export failure tears the stream down, Run backs
// off and restarts, and nothing commits while the collector is down — so on a
// fresh install with an unreachable collector the gap is the whole outage. Only
// restart and failure counters moved; no drop counter existed.
//
// A stream that has already run in this process must resume from the highest
// revision it observed instead.
func TestColdRestartResumesFromTheHighestSeenRevision(t *testing.T) {
	var lists int
	client := fake.NewSimpleClientset()
	client.PrependReactor("list", "events", func(k8stesting.Action) (bool, runtime.Object, error) {
		lists++
		return true, &corev1.EventList{ListMeta: metav1.ListMeta{ResourceVersion: "500"}}, nil
	})
	r := New(Config{Client: client, Exporter: &captureExporter{}, StartMode: StartEnd})

	// First stream: no committed position and nothing seen -> List.
	rv, redelivers, err := r.startResourceVersion(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rv != "500" || redelivers {
		t.Fatalf("first start: rv=%q redelivers=%v, want 500/false", rv, redelivers)
	}
	if lists != 1 {
		t.Fatalf("first start issued %d Lists, want 1", lists)
	}

	// The stream observes events but NOTHING is acked (the collector is down).
	r.noteSeen("501")
	r.noteSeen("507")
	r.noteSeen("503") // out of order: the highest must win
	if r.committed.ResourceVersion != "" {
		t.Fatal("setup: nothing should be committed")
	}

	// The stream dies and restarts. It must resume at 507, not re-List at 500
	// (or at whatever the store has advanced to since).
	rv, redelivers, err = r.startResourceVersion(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rv != "507" {
		t.Errorf("cold restart resumed at %q, want 507 — the events between the dead watch "+
			"and the restart are lost", rv)
	}
	if redelivers {
		t.Error("resuming past the seen revision does not redeliver, so the batch must be retained")
	}
	if lists != 1 {
		t.Errorf("cold restart issued another List (%d total): that is the gap", lists)
	}

	// A Gone revision is dead: the in-process memory of it must go too, or the
	// restart resumes from a revision the API server has dropped.
	r.expire("watch")
	if r.seenRV != "" {
		t.Error("expire() must clear seenRV; a Gone revision cannot be resumed from")
	}
}

// The watermark is a running MAXIMUM over timestamps written by whichever
// component reported each event, so one reporter with a fast clock would latch
// a boundary in the FUTURE and make the relist filter discard everything until
// real time caught up.
func TestWatermarkIsClampedToWallClock(t *testing.T) {
	now := time.Now()
	r := New(Config{Client: fake.NewSimpleClientset(), Exporter: &captureExporter{}})
	r.now = func() time.Time { return now }

	future := now.Add(2 * time.Hour)
	r.batch = []entry{{when: future}}
	r.settle(entry{when: future}, len(r.batch))

	if r.committed.Watermark.After(now) {
		t.Errorf("watermark = %v, later than wall clock %v: a fast reporter clock latched the boundary",
			r.committed.Watermark, now)
	}
	if r.committed.Watermark.IsZero() {
		t.Error("the clamp must not discard the watermark entirely")
	}
}

// A redelivers=false restart (cold — before the first commit) retains the batch
// AND its already-rendered payload while the new watch appends fresh entries.
// A subsequent successful flush exports only the frozen prefix, so it must
// commit the position over that prefix alone: committing past the appended-but-
// unexported tail loses those events silently (the watermark then filters them
// out of any relist forever).
func TestColdRestartGrowthDoesNotCommitPastUnexported(t *testing.T) {
	exp := &captureExporter{failN: 1, err: context.DeadlineExceeded}
	r, _, _ := newReader(t, Config{Exporter: exp, BatchSize: 100})
	ctx := context.Background()
	now := time.Now()

	// Cold reader (nothing committed). Ingest A (rv 10); the first flush fails
	// transiently, so the batch and its rendering are retained.
	if err := r.handle(ctx, watch.Event{Type: watch.Added, Object: event("a", "R", "m", "Normal", "10", 1, now)}); err != nil {
		t.Fatal(err)
	}
	if err := r.flush(ctx); err == nil {
		t.Fatal("first flush should fail transiently")
	}

	// The redelivers=false restart's new watch appends B (rv 11) past the frozen
	// prefix — the batch grows while the pending payload still covers only [A].
	if err := r.handle(ctx, watch.Event{Type: watch.Added, Object: event("b", "R", "m", "Normal", "11", 1, now.Add(time.Second))}); err != nil {
		t.Fatal(err)
	}

	// The second flush succeeds but ships only [A]; it must NOT commit past B.
	if err := r.flush(ctx); err != nil {
		t.Fatal(err)
	}
	if r.committed.ResourceVersion == "11" {
		t.Fatal("committed rv=11 but B was never exported — silent event loss")
	}
	if r.committed.ResourceVersion != "10" {
		t.Fatalf("committed=%q, want 10 (A's rv, the exported prefix)", r.committed.ResourceVersion)
	}
	if len(r.batch) != 1 || r.batch[0].rv != "11" {
		t.Fatalf("the unexported tail must be retained; batch=%+v", r.batch)
	}

	// The third flush ships B and commits it — nothing lost.
	if err := r.flush(ctx); err != nil {
		t.Fatal(err)
	}
	if r.committed.ResourceVersion != "11" {
		t.Fatalf("committed=%q, want 11 after shipping B", r.committed.ResourceVersion)
	}
	if got := exp.records(); len(got) != 2 {
		t.Fatalf("exported %d records, want 2 (A then B, each exactly once)", len(got))
	}
}

// The bookmark variant of the cold-restart-growth case: a bookmark's revision
// vouches for EVERYTHING the stream delivered before it, including entries
// retained past the rendered prefix (a redelivers=false restart's fresh
// appends), so applying it after a flush that covered only the prefix put the
// committed position ABOVE the unexported tail — a later stream restart's
// redelivers=true clear then dropped the tail, and the watch resumed from the
// bookmark never re-delivered it: silent, uncounted loss.
func TestBookmarkStaysPendingWhileATailIsRetained(t *testing.T) {
	exp := &captureExporter{failN: 1, err: context.DeadlineExceeded}
	r, _, _ := newReader(t, Config{Exporter: exp, BatchSize: 100})
	ctx := context.Background()
	now := time.Now()

	// Cold reader (nothing committed). Ingest A (rv 10); the first flush fails
	// transiently, so the batch and its rendering are retained.
	if err := r.handle(ctx, watch.Event{Type: watch.Added, Object: event("a", "R", "m", "Normal", "10", 1, now)}); err != nil {
		t.Fatal(err)
	}
	if err := r.flush(ctx); err == nil {
		t.Fatal("first flush should fail transiently")
	}

	// The redelivers=false restart's new watch appends B (rv 11) past the
	// frozen prefix, then a bookmark whose revision covers B arrives.
	if err := r.handle(ctx, watch.Event{Type: watch.Added, Object: event("b", "R", "m", "Normal", "11", 1, now.Add(time.Second))}); err != nil {
		t.Fatal(err)
	}
	bm := &corev1.Event{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "12"}}
	if err := r.handle(ctx, watch.Event{Type: watch.Bookmark, Object: bm}); err != nil {
		t.Fatal(err)
	}

	// The collector recovers; this flush ships only [A]. The bookmark must NOT
	// commit: its revision is above B, which is still unexported.
	if err := r.flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got := r.committed.ResourceVersion; got != "10" {
		t.Fatalf("committed=%q, want 10 (A's rv) — a bookmark above the retained tail must not commit", got)
	}
	if r.pendingRV != "12" {
		t.Fatalf("pendingRV=%q, want the bookmark kept pending for the flush that covers the tail", r.pendingRV)
	}

	// The flush that empties the batch consumes the bookmark.
	if err := r.flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got := r.committed.ResourceVersion; got != "12" {
		t.Fatalf("committed=%q, want 12 once the tail shipped", got)
	}
	if r.pendingRV != "" {
		t.Fatalf("pendingRV=%q, want cleared once applied", r.pendingRV)
	}
	if got := exp.records(); len(got) != 2 {
		t.Fatalf("exported %d records, want 2 (A then B, each exactly once)", len(got))
	}
}

// Before anything commits, every stream restart retains the batch (the
// redelivers=false contract: the new watch does not re-send it) while
// appending fresh entries, and BatchSize only triggers flush ATTEMPTS — which
// fail for as long as the collector is down. Unbounded, that is an OOM
// against the singleton's memory limit that loses the whole batch AND the
// whole outage window (StartMode=end after the restart) and takes the
// co-located pipelines down. The cap sheds the oldest entry the pending
// payload does not cover, counted into obs.EventsOverflowDropped.
func TestRetainedBatchIsBoundedDuringOutage(t *testing.T) {
	exp := &captureExporter{failN: 1 << 30, err: context.DeadlineExceeded}
	r, _, _ := newReader(t, Config{Exporter: exp, BatchSize: 100})
	ctx := context.Background()
	now := time.Now()
	before := obs.EventsOverflowDropped.Value()

	limit := r.retainCap()
	over := 2*shedChunk + 25
	total := limit + over
	for i := 0; i < total; i++ {
		// Flush attempts past BatchSize fail transiently; the batch is retained
		// for the retry, which is exactly the retention path under test.
		if err := r.handle(ctx, watch.Event{Type: watch.Added,
			Object: event(fmt.Sprintf("e%d", i), "R", "m", "Normal", strconv.Itoa(100+i), 1, now)}); err != nil {
			t.Fatalf("handle returned %v — a failed export must not surface as a stream failure", err)
		}
	}
	if len(r.batch) > limit {
		t.Fatalf("batch=%d, want at most the cap %d", len(r.batch), limit)
	}
	dropped := int(obs.EventsOverflowDropped.Value() - before)
	if dropped+len(r.batch) != total {
		t.Fatalf("dropped=%d + retained=%d != ingested=%d — the loss must be counted exactly", dropped, len(r.batch), total)
	}
	// Shedding a run costs at most one chunk more than the strict surplus.
	if dropped < over || dropped >= over+shedChunk {
		t.Fatalf("dropped=%d, want in [%d,%d)", dropped, over, over+shedChunk)
	}
	// The rendered prefix survives (its entries are in the retained payload,
	// not lost) and the newest entries survive (drop-oldest); the shed range
	// sits in between.
	if r.batch[0].rv != "100" {
		t.Fatalf("prefix rv=%q, want 100 — shedding inside the rendered prefix orphans the payload", r.batch[0].rv)
	}
	if got, want := r.batch[len(r.batch)-1].rv, strconv.Itoa(100+total-1); got != want {
		t.Fatalf("newest rv=%q, want %s retained", got, want)
	}

	// Recovery: everything retained ships exactly once and the position lands
	// on the newest entry — the shed range cost nothing extra.
	retained := len(r.batch)
	exp.mu.Lock()
	exp.failN = 0
	exp.mu.Unlock()
	for len(r.batch) > 0 {
		if err := r.flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(exp.records()); got != retained {
		t.Fatalf("exported %d records, want the %d retained, each exactly once", got, retained)
	}
	if got, want := r.committed.ResourceVersion, strconv.Itoa(100+total-1); got != want {
		t.Fatalf("committed=%q, want %s", got, want)
	}
}

// One shed must drop a RUN. Dropping exactly one entry per admitted event made
// the shed O(batch): at the cap EVERY event sheds, and each shed memmoved the
// whole post-prefix tail (measured 47.5 µs against 2.8 µs below the cap,
// 1.18 MB moved per event). That was masked while a failed export tore the
// watch down and throttled arrivals to one per backoff; with the watch held
// open it is the binding cost.
func TestShedDropsARunSoTheMemmoveAmortises(t *testing.T) {
	exp := &captureExporter{failN: 1 << 30, err: context.DeadlineExceeded}
	r, _, _ := newReader(t, Config{Exporter: exp, BatchSize: 100})
	ctx := context.Background()
	now := time.Now()
	before := obs.EventsOverflowDropped.Value()

	limit := r.retainCap()
	// Fewer over-cap events than one chunk: a per-event shed drops exactly
	// this many, a chunked shed drops a whole chunk once.
	over := shedChunk / 2
	for i := 0; i < limit+over; i++ {
		if err := r.handle(ctx, watch.Event{Type: watch.Added,
			Object: event(fmt.Sprintf("e%d", i), "R", "m", "Normal", strconv.Itoa(100+i), 1, now)}); err != nil {
			t.Fatal(err)
		}
	}
	dropped := int(obs.EventsOverflowDropped.Value() - before)
	if dropped < shedChunk {
		t.Fatalf("dropped=%d after %d over-cap admissions, want a whole chunk (%d): one entry per event memmoves the tail per event",
			dropped, over, shedChunk)
	}
	if len(r.batch) > limit {
		t.Fatalf("batch=%d, want at most the cap %d", len(r.batch), limit)
	}
}

// The cap's floor exists so the count-triggered flush stays reachable however
// BatchSize is tuned — but -events-batch-size has no upper bound, so an
// unfloored max() let it repeal the memory bound outright (100000 => ~400 MB
// retained at the measured ~2 KB per entry, against a 256Mi limit).
func TestRetainCapIsBoundedHoweverBatchSizeIsTuned(t *testing.T) {
	for _, batchSize := range []int{1, 100, 512, 8192, 100000} {
		r, _, _ := newReader(t, Config{BatchSize: batchSize})
		got := r.retainCap()
		if got < maxRetained {
			t.Errorf("BatchSize=%d: cap=%d, want at least %d", batchSize, got, maxRetained)
		}
		if got > maxRetainedCeiling {
			t.Errorf("BatchSize=%d: cap=%d exceeds the ceiling %d — the cap is no longer a memory bound",
				batchSize, got, maxRetainedCeiling)
		}
	}
}

// A transient export failure is a FLUSH failure, not a STREAM failure. It used
// to propagate handle -> stream and tear down the API-server watch; past
// BatchSize the batch is retained, so the condition held forever and every
// subsequent event bought another failing export and another teardown. Only
// one event was handled per round, so seenRV fell behind the cluster's event
// rate, aged out of the watch window within minutes, and the Gone expired into
// a relist a cold reader cannot take — the whole gap discarded, counted only
// by a metric whose name asserts a relist that did not happen.
func TestExportFailureKeepsTheWatchOpen(t *testing.T) {
	exp := &captureExporter{failN: 1 << 30, err: context.DeadlineExceeded}
	// FlushInterval an hour: the only export attempts are the count-triggered
	// ones, so the pacing is observable.
	r, _, _ := newReader(t, Config{Exporter: exp, BatchSize: 2, FlushInterval: time.Hour})
	ctx := context.Background()
	now := time.Now()

	const n = 10
	for i := 0; i < n; i++ {
		if err := r.handle(ctx, watch.Event{Type: watch.Added,
			Object: event(fmt.Sprintf("e%d", i), "R", "m", "Normal", strconv.Itoa(100+i), 1, now)}); err != nil {
			t.Fatalf("event %d: handle returned %v; stream() turns that into a watch teardown", i, err)
		}
	}
	// The watch is still the reader's position: seenRV tracks the stream even
	// though nothing has been acked.
	if want := strconv.Itoa(100 + n - 1); r.seenRV != want {
		t.Fatalf("seenRV=%q, want %s — the reader must keep tracking the live stream while the collector is down", r.seenRV, want)
	}
	if len(r.batch) != n {
		t.Fatalf("batch=%d, want all %d retained for the retry", len(r.batch), n)
	}
	if r.committed.ResourceVersion != "" {
		t.Fatalf("committed=%q, want nothing committed — no export succeeded", r.committed.ResourceVersion)
	}
	// And the retry is PACED: one attempt, not one per event past BatchSize.
	if got := exp.attempts(); got != 1 {
		t.Fatalf("export attempts=%d, want 1 per FlushInterval — a batch parked at BatchSize must not re-export per event", got)
	}

	// Recovery goes through the same door.
	exp.mu.Lock()
	exp.failN = 0
	exp.mu.Unlock()
	for len(r.batch) > 0 {
		if err := r.flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := r.committed.ResourceVersion, strconv.Itoa(100+n-1); got != want {
		t.Fatalf("committed=%q, want %s once the collector recovered", got, want)
	}
	if got := len(exp.records()); got != n {
		t.Fatalf("exported %d records, want all %d, each exactly once", got, n)
	}
}

// logchain.Groups.Get fills a group's resource from the FIRST entry that lands
// in it, so a resource built per EVENT was discarded unread for every repeat —
// and repeats dominate here by construction, since Kubernetes aggregates
// recurrences into one object re-sent as Modified. Retained heap was flat in
// the number of distinct involved objects, which is the tell.
func TestResourceIsBuiltOncePerInvolvedObject(t *testing.T) {
	meta := &countingMeta{pod: &kubemeta.Pod{Name: "web-abc", Namespace: "default", UID: "pod-uid"}}
	r, _, _ := newReader(t, Config{Meta: meta, BatchSize: 1000})
	ctx := context.Background()
	now := time.Now()

	// 50 occurrences of the same event object (the "BackOff x47" shape).
	for i := 0; i < 50; i++ {
		e := event("a", "BackOff", "back-off", "Warning", strconv.Itoa(100+i), int32(i+1), now)
		if err := r.handle(ctx, watch.Event{Type: watch.Modified, Object: e}); err != nil {
			t.Fatal(err)
		}
	}
	if meta.calls != 1 {
		t.Fatalf("resolved the involved pod %d times for 50 occurrences of one object, want 1", meta.calls)
	}
	// A different involved object is a different resource.
	e := event("b", "Pulled", "pulled", "Normal", "200", 1, now)
	e.InvolvedObject.Name = "web-def"
	e.InvolvedObject.UID = "pod-uid-2"
	if err := r.handle(ctx, watch.Event{Type: watch.Added, Object: e}); err != nil {
		t.Fatal(err)
	}
	if meta.calls != 2 {
		t.Fatalf("meta lookups = %d, want 2 (one per distinct involved object)", meta.calls)
	}
	// The payload still groups per object, with the identity intact.
	ld := r.convert()
	if got := ld.ResourceLogs().Len(); got != 2 {
		t.Fatalf("resource groups = %d, want 2", got)
	}
	if got := ld.LogRecordCount(); got != 51 {
		t.Fatalf("records = %d, want 51", got)
	}

	// The memo is per BATCH: a settled batch must not keep resolving from it.
	if err := r.flush(ctx); err != nil {
		t.Fatal(err)
	}
	if len(r.resCache) != 0 {
		t.Fatalf("resCache holds %d entries after the batch settled", len(r.resCache))
	}
}

// recordingStore records whether the context Save ran on was already expired.
type recordingStore struct {
	saved   Position
	savedAt error
	calls   int
}

func (s *recordingStore) Load(context.Context) (Position, bool, error) { return Position{}, false, nil }

func (s *recordingStore) Save(ctx context.Context, pos Position) error {
	s.calls++
	s.savedAt = ctx.Err()
	s.saved = pos
	return ctx.Err()
}

// budgetBurner fails transiently while the reader is live (so nothing settles
// before the shutdown) and, on the DEADLINED shutdown context, consumes the
// whole budget before succeeding — the shape of an exporter whose retries fill
// it. live signals the first live attempt, which proves the event reached the
// batch without the test racing the reader's own fields.
type budgetBurner struct{ live chan struct{} }

func (b *budgetBurner) ExportLogs(ctx context.Context, _ plog.Logs) error {
	if _, deadlined := ctx.Deadline(); !deadlined {
		select {
		case b.live <- struct{}{}:
		default:
		}
		return context.DeadlineExceeded
	}
	<-ctx.Done()
	return nil
}

// The final flush and the final position write shared ONE deadline, so the
// exporter's retries could spend all of it and leave persist a Get+Update on
// an expired context that failed instantly — the successor then replayed up to
// PersistInterval of events at exactly the handover the ConfigMap position
// exists for.
func TestShutdownPersistGetsItsOwnDeadline(t *testing.T) {
	store := &recordingStore{}
	exp := &budgetBurner{live: make(chan struct{}, 1)}
	r, _, w := newReader(t, Config{
		Exporter:   exp,
		Positions:  store,
		StopBudget: 300 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()

	w.Add(event("a", "R", "m", "Normal", "42", 1, time.Now()))
	select {
	case <-exp.live: // the event is in the batch
	case <-time.After(5 * time.Second):
		cancel()
		<-done
		t.Fatal("timed out waiting for the first export attempt")
	}
	cancel()
	<-done

	if store.calls == 0 {
		t.Fatal("the final position was never written")
	}
	if store.savedAt != nil {
		t.Fatalf("persist ran on an expired context (%v): the flush spent the whole shutdown budget", store.savedAt)
	}
	if store.saved.ResourceVersion != "42" {
		t.Fatalf("saved position = %q, want 42", store.saved.ResourceVersion)
	}
}
