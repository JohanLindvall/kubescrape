package events

import (
	"context"
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
type captureExporter struct {
	mu    sync.Mutex
	sent  []plog.Logs
	failN int
	err   error
}

func (c *captureExporter) ExportLogs(_ context.Context, ld plog.Logs) error {
	c.mu.Lock()
	defer c.mu.Unlock()
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
	r.settle(entry{rv: "1001", when: time.Now()})
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
// committed resourceVersion, starts unfiltered.
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
	if !r.committed.Watermark.After(base) {
		t.Fatal("watermark did not advance with the committed batch")
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

	r.settle(entry{rv: "9880001", when: mark.Add(-time.Minute)})
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
	r.settle(entry{rv: "not-a-number", when: mark})
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

// Nothing WITHIN a stream may disarm the replay filter. A commit only secures
// the relist (r.relist), and a bookmark only moves the position — neither says
// the backlog has finished arriving, and clearing the flag on either one
// re-exported the remainder of a TTL window's events, each Pod event costing a
// metadata lookup.
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
	if r.relist {
		t.Error("a commit must still secure the relist — that IS what settle clears")
	}

	// A bookmark, applied immediately (nothing buffered).
	if err := r.handle(ctx, watch.Event{Type: watch.Bookmark,
		Object: &corev1.Event{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "120"}}}); err != nil {
		t.Fatal(err)
	}
	if !r.replaying {
		t.Fatal("a bookmark disarmed the replay filter; a bookmark carries a position, not the end of the backlog")
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
	r.settle(entry{when: future})

	if r.committed.Watermark.After(now) {
		t.Errorf("watermark = %v, later than wall clock %v: a fast reporter clock latched the boundary",
			r.committed.Watermark, now)
	}
	if r.committed.Watermark.IsZero() {
		t.Error("the clamp must not discard the watermark entirely")
	}
}
