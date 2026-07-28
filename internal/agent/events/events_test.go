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

	"github.com/JohanLindvall/kubescrape/internal/logline"
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
