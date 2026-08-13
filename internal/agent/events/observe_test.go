package events

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// metricCapture reads a counter's exported total.
type metricCapture struct{ md []pmetric.Metrics }

func (c *metricCapture) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	cp := pmetric.NewMetrics()
	md.CopyTo(cp)
	c.md = append(c.md, cp)
	return nil
}

func (c *metricCapture) sum(name string) float64 {
	total := 0.0
	for _, md := range c.md {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					m := ms.At(k)
					if m.Name() != name || m.Type() != pmetric.MetricTypeSum {
						continue
					}
					dps := m.Sum().DataPoints()
					// The last point: a counter's first export is preceded by
					// synthetic zero points giving rate() a baseline.
					if dps.Len() > 0 {
						total += dps.At(dps.Len() - 1).DoubleValue()
					}
				}
			}
		}
	}
	return total
}

// redeliverClient hands every Watch call a FRESH watcher pre-loaded with the
// same events and already stopped — the API server's behaviour for a watch
// positioned at a resourceVersion the reader never committed past: the stream
// ends (a randomized timeout, an apiserver rollout, an LB blip) and the next
// one re-sends everything after that position.
type redeliverClient struct {
	*fake.Clientset
	events  []*corev1.Event
	watches int
}

func newRedeliverClient(events ...*corev1.Event) *redeliverClient {
	c := &redeliverClient{Clientset: fake.NewSimpleClientset(), events: events}
	c.PrependReactor("list", "events", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.EventList{ListMeta: metav1.ListMeta{ResourceVersion: "500"}}, nil
	})
	c.PrependWatchReactor("events", func(k8stesting.Action) (bool, watch.Interface, error) {
		c.watches++
		w := watch.NewFakeWithChanSize(len(c.events), false)
		for _, e := range c.events {
			w.Add(e)
		}
		// Stop closes the channel; the buffered events are still delivered
		// before the consumer sees the close, so one stream carries them all
		// and then ends.
		w.Stop()
		return true, w, nil
	})
	return c
}

// A collector outage must not inflate the counters an operator reads DURING
// that outage.
//
// The events reader cannot retry in place the way journald does: when a watch
// ends while a batch is un-acked, the next stream re-delivers every buffered
// entry, so the batch and its rendering are dropped and rebuilt (stream). That
// rebuild re-runs the shared per-record chain — so before the observed set,
// every user-configured log metric and every kubescrape_log_rules_dropped_total
// increment was multiplied by the number of watch restarts the outage spanned.
// Measured with the shape below: 10 observations for 2 events across 5 laps,
// and 5 rule drops for 1 dropped event, with nothing delivered at all.
//
// Delivery is at-least-once and duplicate records are the accepted cost of
// that; OBSERVATION is once per delivery, because these series are cumulative
// and nothing downstream can undo a double count.
func TestEventsObserveOncePerDeliveryAcrossWatchRestarts(t *testing.T) {
	set, err := metrics.NewDynamicMetricSet([]metrics.Dynamic{{
		Name: "event_lines_total", Type: metrics.CounterType, Value: "1",
	}})
	if err != nil {
		t.Fatal(err)
	}
	rules, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "drop", MatchRegexp: []string{"__line__=noise"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	client := newRedeliverClient(
		event("pulled", "Pulled", "pulled image", "Normal", "20", 1, now),
		event("backoff", "BackOff", "noise back-off", "Warning", "21", 1, now),
	)
	exp := &captureExporter{failN: 1000, err: errors.New("collector down")}
	// BatchSize above the two events: nothing flushes inside stream, so each
	// lap is exactly one restart plus one export attempt and the test needs no
	// clock at all.
	r := New(Config{
		Client: client, Exporter: exp, BatchSize: 100,
		LogMetrics: set, Rules: rules, Meta: fakeMeta{},
	})
	// A committed position is the precondition: it is what makes the restart a
	// REDELIVERING one (startResourceVersion), which is the path that drops the
	// batch and rebuilds it.
	r.committed.ResourceVersion = "5"

	dropped := obs.LogRulesDropped.Value()
	rewinds := obs.LogExportFailures.Value()
	failures := obs.EventsExportFailures.Value()
	ctx := context.Background()
	const laps = 5
	for i := 0; i < laps; i++ {
		if err := r.stream(ctx); err == nil {
			t.Fatalf("lap %d: the pre-stopped watch must end the stream", i)
		}
		r.tryFlush(ctx)
	}
	if exp.attempts() != laps {
		t.Fatalf("export attempts = %d, want %d (one per lap)", exp.attempts(), laps)
	}
	if client.watches != laps {
		t.Fatalf("watches opened = %d, want %d: each lap must really be a restart, which is what "+
			"makes the batch dropped and rebuilt", client.watches, laps)
	}
	if got := len(exp.records()); got != 0 {
		t.Fatalf("delivered %d records, want 0: the collector is down for the whole test", got)
	}

	mexp := &metricCapture{}
	if err := set.Export(context.Background(), mexp, 0); err != nil {
		t.Fatal(err)
	}
	if got := mexp.sum("event_lines_total"); got != 2 {
		t.Errorf("event_lines_total = %v after %d watch restarts over the same 2 un-acked events, want 2 — "+
			"one observation per event per DELIVERY, not per restart", got, laps)
	}
	if got := obs.LogRulesDropped.Value() - dropped; got != 1 {
		t.Errorf("kubescrape_log_rules_dropped_total delta = %v, want 1: the one dropped event "+
			"was re-counted on every restart", got)
	}
	if got := obs.EventsExportFailures.Value() - failures; got != laps {
		t.Errorf("kubescrape_events_export_failures_total delta = %v, want %d (one per failed attempt)", got, laps)
	}
	// kubescrape_log_export_failures_total documents itself as the TAILER's
	// rewind counter ("files rewound"). This reader rewinds nothing and the
	// singleton that runs it has -logs=false, so there is no file anywhere in
	// the process to rewind — an operator alerting on that counter was paged
	// by a pod that owns none.
	if got := obs.LogExportFailures.Value() - rewinds; got != 0 {
		t.Errorf("kubescrape_log_export_failures_total delta = %v, want 0: the events reader must not "+
			"bump the tailer's files-rewound counter", got)
	}

	// The collector returns: the batch must still deliver, once, with the
	// kept record — suppressing the COUNTING must not suppress the record.
	exp.mu.Lock()
	exp.failN = 0
	exp.mu.Unlock()
	if err := r.flush(ctx); err != nil {
		t.Fatalf("flush after recovery: %v", err)
	}
	recs := exp.records()
	if len(recs) != 1 || recs[0].Body().Str() != "pulled image" {
		t.Fatalf("delivered %d records, want the one kept event", len(recs))
	}
	mexp = &metricCapture{}
	if err := set.Export(context.Background(), mexp, 0); err != nil {
		t.Fatal(err)
	}
	if got := mexp.sum("event_lines_total"); got != 2 {
		t.Errorf("event_lines_total = %v after the delivery, want 2", got)
	}
	// Settled entries retire their proof: the set is bounded by what the batch
	// still owes, or a long-lived leader accumulates a key per event forever.
	if len(r.observed) != 0 {
		t.Errorf("observed set holds %d keys after the batch settled, want 0", len(r.observed))
	}
}

// The suppression must key on the OCCURRENCE, not on the content and not on
// the object: two distinct events carrying the same message are two
// observations, and a REPEAT — which Kubernetes aggregates into one object
// re-sent as Modified with a growing count, the "BackOff x47" case — is a new
// occurrence with a new resourceVersion and must be observed again.
//
// This is the over-claiming direction, and it is the dangerous one:
// re-observing merely inflates a counter, while suppressing an occurrence that
// was never counted destroys it invisibly. So the redelivery has to be crossed
// with a genuine repeat, which is what the second lap does — a key that dropped
// the resourceVersion (the object alone) or hashed the BODY would each read one
// of these three as already counted.
func TestObservationKeysOnTheOccurrence(t *testing.T) {
	set, err := metrics.NewDynamicMetricSet([]metrics.Dynamic{{
		Name: "event_lines_total", Type: metrics.CounterType, Value: "1",
	}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	// Two DIFFERENT objects with the SAME body; the repeat arrives on lap two.
	same := "BackOff pulling image"
	a := event("a", "BackOff", same, "Warning", "10", 1, now)
	b := event("b", "BackOff", same, "Warning", "11", 1, now)
	repeat := event("a", "BackOff", same, "Warning", "12", 2, now)
	client := newRedeliverClient(a, b)
	exp := &captureExporter{failN: 1000, err: errors.New("collector down")}
	r := New(Config{
		Client: client, Exporter: exp, BatchSize: 100, LogMetrics: set, Meta: fakeMeta{},
	})
	r.committed.ResourceVersion = "5" // a redelivering restart

	ctx := context.Background()
	if err := r.stream(ctx); err == nil {
		t.Fatal("the pre-stopped watch must end the stream")
	}
	r.tryFlush(ctx)
	// The watch restarts and re-delivers both, and object a has meanwhile been
	// re-sent with a growing count.
	client.events = []*corev1.Event{a, b, repeat}
	if err := r.stream(ctx); err == nil {
		t.Fatal("the pre-stopped watch must end the stream")
	}
	r.tryFlush(ctx)

	mexp := &metricCapture{}
	if err := set.Export(context.Background(), mexp, 0); err != nil {
		t.Fatal(err)
	}
	if got := mexp.sum("event_lines_total"); got != 3 {
		t.Errorf("event_lines_total = %v, want 3: two distinct events and one repeat are three "+
			"occurrences, however alike their bodies and whatever object they name", got)
	}
}

// An event carrying neither a UID nor a resourceVersion claims nothing: with no
// identity there is no positional proof, and under-claiming re-counts while
// over-claiming would suppress a genuine observation.
func TestUnidentifiableEventIsNeverClaimedObserved(t *testing.T) {
	r := New(Config{Client: fake.NewSimpleClientset(), Exporter: &captureExporter{}})
	if (obsKey{}).valid() {
		t.Fatal("an empty key must not be valid")
	}
	if r.wasObserved(obsKey{}) {
		t.Fatal("an empty key must never read as observed")
	}
	// Marking it must not put anything in the set either — a second entry with
	// the same empty key would otherwise be suppressed.
	r.cfg.LogMetrics = &metrics.DynamicMetricSet{}
	r.markObserved([]entry{{}, {}})
	if len(r.observed) != 0 {
		t.Fatalf("observed set holds %d keys for unidentifiable entries, want 0", len(r.observed))
	}
}

// The RELIST lap is the same defect by another route, and it is the worse one:
// a Gone watch replays the whole event TTL, so the batch dropped and rebuilt on
// every lap is the entire backlog rather than one flush window.
//
// The reader is armed exactly as expire leaves it — no committed
// resourceVersion, relist set, an hour-old watermark that filters nothing — and
// the collector is down, so no lap ever commits and each one re-lists, re-
// ingests and re-converts the same three events.
func TestRelistLapsObserveTheBacklogOnce(t *testing.T) {
	set, err := metrics.NewDynamicMetricSet([]metrics.Dynamic{{
		Name: "event_lines_total", Type: metrics.CounterType, Value: "1",
	}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	backlog := []corev1.Event{
		*event("a", "Pulled", "pulled image", "Normal", "20", 1, now),
		*event("b", "BackOff", "back-off restarting", "Warning", "21", 1, now),
		*event("c", "Killing", "stopping container", "Normal", "22", 1, now),
	}
	client := fake.NewSimpleClientset()
	client.PrependReactor("list", "events", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.EventList{
			ListMeta: metav1.ListMeta{ResourceVersion: "500"},
			Items:    backlog,
		}, nil
	})
	client.PrependWatchReactor("events", func(k8stesting.Action) (bool, watch.Interface, error) {
		w := watch.NewFake()
		w.Stop()
		return true, w, nil
	})

	exp := &captureExporter{failN: 1000, err: errors.New("collector down")}
	r := New(Config{
		Client: client, Exporter: exp, BatchSize: 100,
		FlushInterval: time.Millisecond, LogMetrics: set, Meta: fakeMeta{},
	})
	// Exactly what expire() leaves behind: no resume point, a relist armed, and
	// a watermark old enough that wanted() filters nothing out of the backlog.
	r.relist = true
	r.committed.Watermark = now.Add(-time.Hour)

	ctx := context.Background()
	const laps = 3
	for i := 0; i < laps; i++ {
		if err := r.stream(ctx); err == nil {
			t.Fatalf("lap %d: the pre-stopped watch must end the stream", i)
		}
		time.Sleep(2 * time.Millisecond) // past FlushInterval, so the next lap's tail flush is due
	}
	if got := len(exp.records()); got != 0 {
		t.Fatalf("delivered %d records, want 0: the collector is down for the whole test", got)
	}
	if !r.relist {
		t.Fatal("the relist must stay armed while nothing commits, or these laps are not relists")
	}

	mexp := &metricCapture{}
	if err := set.Export(context.Background(), mexp, 0); err != nil {
		t.Fatal(err)
	}
	if got := mexp.sum("event_lines_total"); got != 3 {
		t.Errorf("event_lines_total = %v after %d relist laps over the same 3 backlog events, want 3 — "+
			"a replayed backlog is the same delivery, not a new one", got, laps)
	}
}
