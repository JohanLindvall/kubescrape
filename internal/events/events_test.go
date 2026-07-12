package events

import (
	"context"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

type capture struct {
	mu      sync.Mutex
	batches []plog.Logs
}

func (c *capture) ExportLogs(_ context.Context, ld plog.Logs) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.batches = append(c.batches, ld)
	return nil
}

func (c *capture) records() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, b := range c.batches {
		n += b.LogRecordCount()
	}
	return n
}

func testEvent(reason, kind, name string, uid string, when time.Time) *corev1.Event {
	return &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name: reason + "-" + name, Namespace: "ns1",
			CreationTimestamp: metav1.Time{Time: when},
		},
		InvolvedObject: corev1.ObjectReference{
			Kind: kind, Namespace: "ns1", Name: name, UID: types.UID(uid),
		},
		Reason:        reason,
		Message:       "message for " + name,
		Type:          corev1.EventTypeWarning,
		LastTimestamp: metav1.Time{Time: when},
		Count:         1,
	}
}

func testStoreWithPod(t *testing.T) *store.Store {
	t.Helper()
	st := store.New(time.Minute)
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod1", Namespace: "ns1", UID: "pod-uid", ResourceVersion: "1",
		},
		Spec: corev1.PodSpec{NodeName: "node1"},
	})
	return st
}

func TestEventsExport(t *testing.T) {
	exp := &capture{}
	e := New(Config{Store: testStoreWithPod(t), Exporter: exp, BatchSize: 10, FlushInterval: 20 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.Run(ctx)

	now := time.Now().Add(time.Second)
	e.OnAdd(testEvent("BackOff", "Pod", "pod1", "pod-uid", now))
	e.OnAdd(testEvent("FailedScheduling", "Deployment", "dep1", "dep-uid", now))

	deadline := time.Now().Add(3 * time.Second)
	for exp.records() < 2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if exp.records() != 2 {
		t.Fatalf("records = %d", exp.records())
	}

	// Find the pod-attributed record and the generic one.
	var podAttrs, depAttrs map[string]any
	exp.mu.Lock()
	for _, b := range exp.batches {
		for i := 0; i < b.ResourceLogs().Len(); i++ {
			rl := b.ResourceLogs().At(i)
			raw := rl.Resource().Attributes().AsRaw()
			if raw["k8s.pod.name"] == "pod1" {
				podAttrs = raw
			}
			if raw["k8s.object.kind"] == "Deployment" {
				depAttrs = raw
			}
			lr := rl.ScopeLogs().At(0).LogRecords().At(0)
			if lr.SeverityText() != "Warning" {
				t.Errorf("severity = %q", lr.SeverityText())
			}
			if v, _ := lr.Attributes().Get("k8s.event.reason"); v.Str() == "" {
				t.Error("missing k8s.event.reason")
			}
		}
	}
	exp.mu.Unlock()
	if podAttrs == nil || podAttrs["k8s.pod.uid"] != "pod-uid" {
		t.Fatalf("pod event not enriched: %v", podAttrs)
	}
	if depAttrs == nil || depAttrs["k8s.deployment.name"] != "dep1" {
		t.Fatalf("deployment event attrs: %v", depAttrs)
	}
}

func TestEventsSkipHistory(t *testing.T) {
	exp := &capture{}
	e := New(Config{Store: store.New(time.Minute), Exporter: exp, FlushInterval: 20 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.Run(ctx)

	// Events from before the exporter started (the informer's initial list)
	// must be skipped.
	e.OnAdd(testEvent("Old", "Pod", "old", "u", time.Now().Add(-time.Hour)))
	time.Sleep(100 * time.Millisecond)
	if exp.records() != 0 {
		t.Fatalf("historical events exported: %d", exp.records())
	}
}

func TestEventsCountIncrement(t *testing.T) {
	exp := &capture{}
	e := New(Config{Store: store.New(time.Minute), Exporter: exp, FlushInterval: 20 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.Run(ctx)

	now := time.Now().Add(time.Second)
	old := testEvent("BackOff", "Pod", "p", "u", now)
	newer := testEvent("BackOff", "Pod", "p", "u", now.Add(time.Second))
	newer.Count = 2
	e.OnUpdate(old, newer)   // count bump -> export
	e.OnUpdate(newer, newer) // no change -> no export

	deadline := time.Now().Add(3 * time.Second)
	for exp.records() < 1 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if exp.records() != 1 {
		t.Fatalf("records = %d", exp.records())
	}
}

// Recurrences recorded through events.k8s.io/v1 bump series.count while the
// legacy count/lastTimestamp stay zero — they must re-emit too.
func TestEventsSeriesIncrement(t *testing.T) {
	exp := &capture{}
	e := New(Config{Store: store.New(time.Minute), Exporter: exp, FlushInterval: 20 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.Run(ctx)

	now := time.Now().Add(time.Second)
	old := testEvent("FailedScheduling", "Pod", "p", "u", now)
	old.Count = 0
	old.LastTimestamp = metav1.Time{}
	old.Series = &corev1.EventSeries{Count: 1, LastObservedTime: metav1.MicroTime{Time: now}}
	newer := testEvent("FailedScheduling", "Pod", "p", "u", now)
	newer.Count = 0
	newer.LastTimestamp = metav1.Time{}
	newer.Series = &corev1.EventSeries{Count: 5, LastObservedTime: metav1.MicroTime{Time: now.Add(time.Second)}}

	e.OnUpdate(old, newer) // series bump -> export
	e.OnUpdate(newer, newer)

	deadline := time.Now().Add(3 * time.Second)
	for exp.records() < 1 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if exp.records() != 1 {
		t.Fatalf("records = %d, want 1 (series recurrence must re-emit)", exp.records())
	}
}

// On shutdown the pending batch is flushed before Run returns — events
// arriving between the last tick and cancellation must not be lost.
func TestEventsFinalFlushOnShutdown(t *testing.T) {
	exp := &capture{}
	// A long flush interval guarantees the ticker never fires; only the final
	// flush can export.
	e := New(Config{Store: store.New(time.Minute), Exporter: exp, FlushInterval: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); e.Run(ctx) }()

	e.OnAdd(testEvent("Pending", "Pod", "p1", "u1", time.Now().Add(time.Second)))
	e.OnAdd(testEvent("Pending", "Pod", "p2", "u2", time.Now().Add(time.Second)))
	// Let Run pick the events up (or leave them queued — the drain covers both).
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done
	if exp.records() != 2 {
		t.Fatalf("records after shutdown = %d, want 2 (final flush)", exp.records())
	}
}

// Queue-full drops are counted (and the warn is rate-limited rather than
// per-event, which this test exercises by volume).
func TestEventsQueueFullDropsCounted(t *testing.T) {
	exp := &capture{}
	e := New(Config{Store: store.New(time.Minute), Exporter: exp, BatchSize: 1})
	// No Run goroutine: the channel (cap 4*BatchSize = 4) fills, the rest drop.
	before := obs.EventsDropped.Value()
	when := time.Now().Add(time.Second)
	for i := 0; i < 10; i++ {
		e.OnAdd(testEvent("Flood", "Pod", "p", "u", when))
	}
	if got := obs.EventsDropped.Value() - before; got != 6 {
		t.Fatalf("dropped = %v, want 6 (10 enqueued, queue holds 4)", got)
	}
}

// UID-less events for different objects must not collapse into one resource
// (the second event would inherit the first object's attributes).
func TestEventsUIDLessDistinctObjects(t *testing.T) {
	exp := &capture{}
	e := New(Config{Store: store.New(time.Minute), Exporter: exp, FlushInterval: 20 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.Run(ctx)

	when := time.Now().Add(time.Second)
	e.OnAdd(testEvent("R1", "Foo", "a", "", when))
	e.OnAdd(testEvent("R2", "Bar", "b", "", when))

	deadline := time.Now().Add(3 * time.Second)
	for exp.records() < 2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	exp.mu.Lock()
	defer exp.mu.Unlock()
	kinds := map[string]bool{}
	for _, b := range exp.batches {
		rls := b.ResourceLogs()
		for i := 0; i < rls.Len(); i++ {
			if v, ok := rls.At(i).Resource().Attributes().Get("k8s.object.kind"); ok {
				kinds[v.Str()] = true
			}
		}
	}
	if !kinds["Foo"] || !kinds["Bar"] {
		t.Fatalf("resource kinds = %v, want both Foo and Bar", kinds)
	}
}
