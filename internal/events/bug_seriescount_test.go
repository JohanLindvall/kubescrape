package events

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// TestEventsSeriesCountAttribute is the regression test for k8s.event.count
// going missing on modern events.k8s.io/v1 recurrences: those keep the legacy
// Count at zero and carry occurrence multiplicity in Series.Count. convert()
// read only ev.Count, so the exported record had no count attribute at all.
func TestEventsSeriesCountAttribute(t *testing.T) {
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
	newer.Series = &corev1.EventSeries{Count: 7, LastObservedTime: metav1.MicroTime{Time: now.Add(time.Second)}}
	e.OnUpdate(old, newer)

	deadline := time.Now().Add(3 * time.Second)
	for exp.records() < 1 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	var count int64 = -1
	exp.mu.Lock()
	for _, b := range exp.batches {
		for i := 0; i < b.ResourceLogs().Len(); i++ {
			lrs := b.ResourceLogs().At(i).ScopeLogs().At(0).LogRecords()
			for j := 0; j < lrs.Len(); j++ {
				if v, ok := lrs.At(j).Attributes().Get("k8s.event.count"); ok {
					count = v.Int()
				}
			}
		}
	}
	exp.mu.Unlock()
	if count != 7 {
		t.Fatalf("k8s.event.count = %d, want 7 (from Series.Count); a modern series event lost its occurrence count", count)
	}
}

// An event enqueued AFTER shutdown closed the exporter must be counted as a
// drop, never parked silently in the channel forever.
func TestEnqueueAfterShutdownCountsDrop(t *testing.T) {
	exp := &capture{}
	e := New(Config{Store: store.New(time.Minute), Exporter: exp, FlushInterval: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); e.Run(ctx) }()
	cancel()
	<-done

	before := obs.EventsDropped.Value()
	e.OnAdd(testEvent("Late", "Pod", "p", "u", time.Now().Add(time.Second)))
	if got := obs.EventsDropped.Value() - before; got != 1 {
		t.Fatalf("post-shutdown enqueue dropped delta = %v, want 1 (counted, not silent)", got)
	}
}
