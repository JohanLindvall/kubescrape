package otlpexport

import (
	"context"
	"errors"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/spool"
)

// TestFullSpoolCountsMetricDrop: a producer that cannot rewind (the scraper,
// the self-metrics registry) loses the batch when the spool is full. The loss
// must be counted, not merely returned as an error the caller logs.
func TestFullSpoolCountsMetricDrop(t *testing.T) {
	send := &fakeSender{}
	// A cap of a single byte: no frame fits, so every Append is ErrFull.
	b, ls, ms := openBuffer(t, t.TempDir(), send, 1)
	defer func() { _ = ls.Close() }()
	defer func() { _ = ms.Close() }()

	before := obs.BufferFull.WithLabelValues("metrics").Value()
	err := b.ExportMetrics(context.Background(), metricsWith("cpu"))
	if !errors.Is(err, spool.ErrFull) {
		t.Fatalf("ExportMetrics err = %v, want ErrFull", err)
	}
	if got := obs.BufferFull.WithLabelValues("metrics").Value() - before; got != 1 {
		t.Fatalf("BufferFull{metrics} rose by %v, want 1: a dropped metric batch must be counted", got)
	}
}

// TestCorruptBatchDropIsCounted: an undecodable frame is dropped and the queue
// advances past it. That is a real data loss and must land in BufferDropped.
func TestCorruptBatchDropIsCounted(t *testing.T) {
	send := &fakeSender{}
	dir := t.TempDir()
	b, ls, ms := openBuffer(t, dir, send, 0)
	defer func() { _ = ls.Close() }()
	defer func() { _ = ms.Close() }()

	// Append a frame the pmetric unmarshaler cannot decode.
	if err := ms.Append([]byte("definitely not a protobuf payload")); err != nil {
		t.Fatal(err)
	}
	if err := b.ExportMetrics(context.Background(), metricsWith("cpu")); err != nil {
		t.Fatal(err)
	}

	before := obs.BufferDropped.WithLabelValues("metrics").Value()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	// The good batch behind it still gets through (the queue must not wedge).
	waitFor(t, func() bool { return len(send.gotMetrics()) == 1 }, "the valid metric batch delivered")
	if got := obs.BufferDropped.WithLabelValues("metrics").Value() - before; got != 1 {
		t.Fatalf("BufferDropped{metrics} rose by %v, want 1: the corrupt batch drop must be counted", got)
	}
}
