package otlpexport

import (
	"context"
	"errors"
	"testing"

	"github.com/JohanLindvall/diskqueue"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/obs"
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
	// The batch exceeds the whole 1-byte cap, which diskqueue reports as the
	// permanent ErrRecordTooLarge rather than the transient ErrFull; both are
	// refusals the producer sees and both count into BufferFull.
	if !errors.Is(err, diskqueue.ErrRecordTooLarge) {
		t.Fatalf("ExportMetrics err = %v, want ErrRecordTooLarge", err)
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
	if err := ms.add([]byte("definitely not a protobuf payload")); err != nil {
		t.Fatal(err)
	}
	if err := b.ExportMetrics(context.Background(), metricsWith("cpu")); err != nil {
		t.Fatal(err)
	}

	before := obs.BufferDroppedBatches.WithLabelValues("metrics").Value()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	// The good batch behind it still gets through (the queue must not wedge).
	waitFor(t, func() bool { return len(send.gotMetrics()) == 1 }, "the valid metric batch delivered")
	if got := obs.BufferDroppedBatches.WithLabelValues("metrics").Value() - before; got != 1 {
		t.Fatalf("BufferDropped{metrics} rose by %v, want 1: the corrupt batch drop must be counted", got)
	}
}

// logsWithN builds one payload of n records, bodies body0..bodyN-1.
func logsWithN(body string, n int) plog.Logs {
	ld := plog.NewLogs()
	sl := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	for i := 0; i < n; i++ {
		sl.LogRecords().AppendEmpty().Body().SetStr(body)
	}
	return ld
}

// A permanent rejection must count the RECORDS it discarded, not only the
// batch.
//
// This is the drop path CLAUDE.md, the README and docs/CONFIGURATION.md all
// point at as THE alert for the durable (-buffer-dir) configuration: with a
// spool the tailer's own export returns the enqueue verdict, so the
// collector's permanent rejections land here instead of on
// kubescrape_log_permanent_dropped_total — which counts records. A batch is
// 1..1024 records, so the recommended alert could say a loss happened and
// could not say whether it was one line or a thousand. The sink holds the
// decoded payload at the moment it drops it, so the count is free.
func TestPermanentRejectionCountsRecords(t *testing.T) {
	send := &errSender{errs: map[string]error{
		"poison": &HTTPStatusError{Code: 400, Body: "bad payload"},
	}}
	b, ls, ms := openBuffer(t, t.TempDir(), &send.fakeSender, 0)
	defer func() { _ = ls.Close(); _ = ms.Close() }()
	b.logs.send = send.ExportLogs

	beforeBatches := obs.BufferDroppedBatches.WithLabelValues("logs").Value()
	beforeRecords := obs.BufferDroppedRecords.WithLabelValues("logs").Value()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	if err := b.ExportLogs(ctx, logsWithN("poison", 7)); err != nil {
		t.Fatal(err)
	}
	if err := b.ExportLogs(ctx, logsWith("good")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		got := send.gotLogs()
		return len(got) == 1 && got[0] == "good"
	}, "good batch delivered past the poison one")

	if got := obs.BufferDroppedBatches.WithLabelValues("logs").Value() - beforeBatches; got != 1 {
		t.Fatalf("dropped batches = %v, want 1", got)
	}
	if got := obs.BufferDroppedRecords.WithLabelValues("logs").Value() - beforeRecords; got != 7 {
		t.Fatalf("dropped records = %v, want 7 — a batch is 1..1024 records, so the batch "+
			"counter alone cannot size the loss on the path operators are told to alert on", got)
	}
}

// Metrics count DATA POINTS, the unit that signal is measured in everywhere
// else (BatchPoints, the converter's chunking, DataPointCount).
func TestPermanentRejectionCountsMetricDataPoints(t *testing.T) {
	send := &fakeSender{}
	b, ls, ms := openBuffer(t, t.TempDir(), send, 0)
	defer func() { _ = ls.Close(); _ = ms.Close() }()
	b.metrics.send = func(context.Context, pmetric.Metrics) error {
		return &HTTPStatusError{Code: 400, Body: "bad payload"}
	}

	// The shape is deliberately asymmetric — 2 resources / 2 scopes / 2 metrics
	// / 5 data points — so the assertion can only be satisfied by counting
	// POINTS. SetEmptyGauge installs a fresh Gauge, so it must stay out of the
	// append loop: calling it per point left this fixture at one point of
	// everything, where a resource, scope, metric or per-batch count all read
	// the same and the unit this test exists to pin was unpinned.
	md := pmetric.NewMetrics()
	cpu := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	cpu.SetName("cpu")
	cpuPoints := cpu.SetEmptyGauge().DataPoints()
	for i := 0; i < 3; i++ {
		cpuPoints.AppendEmpty()
	}
	mem := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	mem.SetName("mem")
	memPoints := mem.SetEmptyGauge().DataPoints()
	for i := 0; i < 2; i++ {
		memPoints.AppendEmpty()
	}
	const want = 5.0
	if got := md.DataPointCount(); got != int(want) {
		t.Fatalf("fixture builds %d data points, want %v", got, want)
	}

	beforeRecords := obs.BufferDroppedRecords.WithLabelValues("metrics").Value()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	if err := b.ExportMetrics(ctx, md); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return ms.Bytes() == 0 }, "poison metric batch dropped from the spool")
	if got := obs.BufferDroppedRecords.WithLabelValues("metrics").Value() - beforeRecords; got != want {
		t.Fatalf("dropped metric data points = %v, want %v", got, want)
	}
}
