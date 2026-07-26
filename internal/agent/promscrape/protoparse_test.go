package promscrape

import (
	"context"
	"encoding/binary"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func protoBody(t *testing.T, families ...*dto.MetricFamily) []byte {
	t.Helper()
	var out []byte
	var lenBuf [binary.MaxVarintLen64]byte
	for _, mf := range families {
		b, err := proto.Marshal(mf)
		if err != nil {
			t.Fatal(err)
		}
		n := binary.PutUvarint(lenBuf[:], uint64(len(b)))
		out = append(out, lenBuf[:n]...)
		out = append(out, b...)
	}
	return out
}

func ptr[T any](v T) *T { return &v }

// A native histogram converts to an OTLP exponential histogram: scale =
// schema, zero bucket carried, span/delta buckets decoded to dense counts
// with the right offset; classic families in the same exposition convert as
// usual.
func TestNativeHistogramScrape(t *testing.T) {
	nh := &dto.MetricFamily{
		Name: ptr("rpc_latency_seconds"),
		Type: dto.MetricType_HISTOGRAM.Enum(),
		Metric: []*dto.Metric{{
			Label: []*dto.LabelPair{{Name: ptr("svc"), Value: ptr("a")}},
			Histogram: &dto.Histogram{
				SampleCount:   ptr(uint64(10)),
				SampleSum:     ptr(3.5),
				Schema:        ptr(int32(3)),
				ZeroThreshold: ptr(1e-9),
				ZeroCount:     ptr(uint64(2)),
				// Buckets at indexes 1,2 then a gap of 2, then 5: counts 3,2,1.
				PositiveSpan: []*dto.BucketSpan{
					{Offset: ptr(int32(1)), Length: ptr(uint32(2))},
					{Offset: ptr(int32(2)), Length: ptr(uint32(1))},
				},
				PositiveDelta: []int64{3, -1, -1},
			},
		}},
	}
	classic := &dto.MetricFamily{
		Name:   ptr("http_requests_total"),
		Type:   dto.MetricType_COUNTER.Enum(),
		Metric: []*dto.Metric{{Counter: &dto.Counter{Value: ptr(7.0)}}},
	}
	body := protoBody(t, nh, classic)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if acc := r.Header.Get("Accept"); acc == "" || acc[:20] != "application/vnd.goog" {
			t.Errorf("Accept = %q", acc)
		}
		w.Header().Set("Content-Type", "application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited")
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	exp := &captureExporter{}
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		NativeHistograms: true,
		Targets:          staticTargets{testTarget(srv.URL)},
		Exporter:         exp, StartTime: time.Now(),
	})
	s.cycle(context.Background())

	var expHist pmetric.Metric
	var counter pmetric.Metric
	for _, md := range exp.batches {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			ms := rms.At(i).ScopeMetrics().At(0).Metrics()
			for j := 0; j < ms.Len(); j++ {
				switch ms.At(j).Name() {
				case "rpc_latency_seconds":
					expHist = ms.At(j)
				case "http_requests_total":
					counter = ms.At(j)
				}
			}
		}
	}
	if expHist.Type() != pmetric.MetricTypeExponentialHistogram {
		t.Fatalf("native histogram type = %v", expHist.Type())
	}
	dp := expHist.ExponentialHistogram().DataPoints().At(0)
	if dp.Scale() != 3 || dp.ZeroCount() != 2 || dp.Count() != 10 || dp.Sum() != 3.5 {
		t.Fatalf("dp: scale=%d zero=%d count=%d sum=%v", dp.Scale(), dp.ZeroCount(), dp.Count(), dp.Sum())
	}
	// Spans: indexes 1,2 (counts 3,2), gap 2 (zeros), index 5 (count 1).
	// OTLP offset = first index - 1 = 0.
	if dp.Positive().Offset() != 0 {
		t.Fatalf("offset = %d", dp.Positive().Offset())
	}
	want := []uint64{3, 2, 0, 0, 1}
	got := dp.Positive().BucketCounts().AsRaw()
	if len(got) != len(want) {
		t.Fatalf("buckets = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("buckets = %v, want %v", got, want)
		}
	}
	if v, _ := dp.Attributes().Get("svc"); v.Str() != "a" {
		t.Fatal("labels lost")
	}
	if counter.Type() != pmetric.MetricTypeSum || counter.Sum().DataPoints().At(0).DoubleValue() != 7 {
		t.Fatalf("classic counter in the same exposition: %v", counter.Type())
	}
}

// The protobuf path must carry exemplars exactly as the OpenMetrics text path
// does, under the SAME -scrape-exemplars gate: -scrape-native-histograms makes
// the Accept header prefer protobuf, so a target honouring it would otherwise
// lose every exemplar and the two flags would be mutually exclusive in
// practice. HELP/UNIT ride along on the same messages.
func TestProtoExemplarsAndDescriptions(t *testing.T) {
	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	const spanID = "00f067aa0ba902b7"
	exTime := time.Unix(1700000000, 0).UTC()
	counter := &dto.MetricFamily{
		Name: ptr("http_requests_total"), Help: ptr("Total requests."), Unit: ptr("requests"),
		Type: dto.MetricType_COUNTER.Enum(),
		Metric: []*dto.Metric{{
			Counter: &dto.Counter{Value: ptr(7.0), Exemplar: &dto.Exemplar{
				Label: []*dto.LabelPair{
					{Name: ptr("trace_id"), Value: ptr(traceID)},
					{Name: ptr("span_id"), Value: ptr(spanID)},
					{Name: ptr("shard"), Value: ptr("b")},
				},
				Value:     ptr(1.0),
				Timestamp: timestamppb.New(exTime),
			}},
		}},
	}
	hist := &dto.MetricFamily{
		Name: ptr("rpc_latency_seconds"), Help: ptr("RPC latency."), Unit: ptr("seconds"),
		Type: dto.MetricType_HISTOGRAM.Enum(),
		Metric: []*dto.Metric{{
			Histogram: &dto.Histogram{
				SampleCount: ptr(uint64(3)), SampleSum: ptr(0.6),
				Bucket: []*dto.Bucket{
					{UpperBound: ptr(0.5), CumulativeCount: ptr(uint64(2)), Exemplar: &dto.Exemplar{
						Label: []*dto.LabelPair{{Name: ptr("trace_id"), Value: ptr(traceID)}},
						Value: ptr(0.3),
					}},
					{UpperBound: ptr(math.Inf(1)), CumulativeCount: ptr(uint64(3))},
				},
			},
		}},
	}
	body := protoBody(t, counter, hist)

	run := func(t *testing.T, exemplars bool) map[string]pmetric.Metric {
		t.Helper()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.google.protobuf; encoding=delimited")
			_, _ = w.Write(body)
		}))
		t.Cleanup(srv.Close)
		exp := &captureExporter{}
		s := New(Config{
			Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
			NativeHistograms: true, Exemplars: exemplars,
			Targets:  staticTargets{testTarget(srv.URL)},
			Exporter: exp, StartTime: time.Now(),
		})
		s.cycle(context.Background())
		out := map[string]pmetric.Metric{}
		for _, md := range exp.batches {
			rms := md.ResourceMetrics()
			for i := 0; i < rms.Len(); i++ {
				ms := rms.At(i).ScopeMetrics().At(0).Metrics()
				for j := 0; j < ms.Len(); j++ {
					out[ms.At(j).Name()] = ms.At(j)
				}
			}
		}
		return out
	}

	got := run(t, true)
	c, ok := got["http_requests_total"]
	if !ok {
		t.Fatalf("counter missing: %v", got)
	}
	if c.Description() != "Total requests." || c.Unit() != "requests" {
		t.Errorf("counter description/unit = %q/%q", c.Description(), c.Unit())
	}
	cex := c.Sum().DataPoints().At(0).Exemplars()
	if cex.Len() != 1 {
		t.Fatalf("counter exemplars = %d, want 1", cex.Len())
	}
	e := cex.At(0)
	if e.TraceID().String() != traceID || e.SpanID().String() != spanID {
		t.Errorf("exemplar ids = %s/%s, want %s/%s", e.TraceID(), e.SpanID(), traceID, spanID)
	}
	if e.DoubleValue() != 1 {
		t.Errorf("exemplar value = %v, want 1", e.DoubleValue())
	}
	if e.Timestamp().AsTime().UTC() != exTime {
		t.Errorf("exemplar timestamp = %v, want %v", e.Timestamp().AsTime().UTC(), exTime)
	}
	if v, ok := e.FilteredAttributes().Get("shard"); !ok || v.Str() != "b" {
		t.Errorf("exemplar labels beyond trace/span must become filtered attributes: %v", e.FilteredAttributes().AsRaw())
	}

	h, ok := got["rpc_latency_seconds"]
	if !ok {
		t.Fatalf("histogram missing: %v", got)
	}
	if h.Description() != "RPC latency." || h.Unit() != "seconds" {
		t.Errorf("histogram description/unit = %q/%q", h.Description(), h.Unit())
	}
	hex := h.Histogram().DataPoints().At(0).Exemplars()
	if hex.Len() != 1 {
		t.Fatalf("histogram bucket exemplars = %d, want 1", hex.Len())
	}
	if hex.At(0).DoubleValue() != 0.3 || hex.At(0).TraceID().String() != traceID {
		t.Errorf("bucket exemplar = %v/%s", hex.At(0).DoubleValue(), hex.At(0).TraceID())
	}
	// A bucket exemplar with no timestamp falls back to the scrape time, never 0.
	if hex.At(0).Timestamp() == 0 {
		t.Error("timestamp-less exemplar must fall back to the scrape time")
	}

	// The gate: with -scrape-exemplars off, nothing is attached.
	off := run(t, false)
	if n := off["http_requests_total"].Sum().DataPoints().At(0).Exemplars().Len(); n != 0 {
		t.Errorf("counter exemplars with the flag off = %d", n)
	}
	if n := off["rpc_latency_seconds"].Histogram().DataPoints().At(0).Exemplars().Len(); n != 0 {
		t.Errorf("histogram exemplars with the flag off = %d", n)
	}
}

// A native histogram's exemplars live on the Histogram message rather than on
// buckets; the exponential point must carry them too.
func TestProtoNativeHistogramExemplars(t *testing.T) {
	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	nh := &dto.MetricFamily{
		Name: ptr("rpc_latency_seconds"), Help: ptr("RPC latency."), Unit: ptr("seconds"),
		Type: dto.MetricType_HISTOGRAM.Enum(),
		Metric: []*dto.Metric{{
			Histogram: &dto.Histogram{
				SampleCount: ptr(uint64(4)), SampleSum: ptr(1.5),
				Schema: ptr(int32(2)), ZeroThreshold: ptr(1e-9), ZeroCount: ptr(uint64(1)),
				PositiveSpan:  []*dto.BucketSpan{{Offset: ptr(int32(1)), Length: ptr(uint32(1))}},
				PositiveDelta: []int64{3},
				Exemplars: []*dto.Exemplar{{
					Label: []*dto.LabelPair{{Name: ptr("trace_id"), Value: ptr(traceID)}},
					Value: ptr(0.25),
				}},
			},
		}},
	}
	body := protoBody(t, nh)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.google.protobuf; encoding=delimited")
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	exp := &captureExporter{}
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		NativeHistograms: true, Exemplars: true,
		Targets:  staticTargets{testTarget(srv.URL)},
		Exporter: exp, StartTime: time.Now(),
	})
	s.cycle(context.Background())

	m := exp.batches[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
	if m.Type() != pmetric.MetricTypeExponentialHistogram {
		t.Fatalf("type = %v", m.Type())
	}
	if m.Description() != "RPC latency." || m.Unit() != "seconds" {
		t.Errorf("description/unit = %q/%q", m.Description(), m.Unit())
	}
	ex := m.ExponentialHistogram().DataPoints().At(0).Exemplars()
	if ex.Len() != 1 || ex.At(0).TraceID().String() != traceID || ex.At(0).DoubleValue() != 0.25 {
		t.Fatalf("native histogram exemplars = %d", ex.Len())
	}
}

// Span decoding rejects hostile shapes instead of allocating unbounded
// buckets or wrapping counts.
func TestDecodeSpansGuards(t *testing.T) {
	// Gap past the cap.
	_, _, ok := decodeSpans([]*dto.BucketSpan{
		{Offset: ptr(int32(0)), Length: ptr(uint32(1))},
		{Offset: ptr(int32(100000)), Length: ptr(uint32(1))},
	}, []int64{1, 0})
	if ok {
		t.Fatal("unbounded gap accepted")
	}
	// Negative running count.
	_, _, ok = decodeSpans([]*dto.BucketSpan{{Offset: ptr(int32(0)), Length: ptr(uint32(2))}}, []int64{1, -5})
	if ok {
		t.Fatal("negative count accepted")
	}
	// Missing deltas.
	_, _, ok = decodeSpans([]*dto.BucketSpan{{Offset: ptr(int32(0)), Length: ptr(uint32(3))}}, []int64{1})
	if ok {
		t.Fatal("missing deltas accepted")
	}
}

// The protobuf path must count each classic sample ONCE (emit counts it via
// the shared counter; the family must not also add it to c.samples). A
// double-count inflated scrape_samples_scraped and halved MaxSamples.
func TestProtoClassicSampleCountAccurate(t *testing.T) {
	c := &dto.Counter{Value: ptr(1.0)}
	g1 := &dto.Gauge{Value: ptr(2.0)}
	g2 := &dto.Gauge{Value: ptr(3.0)}
	fams := []*dto.MetricFamily{
		{Name: ptr("reqs_total"), Type: dto.MetricType_COUNTER.Enum(), Metric: []*dto.Metric{{Counter: c}}},
		{Name: ptr("temp"), Type: dto.MetricType_GAUGE.Enum(), Metric: []*dto.Metric{{Gauge: g1}, {Gauge: g2}}},
	}
	body := protoBody(t, fams...)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.google.protobuf; encoding=delimited")
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	exp := &captureExporter{}
	s := New(Config{Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		NativeHistograms: true, Targets: staticTargets{testTarget(srv.URL)}, Exporter: exp, StartTime: time.Now()})

	// 3 real samples (1 counter + 2 gauges); scrapeProto reports the count.
	got, err := s.scrapeProto(context.Background(), strings.NewReader(string(body)), newBatcher(func(pcommon.Resource) {}, time.Now(), time.Now()), nil, "t")
	if err != nil {
		t.Fatal(err)
	}
	if got != 3 {
		t.Fatalf("proto sample count = %d, want 3 (double-counted?)", got)
	}
}
