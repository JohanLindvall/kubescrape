package promscrape

import (
	"context"
	"encoding/binary"
	"maps"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func protoBody(t testing.TB, families ...*dto.MetricFamily) []byte {
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
	}, []int64{1, 0}, nil)
	if ok {
		t.Fatal("unbounded gap accepted")
	}
	// Negative running count.
	_, _, ok = decodeSpans([]*dto.BucketSpan{{Offset: ptr(int32(0)), Length: ptr(uint32(2))}}, []int64{1, -5}, nil)
	if ok {
		t.Fatal("negative count accepted")
	}
	// Missing deltas.
	_, _, ok = decodeSpans([]*dto.BucketSpan{{Offset: ptr(int32(0)), Length: ptr(uint32(3))}}, []int64{1}, nil)
	if ok {
		t.Fatal("missing deltas accepted")
	}
}

// Span indexes are the TARGET's int32 offsets and uint32 lengths, and in int32
// arithmetic they wrap: a running index driven past MaxInt32 came back as a
// small (or negative) one, so the gap check — computed from the wrapped values
// — saw a legal gap and the point shipped with buckets at a mathematically
// impossible index. The guard's job is to REFUSE hostile shapes, so leaving
// int32 range is rejected rather than folded back into it.
func TestDecodeSpansRejectsIndexOverflow(t *testing.T) {
	if _, _, ok := decodeSpans([]*dto.BucketSpan{
		{Offset: ptr(int32(math.MaxInt32)), Length: ptr(uint32(1))},
		{Offset: ptr(int32(2)), Length: ptr(uint32(1))},
	}, []int64{1, 1}, nil); ok {
		t.Fatal("span index past MaxInt32 accepted: the gap guard was bypassed by the wrap")
	}
	// The OTLP offset is the first index MINUS ONE, which underflows on its own.
	if _, _, ok := decodeSpans([]*dto.BucketSpan{
		{Offset: ptr(int32(math.MinInt32)), Length: ptr(uint32(1))},
	}, []int64{1}, nil); ok {
		t.Fatal("span index at MinInt32 accepted: offset-1 underflows to a positive index")
	}
}

// A FLOAT native histogram (spans plus absolute positive_count, no deltas) is
// well formed — Prometheus parses it into a FloatHistogram. Reading only the
// delta encoding made it "malformed" and dropped the family WHOLE: no buckets,
// no count, no sum, no classic fallback.
func TestFloatNativeHistogramConverts(t *testing.T) {
	fam := &dto.MetricFamily{
		Name: ptr("rpc_latency_seconds"),
		Type: dto.MetricType_HISTOGRAM.Enum(),
		Metric: []*dto.Metric{{
			Histogram: &dto.Histogram{
				SampleCountFloat: ptr(9.0),
				SampleSum:        ptr(2.25),
				Schema:           ptr(int32(2)),
				ZeroThreshold:    ptr(1e-9),
				ZeroCountFloat:   ptr(2.0),
				PositiveSpan:     []*dto.BucketSpan{{Offset: ptr(int32(1)), Length: ptr(uint32(3))}},
				PositiveCount:    []float64{4, 2, 1}, // absolute, not deltas
			},
		}},
	}
	got := scrapeProtoFamilies(t, fam)
	m, ok := got["rpc_latency_seconds"]
	if !ok {
		t.Fatalf("the family was dropped whole; exported %v", slices.Sorted(maps.Keys(got)))
	}
	if m.Type() != pmetric.MetricTypeExponentialHistogram {
		t.Fatalf("float native histogram type = %v", m.Type())
	}
	dp := m.ExponentialHistogram().DataPoints().At(0)
	if dp.Count() != 9 || dp.Sum() != 2.25 || dp.ZeroCount() != 2 || dp.Scale() != 2 {
		t.Fatalf("dp: count=%d sum=%v zero=%d scale=%d", dp.Count(), dp.Sum(), dp.ZeroCount(), dp.Scale())
	}
	if got, want := dp.Positive().BucketCounts().AsRaw(), []uint64{4, 2, 1}; !slices.Equal(got, want) {
		t.Fatalf("buckets = %v, want %v (absolute float counts, not delta-decoded)", got, want)
	}
	if dp.Positive().Offset() != 0 {
		t.Fatalf("offset = %d, want the first index minus one", dp.Positive().Offset())
	}
}

// An exporter that materialises default fields sends a CLASSIC histogram whose
// schema/zero_threshold/zero_count are all present and all zero. A presence
// test read that as an empty NATIVE histogram: the classic buckets never
// converted and the exported point carried count and sum with no buckets at
// all. Detection is on the VALUES, exactly as Prometheus decides it.
func TestClassicHistogramWithZeroValuedNativeFields(t *testing.T) {
	fam := &dto.MetricFamily{
		Name: ptr("rpc_latency_seconds"),
		Type: dto.MetricType_HISTOGRAM.Enum(),
		Metric: []*dto.Metric{{
			Histogram: &dto.Histogram{
				SampleCount:   ptr(uint64(4)),
				SampleSum:     ptr(1.5),
				Schema:        ptr(int32(0)),
				ZeroThreshold: ptr(0.0),
				ZeroCount:     ptr(uint64(0)),
				Bucket: []*dto.Bucket{
					{UpperBound: ptr(0.5), CumulativeCount: ptr(uint64(3))},
					{UpperBound: ptr(math.Inf(1)), CumulativeCount: ptr(uint64(4))},
				},
			},
		}},
	}
	got := scrapeProtoFamilies(t, fam)
	m, ok := got["rpc_latency_seconds"]
	if !ok {
		t.Fatalf("the family was not exported at all; got %v", slices.Sorted(maps.Keys(got)))
	}
	if m.Type() != pmetric.MetricTypeHistogram {
		t.Fatalf("type = %v, want a classic histogram: zero-VALUED native fields are not native data", m.Type())
	}
	dp := m.Histogram().DataPoints().At(0)
	if got, want := dp.BucketCounts().AsRaw(), []uint64{3, 1}; !slices.Equal(got, want) {
		t.Fatalf("buckets = %v, want %v", got, want)
	}
	if dp.Count() != 4 || dp.Sum() != 1.5 {
		t.Fatalf("dp: count=%d sum=%v", dp.Count(), dp.Sum())
	}
}

// A native histogram with no observations declares itself with a zero-length
// no-op span; the value-based detection must still find it, or the ecosystem's
// own convention breaks.
func TestNoOpSpanIsStillNative(t *testing.T) {
	h := &dto.Histogram{
		SampleCount:  ptr(uint64(0)),
		Schema:       ptr(int32(3)),
		PositiveSpan: []*dto.BucketSpan{{Offset: ptr(int32(0)), Length: ptr(uint32(0))}},
	}
	if !isNative(h) {
		t.Fatal("a no-op span must mark a histogram native")
	}
}

// GAUGE_HISTOGRAM buckets are a CURRENT snapshot and may decrease, so the
// cumulative monotonic histogram the HISTOGRAM path builds misdeclares their
// temporality — and the text front degrades the same family to gauges, so the
// two formats disagreed about one target. Both ship gauges now.
func TestGaugeHistogramConvertsToGauges(t *testing.T) {
	fam := &dto.MetricFamily{
		Name: ptr("cache_entries"),
		Type: dto.MetricType_GAUGE_HISTOGRAM.Enum(),
		Metric: []*dto.Metric{{
			Histogram: &dto.Histogram{
				SampleCount: ptr(uint64(4)),
				SampleSum:   ptr(1.5),
				Bucket: []*dto.Bucket{
					{UpperBound: ptr(0.5), CumulativeCount: ptr(uint64(3))},
					{UpperBound: ptr(math.Inf(1)), CumulativeCount: ptr(uint64(4))},
				},
			},
		}},
	}
	got := scrapeProtoFamilies(t, fam)
	if m, ok := got["cache_entries"]; ok {
		t.Fatalf("gauge histogram exported as %v: OTLP has no gauge-histogram shape", m.Type())
	}
	for _, name := range []string{"cache_entries_bucket", "cache_entries_gsum", "cache_entries_gcount"} {
		m, ok := got[name]
		if !ok {
			t.Fatalf("%s missing; got %v", name, slices.Sorted(maps.Keys(got)))
		}
		if m.Type() != pmetric.MetricTypeGauge {
			t.Fatalf("%s type = %v, want a gauge", name, m.Type())
		}
	}
	if v := got["cache_entries_gcount"].Gauge().DataPoints().At(0).DoubleValue(); v != 4 {
		t.Fatalf("_gcount = %v, want 4", v)
	}
	if got["cache_entries_bucket"].Gauge().DataPoints().Len() != 2 {
		t.Fatal("bucket rows lost")
	}
}

// scrapeProtoFamilies runs one protobuf exposition through a real scrape and
// returns the exported metrics by name.
func scrapeProtoFamilies(t *testing.T, families ...*dto.MetricFamily) map[string]pmetric.Metric {
	t.Helper()
	body := protoBody(t, families...)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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

	out := map[string]pmetric.Metric{}
	for _, md := range exp.batches {
		rms := md.ResourceMetrics()
		for i := range rms.Len() {
			ms := rms.At(i).ScopeMetrics().At(0).Metrics()
			for j := range ms.Len() {
				out[ms.At(j).Name()] = ms.At(j)
			}
		}
	}
	return out
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
	got, err := s.scrapeProto(context.Background(), strings.NewReader(string(body)), newBatcher(func(pcommon.Resource) {}, time.Now(), time.Now()), nil, "t", "t")
	if err != nil {
		t.Fatal(err)
	}
	if got != 3 {
		t.Fatalf("proto sample count = %d, want 3 (double-counted?)", got)
	}
}

// A HISTOGRAM family whose Metric omits the histogram submessage yields nil
// from GetHistogram(). isNative's raw field reads (h.Schema, h.ZeroThreshold,
// h.ZeroCount) dereferenced it — the generated GetSchema() above them is
// nil-safe and hid the hazard.
//
// This is reachable from the WIRE by any target the agent scrapes, and nothing
// in the agent recovers a panic, so the whole process died and the same target
// was re-scraped every cycle: a permanent CrashLoopBackOff for the node's
// DaemonSet, triggerable by an ordinary annotated workload serving a few bytes
// of malformed protobuf.
//
// The family must degrade to the classic path (all nil-safe getters), which for
// an absent histogram means _sum 0 / _count 0 and no buckets — and the rest of
// the exposition must still convert.
func TestProtoHistogramWithNoHistogramSubmessage(t *testing.T) {
	for _, mt := range []dto.MetricType{dto.MetricType_HISTOGRAM, dto.MetricType_GAUGE_HISTOGRAM} {
		t.Run(mt.String(), func(t *testing.T) {
			bad := &dto.MetricFamily{
				Name:   ptr("broken_histogram_seconds"),
				Type:   mt.Enum(),
				Metric: []*dto.Metric{{Label: []*dto.LabelPair{{Name: ptr("a"), Value: ptr("b")}}}}, // no Histogram
			}
			// A well-formed family after it: the exposition must keep parsing.
			good := &dto.MetricFamily{
				Name:   ptr("http_requests_total"),
				Type:   dto.MetricType_COUNTER.Enum(),
				Metric: []*dto.Metric{{Counter: &dto.Counter{Value: ptr(7.0)}}},
			}
			body := protoBody(t, bad, good)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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
			s.cycle(context.Background()) // must not panic

			var sawCounter bool
			for _, md := range exp.batches {
				rms := md.ResourceMetrics()
				for i := range rms.Len() {
					ms := rms.At(i).ScopeMetrics().At(0).Metrics()
					for j := range ms.Len() {
						if ms.At(j).Name() == "http_requests_total" {
							sawCounter = true
						}
					}
				}
			}
			if !sawCounter {
				t.Error("the family after the malformed one was not converted: the exposition aborted")
			}
		})
	}
}

// conv.finish() emits through the converter's own exportIfFull hook, so it can
// ship the whole chunk itself and return with nothing left. flushIfFull then
// exported an EMPTY payload — a wasted OTLP RPC, and a wasted durable record
// with -buffer-dir. The three sibling export sites all guard.
func TestProtoFlushDoesNotExportEmptyChunks(t *testing.T) {
	classic := &dto.MetricFamily{
		Name: ptr("rpc_latency_seconds"),
		Type: dto.MetricType_HISTOGRAM.Enum(),
		Metric: []*dto.Metric{{
			Histogram: &dto.Histogram{
				SampleCount: ptr(uint64(4)), SampleSum: ptr(1.5),
				Bucket: []*dto.Bucket{
					{UpperBound: ptr(0.5), CumulativeCount: ptr(uint64(3))},
					{UpperBound: ptr(math.Inf(1)), CumulativeCount: ptr(uint64(4))},
				},
			},
		}},
	}
	native := &dto.MetricFamily{
		Name: ptr("io_latency_seconds"),
		Type: dto.MetricType_HISTOGRAM.Enum(),
		Metric: []*dto.Metric{{
			Histogram: &dto.Histogram{
				SampleCount: ptr(uint64(3)), SampleSum: ptr(0.9),
				Schema: ptr(int32(2)), ZeroThreshold: ptr(1e-9), ZeroCount: ptr(uint64(1)),
				PositiveSpan:  []*dto.BucketSpan{{Offset: ptr(int32(1)), Length: ptr(uint32(1))}},
				PositiveDelta: []int64{2},
			},
		}},
	}
	body := protoBody(t, classic, native)

	exp := &captureExporter{}
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		NativeHistograms: true, BatchPoints: 1,
		Targets:  staticTargets{},
		Exporter: exp, StartTime: time.Now(),
	})
	cb := newBatcher(func(pcommon.Resource) {}, time.Now(), time.Now())
	if _, err := s.scrapeProto(context.Background(), strings.NewReader(string(body)), cb, nil, "t", "t"); err != nil {
		t.Fatal(err)
	}
	if len(exp.batches) == 0 {
		t.Fatal("nothing exported")
	}
	for i, md := range exp.batches {
		if md.DataPointCount() == 0 {
			t.Fatalf("export %d of %d carried no data points", i+1, len(exp.batches))
		}
	}
}

// protoConvert runs one protobuf exposition through the protobuf front and
// returns the exported metrics by name plus the malformed count — which
// scrapeProtoFamilies' full-scrape path cannot show.
func protoConvert(t *testing.T, families ...*dto.MetricFamily) (map[string]pmetric.Metric, int) {
	t.Helper()
	body := protoBody(t, families...)
	exp := &captureExporter{}
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		NativeHistograms: true, Targets: staticTargets{},
		Exporter: exp, StartTime: time.Now(),
	})
	cb := newBatcher(func(pcommon.Resource) {}, time.Now(), time.Now())
	ss := s.newScrapeSession(context.Background(), cb, pipelineTargets, "t", "t", nil, true)
	malformed, err := s.parseProtoAndExport(ss, strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	if cb.count() > 0 {
		if err := ss.export(); err != nil {
			t.Fatal(err)
		}
	}
	out := map[string]pmetric.Metric{}
	for _, md := range exp.batches {
		rms := md.ResourceMetrics()
		for i := range rms.Len() {
			ms := rms.At(i).ScopeMetrics().At(0).Metrics()
			for j := range ms.Len() {
				out[ms.At(j).Name()] = ms.At(j)
			}
		}
	}
	return out, malformed
}

// The protobuf front must not export shapes the text grammar structurally
// cannot produce: an unset family name decodes cleanly (type defaulting to
// COUNTER) and became an empty-named OTLP metric, and an unset label-pair name
// became an empty attribute key — both junk a downstream OTLP→Prometheus
// translation rejects per-series. Both are rejected as malformed, mirroring
// the text front's whole-line reject; well-formed neighbours still convert.
func TestProtoRejectsEmptyNames(t *testing.T) {
	nameless := &dto.MetricFamily{ // Name unset entirely
		Metric: []*dto.Metric{{Counter: &dto.Counter{Value: ptr(1.0)}}},
	}
	badLabel := &dto.MetricFamily{
		Name: ptr("bad_label_total"),
		Type: dto.MetricType_COUNTER.Enum(),
		Metric: []*dto.Metric{{
			Label:   []*dto.LabelPair{{Value: ptr("v")}}, // Name unset
			Counter: &dto.Counter{Value: ptr(2.0)},
		}},
	}
	good := &dto.MetricFamily{
		Name:   ptr("good_total"),
		Type:   dto.MetricType_COUNTER.Enum(),
		Metric: []*dto.Metric{{Counter: &dto.Counter{Value: ptr(3.0)}}},
	}
	got, malformed := protoConvert(t, nameless, badLabel, good)
	if _, ok := got[""]; ok {
		t.Error("an empty-named metric was exported")
	}
	if _, ok := got["bad_label_total"]; ok {
		t.Error("a metric with an empty-named label was exported")
	}
	if _, ok := got["good_total"]; !ok {
		t.Errorf("the well-formed family was lost; got %v", slices.Sorted(maps.Keys(got)))
	}
	if malformed != 2 {
		t.Errorf("malformed = %d, want 2 (one per rejected metric)", malformed)
	}
}

// dto's *_float count fields override their integer counterparts ("Overrides
// sample_count if > 0"), which is how a FLOAT histogram carries its counts —
// classic buckets included. Reading the integer fields turned every observation
// of such a target into a zero, with nothing counted: the series was alive and
// empty.
func TestProtoClassicHistogramFloatCounts(t *testing.T) {
	fam := &dto.MetricFamily{
		Name: ptr("rpc_latency_seconds"),
		Type: dto.MetricType_HISTOGRAM.Enum(),
		Metric: []*dto.Metric{{
			Histogram: &dto.Histogram{
				SampleCountFloat: ptr(6.5),
				SampleSum:        ptr(1.5),
				Bucket: []*dto.Bucket{
					{UpperBound: ptr(1.0), CumulativeCount: ptr(uint64(0)), CumulativeCountFloat: ptr(2.5)},
					{UpperBound: ptr(5.0), CumulativeCount: ptr(uint64(0)), CumulativeCountFloat: ptr(6.5)},
				},
			},
		}},
	}
	got, malformed := protoConvert(t, fam)
	m, ok := got["rpc_latency_seconds"]
	if !ok {
		t.Fatalf("family missing; got %v", slices.Sorted(maps.Keys(got)))
	}
	if malformed != 0 {
		t.Fatalf("malformed = %d, want 0", malformed)
	}
	dp := m.Histogram().DataPoints().At(0)
	if dp.Count() != 6 {
		t.Errorf("count = %d, want 6 (sample_count_float overrides the zero integer field)", dp.Count())
	}
	if b := dp.ExplicitBounds().AsRaw(); !slices.Equal(b, []float64{1, 5}) {
		t.Errorf("bounds = %v, want [1 5]", b)
	}
	if c := dp.BucketCounts().AsRaw(); !slices.Equal(c, []uint64{2, 4, 0}) {
		t.Errorf("bucket counts = %v, want [2 4 0]: cumulative_count_float overrides the zero integer field", c)
	}
}

// The same override on the GAUGE_HISTOGRAM branch, which ships its buckets as
// gauges: a float gauge histogram got a correct _gcount beside all-zero buckets.
func TestProtoGaugeHistogramFloatBucketCounts(t *testing.T) {
	fam := &dto.MetricFamily{
		Name: ptr("cache_entries"),
		Type: dto.MetricType_GAUGE_HISTOGRAM.Enum(),
		Metric: []*dto.Metric{{
			Histogram: &dto.Histogram{
				SampleCountFloat: ptr(6.5),
				SampleSum:        ptr(1.5),
				Bucket: []*dto.Bucket{
					{UpperBound: ptr(1.0), CumulativeCount: ptr(uint64(0)), CumulativeCountFloat: ptr(2.5)},
					{UpperBound: ptr(math.Inf(1)), CumulativeCount: ptr(uint64(0)), CumulativeCountFloat: ptr(6.5)},
				},
			},
		}},
	}
	got, _ := protoConvert(t, fam)
	m, ok := got["cache_entries_bucket"]
	if !ok {
		t.Fatalf("bucket rows missing; got %v", slices.Sorted(maps.Keys(got)))
	}
	dps := m.Gauge().DataPoints()
	if dps.Len() != 2 {
		t.Fatalf("bucket rows = %d, want 2", dps.Len())
	}
	for i := range dps.Len() {
		if dps.At(i).DoubleValue() == 0 {
			le, _ := dps.At(i).Attributes().Get("le")
			t.Errorf("bucket le=%v carries 0: the integer field was read, not cumulative_count_float", le.AsString())
		}
	}
	if v := got["cache_entries_gcount"].Gauge().DataPoints().At(0).DoubleValue(); v != 6.5 {
		t.Errorf("_gcount = %v, want 6.5", v)
	}
}

// An NHCB message (schema -53) span-encodes its counts and puts the BOUNDS in
// custom_values, which this client_model does not generate — so there is
// nothing to convert. Falling through to the classic branch shipped an OTLP
// Histogram with no bounds and one overflow bucket: a correct count and sum
// beside a distribution every histogram_quantile reads as nonsense, and nothing
// counted. The loss must be visible, and the rest of the exposition must
// survive it.
func TestProtoNHCBIsRefusedRatherThanShippedBucketless(t *testing.T) {
	nhcb := &dto.MetricFamily{
		Name: ptr("rpc_latency_seconds"),
		Type: dto.MetricType_HISTOGRAM.Enum(),
		Metric: []*dto.Metric{{
			Histogram: &dto.Histogram{
				SampleCount: ptr(uint64(6)), SampleSum: ptr(1.5),
				Schema:        ptr(int32(-53)),
				PositiveSpan:  []*dto.BucketSpan{{Offset: ptr(int32(0)), Length: ptr(uint32(2))}},
				PositiveDelta: []int64{2, 2},
			},
		}},
	}
	after := &dto.MetricFamily{
		Name:   ptr("http_requests_total"),
		Type:   dto.MetricType_COUNTER.Enum(),
		Metric: []*dto.Metric{{Counter: &dto.Counter{Value: ptr(7.0)}}},
	}
	got, malformed := protoConvert(t, nhcb, after)
	if m, ok := got["rpc_latency_seconds"]; ok {
		dp := m.Histogram().DataPoints().At(0)
		t.Errorf("NHCB shipped as a histogram with bounds %v and counts %v: its bounds are not on the wire",
			dp.ExplicitBounds().AsRaw(), dp.BucketCounts().AsRaw())
	}
	if malformed != 1 {
		t.Errorf("malformed = %d, want 1: the refusal must be counted", malformed)
	}
	if _, ok := got["http_requests_total"]; !ok {
		t.Error("the family after the NHCB one was not converted")
	}
}

// isNative/isNHCB are evaluated per Metric, but the OTLP TYPE is per metric
// NAME: a family whose children mix representations used to have its type fixed
// by whichever converted first, and the other child's points were then dropped
// by the batcher's type guard — moving only the unlabelled collision counter, so
// a whole child could vanish with nothing naming it. The family resolves once,
// native wins, and the loser is counted with a pipeline label.
func TestProtoMixedHistogramFamilyResolvesOnceAndCountsTheLoser(t *testing.T) {
	nativeChild := &dto.Metric{
		Label: []*dto.LabelPair{{Name: ptr("svc"), Value: ptr("a")}},
		Histogram: &dto.Histogram{
			SampleCount: ptr(uint64(3)), SampleSum: ptr(0.9),
			Schema: ptr(int32(2)), ZeroThreshold: ptr(1e-9), ZeroCount: ptr(uint64(1)),
			PositiveSpan:  []*dto.BucketSpan{{Offset: ptr(int32(1)), Length: ptr(uint32(1))}},
			PositiveDelta: []int64{2},
		},
	}
	classicChild := &dto.Metric{
		Label: []*dto.LabelPair{{Name: ptr("svc"), Value: ptr("b")}},
		Histogram: &dto.Histogram{
			SampleCount: ptr(uint64(4)), SampleSum: ptr(1.5),
			Bucket: []*dto.Bucket{
				{UpperBound: ptr(0.5), CumulativeCount: ptr(uint64(3))},
				{UpperBound: ptr(math.Inf(1)), CumulativeCount: ptr(uint64(4))},
			},
		},
	}
	for _, tc := range []struct {
		name    string
		metrics []*dto.Metric
	}{
		{"native first", []*dto.Metric{nativeChild, classicChild}},
		{"classic first", []*dto.Metric{classicChild, nativeChild}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fam := &dto.MetricFamily{Name: ptr("h"), Type: dto.MetricType_HISTOGRAM.Enum(), Metric: tc.metrics}
			beforeMixed := obs.ScrapeHistogramMixed.WithLabelValues(pipelineTargets, "classic").Value()
			beforeCollisions := obs.ScrapeCollisions.Value()

			got, malformed := protoConvert(t, fam)
			m, ok := got["h"]
			if !ok {
				t.Fatalf("family missing; got %v", slices.Sorted(maps.Keys(got)))
			}
			if m.Type() != pmetric.MetricTypeExponentialHistogram {
				t.Fatalf("type = %v, want the native representation to carry the family whatever the child order", m.Type())
			}
			if n := m.ExponentialHistogram().DataPoints().Len(); n != 1 {
				t.Fatalf("exponential points = %d, want 1 (the native child)", n)
			}
			if malformed != 0 {
				t.Errorf("malformed = %d, want 0: a mixed family is not malformed exposition", malformed)
			}
			if got := obs.ScrapeHistogramMixed.WithLabelValues(pipelineTargets, "classic").Value() - beforeMixed; got != 1 {
				t.Errorf("dropped classic representations counted = %v, want 1", got)
			}
			if got := obs.ScrapeCollisions.Value() - beforeCollisions; got != 0 {
				t.Errorf("name collisions = %v, want 0: the drop must be attributed, not left to the type guard", got)
			}
		})
	}
}

// schema and zero_threshold are the TARGET's values and are stamped verbatim
// onto the OTLP point (scale, zero region). Prometheus only defines exponential
// schemas in [-4, 8] (-53, NHCB, is routed off before this path), and a zero
// region cannot be negative or non-finite: a point built from anything else is
// spec-invalid and a strict backend rejects the whole BATCH carrying it — the
// target's co-batched metrics, every cycle. Such a histogram is refused and
// counted malformed like any other hostile shape (no clamp — a silent rewrite
// hides the producer bug), and the rest of the exposition survives; the full
// range Prometheus defines, boundaries included, still converts.
func TestNativeHistogramSchemaAndZeroThresholdBounds(t *testing.T) {
	mk := func(schema int32, zeroTh float64) *dto.MetricFamily {
		return &dto.MetricFamily{
			Name: ptr("rpc_latency_seconds"),
			Type: dto.MetricType_HISTOGRAM.Enum(),
			Metric: []*dto.Metric{{
				Histogram: &dto.Histogram{
					SampleCount: ptr(uint64(3)), SampleSum: ptr(0.9),
					Schema: ptr(schema), ZeroThreshold: ptr(zeroTh), ZeroCount: ptr(uint64(1)),
					PositiveSpan:  []*dto.BucketSpan{{Offset: ptr(int32(1)), Length: ptr(uint32(1))}},
					PositiveDelta: []int64{2},
				},
			}},
		}
	}
	after := &dto.MetricFamily{
		Name:   ptr("http_requests_total"),
		Type:   dto.MetricType_COUNTER.Enum(),
		Metric: []*dto.Metric{{Counter: &dto.Counter{Value: ptr(7.0)}}},
	}
	for _, tc := range []struct {
		name   string
		schema int32
		zeroTh float64
		valid  bool
	}{
		{"schema 1000000", 1000000, 1e-9, false},
		{"schema -100", -100, 1e-9, false},
		{"zero_threshold NaN", 2, math.NaN(), false},
		{"zero_threshold negative", 2, -1e-9, false},
		{"zero_threshold +Inf", 2, math.Inf(1), false},
		{"schema -4 boundary", -4, 1e-9, true},
		{"schema 8 boundary", 8, 1e-9, true},
		{"zero_threshold 0 counts exact zeros", 2, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, malformed := protoConvert(t, mk(tc.schema, tc.zeroTh), after)
			if _, ok := got["http_requests_total"]; !ok {
				t.Error("the family after this one was not converted: the exposition aborted")
			}
			if tc.valid {
				m, ok := got["rpc_latency_seconds"]
				if !ok {
					t.Fatalf("valid native histogram refused; got %v", slices.Sorted(maps.Keys(got)))
				}
				if m.Type() != pmetric.MetricTypeExponentialHistogram {
					t.Fatalf("type = %v", m.Type())
				}
				dp := m.ExponentialHistogram().DataPoints().At(0)
				if dp.Scale() != tc.schema || dp.ZeroThreshold() != tc.zeroTh {
					t.Errorf("scale=%d zero_threshold=%v, want %d/%v", dp.Scale(), dp.ZeroThreshold(), tc.schema, tc.zeroTh)
				}
				if malformed != 0 {
					t.Errorf("malformed = %d, want 0", malformed)
				}
				return
			}
			if m, ok := got["rpc_latency_seconds"]; ok {
				t.Errorf("spec-invalid native histogram shipped as %v", m.Type())
			}
			if malformed != 1 {
				t.Errorf("malformed = %d, want 1: the refusal must be counted", malformed)
			}
		})
	}
}
