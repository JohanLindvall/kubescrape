package promscrape

import (
	"context"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"google.golang.org/protobuf/proto"
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
