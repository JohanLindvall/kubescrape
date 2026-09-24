package promscrape

import (
	"bytes"
	"context"
	"encoding/binary"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/pdatacheck"
	"github.com/JohanLindvall/kubescrape/pkg/promparse"
	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// fuzzSeedsFile is pkg/promparse's exposition seed file, SHARED with its
// FuzzParser so a seed added for a parser bug reaches the converter too.
const fuzzSeedsFile = "../../../pkg/promparse/testdata/fuzzseeds.txt"

// loadFuzzSeeds reads a seed file in fuzzseeds.txt's format (see its header);
// the same few lines as pkg/promparse's, which a test helper cannot share
// across the package boundary.
func loadFuzzSeeds(tb testing.TB, path string) []string {
	tb.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		tb.Fatal(err)
	}
	var seeds []string
	for n, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || line[0] == '#' {
			continue
		}
		s, err := strconv.Unquote(line)
		if err != nil {
			tb.Fatalf("%s:%d: %v", path, n+1, err)
		}
		seeds = append(seeds, s)
	}
	if len(seeds) == 0 {
		tb.Fatalf("%s holds no seeds", path)
	}
	return seeds
}

// FuzzConverter pipes fuzzed parses through the converter and batcher
// (including mid-parse chunking via take) and requires that every produced
// pmetric.Metrics marshals cleanly.
func FuzzConverter(f *testing.F) {
	for _, body := range loadFuzzSeeds(f, fuzzSeedsFile) {
		f.Add([]byte(body), byte(0))
		f.Add([]byte(body), byte(3))
	}
	f.Fuzz(func(t *testing.T, data []byte, mode byte) {
		openMetrics := mode&1 != 0
		exemplars := mode&2 != 0
		limit := 1 + int(mode>>2) // small chunk limit exercises take() mid-parse

		marshaler := &pmetric.ProtoMarshaler{}
		checkTaken := func(md pmetric.Metrics) {
			if md.ResourceMetrics().Len() != 1 {
				t.Fatalf("batch has %d ResourceMetrics, want 1", md.ResourceMetrics().Len())
			}
			if _, err := marshaler.MarshalMetrics(md); err != nil {
				t.Fatalf("MarshalProto: %v", err)
			}
			checkHistogramsSumToCount(t, md)
		}

		b := newBatcher(func(res pcommon.Resource) {
			res.Attributes().PutStr("url.full", "http://fuzz.local/metrics")
		}, time.Unix(1e9, 0), time.Unix(1e9+60, 0))
		conv := newConverter(b, nil)
		pp := promparse.Get(promparse.Options{MaxLineBytes: 1 << 20, OpenMetrics: openMetrics, Exemplars: exemplars})
		_, err := pp.Parse(bytes.NewReader(data), func(s Sample) error {
			_ = conv.add(s)
			if b.count() >= limit {
				checkTaken(b.take())
			}
			return nil
		})
		promparse.Put(pp)
		if err != nil {
			t.Fatalf("parse returned error: %v", err)
		}
		_ = conv.finish()
		if conv.malformed < 0 {
			t.Fatalf("converter malformed count negative: %d", conv.malformed)
		}
		checkTaken(b.take())
	})
}

// protoFuzzSeeds are delimited-protobuf expositions of the family shapes the
// protobuf front converts — and refuses — built with the same dto messages
// protoparse_test.go pins each behaviour with, plus raw byte shapes no
// marshaller would produce (a length prefix cut mid-varint, one promising more
// than the body holds).
func protoFuzzSeeds(tb testing.TB) [][]byte {
	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	ex := func(v float64) *dto.Exemplar {
		return &dto.Exemplar{Label: []*dto.LabelPair{{Name: new("trace_id"), Value: new(traceID)}}, Value: new(v)}
	}
	intNative := &dto.MetricFamily{
		Name: new("rpc_latency_seconds"), Help: new("RPC latency."), Unit: new("seconds"),
		Type: dto.MetricType_HISTOGRAM.Enum(),
		Metric: []*dto.Metric{{
			Label: []*dto.LabelPair{{Name: new("svc"), Value: new("a")}},
			Histogram: &dto.Histogram{
				SampleCount: new(uint64(14)), SampleSum: new(3.5),
				Schema: new(int32(3)), ZeroThreshold: new(1e-9), ZeroCount: new(uint64(2)),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: new(int32(1)), Length: new(uint32(2))},
					{Offset: new(int32(2)), Length: new(uint32(1))},
				},
				PositiveDelta: []int64{3, -1, -1},
				NegativeSpan:  []*dto.BucketSpan{{Offset: new(int32(-2)), Length: new(uint32(2))}},
				NegativeDelta: []int64{2, 0},
				Exemplars:     []*dto.Exemplar{ex(0.25)},
			},
		}},
	}
	floatNative := &dto.MetricFamily{
		Name: new("rpc_latency_float_seconds"),
		Type: dto.MetricType_HISTOGRAM.Enum(),
		Metric: []*dto.Metric{{
			Histogram: &dto.Histogram{
				SampleCountFloat: new(9.0), SampleSum: new(2.25),
				Schema: new(int32(2)), ZeroThreshold: new(1e-9), ZeroCountFloat: new(2.0),
				PositiveSpan:  []*dto.BucketSpan{{Offset: new(int32(1)), Length: new(uint32(3))}},
				PositiveCount: []float64{4, 2, 1},
			},
		}},
	}
	nhcb := &dto.MetricFamily{
		Name: new("nhcb_seconds"),
		Type: dto.MetricType_HISTOGRAM.Enum(),
		Metric: []*dto.Metric{{
			Histogram: &dto.Histogram{
				SampleCount: new(uint64(6)), SampleSum: new(1.5), Schema: new(int32(-53)),
				PositiveSpan:  []*dto.BucketSpan{{Offset: new(int32(0)), Length: new(uint32(2))}},
				PositiveDelta: []int64{2, 2},
			},
		}},
	}
	classic := &dto.MetricFamily{
		Name: new("http_duration_seconds"), Help: new("Duration."),
		Type: dto.MetricType_HISTOGRAM.Enum(),
		Metric: []*dto.Metric{{
			Label: []*dto.LabelPair{{Name: new("code"), Value: new("200")}},
			Histogram: &dto.Histogram{
				SampleCount: new(uint64(3)), SampleSum: new(0.6),
				Bucket: []*dto.Bucket{
					{UpperBound: new(0.5), CumulativeCount: new(uint64(2)), Exemplar: ex(0.3)},
					{UpperBound: new(math.Inf(1)), CumulativeCount: new(uint64(3))},
				},
			},
		}},
	}
	summary := &dto.MetricFamily{
		Name: new("rpc_summary_seconds"),
		Type: dto.MetricType_SUMMARY.Enum(),
		Metric: []*dto.Metric{{
			Summary: &dto.Summary{
				SampleCount: new(uint64(2)), SampleSum: new(1.0),
				Quantile: []*dto.Quantile{{Quantile: new(0.5), Value: new(0.1)}, {Quantile: new(0.99), Value: new(0.4)}},
			},
		}},
	}
	gaugeHist := &dto.MetricFamily{
		Name: new("cache_entries"),
		Type: dto.MetricType_GAUGE_HISTOGRAM.Enum(),
		Metric: []*dto.Metric{{
			Histogram: &dto.Histogram{
				SampleCount: new(uint64(4)), SampleSum: new(1.5),
				Bucket: []*dto.Bucket{
					{UpperBound: new(0.5), CumulativeCount: new(uint64(3))},
					{UpperBound: new(math.Inf(1)), CumulativeCount: new(uint64(4))},
				},
			},
		}},
	}
	mixed := &dto.MetricFamily{
		Name: new("mixed_seconds"),
		Type: dto.MetricType_HISTOGRAM.Enum(),
		Metric: []*dto.Metric{
			{Histogram: &dto.Histogram{
				SampleCount: new(uint64(4)), SampleSum: new(1.5),
				Bucket: []*dto.Bucket{{UpperBound: new(math.Inf(1)), CumulativeCount: new(uint64(4))}},
			}},
			{Label: []*dto.LabelPair{{Name: new("svc"), Value: new("b")}}, Histogram: &dto.Histogram{
				SampleCount: new(uint64(3)), SampleSum: new(0.9),
				Schema: new(int32(2)), ZeroThreshold: new(1e-9), ZeroCount: new(uint64(1)),
				PositiveSpan:  []*dto.BucketSpan{{Offset: new(int32(1)), Length: new(uint32(1))}},
				PositiveDelta: []int64{2},
			}},
		},
	}
	noSubmessage := &dto.MetricFamily{
		Name:   new("broken_histogram_seconds"),
		Type:   dto.MetricType_HISTOGRAM.Enum(),
		Metric: []*dto.Metric{{Label: []*dto.LabelPair{{Name: new("a"), Value: new("b")}}}},
	}
	nameless := &dto.MetricFamily{Metric: []*dto.Metric{{Counter: &dto.Counter{Value: new(1.0)}}}}
	counter := &dto.MetricFamily{
		Name: new("http_requests_total"), Type: dto.MetricType_COUNTER.Enum(),
		Metric: []*dto.Metric{{Counter: &dto.Counter{Value: new(7.0), Exemplar: ex(1)}}},
	}
	gauge := &dto.MetricFamily{
		Name: new("temperature"), Type: dto.MetricType_GAUGE.Enum(),
		Metric: []*dto.Metric{{Gauge: &dto.Gauge{Value: new(-17.5)}}},
	}

	seeds := [][]byte{
		{},
		{0x80},       // a length prefix cut mid-varint
		{0x05, 0x0a}, // a prefix promising more than the body holds
	}
	for _, fam := range []*dto.MetricFamily{intNative, floatNative, nhcb, classic, summary, gaugeHist, mixed, noSubmessage, nameless, counter, gauge} {
		seeds = append(seeds, protoBody(tb, fam))
	}
	return append(seeds, protoBody(tb, counter, intNative, classic, noSubmessage, summary, nhcb, gaugeHist, gauge))
}

// FuzzProtoExposition drives the protobuf front — the delimited reader, the
// decoded-size guard, span decoding, native and classic histogram conversion
// and exemplar handling — over target-controlled bytes, through the same
// scrapeProto entry the targets pipeline calls. Invariants: no panic (nothing
// in the agent recovers one, so a reachable panic here is a crashed DaemonSet
// pod re-scraping the same target every cycle), and every exported chunk
// marshals, names every metric, carries no point-less metric, keeps each
// exponential scale inside the [-4, 8] Prometheus defines and satisfies
// checkHistogramsSumToCount. mode varies the exemplar gate and the chunk size,
// so mid-family flushes are exercised too.
func FuzzProtoExposition(f *testing.F) {
	for _, body := range protoFuzzSeeds(f) {
		f.Add(body, byte(1))
		f.Add(body, byte(6))
	}
	marshaler := &pmetric.ProtoMarshaler{}
	f.Fuzz(func(t *testing.T, data []byte, mode byte) {
		exp := &captureExporter{}
		s := New(Config{
			Node: "n1", Interval: time.Hour, Timeout: time.Hour,
			NativeHistograms: true, Exemplars: mode&1 != 0,
			BatchPoints: 1 + int(mode>>1),
			Targets:     staticTargets{}, Exporter: exp, StartTime: time.Unix(1e9, 0),
			Logger: slog.New(slog.DiscardHandler),
		})
		cb := newBatcher(func(res pcommon.Resource) {
			res.Attributes().PutStr("url.full", "http://fuzz.local/metrics")
		}, time.Unix(1e9, 0), time.Unix(1e9+60, 0))
		// An error is a verdict about the input (a truncated body, an over-cap
		// message), never a failure of the target under test.
		_, _ = s.scrapeProto(context.Background(), bytes.NewReader(data), cb, nil, "fuzz", "fuzz")
		for i, md := range exp.batches {
			if md.ResourceMetrics().Len() != 1 {
				t.Fatalf("batch %d has %d ResourceMetrics, want 1", i, md.ResourceMetrics().Len())
			}
			if _, err := marshaler.MarshalMetrics(md); err != nil {
				t.Fatalf("batch %d: MarshalProto: %v", i, err)
			}
			if names := pdatacheck.EmptyMetrics(md); len(names) > 0 {
				t.Fatalf("batch %d exports metrics with no data points: %v", i, names)
			}
			checkHistogramsSumToCount(t, md)
			sms := md.ResourceMetrics().At(0).ScopeMetrics()
			for j := range sms.Len() {
				ms := sms.At(j).Metrics()
				for k := range ms.Len() {
					m := ms.At(k)
					if m.Name() == "" {
						t.Fatalf("batch %d metric %d has an empty name", i, k)
					}
					if m.Type() != pmetric.MetricTypeExponentialHistogram {
						continue
					}
					dps := m.ExponentialHistogram().DataPoints()
					for d := range dps.Len() {
						if sc := dps.At(d).Scale(); sc < -4 || sc > 8 {
							t.Fatalf("metric %q point %d: scale %d outside the [-4, 8] Prometheus defines", m.Name(), d, sc)
						}
					}
				}
			}
		}
	})
}

// checkHistogramsSumToCount asserts the one OTLP invariant a histogram point
// cannot express its way out of: sum(bucket_counts) == count. It is checked on
// every batch BOTH conversion fuzz targets produce (seed corpora included, so a
// plain `go test` runs it) rather than only on hand-written bodies, because the
// inputs that break it are exactly the ones nobody writes by hand — a
// cumulative sequence that decreases, a bucket claiming more than _count, the
// same le spelled twice. A point that violates it is invalid OTLP, and a
// validating collector may answer by rejecting the whole chunk, which costs
// every other target batched with it.
//
// EXPONENTIAL points are checked too, and skipping them is how the invariant
// escaped once already: the native-histogram path rounds the sample count, the
// zero count and every float bucket independently, so a wire-consistent message
// converted to buckets carrying up to twice the observations their count
// declared. Only FuzzProtoExposition produces them — FuzzConverter feeds the
// text parser, which has no native histograms — and its seeds carry integer and
// float native histograms, so the exponential branch runs on every plain
// `go test`. Their assertion is the ONE-DIRECTIONAL half — buckets may never
// claim MORE than count, while count may legitimately exceed them, since a NaN
// observation increments a Prometheus histogram's count without entering any
// bucket and that shape is passed through as the target reported it.
func checkHistogramsSumToCount(t *testing.T, md pmetric.Metrics) {
	t.Helper()
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				switch m.Type() {
				case pmetric.MetricTypeHistogram:
					dps := m.Histogram().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						var sum uint64
						for _, c := range dp.BucketCounts().AsRaw() {
							sum += c
						}
						if sum != dp.Count() {
							t.Fatalf("metric %q point %d: sum(bucket_counts) = %d, count = %d (bounds=%v counts=%v) — OTLP requires them equal",
								m.Name(), d, sum, dp.Count(), dp.ExplicitBounds().AsRaw(), dp.BucketCounts().AsRaw())
						}
					}
				case pmetric.MetricTypeExponentialHistogram:
					dps := m.ExponentialHistogram().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						sum := dp.ZeroCount()
						for _, c := range dp.Positive().BucketCounts().AsRaw() {
							sum += c
						}
						for _, c := range dp.Negative().BucketCounts().AsRaw() {
							sum += c
						}
						if sum > dp.Count() {
							t.Fatalf("metric %q point %d: zero+sum(bucket_counts) = %d > count = %d (pos=%v neg=%v) — the buckets claim observations the point's population does not have",
								m.Name(), d, sum, dp.Count(), dp.Positive().BucketCounts().AsRaw(), dp.Negative().BucketCounts().AsRaw())
						}
					}
				}
			}
		}
	}
}

// fuzzSpans decodes a span list and its values from fuzz bytes: data[0] is the
// span count (at most 16), then 8 bytes per span — an int32 offset and a uint32
// length, full range, since the index arithmetic is what is under test — and
// every remaining byte is one value. Lengths the values cannot back are refused
// by decodeSpans as soon as the values run out, so a huge length costs nothing.
func fuzzSpans(data []byte) (spans []*dto.BucketSpan, values []byte) {
	if len(data) == 0 {
		return nil, nil
	}
	n := int(data[0] % 17)
	data = data[1:]
	for range n {
		if len(data) < 8 {
			break
		}
		spans = append(spans, &dto.BucketSpan{
			Offset: new(int32(binary.LittleEndian.Uint32(data))),
			Length: new(binary.LittleEndian.Uint32(data[4:])),
		})
		data = data[8:]
	}
	return spans, data
}

// FuzzDecodeSpans drives the span/delta decoder directly with target-controlled
// offsets and lengths. FuzzProtoExposition reaches it too, but only through a
// protobuf body the mutator must keep decodable; the int32 wrap its doc
// records got past the gap guard, which is exactly the kind of arithmetic a
// direct target explores. Invariants: no panic; an accepted decoding holds at
// most maxExpBuckets counts, its last Prometheus index stays in int32 range,
// and it agrees bucket for bucket with the sparse DEFINITION of the encoding
// (each span's offset relative to the index after the previous span; integer
// counts delta-encoded, float ones absolute, deltas winning when both are sent).
func FuzzDecodeSpans(f *testing.F) {
	span := func(off int32, length uint32) []byte {
		return binary.LittleEndian.AppendUint32(binary.LittleEndian.AppendUint32(nil, uint32(off)), length)
	}
	cat := func(parts ...[]byte) []byte { return bytes.Join(parts, nil) }
	f.Add(cat([]byte{2}, span(-3, 2), span(1, 2), []byte{66, 65, 64, 66}), byte(0))
	f.Add(cat([]byte{1}, span(0, 3), []byte{2, 4, 6}), byte(1))
	f.Add(cat([]byte{2}, span(math.MaxInt32, 1), span(2, 1), []byte{65, 65}), byte(0))
	f.Add(cat([]byte{1}, span(math.MinInt32, 1), []byte{65}), byte(0))
	f.Add(cat([]byte{2}, span(0, 1), span(100000, 1), []byte{65, 64}), byte(2))
	f.Fuzz(func(t *testing.T, data []byte, mode byte) {
		spans, raw := fuzzSpans(data)
		var deltas []int64
		var absolute []float64
		if mode&1 == 0 || mode&2 != 0 {
			for _, b := range raw {
				deltas = append(deltas, int64(b)-64) // mostly small, either sign
			}
		}
		if mode&1 != 0 {
			for _, b := range raw {
				v := float64(int8(b)) / 2 // halves exercise countOf's rounding
				if b == 0x7f {
					v = math.NaN()
				}
				absolute = append(absolute, v)
			}
		}

		counts, offset, ok := decodeSpans(spans, deltas, absolute)
		if !ok {
			return
		}
		if len(counts) > maxExpBuckets {
			t.Fatalf("%d buckets accepted, over the %d cap", len(counts), maxExpBuckets)
		}
		if last := int64(offset) + int64(len(counts)); last > math.MaxInt32 {
			t.Fatalf("offset %d + %d buckets reaches Prometheus index %d, past int32", offset, len(counts), last)
		}

		// The definition, in int64 with no guards: bounded here because an
		// accepted decoding consumed one value per bucket and no more.
		want := map[int64]uint64{}
		var idx, cur int64
		di := 0
		for _, sp := range spans {
			idx += int64(sp.GetOffset())
			for range sp.GetLength() {
				var v uint64
				if len(deltas) > 0 {
					cur += deltas[di]
					v = uint64(cur)
				} else {
					v, _ = countOf(absolute[di])
				}
				want[idx] = v
				idx++
				di++
			}
		}
		for i, c := range counts {
			k := int64(offset) + 1 + int64(i)
			if c != want[k] {
				t.Fatalf("bucket at Prometheus index %d = %d, the encoding defines %d (spans %v)", k, c, want[k], spans)
			}
			delete(want, k)
		}
		if len(want) != 0 {
			t.Fatalf("the encoding defines buckets outside the decoded range [%d, %d): %v",
				int64(offset)+1, int64(offset)+1+int64(len(counts)), want)
		}
	})
}
