package promscrape

// Prometheus protobuf exposition parsing — the only format that carries
// NATIVE histograms, which convert to OTLP exponential histograms. Opt-in
// (-scrape-native-histograms): the target scrape then offers the protobuf
// Accept and this path handles a protobuf response; text responses keep the
// streaming text parser. Native histogram fields (schema, zero bucket,
// span/delta-encoded buckets) map 1:1 onto OTLP's exponential histogram
// (same base-2 scheme); classic families in the same response convert
// through the ordinary Sample path. A family carrying BOTH native and
// classic data uses the native representation (Prometheus's own preference
// when scraping native histograms).

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"
)

// acceptProto is the Accept header offering protobuf exposition first.
const acceptProto = "application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited;q=1," +
	"application/openmetrics-text;version=1.0.0;q=0.8,text/plain;version=0.0.4;q=0.5"

// maxProtoMessageBytes bounds one delimited MetricFamily message.
const maxProtoMessageBytes = 64 << 20

// maxExpBuckets bounds the dense expansion of span-encoded buckets: spans
// can declare arbitrary gaps, and a hostile exposition must not allocate
// unbounded bucket slices.
const maxExpBuckets = 4096

// parseProtoAndExport consumes a delimited-protobuf exposition. Classic
// families flow through the same converter/filter machinery as text
// samples; native histograms go straight to the batcher as exponential
// histogram points.
func (s *Scraper) parseProtoAndExport(body io.Reader, cb chunker, pipeline string, relabel *relabelFilter, export func() error, full func() bool) (samples, malformed int, err error) {
	filter := s.cfg.Filters.filterFor(pipeline).session()
	conv := newConverter(cb, func() error {
		if full() {
			return export()
		}
		return nil
	})
	keep := func(name string, labels []Label) bool {
		if !filter.Keep(name, labels) {
			return false
		}
		return relabel == nil || relabel.Keep(name, labels)
	}
	emit := func(sample Sample) error {
		samples++
		if s.cfg.MaxSamples > 0 && samples > s.cfg.MaxSamples {
			return ErrTooManySamples
		}
		if !keep(sample.Name, sample.Labels) {
			return nil
		}
		return conv.add(sample)
	}

	br := bufio.NewReaderSize(body, 64*1024)
	var buf []byte
	var mf dto.MetricFamily
	for {
		n, rerr := binary.ReadUvarint(br)
		if rerr != nil {
			if rerr == io.EOF {
				break
			}
			return samples, malformed + conv.malformed, rerr
		}
		if n > maxProtoMessageBytes {
			return samples, malformed + conv.malformed, fmt.Errorf("proto message of %d bytes exceeds the cap", n)
		}
		if cap(buf) < int(n) {
			buf = make([]byte, n)
		}
		buf = buf[:n]
		if _, rerr := io.ReadFull(br, buf); rerr != nil {
			return samples, malformed + conv.malformed, rerr
		}
		mf.Reset()
		if perr := proto.Unmarshal(buf, &mf); perr != nil {
			malformed++
			continue
		}
		m, bad, ferr := s.protoFamily(&mf, cb, keep, emit)
		samples += m.samples
		malformed += bad
		if ferr != nil {
			return samples, malformed + conv.malformed, ferr
		}
		if full() {
			if ferr := conv.finish(); ferr != nil {
				return samples, malformed + conv.malformed, ferr
			}
			if eerr := export(); eerr != nil {
				return samples, malformed + conv.malformed, eerr
			}
		}
	}
	if ferr := conv.finish(); ferr != nil {
		return samples, malformed + conv.malformed, ferr
	}
	return samples, malformed + conv.malformed, nil
}

type protoCounts struct{ samples int }

// protoFamily converts one MetricFamily.
func (s *Scraper) protoFamily(mf *dto.MetricFamily, cb chunker, keep func(string, []Label) bool, emit func(Sample) error) (protoCounts, int, error) {
	var c protoCounts
	malformed := 0
	name := mf.GetName()
	for _, m := range mf.GetMetric() {
		labels := protoLabels(m)
		ts := m.GetTimestampMs()
		switch mf.GetType() {
		case dto.MetricType_COUNTER:
			if err := emit(Sample{Name: name, Family: name, Role: RoleCounter, Labels: labels, Value: m.GetCounter().GetValue(), TimestampMs: ts}); err != nil {
				return c, malformed, err
			}
			c.samples++
		case dto.MetricType_GAUGE, dto.MetricType_UNTYPED:
			v := m.GetGauge().GetValue()
			if mf.GetType() == dto.MetricType_UNTYPED {
				v = m.GetUntyped().GetValue()
			}
			if err := emit(Sample{Name: name, Family: name, Role: RoleGauge, Labels: labels, Value: v, TimestampMs: ts}); err != nil {
				return c, malformed, err
			}
			c.samples++
		case dto.MetricType_SUMMARY:
			sum := m.GetSummary()
			for _, q := range sum.GetQuantile() {
				ql := append(labels[:len(labels):len(labels)], Label{Name: "quantile", Value: formatFloat(q.GetQuantile())})
				if err := emit(Sample{Name: name, Family: name, Role: RoleSummaryQuantile, Labels: ql, Value: q.GetValue(), TimestampMs: ts}); err != nil {
					return c, malformed, err
				}
				c.samples++
			}
			if err := emit(Sample{Name: name + "_sum", Family: name, Role: RoleSummarySum, Labels: labels, Value: sum.GetSampleSum(), TimestampMs: ts}); err != nil {
				return c, malformed, err
			}
			if err := emit(Sample{Name: name + "_count", Family: name, Role: RoleSummaryCount, Labels: labels, Value: float64(sum.GetSampleCount()), TimestampMs: ts}); err != nil {
				return c, malformed, err
			}
			c.samples += 2
		case dto.MetricType_HISTOGRAM, dto.MetricType_GAUGE_HISTOGRAM:
			h := m.GetHistogram()
			if isNative(h) {
				c.samples++
				if !keep(name, labels) {
					continue
				}
				if !s.addNativeHistogram(cb, name, labels, h, ts) {
					malformed++
				}
				continue
			}
			for _, b := range h.GetBucket() {
				bl := append(labels[:len(labels):len(labels)], Label{Name: "le", Value: formatFloat(b.GetUpperBound())})
				if err := emit(Sample{Name: name + "_bucket", Family: name, Role: RoleHistogramBucket, Labels: bl, Value: float64(b.GetCumulativeCount()), TimestampMs: ts}); err != nil {
					return c, malformed, err
				}
				c.samples++
			}
			if err := emit(Sample{Name: name + "_sum", Family: name, Role: RoleHistogramSum, Labels: labels, Value: h.GetSampleSum(), TimestampMs: ts}); err != nil {
				return c, malformed, err
			}
			if err := emit(Sample{Name: name + "_count", Family: name, Role: RoleHistogramCount, Labels: labels, Value: float64(h.GetSampleCount()), TimestampMs: ts}); err != nil {
				return c, malformed, err
			}
			c.samples += 2
		default:
			malformed++
		}
	}
	return c, malformed, nil
}

// isNative reports whether a histogram carries native (exponential) data:
// a schema plus span-encoded buckets or a zero bucket. NHCB (custom bounds,
// schema -53) is NOT native-exponential and falls back to classic buckets.
func isNative(h *dto.Histogram) bool {
	if h.GetSchema() == -53 {
		return false
	}
	return (h.Schema != nil && (len(h.GetPositiveSpan()) > 0 || len(h.GetNegativeSpan()) > 0)) ||
		h.ZeroThreshold != nil || h.ZeroCount != nil
}

// addNativeHistogram appends one exponential histogram point to the
// batcher; false = undecodable (counted malformed by the caller).
func (s *Scraper) addNativeHistogram(cb chunker, name string, labels []Label, h *dto.Histogram, ts int64) bool {
	eb, ok := cb.(expSink)
	if !ok {
		return false // batcher variant without exponential support
	}
	pos, posOff, ok := decodeSpans(h.GetPositiveSpan(), h.GetPositiveDelta())
	if !ok {
		return false
	}
	neg, negOff, ok := decodeSpans(h.GetNegativeSpan(), h.GetNegativeDelta())
	if !ok {
		return false
	}
	eb.addExponential(name, expPoint{
		labels:    labels,
		ts:        ts,
		schema:    h.GetSchema(),
		zeroCount: h.GetZeroCount(),
		zeroTh:    h.GetZeroThreshold(),
		count:     h.GetSampleCount(),
		sum:       h.GetSampleSum(),
		hasSum:    h.SampleSum != nil,
		pos:       pos, posOffset: posOff,
		neg: neg, negOffset: negOff,
	})
	return true
}

// expPoint is one decoded native histogram.
type expPoint struct {
	labels    []Label
	ts        int64
	schema    int32
	zeroCount uint64
	zeroTh    float64
	count     uint64
	sum       float64
	hasSum    bool
	pos, neg  []uint64
	posOffset int32
	negOffset int32
}

// expSink is a chunker that can take exponential histogram points (the
// plain batcher; the split/cadvisor batchers do not).
type expSink interface {
	addExponential(family string, p expPoint)
}

// decodeSpans expands Prometheus span/delta bucket encoding into the dense
// absolute counts OTLP wants. Prometheus indexes are 1-based upper-bound
// indexes (bucket i covers (base^(i-1), base^i]); OTLP buckets are 0-based
// lower-bound (index j covers (base^(offset+j), base^(offset+j+1)]), so the
// OTLP offset is the first Prometheus index minus one.
func decodeSpans(spans []*dto.BucketSpan, deltas []int64) (counts []uint64, offset int32, ok bool) {
	if len(spans) == 0 {
		return nil, 0, true
	}
	idx := int32(0)
	first := true
	var cur int64
	di := 0
	var start int32
	for _, sp := range spans {
		idx += sp.GetOffset()
		if first {
			start = idx
			first = false
		} else {
			gap := int(idx - start - int32(len(counts)))
			if gap < 0 || len(counts)+gap > maxExpBuckets {
				return nil, 0, false
			}
			counts = append(counts, make([]uint64, gap)...)
		}
		for i := uint32(0); i < sp.GetLength(); i++ {
			if di >= len(deltas) {
				return nil, 0, false
			}
			cur += deltas[di]
			di++
			if cur < 0 || len(counts) >= maxExpBuckets {
				return nil, 0, false
			}
			counts = append(counts, uint64(cur))
		}
		// The next span's offset is relative to the index AFTER this span.
		idx += int32(sp.GetLength())
	}
	return counts, start - 1, true
}

// protoLabels converts a metric's label pairs.
func protoLabels(m *dto.Metric) []Label {
	lps := m.GetLabel()
	if len(lps) == 0 {
		return nil
	}
	out := make([]Label, 0, len(lps))
	for _, lp := range lps {
		out = append(out, Label{Name: lp.GetName(), Value: lp.GetValue()})
	}
	return out
}

func formatFloat(v float64) string {
	if v == math.Trunc(v) && math.Abs(v) < 1e15 {
		return fmt.Sprintf("%g", v)
	}
	return fmt.Sprintf("%v", v)
}
