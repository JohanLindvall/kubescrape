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
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"

	"github.com/JohanLindvall/kubescrape/internal/obs"
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
func (s *Scraper) parseProtoAndExport(ctx context.Context, body io.Reader, cb chunker, pipeline string, relabel *relabelFilter, export func() error, full func() bool) (samples, malformed int, err error) {
	filter := s.cfg.Filters.filterFor(pipeline).session()
	exportFailed := false
	exportChunk := func() error {
		if eerr := export(); eerr != nil {
			exportFailed = true
			return eerr
		}
		return nil
	}
	conv := newConverter(cb, func() error {
		if full() {
			return exportChunk()
		}
		return nil
	})
	// Salvage the partially converted scrape on ANY abort (sample limit,
	// truncated body, read timeout mid-body, over-cap message), exactly as the
	// text path does. Every metric kind here is cumulative, so a partial scrape
	// costs only the missing series — whereas discarding the whole conversion
	// threw away everything parsed before the abort. Pointless when the failure
	// WAS the export (the collector just rejected a chunk) or when the context
	// is gone (the send cannot succeed either).
	defer func() {
		if err == nil || exportFailed || ctx.Err() != nil {
			return
		}
		if ferr := conv.finish(); ferr == nil && cb.count() > 0 {
			if eerr := export(); eerr != nil {
				s.log.Warn("exporting partial proto scrape", "pipeline", pipeline, "error", eerr)
			}
		}
	}()
	// Counted in locals and reported once, for the reason the text path does
	// it (see parseAndExportFiltered): keep runs per sample. The deferred
	// report covers the abort paths too.
	var droppedFilter, droppedRelabel int
	defer func() {
		if droppedFilter > 0 {
			obs.ScrapeSamplesDropped.WithLabelValues(pipeline, "filter").Add(float64(droppedFilter))
		}
		if droppedRelabel > 0 {
			obs.ScrapeSamplesDropped.WithLabelValues(pipeline, "relabel").Add(float64(droppedRelabel))
		}
	}()
	keep := func(name string, labels []Label) bool {
		if !filter.Keep(name, labels) {
			droppedFilter++
			return false
		}
		if relabel != nil && !relabel.Keep(name, labels) {
			droppedRelabel++
			return false
		}
		return true
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
		// flushIfFull lets protoFamily flush BETWEEN the points of a single
		// native-histogram family: a delimited exposition emits one
		// MetricFamily per metric holding ALL its series, so checking only at
		// the family boundary would build the whole batch in memory and blow
		// BatchBytes for a high-cardinality native family (classic samples
		// already flush mid-family via converter.check).
		flushIfFull := func() error {
			if !full() {
				return nil
			}
			if ferr := conv.finish(); ferr != nil {
				return ferr
			}
			return exportChunk()
		}
		m, bad, ferr := s.protoFamily(&mf, cb, keep, emit, samples, flushIfFull)
		samples += m.samples
		malformed += bad
		if ferr != nil {
			return samples, malformed + conv.malformed, ferr
		}
		if ferr := flushIfFull(); ferr != nil {
			return samples, malformed + conv.malformed, ferr
		}
	}
	if ferr := conv.finish(); ferr != nil {
		return samples, malformed + conv.malformed, ferr
	}
	return samples, malformed + conv.malformed, nil
}

type protoCounts struct{ samples int }

// protoFamily converts one MetricFamily. baseSamples is the sample count
// BEFORE this family (for the MaxSamples cap on the native path, which does
// not go through emit); flushIfFull flushes a full batch between native
// points.
func (s *Scraper) protoFamily(mf *dto.MetricFamily, cb chunker, keep func(string, []Label) bool, emit func(Sample) error, baseSamples int, flushIfFull func() error) (protoCounts, int, error) {
	var c protoCounts
	malformed := 0
	name := mf.GetName()
	// The family's HELP/UNIT ride on every sample, exactly as the text path
	// carries them from the "# HELP"/"# UNIT" comments.
	help, unit := mf.GetHelp(), mf.GetUnit()
	// One reusable exemplar per family: a Sample only borrows it for the emit
	// call (the converter deep-copies the ones it keeps).
	var ex Exemplar
	for _, m := range mf.GetMetric() {
		labels := protoLabels(m)
		ts := m.GetTimestampMs()
		switch mf.GetType() {
		case dto.MetricType_COUNTER:
			cnt := m.GetCounter()
			smp := Sample{Name: name, Family: name, Role: RoleCounter, Labels: labels, Value: cnt.GetValue(), TimestampMs: ts, Help: help, Unit: unit}
			smp.Exemplar = s.protoExemplar(cnt.GetExemplar(), &ex)
			if err := emit(smp); err != nil {
				return c, malformed, err
			}
		case dto.MetricType_GAUGE, dto.MetricType_UNTYPED:
			v := m.GetGauge().GetValue()
			if mf.GetType() == dto.MetricType_UNTYPED {
				v = m.GetUntyped().GetValue()
			}
			if err := emit(Sample{Name: name, Family: name, Role: RoleGauge, Labels: labels, Value: v, TimestampMs: ts, Help: help, Unit: unit}); err != nil {
				return c, malformed, err
			}
		case dto.MetricType_SUMMARY:
			sum := m.GetSummary()
			for _, q := range sum.GetQuantile() {
				ql := append(labels[:len(labels):len(labels)], Label{Name: "quantile", Value: formatFloat(q.GetQuantile())})
				if err := emit(Sample{Name: name, Family: name, Role: RoleSummaryQuantile, Labels: ql, Value: q.GetValue(), TimestampMs: ts, Help: help, Unit: unit}); err != nil {
					return c, malformed, err
				}
			}
			if err := emit(Sample{Name: name + "_sum", Family: name, Role: RoleSummarySum, Labels: labels, Value: sum.GetSampleSum(), TimestampMs: ts, Help: help, Unit: unit}); err != nil {
				return c, malformed, err
			}
			if err := emit(Sample{Name: name + "_count", Family: name, Role: RoleSummaryCount, Labels: labels, Value: float64(sum.GetSampleCount()), TimestampMs: ts, Help: help, Unit: unit}); err != nil {
				return c, malformed, err
			}
		case dto.MetricType_HISTOGRAM, dto.MetricType_GAUGE_HISTOGRAM:
			h := m.GetHistogram()
			if isNative(h) {
				c.samples++
				// MaxSamples bounds native points too (they bypass emit).
				if s.cfg.MaxSamples > 0 && baseSamples+c.samples > s.cfg.MaxSamples {
					return c, malformed, ErrTooManySamples
				}
				if !keep(name, labels) {
					continue
				}
				if !s.addNativeHistogram(cb, name, metricMeta{help: help, unit: unit}, labels, h, ts) {
					malformed++
					continue
				}
				if err := flushIfFull(); err != nil {
					return c, malformed, err
				}
				continue
			}
			for _, b := range h.GetBucket() {
				bl := append(labels[:len(labels):len(labels)], Label{Name: "le", Value: formatFloat(b.GetUpperBound())})
				smp := Sample{Name: name + "_bucket", Family: name, Role: RoleHistogramBucket, Labels: bl, Value: float64(b.GetCumulativeCount()), TimestampMs: ts, Help: help, Unit: unit}
				smp.Exemplar = s.protoExemplar(b.GetExemplar(), &ex)
				if err := emit(smp); err != nil {
					return c, malformed, err
				}
			}
			if err := emit(Sample{Name: name + "_sum", Family: name, Role: RoleHistogramSum, Labels: labels, Value: h.GetSampleSum(), TimestampMs: ts, Help: help, Unit: unit}); err != nil {
				return c, malformed, err
			}
			if err := emit(Sample{Name: name + "_count", Family: name, Role: RoleHistogramCount, Labels: labels, Value: float64(h.GetSampleCount()), TimestampMs: ts, Help: help, Unit: unit}); err != nil {
				return c, malformed, err
			}
		default:
			malformed++
		}
	}
	return c, malformed, nil
}

// protoExemplar converts a protobuf exemplar into the shape the text path
// produces, reusing the caller's scratch (a Sample only borrows its exemplar
// for the emit call). nil when the target sent none or -scrape-exemplars is
// off — the SAME gate the OpenMetrics text path uses, so the two formats agree
// on what the flag means.
func (s *Scraper) protoExemplar(pe *dto.Exemplar, scratch *Exemplar) *Exemplar {
	if pe == nil || !s.cfg.Exemplars {
		return nil
	}
	scratch.Labels = scratch.Labels[:0]
	for _, lp := range pe.GetLabel() {
		scratch.Labels = append(scratch.Labels, Label{Name: lp.GetName(), Value: lp.GetValue()})
	}
	scratch.Value = pe.GetValue()
	scratch.TimestampMs = 0
	if t := pe.GetTimestamp(); t.IsValid() {
		scratch.TimestampMs = t.AsTime().UnixMilli()
	}
	return scratch
}

// isNative reports whether a histogram carries native (exponential) data:
// a schema plus span-encoded buckets or a zero bucket. NHCB (custom bounds,
// schema -53) is NOT native-exponential and falls back to classic buckets.
//
// A NIL histogram is not native. That case is reachable from the wire: a
// HISTOGRAM (or GAUGE_HISTOGRAM) family whose Metric omits the histogram
// submessage yields nil from GetHistogram(), and the raw field reads below
// (h.Schema, h.ZeroThreshold, h.ZeroCount) would dereference it — the generated
// GetSchema() above is nil-safe and hides that. Nothing in the agent recovers a
// panic, so a scraped target could crash the process with a few bytes of
// malformed protobuf and hold the node's DaemonSet in CrashLoopBackOff, since
// the same target is re-scraped every cycle. The classic fallback below is all
// nil-safe getters and degrades to an empty histogram, which is what a family
// carrying no data should produce.
func isNative(h *dto.Histogram) bool {
	if h == nil {
		return false
	}
	if h.GetSchema() == -53 {
		return false
	}
	return (h.Schema != nil && (len(h.GetPositiveSpan()) > 0 || len(h.GetNegativeSpan()) > 0)) ||
		h.ZeroThreshold != nil || h.ZeroCount != nil
}

// addNativeHistogram appends one exponential histogram point to the
// batcher; false = undecodable (counted malformed by the caller).
func (s *Scraper) addNativeHistogram(cb chunker, name string, meta metricMeta, labels []Label, h *dto.Histogram, ts int64) bool {
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
	// A native histogram carries its exemplars on the family message rather
	// than per bucket; they are point-scoped either way.
	var exemplars []Exemplar
	if s.cfg.Exemplars {
		var scratch Exemplar
		for _, pe := range h.GetExemplars() {
			if len(exemplars) >= maxExemplarsPerPoint {
				break
			}
			if e := s.protoExemplar(pe, &scratch); e != nil {
				exemplars = append(exemplars, copyExemplar(*e))
			}
		}
	}
	eb.addExponential(name, expPoint{
		labels:    labels,
		meta:      meta,
		exemplars: exemplars,
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
	meta      metricMeta
	exemplars []Exemplar
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

// expSink is a chunker that can take exponential histogram points (the plain
// and split batchers; the cadvisor batcher does not — the kubelet scrape
// stays on the text exposition).
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
