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
	"strconv"

	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// acceptProto is the Accept header offering protobuf exposition first.
const acceptProto = "application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited;q=1," +
	"application/openmetrics-text;version=1.0.0;q=0.8,text/plain;version=0.0.4;q=0.5"

// maxProtoMessageBytes bounds one delimited MetricFamily message.
const maxProtoMessageBytes = 64 << 20

// protoPresizeBytes bounds what an UNVERIFIED length prefix may allocate before
// the target has delivered a single byte of the message. See readDelimited.
const protoPresizeBytes = 64 << 10

// readDelimited reads the n-byte message body into dst, growing the buffer only
// as the target actually produces bytes.
//
// The length is the TARGET's claim, and this scrape is issued to whatever a
// pod annotation or a ServiceMonitor points at — not necessarily something the
// cluster's operator wrote. Sizing the destination from that claim let a target
// declare the 64 MiB cap and send nothing, allocating 64 MiB per concurrent
// scrape for free (the scraper runs targets in parallel, so it multiplies). The
// same reasoning, and the same shape, as otlpingest.readAllCapped and its
// maxPresizeBytes — the ingest path fixed this for bodies it receives; a scrape
// response is the identical hazard pointing outward.
//
// Growth doubles from a bounded head, so a legitimate large family still costs
// O(log n) allocations, and the buffer is returned for reuse across messages.
func readDelimited(r io.Reader, dst []byte, n int) ([]byte, error) {
	start := n
	if start > protoPresizeBytes {
		start = protoPresizeBytes
	}
	if cap(dst) < start {
		dst = make([]byte, 0, start)
	}
	dst = dst[:0]
	for len(dst) < n {
		if len(dst) == cap(dst) {
			next := cap(dst) * 2
			if next > n {
				next = n
			}
			grown := make([]byte, len(dst), next)
			copy(grown, dst)
			dst = grown
		}
		want := cap(dst)
		if want > n {
			want = n
		}
		m, err := io.ReadFull(r, dst[len(dst):want])
		dst = dst[:len(dst)+m]
		if err != nil {
			return dst, err
		}
	}
	return dst, nil
}

// maxExpBuckets bounds the dense expansion of span-encoded buckets: spans
// can declare arbitrary gaps, and a hostile exposition must not allocate
// unbounded bucket slices.
const maxExpBuckets = 4096

// parseProtoAndExport consumes a delimited-protobuf exposition. Classic
// families flow through the same converter/filter machinery as text samples
// (ss.accept); native histograms go straight to the batcher as exponential
// histogram points. The per-scrape policy — filter/relabel drops, MaxSamples,
// chunk flushing, the salvage-on-abort — is the shared scrapeSession's; only
// the format loop lives here. malformed includes the converter's count on
// every return path.
func (s *Scraper) parseProtoAndExport(ss *scrapeSession, body io.Reader) (malformed int, err error) {
	defer func() {
		malformed += ss.conv.malformed
		if err != nil {
			// Salvage on ANY abort (sample limit, truncated body, read timeout
			// mid-body, over-cap message), exactly as the text path does.
			ss.salvage()
		}
	}()

	br := bufio.NewReaderSize(body, 64*1024)
	var buf []byte
	var mf dto.MetricFamily
	for {
		n, rerr := binary.ReadUvarint(br)
		if rerr != nil {
			if rerr == io.EOF {
				break
			}
			return malformed, rerr
		}
		if n > maxProtoMessageBytes {
			return malformed, fmt.Errorf("proto message of %d bytes exceeds the cap", n)
		}
		var rerr2 error
		if buf, rerr2 = readDelimited(br, buf, int(n)); rerr2 != nil {
			return malformed, rerr2
		}
		mf.Reset()
		if perr := proto.Unmarshal(buf, &mf); perr != nil {
			malformed++
			continue
		}
		bad, ferr := s.protoFamily(&mf, ss)
		malformed += bad
		if ferr != nil {
			return malformed, ferr
		}
		if ferr := ss.flushIfFull(); ferr != nil {
			return malformed, ferr
		}
	}
	if ferr := ss.conv.finish(); ferr != nil {
		return malformed, ferr
	}
	return malformed, nil
}

// protoFamily converts one MetricFamily. Native points bypass ss.accept (they
// do not flow through the converter), so they charge MaxSamples via
// ss.countNative and flush a full batch between points via ss.flushIfFull.
func (s *Scraper) protoFamily(mf *dto.MetricFamily, ss *scrapeSession) (int, error) {
	malformed := 0
	name := mf.GetName()
	// The text front structurally cannot produce a nameless sample (the
	// grammar rejects the line), but an unset proto `name` decodes cleanly —
	// with type defaulting to COUNTER — and would export an empty-named OTLP
	// metric that a downstream OTLP→Prometheus translation rejects
	// per-series. Reject the family as malformed, mirroring the text front.
	if name == "" {
		return len(mf.GetMetric()), nil
	}
	// The family's HELP/UNIT ride on every sample, exactly as the text path
	// carries them from the "# HELP"/"# UNIT" comments.
	help, unit := mf.GetHelp(), mf.GetUnit()
	// One reusable exemplar per family: a Sample only borrows it for the emit
	// call (the converter deep-copies the ones it keeps).
	var ex Exemplar
	typ := mf.GetType()
	// The component names are FAMILY-invariant, so they are built once here
	// rather than once per Metric — a 500-metric family rebuilt three of them
	// 500 times each, for nothing.
	var nameBucket, nameSum, nameCount, nameGSum, nameGCount string
	switch typ {
	case dto.MetricType_SUMMARY:
		nameSum, nameCount = name+"_sum", name+"_count"
	case dto.MetricType_HISTOGRAM:
		nameBucket, nameSum, nameCount = name+"_bucket", name+"_sum", name+"_count"
	case dto.MetricType_GAUGE_HISTOGRAM:
		nameBucket, nameGSum, nameGCount = name+"_bucket", name+"_gsum", name+"_gcount"
	}
	// Bucket bounds and quantiles repeat verbatim on every Metric of the
	// family; the memo renders each distinct value once.
	var floats floatStrings
	// Resolved ONCE per family, never per Metric: see familyIsNative.
	native := typ == dto.MetricType_HISTOGRAM && familyIsNative(mf)
	var droppedClassic, droppedNHCB int
	defer func() {
		if droppedClassic > 0 {
			obs.ScrapeHistogramMixed.WithLabelValues(ss.pipeline, "classic").Add(float64(droppedClassic))
		}
		if droppedNHCB > 0 {
			obs.ScrapeHistogramMixed.WithLabelValues(ss.pipeline, "nhcb").Add(float64(droppedNHCB))
		}
	}()
	for _, m := range mf.GetMetric() {
		labels, ok := protoLabels(m)
		if !ok {
			malformed++
			continue
		}
		ts := m.GetTimestampMs()
		switch typ {
		case dto.MetricType_COUNTER:
			cnt := m.GetCounter()
			smp := Sample{Name: name, Family: name, Role: RoleCounter, Labels: labels, Value: cnt.GetValue(), TimestampMs: ts, Help: help, Unit: unit}
			smp.Exemplar = s.protoExemplar(cnt.GetExemplar(), &ex)
			if err := ss.accept(smp); err != nil {
				return malformed, err
			}
		case dto.MetricType_GAUGE, dto.MetricType_UNTYPED:
			v := m.GetGauge().GetValue()
			if typ == dto.MetricType_UNTYPED {
				v = m.GetUntyped().GetValue()
			}
			if err := ss.accept(Sample{Name: name, Family: name, Role: RoleGauge, Labels: labels, Value: v, TimestampMs: ts, Help: help, Unit: unit}); err != nil {
				return malformed, err
			}
		case dto.MetricType_SUMMARY:
			sum := m.GetSummary()
			for _, q := range sum.GetQuantile() {
				ql := append(labels[:len(labels):len(labels)], Label{Name: "quantile", Value: floats.get(q.GetQuantile())})
				if err := ss.accept(Sample{Name: name, Family: name, Role: RoleSummaryQuantile, Labels: ql, Value: q.GetValue(), TimestampMs: ts, Help: help, Unit: unit}); err != nil {
					return malformed, err
				}
			}
			if err := ss.accept(Sample{Name: nameSum, Family: name, Role: RoleSummarySum, Labels: labels, Value: sum.GetSampleSum(), TimestampMs: ts, Help: help, Unit: unit}); err != nil {
				return malformed, err
			}
			if err := ss.accept(Sample{Name: nameCount, Family: name, Role: RoleSummaryCount, Labels: labels, Value: float64(sum.GetSampleCount()), TimestampMs: ts, Help: help, Unit: unit}); err != nil {
				return malformed, err
			}
		case dto.MetricType_GAUGE_HISTOGRAM:
			// OTLP has no gauge-histogram shape, so every mapping is lossy and
			// the question is only which loss lies. A gauge histogram's buckets
			// are a CURRENT snapshot and may DECREASE, so shipping them as the
			// cumulative monotonic histogram (or exponential point) the
			// HISTOGRAM path builds misdeclares their temporality and makes
			// every downstream rate()/delta read nonsense. The components ship
			// as GAUGES under the OpenMetrics names instead — which is exactly
			// what the text front produces for `# TYPE x gaugehistogram` (an
			// unrecognised TYPE degrades to untyped, i.e. gauges), so the two
			// formats describe one target the same way. A NATIVE gauge histogram
			// keeps only its _gcount/_gsum: its distribution has no OTLP shape
			// that would not lie about temporality.
			h := m.GetHistogram()
			for _, b := range h.GetBucket() {
				bl := append(labels[:len(labels):len(labels)], Label{Name: "le", Value: floats.get(b.GetUpperBound())})
				if err := ss.accept(Sample{Name: nameBucket, Family: name, Role: RoleGauge, Labels: bl, Value: bucketCount(b), TimestampMs: ts, Help: help, Unit: unit}); err != nil {
					return malformed, err
				}
			}
			if err := ss.accept(Sample{Name: nameGSum, Family: name, Role: RoleGauge, Labels: labels, Value: h.GetSampleSum(), TimestampMs: ts, Help: help, Unit: unit}); err != nil {
				return malformed, err
			}
			if err := ss.accept(Sample{Name: nameGCount, Family: name, Role: RoleGauge, Labels: labels, Value: sampleCount(h), TimestampMs: ts, Help: help, Unit: unit}); err != nil {
				return malformed, err
			}
		case dto.MetricType_HISTOGRAM:
			h := m.GetHistogram()
			if native {
				if r := reprOf(h); r != reprNative {
					// The family ships as ONE exponential histogram metric, and
					// this child is not carrying that shape: its points would be
					// refused by the batcher's type guard anyway. Counted so the
					// discard has an attributable signal.
					if r == reprNHCB {
						droppedNHCB++
					} else {
						droppedClassic++
					}
					continue
				}
				if err := ss.countNative(); err != nil {
					return malformed, err
				}
				if !ss.keep(name, labels) {
					continue
				}
				if !s.addNativeHistogram(ss.cb, name, metricMeta{help: help, unit: unit}, labels, h, ts) {
					malformed++
					continue
				}
				if err := ss.flushIfFull(); err != nil {
					return malformed, err
				}
				continue
			}
			if isNHCB(h) {
				// An NHCB message's bounds live in custom_values, and
				// client_model v0.6.2 — the version Prometheus itself pins —
				// does not generate that field: they are not reachable from this
				// message at all, so there is nothing to convert. Falling through
				// to the classic branch shipped a point with NO bounds and a
				// single overflow bucket, i.e. a correct count and sum beside a
				// distribution every histogram_quantile reads as nonsense, and
				// counted nothing. Refused instead, so the loss is visible.
				malformed++
				continue
			}
			for _, b := range h.GetBucket() {
				bl := append(labels[:len(labels):len(labels)], Label{Name: "le", Value: floats.get(b.GetUpperBound())})
				smp := Sample{Name: nameBucket, Family: name, Role: RoleHistogramBucket, Labels: bl, Value: bucketCount(b), TimestampMs: ts, Help: help, Unit: unit}
				smp.Exemplar = s.protoExemplar(b.GetExemplar(), &ex)
				if err := ss.accept(smp); err != nil {
					return malformed, err
				}
			}
			if err := ss.accept(Sample{Name: nameSum, Family: name, Role: RoleHistogramSum, Labels: labels, Value: h.GetSampleSum(), TimestampMs: ts, Help: help, Unit: unit}); err != nil {
				return malformed, err
			}
			if err := ss.accept(Sample{Name: nameCount, Family: name, Role: RoleHistogramCount, Labels: labels, Value: sampleCount(h), TimestampMs: ts, Help: help, Unit: unit}); err != nil {
				return malformed, err
			}
		default:
			malformed++
		}
	}
	return malformed, nil
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

// histRepr is the representation one HISTOGRAM message carries its
// distribution in. Each maps to a DIFFERENT OTLP metric type (or, for NHCB, to
// none at all), which is why the choice cannot be made per data point.
type histRepr uint8

const (
	reprClassic histRepr = iota // per-bucket `bucket` rows -> OTLP Histogram
	reprNative                  // span-encoded exponential -> OTLP ExponentialHistogram
	reprNHCB                    // custom-bucket native (schema -53) -> nothing, see isNHCB
)

func reprOf(h *dto.Histogram) histRepr {
	switch {
	case isNative(h):
		return reprNative
	case isNHCB(h):
		return reprNHCB
	default:
		return reprClassic
	}
}

// familyIsNative resolves native-vs-classic ONCE per family.
//
// The decision cannot be per Metric: one metric NAME carries exactly one OTLP
// type, so a family whose children mix representations had its type fixed by
// whichever child was converted first and the other's points were then dropped
// by the batcher's type guard — visible only as obs.ScrapeCollisions, a bare
// counter with no pipeline or target label, so a whole classic child could
// vanish with nothing naming it. Native wins the family, which is Prometheus's
// own preference; the loser is counted in obs.ScrapeHistogramMixed.
func familyIsNative(mf *dto.MetricFamily) bool {
	for _, m := range mf.GetMetric() {
		if isNative(m.GetHistogram()) {
			return true
		}
	}
	return false
}

// isNHCB reports whether a histogram carries CUSTOM-BUCKET native data: schema
// -53, span-encoded counts, and bounds that live in the message's custom_values
// field — which client_model v0.6.2 (the version prometheus/prometheus itself
// pins, and whose own protobuf parser therefore does not read NHCB either) does
// not generate. The bounds are unreachable, so such a message is refused and
// counted rather than converted: see the HISTOGRAM branch of protoFamily.
//
// A message that ALSO carries classic `bucket` rows is not NHCB by this test —
// those bounds ARE readable, and the classic path uses them.
func isNHCB(h *dto.Histogram) bool {
	if h == nil || h.GetSchema() != -53 || len(h.GetBucket()) > 0 {
		return false
	}
	return len(h.GetPositiveSpan()) > 0 || len(h.GetNegativeSpan()) > 0
}

// isNative reports whether a histogram carries native (exponential) data:
// span-encoded buckets or a non-empty zero bucket. NHCB (custom bounds,
// schema -53) is NOT native-exponential — see isNHCB for what happens to it.
//
// The test is on the VALUES, mirroring Prometheus's own isNativeHistogram
// (model/textparse/protobufparse.go). Testing FIELD PRESENCE instead read a
// plain classic histogram from an exporter that materialises default fields —
// schema/zero_threshold/zero_count all set, all zero — as an EMPTY native one:
// the classic buckets were never converted and the point that shipped carried
// count and sum with no buckets at all. The ecosystem contract runs the other
// way: a native histogram with no observations yet declares itself with a
// zero-length no-op span (client_golang emits one) precisely so that a
// value-based test still finds it.
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
	return len(h.GetPositiveSpan()) > 0 || len(h.GetNegativeSpan()) > 0 ||
		h.GetZeroThreshold() > 0 || h.GetZeroCount() > 0
}

// sampleCount, bucketCount and zeroBucketCount read a histogram's observation
// counts. dto's sample_count_float / cumulative_count_float / zero_count_float
// OVERRIDE their integer counterparts when > 0 ("Overrides sample_count if > 0",
// says the message) — that is how a FLOAT histogram carries them, classic
// buckets included, and it is the pair Prometheus keys its own float variant on
// (model/textparse/protobufparse.go). EVERY count read goes through these:
// reading the integer field of a float exposition yields a zero for every
// observation the target made, with no counter moving.
func sampleCount(h *dto.Histogram) float64 {
	if f := h.GetSampleCountFloat(); f > 0 {
		return f
	}
	return float64(h.GetSampleCount())
}

func bucketCount(b *dto.Bucket) float64 {
	if f := b.GetCumulativeCountFloat(); f > 0 {
		return f
	}
	return float64(b.GetCumulativeCount())
}

func zeroBucketCount(h *dto.Histogram) float64 {
	if f := h.GetZeroCountFloat(); f > 0 {
		return f
	}
	return float64(h.GetZeroCount())
}

// roundCount folds an observation count into the uint64 OTLP buckets take.
// Rounding is the whole cost of accepting a float native histogram, and it is
// bounded: a negative, NaN or out-of-range value is not a count and is refused
// (counted malformed by the caller) rather than wrapped into a ~1.8e19 bucket.
func roundCount(f float64) (uint64, bool) {
	if math.IsNaN(f) || f < 0 || f >= math.MaxUint64 {
		return 0, false
	}
	return uint64(math.Round(f)), true
}

// addNativeHistogram appends one exponential histogram point to the
// batcher; false = undecodable (counted malformed by the caller).
func (s *Scraper) addNativeHistogram(cb chunker, name string, meta metricMeta, labels []Label, h *dto.Histogram, ts int64) bool {
	eb, ok := cb.(expSink)
	if !ok {
		return false // batcher variant without exponential support
	}
	pos, posOff, ok := decodeSpans(h.GetPositiveSpan(), h.GetPositiveDelta(), h.GetPositiveCount())
	if !ok {
		return false
	}
	neg, negOff, ok := decodeSpans(h.GetNegativeSpan(), h.GetNegativeDelta(), h.GetNegativeCount())
	if !ok {
		return false
	}
	count, ok := roundCount(sampleCount(h))
	if !ok {
		return false
	}
	zero, ok := roundCount(zeroBucketCount(h))
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
		zeroCount: zero,
		zeroTh:    h.GetZeroThreshold(),
		count:     count,
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
//
// An INTEGER histogram delta-encodes its bucket counts in deltas; a FLOAT one
// carries them ABSOLUTELY in the parallel *_count field ("Absolute count of
// each bucket", says the message), which is what Prometheus reads into a
// FloatHistogram. Both are well-formed, so the encoding follows what the
// message actually carries: reading only deltas made every float histogram
// look like a span run with missing deltas — counted malformed and dropped
// WHOLE, count and sum included.
//
// The running index is int64 because it is driven by int32 offsets and uint32
// lengths the TARGET chooses: in int32 arithmetic a hostile span pair wrapped
// past the gap guard and produced a point whose buckets sat at a
// mathematically impossible index. An index (or an OTLP offset) leaving int32
// range is refused, never wrapped.
func decodeSpans(spans []*dto.BucketSpan, deltas []int64, absolute []float64) (counts []uint64, offset int32, ok bool) {
	if len(spans) == 0 {
		return nil, 0, true
	}
	// Deltas win when a message carries both: that is the integer encoding.
	floats := len(deltas) == 0 && len(absolute) > 0
	var idx, start int64
	first := true
	var cur int64
	di := 0
	for _, sp := range spans {
		idx += int64(sp.GetOffset())
		if idx < math.MinInt32 || idx > math.MaxInt32 {
			return nil, 0, false
		}
		if first {
			start = idx
			first = false
		} else {
			gap := idx - start - int64(len(counts))
			if gap < 0 || int64(len(counts))+gap > maxExpBuckets {
				return nil, 0, false
			}
			counts = append(counts, make([]uint64, gap)...)
		}
		for i := uint32(0); i < sp.GetLength(); i++ {
			var v uint64
			if floats {
				if di >= len(absolute) {
					return nil, 0, false
				}
				var vok bool
				if v, vok = roundCount(absolute[di]); !vok {
					return nil, 0, false
				}
			} else {
				if di >= len(deltas) {
					return nil, 0, false
				}
				cur += deltas[di]
				if cur < 0 {
					return nil, 0, false
				}
				v = uint64(cur)
			}
			di++
			if len(counts) >= maxExpBuckets {
				return nil, 0, false
			}
			counts = append(counts, v)
		}
		// The next span's offset is relative to the index AFTER this span.
		idx += int64(sp.GetLength())
		if idx < math.MinInt32 || idx > math.MaxInt32 {
			return nil, 0, false
		}
	}
	if start-1 < math.MinInt32 {
		return nil, 0, false
	}
	return counts, int32(start - 1), true
}

// protoLabels converts a metric's label pairs. ok is false when a pair has an
// empty NAME — an unset proto field the text grammar cannot express — which
// would otherwise become an empty OTLP attribute key; the caller drops the
// metric as malformed, mirroring the text front's whole-line reject.
func protoLabels(m *dto.Metric) ([]Label, bool) {
	lps := m.GetLabel()
	if len(lps) == 0 {
		return nil, true
	}
	out := make([]Label, 0, len(lps))
	for _, lp := range lps {
		if lp.GetName() == "" {
			return nil, false
		}
		out = append(out, Label{Name: lp.GetName(), Value: lp.GetValue()})
	}
	return out, true
}

// floatStrings renders the float64 label values of one family — bucket bounds
// and summary quantiles — once per distinct value.
//
// Both are FAMILY-invariant (every Metric of a family repeats the same bound
// set), and the fmt.Sprintf this replaces cost a string plus the interface
// boxing of its argument on every bucket of every metric. The rendering is
// unchanged: fmt's %v for a float64 IS %g at the default precision, so the old
// integral-vs-not branch produced the same shortest-round-trip form on both
// sides, which is what strconv writes here.
type floatStrings struct {
	buf  []byte
	memo map[float64]string
}

func (f *floatStrings) get(v float64) string {
	if s, ok := f.memo[v]; ok {
		return s
	}
	f.buf = strconv.AppendFloat(f.buf[:0], v, 'g', -1, 64)
	s := string(f.buf)
	if f.memo == nil {
		f.memo = make(map[float64]string, 16)
	}
	// NaN never equals itself, so a bound set of NaNs would insert one entry per
	// row: bounded like the text parser's own interning.
	if len(f.memo) < maxInternedValues {
		f.memo[v] = s
	}
	return s
}
