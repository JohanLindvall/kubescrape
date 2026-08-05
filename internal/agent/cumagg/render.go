package cumagg

import (
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// AppendKeyPart appends one length-prefixed value to a series key so distinct
// tuples never collide (("a","bc") vs ("ab","c")). Building the key on a stack
// buffer and looking it up with Store.AdmitLocked keeps a warm series
// allocation-free.
func AppendKeyPart(dst []byte, v string) []byte {
	dst = strconv.AppendInt(dst, int64(len(v)), 10)
	dst = append(dst, ':')
	return append(dst, v...)
}

// BucketIndex is the index of the first bound >= v, or the +Inf overflow bucket.
func BucketIndex(bounds []float64, v float64) int {
	for i, b := range bounds {
		if v <= b {
			return i
		}
	}
	return len(bounds)
}

// MaxLabelBytes bounds one label value. The cardinality cap counts SERIES, not
// bytes, and these values arrive from unauthenticated listeners: a sender
// controlling a span name or a peer attribute could otherwise pin
// maxCardinality x arbitrary length in memory for the whole staleAfter and
// re-render it into every export. The OTel Collector's spanmetrics connector
// and Tempo truncate for the same reason.
const MaxLabelBytes = 256

// Trunc bounds a value that is about to be CONSUMED and dropped — the series
// key a caller builds on the stack, which the map copies on insert. Slicing a
// Go string allocates nothing but keeps the WHOLE original alive, so it is only
// safe where the result does not outlive the payload it points into.
//
// Every value that goes into a key must be cut at exactly the length the
// rendered label is cut at. Keying on the untruncated value instead makes the
// key FINER than the data points it identifies: two spans differing only past
// the cut hold two series that render byte-identical attribute sets — a
// duplicate series in one payload, which downstream reads as a conflict rather
// than as extra detail — and the retained key is then as long as the sender
// cared to make it, leaking through the bound truncation exists to impose.
func Trunc(v string) string {
	if len(v) <= MaxLabelBytes {
		return v
	}
	return v[:truncLen(v)]
}

// truncLen is where a value longer than MaxLabelBytes is cut: the byte bound,
// backed off to the start of any rune straddling it.
//
// A blind v[:MaxLabelBytes] splits a multi-byte rune whenever the 256th byte
// lands inside one, and the result is an invalid-UTF-8 string marshalled
// straight into an OTLP attribute — protobuf `string` fields are defined as
// UTF-8, so a strict collector may reject the whole payload and a lenient one
// stores mojibake. Any non-ASCII dimension (a span name or peer.service with a
// hostname in a non-Latin script) hits it.
//
// It is shared by Trunc and Retain deliberately: they must cut at EXACTLY the
// same length, or the key and the rendered label disagree and one edge holds
// two series that render byte-identical attribute sets.
func truncLen(v string) int {
	end := MaxLabelBytes
	for end > 0 && v[end]&0xC0 == 0x80 {
		end--
	}
	if end == 0 {
		// Every byte of the prefix is a continuation byte: the value was
		// already invalid UTF-8 and has no boundary to back off to. Cut at the
		// byte bound rather than returning an empty label, which would blank a
		// dimension and merge two series that differ only in it.
		return MaxLabelBytes
	}
	return end
}

// Retain is Trunc for a value something KEEPS: a series' label set, or a
// half-edge waiting for its partner. The slice Trunc returns still points into
// the sender's string, so retaining it pins the whole thing — a 4 MiB span name
// held for staleAfter by a 256-byte label, which is precisely the bound
// MaxLabelBytes claims to provide and did not. Cloning only on the truncating
// branch leaves the ordinary short value allocation-free, and this runs once per
// ADMITTED series, never per span.
func Retain(v string) string {
	if len(v) <= MaxLabelBytes {
		return v
	}
	return strings.Clone(v[:truncLen(v)])
}

// AttrStr is an attribute's value as a string, or "" when it is absent.
func AttrStr(m pcommon.Map, key string) string {
	if v, ok := m.Get(key); ok {
		return v.AsString()
	}
	return ""
}

// SpanSeconds is a span's own measured duration. An unset or clock-skewed end
// yields 0 rather than a negative duration, which is meaningless and which no
// histogram can hold.
func SpanSeconds(span ptrace.Span) float64 {
	end, start := span.EndTimestamp(), span.StartTimestamp()
	if end <= start {
		return 0
	}
	return float64(end-start) / float64(time.Second)
}

// Exemplar is the evidence kept for one latency bucket: a request that landed
// in it, so the histogram links to the trace that explains it.
type Exemplar struct {
	Set     bool
	Value   float64
	TS      pcommon.Timestamp
	TraceID pcommon.TraceID
	SpanID  pcommon.SpanID
}

// RecordExemplar keeps this sample as bucket idx's exemplar, replacing whatever
// was there: the newest is what an operator looking at a live graph wants, and
// one per bucket bounds the cost at buckets per series per export. It returns
// ex, allocating the per-bucket slice on first use — an aggregate with
// exemplars off, or whose spans carry no trace id, never pays for one.
//
// A sample with no trace id is SKIPPED rather than recorded with a zero one: an
// exemplar whose trace cannot be looked up is a dead link in the UI.
func RecordExemplar(ex []Exemplar, buckets, idx int, v float64, ts pcommon.Timestamp, tid pcommon.TraceID, sid pcommon.SpanID) []Exemplar {
	if tid.IsEmpty() {
		return ex
	}
	if ex == nil {
		ex = make([]Exemplar, buckets)
	}
	ex[idx] = Exemplar{Set: true, Value: v, TS: ts, TraceID: tid, SpanID: sid}
	return ex
}

// ClearExemplars marks every slot unset, keeping the slice. Call it only for a
// DELIVERED payload: a failed send keeps its evidence for the retry.
func ClearExemplars(ex []Exemplar) {
	for i := range ex {
		ex[i].Set = false
	}
}

// PutExemplar appends e to a data point's exemplars.
func PutExemplar(dst pmetric.ExemplarSlice, e Exemplar) {
	x := dst.AppendEmpty()
	x.SetDoubleValue(e.Value)
	x.SetTimestamp(e.TS)
	x.SetTraceID(e.TraceID)
	x.SetSpanID(e.SpanID)
}

// SumMetric appends a monotonic cumulative Sum shell and returns its data point
// slice. An empty unit sets none.
//
// Monotonic and cumulative are not decoration: the Prometheus translation keys
// its `_total` suffix rule off the metric SHAPE, so a name that already carries
// the suffix (servicegraph's Tempo-verbatim ones) survives it unchanged only
// while the shape stays this.
func SumMetric(sm pmetric.ScopeMetrics, name, desc, unit string) pmetric.NumberDataPointSlice {
	m := sm.Metrics().AppendEmpty()
	m.SetName(name)
	m.SetDescription(desc)
	if unit != "" {
		m.SetUnit(unit)
	}
	s := m.SetEmptySum()
	s.SetIsMonotonic(true)
	s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	return s.DataPoints()
}

// HistMetric appends a cumulative Histogram shell in seconds and returns its
// data point slice. The unit is what an OTLP-native consumer displays, and it is
// the token the Prometheus unit rule finds already present and leaves alone.
func HistMetric(sm pmetric.ScopeMetrics, name, desc string) pmetric.HistogramDataPointSlice {
	m := sm.Metrics().AppendEmpty()
	m.SetName(name)
	m.SetDescription(desc)
	m.SetUnit("s")
	h := m.SetEmptyHistogram()
	h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	return h.DataPoints()
}
