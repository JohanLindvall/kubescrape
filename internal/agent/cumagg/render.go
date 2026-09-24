package cumagg

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// AppendKeyPart appends one length-prefixed value to a series key so distinct
// tuples never collide (("a","bc") vs ("ab","c")). Building the key on a stack
// buffer and looking it up with Store.AdmitLocked keeps a warm series
// allocation-free.
//
// The length is a UVARINT, not decimal. The prefix's only job is to make the
// concatenation injective — nothing ever reads a key back, and it is never
// persisted or put on the wire — so the encoding is free to be whatever is
// cheapest, and the decimal one was not cheap: this is called four to a dozen
// times per SPAN and per completed edge on the trace tier's receive path, and
// a profile of the fold at the 20000-series cardinality cap charged 10% of it
// to strconv.AppendInt's division loop alone. A uvarint is one byte for every
// value shorter than 128 (which, with cumagg.Trunc cutting at 256, is very
// nearly all of them), and the two or three bytes it saves per part also
// shorten the key the map then hashes and compares.
//
// Injective for the same reason the decimal form was: a uvarint is
// prefix-free, so the boundary between the length and the value, and between
// one part and the next, is unambiguous. TestKeyPartsAreInjective pins it at
// the encoding's own length boundaries.
func AppendKeyPart(dst []byte, v string) []byte {
	n := uint(len(v))
	for n >= 0x80 {
		dst = append(dst, byte(n)|0x80)
		n >>= 7
	}
	dst = append(dst, byte(n))
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
// MaxLabelBytes claims to provide and did not.
//
// It clones only on the truncating branch, so an ordinary value costs a length
// compare and nothing else. Call it where a value is actually KEPT, not on the
// way past: a newly admitted series' labels (once per series), a half-edge the
// pairing store holds for a Wait, and servicegraph's service name (once per
// RESOURCE per push, shared by every half-edge that resource's spans store). A
// value that is only consumed — a key part, or the arriving half that completes
// a pair and is emitted on the spot — takes Trunc, and cloning it there would
// allocate per span for a string that is dropped at once.
func Retain(v string) string {
	if len(v) <= MaxLabelBytes {
		return v
	}
	return strings.Clone(v[:truncLen(v)])
}

// AttrStr is an attribute's value as a string (ValueStr), or "" when it is
// absent.
func AttrStr(m pcommon.Map, key string) string {
	if v, ok := m.Get(key); ok {
		return ValueStr(v)
	}
	return ""
}

// DimValue resolves one configured dimension: the span attribute first, the
// resource attribute as the fallback, and ok=false when neither is set. A span
// attribute that RENDERS empty (see rendersEmpty) falls through to the
// resource, exactly as comparing AttrStr to "" would.
//
// It is the ONE spelling of that precedence for both aggregators — a deliberate
// divergence from Tempo, which reads the resource first, and one the two had to
// agree on without anything tying them together: spanmetrics' key loop and
// label loop, and servicegraph's per-half dimensions, were three copies of it.
// Within spanmetrics a drift between the copies would make a series' KEY
// disagree with the labels it RENDERS (distinct tuples merging into one label
// set, or one tuple rendering two). It returns the Value rather than its string
// so a key can append it without materializing it (AppendValueKeyPart); DimStr
// is the rendered form.
func DimValue(spanAttrs, resAttrs pcommon.Map, key string) (pcommon.Value, bool) {
	if v, ok := spanAttrs.Get(key); ok && !rendersEmpty(v) {
		return v, true
	}
	return resAttrs.Get(key)
}

// DimStr is DimValue's label value, "" when neither the span nor the resource
// sets it.
func DimStr(spanAttrs, resAttrs pcommon.Map, key string) string {
	if v, ok := DimValue(spanAttrs, resAttrs, key); ok {
		return ValueStr(v)
	}
	return ""
}

// rendersEmpty reports whether v.AsString() is "", without calling it: an empty
// value, an empty string, or empty bytes (base64 of nothing). Every other type
// renders something — a number, a bool, and even an empty map or slice ("{}",
// "[]") — so TestRendersEmptyIsAsStringEmpty holds it to AsString for all of
// them.
func rendersEmpty(v pcommon.Value) bool {
	switch v.Type() {
	case pcommon.ValueTypeEmpty:
		return true
	case pcommon.ValueTypeStr:
		return v.Str() == ""
	case pcommon.ValueTypeBytes:
		return v.Bytes().Len() == 0
	}
	return false
}

// ValueStr is v.AsString() — byte for byte, so a key or label built from either
// is the same one — without the heap allocation AsString makes for the Int an
// operator most often configures as a dimension.
//
// pdata renders an Int through strconv.FormatInt, which serves 0-99 from a
// static table and ALLOCATES for everything else, so an HTTP or gRPC status
// code dimension (http.response.status_code=200) cost one allocation per span
// in spanmetrics and one per half-edge in servicegraph, on the two receive
// paths whose budget tests assert zero — tests whose fixtures only ever set
// string attributes, which is why they never saw it. [0, smallInts) is served
// from a table of static strings instead. That is also what makes it free where
// the value is RETAINED (servicegraph holds a half-edge's dimensions for a
// Wait): a table entry pins nothing. Every other type and range falls through
// to AsString — Bool is already allocation-free there (FormatBool returns
// constants), and pdata's Double formatting is internal to pcommon and not
// worth re-deriving at the risk of a key that disagrees with its label.
func ValueStr(v pcommon.Value) string {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return v.Str()
	case pcommon.ValueTypeInt:
		if n := v.Int(); n >= 0 && n < smallInts {
			return smallIntStrs()[n]
		}
	}
	return v.AsString()
}

// smallInts bounds ValueStr's table: every HTTP status code, every gRPC status
// code and every well-known port.
const smallInts = 1000

// smallIntStrs is ValueStr's table, built on first use so an importer that
// never renders an Int attribute pays nothing for it. The entries are slices of
// ONE backing string, so building it is a few allocations in the process' life
// rather than one per entry.
var smallIntStrs = sync.OnceValue(func() *[smallInts]string {
	var ends [smallInts]int
	buf := make([]byte, 0, 3*smallInts)
	for n := range smallInts {
		buf = strconv.AppendInt(buf, int64(n), 10)
		ends[n] = len(buf)
	}
	t := new([smallInts]string)
	all, start := string(buf), 0
	for n, end := range ends {
		t[n] = all[start:end]
		start = end
	}
	return t
})

// AppendValueKeyPart appends v as one key part: exactly
// AppendKeyPart(dst, Trunc(v.AsString())), byte for byte, so a key built from
// the Value and the label rendered from its string (ValueStr, cut by Retain)
// stay one function of the attribute — a key finer or coarser than its label is
// a duplicate series or a merged one. What it adds is that nothing is
// materialized on the way: an Int is formatted into a stack buffer at ANY
// magnitude, where ValueStr's table covers only small ones, so a key-only path
// (spanmetrics' per-span series key) pays nothing for an Int dimension of any
// value. TestAppendValueKeyPartMatchesTheRenderedString pins the equality for
// every value type.
func AppendValueKeyPart(dst []byte, v pcommon.Value) []byte {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return AppendKeyPart(dst, Trunc(v.Str()))
	case pcommon.ValueTypeInt:
		var digits [20]byte // len("-9223372036854775808")
		d := strconv.AppendInt(digits[:0], v.Int(), 10)
		// AppendKeyPart's uvarint length prefix, which for a length under 0x80
		// is the length itself in one byte — and an int64 is at most 20 digits.
		dst = append(dst, byte(len(d)))
		return append(dst, d...)
	}
	return AppendKeyPart(dst, Trunc(ValueStr(v)))
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

// ClampStart bounds a series' start timestamp by the point timestamp it is
// about to be stamped beside, and exists for one narrow race: Export reads the
// export clock ONCE, before the render's first hold of the series lock, so a
// series ADMITTED in that window — its Meta.Start stamped from a later clock
// read on a receive goroutine — would render StartTimestamp > Timestamp on its
// very first export. The window is microseconds and self-correcting, but the
// point is malformed: OTLP requires a cumulative point's start at or before its
// time, and a consumer may reject the payload or misread the inversion as a
// reset. Clamping the STAMP to ts is the OTLP first-point spelling of "the
// cumulative began now"; Meta.Start itself is untouched, so the true start
// renders from the next export on, whose ts lies past it. Call it at every
// site that writes a Meta.Start next to an export timestamp.
func ClampStart(start, ts pcommon.Timestamp) pcommon.Timestamp {
	if start > ts {
		return ts
	}
	return start
}
