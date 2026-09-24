package otlpingest

// The decoded-structure ESTIMATE, read off the WIRE bytes before the decode.
//
// The decoded budget (admit.go) is only a bound if it is charged BEFORE pdata
// materialises the payload. It used to be charged after: UnmarshalProto built
// the whole structure, and only then did a walk over the result decide whether
// it fit — so the budget bounded how long a decoded payload was RETAINED and
// nothing about the PEAK. Four pushes it refused were four payloads already on
// the heap together (measured: four refused 2.66 MB pushes, 242 MiB peak), and
// a single push was never refused before it had cost what it cost. The walk
// here runs on the bytes, after the nesting guard and before the decode, on
// both doors: servePush on HTTP, and the gRPC codec (depthGuardCodec), which is
// the only thing grpc-go runs between receiving a message and decoding it.
//
// It is SCHEMA-AWARE — it knows the OTLP message layouts pdata decodes — and
// charges every repeated sub-message the decode materialises, not only the
// resources, scopes and items the post-decode walk counted. That was the second
// half of the same hole: a KeyValue is two wire bytes and a 40-byte struct, an
// array element two bytes and a 16-byte slot, an exemplar or a span event two
// bytes and ~70-90 bytes, and none of them was charged at all. One 16 MiB push
// of empty attributes (~16 KB gzipped) decoded to 385 MiB of live heap and was
// charged 512 B; exemplars, span events and span links amplify further still
// (~37-45x the wire bytes).
//
// THE INVARIANT this walk is written against is the nesting guard's, turned
// around: IT MUST NEVER STOP EARLIER THAN PDATA DOES. Both walks read the
// payload in the same order, so as long as this one does not give up on bytes
// the decoder goes on to decode, it has counted everything the decoder built —
// including the structure a decode that FAILS part-way allocates before it
// fails. Stopping LATER (counting fields the decoder would refuse, or scanning
// on past a byte the decoder errors on) is merely an over-estimate. So the
// field parser (wireFields.next) shares consumeVarint with nestingOver, which is
// matched to pdata's own wire reader, and a field this walk does not
// recognise is skipped, never a reason to stop.
//
// What is charged is STRUCTURE, per the coefficients below. CONTENT — the
// strings and byte slices a decode copies out of the body — is still not, for
// the reason admit.go gives; the model is only honest because everything that
// is not a string copy is in here.
//
// Allocation-free and O(len(body)); it recurses only through the one cycle in
// the schema (AnyValue -> ArrayValue/KeyValueList -> AnyValue), whose depth the
// nesting guard has already bounded by the time this runs. Its constant is the
// nesting walk's order of magnitude (BenchmarkDecodedSize: ~170-310 MB/s on
// SDK-shaped bodies, measured on a loaded machine — quote the range), a
// fraction of the decode it precedes, and every byte it walks is already
// charged to the raw budget, which bounds all concurrent walks at once exactly
// as depth.go argues for its own.

import (
	"reflect"

	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

// What one decoded object costs on the heap, measured (Go 1.26, pdata v1.65) as
// the live-heap delta of UnmarshalProto with the result kept alive:
//
//	logs    200k resources x 1 tiny record   8.0 MB wire -> 97.9 MB  (489 B/resource-chain)
//	logs    1 resource x 400k tiny records   4.4 MB wire -> 61.3 MB  (153 B/record)
//	logs    60k records, 200 B body, 4 attrs 17.6 MB wire -> 38.9 MB (648 B/record, mostly content)
//	metrics 200k resources x 1 point        11.0 MB wire -> 131.5 MB (657 B/resource-chain)
//	metrics 1 resource x 400k points         8.8 MB wire -> 74.1 MB  (185 B/point)
//	traces  200k resources x 1 span         17.8 MB wire -> 139.5 MB (697 B/resource-chain)
//	traces  1 resource x 200k spans         12.0 MB wire -> 72.3 MB  (361 B/span)
//
// and, for the repeated sub-messages below the item (1M empty elements each,
// the shape that amplifies most): a KeyValue 41-70 B, an ArrayValue element
// 17 B, an exemplar 75-84 B, a span event 73 B, a span link 89 B, an entity
// ref 89 B, a summary quantile 25 B, a varint bucket count 10 B.
//
// The coefficients round those UP, and past the measured figure where the
// element lives in a slice grown by append: a decode appends one element at a
// time, so a list whose length an attacker picks just past a power of two
// retains up to twice its length in capacity. They are deliberately generous on
// the resource term too: resources are the multiplier a hostile sender reaches
// for (30 wire bytes each), records are what an honest one has a lot of, and an
// estimate that is generous where the attack lives and tight where the traffic
// lives sheds the right one. They are a MODEL of a shape that varies by an
// order of magnitude, not a measurement of any particular payload — which is
// why the refusal they drive stays retryable.
// TestWireEstimateCoversTheDecodedHeap pins each one against the heap a real
// decode retains, so a pdata upgrade that grows a struct fails a test rather
// than quietly re-opening the gap.
const (
	decodedResourceBytes = 512
	decodedScopeBytes    = 256
	// A log record, a metric shell, a data point or a span.
	decodedItemBytes = 256
	// A KeyValue (40 B) in a []KeyValue — attributes at every level, a kvlist
	// entry, metric metadata, an exemplar's filtered attributes. Measured at
	// 41 B in a long list and up to ~70 B in one whose length sits just past a
	// doubling (nine entries retain sixteen). 64 is deliberately BETWEEN the
	// two rather than past the worst: KeyValues are where honest traffic lives
	// too, and a 16 MiB push of ten-label points (~110 000 of them, ~110 MiB of
	// real heap) must still fit the budget alone. The most an attacker buys by
	// choosing list lengths is ~1.2x under-estimate on that one term.
	decodedAttrBytes = 64
	// The oneof wrapper an AnyValue allocates for a scalar (string, bool, int,
	// double, bytes, string-table index): an 8-16 B object per value.
	decodedValueBytes = 16
	// An AnyValue's array or kvlist: the oneof wrapper plus the container it
	// points at. A metric's data oneof (gauge, sum, ...) costs the same.
	decodedNodeBytes = 48
	// One AnyValue (16 B) in an ArrayValue's []AnyValue.
	decodedElemBytes = 32
	// An exemplar (72 B) in a []Exemplar.
	decodedExemplarBytes = 128
	// A span event, a span link, an entity ref: a pointer slot plus the struct.
	decodedEventBytes  = 96
	decodedLinkBytes   = 112
	decodedEntityBytes = 112
	// A summary quantile: a pointer slot plus a 16 B struct.
	decodedQuantileBytes = 32
	// One string header in an entity ref's key list ([]string).
	decodedKeyStrBytes = 32
	// One varint-encoded exponential-histogram bucket count: a uint64 in an
	// appended []uint64, from as little as ONE wire byte when packed. (The
	// fixed64 bucket counts and bounds of a classic histogram are not charged:
	// eight wire bytes become eight heap bytes, which is content.)
	decodedBucketBytes = 16
)

// decodedSizeSaturated is what a walk returns for a chain deeper than the
// nesting guard admits. The guard runs first on both doors, so this is
// unreachable there; it exists so a caller that ever runs the estimate on an
// UNGUARDED body is refused (retryably) rather than admitted with the deep part
// uncounted.
const decodedSizeSaturated = int64(1) << 40

// The request types grpc-go decodes into are pdata's own, in its internal/
// package, so nothing here can NAME them — which is what once left the gRPC arm
// charging nothing. But each public request wrapper holds one, and a
// reflect.Type is comparable, so the codec can still tell the three signals
// apart without naming anything. Resolved once, by method set rather than by
// field name; TestGRPCRequestTypesAreResolved fails if a pdata upgrade moves
// them out of reach.
var (
	grpcLogsRequest    = wrappedRequestType(reflect.TypeFor[plogotlp.ExportRequest]())
	grpcMetricsRequest = wrappedRequestType(reflect.TypeFor[pmetricotlp.ExportRequest]())
	grpcTracesRequest  = wrappedRequestType(reflect.TypeFor[ptraceotlp.ExportRequest]())
)

// wrappedRequestType is the one field of a public ExportRequest wrapper that is
// itself an OTLP message: the internal request the wrapper decodes into.
func wrappedRequestType(wrapper reflect.Type) reflect.Type {
	msg := reflect.TypeFor[otelProtoMessage]()
	for field := range wrapper.Fields() {
		if t := field.Type; t.Implements(msg) {
			return t
		}
	}
	return nil
}

// decodedSizeOf estimates b for the message v it is about to be decoded into
// (the gRPC codec's door; HTTP knows its signal from the route).
func decodedSizeOf(v any, b []byte) int64 {
	switch reflect.TypeOf(v) {
	case grpcLogsRequest:
		return decodedLogsSize(b)
	case grpcMetricsRequest:
		return decodedMetricsSize(b)
	case grpcTracesRequest:
		return decodedTracesSize(b)
	}
	// Unreachable while the three types resolve. Were it reached, the safe
	// answer is the largest reading of the bytes, never none.
	return max(decodedLogsSize(b), decodedMetricsSize(b), decodedTracesSize(b))
}

// Protobuf wire types, as the field parser reports them.
const (
	wireVarint = 0
	wireI64    = 1
	wireLen    = 2
)

// wireField is one parsed field: its number, its wire type, and — for a
// length-delimited field — its payload.
type wireField struct {
	num  uint64
	typ  uint64
	data []byte
}

// wireFields walks the fields of one message's payload.
type wireFields struct{ b []byte }

// next parses the next field, reporting false when no further field can be
// parsed. Every arm is nestingOver's (depth.go), and for the same reason: where
// this stops, pdata's decoder has already stopped too — a truncated or
// overlong varint, a length running past the buffer, a fixed-width field cut
// short, and the reserved wire types 6 and 7 are all decode errors there — so
// stopping hides nothing the decode would build. A group tag (3, 4) carries no
// payload and the walk simply scans on: pdata does too, and scanning the
// group's contents as if they were the message's own fields can only count
// MORE than the decode builds.
func (w *wireFields) next() (wireField, bool) {
	b := w.b
	key, n := consumeVarint(b)
	if n == 0 {
		return wireField{}, false
	}
	b = b[n:]
	f := wireField{num: key >> 3, typ: key & 7}
	switch f.typ {
	case wireVarint:
		_, n := consumeVarint(b)
		if n == 0 {
			return wireField{}, false
		}
		b = b[n:]
	case wireI64:
		if len(b) < 8 {
			return wireField{}, false
		}
		b = b[8:]
	case 5: // fixed32
		if len(b) < 4 {
			return wireField{}, false
		}
		b = b[4:]
	case 3, 4: // START_GROUP / END_GROUP: no payload of their own
	case wireLen:
		l, n := consumeVarint(b)
		if n == 0 {
			return wireField{}, false
		}
		b = b[n:]
		ln := int(l) // truncated exactly as pdata's ConsumeLen truncates it
		if ln < 0 || ln > len(b) {
			return wireField{}, false
		}
		f.data = b[:ln]
		b = b[ln:]
	default: // 6, 7: refused wherever pdata meets them
		return wireField{}, false
	}
	w.b = b
	return f, true
}

// is reports whether f is field num carried as a length-delimited payload —
// the only encoding pdata accepts for every message field walked here, so any
// other encoding of the same number is a decode error and builds nothing.
func (f wireField) is(num uint64) bool { return f.num == num && f.typ == wireLen }

// decodedLogsSize estimates the structural heap an ExportLogsServiceRequest
// body decodes into. It and its two siblings are the ONLY estimate: the charge
// is taken from the bytes, before anything is built.
func decodedLogsSize(b []byte) int64 {
	var n int64
	it := wireFields{b}
	for f, ok := it.next(); ok; f, ok = it.next() {
		if f.is(1) {
			n += resourceLogsSize(f.data)
		}
	}
	return n
}

func resourceLogsSize(b []byte) int64 {
	n := int64(decodedResourceBytes)
	it := wireFields{b}
	for f, ok := it.next(); ok; f, ok = it.next() {
		switch {
		case f.is(1):
			n += resourceSize(f.data, 2)
		case f.is(2), f.is(1000): // scope_logs, and the deprecated spelling pdata still decodes
			n += scopeLogsSize(f.data)
		}
	}
	return n
}

func scopeLogsSize(b []byte) int64 {
	n := int64(decodedScopeBytes)
	it := wireFields{b}
	for f, ok := it.next(); ok; f, ok = it.next() {
		switch {
		case f.is(1):
			n += scopeSize(f.data, 3)
		case f.is(2):
			n += logRecordSize(f.data)
		}
	}
	return n
}

// logRecordSize is one record at wire depth 3 (request -> resource_logs ->
// scope_logs -> log_records): its body and attributes sit at depth 4.
func logRecordSize(b []byte) int64 {
	n := int64(decodedItemBytes)
	it := wireFields{b}
	for f, ok := it.next(); ok; f, ok = it.next() {
		switch {
		case f.is(5):
			n += anyValueSize(f.data, 4)
		case f.is(6):
			n += keyValueSize(f.data, 4)
		}
	}
	return n
}

// decodedMetricsSize is decodedLogsSize's metrics sibling.
func decodedMetricsSize(b []byte) int64 {
	var n int64
	it := wireFields{b}
	for f, ok := it.next(); ok; f, ok = it.next() {
		if f.is(1) {
			n += resourceMetricsSize(f.data)
		}
	}
	return n
}

func resourceMetricsSize(b []byte) int64 {
	n := int64(decodedResourceBytes)
	it := wireFields{b}
	for f, ok := it.next(); ok; f, ok = it.next() {
		switch {
		case f.is(1):
			n += resourceSize(f.data, 2)
		case f.is(2), f.is(1000):
			n += scopeMetricsSize(f.data)
		}
	}
	return n
}

func scopeMetricsSize(b []byte) int64 {
	n := int64(decodedScopeBytes)
	it := wireFields{b}
	for f, ok := it.next(); ok; f, ok = it.next() {
		switch {
		case f.is(1):
			n += scopeSize(f.data, 3)
		case f.is(2):
			n += metricSize(f.data)
		}
	}
	return n
}

// metricSize is one metric at wire depth 3. A metric SHELL is charged like an
// item: a payload of a million point-less metrics is legal (emptymetrics.go
// prunes them, but only after they are resident).
func metricSize(b []byte) int64 {
	n := int64(decodedItemBytes)
	it := wireFields{b}
	for f, ok := it.next(); ok; f, ok = it.next() {
		switch {
		case f.is(5), f.is(7): // gauge, sum
			n += decodedNodeBytes + dataPointsSize(f.data, numberDataPointSize)
		case f.is(9):
			n += decodedNodeBytes + dataPointsSize(f.data, histogramDataPointSize)
		case f.is(10):
			n += decodedNodeBytes + dataPointsSize(f.data, expHistogramDataPointSize)
		case f.is(11):
			n += decodedNodeBytes + dataPointsSize(f.data, summaryDataPointSize)
		case f.is(12): // metadata
			n += keyValueSize(f.data, 4)
		}
	}
	return n
}

// dataPointsSize walks a gauge/sum/histogram/exponential-histogram/summary
// payload, whose field 1 is its repeated data points (at wire depth 5). point is
// always a top-level function, so passing it allocates nothing.
func dataPointsSize(b []byte, point func([]byte) int64) int64 {
	var n int64
	it := wireFields{b}
	for f, ok := it.next(); ok; f, ok = it.next() {
		if f.is(1) {
			n += point(f.data)
		}
	}
	return n
}

func numberDataPointSize(b []byte) int64 {
	n := int64(decodedItemBytes)
	it := wireFields{b}
	for f, ok := it.next(); ok; f, ok = it.next() {
		switch {
		case f.is(7):
			n += keyValueSize(f.data, 6)
		case f.is(5):
			n += exemplarSize(f.data)
		}
	}
	return n
}

func histogramDataPointSize(b []byte) int64 {
	n := int64(decodedItemBytes)
	it := wireFields{b}
	for f, ok := it.next(); ok; f, ok = it.next() {
		switch {
		case f.is(9):
			n += keyValueSize(f.data, 6)
		case f.is(8):
			n += exemplarSize(f.data)
		}
	}
	return n
}

func expHistogramDataPointSize(b []byte) int64 {
	n := int64(decodedItemBytes)
	it := wireFields{b}
	for f, ok := it.next(); ok; f, ok = it.next() {
		switch {
		case f.is(1):
			n += keyValueSize(f.data, 6)
		case f.is(11):
			n += exemplarSize(f.data)
		case f.is(8), f.is(9): // positive, negative
			n += bucketsSize(f.data)
		}
	}
	return n
}

// bucketsSize charges an exponential histogram's bucket counts, the one
// numeric list whose decoded form outgrows its encoding: a packed varint can be
// ONE byte, and pdata appends each as a uint64. A varint ends at the first byte
// below 0x80, so counting those bytes counts at least as many values as the
// decode appends before it stops.
func bucketsSize(b []byte) int64 {
	var n int64
	it := wireFields{b}
	for f, ok := it.next(); ok; f, ok = it.next() {
		if f.num != 2 {
			continue
		}
		switch f.typ {
		case wireLen: // packed
			for _, c := range f.data {
				if c < 0x80 {
					n += decodedBucketBytes
				}
			}
		case wireVarint: // one value, unpacked
			n += decodedBucketBytes
		}
	}
	return n
}

func summaryDataPointSize(b []byte) int64 {
	n := int64(decodedItemBytes)
	it := wireFields{b}
	for f, ok := it.next(); ok; f, ok = it.next() {
		switch {
		case f.is(7):
			n += keyValueSize(f.data, 6)
		case f.is(6):
			n += decodedQuantileBytes
		}
	}
	return n
}

// exemplarSize is one exemplar at wire depth 6; its filtered attributes sit at 7.
func exemplarSize(b []byte) int64 {
	n := int64(decodedExemplarBytes)
	it := wireFields{b}
	for f, ok := it.next(); ok; f, ok = it.next() {
		if f.is(7) {
			n += keyValueSize(f.data, 7)
		}
	}
	return n
}

// decodedTracesSize is decodedLogsSize's traces sibling.
func decodedTracesSize(b []byte) int64 {
	var n int64
	it := wireFields{b}
	for f, ok := it.next(); ok; f, ok = it.next() {
		if f.is(1) {
			n += resourceSpansSize(f.data)
		}
	}
	return n
}

func resourceSpansSize(b []byte) int64 {
	n := int64(decodedResourceBytes)
	it := wireFields{b}
	for f, ok := it.next(); ok; f, ok = it.next() {
		switch {
		case f.is(1):
			n += resourceSize(f.data, 2)
		case f.is(2), f.is(1000):
			n += scopeSpansSize(f.data)
		}
	}
	return n
}

func scopeSpansSize(b []byte) int64 {
	n := int64(decodedScopeBytes)
	it := wireFields{b}
	for f, ok := it.next(); ok; f, ok = it.next() {
		switch {
		case f.is(1):
			n += scopeSize(f.data, 3)
		case f.is(2):
			n += spanSize(f.data)
		}
	}
	return n
}

// spanSize is one span at wire depth 3.
func spanSize(b []byte) int64 {
	n := int64(decodedItemBytes)
	it := wireFields{b}
	for f, ok := it.next(); ok; f, ok = it.next() {
		switch {
		case f.is(9):
			n += keyValueSize(f.data, 4)
		case f.is(11):
			n += eventOrLinkSize(f.data, decodedEventBytes, 3)
		case f.is(13):
			n += eventOrLinkSize(f.data, decodedLinkBytes, 4)
		}
	}
	return n
}

// eventOrLinkSize is a span event (attributes are field 3) or a span link
// (field 4), at wire depth 4.
func eventOrLinkSize(b []byte, base int64, attrs uint64) int64 {
	n := base
	it := wireFields{b}
	for f, ok := it.next(); ok; f, ok = it.next() {
		if f.is(attrs) {
			n += keyValueSize(f.data, 5)
		}
	}
	return n
}

// --- shared by all three signals ---

// resourceSize is a Resource at wire depth `depth`: its attributes and its
// entity refs.
func resourceSize(b []byte, depth int) int64 {
	var n int64
	it := wireFields{b}
	for f, ok := it.next(); ok; f, ok = it.next() {
		switch {
		case f.is(1):
			n += keyValueSize(f.data, depth+1)
		case f.is(3):
			n += entityRefSize(f.data)
		}
	}
	return n
}

func entityRefSize(b []byte) int64 {
	n := int64(decodedEntityBytes)
	it := wireFields{b}
	for f, ok := it.next(); ok; f, ok = it.next() {
		if f.is(3) || f.is(4) { // id_keys, description_keys
			n += decodedKeyStrBytes
		}
	}
	return n
}

// scopeSize is an InstrumentationScope at wire depth `depth`: its attributes.
func scopeSize(b []byte, depth int) int64 {
	var n int64
	it := wireFields{b}
	for f, ok := it.next(); ok; f, ok = it.next() {
		if f.is(3) {
			n += keyValueSize(f.data, depth+1)
		}
	}
	return n
}

// keyValueSize is one KeyValue whose payload sits at wire depth `depth`.
func keyValueSize(b []byte, depth int) int64 {
	if depth > maxNestingDepth {
		return decodedSizeSaturated
	}
	n := int64(decodedAttrBytes)
	it := wireFields{b}
	for f, ok := it.next(); ok; f, ok = it.next() {
		if f.is(2) {
			n += anyValueSize(f.data, depth+1)
		}
	}
	return n
}

// anyValueSize is one AnyValue at wire depth `depth` — a log body, an
// attribute's value, an array element. Every oneof arm pdata decodes allocates
// its wrapper; a second arm REPLACES the first and the replaced one is garbage,
// so counting every occurrence over-counts only a payload no encoder emits.
func anyValueSize(b []byte, depth int) int64 {
	if depth > maxNestingDepth {
		return decodedSizeSaturated
	}
	var n int64
	it := wireFields{b}
	for f, ok := it.next(); ok; f, ok = it.next() {
		switch f.num {
		case 1, 7: // string, bytes
			if f.typ == wireLen {
				n += decodedValueBytes
			}
		case 2, 3, 8: // bool, int, string-table index
			if f.typ == wireVarint {
				n += decodedValueBytes
			}
		case 4: // double
			if f.typ == wireI64 {
				n += decodedValueBytes
			}
		case 5: // array
			if f.typ == wireLen {
				n += decodedNodeBytes + arrayValueSize(f.data, depth+1)
			}
		case 6: // kvlist
			if f.typ == wireLen {
				n += decodedNodeBytes + keyValueListSize(f.data, depth+1)
			}
		}
	}
	return n
}

func arrayValueSize(b []byte, depth int) int64 {
	var n int64
	it := wireFields{b}
	for f, ok := it.next(); ok; f, ok = it.next() {
		if f.is(1) {
			n += decodedElemBytes + anyValueSize(f.data, depth+1)
		}
	}
	return n
}

func keyValueListSize(b []byte, depth int) int64 {
	var n int64
	it := wireFields{b}
	for f, ok := it.next(); ok; f, ok = it.next() {
		if f.is(1) {
			n += keyValueSize(f.data, depth+1)
		}
	}
	return n
}
