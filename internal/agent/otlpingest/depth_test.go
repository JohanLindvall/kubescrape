package otlpingest

// The wire-SHAPE guard (depth.go): a body that is inside every size cap and
// decodes perfectly, and whose decoding costs unbounded goroutine stack.
//
// The payloads here are hand-built protobuf rather than pdata objects, because
// the whole point is what the DECODER does with bytes it has not seen yet — and
// because building 200 000 nested pdata values would recurse in the test before
// it recursed in the receiver.

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

// protoField appends one length-delimited field.
func protoField(dst []byte, num int, payload []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(num)<<3|2)
	dst = binary.AppendUvarint(dst, uint64(len(payload)))
	return append(dst, payload...)
}

// anyValueEmptyString is AnyValue{string_value: ""} — a chain ending in this
// one decodes. anyValueTruncatedString claims five bytes it does not carry, so
// a chain ending in THAT one fails at its deepest point, after pdata has
// already spent every level of stack getting there.
var (
	anyValueEmptyString     = []byte{1<<3 | 2, 0}
	anyValueTruncatedString = []byte{1<<3 | 2, 5}
)

// nestedAnyValue builds an AnyValue nesting `levels` array_value levels deep,
// which is the shape pdata's AnyValue <-> ArrayValue decoders recurse through.
// `inner` is the innermost AnyValue's payload; the two above are the only ones
// used, and they must be the same length so the size arithmetic below is shared.
//
// Sizes are computed first and the bytes written outermost-first, because the
// obvious inside-out build copies the whole payload once per level — O(n^2),
// and at the 200 000 levels this file needs that is hours, not seconds.
func nestedAnyValue(levels int, inner []byte) []byte {
	uvarintLen := func(n int) int { return len(binary.AppendUvarint(nil, uint64(n))) }
	// size[i] is the byte length of an AnyValue nesting i levels; size[0] is
	// the innermost AnyValue{string_value: ""}.
	size := make([]int, levels+1)
	size[0] = len(inner)
	for i := 0; i < levels; i++ {
		arr := 1 + uvarintLen(size[i]) + size[i] // ArrayValue{values: [v]}
		size[i+1] = 1 + uvarintLen(arr) + arr    // AnyValue{array_value: arr}
	}
	out := make([]byte, 0, size[levels])
	for i := levels; i > 0; i-- {
		arr := 1 + uvarintLen(size[i-1]) + size[i-1]
		out = binary.AppendUvarint(out, 5<<3|2) // AnyValue.array_value
		out = binary.AppendUvarint(out, uint64(arr))
		out = binary.AppendUvarint(out, 1<<3|2) // ArrayValue.values
		out = binary.AppendUvarint(out, uint64(size[i-1]))
	}
	return append(out, inner...)
}

// deepLogsRequest wraps a nested body in a legal ExportLogsServiceRequest.
func deepLogsRequest(levels int) []byte {
	return logsRequestAround(nestedAnyValue(levels, anyValueEmptyString))
}

// deepLogsRequestFailingDeep is the same chain with its innermost value
// truncated: pdata descends every level and only then unwinds an error, which
// is the reject-AFTER-recursion shape the guard exists for and the one no
// destination-based measurement can see (decodedDepth says why).
func deepLogsRequestFailingDeep(levels int) []byte {
	return logsRequestAround(nestedAnyValue(levels, anyValueTruncatedString))
}

func logsRequestAround(body []byte) []byte {
	rec := protoField(nil, 5, body) // LogRecord{body}
	sl := protoField(nil, 2, rec)   // ScopeLogs{log_records}
	rl := protoField(nil, 2, sl)    // ResourceLogs{scope_logs}
	return protoField(nil, 1, rl)   // ExportLogsServiceRequest
}

// The guard has to bind on a payload the decoder would otherwise accept, and
// leave an ordinary one alone. The depth arithmetic is the part worth pinning:
// the OTLP envelope costs 5 wire levels before a sender's own structure starts
// and each nested value costs 3 more, so the bound must sit well above both.
func TestDeeplyNestedPayloadIsRefusedBeforeItIsDecoded(t *testing.T) {
	shallow := deepLogsRequest(4) // ~17 wire levels: an ordinary structured body
	if err := checkNesting(shallow); err != nil {
		t.Fatalf("an ordinary nested body was refused: %v", err)
	}
	// It really is a legal payload: the guard is refusing shape, not garbage.
	req := plogotlp.NewExportRequest()
	if err := req.UnmarshalProto(shallow); err != nil {
		t.Fatalf("the fixture is not decodable OTLP: %v", err)
	}

	deep := deepLogsRequest(200000)
	if len(deep) > maxIngestBody {
		t.Fatalf("fixture is %d bytes, past the %d body cap: it must be admissible to prove the point",
			len(deep), maxIngestBody)
	}
	if err := checkNesting(deep); err == nil {
		t.Fatalf("a %d-level payload of %d bytes was admitted for decoding; pdata recurses ~674 B of stack "+
			"per level, so this is ~128 MiB of goroutine stack per push", 200000, len(deep))
	}
}

// Exactly where the bound binds, so a future "simplification" of the walk
// cannot quietly move it. maxNestingDepth counts WIRE levels.
func TestNestingBoundIsExactlyMaxNestingDepth(t *testing.T) {
	// A chain of N length-delimited levels, each a bare submessage.
	chain := func(levels int) []byte {
		b := protoField(nil, 1, nil)
		for i := 1; i < levels; i++ {
			b = protoField(nil, 1, b)
		}
		return b
	}
	if err := checkNesting(chain(maxNestingDepth)); err != nil {
		t.Errorf("a payload exactly at the bound was refused: %v", err)
	}
	if err := checkNesting(chain(maxNestingDepth + 1)); err == nil {
		t.Errorf("a payload one level past the bound was admitted")
	}
}

// The walk is a shape PROBE, not a validator: anything it cannot parse is a
// leaf. A body of arbitrary bytes must never be refused for its shape — the
// decoder owns that verdict and answers the same 400.
func TestUnparseableBytesAreALeafNotARefusal(t *testing.T) {
	for name, b := range map[string][]byte{
		"empty":     {},
		"text":      []byte(`{"level":"debug","msg":"hello"}`),
		"truncated": deepLogsRequest(3)[:7],
		// Wire types 3/4. The walk scans straight through them (see the group
		// arm) rather than stopping, and two bare group tags carry no nesting,
		// so the verdict is unchanged: a leaf, not a refusal.
		"groups": {0x0b, 0x0c},
	} {
		if err := checkNesting(b); err != nil {
			t.Errorf("%s: shape guard refused non-message bytes: %v", name, err)
		}
	}
}

// End to end on the HTTP arm: the refusal happens at the DOOR (BodyReader), so
// the trace tier's internal hop — the other user of that seam — is covered by
// the same code, and the push is answered 400 and counted.
func TestHTTPDeepPayloadIsRefusedAtTheDoorAndCounted(t *testing.T) {
	exp := &captureExporter{}
	srv := httpTestServer(t, exp)

	before := obs.IngestBodyRejected.WithLabelValues(reasonTooDeep).Value()
	resp, err := http.Post(srv.URL+"/v1/logs", "application/x-protobuf",
		bytes.NewReader(deepLogsRequest(200000)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 (permanent: the same bytes cannot be shallower on a retry)", resp.StatusCode)
	}
	if len(exp.logs) != 0 {
		t.Errorf("a refused payload was forwarded (%d exports)", len(exp.logs))
	}
	if got := obs.IngestBodyRejected.WithLabelValues(reasonTooDeep).Value() - before; got != 1 {
		t.Errorf("kubescrape_ingest_body_rejected_total{reason=too_deep} moved %v, want 1: a refusal "+
			"nothing counts is indistinguishable from a quiet listener", got)
	}
}

// A normal push over the same door still works, which is the other half of the
// guard being correct: it must refuse the shape, not the traffic.
func TestHTTPOrdinaryNestedBodyStillRoundTrips(t *testing.T) {
	exp := &captureExporter{}
	srv := httpTestServer(t, exp)

	ld := plog.NewLogs()
	body := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body()
	m := body.SetEmptyMap()
	inner := m.PutEmptyMap("a").PutEmptyMap("b").PutEmptySlice("c").AppendEmpty()
	inner.SetEmptyMap().PutStr("d", "deep enough for any SDK")
	raw, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(srv.URL+"/v1/logs", "application/x-protobuf", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(exp.logs) != 1 {
		t.Fatalf("exports = %d, want 1", len(exp.logs))
	}
}

// rawProto is a message that carries wire bytes verbatim. It implements the
// same (structural) interface pdata's codec keys on, so the client marshals
// exactly the bytes below and the server's codec takes the OTLP path.
type rawProto struct{ b []byte }

func (r *rawProto) SizeProto() int              { return len(r.b) }
func (r *rawProto) MarshalProto(dst []byte) int { return copy(dst, r.b) }
func (r *rawProto) UnmarshalProto(b []byte) error {
	r.b = append([]byte(nil), b...)
	return nil
}

// The gRPC arm decodes BEFORE the interceptor, so the guard has to ride the
// codec. This drives a real grpc.Server wired exactly as Run wires it.
func TestGRPCDeepPayloadIsRefusedInTheCodec(t *testing.T) {
	s := NewServer(ServerConfig{
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Exporter: exporterFunc(func(plog.Logs) error { return nil }),
	})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(NestingGuardOption(s.noteTooDeep)) // exactly what Run wires
	plogotlp.RegisterGRPCServer(srv, &logsGRPC{s: s})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	before := obs.IngestBodyRejected.WithLabelValues(reasonTooDeep).Value()
	// 400k levels is 3.5 MiB — inside grpc-go's own 4 MiB message cap, which is
	// the point: every existing bound admits it.
	deep := &rawProto{b: deepLogsRequest(400000)}
	if len(deep.b) > maxIngestGRPCMessage {
		t.Fatalf("fixture is %d bytes, past the %d gRPC message cap", len(deep.b), maxIngestGRPCMessage)
	}
	err = conn.Invoke(ctx, "/opentelemetry.proto.collector.logs.v1.LogsService/Export", deep, &rawProto{})
	if err == nil {
		t.Fatal("a 400 000-level payload was decoded; that is ~256 MiB of goroutine stack per push")
	}
	if code := status.Code(err); code != codes.Internal {
		t.Errorf("status = %v, want Internal (grpc-go's own rewrite of a codec failure, and NON-retryable "+
			"per the OTLP spec, which is right: the bytes cannot be shallower on a retry)", code)
	}
	if got := obs.IngestBodyRejected.WithLabelValues(reasonTooDeep).Value() - before; got != 1 {
		t.Errorf("kubescrape_ingest_body_rejected_total{reason=too_deep} moved %v, want 1", got)
	}

	// The same server still serves an ordinary push: the guard is a targeted
	// refusal, not collateral damage on the codec.
	client := plogotlp.NewGRPCClient(conn)
	if _, err := client.Export(ctx, oneLog()); err != nil {
		t.Fatalf("ordinary push through the guarded codec: %v", err)
	}
}

// --- the walk must not stop where the decoder does not ---
//
// Every case below was a live bypass of the depth bound: a prefix the DECODER
// skips and the walk stopped on, after which the whole remaining buffer — the
// deep chain included — was admitted unwalked. They are grouped here because
// they are one defect wearing five hats: the walk parsing the wire format its
// own way instead of pdata's.

// concatBytes is the corpus builder; the fixtures are tiny and built per case.
func concatBytes(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// wireTag spells one field header.
func wireTag(num, wire uint64) []byte { return binary.AppendUvarint(nil, num<<3|wire) }

// tenByteVarint spells v in the 10-byte non-canonical encoding whose final byte
// is 2 — the exact shape encoding/binary.Uvarint calls an overflow and pdata's
// ConsumeVarint accepts (it bounds the LENGTH at ten bytes and shifts the top
// byte's excess bits out, so the value is unchanged). v must fit in 63 bits.
func tenByteVarint(v uint64) []byte {
	out := make([]byte, 0, 10)
	for i := 0; i < 9; i++ {
		out = append(out, byte(v&0x7f)|0x80)
		v >>= 7
	}
	return append(out, 0x02)
}

// hostileWrappers are byte sequences pdata SKIPS: unknown fields, spelled in
// every shape the wire format allows. Each one used to end the walk.
func hostileWrappers() map[string][]byte {
	return map[string][]byte{
		// The reported defect: ConsumeUnknown counts group depth iteratively
		// and returns to the field loop, which decodes everything after.
		"empty group":         concatBytes(wireTag(100, 3), wireTag(100, 4)),
		"nested empty groups": concatBytes(wireTag(100, 3), wireTag(101, 3), wireTag(101, 4), wireTag(100, 4)),
		// A group holding a field: ConsumeUnknown consumes that field and
		// returns at its end, so the parent resumes there — the walk has to
		// land on the same offset.
		"group holding a varint": concatBytes(wireTag(100, 3), wireTag(101, 0), []byte{0x01}),
		"group holding bytes":    concatBytes(wireTag(100, 3), wireTag(101, 2), []byte{0x03, 'a', 'b', 'c'}),
		// The three varint positions, each non-canonically ten bytes long.
		"10-byte tag":           concatBytes(tenByteVarint(100<<3), []byte{0x00}), // field 100, VARINT
		"10-byte varint value":  concatBytes(wireTag(100, 0), tenByteVarint(1)),
		"10-byte length prefix": concatBytes(wireTag(100, 2), tenByteVarint(0)),
		// The arms that were already right, kept so a rewrite cannot break them
		// silently.
		"unknown fixed64": concatBytes(wireTag(100, 1), make([]byte, 8)),
		"unknown fixed32": concatBytes(wireTag(100, 5), make([]byte, 4)),
		"unknown bytes":   concatBytes(wireTag(100, 2), []byte{0x02, 'h', 'i'}),
	}
}

// The reported defect, at the size the guard's own comment quotes: 200 000
// levels is ~1.6 MB on the wire, inside every cap this receiver has, and ~128
// MiB of goroutine stack to decode. Two four-byte tags in front of it made
// checkNesting return nil.
//
// The 200 000-level fixture is only walked, never decoded — proving that pdata
// reads straight past the prefix needs no depth at all, so that half uses a
// cheap 200-level body rather than 128 MiB of test stack.
func TestGroupPrefixCannotBypassTheDepthGuard(t *testing.T) {
	prefix := concatBytes(wireTag(100, 3), wireTag(100, 4)) // START_GROUP, END_GROUP

	// The load-bearing fact: the prefix does not stop the DECODER.
	decodable := concatBytes(prefix, deepLogsRequest(200))
	if err := plogotlp.NewExportRequest().UnmarshalProto(decodable); err != nil {
		t.Fatalf("pdata refuses a group-prefixed payload (%v); if that is now true the walk owes it nothing, "+
			"but this test no longer pins anything and should be rewritten, not deleted", err)
	}

	evil := concatBytes(prefix, deepLogsRequest(200000))
	if len(evil) > maxIngestBody {
		t.Fatalf("fixture is %d bytes, past the %d body cap: it must be admissible to prove the point",
			len(evil), maxIngestBody)
	}
	if err := checkNesting(evil); err == nil {
		t.Fatalf("a %d-byte payload was admitted behind a %d-byte group prefix: the walk abandoned the whole "+
			"remaining buffer at wire type 3 and pdata decodes it anyway, so the ~128 MiB stack recursion is "+
			"available on the HTTP door, the gRPC codec and the trace tier's internal hop", len(evil), len(prefix))
	}
}

// The siblings: three varint positions where encoding/binary.Uvarint stops and
// pdata's ConsumeVarint does not, plus the group shapes that resume mid-group.
// Each is its own bypass of the same bound.
func TestHostileWrappersCannotBypassTheDepthGuard(t *testing.T) {
	deep := deepLogsRequest(200) // ~605 wire levels: past the bound, cheap to decode
	if err := checkNesting(deep); err == nil {
		t.Fatalf("the bare fixture is not deep enough to trip the bound; every case below would pass vacuously")
	}
	for name, w := range hostileWrappers() {
		evil := concatBytes(w, deep)
		// The wrapper is only interesting while pdata really does read past it.
		if err := plogotlp.NewExportRequest().UnmarshalProto(evil); err != nil {
			t.Errorf("%s: pdata now refuses this shape (%v); the case is vacuous and needs re-deriving from "+
				"pdata's internal/proto/unmarshal.go, not deleting", name, err)
			continue
		}
		if err := checkNesting(evil); err == nil {
			t.Errorf("%s: the walk admitted %d bytes pdata decodes; it stopped on the %d-byte wrapper and "+
				"never looked at the deep chain behind it", name, len(evil), len(w))
		}
	}
}

// maxDecodedDepthUnderTheBound is the deepest decoded value nesting a payload
// the guard ADMITS can reach. maxNestingDepth counts WIRE levels: the OTLP
// envelope spends 4 of them before a LogRecord's body (request -> resource_logs
// -> scope_logs -> log_records), the body's own AnyValue is the 5th, and every
// further decoded level costs at least 2 more (AnyValue -> ArrayValue ->
// AnyValue; a map costs 3). So an admitted body decodes to at most this, and
// anything past it is a payload the walk was supposed to refuse.
const maxDecodedDepthUnderTheBound = 1 + (maxNestingDepth-5)/2

// decodedDepth reports the deepest value nesting pdata's decoder actually built
// out of b: 1 for a scalar body, one more per array/map level, 0 if it never
// reached a log record at all. The decode ERROR is deliberately discarded.
//
// It is what the property test below turns on, and it replaced a filter on
// `err == nil`. The dangerous payload is not the one pdata ACCEPTS, it is the
// one pdata rejects only AFTER recursing: the stack is spent before the error
// unwinds, which is the entire harm this guard exists to prevent, so a test
// that skips every payload the decoder rejects is blind to exactly the case
// worth catching. Measuring the destination sees the recursion whatever the
// verdict.
//
// A partially-filled destination measures a partial descent because every
// repeated message field APPENDS its element and then decodes INTO it (pdata
// v1.65 internal/generated_proto_arrayvalue.go appends an empty AnyValue to
// ArrayValue.Values and only then unmarshals into it), so a sibling that
// completed survives a later sibling's failure — which is the shape of every
// corpus payload here: a well-formed deep chain beside a wrapper that may or
// may not decode.
//
// KNOWN BLIND SPOT, and the reason the test below exists beside this one: a
// chain that fails DURING its own descent leaves nothing behind. AnyValue's
// oneof arms assign `orig.Value` only after the nested decode returns nil
// (generated_proto_anyvalue.go case 5), so a 200-level chain truncated at its
// innermost byte measures 1 here after spending 200 levels of stack. No
// destination-based measurement can see that shape; the test that covers it
// takes its "pdata reads past this wrapper" evidence from the chain's
// OBSERVABLE twin instead.
func decodedDepth(b []byte) int {
	req := plogotlp.NewExportRequest()
	_ = req.UnmarshalProto(b)
	best := 0
	rls := req.Logs().ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		sls := rls.At(i).ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			recs := sls.At(j).LogRecords()
			for k := 0; k < recs.Len(); k++ {
				if d := valueDepth(recs.At(k).Body()); d > best {
					best = d
				}
			}
		}
	}
	return best
}

func valueDepth(v pcommon.Value) int {
	deepest := 0
	switch v.Type() {
	case pcommon.ValueTypeSlice:
		s := v.Slice()
		for i := 0; i < s.Len(); i++ {
			if d := valueDepth(s.At(i)); d > deepest {
				deepest = d
			}
		}
	case pcommon.ValueTypeMap:
		v.Map().Range(func(_ string, mv pcommon.Value) bool {
			if d := valueDepth(mv); d > deepest {
				deepest = d
			}
			return true
		})
	default:
		return 1
	}
	return 1 + deepest
}

// The property the individual cases are instances of: for a corpus of hostile
// wrappers in every position, ANYTHING PDATA RECURSES DEEPLY INTO THE WALK HAS
// REFUSED. A prefix that ends the walk early is a full bypass of the bound; a
// suffix or an interleaving proves the walk resumes at the decoder's offset
// rather than merely surviving the wrapper.
//
// The predecessor asserted this only over payloads pdata ACCEPTS, which cannot
// see the strictly more dangerous one it rejects after descending — so the
// filter is the DECODED DEPTH the payload actually cost, not the verdict. That
// is not a hypothetical widening: measured over this corpus, the verdict filter
// asserted on 360 of the 484 (wrapper, wrapper, shape) cases and the depth
// filter asserts on 400 — the extra 40 being payloads pdata recurses 201 levels
// into and THEN rejects.
func TestNothingPdataRecursesIntoCanEndTheWalkEarly(t *testing.T) {
	deep := deepLogsRequest(200)
	// Non-vacuity for the MEASUREMENT: it has to see the chain, at the depth
	// the fixture builds, or every case below is skipped rather than passed.
	if got, want := decodedDepth(deep), 201; got != want {
		t.Fatalf("decodedDepth(deepLogsRequest(200)) = %d, want %d: the measurement no longer sees the chain "+
			"it filters on, so this test would silently stop asserting anything", got, want)
	}
	// Non-vacuity for the THRESHOLD: the deepest payload the guard admits sits
	// exactly at it, so it is tight rather than slack enough to skip the corpus.
	edge := deepLogsRequest(maxDecodedDepthUnderTheBound - 1)
	if err := checkNesting(edge); err != nil {
		t.Fatalf("the deepest admissible chain was refused (%v): maxDecodedDepthUnderTheBound is derived from "+
			"maxNestingDepth and one of the two is now wrong", err)
	}
	if got := decodedDepth(edge); got != maxDecodedDepthUnderTheBound {
		t.Fatalf("the deepest admissible chain decodes to %d, not %d: the threshold below would admit a payload "+
			"the guard refuses (or skip one it admits)", got, maxDecodedDepthUnderTheBound)
	}

	wrappers := hostileWrappers()
	deepCases := 0
	for name, w := range wrappers {
		for _, other := range wrappers { // an interleaving of two shapes
			for shape, b := range map[string][]byte{
				"prefix": concatBytes(w, deep),
				"suffix": concatBytes(deep, w),
				"around": concatBytes(w, deep, w),
				"pair":   concatBytes(w, other, deep),
			} {
				if decodedDepth(b) <= maxDecodedDepthUnderTheBound {
					continue // the decoder never got into the chain; the walk owes nothing
				}
				deepCases++
				if err := checkNesting(b); err == nil {
					t.Fatalf("%s/%s: pdata recurses %d levels into this %d-byte payload and the walk admitted "+
						"it — the walk stopped on bytes the decoder reads, which admits everything behind them "+
						"unwalked", name, shape, decodedDepth(b), len(b))
				}
			}
		}
	}
	if deepCases == 0 {
		t.Fatal("no corpus payload reached deep recursion: every case was skipped, so this test asserted nothing")
	}
}

// The shape no destination-based measurement can see, and therefore the one the
// property test above would silently skip: a chain that fails at its DEEPEST
// point. pdata descends all 200 levels and only then unwinds an error, so the
// stack is spent exactly as if it had succeeded, while the destination it
// leaves behind measures 1.
//
// The evidence that pdata reads past a given wrapper is taken from the chain's
// OBSERVABLE twin — the same wrapper in front of a well-formed chain, whose
// depth the destination does report — and then applied to the unobservable one,
// which carries byte-for-byte the same prefix.
func TestWrappersCannotHideAChainThatFailsAtItsDeepestPoint(t *testing.T) {
	blind := deepLogsRequestFailingDeep(200)
	if err := plogotlp.NewExportRequest().UnmarshalProto(blind); err == nil {
		t.Fatal("the fixture decodes; it has to be the reject-AFTER-recursion shape or it pins nothing")
	}
	if got := decodedDepth(blind); got > maxDecodedDepthUnderTheBound {
		t.Logf("pdata now retains a failed descent (depth %d): the property test above can cover this shape "+
			"directly, and this one is redundant rather than wrong", got)
	}
	if err := checkNesting(blind); err == nil {
		t.Fatalf("a %d-byte chain pdata recurses 200 levels into before rejecting was admitted: the guard is "+
			"there to stop the stack being spent, not to agree with the decoder's verdict", len(blind))
	}
	covered := 0
	for name, w := range hostileWrappers() {
		if decodedDepth(concatBytes(w, deepLogsRequest(200))) <= maxDecodedDepthUnderTheBound {
			continue // pdata does not read past this wrapper, so it owes nothing behind it
		}
		covered++
		if err := checkNesting(concatBytes(w, blind)); err == nil {
			t.Errorf("%s: the walk admitted a chain behind a %d-byte wrapper pdata reads straight past; the "+
				"decode spends 200 levels of stack and then reports an error nobody counts", name, len(w))
		}
	}
	if covered == 0 {
		t.Error("no wrapper was shown to be one pdata reads past, so the loop above asserted nothing")
	}
}

// The other direction, and the reason a blanket refusal of groups was rejected:
// the walk is schema-free and descends into strings, and `{` is field 15
// START_GROUP. Refusing the shape would answer 400 to every JSON log body past
// the prune threshold.
func TestOrdinaryTextBodiesAreStillALeaf(t *testing.T) {
	for name, line := range map[string]string{
		"json":   `{"level":"info","msg":"` + strings.Repeat("a request was served ", 40) + `"}`,
		"logfmt": strings.Repeat("level=info msg=served dur=1.2ms ", 40),
		"plain":  strings.Repeat("2026-08-19T00:00:00Z something happened here ", 20),
	} {
		ld := plog.NewLogs()
		ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr(line)
		raw, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
		if err != nil {
			t.Fatal(err)
		}
		if err := checkNesting(raw); err != nil {
			t.Errorf("%s: a %d-byte ordinary log body was refused for its shape: %v", name, len(line), err)
		}
	}
}

// The walk runs on the receive path of an unauthenticated listener, once per
// push, over the whole body. The file's own doc calls it allocation-free; this
// is what keeps that true across a rewrite of the wire reader.
func TestNestingWalkIsAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector adds bookkeeping allocations; widening the ceiling would let a real one through")
	}
	ordinary := deepLogsRequest(4)
	evil := concatBytes(wireTag(100, 3), wireTag(100, 4), deepLogsRequest(200))
	if got := testing.AllocsPerRun(100, func() {
		_ = checkNesting(ordinary)
		_ = checkNesting(evil)
	}); got != 0 {
		t.Errorf("checkNesting allocated %v times per run, want 0", got)
	}
}

// The header quotes this walk's throughput as a bound on what an
// unauthenticated sender buys per compressed byte, so the shape it quotes is
// BUILT here rather than left as a claim. It is the worst case by construction:
// a body with no length-delimited field below the wrappers, so nothing is ever
// refused (the walk runs to the last byte), nothing is ever pruned (the prune
// only skips a sub-payload too short to hold the remaining levels), and every
// one of its two-byte fields is a position the decoder could read and the walk
// must therefore parse.
//
// Re-measure before quoting a number: this fixture re-measured 23-31 ms on ONE
// machine depending on what else it was doing, and other hardware will differ
// again — the header quotes the range for that reason.
func BenchmarkNestingWalkFlatBody(b *testing.B) {
	body := wrapped(maxNestingDepth, flatVarintFields(15<<20))
	if len(body) > maxIngestBody {
		b.Fatalf("fixture is %d bytes, past the %d body cap: the shape only matters while a sender can push it",
			len(body), maxIngestBody)
	}
	// Admitted, which is what makes it the worst case: a refusal would abort
	// the walk after a couple of bytes per level and never touch the filler.
	if err := checkNesting(body); err != nil {
		b.Fatalf("the fixture is refused (%v), so it measures the abort path, not the scan", err)
	}
	b.SetBytes(int64(len(body)))
	for b.Loop() {
		_ = checkNesting(body)
	}
}

// flatVarintFields is n bytes of `field 1, VARINT 0` — two bytes per field, no
// nesting anywhere, nothing the walk can prune or refuse. Wire type 0 is the
// zero low bits of the tag and the value is zero too, so only the tag's field
// number has to be written.
func flatVarintFields(n int) []byte {
	b := make([]byte, n)
	for i := 0; i+1 < n; i += 2 {
		b[i] = 1 << 3
	}
	return b
}

// wrapped nests payload inside `levels` bare `field 1, LEN` submessages,
// outermost first (the inside-out build would copy the payload once per level).
// It puts the filler at the deepest level the guard admits, which is where a
// sender would put it: pdata stops descending after a handful of levels and
// treats the rest as one string, so the walk is left scanning alone.
func wrapped(levels int, payload []byte) []byte {
	uvarintLen := func(n int) int { return len(binary.AppendUvarint(nil, uint64(n))) }
	size := make([]int, levels+1)
	size[0] = len(payload)
	for i := 0; i < levels; i++ {
		size[i+1] = 1 + uvarintLen(size[i]) + size[i]
	}
	out := make([]byte, 0, size[levels])
	for i := levels; i > 0; i-- {
		out = binary.AppendUvarint(out, 1<<3|2)
		out = binary.AppendUvarint(out, uint64(size[i-1]))
	}
	return append(out, payload...)
}

// consumeVarint's one-byte fast path exists only to cut the constant the header
// quotes, so it has to be indistinguishable from the loop it short-circuits —
// a fast path that disagreed about a varint would be the same bypass the
// 10-byte-encoding cases above were, arriving through an optimisation instead
// of a misreading. This is the exhaustive statement of that equivalence over
// every input short enough to enumerate, plus the long shapes that only exist
// because pdata and encoding/binary disagree about them.
func TestConsumeVarintFastPathMatchesTheLoop(t *testing.T) {
	// The loop as it stood before the fast path was spelled out in front of it.
	reference := func(b []byte) (uint64, int) {
		var num uint64
		for shift, i := uint(0), 0; ; shift, i = shift+7, i+1 {
			if shift >= 64 || i >= len(b) {
				return 0, 0
			}
			c := b[i]
			num |= uint64(c&0x7f) << shift
			if c < 0x80 {
				return num, i + 1
			}
		}
	}
	check := func(b []byte) {
		t.Helper()
		wantV, wantN := reference(b)
		gotV, gotN := consumeVarint(b)
		if gotV != wantV || gotN != wantN {
			t.Fatalf("consumeVarint(%x) = (%d, %d), the loop reads (%d, %d)", b, gotV, gotN, wantV, wantN)
		}
	}
	check(nil)
	for i := 0; i < 256; i++ {
		check([]byte{byte(i)})
		for j := 0; j < 256; j++ {
			check([]byte{byte(i), byte(j)})
		}
	}
	// The shapes the walk's own bypass history is made of: ten-byte
	// non-canonical encodings, an eleventh continuation byte, and a varint
	// running off the end of the buffer.
	check(tenByteVarint(100 << 3))
	check(tenByteVarint(1))
	check(append(tenByteVarint(1), 0x00))
	check(bytes.Repeat([]byte{0x80}, 11))
	check(bytes.Repeat([]byte{0x80}, 3))
}
