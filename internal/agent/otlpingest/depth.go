package otlpingest

// Wire-SHAPE admission: how deeply a pushed payload nests, decided BEFORE the
// protobuf decoder sees it.
//
// pdata's generated decoder recurses without a limit of its own: an AnyValue
// holding an ArrayValue calls back into AnyValue (pdata v1.65
// internal/generated_proto_anyvalue.go -> generated_proto_arrayvalue.go), and
// nothing on either side counts. One legal, small body therefore buys an
// unbounded goroutine stack: measured at ~619-674 B of stack per nesting level,
// so 200k levels (1.6 MB on the wire, inside every cap this receiver has) peaks
// at 128 MiB of stack for ONE decode and 435k levels at 256 MiB — several of
// which fit inside the byte budget and the in-flight count at once. The
// outcome is an OOM-kill of the DaemonSet (or trace-tier) pod with NO counter
// moving, because nothing survives the decode to move one; at Go's default
// 1 GiB max stack a large enough body is an outright `fatal error: stack
// overflow`. Both listeners are unauthenticated by design, so the shape has to
// be refused before it is decoded — a guard AFTER the decode is a guard that
// runs on the far side of the crash.
//
// The existing depth bound (maxBodyScrubDepth, enrich.go) does not help and
// says so where it is used: it walks an ALREADY-DECODED pcommon.Value. The
// decode is the unbounded step.
//
// The walk here is O(len(body)), allocation-free, and reads only the protobuf
// wire format — no schema, because it must judge a body the decoder has not
// looked at yet. It therefore cannot tell a nested MESSAGE from a string that
// happens to parse as one; that is fine in the direction that matters, since
// counting a string as a level can only make the guard stricter, and the bound
// is far above where any real payload's strings sit (see maxNestingDepth).
//
// WHAT O(len(body)) COSTS, spelled out because "linear and allocation-free"
// names no constant and an attacker multiplies the constant by compressing. The
// worst shape is a body carrying no length-delimited field ANYWHERE: nothing is
// ever refused (so the walk runs to the last byte), nothing is ever pruned (the
// prune only skips a sub-payload too short to hold the remaining levels), and
// every two-byte field is a position the decoder could read, so every one must
// be parsed. Measured on a 13th-gen i7 over a 15 MiB body of that shape
// (BenchmarkNestingWalkFlatBody, 100 `field 1, LEN` wrappers around a flat run
// of varint fields): 23-31 ms, i.e. ~500-700 MB/s — quote the RANGE and
// re-measure, since the two ends of it are the same binary on the same machine
// under different load. It is near-pure repetition and gzips ~1000:1, so
// ~15 KiB of upload inflates to 15 MiB in ~5.5 ms and then costs that here,
// against the ~6 ms pdata spends before rejecting the same bytes (it stops
// descending a few levels in and copies the rest out as one string). So this
// guard roughly TRIPLES the CPU that compressed byte buys.
//
// That is a worsening of an already-bounded path, not a new door. Every byte
// walked is a byte already charged to the raw budget and still held (HTTP: the
// body is charged as it is read and released when the handler returns; gRPC:
// the tap's reservation is taken before the decode the codec's walk rides), so
// maxBufferBytes bounds ALL concurrent walks at once — 64 MiB at ~500-700 MB/s
// is ~0.1 s of CPU however a sender splits it into requests. admit.go's own doc
// sizes the far larger heap inflation that same upload already buys, which is
// the bound that actually binds. The walk cannot simply move behind the
// in-flight semaphore, where -ingest-max-in-flight would bound it instead: on
// gRPC there is no "behind the semaphore" still in front of the decode (grpc-go
// unmarshals before the interceptor runs, which is the whole reason the guard
// rides the codec), and on HTTP the door is BodyReader — shared with the trace
// tier's internal hop, which has no semaphore at all — where the read
// deliberately precedes the slot, holding one across a trickled 16 MiB upload
// being the denial of service the slot exists to prevent.
//
// The prune cannot be made to fire more often on that shape and stay sound: the
// cost is a long FLAT run of fields at ONE depth, and a length bound says
// nothing about a buffer that is plainly long enough — the prune only ever
// skips a sub-payload too SHORT to hold the levels that would break the bound.
// The CONSTANT was cut instead: consumeVarint's one-byte fast path, worth
// ~1.3-1.6x on the fixture above (interleaved A/B, n=5, minimum of 3 per arm),
// which is where the upper end of the throughput range comes from.

import (
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
)

// maxNestingDepth bounds length-delimited nesting in a pushed payload.
//
// It counts WIRE levels, not user-visible ones, and the two are not the same:
// an OTLP logs body sits 5 levels down before a sender's own structure begins
// (request -> resource_logs -> scope_logs -> log_records -> body AnyValue), and
// each further level of a map/array body costs THREE wire levels (AnyValue ->
// KeyValueList -> KeyValue -> AnyValue). So 100 admits roughly 31 levels of
// nested attribute structure — an order of magnitude past what any SDK emits
// (2-3 in practice), and past maxBodyScrubDepth (8), where this package already
// stops walking a decoded body.
//
// 100 is also protobuf's OWN portability limit: the C++ implementation's
// default recursion limit is 100, so a payload deeper than this is already
// undecodable by a conformant consumer and cannot be something a real sender
// relies on. The cost of the bound is one goroutine stack of at most
// 100 x ~674 B = ~67 KiB per decode, which is the point.
//
// Raising it is a memory decision, not a compatibility one: the stack cost is
// linear in the bound and paid per concurrent decode.
const maxNestingDepth = 100

// errBodyTooDeep is the refusal. It is PERMANENT (400 / the malformed arm's
// status): no retry of the same bytes can be shallower, and the sender must
// change what it emits.
var errBodyTooDeep = errors.New("request body nests deeper than the receiver decodes")

// checkNesting refuses a body whose wire nesting exceeds maxNestingDepth.
func checkNesting(b []byte) error {
	if nestingOver(b, 0, maxNestingDepth) {
		return errBodyTooDeep
	}
	return nil
}

// nestingOver reports whether b — the payload of a message at `depth` — holds a
// length-delimited chain that reaches deeper than max.
//
// Everything it cannot parse is a LEAF, never an error: this is a shape probe,
// not a validator. A body that is malformed protobuf is the decoder's to reject
// (and it answers the same 400), and a string field that does not happen to
// look like a message simply stops the walk where it stands.
//
// THE INVARIANT, and the one every arm below is written against: THE WALK MUST
// NEVER STOP EARLY ON A PAYLOAD PDATA WOULD STILL DECODE. Any byte sequence the
// walk declines to examine must also be one the decoder refuses — otherwise the
// bytes behind the stop are admitted unwalked, which is the whole guard
// bypassed by whatever prefix produced the stop. So each arm is matched against
// pdata's own wire reader (internal/proto/unmarshal.go: ConsumeTag,
// ConsumeVarint, ConsumeLen, ConsumeUnknown) rather than against what a
// reasonable protobuf parser might do, and where the two could disagree the
// walk keeps going rather than admit bytes it has not looked at. Both directions
// of that were once wrong here and both were reachable from the unauthenticated
// door: see the group arm and consumeVarint.
//
// Two properties keep it cheap. It aborts at the first chain over the bound, so
// the hostile shape is refused after ~2 bytes per level and never touches the
// rest of the body. And it does not descend into a sub-payload too SHORT to
// hold the remaining levels — each level costs at least two bytes (a key and a
// length), so a leaf string shorter than 2x(max-depth) cannot possibly carry
// them. That prune is what keeps ordinary log bodies from being re-scanned.
//
// Its own recursion is bounded by max for the same reason the bound exists.
func nestingOver(b []byte, depth, max int) bool {
	for len(b) > 0 {
		key, n := consumeVarint(b)
		if n == 0 {
			// Not a parseable field header, and ConsumeTag fails on exactly the
			// same bytes (truncated, or an 11th continuation byte), so the
			// decoder reads nothing past here either.
			return false
		}
		b = b[n:]
		// The field NUMBER is deliberately not checked. ConsumeTag refuses
		// fieldNum <= 0, so a payload carrying one is a decode failure and the
		// walk could stop — but it only ever KEEPS WALKING by not looking, and
		// a zero field number is a common byte inside the strings this walk
		// descends into, where the decoder parses nothing at all.
		switch key & 7 {
		case 0: // varint
			_, n := consumeVarint(b)
			if n == 0 {
				return false
			}
			b = b[n:]
		case 1: // fixed64
			if len(b) < 8 {
				return false
			}
			b = b[8:]
		case 5: // fixed32
			if len(b) < 4 {
				return false
			}
			b = b[4:]
		case 3, 4: // START_GROUP / END_GROUP
			// A group tag carries no payload of its own, so the walk keeps
			// scanning tags — which is exactly what the decoder does, and the
			// reason it must: PDATA DOES NOT REJECT GROUPS. ConsumeUnknown
			// counts group depth iteratively and RETURNS to the field loop,
			// which then decodes every field after the group. This arm used to
			// `return false` for wire types 3 and 4, so two four-byte tags —
			// `field 100 START_GROUP` then `field 100 END_GROUP` — in front of
			// a 200 000-level body abandoned the walk of the ENTIRE remaining
			// buffer while pdata decoded it: the unbounded stack recursion was
			// fully available on the HTTP door, on the gRPC codec and on the
			// trace tier's internal hop, behind a 4-byte prefix.
			//
			// Refusing a group outright would honour the invariant too, and it
			// is tempting (no OTLP encoder emits one). It cannot be done HERE:
			// the walk is schema-free and descends into strings, and `{` (0x7b)
			// is field 15 START_GROUP — so every JSON log body past the prune
			// threshold would be answered 400. Scanning on is the superset
			// instead: pdata reads a tag stream inside a group as well, so
			// every position the decoder visits the walk visits, at the same
			// offsets, plus the group's own length-delimited fields, which
			// pdata's skip returns out of rather than descending into.
		case 2: // length-delimited: a submessage, a string or bytes
			l, n := consumeVarint(b)
			if n == 0 {
				return false
			}
			b = b[n:]
			// int, not uint64, because ConsumeLen truncates the same way and
			// then refuses a negative length (ErrInvalidLength) or one running
			// past the buffer (ErrUnexpectedEOF) — so on a hypothetical 32-bit
			// build a length of 2^32+5 must consume the 5 bytes the decoder
			// consumes, not stop the walk on a bound the decoder never applies.
			ln := int(l)
			if ln < 0 || ln > len(b) {
				return false
			}
			sub := b[:ln]
			b = b[ln:]
			if depth+1 > max {
				return true
			}
			if len(sub) < 2*(max-depth) {
				continue // too short to hold the levels that would break the bound
			}
			if nestingOver(sub, depth+1, max) {
				return true
			}
		default: // 6, 7: reserved
			// The only wire types with no decoder path past them:
			// ConsumeUnknown's own default arm is an error, and every generated
			// field decoder checks its wire type before consuming. A payload
			// carrying one is refused by pdata wherever it appears, so stopping
			// here hides nothing.
			return false
		}
	}
	return false
}

// consumeVarint reads one varint off the front of b, reporting its value and
// the bytes consumed — 0 meaning the decoder fails on these bytes too.
//
// It exists because encoding/binary.Uvarint is STRICTER than the decoder, and a
// walk that stops where the decoder does not is the bypass above in another
// costume. Go reports overflow for a 10-byte encoding whose last byte is > 1;
// pdata's ConsumeVarint bounds only the LENGTH — shifts 0..63, i.e. ten bytes —
// and lets the top byte's excess bits shift harmlessly out. So `field 100,
// VARINT` spelled non-canonically as A0 86 80 80 80 80 80 80 80 02 ended the
// walk at byte zero while pdata read it as a field, skipped it, and decoded
// everything after it; the same slack applies to a field's varint VALUE and to
// a length prefix, and each of the three was an independent way in.
//
// It agrees with pdata in the stopping direction too: an 11th continuation byte
// (shift 70) is ErrIntOverflow there and a varint running off the end of the
// buffer is ErrUnexpectedEOF, so returning 0 for either hides no bytes the
// decoder would read. Allocation-free, like the walk it serves.
func consumeVarint(b []byte) (uint64, int) {
	// The one-byte case, spelled out rather than left to the loop, because this
	// is the walk's entire inner loop and the header quotes its throughput as a
	// bound on what an unauthenticated sender can buy per compressed byte: a
	// tag with field number < 16 and any value below 128 is one byte, which is
	// every byte of the worst-case shape, and the loop's setup costs more than
	// the work. It is EXACTLY what the loop returns on its first iteration
	// (shift 0 < 64, i 0 < len(b), c < 0x80 so c&0x7f == c), so the two cannot
	// disagree — and disagreeing with pdata about a varint is how this function
	// was a bypass before, which is why the equivalence is spelled out here
	// rather than assumed. Worth ~1.6x on that shape.
	if len(b) > 0 && b[0] < 0x80 {
		return uint64(b[0]), 1
	}
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

// --- the gRPC half ---
//
// grpc-go decodes a message BEFORE any interceptor runs (server.go's
// processUnaryRPC calls the codec's Unmarshal and only then the handler chain),
// so the tap and the in-flight semaphore are both on the far side of the
// recursion this guard exists to stop. The only hook on the near side is the
// codec itself, which is why the shape check rides one.
//
// It wraps whatever codec is registered for "proto" — pdata registers its own
// (internal/otelgrpc), whose buffer pooling and OTLP fast path we must not lose
// — and reproduces exactly what that codec does for an OTLP message: ONE
// materialization of the buffer slice, walked and then unmarshalled from the
// same bytes, so the guard costs no extra copy. Anything that is not an OTLP
// message (a health check, a reflection call) goes to the delegate untouched.
//
// The refusal surfaces to the sender as codes.Internal — grpc-go rewrites every
// codec failure that way ("grpc: error unmarshalling request: ...") — which the
// OTLP spec lists as NON-retryable. That is the right answer here and the same
// one the HTTP arm's 400 gives: the bytes cannot become shallower on a retry.

// otelProtoMessage is pdata's own (unexported) codec interface, restated
// structurally. Every OTLP request/response type implements it; matching on it
// is how the delegate tells an OTLP payload from anything else, and this
// wrapper has to make the same distinction to unmarshal from the bytes it
// walked.
type otelProtoMessage interface {
	SizeProto() int
	MarshalProto([]byte) int
	UnmarshalProto([]byte) error
}

// codecBufferPool mirrors the tier list pdata's own codec uses
// (internal/otelgrpc), which is not importable. It matters because this
// wrapper materializes the message ITSELF rather than letting the delegate do
// it — the walk needs contiguous bytes and a second materialization would
// double the copy — and grpc's DefaultBufferPool pools nothing above 1 MiB, so
// borrowing it would turn every multi-MiB push into a fresh allocation that
// pdata's path pooled.
var codecBufferPool = mem.NewTieredBufferPool(
	256,
	4<<10,   // Go page size
	16<<10,  // max HTTP/2 frame size used by gRPC
	32<<10,  // io.Copy's default
	512<<10, //
	1<<20,   //
	4<<20,   // grpc-go's default MaxRecvMsgSize
	16<<20,  // this receiver's raised cap
)

// depthGuardCodec is the registered proto codec plus the wire-shape check.
type depthGuardCodec struct {
	delegate encoding.CodecV2
	// tooDeep reports a refusal (counted and warned by the Server). Never nil
	// in production; a nil is tolerated so a bare codec is usable in tests.
	tooDeep func()
}

// newDepthGuardCodec wraps the codec registered for "proto" — pdata's, since
// its init() registers over grpc-go's default and this package imports pdata.
func newDepthGuardCodec(tooDeep func()) *depthGuardCodec {
	return &depthGuardCodec{delegate: encoding.GetCodecV2("proto"), tooDeep: tooDeep}
}

// NestingGuardOption is the server option that puts the shape check on a gRPC
// receiver this package does not build itself — the trace tier's internal hop,
// which assembles its own grpc.Server. Every OTLP gRPC listener in this repo
// wants it: the recursion it prevents is pdata's, not this package's, and it
// runs before the decode wherever the decode happens.
//
// onRefused is the observation hook and may be nil, which is what an
// authenticated kubescrape-to-kubescrape hop wants for the same reason
// NewBodyReader turns its counting off there: that series means "an APPLICATION
// push was refused at a listener nothing authenticates".
func NestingGuardOption(onRefused func()) grpc.ServerOption {
	return grpc.ForceServerCodecV2(newDepthGuardCodec(onRefused))
}

func (c *depthGuardCodec) Name() string { return "proto" }

func (c *depthGuardCodec) Marshal(v any) (mem.BufferSlice, error) { return c.delegate.Marshal(v) }

func (c *depthGuardCodec) Unmarshal(data mem.BufferSlice, v any) error {
	m, ok := v.(otelProtoMessage)
	if !ok {
		return c.delegate.Unmarshal(data, v)
	}
	// MaterializeToBuffer takes a reference rather than a copy when the slice
	// already holds one buffer, which is what pdata's codec relies on too.
	buf := data.MaterializeToBuffer(codecBufferPool)
	defer buf.Free()
	b := buf.ReadOnlyData()
	if err := checkNesting(b); err != nil {
		if c.tooDeep != nil {
			c.tooDeep()
		}
		return err
	}
	return m.UnmarshalProto(b)
}
