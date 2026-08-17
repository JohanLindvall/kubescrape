// Package tracehash owns the nesting-critical trace-ID sampling arithmetic
// shared by the head sampler (agent/tracesample) and the tail sampler's
// probabilistic policy (agent/tailsample), plus the token bucket both spans/
// second caps are built on.
//
// # The nesting contract
//
// Both stages hash the trace id UNSALTED with the same rapidhash and compare it
// against a threshold computed by the same arithmetic, so the two stages NEST
// rather than compound: a 50% tail policy keeps exactly the traces a head
// sampler at probability 0.5 already passed, instead of independently
// discarding half of them again. The Collector's hash salt exists to break
// that nesting and is deliberately not implemented — nobody has asked to
// decorrelate two stages, and the nesting is the safer default. The contract
// also gives per-trace determinism: the same trace decides identically
// wherever and whenever it is judged (across shards, across retries, across a
// restart that re-buffers it), so whole traces are kept or dropped, never
// halves.
//
// This package replaced two copies of the arithmetic that were held equal only
// by cross-package tests (tailsample.TestProbabilisticNestsWithTheHeadSampler,
// tailbuffer.TestNestsWithTheHeadSampler) and had ALREADY micro-drifted: the
// threshold==MaxUint64 keep-all guard in Keep existed on the tracesample side
// only, so a tailsample policy at samplingPercentage: 100 dropped the one
// trace in 2^64 whose hash is exactly MaxUint64. Sharing the function is what
// keeps the next drift from being a bigger one.
//
// # Changing the hash re-rolls the sampled set
//
// WHICH traces are kept is a property of the hash function, not just of the
// threshold, so replacing it re-rolls the whole set: the same trace ids decide
// differently afterwards. That is invisible in steady state — the same
// FRACTION is kept and any single trace is still decided consistently — but
// during a ROLLOUT the fleet runs two functions at once, and a trace whose
// spans are judged by both is kept by one shard and dropped by another, i.e.
// split. The window is the rollout, it is self-healing, and the alternative
// (never changing the function) is what this package exists to make a
// deliberate decision rather than an accident. This hash moved from xxhash to
// rapidhash once, knowingly, for the ~5x it buys on a per-span path.
//
// NOT in scope: agent/servicegraph's ring.TokenFor, which is Tempo's FNV-1
// 32-bit hash ON PURPOSE — the ring must place spans where Tempo's would, and
// it distributes load rather than deciding keeps, so it must NOT correlate
// with the sampling hash.
package tracehash

import (
	"encoding/binary"
	"math"

	"github.com/JohanLindvall/haste/rapidhash"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Threshold converts a keep FRACTION in [0,1] into the hash threshold Keep
// compares against, saturating to MaxUint64 at >= 1 (keep everything).
//
// It takes the fraction, not a percentage: tailsample's probabilistic policy
// is configured in percent and passes pct/100, so the float rounding of
// fraction*MaxUint64 is computed identically on both sides — the division at
// the call site followed by the multiplication here is bit-for-bit the old
// pct/100*float64(math.MaxUint64), which is what keeps a 50 tail policy's
// threshold equal to a 0.5 head probability's.
func Threshold(fraction float64) uint64 {
	if fraction >= 1 {
		return math.MaxUint64
	}
	return uint64(fraction * float64(math.MaxUint64))
}

// Keep reports the sampling decision for a trace id against a Threshold. A
// threshold of MaxUint64 keeps ALL traces — without the fast path, the one
// hash in 2^64 equal to MaxUint64 would fail the strict comparison and a
// sampler configured to keep everything would not.
//
// Hashing the id (rather than reading its low bits, as a W3C-random id would
// permit) keeps the distribution uniform even for senders whose ids are not
// random.
//
// The id is fed as its two little-endian halves rather than as a slice.
// rapidhash.Sum64Uint128 hashes exactly the bytes Sum64(id[:]) would — the two
// are equal for every input, which TestUint128FormEqualsRawBytes pins — but it
// takes them in registers instead of through a slice header.
//
// Measured on the real Keep with VARYING ids (an in-binary A/B against the old
// implementation, interleaved n=5): ~12.9ns -> ~5.6ns, about 2.3x. Quote that
// pair, not the 2.4-vs-12.6 an isolated microbenchmark first produced — that
// one hashed a CONSTANT id, which flatters the cheaper function and overstated
// the win as ~5x. This runs once per span in BOTH samplers, so 2.3x is still
// worth the two loads.
func Keep(id pcommon.TraceID, threshold uint64) bool {
	if threshold == math.MaxUint64 {
		return true
	}
	lo := binary.LittleEndian.Uint64(id[0:8])
	hi := binary.LittleEndian.Uint64(id[8:16])
	return rapidhash.Sum64Uint128(lo, hi) < threshold
}
