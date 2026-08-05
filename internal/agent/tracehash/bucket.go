package tracehash

import (
	"sync"
	"time"
)

// Bucket is the spans/second token bucket both samplers' rate caps are built
// on (tracesample's maxSpansPerSecond, tailsample's rateLimiting policy and
// each composite sub-policy's allocation). One refill core, three named
// admission methods — the METHODS differ because the two callers' semantics
// genuinely differ (all-or-nothing versus admit-with-debt), and a mode flag
// would let a call site pick the wrong one silently.
//
// The bucket carries its own mutex, which is what both replaced copies did:
// tracesample's fields were guarded by a Sampler-level mutex serving only
// them (ExportTraces runs on concurrent ingest handlers), and tailsample's
// bucket embedded one (Decide runs on the buffering layer's goroutine while
// composite buckets are shared across policies). Callers pass now explicitly
// — tailsample reads the clock once per decision, tracesample passes its
// injectable clock — so the bucket itself never consults time.Now.
//
// Deliberately NOT unified here: the tailer's allowLine, whose per-file
// token-bucket fields are unsynchronized on purpose (one sweep goroutine) and
// whose pause/drop semantics are a different machine.
type Bucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

// NewBucket builds a bucket that starts FULL, with one second of headroom
// floored at a single token: refill caps tokens at burst and admission needs
// a whole token, so a burst equal to a fractional rate (0 < r < 1) could
// never accumulate one and dropped 100% of spans forever — a total trace
// outage rather than the configured cap. Both adopters carried that war story
// separately; the floor lives here now.
func NewBucket(rate float64) *Bucket {
	burst := max(1, rate)
	return &Bucket{rate: rate, burst: burst, tokens: burst}
}

// refillLocked banks the tokens accrued since the last operation. The
// first-call guard matters: with last still zero there is no elapsed interval
// to bill, only the initial full burst.
func (b *Bucket) refillLocked(now time.Time) {
	if !b.last.IsZero() {
		b.tokens = min(b.burst, b.tokens+b.rate*now.Sub(b.last).Seconds())
	}
	b.last = now
}

// TakeExact takes n tokens if the bucket holds them all, and reports whether
// it did — agent/tracesample's semantics. All-or-nothing, so its fast path
// either pays for the whole payload or hands it to the per-span path; a
// partial take would let the two paths double-bill the same spans.
func (b *Bucket) TakeExact(n float64, now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked(now)
	if b.tokens < n {
		return false
	}
	b.tokens -= n
	return true
}

// AdmitDebt admits n if min(n, burst) tokens are present, charging the FULL n
// — agent/tailsample's semantics, and the two halves are both deliberate.
// Admission needs min(n, burst), NOT n: a trace with more spans than the
// entire bucket could never fit, so a workload whose traces are bigger than
// the per-second budget would be shut out permanently — a silent total loss
// for exactly the deep traces most worth sampling. And the charge is the full
// n, so an over-sized trace leaves the bucket in debt and the refill pays it
// back before anything else is admitted: the long-run rate stays what the
// operator configured, exceeded only in the instant, never on average.
func (b *Bucket) AdmitDebt(n float64, now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked(now)
	need := min(n, b.burst)
	if b.tokens < need {
		return false
	}
	b.tokens -= n
	return true
}

// Peek answers AdmitDebt's question WITHOUT spending anything —
// agent/tailsample's re-decision of an already-charged trace (Trace.Charged):
// the budget is a rate of spans leaving, and those spans were billed the
// first time. It does not even bank the refill; leaving the bucket's state
// entirely untouched is what makes a re-decision invisible to every trace
// being decided for the first time.
func (b *Bucket) Peek(n float64, now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	tokens := b.tokens
	if !b.last.IsZero() {
		tokens = min(b.burst, tokens+b.rate*now.Sub(b.last).Seconds())
	}
	return tokens >= min(n, b.burst)
}
