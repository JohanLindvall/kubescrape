package tracehash

import (
	"testing"
	"time"
)

// The burst floor, pinned once for both adopters (ported from tracesample's
// TestFractionalRateCapStillForwards, which still exercises it end-to-end): a
// fractional rate must throttle, not black-hole. Refill caps tokens at burst
// and admission requires a whole token, so a burst equal to a rate below 1
// could never accumulate one — everything dropped forever.
func TestFractionalRateBurstFloor(t *testing.T) {
	t0 := time.Unix(0, 0)
	for _, m := range []struct {
		name string
		take func(b *Bucket, n float64, now time.Time) bool
	}{
		{"TakeExact", (*Bucket).TakeExact},
		{"AdmitDebt", (*Bucket).AdmitDebt},
	} {
		b := NewBucket(0.5)
		// Starts full at the floored burst: exactly one whole token.
		if !m.take(b, 1, t0) {
			t.Fatalf("%s: a fresh 0.5/s bucket refused its first token (burst floor gone)", m.name)
		}
		if m.take(b, 1, t0) {
			t.Fatalf("%s: a drained 0.5/s bucket admitted a second token instantly", m.name)
		}
		// Two seconds at 0.5/s refills one whole token.
		if !m.take(b, 1, t0.Add(2*time.Second)) {
			t.Fatalf("%s: no refill after 2s at 0.5/s — a fractional rate drops 100%% forever", m.name)
		}
	}
}

// TakeExact is all-or-nothing: a payload larger than the tokens present takes
// NOTHING (tracesample's fast path hands it to the per-span path, which bills
// span by span).
func TestTakeExactIsAllOrNothing(t *testing.T) {
	t0 := time.Unix(0, 0)
	b := NewBucket(10)
	if b.TakeExact(11, t0) {
		t.Fatal("TakeExact admitted a payload larger than the full bucket")
	}
	if !b.TakeExact(10, t0) {
		t.Fatal("the failed TakeExact spent tokens; all-or-nothing must leave the bucket untouched")
	}
}

// AdmitDebt admits a trace larger than the whole bucket (need is min(n,
// burst)) and charges the full n, so the refill pays the debt back before the
// next admission.
func TestAdmitDebtAdmitsOversizedAndGoesIntoDebt(t *testing.T) {
	t0 := time.Unix(0, 0)
	b := NewBucket(10)
	if !b.AdmitDebt(25, t0) {
		t.Fatal("AdmitDebt shut out a trace larger than the bucket; deep traces would be lost forever")
	}
	// tokens are now -15; one second refills 10 → -5, still below need=1.
	if b.AdmitDebt(1, t0.Add(time.Second)) {
		t.Fatal("AdmitDebt admitted while the bucket was still in debt")
	}
	// Another second → +5, a single span fits again.
	if !b.AdmitDebt(1, t0.Add(2*time.Second)) {
		t.Fatal("AdmitDebt refused after the debt was repaid")
	}
}

// Peek answers AdmitDebt's question without spending tokens OR banking the
// refill — a re-decision must be invisible to first-time traces.
func TestPeekLeavesTheBucketUntouched(t *testing.T) {
	t0 := time.Unix(0, 0)
	b := NewBucket(10)
	if !b.AdmitDebt(10, t0) {
		t.Fatal("setup: draining the bucket failed")
	}
	// One second later ten tokens WOULD be back; Peek sees them...
	if !b.Peek(10, t0.Add(time.Second)) {
		t.Fatal("Peek did not apply the refill to its answer")
	}
	// ...but must not have banked the refill or spent anything: AdmitDebt at
	// the ORIGINAL instant still sees the drained bucket.
	if b.AdmitDebt(1, t0) {
		t.Fatal("Peek mutated the bucket (banked the refill or moved last)")
	}
	// The assertion above cannot see a Peek that moved `last` forward WITHOUT
	// banking: the out-of-order AdmitDebt bills a negative elapsed as zero and
	// refuses either way. This one can — the refill accrued between t0 and
	// t0+1s must still be there for the first-time decision that follows a
	// re-decision, or the rate cap under-admits every trace after one.
	if !b.AdmitDebt(10, t0.Add(time.Second)) {
		t.Fatal("Peek advanced last without banking: the refill accrued before it is lost")
	}
}

// An out-of-order `now` (concurrent handlers read the clock outside the bucket
// mutex) must not debit tokens or rewind `last`: it is billed as zero elapsed.
func TestBucketOutOfOrderNowDoesNotDebit(t *testing.T) {
	b := NewBucket(10) // burst 10, starts full
	t0 := time.Unix(1_000_000, 0)
	// Prime `last` and spend down to a known level at t0.
	if !b.TakeExact(10, t0) {
		t.Fatal("full bucket must admit its whole burst")
	}
	// A later handler advances to t0+1s, crediting 10 tokens, and takes 5.
	if !b.TakeExact(5, t0.Add(time.Second)) {
		t.Fatal("after 1s the bucket refilled 10; taking 5 must succeed")
	}
	// A straggler now reads an EARLIER clock (t0). It must neither debit nor
	// rewind: the 5 tokens banked as of t0+1s stay available.
	if !b.TakeExact(5, t0) {
		t.Fatal("an out-of-order earlier `now` debited the bucket (F8 regression)")
	}
	// And a genuine later time still refills from the highest observed instant,
	// not the stale straggler value.
	if !b.TakeExact(10, t0.Add(2*time.Second)) {
		t.Fatal("refill must resume from the max observed time, not the rewound one")
	}
}

// Peek is the re-decision stand-in for AdmitDebt, so for every bucket state
// and every n it must give AdmitDebt's answer — the no-mutation half is
// TestPeekLeavesTheBucketUntouched's; this is the agreement half. The states
// cover a first call (no refill billed yet), a full and a part-spent bucket,
// a bucket in DEBT, n below, at and above the burst, and a `now` earlier than
// `last` (billed as zero elapsed).
func TestPeekAgreesWithAdmitDebt(t *testing.T) {
	t0 := time.Unix(1_000_000, 0)
	type state struct {
		name   string
		rate   float64
		tokens float64
		last   time.Time
	}
	states := []state{
		{"first call", 10, 10, time.Time{}},
		{"full", 10, 10, t0},
		{"part spent", 10, 3.5, t0},
		{"empty", 10, 0, t0},
		{"in debt", 10, -15, t0},
		{"fractional rate", 0.5, 0.25, t0},
	}
	nows := []time.Duration{-time.Second, 0, 100 * time.Millisecond, time.Second, 2 * time.Second, time.Minute}
	ns := []float64{0, 0.5, 1, 3, 9.99, 10, 11, 25, 1000}
	for _, st := range states {
		for _, d := range nows {
			for _, n := range ns {
				now := t0.Add(d)
				mk := func() *Bucket {
					return &Bucket{rate: st.rate, burst: max(1, st.rate), tokens: st.tokens, last: st.last}
				}
				peeked := mk().Peek(n, now)
				admitted := mk().AdmitDebt(n, now)
				if peeked != admitted {
					t.Errorf("%s, now=t0 + %v, n=%v: Peek=%v but AdmitDebt=%v", st.name, d, n, peeked, admitted)
				}
			}
		}
	}
}
