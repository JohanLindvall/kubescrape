package tailsample

import (
	"testing"
	"time"
)

// nSpans builds a trace of n minimal spans under one id — the rate policies care
// only about the count.
func nSpans(id byte, n int) Trace {
	defs := make([]spanDef, n)
	for i := range defs {
		defs[i] = spanDef{start: 0, end: 1}
	}
	return mkTrace(id, nil, defs...)
}

// charged returns tr with the already-charged flag set.
func charged(tr Trace) Trace { tr.Charged = true; return tr }

func rateEvaluator(t *testing.T, perSecond float64) *Evaluator {
	t.Helper()
	ev, err := New(Config{Policies: []PolicyConfig{{
		Name: "rate", Type: TypeRateLimiting,
		RateLimiting: &RateLimitingConfig{SpansPerSecond: perSecond},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return ev
}

// A trace decided a SECOND time (its assembler still remembers deciding it, but
// no longer what it decided) must not pay the spans/second budget again: the
// budget is a rate of spans leaving, and these were already counted. Without
// Trace.Charged the re-decision spends the bucket a second time, and the next
// genuinely new trace is dropped for budget its predecessor's stragglers ate.
func TestChargedReDecisionDoesNotSpendTheBudget(t *testing.T) {
	t.Parallel()
	ev := rateEvaluator(t, 20) // burst 20

	first := ev.Decide(nSpans(1, 10))
	if !first.Sampled || first.Policy != "rate" {
		t.Fatalf("first decision: %+v", first)
	}

	again := ev.Decide(charged(nSpans(1, 10)))
	if !again.Sampled || again.Policy != "rate" {
		t.Fatalf("the re-decision must still be attributed to the policy that fits: %+v", again)
	}

	// The whole point: an unrelated trace still has the budget it was owed.
	other := ev.Decide(nSpans(2, 10))
	if !other.Sampled {
		t.Fatal("a new trace was dropped because a re-decided trace was charged twice")
	}
}

// Charged only skips the SPEND, not the check: a re-decision that does not fit
// the remaining budget still abstains, so the flag cannot be used to smuggle a
// trace past the cap.
func TestChargedStillRespectsTheBudget(t *testing.T) {
	t.Parallel()
	ev := rateEvaluator(t, 10) // burst 10
	if d := ev.Decide(nSpans(1, 10)); !d.Sampled {
		t.Fatal("first trace should fit the whole burst")
	}
	// The bucket is empty now.
	if d := ev.Decide(charged(nSpans(1, 10))); d.Sampled {
		t.Fatal("a charged re-decision was admitted against an empty bucket")
	}
}

// Composite allocates its budget per sub-policy through the same buckets, so it
// gets the same treatment.
func TestChargedReDecisionDoesNotSpendACompositeBudget(t *testing.T) {
	t.Parallel()
	ev, err := New(Config{Policies: []PolicyConfig{{
		Name: "comp", Type: TypeComposite,
		Composite: &CompositeConfig{
			MaxTotalSpansPerSecond: 20,
			SubPolicies: []PolicyConfig{
				{Name: "all", Type: TypeAlwaysSample},
			},
		},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if d := ev.Decide(nSpans(1, 10)); !d.Sampled {
		t.Fatalf("first: %+v", d)
	}
	if d := ev.Decide(charged(nSpans(1, 10))); !d.Sampled {
		t.Fatalf("charged re-decision: %+v", d)
	}
	if d := ev.Decide(nSpans(2, 10)); !d.Sampled {
		t.Fatal("a new trace was dropped because a re-decided trace was charged twice against the composite budget")
	}
}

// Decision.Charged is the SPEND, not the fact of a decision. A rateLimiting
// policy that refused the trace deducted nothing, and an assembler told
// otherwise would feed Charged back on the re-decision, which then peeks a
// budget the trace never paid and admits its spans free.
func TestDecisionReportsWhetherItSpentTheBudget(t *testing.T) {
	t.Parallel()
	ev := rateEvaluator(t, 10) // burst 10
	now := time.Unix(1700000000, 0)
	fixedClock(ev, &now)

	if d := ev.Decide(nSpans(1, 8)); !d.Sampled || !d.Charged {
		t.Fatalf("the decision that spent 8 of the 10 tokens: %+v", d)
	}
	refused := ev.Decide(nSpans(2, 6)) // 6 > the 2 tokens left
	if refused.Sampled {
		t.Fatalf("the trace was admitted over its budget: %+v", refused)
	}
	if refused.Charged {
		t.Fatal("a refused rateLimiting decision reported a charge it never made")
	}

	// The assembler feeds that back. Since nothing was billed, the re-decision
	// must BILL — so the trace after it finds the budget where the spending left
	// it rather than untouched.
	straggler := nSpans(2, 1)
	straggler.Charged = refused.Charged
	again := ev.Decide(straggler)
	if !again.Sampled || !again.Charged {
		t.Fatalf("the re-decision of a trace that never paid: %+v", again)
	}
	if d := ev.Decide(nSpans(3, 2)); d.Sampled {
		t.Fatal("a new trace was admitted on the token the re-decision was supposed to have spent")
	}
}

// A decision that DID bill reports it, and the re-decision it authorises spends
// nothing — the other half of the same flag, and what keeps a trace's late spans
// from shrinking the budget available to genuinely new traces.
func TestAChargedReDecisionReportsNoFurtherSpend(t *testing.T) {
	t.Parallel()
	ev := rateEvaluator(t, 20)
	first := ev.Decide(nSpans(1, 10))
	if !first.Charged {
		t.Fatal("the first decision billed the budget but did not say so")
	}
	redecide := nSpans(1, 10)
	redecide.Charged = first.Charged
	again := ev.Decide(redecide)
	if !again.Sampled {
		t.Fatalf("re-decision: %+v", again)
	}
	if again.Charged {
		t.Fatal("a re-decision that only PEEKED the budget reported a spend, so the trace would be billed again on the next window")
	}
}
