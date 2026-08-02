package tailsample

import "testing"

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
