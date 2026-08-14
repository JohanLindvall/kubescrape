package metrics

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
	"github.com/zeebo/xxh3"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// orderedResource builds a resource from an ORDERED attribute list, which is
// what the OTLP wire carries and what pdata's public Map API cannot express: a
// repeated key. dupKeyResource is the two-entry special case of this.
func orderedResource(t *testing.T, kv ...string) pcommon.Map {
	t.Helper()
	if len(kv)%2 != 0 {
		t.Fatalf("orderedResource: %d strings, want pairs", len(kv))
	}
	var b strings.Builder
	b.WriteString(`{"resourceMetrics":[{"resource":{"attributes":[`)
	for i := 0; i < len(kv); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		k, _ := json.Marshal(kv[i])
		v, _ := json.Marshal(kv[i+1])
		fmt.Fprintf(&b, `{"key":%s,"value":{"stringValue":%s}}`, k, v)
	}
	b.WriteString(`]},"scopeMetrics":[{}]}]}`)
	var um pmetric.JSONUnmarshaler
	md, err := um.UnmarshalMetrics([]byte(b.String()))
	if err != nil {
		t.Fatalf("orderedResource: %v", err)
	}
	return md.ResourceMetrics().At(0).Resource().Attributes()
}

// seriesKeyFor is the resource half of the key series.observeFold computes: the
// identity fold of the resource plus its extra resource labels. A test that
// asserts on this asserts on the map key an observation actually lands under.
//
// It composes the two halves the way the store does — the one-pass fold for the
// resource, the materialized identity for the cancel to resolve against — so
// the assertions below also hold the two definitions to each other.
func seriesKeyFor(res pcommon.Map, extra labels) xxh3.Uint128 {
	ident := resourceIdentity(res, make(labels, 0, res.Len()))
	return mixHash(xor128(resourceAccum(res), resLabelsAccum(ident, extra)))
}

// TestResourceIdentityIsWhatItRenders is the whole invariant in one statement:
// two resources land in the SAME series exactly when they RENDER the same
// resource. The hash is the map key and the string is what the sample carries
// into the export, so a disagreement is a lookup that succeeds on a key naming
// something else — two live samples sharing one serialized identity, which is
// duplicate same-timestamp points every export and, for a histogram, one merged
// point whose buckets summed both. Nothing counts it and nothing logs it.
//
// Every case here involves a resource that REPEATS a key, which is legal OTLP
// (attributes are a repeated KeyValue and pdata does not dedupe on decode) and
// therefore reachable from the unauthenticated ingest listeners.
func TestResourceIdentityIsWhatItRenders(t *testing.T) {
	cases := []struct {
		name string
		res  pcommon.Map
	}{
		{"clean", orderedResource(t, "svc", "a")},
		{"empty", orderedResource(t)},
		{"twice the same pair", orderedResource(t, "svc", "a", "svc", "a")},
		{"three times", orderedResource(t, "svc", "a", "svc", "a", "svc", "a")},
		{"two values", orderedResource(t, "svc", "p", "svc", "q")},
		{"two values reversed", orderedResource(t, "svc", "q", "svc", "p")},
		{"last one empty", orderedResource(t, "svc", "a", "svc", "")},
		{"first one empty", orderedResource(t, "svc", "", "svc", "a")},
		{"empty in the middle", orderedResource(t, "svc", "p", "svc", "", "svc", "q")},
		{"dup beside a distinct key", orderedResource(t, "node", "n1", "svc", "p", "svc", "q")},
		{"same pairs, other order", orderedResource(t, "svc", "q", "node", "n1", "svc", "q")},
		{"empty key repeated", orderedResource(t, "", "x", "", "y", "svc", "a")},
		{"plain q", orderedResource(t, "svc", "q")},
		{"plain p", orderedResource(t, "svc", "p")},
	}

	for i, a := range cases {
		for j, b := range cases {
			if j <= i {
				continue
			}
			sameRender := resourceString(a.res, nil) == resourceString(b.res, nil)
			sameKey := seriesKeyFor(a.res, nil) == seriesKeyFor(b.res, nil)
			if sameRender != sameKey {
				verb := "hash apart while rendering identically"
				if sameKey {
					verb = "share a series key while rendering differently"
				}
				t.Errorf("%q and %q %s\n  %s renders %s\n  %s renders %s",
					a.name, b.name, verb,
					a.name, resourceString(a.res, nil),
					b.name, resourceString(b.res, nil))
			}
		}
	}
}

// The sharpest single case, called out because it is the one the XOR fold makes
// worse than a wrapping sum would: an even multiplicity CANCELS, so a resource
// repeating a pair used to fold to the accumulator of the EMPTY resource, bit
// for bit — merging with whatever unrelated attribute-less resource happens to
// be in the same store rather than merely minting a duplicate of itself.
func TestARepeatedPairDoesNotCancelToTheEmptyResource(t *testing.T) {
	dup := orderedResource(t, "service.name", "a", "service.name", "a")
	if resourceAccum(dup) == resourceAccum(orderedResource(t)) {
		t.Fatal("{k=v, k=v} folded to the empty resource's accumulator: it will merge with every attribute-less resource in the store")
	}
	if got, want := resourceAccum(dup), resourceAccum(orderedResource(t, "service.name", "a")); got != want {
		t.Fatalf("{k=v, k=v} must hash as the {k=v} it renders")
	}
}

// TestOverrideCancelsTheValueTheResourceRenders pins the half of the invariant
// that lived nowhere: resLabelsAccum cancels the resource pair a resourceLabel
// replaces, and it must cancel the value the resource RENDERS. It used to read
// that value with pcommon.Map.Get, which returns the FIRST match, while the
// render is last-wins — so on a resource repeating the overridden key the
// cancel folded out a pair that was never folded in.
//
// This is what made the old "call the dedup-ing fold on a resource of unknown
// provenance" contract insufficient rather than merely fragile: it was wrong
// even when followed, as soon as a rule declared resourceLabels.
func TestOverrideCancelsTheValueTheResourceRenders(t *testing.T) {
	dup := orderedResource(t, "svc", "p", "svc", "q") // renders svc="q"
	override := labels{{"svc", "r"}}

	if got, want := resourceString(dup, override), `{svc="r"}`; got != want {
		t.Fatalf("test premise: renders %s, want %s", got, want)
	}
	plain := orderedResource(t, "svc", "r")
	if seriesKeyFor(dup, override) != seriesKeyFor(plain, nil) {
		t.Fatal("a resourceLabel overriding a REPEATED key keys a different series than the identity it renders")
	}

	// And the ordinary case still holds: the cancel is not simply disabled.
	if seriesKeyFor(orderedResource(t, "svc", "p"), override) != seriesKeyFor(plain, nil) {
		t.Fatal("the override cancel stopped keying the merged set")
	}
	// A label overriding a key the resource does NOT carry adds, cancels nothing.
	if seriesKeyFor(orderedResource(t, "node", "n1"), override) == seriesKeyFor(plain, nil) {
		t.Fatal("an override of an absent key must not collapse onto the plain resource")
	}
}

// The fast path in resourceAccum folds in one pass only while it can PROVE the
// resource key-unique, and its proof table is finite. Past it the answer must
// still be the identity the resource renders — a resource too wide to prove is
// not a resource that may be hashed wrongly.
func TestWideResourcesHashAsTheirRenderedIdentity(t *testing.T) {
	build := func(n int, dupAt int) pcommon.Map {
		var kv []string
		for i := 0; i < n; i++ {
			kv = append(kv, fmt.Sprintf("attr.%03d", i), fmt.Sprintf("v%03d", i))
		}
		if dupAt >= 0 {
			kv = append(kv, fmt.Sprintf("attr.%03d", dupAt), "overridden")
		}
		return orderedResource(t, kv...)
	}
	for _, n := range []int{resourceIdentityAttrs - 1, resourceIdentityAttrs, resourceIdentityAttrs + 1, 2 * resourceIdentityAttrs} {
		clean := build(n, -1)
		if got, want := resourceAccum(clean), resourceIdentity(clean, nil).resAccum(); got != want {
			t.Errorf("n=%d: the one-pass fold disagrees with the materialized identity", n)
		}
		// A duplicate PAST the proof table (the last key repeated) must still
		// resolve last-wins.
		dup := build(n, n-1)
		last := fmt.Sprintf("attr.%03d", n-1)
		want := build(n-1, -1)
		want.PutStr(last, "overridden")
		if resourceAccum(dup) != resourceAccum(want) {
			t.Errorf("n=%d: a duplicate beyond the proof table did not resolve last-wins", n)
		}
	}
}

// Bind resolves a resource one of two ways: a set whose rules lift labels ONTO
// the resource materializes the identity (the override cancel needs the value
// each key renders, and re-deriving that per line would make a per-observation
// cost out of a per-resource fact), and every other set keeps the one-pass
// fold. This is the wiring of that choice: the branch a set takes must decide
// only what it SPENDS. The two must key the same series — a resource whose
// series moved because a rule elsewhere in the config gained a resourceLabel
// would break its continuity for no reason the operator could see — and the
// cheap branch must genuinely stay cheap.
func TestBindKeysAResourceTheSameWhicheverBranchItTakes(t *testing.T) {
	plain, err := newTestSet([]Dynamic{{Name: "m", Type: CounterType, Value: "1"}})
	if err != nil {
		t.Fatal(err)
	}
	lifted, err := newTestSet([]Dynamic{{
		Name: "m", Type: CounterType, Value: "1", ResourceLabels: []string{"tenant=$tenant"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{7, 16, 32, 64} {
		res := wideResource(n)
		cheap, resolved := plain.Bind(res), lifted.Bind(res)
		if cheap.res.ident != nil {
			t.Errorf("n=%d: a set declaring no resourceLabels materialized the identity anyway", n)
		}
		if resolved.res.ident == nil {
			t.Errorf("n=%d: a set lifting labels onto the resource must resolve the identity ONCE, at Bind", n)
		}
		if cheap.res.accum != resolved.res.accum {
			t.Errorf("n=%d: the two Bind branches fold the same resource to different keys", n)
		}
	}
}

// The fold runs once per file per flush and once per pushed resource; the
// package's discipline is that it costs no allocation, and the proof is what
// could have taken one away.
func TestResourceAccumIsAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	wide := pcommon.NewMap()
	for i := 0; i < resourceIdentityAttrs; i++ {
		wide.PutStr(fmt.Sprintf("attr.%03d", i), fmt.Sprintf("value-%03d", i))
	}
	for _, tc := range []struct {
		name string
		res  pcommon.Map
	}{
		{"typical", benchResource()},
		{"widest provable", wide},
	} {
		var sink xxh3.Uint128
		allocs := testing.AllocsPerRun(200, func() { sink = resourceAccum(tc.res) })
		sinkAccum(sink)
		if allocs != 0 {
			t.Errorf("%s: resourceAccum allocates %v times, want 0 (the uniqueness proof must stay on the stack)", tc.name, allocs)
		}
	}

	// The override cancel runs per OBSERVATION, not per Bind. The identity it
	// resolves replaced keys against is resolved once (Bind hands it forward),
	// so what has to hold here is that reading it costs nothing: no allocation,
	// and — the reason the identity is passed rather than the map — a lookup
	// that stops at the first match instead of walking every attribute.
	extra := labels{{"service.name", "override"}, {"tenant", "acme"}}
	res := benchResource()
	ident := resourceIdentity(res, make(labels, 0, res.Len()))
	var sink xxh3.Uint128
	if allocs := testing.AllocsPerRun(200, func() { sink = resLabelsAccum(ident, extra) }); allocs != 0 {
		t.Errorf("resLabelsAccum allocates %v times, want 0", allocs)
	}
	sinkAccum(sink)
}

// FuzzResourceIdentityMatchesItsRender is the general form of the table above,
// and it is what keeps the one-pass fold — and resLabelsAccum's cancel — from
// drifting away from the identity definition they shortcut. labels.set decides
// what a resource's identity IS (empty key or value dropped, value cut at
// maxLabelValueBytes, repeated key last-wins); resourceString renders it and
// the accumulators fold it, and this asserts on arbitrary inputs that the fold
// keys the series the render NAMES, rather than on the shapes someone thought
// of. A new rule in set() that a fold does not learn fails here.
//
// The oracle is the rendered string itself, re-parsed into a clean key-unique
// resource: a resource keys exactly as the resource it renders as. parseLabels
// is String's declared inverse (FuzzLabelsParse pins that separately), so the
// canonical form is built by a path that shares no arithmetic with the fold.
func FuzzResourceIdentityMatchesItsRender(f *testing.F) {
	// Seeds chosen to reach the two shapes the guards exist for, so the seed
	// corpus alone fails when either is reverted: {svc=p, svc=p} (the pair that
	// CANCELS under XOR) and {svc=p, svc=q} under an override (where the cancel
	// used to read the FIRST value and the render the LAST).
	f.Add([]byte{0, 0, 0, 0}, []byte(nil))
	f.Add([]byte{0, 0, 0, 1}, []byte{0, 0, 0})
	f.Add([]byte{0, 1, 0, 0}, []byte{0, 1, 0})
	f.Add([]byte{1, 2, 3, 4, 5, 6}, []byte{7, 8, 9})
	f.Add([]byte{9, 9, 9, 1, 1, 1, 2, 2, 2}, []byte{3, 3, 3, 3, 3, 3})
	f.Add(make([]byte, 40), []byte{4, 5, 6})

	f.Fuzz(func(t *testing.T, seed, ex []byte) {
		pairs := fuzzPairs(seed)
		res := orderedResource(t, pairs...)
		extra := fuzzLabels(ex)

		rendered := resourceString(res, extra)
		parsed, err := parseLabels(rendered)
		if err != nil {
			t.Fatalf("a resource rendered a string its own parser rejects: %s: %v", rendered, err)
		}
		canon := pcommon.NewMap()
		for _, e := range parsed {
			canon.PutStr(e.key, e.value)
		}
		if got := resourceString(canon, nil); got != rendered {
			t.Fatalf("the rendered identity does not round-trip: %s -> %s", rendered, got)
		}
		if seriesKeyFor(res, extra) != seriesKeyFor(canon, nil) {
			t.Fatalf("hashed identity is not the rendered identity\n  attributes %q\n  +labels    %v\n  render     %s",
				pairs, extra, rendered)
		}
	})
}

// A deliberately small alphabet: repeated keys and colliding renders are the
// interesting half of the property, and random strings produce neither. The
// entries are chosen for the identity rules they exercise — an empty key, an
// empty value, a key needing escapes, values that differ only past the
// truncation bound.
var (
	fuzzKeys   = []string{"svc", "node", "", "k8s.pod.name", "svc ", "a=b"}
	fuzzValues = []string{"p", "q", "", "a", strings.Repeat("x", maxLabelValueBytes+3) + "1", strings.Repeat("x", maxLabelValueBytes+3) + "2"}
)

func fuzzPairs(seed []byte) []string {
	var out []string
	for i := 0; i+1 < len(seed) && len(out) < 48; i += 2 {
		out = append(out, fuzzKeys[int(seed[i])%len(fuzzKeys)], fuzzValues[int(seed[i+1])%len(fuzzValues)])
	}
	return out
}

func fuzzLabels(b []byte) labels {
	var l labels
	for i := 0; i+1 < len(b) && len(l) < 4; i += 3 {
		l = l.set(fuzzKeys[int(b[i])%len(fuzzKeys)], fuzzValues[int(b[i+1])%len(fuzzValues)])
	}
	return l
}
