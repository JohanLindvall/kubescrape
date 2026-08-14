package metrics

import (
	"log/slog"
	"testing"

	"github.com/zeebo/xxh3"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestLabelsHashOrderIndependent(t *testing.T) {
	a := labels{{"country", "ad"}, {"status", "2xx"}, {"zone", "eu"}}
	b := labels{{"zone", "eu"}, {"country", "ad"}, {"status", "2xx"}}
	if a.hash() != b.hash() {
		t.Errorf("hash not order-independent: %d vs %d", a.hash(), b.hash())
	}
	if a.hash() == (labels{{"country", "ad"}, {"status", "3xx"}, {"zone", "eu"}}).hash() {
		t.Error("different label sets share a hash")
	}
	if labels(nil).hash() != mixHash(xxh3.Uint128{}) {
		t.Error("empty hash unexpected")
	}
}

func TestLabelsHashAccumFoldable(t *testing.T) {
	// Folding a label in via addition must equal hashing the full set — the
	// property the histogram observe path relies on.
	base := labels{{"a", "1"}, {"b", "2"}}
	full := append(labels{}, base...).set("le", "0.5")
	// The fold is xor128, and folding a pair in must equal hashing the full set.
	folded := xor128(base.hashAccum(), combineHash(strHash("le"), strHash("0.5")))
	if mixHash(folded) != full.hash() {
		t.Error("the folded-in le label does not match hashing the full set")
	}
}

// TestXorFoldIsSafeBecauseNoFoldCanRepeatAPair pins the price of the XOR fold
// and the guarantees that pay it.
//
// XOR is blind to EVEN MULTIPLICITY: a contribution folded twice cancels. That
// is not a defect to fix in the arithmetic — it is the property that makes the
// fold self-inverse, which is what resLabelsAccum's cancel relies on. It is
// safe only because no fold in this package can be handed a duplicate, so this
// test asserts BOTH halves: that the hazard is real, and that each fold closes
// it itself rather than requiring it of a caller.
func TestXorFoldIsSafeBecauseNoFoldCanRepeatAPair(t *testing.T) {
	// The hazard, stated outright: fold one pair twice and it vanishes.
	c := combineHash(strHash("user"), strHash("alice"))
	if xor128(c, c) != (xxh3.Uint128{}) {
		t.Fatal("xor128 is no longer self-inverse; the cancel in resLabelsAccum depends on it")
	}

	// Door 1: labels.set replaces by key, so a label set cannot hold one key
	// twice — the shape that would cancel is unrepresentable.
	l := labels{}.set("user", "alice").set("user", "alice")
	if len(l) != 1 {
		t.Fatalf("set kept %d entries for one key; a duplicate would cancel under XOR", len(l))
	}
	if l.hashAccum() == (labels{}).hashAccum() {
		t.Fatal("a deduped single-pair set collided with the empty set")
	}

	// Door 2: a WIRE resource may legally repeat a key (OTLP encodes attributes
	// as a repeated KeyValue; pdata does not dedupe on decode). Folded entry by
	// entry that would cancel to the EMPTY resource's hash — a merge with
	// something unrelated, which is strictly worse than the duplicate-series
	// bug a wrapping sum would have left. resourceAccum proves key-uniqueness
	// before it takes that shortcut, so it does not.
	dup := dupKeyResource(t, "service.name", "a", "a")
	if got := resourceAccum(dup); got == (xxh3.Uint128{}) {
		t.Fatal("a resource repeating one key cancelled to the empty hash")
	}
	if resourceAccum(dup) != resourceAccum(res(map[string]string{"service.name": "a"})) {
		t.Fatal("a repeated key must hash as the identity it RENDERS, which is the deduped one")
	}
}

func TestLabelsSetGetWithout(t *testing.T) {
	l := labels{{"a", "1"}}
	l = l.set("b", "2")
	l = l.set("a", "9") // replace
	l = l.set("c", "")  // empty ignored
	if v, _ := l.get("a"); v != "9" {
		t.Errorf("a = %q", v)
	}
	if _, ok := l.get("c"); ok {
		t.Error("empty value stored")
	}
	l = l.without("a")
	if _, ok := l.get("a"); ok {
		t.Error("without did not remove")
	}
	if len(l) != 1 || l[0].key != "b" {
		t.Errorf("after without = %+v", l)
	}
}

func TestLabelsParseUnparseRoundTrip(t *testing.T) {
	l := labels{{"z", "last"}, {"a", `quote"and\slash`}, {"b", "line\nbreak"}}
	s := l.String()
	if s != `{a="quote\"and\\slash", b="line\nbreak", z="last"}` {
		t.Fatalf("String = %s", s)
	}
	back, err := parseLabels(s)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := back.get("a"); v != `quote"and\slash` {
		t.Errorf("round-trip a = %q", v)
	}
	if v, _ := back.get("b"); v != "line\nbreak" {
		t.Errorf("round-trip b = %q", v)
	}
}

// TestResourceLabelOverrideKeysMergedIdentity pins resLabelsAccum: a resource
// label overriding a resource attribute must hash like the merged set that
// resourceString serializes, so {svc:foo}+override svc=bar and a plain
// {svc:bar} are ONE series, not two samples with identical serialized
// identity (duplicate data points in one payload).
func TestResourceLabelOverrideKeysMergedIdentity(t *testing.T) {
	resFoo := pcommon.NewMap()
	resFoo.PutStr("svc", "foo")
	resBar := pcommon.NewMap()
	resBar.PutStr("svc", "bar")

	s := newTestSeries(seriesSpec{name: "m", kind: kindCounter, action: actionSet, log: slog.Default()})
	s.observe(nil, 1, resourceAccum(resFoo), resFoo, labels{{"svc", "bar"}})
	s.observe(nil, 1, resourceAccum(resBar), resBar, nil)

	if got := len(s.db); got != 1 {
		t.Fatalf("distinct samples: %d, want 1 (override must key as the merged set)", got)
	}
	for _, samp := range s.db {
		if samp.value != 2 {
			t.Fatalf("merged value: %v, want 2", samp.value)
		}
	}
}

// Resource keys are arbitrary config-supplied strings: a key containing '='
// (or ',', '"', '\') must round-trip String→parseLabels exactly — an
// unescaped '=' made the parser cut the pair at the wrong place, silently
// renaming the exported attribute and mangling its value.
func TestLabelsKeyEscapingRoundTrip(t *testing.T) {
	in := labels{}
	in = in.set("evil=key", "v")
	in = in.set("comma,key", "w")
	in = in.set(`back\slash`, "x")
	in = in.set("plain", "y")
	out, err := parseLabels(in.String())
	if err != nil {
		t.Fatal(err)
	}
	if in.String() != out.String() {
		t.Fatalf("round trip changed the set:\n in: %s\nout: %s", in.String(), out.String())
	}
	if got, _ := out.get("evil=key"); got != "v" {
		t.Fatalf(`get("evil=key") = %q, want "v"`, got)
	}
}

// A key's HASHED identity must equal its RENDERED one. String writes ", "
// between pairs, so parseLabels — which is what export.go reads a sample's
// labels back through — used to TrimSpace the key and ate an edge space the
// hash had counted: " env" and "env" were two live series exporting
// byte-identical attributes, and an escaped trailing space came back with a
// dangling backslash.
func TestLabelKeyEdgeSpaceRoundTrips(t *testing.T) {
	for _, key := range []string{" env", "env ", " env ", " ", "two words"} {
		in := labels{}.set(key, "v")
		in = in.set("other", "w")
		out, err := parseLabels(in.String())
		if err != nil {
			t.Fatalf("parseLabels(%q): %v", in.String(), err)
		}
		if got, ok := out.get(key); !ok || got != "v" {
			t.Fatalf("key %q round-tripped as %v (serialized %q)", key, out, in.String())
		}
		if out.hash() != in.hash() {
			t.Fatalf("key %q: hash changed across the round trip", key)
		}
	}
	// And the two keys stay two series all the way to the rendered form.
	spaced, plain := labels{}.set(" env", "v"), labels{}.set("env", "v")
	rs, _ := parseLabels(spaced.String())
	rp, _ := parseLabels(plain.String())
	if rs.String() == rp.String() {
		t.Fatalf("%q and %q render identically as %s", " env", "env", rs.String())
	}
}

// TestXor128IsItsOwnInverse pins the property the fold rests on: folding a
// contribution out is the SAME operation as folding it in, which is what lets
// resLabelsAccum cancel a resource key an extra label overrides without a
// second primitive. Checked over values that exercise both halves.
func TestXor128IsItsOwnInverse(t *testing.T) {
	const max = ^uint64(0)
	vals := []xxh3.Uint128{
		{},
		{Lo: 1},
		{Hi: 1},
		{Hi: max, Lo: max},
		{Lo: max},
		{Hi: max},
		{Hi: 7, Lo: max - 3},
		{Hi: 0x0123456789abcdef, Lo: 0xfedcba9876543210},
	}
	for _, a := range vals {
		for _, b := range vals {
			if got := xor128(xor128(a, b), b); got != a {
				t.Errorf("xor128(xor128(%v, %v), %v) = %v, want %v", a, b, b, got, a)
			}
		}
	}
}
