package metrics

import (
	"testing"

	"github.com/cespare/xxhash/v2"
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
	if labels(nil).hash() != mixHash(0) {
		t.Error("empty hash unexpected")
	}
}

func TestLabelsHashAccumFoldable(t *testing.T) {
	// Folding a label in via addition must equal hashing the full set — the
	// property the histogram observe path relies on.
	base := labels{{"a", "1"}, {"b", "2"}}
	full := append(labels{}, base...).set("le", "0.5")
	folded := base.hashAccum() + combineHash(xxhash.Sum64String("le"), xxhash.Sum64String("0.5"))
	if mixHash(folded) != full.hash() {
		t.Error("sum-folded le label does not match full hash")
	}
}

func TestLabelsHashNoDuplicateCancellation(t *testing.T) {
	// The regression the sum fold fixes: with XOR, an identical key=value pair
	// contributed from two sets (data-point labels and resource labels)
	// cancelled out, making every user's series hash identical.
	alice := labels{{"user", "alice"}}
	bob := labels{{"user", "bob"}}
	if alice.hashAccum()+alice.hashAccum() == bob.hashAccum()+bob.hashAccum() {
		t.Error("duplicated pair still cancels: distinct users share a hash")
	}
	if alice.checkAccum() == bob.checkAccum() {
		t.Error("check accumulators collide for distinct values")
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
