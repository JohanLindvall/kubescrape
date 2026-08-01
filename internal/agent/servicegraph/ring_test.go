package servicegraph

import (
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"testing"
)

// randTraceIDs builds n pseudo-random 16-byte trace ids from a FIXED seed, so
// the statistical assertions below are a deterministic pass/fail rather than a
// test that flakes one run in a hundred.
func randTraceIDs(n int) [][]byte {
	rng := rand.New(rand.NewPCG(0x5eed, 0xf00d))
	out := make([][]byte, n)
	for i := range out {
		id := make([]byte, 16)
		hi, lo := rng.Uint64(), rng.Uint64()
		for b := 0; b < 8; b++ {
			id[b] = byte(hi >> (8 * b))
			id[8+b] = byte(lo >> (8 * b))
		}
		out[i] = id
	}
	return out
}

func shardNames(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("kubescrape-servicegraph-%d", i)
	}
	return out
}

// TestTokenForMatchesStdlib pins the hand-rolled FNV-1 against hash/fnv's
// New32 — the whole point of TokenFor is that it IS Tempo's, and Tempo's is
// hash/fnv. An accidental slide into FNV-1a (xor before multiply) would still
// distribute fine and would never be noticed without this.
func TestTokenForMatchesStdlib(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < 1000; i++ {
		id := make([]byte, 16)
		for j := range id {
			id[j] = byte(rng.Uint32())
		}
		tenant := ""
		if i%3 == 0 {
			tenant = fmt.Sprintf("tenant-%d", i)
		}
		h := fnv.New32()
		_, _ = h.Write([]byte(tenant))
		_, _ = h.Write(id)
		if got, want := TokenFor(tenant, id), h.Sum32(); got != want {
			t.Fatalf("TokenFor(%q, %x) = %d, hash/fnv New32 = %d", tenant, id, got, want)
		}
	}
	// A known-answer check for the empty key, so a future refactor of the
	// constants cannot pass by breaking both sides identically.
	if got := TokenFor("", nil); got != fnvOffset32 {
		t.Fatalf("TokenFor(\"\", nil) = %d, want the FNV offset basis %d", got, uint32(fnvOffset32))
	}
}

// TestOwnerStable is the invariant the whole feature rests on: the same trace
// id must resolve to the same shard every time, in every process. Two
// independently constructed rings (from differently ORDERED name lists) must
// agree, or two agents would send the two halves of one request to two
// different shards and no edge would ever form.
func TestOwnerStable(t *testing.T) {
	names := shardNames(8)
	a := NewRing(names, 0)
	shuffled := append([]string(nil), names...)
	rand.New(rand.NewPCG(7, 7)).Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	// Duplicates and an empty entry must not perturb the ring either: a
	// rendered config can repeat a name, and dropping one would re-partition
	// the circle for that agent alone.
	shuffled = append(shuffled, shuffled[0], "")
	b := NewRing(shuffled, 0)

	for _, id := range randTraceIDs(10000) {
		o1, o2 := a.Owner(id), b.Owner(id)
		if o1 != o2 {
			t.Fatalf("ring order changed ownership of %x: %q vs %q", id, o1, o2)
		}
		if o1 != a.Owner(id) {
			t.Fatalf("Owner is not deterministic for %x", id)
		}
	}
}

func TestEmptyRing(t *testing.T) {
	r := NewRing(nil, 0)
	if got := r.Owner([]byte{1, 2, 3}); got != "" {
		t.Fatalf("empty ring Owner = %q, want \"\"", got)
	}
	if len(r.Shards()) != 0 {
		t.Fatalf("empty ring has shards %v", r.Shards())
	}
	// One shard owns everything, and the arc accounting must say so (the
	// n == 1 wrap case).
	one := NewRing([]string{"only"}, 1)
	if got := one.Ownership()["only"]; got != 1 {
		t.Fatalf("single-token ring ownership = %v, want 1", got)
	}
	if got := one.Owner([]byte{9}); got != "only" {
		t.Fatalf("single shard Owner = %q", got)
	}
}

// TestDistribution checks both the ring's exact arc ownership and an actual
// draw of 100k trace ids against uniformity for 8 shards.
func TestDistribution(t *testing.T) {
	const (
		shards    = 8
		samples   = 100_000
		tolerance = 0.15 // +/-15% of the uniform share
	)
	r := NewRing(shardNames(shards), 0)

	uniform := 1.0 / shards
	for name, frac := range r.Ownership() {
		if dev := (frac - uniform) / uniform; dev < -tolerance || dev > tolerance {
			t.Errorf("arc ownership of %s = %.4f (%.1f%% off uniform %.4f)", name, frac, dev*100, uniform)
		}
	}

	counts := make(map[string]int, shards)
	for _, id := range randTraceIDs(samples) {
		counts[r.Owner(id)]++
	}
	if len(counts) != shards {
		t.Fatalf("only %d of %d shards received traces", len(counts), shards)
	}
	expect := float64(samples) / shards
	for name, n := range counts {
		dev := (float64(n) - expect) / expect
		t.Logf("%s: %d spans (%.1f%% off uniform)", name, n, dev*100)
		if dev < -tolerance || dev > tolerance {
			t.Errorf("shard %s got %d of %d traces (%.1f%% off uniform %.0f)", name, n, samples, dev*100, expect)
		}
	}
}

// TestScaleMovesOneOverN is the consistent-hashing property: growing the tier
// from N to N+1 shards must move ~1/(N+1) of the keys and leave the rest
// exactly where they were. A naive modulo-of-hash sharding would move ~N/(N+1)
// — i.e. nearly everything — and every in-flight half-edge in the tier would
// be orphaned on every scale change.
func TestScaleMovesOneOverN(t *testing.T) {
	const n = 8
	before := NewRing(shardNames(n), 0)
	after := NewRing(shardNames(n+1), 0)

	ids := randTraceIDs(100_000)
	moved, movedToNew := 0, 0
	newShard := fmt.Sprintf("kubescrape-servicegraph-%d", n)
	for _, id := range ids {
		o1, o2 := before.Owner(id), after.Owner(id)
		if o1 == o2 {
			continue
		}
		moved++
		if o2 == newShard {
			movedToNew++
		}
	}
	frac := float64(moved) / float64(len(ids))
	want := 1.0 / float64(n+1)
	t.Logf("scaling %d -> %d moved %.2f%% of keys (ideal %.2f%%)", n, n+1, frac*100, want*100)
	if frac < want*0.7 || frac > want*1.3 {
		t.Errorf("scaling moved %.3f of keys, want ~%.3f (1/N)", frac, want)
	}
	// Every moved key must have moved TO the new shard: consistent hashing
	// never reshuffles keys between incumbents.
	if movedToNew != moved {
		t.Errorf("%d of %d moved keys went somewhere other than the new shard", moved-movedToNew, moved)
	}
}

// TestTokensPerShardTightensDistribution documents why the default is not
// Cortex's 128: more tokens is strictly less skew, and the default must be on
// the flat part of that curve for a single-digit shard count.
func TestTokensPerShardTightensDistribution(t *testing.T) {
	names := shardNames(4)
	spread := func(tokens int) float64 {
		r := NewRing(names, tokens)
		lo, hi := 1.0, 0.0
		for _, f := range r.Ownership() {
			lo, hi = min(lo, f), max(hi, f)
		}
		return hi - lo
	}
	few, many := spread(16), spread(DefaultTokensPerShard)
	t.Logf("ownership spread: 16 tokens/shard = %.4f, %d tokens/shard = %.4f", few, DefaultTokensPerShard, many)
	if many >= few {
		t.Errorf("more tokens did not tighten the distribution: %v vs %v", many, few)
	}
	if many > 0.05 {
		t.Errorf("default tokens/shard leaves a %.3f ownership spread over 4 shards", many)
	}
}

func BenchmarkOwner(b *testing.B) {
	r := NewRing(shardNames(8), 0)
	ids := randTraceIDs(1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.Owner(ids[i&1023])
	}
}
