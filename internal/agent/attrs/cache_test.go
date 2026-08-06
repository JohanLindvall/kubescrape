package attrs

import (
	"fmt"
	"sync"
	"testing"
)

// countGenerations is the live entry count across both generations.
func countGenerations[V any](c *genCache[V]) int {
	n := 0
	count := func(m *sync.Map) {
		if m == nil {
			return
		}
		m.Range(func(any, any) bool { n++; return true })
	}
	count(c.cur.Load())
	count(c.prev.Load())
	return n
}

// Past the cap the cache must keep CACHING. Stopping admission bounds memory
// and nothing else: a working set larger than the cap then recompiles on every
// call — three orders of magnitude dearer than the lookup, on exactly the
// data-derived pattern (a template composing a regex from a label value) the
// cap was added for.
func TestCachedRegexpKeepsCachingPastTheCap(t *testing.T) {
	for i := range maxRegexKeys + maxRegexKeys/2 {
		if _, err := cachedRegexp(fmt.Sprintf(`^fill-%d-[a-z0-9]+$`, i)); err != nil {
			t.Fatal(err)
		}
	}
	const pat = `^(prod|stage)-[a-z0-9]{1,8}$`
	first, err := cachedRegexp(pat)
	if err != nil {
		t.Fatal(err)
	}
	again, err := cachedRegexp(pat)
	if err != nil {
		t.Fatal(err)
	}
	if first != again {
		t.Fatal("a pattern compiled past the cap was not cached: every later call recompiles it")
	}
	// And the eviction is what keeps that bounded: two generations, never more.
	if n := countGenerations(regexCache); n > 2*maxRegexKeys {
		t.Fatalf("cache holds %d entries; the cap bounds it at %d", n, 2*maxRegexKeys)
	}
}

// A compile ERROR is cached like a compiled regex — recompiling a broken
// pattern per resource built is the same cliff, and the error must stay the
// answer.
func TestCachedRegexpCachesFailures(t *testing.T) {
	if _, err := cachedRegexp(`^(unclosed`); err == nil {
		t.Fatal("want a compile error")
	}
	if re, err := cachedRegexp(`^(unclosed`); err == nil || re != nil {
		t.Fatalf("cached failure returned re=%v err=%v", re, err)
	}
}

// An entry in continuous use must survive a rotation: the previous generation
// is consulted and a hit there is PROMOTED, so the hot working set is not
// thrown away every time the cap is reached.
func TestGenCachePromotesAcrossRotations(t *testing.T) {
	c := newGenCache[int](4)
	c.store("hot", 1)
	for i := range 8 {
		key := fmt.Sprintf("cold-%d", i)
		c.store(key, i)
		if v, ok := c.load("hot"); !ok || v != 1 {
			t.Fatalf("after %d admissions: hot = %v, %v", i+1, v, ok)
		}
	}
	if n := countGenerations(c); n > 2*4 {
		t.Fatalf("cache holds %d entries; want at most 2x the cap", n)
	}
}

// A nil cache is a no-op, not a panic: Filter keeps one only when it filters
// anything at all.
func TestGenCacheNilIsNoOp(t *testing.T) {
	var c *genCache[bool]
	c.store("k", true)
	if v, ok := c.load("k"); ok || v {
		t.Fatalf("nil cache returned %v, %v", v, ok)
	}
}
