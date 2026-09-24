package attrs

import (
	"errors"
	"fmt"
	"regexp/syntax"
	"runtime"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/regexcost"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
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

// repeatAlternation is n alternatives of one distinct rune repeated 999 times:
// a few bytes each, ~1000 instructions each once the repeat is expanded.
func repeatAlternation(n int) string {
	alts := make([]string, n)
	for i := range alts {
		alts[i] = fmt.Sprintf("%c{999}", rune(0x4e00+i))
	}
	return strings.Join(alts, "|")
}

// A template regex's pattern may be DATA (an annotation, up to 8 KiB), and a
// repeat multiplies at compile time: 800 alternatives of `x{999}` fit in ~7 KB
// and compile to an 800k-instruction program — ~0.27 s and ~187 MB allocated
// to build, ~35 MB retained, per distinct pattern, with 2048 cached. The count
// cap bounded none of that. Such a pattern is now refused from its parse tree,
// before it is compiled, as regexp/syntax's own ErrLarge.
func TestOversizedRegexIsRefusedBeforeItIsCompiled(t *testing.T) {
	pat := repeatAlternation(800)
	if len(pat) > 8<<10 {
		t.Fatalf("setup: the pattern is %d bytes, over what one annotation value can carry", len(pat))
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	re, err := cachedRegexp(pat)
	runtime.ReadMemStats(&after)

	var syn *syntax.Error
	if !errors.As(err, &syn) || syn.Code != syntax.ErrLarge || re != nil {
		t.Fatalf("cachedRegexp: compiled=%v, error is ErrLarge=%v; want no program and a *syntax.Error with ErrLarge", re != nil, syn != nil && syn.Code == syntax.ErrLarge)
	}
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > 8<<20 {
		t.Fatalf("refusing the pattern allocated %d bytes: it was compiled before it was refused", alloc)
	}
	if re, err := cachedRegexp(pat); re != nil || !errors.As(err, &syn) {
		t.Fatalf("the refusal is not cached: the second call compiled=%v", re != nil)
	}
}

// The ceiling sits where regexcost.Insts puts it: 8 alternatives of x{999}
// are 7999 instructions and compile; 9 are 8999 and are refused.
func TestRegexProgramSizeCeiling(t *testing.T) {
	for _, tc := range []struct {
		alts    int
		refused bool
	}{{8, false}, {9, true}} {
		pat := repeatAlternation(tc.alts)
		parsed, err := syntax.Parse(pat, syntax.Perl)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%d alternatives: %d instructions estimated", tc.alts, regexcost.Insts(parsed, maxRegexInsts))
		re, err := cachedRegexp(pat)
		if refused := err != nil; refused != tc.refused || (re == nil) != tc.refused {
			t.Errorf("%d alternatives: compiled=%v refused=%v, want refused=%v", tc.alts, re != nil, err != nil, tc.refused)
		}
	}
	// And an ordinary DNS-label pattern is nowhere near it.
	parsed, err := syntax.Parse(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`, syntax.Perl)
	if err != nil {
		t.Fatal(err)
	}
	if n := regexcost.Insts(parsed, maxRegexInsts); n > 256 {
		t.Errorf("a DNS-label pattern estimates %d instructions", n)
	}
}

// A LITERAL oversized pattern is config, not data: the construction dry-run
// must refuse it at startup (templateConfigError reads the *syntax.Error), not
// leave the attribute silently absent from every resource.
func TestOversizedLiteralTemplateRegexIsAConfigError(t *testing.T) {
	tmpl := "{{ regexMatch `" + repeatAlternation(20) + "` .Pod.Name }}"
	_, err := NewBuilder(&Config{Attributes: map[string]string{"m": tmpl}}, nil)
	var syn *syntax.Error
	if !errors.As(err, &syn) || syn.Code != syntax.ErrLarge {
		t.Fatalf("NewBuilder = %v, want a startup error carrying syntax.ErrLarge", err)
	}
	// While a data-derived pattern of the same shape fails only its own render.
	b, err := NewBuilder(&Config{Attributes: map[string]string{
		"m": `{{ if regexMatch (index .Pod.Annotations "p") "x" }}yes{{ end }}`,
	}}, nil)
	if err != nil {
		t.Fatalf("NewBuilder refused a data-derived pattern: %v", err)
	}
	res := pcommon.NewResource()
	b.Build(res, Context{Pod: &kubemeta.Pod{Name: "x", Annotations: map[string]string{"p": repeatAlternation(20)}}})
	if v, ok := res.Attributes().Get("m"); ok {
		t.Fatalf("an oversized data-derived pattern rendered %q; want the attribute omitted", v.Str())
	}
	res = pcommon.NewResource()
	b.Build(res, Context{Pod: &kubemeta.Pod{Name: "x", Annotations: map[string]string{"p": "^x$"}}})
	if v, _ := res.Attributes().Get("m"); v.Str() != "yes" {
		t.Fatalf("an ordinary data-derived pattern rendered %q, want \"yes\"", v.Str())
	}
}

// The COUNT cap is not a memory bound when entries differ in size by orders of
// magnitude, so a weighted cache also rotates on WEIGHT.
func TestGenCacheRotatesOnWeight(t *testing.T) {
	c := newWeightedGenCache(100, 10, func(v int) int { return v })
	weight := func() int {
		w := 0
		sum := func(m *sync.Map) {
			if m != nil {
				m.Range(func(_, v any) bool { w += v.(int); return true })
			}
		}
		sum(c.cur.Load())
		sum(c.prev.Load())
		return w
	}
	for i := range 50 {
		key := fmt.Sprintf("k%d", i)
		c.store(key, 4)
		if w := weight(); w > 2*(10+4) {
			t.Fatalf("after %d entries of weight 4 the cache holds weight %d, over 2x its budget", i+1, w)
		}
		if _, ok := c.load(key); !ok {
			t.Fatalf("entry %d was not cached", i)
		}
	}
}

// And the template regex cache is wired to that budget: however large the
// accepted patterns are, what stays live is bounded by weight, not by count.
func TestRegexCacheIsBoundedByWeight(t *testing.T) {
	for i := range 40 {
		// ~7000 instructions each, under the per-pattern ceiling.
		pat := fmt.Sprintf("x%d(?:", i) + repeatAlternation(7) + ")"
		if _, err := cachedRegexp(pat); err != nil {
			t.Fatal(err)
		}
	}
	w := 0
	sum := func(m *sync.Map) {
		if m != nil {
			m.Range(func(_, v any) bool { w += v.(regexEntry).weight; return true })
		}
	}
	sum(regexCache.cur.Load())
	sum(regexCache.prev.Load())
	if limit := 2 * (maxRegexWeight + maxRegexInsts + 1<<10); w > limit {
		t.Fatalf("the regex cache holds %d instructions' weight, over its bound %d", w, limit)
	}
}
