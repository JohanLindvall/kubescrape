package debugtap

// An attribute filter is matched on the EXPORTING goroutine against names and
// values a sender chose. The matcher used to neutralize '/' in BOTH operands on
// every comparison — the pattern, fixed for the stream's life, and the value,
// the sender's length — so a `*nginx*` filter over a pushed 4 MiB '/'-dense
// value cost ~200 ms per resource (a copy, then path.Match's backtracking),
// several times the render the tap is budgeted for. Compiled once, a literal is
// ==, a `*`-only glob is a substring scan, and only a `?`/class/escape glob
// still copies a '/'-bearing operand.

import (
	"path"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/config"
	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

// neutralizedPathMatch is the DEFINITION the compiled matcher shortcuts:
// path.Match with '/' an ordinary character.
func neutralizedPathMatch(pat, s string) bool {
	ok, _ := path.Match(strings.ReplaceAll(pat, "/", "\x00"), strings.ReplaceAll(s, "/", "\x00"))
	return ok
}

func FuzzCompiledGlobMatchesItsDefinition(f *testing.F) {
	for _, seed := range [][2]string{
		{"*nginx*", "docker.io/library/nginx:1.25"},
		{"*/library/*", "docker.io/library/nginx:1.25"},
		{"k8s.pod.label.*/name", "k8s.pod.label.app.kubernetes.io/name"},
		{"team-*", "team-a"},
		{"a*b*c", "abc"},
		{"a*b*c", "acb"},
		{"ab*ba", "aba"},
		{"**", ""},
		{"*", "a/b"},
		{"[abc]/x", "a/x"},
		{`a\/b`, "a/b"},
		{"a?b", "a/b"},
		{"", ""},
		{"x", ""},
		{"*a", "a"},
		{"a*", "a"},
	} {
		f.Add(seed[0], seed[1])
	}
	f.Fuzz(func(t *testing.T, pat, s string) {
		// A NUL is where the definition was WRONG: it aliased a NUL in the
		// name to a '/' in the pattern, which the compiled literal and `*`
		// forms no longer do.
		if strings.ContainsRune(pat, 0) || strings.ContainsRune(s, 0) {
			return
		}
		if config.Glob(pat) != nil {
			return // refused at subscribe; never compiled
		}
		if got, want := compileGlob(pat).match(s), neutralizedPathMatch(pat, s); got != want {
			t.Errorf("glob %q matching %q = %v, the definition says %v", pat, s, got, want)
		}
	})
}

// The NUL aliasing the definition carried: a literal '/' in a pattern matched
// a NUL in a name. The compiled literal and `*` forms compare the bytes.
func TestCompiledGlobDoesNotAliasNULToSlash(t *testing.T) {
	for _, pat := range []string{"a/b", "a/*"} {
		if compileGlob(pat).match("a\x00b") {
			t.Errorf("glob %q matched a name holding NUL where the pattern has '/'", pat)
		}
	}
}

// The case the change exists for: a wide filter over a sender-sized,
// '/'-dense Str value must neither copy it nor render anything.
func TestWideFilterOverALargeStringIsAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector changes escape analysis and adds bookkeeping allocations")
	}
	attrs := pcommon.NewMap()
	attrs.PutStr("sender.blob", strings.Repeat("a/", 512<<10)) // 1 MiB
	attrs.PutStr("k8s.namespace.name", "team-a")
	for _, tc := range []struct {
		key, value string
		want       bool
	}{
		{"*", "*nginx*", false},        // walks the whole value, matches nothing
		{"sender.*", "a/*", true},      // prefix, then anything
		{"*", "*a/a/", true},           // suffix
		{"sender.blob", "a/a/", false}, // literal: a length mismatch, O(1)
		{"k8s.namespace.name", "team-a", true},
	} {
		f := newAttrFilter(tc.key, tc.value)
		if got := f.matches(attrs); got != tc.want {
			t.Fatalf("attr=%s=%s matched %v, want %v", tc.key, tc.value, got, tc.want)
		}
		if got := testing.AllocsPerRun(10, func() { f.matches(attrs) }); got != 0 {
			t.Errorf("attr=%s=%s over a 1 MiB '/'-dense value allocates %.0f times per match, want 0", tc.key, tc.value, got)
		}
	}
}

// BenchmarkWideFilterLargeString is the probe the change was measured with:
// `*nginx*` against one 4 MiB '/'-dense value.
func BenchmarkWideFilterLargeString(b *testing.B) {
	attrs := pcommon.NewMap()
	attrs.PutStr("service.name", strings.Repeat("a/", 2<<20))
	f := newAttrFilter("service.name", "*nginx*")
	b.ReportAllocs()
	for b.Loop() {
		if f.matches(attrs) {
			b.Fatal("matched")
		}
	}
}
