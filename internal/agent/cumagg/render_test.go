package cumagg

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"unicode/utf8"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

// checkCut asserts the properties every cut of v must have, whatever its
// contents: Trunc and Retain agree (the key and the label are one series), the
// result is a prefix of v within the byte bound, a non-empty value never cuts to
// "" (that would blank a dimension and merge two series), and a valid-UTF-8
// value cuts to valid UTF-8. The last is conditional on purpose: an invalid
// value has no rune boundary to honour, and keeps the byte bound instead.
func checkCut(t *testing.T, v string) string {
	t.Helper()
	got, kept := Trunc(v), Retain(v)
	if got != kept {
		t.Fatalf("Trunc and Retain disagree on a %d-byte value: %d vs %d bytes — the series key and its rendered label would differ", len(v), len(got), len(kept))
	}
	if len(got) > MaxLabelBytes {
		t.Fatalf("cut to %d bytes, over MaxLabelBytes %d", len(got), MaxLabelBytes)
	}
	if !strings.HasPrefix(v, got) {
		t.Fatalf("the cut of a %d-byte value is not its prefix", len(v))
	}
	if v != "" && got == "" {
		t.Fatalf("a non-empty %d-byte value cut to the empty string", len(v))
	}
	if utf8.ValidString(v) && !utf8.ValidString(got) {
		t.Fatalf("a valid-UTF-8 value cut to %d bytes of INVALID UTF-8 (the bound split a rune)", len(got))
	}
	return got
}

// The cut backs off to the start of any rune straddling the byte bound, and that
// back-off is the whole UTF-8 guarantee of Trunc and Retain: every value they
// cut becomes an OTLP string attribute, a protobuf field defined as UTF-8, and it
// arrives from unauthenticated senders. Every other truncation test in the tier
// packages uses ASCII, where a blind v[:MaxLabelBytes] is indistinguishable from
// the right answer — so this places 2-, 3- and 4-byte runes at every offset
// around the bound.
func TestTruncCutsOnARuneBoundary(t *testing.T) {
	for _, r := range []string{"é", "€", "😀"} {
		for lead := range 4 {
			// `lead` ASCII bytes, then the rune repeated past the bound: the
			// bound lands at every offset inside the rune across the leads.
			v := strings.Repeat("a", lead) + strings.Repeat(r, MaxLabelBytes)
			got := checkCut(t, v)
			// The back-off is to the LAST boundary at or below the bound, never
			// further: at most len(r)-1 bytes are given up.
			if floor := MaxLabelBytes - (len(r) - 1); len(got) < floor {
				t.Errorf("rune %q, lead %d: cut to %d bytes, want at least %d (backed off past the straddling rune)", r, lead, len(got), floor)
			}
		}
	}

	// The one-rune case the fix was written for: a 3-byte rune whose first byte
	// is the 256th, so a blind cut keeps a dangling lead byte.
	v := strings.Repeat("a", MaxLabelBytes-1) + "€"
	if got := checkCut(t, v); len(got) != MaxLabelBytes-1 {
		t.Errorf("a 3-byte rune straddling the bound: cut to %d bytes, want %d", len(got), MaxLabelBytes-1)
	}

	// No boundary to back off to: every byte of the prefix is a continuation
	// byte. The value is already invalid UTF-8, so the cut keeps the byte bound
	// rather than returning "" and blanking the dimension.
	cont := strings.Repeat("\x80", MaxLabelBytes+10)
	if got := checkCut(t, cont); len(got) != MaxLabelBytes {
		t.Errorf("an all-continuation value: cut to %d bytes, want exactly %d", len(got), MaxLabelBytes)
	}

	// At or under the bound nothing is cut — and Retain returns the SAME
	// string, since there is nothing to clone.
	short := strings.Repeat("€", MaxLabelBytes/3)
	if got := Retain(short); got != short {
		t.Errorf("a %d-byte value was altered", len(short))
	}
}

// The cut's properties over arbitrary input, UTF-8 or not.
func FuzzTruncRetain(f *testing.F) {
	for _, s := range []string{"", "x", strings.Repeat("a", MaxLabelBytes+1),
		strings.Repeat("a", MaxLabelBytes-1) + "€", strings.Repeat("😀", 100), strings.Repeat("\x80", 300)} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, v string) { checkCut(t, v) })
}

// valueCases is every pcommon value type, at the boundaries each one has.
func valueCases() map[string]pcommon.Value {
	m := map[string]pcommon.Value{
		"empty":           pcommon.NewValueEmpty(),
		"str":             pcommon.NewValueStr("GET /orders"),
		"str-empty":       pcommon.NewValueStr(""),
		"str-long":        pcommon.NewValueStr(strings.Repeat("a", MaxLabelBytes-1) + "€"),
		"bool-true":       pcommon.NewValueBool(true),
		"bool-false":      pcommon.NewValueBool(false),
		"double":          pcommon.NewValueDouble(1.5),
		"double-integral": pcommon.NewValueDouble(200),
		"double-nan":      pcommon.NewValueDouble(math.NaN()),
		"bytes":           pcommon.NewValueBytes(),
		"map":             pcommon.NewValueMap(),
		"slice":           pcommon.NewValueSlice(),
	}
	m["bytes"].Bytes().FromRaw([]byte("raw"))
	m["map"].Map().PutInt("status", 200)
	m["slice"].Slice().AppendEmpty().SetStr(strings.Repeat("h", 2*MaxLabelBytes))
	for _, n := range []int64{0, 9, 42, 99, 100, 200, 404, 999, 1000, 8080, -1, -200,
		math.MaxInt64, math.MinInt64} {
		m["int/"+pcommon.NewValueInt(n).AsString()] = pcommon.NewValueInt(n)
	}
	return m
}

// ValueStr exists to be AsString without the allocation, and a label is only the
// same label if it is the same BYTES — so the whole contract is equality.
func TestValueStrIsAsString(t *testing.T) {
	for name, v := range valueCases() {
		if got, want := ValueStr(v), v.AsString(); got != want {
			t.Errorf("%s: ValueStr = %q, AsString = %q", name, got, want)
		}
	}
	for n := int64(-5); n < smallInts+5; n++ {
		v := pcommon.NewValueInt(n)
		if got, want := ValueStr(v), v.AsString(); got != want {
			t.Fatalf("Int %d: ValueStr = %q, AsString = %q", n, got, want)
		}
	}
}

// A key part built from the Value must be exactly the key part built from the
// string the label is rendered from, or one attribute keys one series while
// rendering another's label set (a duplicate series in one payload) — or two
// attributes rendering the same label key two series.
func TestAppendValueKeyPartMatchesTheRenderedString(t *testing.T) {
	for name, v := range valueCases() {
		got := AppendValueKeyPart([]byte("prefix"), v)
		want := AppendKeyPart([]byte("prefix"), Trunc(v.AsString()))
		if !bytes.Equal(got, want) {
			t.Errorf("%s: AppendValueKeyPart = %q, AppendKeyPart(Trunc(AsString)) = %q", name, got, want)
		}
	}
	// An Int and the Str spelling it renders to render ONE label, so they must
	// be one key — the key encodes the rendering, never the type.
	if a, b := AppendValueKeyPart(nil, pcommon.NewValueInt(200)), AppendValueKeyPart(nil, pcommon.NewValueStr("200")); !bytes.Equal(a, b) {
		t.Errorf("Int 200 keys as %q and Str \"200\" as %q, but both render the label 200", a, b)
	}
}

// The point of both: an Int dimension — every HTTP status code — no longer
// allocates on the per-span path. AppendValueKeyPart at any magnitude (it formats
// into a stack buffer), ValueStr for the table's range (it returns a string that
// something may retain, so only a static one can be free).
func TestIntDimensionsAreAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	ValueStr(pcommon.NewValueInt(1)) // build the table outside the measurement
	var scratch [64]byte
	for _, n := range []int64{200, 404, 999, 8080, -1, math.MinInt64} {
		v := pcommon.NewValueInt(n)
		if allocs := testing.AllocsPerRun(100, func() { _ = AppendValueKeyPart(scratch[:0], v) }); allocs != 0 {
			t.Errorf("AppendValueKeyPart(Int %d) allocates %v times, want 0", n, allocs)
		}
	}
	for _, n := range []int64{0, 99, 200, 404, 999} {
		v := pcommon.NewValueInt(n)
		if allocs := testing.AllocsPerRun(100, func() { sinkStr = ValueStr(v) }); allocs != 0 {
			t.Errorf("ValueStr(Int %d) allocates %v times, want 0", n, allocs)
		}
	}
}

var sinkStr string

// rendersEmpty decides the span-then-resource fallback without rendering the
// value, and the fallback it replaced was `AsString() == ""`. So it must answer
// exactly that, for every value type — including the empty map and slice, which
// render "{}" and "[]" and therefore do NOT fall through.
func TestRendersEmptyIsAsStringEmpty(t *testing.T) {
	vals := map[string]pcommon.Value{
		"empty":       pcommon.NewValueEmpty(),
		"str":         pcommon.NewValueStr("x"),
		"str-empty":   pcommon.NewValueStr(""),
		"int-zero":    pcommon.NewValueInt(0),
		"double-zero": pcommon.NewValueDouble(0),
		"bool-false":  pcommon.NewValueBool(false),
		"bytes-empty": pcommon.NewValueBytes(),
		"bytes":       pcommon.NewValueBytes(),
		"map-empty":   pcommon.NewValueMap(),
		"slice-empty": pcommon.NewValueSlice(),
	}
	vals["bytes"].Bytes().FromRaw([]byte{0})
	for name, v := range vals {
		if got, want := rendersEmpty(v), v.AsString() == ""; got != want {
			t.Errorf("%s: rendersEmpty = %v, but AsString() = %q", name, got, v.AsString())
		}
	}
}

// DimValue is the one spelling of the dimension precedence both aggregators
// resolve by: the span attribute, then the resource, with a span attribute that
// renders EMPTY treated as absent.
func TestDimStrPrefersTheSpanAndFallsBackOnEmpty(t *testing.T) {
	span, res := pcommon.NewMap(), pcommon.NewMap()
	if got := DimStr(span, res, "k"); got != "" {
		t.Fatalf("neither sets it: %q, want \"\"", got)
	}
	res.PutStr("k", "from-resource")
	if got := DimStr(span, res, "k"); got != "from-resource" {
		t.Fatalf("resource only: %q", got)
	}
	span.PutStr("k", "")
	if got := DimStr(span, res, "k"); got != "from-resource" {
		t.Fatalf("an EMPTY span attribute must fall through to the resource, got %q", got)
	}
	span.PutInt("k", 200)
	if got := DimStr(span, res, "k"); got != "200" {
		t.Fatalf("the span attribute must win: %q", got)
	}
	if v, ok := DimValue(span, res, "k"); !ok || v.Type() != pcommon.ValueTypeInt {
		t.Fatalf("DimValue returned %v (ok=%v), want the span's Int", v.AsString(), ok)
	}
}
