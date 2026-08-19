package otlpingest

// The rendering-size estimate (logchain.go) may never UNDER-charge: it decides
// whether a structured body from an unauthenticated sender is materialised
// through AsString, and everything downstream — enrichment's regexes,
// log-metrics label extraction, the rules — reads that rendering.

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

// hostileStrings are the shapes whose JSON rendering costs more than their
// bytes. The last two are what the estimate missed: pdata does no UTF-8
// validation on decode, so arbitrary bytes reach a body string.
var hostileStrings = map[string]string{
	"plain":       strings.Repeat("a", 1000),
	"quotes":      strings.Repeat(`"`, 1000),
	"backslashes": strings.Repeat(`\`, 1000),
	"nul":         strings.Repeat("\x00", 1000),
	"control":     strings.Repeat("\x1f", 1000),
	"newlines":    strings.Repeat("\n", 1000),
	"tabs":        strings.Repeat("\t", 1000),
	"invalidUTF8": strings.Repeat("\xff", 1000),
	"u2028":       strings.Repeat(" ", 300),
	"u2029":       strings.Repeat(" ", 300),
	"mixed":       strings.Repeat("a\xff\"\n \x00", 200),
	"multibyte":   strings.Repeat("é🙂", 300),
	"html":        strings.Repeat("<&>", 300), // HTML escaping is OFF: 1x
}

// The invariant, checked against the REAL renderer in both positions a string
// can occupy. A key was charged len() flat and never went through escapedLen at
// all, which put the exact 6x undercharge escapedLen exists to prevent back in
// the one position that skipped it.
func TestEscapedLenNeverUnderchargesTheRenderer(t *testing.T) {
	for name, s := range hostileStrings {
		est := escapedLen(s)

		v := pcommon.NewValueMap()
		v.Map().PutStr("k", s)
		// {"k":"<value>"} — the framing is 8 bytes.
		if got := len(v.AsString()) - 8; est < got {
			t.Errorf("%s as a VALUE: escapedLen = %d, AsString renders %d (%.2fx undercharged)",
				name, est, got, float64(got)/float64(est))
		}

		k := pcommon.NewValueMap()
		k.Map().PutStr(s, "v")
		if got := len(k.AsString()) - 8; est < got {
			t.Errorf("%s as a KEY: escapedLen = %d, AsString renders %d (%.2fx undercharged)",
				name, est, got, float64(got)/float64(est))
		}
	}
}

// The bytes arm has the same invariant: base64 rounds UP, and the truncating
// estimate came in low for every value that is not a multiple of three.
func TestBytesEstimateNeverUnderchargesBase64(t *testing.T) {
	for n := 0; n < 32; n++ {
		v := pcommon.NewValueMap()
		v.Map().PutEmptyBytes("k").FromRaw(make([]byte, n))
		rem := maxChainBodyBytes
		renderedSizeOver(v, &rem, 0)
		charged := maxChainBodyBytes - rem
		if got := len(v.AsString()); charged < got {
			t.Errorf("%d-byte value: charged %d, AsString renders %d", n, charged, got)
		}
	}
}

// The whole estimate, not just its string arm: random value trees, checked
// against the real renderer. This is what keeps the arms that model FRAMING
// honest — the map braces, the per-entry quotes and comma, the `null` an empty
// bytes value renders as — none of which the string cases above can see.
func TestRenderedSizeEstimateNeverUnderchargesTheRenderer(t *testing.T) {
	rnd := rand.New(rand.NewSource(20260818)) // fixed seed: a failure must be reproducible
	strs := []string{"", "a", `"`, "\n", "\x00", "\xff", " ", "é", strings.Repeat("k", 17)}
	var build func(depth int) pcommon.Value
	build = func(depth int) pcommon.Value {
		switch n := rnd.Intn(7); {
		case n == 0 && depth < 4:
			v := pcommon.NewValueMap()
			for i := 0; i < rnd.Intn(4); i++ {
				build(depth + 1).CopyTo(v.Map().PutEmpty(strs[rnd.Intn(len(strs))]))
			}
			return v
		case n == 1 && depth < 4:
			v := pcommon.NewValueSlice()
			for i := 0; i < rnd.Intn(4); i++ {
				build(depth + 1).CopyTo(v.Slice().AppendEmpty())
			}
			return v
		case n == 2:
			return pcommon.NewValueInt(int64(rnd.Intn(1 << 30)))
		case n == 3:
			// Not rnd.Float64(): that is non-negative and effectively never
			// lands in [1e-6, 1e-5), which is the one band where encoding/json
			// takes its 'f' branch and a SIGN costs a 25th character. A
			// positive value there renders exactly the charged width, so the
			// old generator passed on the boundary without ever touching the
			// side that could fail.
			switch rnd.Intn(3) {
			case 0:
				return pcommon.NewValueDouble(-rnd.Float64() * 1e-5)
			case 1:
				return pcommon.NewValueDouble(rnd.Float64() * 1e-5)
			default:
				return pcommon.NewValueDouble(rnd.Float64())
			}
		case n == 4:
			return pcommon.NewValueBool(rnd.Intn(2) == 0)
		case n == 5:
			v := pcommon.NewValueBytes()
			v.Bytes().FromRaw(make([]byte, rnd.Intn(10)))
			return v
		default:
			return pcommon.NewValueStr(strs[rnd.Intn(len(strs))])
		}
	}
	const budget = 1 << 20
	for i := 0; i < 2000; i++ {
		v := build(0)
		if v.Type() != pcommon.ValueTypeMap && v.Type() != pcommon.ValueTypeSlice {
			continue // chainBody only estimates structured bodies
		}
		rem := budget
		renderedSizeOver(v, &rem, 0)
		charged := budget - rem
		if got := len(v.AsString()); charged < got {
			t.Fatalf("charged %d, AsString renders %d for %s", charged, got, v.AsString())
		}
	}
}

// The scalar arm is the one the random tree above structurally CANNOT fail on:
// its bools (5 rendered), ints (20 at worst) and byte values over-charge by
// enough to mask a per-entry shortfall in a sibling. A body that is ALL scalars
// of the widest-rendering shape accumulates it instead — which is how the
// estimate came to be one byte short per entry for a NEGATIVE float64 in
// [1e-6, 1e-5), the only band where encoding/json takes its 'f' branch rather
// than 'e' and a sign buys a 25th character. 30k entries turn that into a ~3%
// breach of a bound documented as hard.
func TestScalarChargeCoversTheWidestRenderedNumber(t *testing.T) {
	const entries = 30000
	m := pcommon.NewValueMap()
	for i := 0; i < entries; i++ {
		m.Map().PutDouble(fmt.Sprintf("k%05d", i), -1.2345678901234567e-6)
	}
	const budget = 1 << 30
	rem := budget
	renderedSizeOver(m, &rem, 0)
	charged := budget - rem
	if got := len(m.AsString()); charged < got {
		t.Fatalf("charged %d, AsString renders %d: the scalar arm under-charges by %d over %d entries",
			charged, got, got-charged, entries)
	}
}

// End to end: the guard has to BIND — chainBody must refuse a body whose
// rendering blows the bound, in either position, rather than returning a
// string many times maxChainBodyBytes.
func TestHostileBodyRenderingIsRefusedByTheBound(t *testing.T) {
	const fill = maxChainBodyBytes - 4096 // a body the estimate would pass at 1x
	for name, tc := range map[string]struct{ key, val string }{
		"invalid UTF-8 value": {"k", strings.Repeat("\xff", fill)},
		"NUL key":             {strings.Repeat("\x00", fill), "v"},
		"quoted key":          {strings.Repeat(`"`, fill), "v"},
		"U+2028 value":        {"k", strings.Repeat(" ", fill/3)},
	} {
		lr := plog.NewLogRecord()
		lr.Body().SetEmptyMap().PutStr(tc.key, tc.val)
		body, ok := chainBody(lr)
		if ok {
			t.Errorf("%s: chainBody admitted a body rendering to %d bytes, %.2fx the %d bound",
				name, len(body), float64(len(body))/float64(maxChainBodyBytes), maxChainBodyBytes)
		}
	}
}

// The other half of correctness: an ordinary body still goes through, and the
// estimate is not so conservative that real structured logs stop being
// enriched. \n, \r and \t used to be charged 6 bytes each — three times what
// they render as — which is a multi-line body refused at a third of the bound.
func TestOrdinaryStructuredBodyStillPassesTheBound(t *testing.T) {
	lr := plog.NewLogRecord()
	m := lr.Body().SetEmptyMap()
	m.PutStr("msg", strings.Repeat("a line of log text\n", 20000)) // ~380 KiB, mostly newlines
	m.PutStr("level", "error")
	m.PutInt("code", 500)
	body, ok := chainBody(lr)
	if !ok {
		t.Fatalf("an ordinary %d-byte multi-line body was refused", m.Len())
	}
	if !strings.Contains(body, `"level":"error"`) {
		t.Errorf("body = %.80q...", body)
	}
}
