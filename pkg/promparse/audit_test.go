package promparse

import (
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
)

// collect parses input with the given parser and returns deep copies of the
// samples plus the malformed count and error.
func collect(t *testing.T, p *Parser, input string) ([]Sample, int, error) {
	t.Helper()
	var out []Sample
	malformed, err := p.Parse(strings.NewReader(input), func(s Sample) error {
		cp := s
		cp.Labels = append([]Label(nil), s.Labels...)
		if s.Exemplar != nil {
			ex := CopyExemplar(*s.Exemplar)
			cp.Exemplar = &ex
		}
		out = append(out, cp)
		return nil
	})
	return out, malformed, err
}

// ---------------------------------------------------------------------------
// P1. MaxLineBytes boundary: exactly at the limit, one over. The parser must
// never DESYNC (a skipped over-long line must not eat the following line).
// ---------------------------------------------------------------------------

func TestAudit_MaxLineBytesBoundary(t *testing.T) {
	const limit = 32
	mk := func(n int) string { // "a_metric{l="xxx"} 1" padded to exactly n bytes
		name := "m"
		pad := n - len(name) - len(" 1")
		if pad < 0 {
			t.Fatal("n too small")
		}
		return name + strings.Repeat("x", pad) + " 1"
	}
	for _, n := range []int{limit - 1, limit, limit + 1} {
		line := mk(n)
		if len(line) != n {
			t.Fatalf("built line of %d bytes, want %d", len(line), n)
		}
		p := New(Options{MaxLineBytes: limit})
		// The over-long line is followed by a good one: it must still parse.
		got, malformed, err := collect(t, p, line+"\nafter 7\n")
		if err != nil {
			t.Fatalf("len=%d: %v", n, err)
		}
		names := make([]string, len(got))
		for i, s := range got {
			names[i] = s.Name
		}
		t.Logf("line len=%d (limit=%d): samples=%v malformed=%d", n, limit, names, malformed)
		if len(got) == 0 || got[len(got)-1].Name != "after" {
			t.Fatalf("len=%d: DESYNC — the line after an over-long line was lost (got %v)", n, names)
		}
	}
}

// TestAudit_MaxLineBytesOffByOne pins the actual boundary: the check counts the
// trailing newline, so the effective content limit is MaxLineBytes-1 — and the
// SAME line is accepted when it is the last line with no trailing newline.
func TestAudit_MaxLineBytesOffByOne(t *testing.T) {
	const limit = 16
	line := "m" + strings.Repeat("x", limit-3) + " 1" // exactly 'limit' bytes
	if len(line) != limit {
		t.Fatalf("line is %d bytes, want %d", len(line), limit)
	}

	p1 := New(Options{MaxLineBytes: limit})
	withNL, m1, _ := collect(t, p1, line+"\n")

	p2 := New(Options{MaxLineBytes: limit})
	noNL, m2, _ := collect(t, p2, line)

	t.Logf("line of exactly MaxLineBytes(%d) bytes: with newline -> %d samples (malformed=%d); "+
		"without trailing newline -> %d samples (malformed=%d)", limit, len(withNL), m1, len(noNL), m2)
	if len(withNL) != len(noNL) {
		t.Logf("INCONSISTENT: MaxLineBytes counts the newline in the ReadSlice chunk, so a line of exactly " +
			"MaxLineBytes bytes is dropped when terminated and kept when it is the final unterminated line")
	}
}

// TestAudit_HugeLineNoDesync: an over-long line spanning many bufio refills.
func TestAudit_HugeLineNoDesync(t *testing.T) {
	huge := "junk" + strings.Repeat("y", 300_000)
	p := New(Options{MaxLineBytes: 1024})
	got, malformed, err := collect(t, p, "before 1\n"+huge+" 2\nafter 3\n")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Name != "before" || got[1].Name != "after" {
		t.Fatalf("desync around a 300KB line: got %+v", got)
	}
	if malformed != 1 {
		t.Errorf("malformed = %d, want 1", malformed)
	}
}

// ---------------------------------------------------------------------------
// P2. Pooled reuse: no state may leak between two different expositions.
// ---------------------------------------------------------------------------

func TestAudit_PooledReuseNoCrossContamination(t *testing.T) {
	first := `# TYPE http_requests counter
# TYPE lat histogram
http_requests_total{code="200",pod="a"} 5
lat_bucket{le="1",pod="a"} 3
lat_sum{pod="a"} 1.5
lat_count{pod="a"} 3
`
	// Second exposition REUSES the names but with DIFFERENT types, and reuses
	// label positions with different names/values — exactly what the lastMetric /
	// lastKV / types caches could leak across.
	second := `# TYPE http_requests gauge
# TYPE lat summary
http_requests{code="500",zone="b"} 9
lat{quantile="0.5",zone="b"} 7
lat_sum{zone="b"} 70
lat_count{zone="b"} 10
`
	run := func(body string) []Sample {
		pp := Get(Options{})
		defer Put(pp)
		var out []Sample
		_, err := pp.Parse(strings.NewReader(body), func(s Sample) error {
			cp := s
			cp.Labels = append([]Label(nil), s.Labels...)
			out = append(out, cp)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}

	// Warm the pooled parser with the first exposition, return it, then take it
	// back for the second — same *Pooled with high probability.
	_ = run(first)
	got := run(second)

	want := []struct {
		name, family string
		role         SampleRole
		labels       map[string]string
		value        float64
	}{
		{"http_requests", "http_requests", RoleGauge, map[string]string{"code": "500", "zone": "b"}, 9},
		{"lat", "lat", RoleSummaryQuantile, map[string]string{"quantile": "0.5", "zone": "b"}, 7},
		{"lat_sum", "lat", RoleSummarySum, map[string]string{"zone": "b"}, 70},
		{"lat_count", "lat", RoleSummaryCount, map[string]string{"zone": "b"}, 10},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d samples, want %d: %+v", len(got), len(want), got)
	}
	for i, w := range want {
		g := got[i]
		if g.Name != w.name || g.Family != w.family || g.Role != w.role || g.Value != w.value {
			t.Errorf("sample %d = {%s %s role=%d v=%v}, want {%s %s role=%d v=%v} — pooled state leaked",
				i, g.Name, g.Family, g.Role, g.Value, w.name, w.family, w.role, w.value)
		}
		if len(g.Labels) != len(w.labels) {
			t.Errorf("sample %d labels = %v, want %v", i, g.Labels, w.labels)
			continue
		}
		for _, l := range g.Labels {
			if w.labels[l.Name] != l.Value {
				t.Errorf("sample %d label %s=%q, want %q — stale lastKV entry", i, l.Name, l.Value, w.labels[l.Name])
			}
		}
	}
}

// TestAudit_PooledEOFNotSticky: a parser that saw "# EOF" must not refuse to
// parse the next (classic) exposition.
func TestAudit_PooledEOFNotSticky(t *testing.T) {
	pp := Get(Options{OpenMetrics: true})
	var n int
	if _, err := pp.Parse(strings.NewReader("a 1\n# EOF\n"), func(Sample) error { n++; return nil }); err != nil {
		t.Fatal(err)
	}
	Put(pp)
	if n != 1 {
		t.Fatalf("first parse emitted %d", n)
	}
	pp2 := Get(Options{})
	defer Put(pp2)
	n = 0
	if _, err := pp2.Parse(strings.NewReader("b 2\nc 3\n"), func(Sample) error { n++; return nil }); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("BUG: sticky eof — second parse emitted %d samples, want 2", n)
	}
}

// ---------------------------------------------------------------------------
// P3. Malformed input.
// ---------------------------------------------------------------------------

func TestAudit_MalformedInputs(t *testing.T) {
	cases := []struct {
		name  string
		input string
		om    bool
		// wantSamples is the number of samples the parser should emit.
		wantSamples int
	}{
		{"unterminated quote", `a{x="unclosed 1` + "\n" + "good 2\n", false, 1},
		{"unterminated brace", `a{x="v" 1` + "\n" + "good 2\n", false, 1},
		{"empty label name", `a{="v"} 1` + "\n" + "good 2\n", false, 1},
		{"no value", "a\ngood 2\n", false, 1},
		{"no value with labels", `a{x="1"}` + "\n" + "good 2\n", false, 1},
		{"value not a number", "a 1x\ngood 2\n", false, 1},
		{"duplicate labels", `a{x="1",x="2"} 3` + "\ngood 2\n", false, 2},
		{"utf8 name", "métrique 1\ngood 2\n", false, 2},
		{"crlf", "a 1\r\ngood 2\r\n", false, 2},
		{"trailing garbage after ts", "a 1 2 3\ngood 2\n", false, 1},
		{"NaN", "a NaN\ngood 2\n", false, 2},
		{"+Inf", "a +Inf\ngood 2\n", false, 2},
		{"-Inf", "a -Inf\ngood 2\n", false, 2},
		{"huge exponent", "a 1e400\ngood 2\n", false, 1}, // see TestAudit_HugeExponentDropped
		{"bare hash line", "#\ngood 2\n", false, 1},
		{"type line no type", "# TYPE a\ngood 2\n", false, 1},
		{"blank lines", "\n\n\ngood 2\n", false, 1},
		{"exemplar in classic", `a 1 # {t="x"} 2` + "\ngood 2\n", false, 1},
		{"om eof mid-stream", "a 1\n# EOF\nb 2\n", true, 1},
		{"om no eof", "a 1\nb 2\n", true, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := New(Options{OpenMetrics: tc.om})
			got, malformed, err := collect(t, p, tc.input)
			if err != nil {
				t.Fatalf("Parse returned err %v (must never fail on malformed input)", err)
			}
			names := make([]string, len(got))
			for i, s := range got {
				names[i] = fmt.Sprintf("%s=%v", s.Name, s.Value)
			}
			t.Logf("samples=%v malformed=%d", names, malformed)
			if len(got) != tc.wantSamples {
				t.Errorf("emitted %d samples %v, want %d", len(got), names, tc.wantSamples)
			}
			// Whatever happens, the parser must not desync: "good 2" must survive
			// wherever it appears after the malformed construct.
			if tc.wantSamples > 0 && strings.Contains(tc.input, "good 2") {
				last := got[len(got)-1]
				if last.Name != "good" || last.Value != 2 {
					t.Errorf("DESYNC: last sample is %s=%v, want good=2", last.Name, last.Value)
				}
			}
		})
	}
}

// TestAudit_HugeExponentDropped pins the divergence from Prometheus, which
// clamps an out-of-range float to ±Inf rather than rejecting the line.
func TestAudit_HugeExponentDropped(t *testing.T) {
	p := New(Options{})
	got, malformed, _ := collect(t, p, "a 1e400\n")
	if len(got) == 1 && math.IsInf(got[0].Value, 1) {
		t.Logf("1e400 -> +Inf (matches Prometheus)")
		return
	}
	t.Logf("DIVERGENCE: 1e400 is counted malformed (%d) and dropped (%d samples); Prometheus clamps it to +Inf "+
		"(strconv.ParseFloat returns +Inf with ErrRange and the error is treated as fatal for the line)",
		malformed, len(got))
}

// ---------------------------------------------------------------------------
// P4. Exemplars.
// ---------------------------------------------------------------------------

func TestAudit_Exemplars(t *testing.T) {
	input := `# TYPE lat histogram
lat_bucket{le="1"} 3 # {trace_id="abc"} 0.7 1520879607.789
lat_bucket{le="2"} 5 # {trace_id="def"} 0.9
lat_bucket{le="+Inf"} 6
# EOF
`
	p := New(Options{OpenMetrics: true, Exemplars: true})
	got, malformed, err := collect(t, p, input)
	if err != nil || malformed != 0 {
		t.Fatalf("err=%v malformed=%d", err, malformed)
	}
	if len(got) != 3 {
		t.Fatalf("got %d samples", len(got))
	}
	if got[0].Exemplar == nil || got[0].Exemplar.Value != 0.7 || got[0].Exemplar.TimestampMs != 1520879607789 {
		t.Fatalf("exemplar 0 = %+v, want value 0.7 ts 1520879607789", got[0].Exemplar)
	}
	if got[1].Exemplar == nil || got[1].Exemplar.TimestampMs != 0 {
		t.Fatalf("exemplar 1 = %+v, want ts 0", got[1].Exemplar)
	}
	if got[2].Exemplar != nil {
		t.Fatalf("exemplar 2 = %+v, want nil", got[2].Exemplar)
	}
	// The parser reuses one Exemplar struct: without CopyExemplar the caller
	// would see the last line's exemplar everywhere. Pin that CopyExemplar
	// actually deep-copies the labels.
	if got[0].Exemplar.Labels[0].Value != "abc" || got[1].Exemplar.Labels[0].Value != "def" {
		t.Fatalf("CopyExemplar did not deep-copy: %v / %v", got[0].Exemplar.Labels, got[1].Exemplar.Labels)
	}
}

// TestAudit_ExemplarsDisabledStillValid: with Exemplars off, an exemplar-bearing
// OpenMetrics line must still yield its sample (not be counted malformed).
func TestAudit_ExemplarsDisabledStillValid(t *testing.T) {
	p := New(Options{OpenMetrics: true, Exemplars: false})
	got, malformed, err := collect(t, p, `a 1 # {t="x"} 2`+"\n# EOF\n")
	if err != nil || malformed != 0 || len(got) != 1 || got[0].Exemplar != nil {
		t.Fatalf("got %d samples (malformed=%d err=%v), want 1 with no exemplar", len(got), malformed, err)
	}
}

// ---------------------------------------------------------------------------
// P5. The "aborted parse still exports what was converted" contract.
// ---------------------------------------------------------------------------

func TestAudit_AbortedParseKeepsEmitted(t *testing.T) {
	var body strings.Builder
	body.WriteString("# TYPE a counter\n")
	for i := 0; i < 100; i++ {
		fmt.Fprintf(&body, "a_total{i=\"%d\"} %d\n", i, i)
	}
	p := New(Options{})
	var n int
	malformed, err := p.Parse(strings.NewReader(body.String()), func(Sample) error {
		n++
		if n == 10 {
			return ErrTooManySamples
		}
		return nil
	})
	if !errors.Is(err, ErrTooManySamples) {
		t.Fatalf("err = %v, want ErrTooManySamples", err)
	}
	if n != 10 {
		t.Fatalf("emitted %d samples after abort, want 10", n)
	}
	t.Logf("aborted after %d samples (malformed=%d) — the caller keeps everything already emitted", n, malformed)
}

// TestAudit_TruncatedBodyMidLine: a read error mid-body must surface but keep
// the samples parsed before it.
func TestAudit_TruncatedBodyMidLine(t *testing.T) {
	r := io.MultiReader(strings.NewReader("a 1\nb 2\nc "), errReader{})
	p := New(Options{})
	var n int
	_, err := p.Parse(r, func(Sample) error { n++; return nil })
	if err == nil {
		t.Fatal("want the read error surfaced")
	}
	if n != 2 {
		t.Fatalf("emitted %d samples before the read error, want 2 (a and b)", n)
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("boom") }

// ---------------------------------------------------------------------------
// P6. Timestamps.
// ---------------------------------------------------------------------------

func TestAudit_Timestamps(t *testing.T) {
	pc := New(Options{})
	got, _, err := collect(t, pc, "a 1 1520879607789\n")
	if err != nil || len(got) != 1 || got[0].TimestampMs != 1520879607789 {
		t.Fatalf("classic ts: %+v err=%v", got, err)
	}
	po := New(Options{OpenMetrics: true})
	got, _, err = collect(t, po, "a 1 1520879607.789\n# EOF\n")
	if err != nil || len(got) != 1 || got[0].TimestampMs != 1520879607789 {
		t.Fatalf("openmetrics ts: %+v err=%v", got, err)
	}
	// A classic-format timestamp fed to an OpenMetrics parser is seconds, so it
	// is scaled by 1000 — 48000 years into the future. That is the caller's
	// problem (parse mode comes from Content-Type), but pin the behavior.
	got, _, _ = collect(t, po, "a 1 1520879607789\n# EOF\n")
	t.Logf("classic ms timestamp parsed in OpenMetrics mode -> %d ms", got[0].TimestampMs)
}
