package promparse

import (
	"math"
	"strings"
	"testing"
)

func parseAllMode(t *testing.T, input string, openMetrics, exemplars bool) []Sample {
	t.Helper()
	p := New(Options{MaxLineBytes: 1 << 20, OpenMetrics: openMetrics, Exemplars: exemplars})
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
	if err != nil {
		t.Fatal(err)
	}
	if malformed != 0 {
		t.Fatalf("%d malformed lines", malformed)
	}
	return out
}

func parseAll(t *testing.T, input string) []Sample {
	return parseAllMode(t, input, false, false)
}

func TestParseBasic(t *testing.T) {
	samples := parseAll(t, `
# HELP http_requests_total Total requests.
# TYPE http_requests_total counter
http_requests_total{method="get",code="200"} 1027 1395066363000
http_requests_total{method="post",code="200"} 3

# TYPE temperature gauge
temperature 36.5
untyped_metric{a="b"} 1
`)
	if len(samples) != 4 {
		t.Fatalf("got %d samples: %+v", len(samples), samples)
	}
	s := samples[0]
	if s.Name != "http_requests_total" || s.Role != RoleCounter || s.Value != 1027 || s.TimestampMs != 1395066363000 {
		t.Fatalf("sample = %+v", s)
	}
	if len(s.Labels) != 2 || s.Labels[0] != (Label{"method", "get"}) || s.Labels[1] != (Label{"code", "200"}) {
		t.Fatalf("labels = %+v", s.Labels)
	}
	if samples[2].Role != RoleGauge || samples[2].Value != 36.5 {
		t.Fatalf("gauge = %+v", samples[2])
	}
	if samples[3].Role != RoleGauge {
		t.Fatalf("untyped should map to gauge: %+v", samples[3])
	}
}

// HELP and UNIT reach every sample of their family — they become the OTLP
// metric's Description and Unit, which is what an OTLP consumer displays. They
// are keyed by FAMILY, so a histogram's component series and an OpenMetrics
// counter's _total series (whose names differ from the family) carry them too.
func TestParseHelpAndUnit(t *testing.T) {
	samples := parseAllMode(t, `# HELP http_duration_seconds Latency of \\HTTP\\ requests.\nSecond line.
# TYPE http_duration_seconds histogram
# UNIT http_duration_seconds seconds
http_duration_seconds_bucket{le="0.1"} 100
http_duration_seconds_sum 53.4
# HELP requests Total requests.
# TYPE requests counter
requests_total 7
undeclared 1
# EOF
`, true, false)
	if len(samples) != 4 {
		t.Fatalf("got %d samples: %+v", len(samples), samples)
	}
	wantHelp := "Latency of \\HTTP\\ requests.\nSecond line."
	for _, s := range samples[:2] {
		if s.Help != wantHelp || s.Unit != "seconds" {
			t.Fatalf("%s: help=%q unit=%q, want %q/%q", s.Name, s.Help, s.Unit, wantHelp, "seconds")
		}
	}
	// The counter's series name is requests_total; its HELP is on the family.
	if samples[2].Name != "requests_total" || samples[2].Help != "Total requests." || samples[2].Unit != "" {
		t.Fatalf("counter = %+v", samples[2])
	}
	// A family that declared neither carries neither — no leakage from the
	// previous family through the last-family memo.
	if samples[3].Help != "" || samples[3].Unit != "" {
		t.Fatalf("undeclared family = %+v", samples[3])
	}
}

// The meta table is per-exposition, like the TYPE table: a reused parser (both
// the plain and the pooled path) must not stamp the previous scrape's HELP on
// this one's samples.
func TestHelpIsPerExposition(t *testing.T) {
	first := "# HELP a First.\na 1\n"
	second := "a 2\n"
	t.Run("New", func(t *testing.T) {
		p := New(Options{})
		var got []Sample
		for _, body := range []string{first, second} {
			got = got[:0]
			if _, err := p.Parse(strings.NewReader(body), func(s Sample) error { got = append(got, s); return nil }); err != nil {
				t.Fatal(err)
			}
		}
		if len(got) != 1 || got[0].Help != "" {
			t.Fatalf("second parse = %+v, want no help", got)
		}
	})
	t.Run("Pooled", func(t *testing.T) {
		p := Get(Options{})
		if _, err := p.Parse(strings.NewReader(first), func(Sample) error { return nil }); err != nil {
			t.Fatal(err)
		}
		Put(p)
		p = Get(Options{})
		defer Put(p)
		var got []Sample
		if _, err := p.Parse(strings.NewReader(second), func(s Sample) error { got = append(got, s); return nil }); err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].Help != "" {
			t.Fatalf("pooled reuse = %+v, want no help", got)
		}
	})
}

// A HELP or UNIT line that declares nothing is ignored, not counted malformed:
// it is a comment, and the parser must not desync on it.
func TestMalformedMetaCommentsIgnored(t *testing.T) {
	p := New(Options{OpenMetrics: true})
	var got []Sample
	malformed, err := p.Parse(strings.NewReader("# HELP\n# UNIT\n# UNIT a\n# UNIT a seconds trailing\na 1\n# EOF\n"),
		func(s Sample) error { got = append(got, s); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if malformed != 0 || len(got) != 1 || got[0].Value != 1 {
		t.Fatalf("malformed=%d samples=%+v", malformed, got)
	}
	if got[0].Unit != "" {
		t.Fatalf("unit = %q, want empty (the UNIT lines were malformed)", got[0].Unit)
	}
}

func TestParseHistogramAndSummaryRoles(t *testing.T) {
	samples := parseAll(t, `
# TYPE http_duration histogram
http_duration_bucket{le="0.1"} 100
http_duration_bucket{le="+Inf"} 150
http_duration_sum 53.4
http_duration_count 150
# TYPE rpc_latency summary
rpc_latency{quantile="0.99"} 3.2
rpc_latency_sum 8000
rpc_latency_count 2000
`)
	want := []struct {
		role   SampleRole
		family string
	}{
		{RoleHistogramBucket, "http_duration"},
		{RoleHistogramBucket, "http_duration"},
		{RoleHistogramSum, "http_duration"},
		{RoleHistogramCount, "http_duration"},
		{RoleSummaryQuantile, "rpc_latency"},
		{RoleSummarySum, "rpc_latency"},
		{RoleSummaryCount, "rpc_latency"},
	}
	if len(samples) != len(want) {
		t.Fatalf("got %d samples", len(samples))
	}
	for i, w := range want {
		if samples[i].Role != w.role || samples[i].Family != w.family {
			t.Errorf("sample %d (%s): role=%v family=%q, want role=%v family=%q",
				i, samples[i].Name, samples[i].Role, samples[i].Family, w.role, w.family)
		}
	}
}

func TestParseCounterTotalSuffixFamily(t *testing.T) {
	// OpenMetrics style: TYPE names the family, samples carry _total.
	samples := parseAll(t, "# TYPE requests counter\nrequests_total 5\n")
	if len(samples) != 1 || samples[0].Role != RoleCounter || samples[0].Family != "requests" {
		t.Fatalf("samples = %+v", samples)
	}
}

func TestParseEscapesAndSpecials(t *testing.T) {
	samples := parseAll(t, `metric{path="C:\\temp\\x",msg="say \"hi\"\n"} +Inf
nan_metric NaN
neg{} -1.5e3
`)
	if len(samples) != 3 {
		t.Fatalf("got %d samples", len(samples))
	}
	if samples[0].Labels[0].Value != `C:\temp\x` {
		t.Errorf("backslash unescape: %q", samples[0].Labels[0].Value)
	}
	if samples[0].Labels[1].Value != "say \"hi\"\n" {
		t.Errorf("quote/newline unescape: %q", samples[0].Labels[1].Value)
	}
	if !math.IsInf(samples[0].Value, 1) {
		t.Errorf("value = %v, want +Inf", samples[0].Value)
	}
	if !math.IsNaN(samples[1].Value) {
		t.Errorf("value = %v, want NaN", samples[1].Value)
	}
	if samples[2].Value != -1500 {
		t.Errorf("value = %v", samples[2].Value)
	}
}

func TestParseMalformed(t *testing.T) {
	p := New(Options{MaxLineBytes: 1 << 20})
	var good int
	malformed, err := p.Parse(strings.NewReader(`ok_metric 1
{no_name} 1
bad_value x
missing_value
trailing_garbage 1 123 junk
ok_metric2 2
`), func(s Sample) error { good++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if good != 2 || malformed != 4 {
		t.Fatalf("good=%d malformed=%d", good, malformed)
	}
}

func TestParseLongLineSkipped(t *testing.T) {
	p := New(Options{MaxLineBytes: 256})
	input := "short 1\nlong{x=\"" + strings.Repeat("a", 1000) + "\"} 2\nshort2 3\n"
	var names []string
	malformed, err := p.Parse(strings.NewReader(input), func(s Sample) error {
		names = append(names, s.Name)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if malformed != 1 || len(names) != 2 || names[0] != "short" || names[1] != "short2" {
		t.Fatalf("malformed=%d names=%v", malformed, names)
	}
}

func TestParseAbort(t *testing.T) {
	p := New(Options{MaxLineBytes: 1 << 20})
	count := 0
	_, err := p.Parse(strings.NewReader("a 1\nb 2\nc 3\n"), func(s Sample) error {
		count++
		if count == 2 {
			return ErrTooManySamples
		}
		return nil
	})
	if err != ErrTooManySamples || count != 2 {
		t.Fatalf("err=%v count=%d", err, count)
	}
}

func TestParseOpenMetrics(t *testing.T) {
	input := `# TYPE requests counter
requests_total{code="200"} 10 1700000000.5 # {trace_id="4bf92f3577b34da6a3ce929d0e0e4736",span_id="00f067aa0ba902b7",user="x"} 1.5 1700000000.25
requests_total{code="500"} 2
# TYPE lat histogram
lat_bucket{le="1"} 5 # {trace_id="4bf92f3577b34da6a3ce929d0e0e4736"} 0.7
lat_bucket{le="+Inf"} 6
lat_count 6
lat_sum 4.2
# EOF
this line must not be parsed
`
	samples := parseAllMode(t, input, true, true)
	if len(samples) != 6 {
		t.Fatalf("got %d samples: %+v", len(samples), samples)
	}

	s := samples[0]
	// OpenMetrics timestamps are float seconds.
	if s.TimestampMs != 1700000000500 {
		t.Errorf("timestamp = %d", s.TimestampMs)
	}
	if s.Exemplar == nil {
		t.Fatal("missing exemplar")
	}
	if s.Exemplar.Value != 1.5 || s.Exemplar.TimestampMs != 1700000000250 {
		t.Errorf("exemplar = %+v", s.Exemplar)
	}
	if len(s.Exemplar.Labels) != 3 || s.Exemplar.Labels[2] != (Label{"user", "x"}) {
		t.Errorf("exemplar labels = %+v", s.Exemplar.Labels)
	}
	if samples[1].Exemplar != nil {
		t.Error("sample without exemplar got one")
	}
	if samples[2].Role != RoleHistogramBucket || samples[2].Exemplar == nil {
		t.Errorf("bucket sample = %+v", samples[2])
	}
}

func TestParseOpenMetricsExemplarsDisabled(t *testing.T) {
	input := "# TYPE r counter\nr_total 1 # {trace_id=\"abc\"} 0.5\n# EOF\n"
	samples := parseAllMode(t, input, true, false)
	if len(samples) != 1 || samples[0].Exemplar != nil {
		t.Fatalf("samples = %+v", samples)
	}
}

// Exemplar labels must use their own positional cache: sharing lastKV with
// the sample labels would make every exemplar-bearing line evict the sample
// entries, permanently missing the sample-label fast path.
func TestParseExemplarLabelsSeparateCache(t *testing.T) {
	input := "# TYPE r counter\n" +
		`r_total{code="200"} 1 # {trace_id="4bf92f3577b34da6a3ce929d0e0e4736",user="x"} 0.5` + "\n" +
		`r_total{code="500"} 2 # {trace_id="00f067aa0ba902b74bf92f3577b34da6",user="y"} 0.7` + "\n" +
		"# EOF\n"
	p := New(Options{MaxLineBytes: 1 << 20, OpenMetrics: true, Exemplars: true})
	var got []Sample
	malformed, err := p.Parse(strings.NewReader(input), func(s Sample) error {
		c := s
		c.Labels = append([]Label(nil), s.Labels...)
		if s.Exemplar != nil {
			e := *s.Exemplar
			e.Labels = append([]Label(nil), s.Exemplar.Labels...)
			c.Exemplar = &e
		}
		got = append(got, c)
		return nil
	})
	if err != nil || malformed != 0 {
		t.Fatalf("err=%v malformed=%d", err, malformed)
	}
	if len(got) != 2 {
		t.Fatalf("got %d samples", len(got))
	}
	for i, want := range []string{"200", "500"} {
		if len(got[i].Labels) != 1 || got[i].Labels[0] != (Label{"code", want}) {
			t.Errorf("sample %d labels = %+v", i, got[i].Labels)
		}
		if got[i].Exemplar == nil || len(got[i].Exemplar.Labels) != 2 || got[i].Exemplar.Labels[1].Name != "user" {
			t.Errorf("sample %d exemplar = %+v", i, got[i].Exemplar)
		}
	}
	// White-box: the sample-label cache still holds the sample's label at
	// position 0, and the exemplar cache holds the exemplar's.
	if len(p.lastKV) == 0 || p.lastKV[0].name != "code" {
		t.Errorf("sample label cache = %+v, want position 0 name %q", p.lastKV, "code")
	}
	if len(p.exLastKV) == 0 || p.exLastKV[0].name != "trace_id" {
		t.Errorf("exemplar label cache = %+v, want position 0 name %q", p.exLastKV, "trace_id")
	}
}

func TestParseClassicRejectsExemplarSyntax(t *testing.T) {
	p := New(Options{MaxLineBytes: 1 << 20, Exemplars: true})
	var good int
	malformed, err := p.Parse(strings.NewReader("a 1 # {x=\"y\"} 2\nb 2\n"), func(s Sample) error {
		good++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if good != 1 || malformed != 1 {
		t.Fatalf("good=%d malformed=%d", good, malformed)
	}
}

func BenchmarkParseLargeScrape(b *testing.B) {
	var sb strings.Builder
	sb.WriteString("# TYPE bench_metric counter\n")
	for i := 0; i < 10_000; i++ {
		sb.WriteString("bench_metric_total{pod=\"pod-")
		sb.WriteString(strings.Repeat("x", 20))
		sb.WriteString("\",idx=\"")
		sb.WriteByte(byte('0' + i%10))
		sb.WriteString("\"} 12345.678\n")
	}
	input := sb.String()
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := Get(Options{MaxLineBytes: 1 << 20}) // the production path: pooled parser + reader
		n := 0
		if _, err := p.Parse(strings.NewReader(input), func(s Sample) error { n++; return nil }); err != nil {
			b.Fatal(err)
		}
		Put(p)
		if n != 10_000 {
			b.Fatalf("n=%d", n)
		}
	}
}

// TestZeroMaxLineBytesParsesNormally: 0 means "use the default", not "every
// line is too long". Read literally it would silently skip every line — the
// worst kind of default for a public API.
func TestZeroMaxLineBytesParsesNormally(t *testing.T) {
	body := "# TYPE reqs counter\nreqs{code=\"200\"} 42\n"
	for name, parse := range map[string]func(emit func(Sample) error) (int, error){
		"NewParser": func(emit func(Sample) error) (int, error) {
			return New(Options{}).Parse(strings.NewReader(body), emit)
		},
		"Get": func(emit func(Sample) error) (int, error) {
			p := Get(Options{})
			defer Put(p)
			return p.Parse(strings.NewReader(body), emit)
		},
	} {
		t.Run(name, func(t *testing.T) {
			var got []Sample
			malformed, err := parse(func(s Sample) error {
				got = append(got, s)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if malformed != 0 {
				t.Fatalf("malformed = %d, want 0", malformed)
			}
			if len(got) != 1 || got[0].Name != "reqs" || got[0].Value != 42 {
				t.Fatalf("samples = %+v, want one reqs=42", got)
			}
		})
	}
}

// A non-pooled parser reused across expositions must parse each one fully:
// the previous exposition's `# EOF` flag and TYPE classifications must not
// leak into the next (the pooled Get/Put path resets them; New()+Parse+Parse
// is equally legal API use and silently truncated instead).
func TestParserSequentialReuse(t *testing.T) {
	p := New(Options{OpenMetrics: true})
	count := func(body string) int {
		n := 0
		if _, err := p.Parse(strings.NewReader(body), func(Sample) error {
			n++
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if got := count("a 1\n# EOF\n"); got != 1 {
		t.Fatalf("first exposition: %d samples, want 1", got)
	}
	if got := count("b 1\nc 2\nd 3\n"); got != 3 {
		t.Fatalf("second exposition after # EOF: %d samples, want 3 (stale eof truncated)", got)
	}
	// Stale TYPE roles must not survive either: `a` was untyped above; now a
	// histogram — its bucket must classify, and vice versa on a third pass.
	if got := count("# TYPE a histogram\na_bucket{le=\"+Inf\"} 1\na_count 1\na_sum 1\n# EOF\n"); got != 3 {
		t.Fatalf("third exposition: %d samples, want 3", got)
	}
	if got := count("a_bucket{le=\"+Inf\"} 1\n"); got != 1 {
		t.Fatalf("fourth exposition: %d samples, want 1", got)
	}
}

// Prometheus 3 quoted UTF-8 names: the metric name as the first brace entry,
// quoted label names, quoted families in TYPE/HELP/UNIT — all resolving to
// the same unescaped strings so classification and metadata line up.
func TestQuotedUTF8Names(t *testing.T) {
	body := `# TYPE "my.requests" counter
# HELP "my.requests" Dotted requests.
{"my.requests",code="200","status class"="2xx"} 5
# TYPE "lat.seconds" histogram
{"lat.seconds_bucket",le="1"} 3
{"lat.seconds_bucket",le="+Inf"} 4
{"lat.seconds_sum"} 2.5
{"lat.seconds_count"} 4
plain{"dotted.label"="v"} 1
`
	var got []Sample
	p := New(Options{})
	malformed, err := p.Parse(strings.NewReader(body), func(s Sample) error {
		s.Labels = append([]Label(nil), s.Labels...)
		got = append(got, s)
		return nil
	})
	if err != nil || malformed != 0 {
		t.Fatalf("err=%v malformed=%d", err, malformed)
	}
	if len(got) != 6 {
		t.Fatalf("samples = %d, want 6", len(got))
	}
	c := got[0]
	if c.Name != "my.requests" || c.Role != RoleCounter || c.Help != "Dotted requests." {
		t.Fatalf("counter = %+v", c)
	}
	if len(c.Labels) != 2 || c.Labels[0] != (Label{"code", "200"}) || c.Labels[1] != (Label{"status class", "2xx"}) {
		t.Fatalf("counter labels = %v", c.Labels)
	}
	if b := got[1]; b.Role != RoleHistogramBucket || b.Family != "lat.seconds" {
		t.Fatalf("bucket = %+v", b)
	}
	if s := got[3]; s.Role != RoleHistogramSum || s.Family != "lat.seconds" {
		t.Fatalf("sum = %+v", s)
	}
	if pl := got[5]; pl.Name != "plain" || len(pl.Labels) != 1 || pl.Labels[0] != (Label{"dotted.label", "v"}) {
		t.Fatalf("plain-with-quoted-label = %+v", pl)
	}
}

// Escapes inside quoted names unescape to the real name; degenerate quoted
// forms stay malformed.
func TestQuotedNameEdgeCases(t *testing.T) {
	var names []string
	body := "{\"esc\\\"aped\"} 1\n" + // name with an escaped quote
		"{} 1\n" + // no name at all
		"{\"\"} 1\n" + // empty name
		"{\"lbl\"=\"v\"} 1\n" + // quoted first entry is a LABEL: still nameless
		"{\"broken} 1\n" // unterminated quote
	p := New(Options{})
	malformed, err := p.Parse(strings.NewReader(body), func(s Sample) error {
		names = append(names, s.Name)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != `esc"aped` {
		t.Fatalf("names = %q", names)
	}
	if malformed != 4 {
		t.Fatalf("malformed = %d, want 4", malformed)
	}
}

// An over-long line must be consumed and counted malformed in its ENTIRETY.
// The tail of such a line is not a line: emitting it invents a series that
// does not appear in the exposition — a silent data-integrity break for any
// caller of this package that sets a small MaxLineBytes, not merely a dropped
// sample. The sizing matters: the physical line has to end with a SHORT final
// chunk (total = whole read buffers + a remainder under MaxLineBytes), because
// nothing is appended to the scratch once the budget is blown, so that
// remainder looked like a complete, in-budget line of its own.
func TestOverLongLineDoesNotEmitItsTail(t *testing.T) {
	const evil = `evil_metric{b="c"} 99` + "\n"
	for _, tail := range []int{len(evil), len(evil) + 8, 99} {
		body := strings.Repeat("x", parseBufSize*2) +
			strings.Repeat("y", tail-len(evil)) + evil
		p := New(Options{MaxLineBytes: 100})
		var samples []Sample
		malformed, err := p.Parse(strings.NewReader(body), func(s Sample) error {
			samples = append(samples, s)
			return nil
		})
		if err != nil {
			t.Fatalf("tail=%d: parse: %v", tail, err)
		}
		for _, s := range samples {
			if s.Name == "evil_metric" {
				t.Errorf("tail=%d: emitted %q from the tail of an over-long line", tail, s.Name)
			}
		}
		if malformed == 0 {
			t.Errorf("tail=%d: over-long line not counted malformed", tail)
		}
	}
}
