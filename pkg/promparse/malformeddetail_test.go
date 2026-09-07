package promparse

import (
	"io"
	"strconv"
	"strings"
	"testing"
)

// The four attributed reasons exist so an operator can tell a bound of OURS
// (an over-long line) from a target bug (a repeated label name) from a cut
// connection — the total alone reads the same for all three.
func TestMalformedDetailAttributesEachCause(t *testing.T) {
	t.Parallel()
	body := "dup{a=\"1\",a=\"2\"} 1\n" + // duplicate label name
		"toolong{x=\"" + strings.Repeat("y", 200) + "\"} 1\n" + // over the bound below
		"good 3\n"
	p := New(Options{MaxLineBytes: 100})
	var got int
	malformed, err := p.Parse(strings.NewReader(body), func(Sample) error { got++; return nil })
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got != 1 {
		t.Fatalf("samples = %d, want 1 (only `good`)", got)
	}
	if malformed != 2 {
		t.Fatalf("malformed = %d, want 2", malformed)
	}
	d := p.MalformedDetail()
	if d.DuplicateLabels != 1 || d.OverLongLines != 1 {
		t.Fatalf("detail = %+v, want one duplicate and one over-long", d)
	}
	if d.TruncatedLines != 0 || d.TooManyLabels != 0 {
		t.Fatalf("detail = %+v, want no truncation and no label-cap drops", d)
	}
	if !d.Any() {
		t.Fatal("Any() = false with two attributed reasons")
	}
}

// A body that ends mid-line is a CUT SCRAPE, which is why it is attributed
// apart from a line that merely failed to parse.
func TestMalformedDetailCountsTruncatedBody(t *testing.T) {
	t.Parallel()
	r := io.MultiReader(strings.NewReader("a 1\nb 2"), errReader{})
	p := New(Options{})
	if _, err := p.Parse(r, func(Sample) error { return nil }); err == nil {
		t.Fatal("err = nil, want the read error")
	}
	if d := p.MalformedDetail(); d.TruncatedLines != 1 || d.OverLongLines != 0 {
		t.Fatalf("detail = %+v, want exactly one truncated line", d)
	}
}

// The counts are PER PARSE: a pooled parser must not charge one target's
// broken exposition to the next scrape that borrows it.
func TestMalformedDetailResetsPerParse(t *testing.T) {
	t.Parallel()
	p := New(Options{})
	if _, err := p.Parse(strings.NewReader("dup{a=\"1\",a=\"2\"} 1\n"), func(Sample) error { return nil }); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if d := p.MalformedDetail(); d.DuplicateLabels != 1 {
		t.Fatalf("detail = %+v, want one duplicate", d)
	}
	if _, err := p.Parse(strings.NewReader("clean 1\n"), func(Sample) error { return nil }); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if d := p.MalformedDetail(); d.Any() {
		t.Fatalf("detail = %+v, want zeroed after a clean parse", d)
	}
}

// MalformedDetail describes sample LINES DROPPED — its doc says so and the
// contract on MalformedDetail() says its sum never exceeds the malformed count.
// parseLabels is also the EXEMPLAR label parser, and a broken exemplar drops
// nothing: finishSample still emits its sample and the fault is already counted
// on MalformedExemplars. Attributing it here made the sum exceed the total (0
// malformed, 1 duplicate) and, in the agent, made one genuinely bad line report
// `malformed=1 duplicateLabels=5000` for a scrape that lost exactly one line.
func TestExemplarLabelFailuresAreNotAttributedToDroppedSamples(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		body string
	}{
		{"duplicate name", "c_total 1 # {a=\"1\",a=\"2\"} 5\n# EOF\n"},
		{"over the label cap", "c_total 1 # {" + manyExemplarLabels(MaxLabelsPerSample+5) + "} 5\n# EOF\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := New(Options{OpenMetrics: true, Exemplars: true, MaxLineBytes: 8 << 20})
			var got int
			malformed, err := p.Parse(strings.NewReader(tc.body), func(Sample) error { got++; return nil })
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got != 1 || malformed != 0 {
				t.Fatalf("samples = %d malformed = %d, want 1 and 0 — the sample survives its exemplar", got, malformed)
			}
			if n := p.MalformedExemplars(); n != 1 {
				t.Fatalf("MalformedExemplars = %d, want 1 — the fault has its own counter", n)
			}
			d := p.MalformedDetail()
			if d.Any() {
				t.Fatalf("detail = %+v, want empty: nothing was dropped, so nothing may be attributed", d)
			}
			if sum := d.DuplicateLabels + d.TooManyLabels + d.OverLongLines + d.TruncatedLines; sum > malformed {
				t.Fatalf("detail sum %d exceeds malformed %d — the documented invariant", sum, malformed)
			}
		})
	}
}

// And the SAMPLE label block must still be attributed, or the fix above would
// have bought the invariant by blinding the detail entirely.
func TestSampleLabelFailuresAreStillAttributed(t *testing.T) {
	t.Parallel()
	body := "dup{a=\"1\",a=\"2\"} 1\n" +
		"wide{" + manyExemplarLabels(MaxLabelsPerSample+5) + "} 1\n"
	p := New(Options{MaxLineBytes: 8 << 20})
	malformed, err := p.Parse(strings.NewReader(body), func(Sample) error { return nil })
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if malformed != 2 {
		t.Fatalf("malformed = %d, want 2", malformed)
	}
	d := p.MalformedDetail()
	if d.DuplicateLabels != 1 || d.TooManyLabels != 1 {
		t.Fatalf("detail = %+v, want one duplicate and one label-cap drop", d)
	}
}

func manyExemplarLabels(n int) string {
	var sb strings.Builder
	for i := range n {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("l")
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString(`="v"`)
	}
	return sb.String()
}
