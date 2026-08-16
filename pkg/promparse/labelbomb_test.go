package promparse

import (
	"strings"
	"testing"
	"time"
)

// A single line with a pathological number of labels must be dropped as
// malformed rather than run the O(n²) dedupe scan to completion (which the
// scrape timeout cannot interrupt). Regression for maxLabelsPerSample.
func TestLabelBombIsDroppedAndFast(t *testing.T) {
	var b strings.Builder
	b.WriteString("m{")
	const n = 200_000 // ~well past maxLabelsPerSample, ~1 MiB
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString("l")
		b.WriteString(itoa(i))
		b.WriteString(`=""`)
	}
	b.WriteString("} 1\n")
	line := b.String()

	p := New(Options{MaxLineBytes: len(line) + 16})
	emitted := 0
	start := time.Now()
	_, err := p.Parse(strings.NewReader(line), func(Sample) error { emitted++; return nil })
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("parse errored: %v", err)
	}
	if emitted != 0 {
		t.Errorf("a label-bomb line emitted %d samples; want 0 (dropped as malformed)", emitted)
	}
	// With the cap the scan is bounded to ~maxLabelsPerSample²; without it this
	// line takes tens of seconds. A generous ceiling catches the quadratic.
	if elapsed > 3*time.Second {
		t.Errorf("parsing a label bomb took %v; the per-sample label cap is not bounding the quadratic scan", elapsed)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [12]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
