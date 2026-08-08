package promparse

import (
	"strings"
	"testing"
)

// The bound is on the LINE the parser returns, which excludes the terminator:
// a line of exactly MaxLineBytes content bytes is not "longer than" the limit
// and must parse. It used to be measured on the ReadSlice chunk, so the '\n'
// counted — and the identical bytes were then accepted or dropped depending on
// whether they happened to be the final unterminated line of the body, which is
// a property of the exposition's last byte and not of the line at all.
func TestMaxLineBytesMeasuresTheLineNotItsTerminator(t *testing.T) {
	const limit = 32
	exact := "m" + strings.Repeat("x", limit-3) + " 1" // exactly limit bytes
	if len(exact) != limit {
		t.Fatalf("line is %d bytes, want %d", len(exact), limit)
	}
	over := exact + "y" // one byte past the limit

	for _, tc := range []struct {
		name  string
		body  string
		want  int // samples before "after"
		wantM int
	}{
		{"exact, terminated", exact + "\nafter 7\n", 1, 0},
		{"exact, unterminated final line", exact, 1, 0},
		{"exact, CRLF-terminated", exact + "\r\nafter 7\r\n", 1, 0},
		{"one over, terminated", over + "\nafter 7\n", 0, 1},
		{"one over, unterminated final line", over, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := New(Options{MaxLineBytes: limit})
			got, malformed, err := collect(t, p, tc.body)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			n := 0
			for _, s := range got {
				if s.Name != "after" {
					n++
				}
			}
			if n != tc.want || malformed != tc.wantM {
				t.Errorf("got %d in-budget samples (malformed=%d), want %d (malformed=%d)", n, malformed, tc.want, tc.wantM)
			}
			// The line after a skipped one must still parse, whichever verdict.
			if strings.Contains(tc.body, "after") && (len(got) == 0 || got[len(got)-1].Name != "after") {
				t.Errorf("DESYNC: the line after was lost (got %+v)", got)
			}
		})
	}
}

// The same boundary through the SPILL path: a line longer than bufio's buffer
// arrives in several chunks, and the terminator rides the last one — so the
// accounting there has to exclude it too, or the two paths disagree about the
// same line.
func TestMaxLineBytesBoundaryAcrossBufioRefills(t *testing.T) {
	// bufio's default reader is 4096 bytes; a limit well past it forces refills.
	const limit = 10_000
	exact := "m" + strings.Repeat("x", limit-3) + " 1"
	if len(exact) != limit {
		t.Fatalf("line is %d bytes, want %d", len(exact), limit)
	}
	for _, tc := range []struct {
		name string
		body string
		want int
	}{
		{"exact", exact + "\nafter 7\n", 2},
		{"one over", exact + "y\nafter 7\n", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := New(Options{MaxLineBytes: limit})
			got, _, err := collect(t, p, tc.body)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if len(got) != tc.want {
				t.Errorf("got %d samples, want %d", len(got), tc.want)
			}
		})
	}
}
