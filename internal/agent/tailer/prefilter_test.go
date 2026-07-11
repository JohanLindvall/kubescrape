package tailer

import (
	"testing"

	"github.com/JohanLindvall/multiline/patterns"
)

// The multiline package's default matcher prefilters its start-state regexes
// with literals derived from the patterns (>10x per-line CPU; see
// BenchmarkIngestLine). If a future pattern change makes the literal set
// unprovable the matcher silently falls back to full regex evaluation —
// still correct, but the per-line budget regresses. This is the alarm.
func TestPrefilterEnabled(t *testing.T) {
	lits := patterns.MustCompile(patterns.All...).StartLiterals()
	if len(lits) == 0 {
		t.Fatal("the compiled matcher has no start literals; the prefilter is disabled and per-line CPU regresses ~12x")
	}
	t.Logf("start literals: %q", lits)
}
