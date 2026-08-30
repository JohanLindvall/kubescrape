package bearer

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

// Tokens() is on the read path of EVERY request to the two authenticated
// receivers in this repo (/v1/scrape-auth on the metadata service, the trace
// tier's internal shard hop), so its steady state — no re-read due — is worth
// reporting. It is one allocation: the returned accept set.
func BenchmarkTokensSteadyState(b *testing.B) {
	r := benchRotating(b)
	b.ReportAllocs()
	for b.Loop() {
		_ = r.Tokens()
	}
}

// The shape that matters is the CONCURRENT one: what this type owes its
// callers is that one slow or wedged read cannot serialise the others, which
// is why neither the file read nor the reporting of its outcome happens under
// r.mu (logstall_test.go is the assertion, this is the bill).
func BenchmarkTokensConcurrent(b *testing.B) {
	r := benchRotating(b)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = r.Tokens()
		}
	})
}

// A benchmark cannot fail a build, so the budget is a TEST as well.
func TestTokensAllocationBudget(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector changes escape analysis; an allocation ceiling means nothing under it")
	}
	r := benchRotating(t)
	// One: the returned accept set. A second one means per-call work crept
	// back onto a path every authenticated request takes.
	if got := testing.AllocsPerRun(200, func() { _ = r.Tokens() }); got > 1 {
		t.Errorf("Tokens() allocates %v per call, want at most 1", got)
	}
}

func benchRotating(tb testing.TB) *Rotating {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "token")
	if err := os.WriteFile(path, []byte("a-token-value"), 0o600); err != nil {
		tb.Fatal(err)
	}
	// A frozen clock keeps every call in the steady state: the re-read is the
	// ticker's job (Run), not the request's.
	r, err := NewRotating(path, nil, WithClock(newClock().now))
	if err != nil {
		tb.Fatal(err)
	}
	return r
}
