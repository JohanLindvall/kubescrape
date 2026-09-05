package tailer

import (
	"context"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// TestSweepIsTheHeartbeat pins that every completed sweep moves
// kubescrape_log_sweeps_total, files or no files: the counter exists to say the
// sweep goroutine is alive when nothing else in the tailer moves.
func TestSweepIsTheHeartbeat(t *testing.T) {
	tl := driveTailer(t.TempDir(), &fakeExporter{})
	before := obs.LogSweeps.Value()
	tl.sweep(context.Background(), true)
	tl.sweep(context.Background(), false)
	if got := obs.LogSweeps.Value() - before; got != 2 {
		t.Fatalf("kubescrape_log_sweeps_total moved by %v for two sweeps of an empty directory, want 2", got)
	}
}
