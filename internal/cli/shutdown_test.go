package cli

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// A path that cancels the process context before its shutdown is done (the
// agent's fatal pipeline failure; the metadata service's normal path, which
// cancels to join its exporters before the final export) must leave SIGTERM
// HANDLED. stop used to be signal.NotifyContext's, which also calls
// signal.Stop — so SIGTERM fell back to Go's default action for the rest of
// that shutdown, and one arriving then (a liveness kill, a rollout) killed the
// process mid-drain with exit 143.
//
// A regression KILLS this test binary (the default action), which go test
// reports as a failure of the whole package run — loud, which is the point.
func TestSIGTERMStaysHandledAfterStop(t *testing.T) {
	ctx, stop, releaseSignals := ShutdownContext()
	defer releaseSignals()
	stop() // what pipelines.fatal, and the service's normal path, do
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("stop did not cancel the process context")
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	// Still running a moment later: the signal was caught, not acted on.
	time.Sleep(100 * time.Millisecond)
}

// Both mains take their lifetime from ShutdownContext. The metadata service
// kept a bare signal.NotifyContext after the agent's was fixed — the same
// defect, reached on its NORMAL shutdown path rather than a fatal one — because
// the fix lived in one main and the other had its own copy.
func TestMainsTakeTheirLifetimeFromShutdownContext(t *testing.T) {
	mains, err := filepath.Glob("../../cmd/*/main.go")
	if err != nil {
		t.Fatal(err)
	}
	if len(mains) < 2 {
		t.Fatalf("found %d mains under cmd/, want both binaries'; the check would pass vacuously", len(mains))
	}
	for _, path := range mains {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		src := string(b)
		if strings.Contains(src, "signal.NotifyContext(") {
			t.Errorf("%s calls signal.NotifyContext itself: its stop also calls signal.Stop, so a SIGTERM "+
				"during the shutdown that follows kills the process — use cli.ShutdownContext", path)
		}
		if !strings.Contains(src, "cli.ShutdownContext()") {
			t.Errorf("%s does not take its lifetime from cli.ShutdownContext", path)
		}
	}
}
