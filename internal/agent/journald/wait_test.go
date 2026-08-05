package journald

import (
	"errors"
	"syscall"
	"testing"
)

// sd_journal_wait failures return a negative errno IMMEDIATELY — the call
// only blocks on success — so a swallowed status left the caught-up loop
// (Next()==0, Wait failing instantly, realistically inotify watch/fd
// exhaustion) spinning at full CPU with nothing surfaced and Run's
// close/reopen/backoff never engaging. next must classify a negative status
// as a source error.
func TestWaitFailureSurfacesAsSourceError(t *testing.T) {
	// Non-negative statuses (SD_JOURNAL_NOP/APPEND/INVALIDATE) mean the wait
	// blocked and returned; not errors.
	for _, status := range []int{0, 1, 2} {
		if err := waitStatusErr(status); err != nil {
			t.Fatalf("waitStatusErr(%d) = %v, want nil", status, err)
		}
	}
	err := waitStatusErr(-int(syscall.EMFILE))
	if err == nil {
		t.Fatal("a negative wait status must surface as an error, or the caught-up loop spins at full CPU")
	}
	if !errors.Is(err, syscall.EMFILE) {
		t.Fatalf("err = %v, want the errno preserved for errors.Is", err)
	}
}
