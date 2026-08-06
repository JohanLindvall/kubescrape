package journald

import (
	"errors"
	"strings"
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

// go-systemd's Wait returns a bare -1 when the dlsym lookup for
// sd_journal_wait fails — its own sentinel, not an errno. Read errno-style it
// renders as syscall.Errno(1) = EPERM, "operation not permitted", which sends
// an operator after a permissions problem while the reader reopens every 30s
// forever against a symbol that will never appear.
func TestSymbolLookupFailureIsNotReportedAsEPERM(t *testing.T) {
	err := waitStatusErr(-1)
	if err == nil {
		t.Fatal("a failed symbol lookup must still surface as an error")
	}
	if errors.Is(err, syscall.EPERM) {
		t.Fatalf("err = %v, want the symbol lookup named rather than a fabricated EPERM", err)
	}
	if !strings.Contains(err.Error(), "sd_journal_wait") {
		t.Fatalf("err = %v, want the missing symbol named", err)
	}
}
