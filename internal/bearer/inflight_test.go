package bearer

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// A refresh read that never returns must not accumulate. The claim is a FLAG,
// not a deadline: a deadline EXPIRES while the read is still blocked, so every
// interval would start another blocking read, and each one parks a goroutine in
// a file syscall — which pins an OS thread (regular files are not pollable),
// walking the process into the runtime's fatal 10000-thread limit in hours.
// Holding the lock across the read bounded this at one; the flag has to too.
//
// The block is a FIFO with no writer, which makes open(2) itself hang — the
// same shape as the wedged CSI/NFS projection the lock-drop exists for, and it
// needs no test seam in the production path.
func TestOnlyOneRefreshReadIsEverInFlight(t *testing.T) {
	dir := t.TempDir()
	real := filepath.Join(dir, "real")
	if err := os.WriteFile(real, []byte("first"), 0o600); err != nil {
		t.Fatal(err)
	}
	fifo := filepath.Join(dir, "wedged")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Skipf("mkfifo unsupported here: %v", err)
	}

	now := time.Now()
	var mu sync.Mutex
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	advance := func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() }

	// Construct against the readable file so the initial fatal read succeeds,
	// then point it at the FIFO so every REFRESH wedges.
	r, err := NewRotating(real, nil, WithClock(clock))
	if err != nil {
		t.Fatal(err)
	}
	r.path = fifo

	var blocked, returned atomic.Int64
	var wg sync.WaitGroup
	// Many callers across many EXPIRED refresh windows. With a deadline claim
	// each expired window admits another blocking read; with the flag exactly
	// one is ever in flight.
	for round := 0; round < 20; round++ {
		advance(10 * time.Second) // far past any refresh interval
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				blocked.Add(1)
				_ = r.Tokens()
				returned.Add(1)
				blocked.Add(-1)
			}()
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)

	stuck := blocked.Load()
	t.Logf("callers issued=100 returned=%d stuck-in-read=%d", returned.Load(), stuck)
	if stuck > 1 {
		t.Errorf("%d callers are blocked in the token read; want at most 1 "+
			"(a wedged mount must pin ONE thread, not one per refresh interval)", stuck)
	}
	// The wedged refresh must not break the answer: everyone else keeps getting
	// the last good token.
	if got := r.Tokens(); len(got) == 0 || got[0] != "first" {
		t.Errorf("Tokens() = %v while a refresh is wedged; want the last good value", got)
	}
	// Leave the FIFO reader parked; the test binary exits with it.
}
