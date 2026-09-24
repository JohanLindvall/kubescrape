package bearer

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// A refresh read that never returns must not accumulate, and must not take its
// callers with it. The claim is a FLAG, not a deadline: a deadline EXPIRES
// while the read is still blocked, so every interval would start another
// blocking read, and each one parks a goroutine in a file syscall — which pins
// an OS thread (regular files are not pollable), walking the process into the
// runtime's fatal 10000-thread limit in hours. And the caller that CLAIMS the
// read waits for it at most refreshWait: on the trace tier's internal hop that
// caller used to be grpc-go's InTapHandle, run with the transport's mutex held,
// so a claimer parked in the read froze the sibling's whole connection.
//
// The block is a FIFO with no writer, which makes open(2) itself hang — the
// same shape as the wedged CSI/NFS projection the lock-drop exists for, and it
// needs no test seam in the production path. wedge's cleanup opens the FIFO for
// writing, which releases the parked read so it cannot leak into later tests.

// wedge makes a FIFO whose open(2) blocks until the test ends.
func wedge(t *testing.T) string {
	t.Helper()
	fifo := filepath.Join(t.TempDir(), "wedged")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Skipf("mkfifo unsupported here: %v", err)
	}
	t.Cleanup(func() {
		// A reader parked in open(O_RDONLY) is released by a writer's open.
		// Hand it a token rather than EOF: a FAILED read would move
		// kubescrape_bearer_token_read_errors_total from a goroutine that
		// outlives this test, under whichever test runs next and asserts a
		// delta on it. Then wait for it to leave readFile, so the next test's
		// readsInFlight baseline is not moved by this one's straggler.
		if f, err := os.OpenFile(fifo, os.O_WRONLY|syscall.O_NONBLOCK, 0); err == nil {
			_, _ = f.WriteString("first")
			_ = f.Close()
		}
		for deadline := time.Now().Add(5 * time.Second); readsInFlight() > 0 && time.Now().Before(deadline); {
			time.Sleep(time.Millisecond)
		}
	})
	return fifo
}

// readsInFlight counts goroutines currently inside this package's readFile —
// the black-box answer to "how many reads is the wedge pinning?".
func readsInFlight() int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	count := 0
	for g := range bytes.SplitSeq(buf, []byte("\n\n")) {
		if bytes.Contains(g, []byte("internal/bearer.readFile(")) {
			count++
		}
	}
	return count
}

// syncBuffer is a log destination safe for the stall report's own goroutine to
// write while the test reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// hammer issues rounds x per concurrent calls of call, advancing the clock past
// any interval before each round, and waits for them all to RETURN — which is
// itself the assertion that no caller is parked with the read.
func hammer(t *testing.T, advance func(time.Duration), rounds, per int, call func()) {
	t.Helper()
	var returned atomic.Int64
	var wg sync.WaitGroup
	for range rounds {
		advance(2 * DefaultReadInterval) // far past both cadences
		for range per {
			wg.Go(func() {
				call()
				returned.Add(1)
			})
		}
		time.Sleep(5 * time.Millisecond)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("only %d of %d callers returned: a caller is parked in the token read "+
			"(a wedged mount must cost a caller at most refreshWait, and the read must run off its goroutine)",
			returned.Load(), rounds*per)
	}
}

func TestOnlyOneRefreshReadIsEverInFlight(t *testing.T) {
	dir := t.TempDir()
	real := filepath.Join(dir, "real")
	if err := os.WriteFile(real, []byte("first"), 0o600); err != nil {
		t.Fatal(err)
	}
	fifo := wedge(t)
	c := newClock()
	logs := &syncBuffer{}

	// Construct against the readable file so the initial fatal read succeeds,
	// then point it at the FIFO so every REFRESH wedges.
	r, err := NewRotating(real, slog.New(slog.NewTextHandler(logs, nil)), WithClock(c.now))
	if err != nil {
		t.Fatal(err)
	}
	r.path = fifo
	before := readsInFlight()

	// Many callers across many EXPIRED refresh windows. With a deadline claim
	// each expired window admits another blocking read; with the flag exactly
	// one is ever in flight. EVERY caller returns — the claimer included — and
	// every one gets the last good set.
	hammer(t, c.advance, 20, 5, func() {
		if got := r.Tokens(); len(got) == 0 || got[0] != "first" {
			t.Errorf("Tokens() = %v while a refresh is wedged; want the last good value", got)
		}
	})
	if n := readsInFlight() - before; n != 1 {
		t.Errorf("%d token reads are parked on the wedged mount; want exactly 1 "+
			"(a wedged mount must pin ONE thread, not one per refresh interval)", n)
	}
	// The wedge is not silent: the claimer that stopped waiting says so, once.
	waitForLog(t, logs, "has not returned")
	if n := strings.Count(logs.String(), "has not returned"); n != 1 {
		t.Errorf("the stall was reported %d times, want once per wedge:\n%s", n, logs.String())
	}
}

// The client half had no claim at all: every caller that found the cache stale
// ran its own read, so on a wedged mount EVERY export and kubelet scrape parked
// in open(2) — one pinned OS thread each — while the last good token sat in
// f.token. Measured before the fix: 20 of 20 concurrent callers stuck.
func TestFileOnlyOneReadIsEverInFlight(t *testing.T) {
	dir := t.TempDir()
	real := filepath.Join(dir, "real")
	if err := os.WriteFile(real, []byte("first"), 0o600); err != nil {
		t.Fatal(err)
	}
	fifo := wedge(t)
	c := newClock()
	logs := &syncBuffer{}
	f := NewFile(real, slog.New(slog.NewTextHandler(logs, nil)), WithClock(c.now))
	if _, err := f.Read(); err != nil {
		t.Fatal(err)
	}
	f.path = fifo
	before := readsInFlight()

	hammer(t, c.advance, 20, 5, func() {
		if got, err := f.Token(); err != nil || got != "first" {
			t.Errorf("Token() = %q, %v while a re-read is wedged; want the last good token", got, err)
		}
	})
	if n := readsInFlight() - before; n != 1 {
		t.Errorf("%d token reads are parked on the wedged mount; want exactly 1", n)
	}
	waitForLog(t, logs, "has not returned")
}

// With NOTHING ever read there is no last good token to hand out. The claimer
// gives up after refreshWait and every other caller waits for the in-flight
// read no longer than that either — on a channel, never in the syscall — and
// then answers with an error, which is the honest outcome; still only one
// read is in flight.
func TestFileWithNothingReadDoesNotParkItsCallers(t *testing.T) {
	fifo := wedge(t)
	c := newClock()
	f := NewFile(fifo, discard(), WithClock(c.now))
	before := readsInFlight()

	hammer(t, c.advance, 4, 5, func() {
		if got, err := f.Token(); err == nil || got != "" {
			t.Errorf("Token() = %q, %v with nothing ever read and the read wedged; want an error", got, err)
		}
	})
	if n := readsInFlight() - before; n != 1 {
		t.Errorf("%d token reads are parked on the wedged mount; want exactly 1", n)
	}
}

// unwedge releases the read parked in wedge's FIFO, handing it content (empty
// content makes the read fail as an empty token file).
func unwedge(t *testing.T, fifo, content string) {
	t.Helper()
	w, err := os.OpenFile(fifo, os.O_WRONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		t.Fatalf("releasing the parked read: %v", err)
	}
	_, _ = w.WriteString(content)
	_ = w.Close()
}

type tokenResult struct {
	tok string
	err error
}

// firstUseRace forces the interleaving of two concurrent FIRST uses of f: the
// claimer's read is parked on the FIFO, a second caller enters Token while it
// is, and only then is the read released with content. It returns what each
// caller got. f.set.wait is raised so the release lands inside the bounded
// wait however loaded the machine is.
func firstUseRace(t *testing.T, f *File, fifo, content string) (claimer, second tokenResult) {
	t.Helper()
	f.set.wait = 10 * time.Second
	before := readsInFlight()
	claimerC := make(chan tokenResult, 1)
	go func() { tok, err := f.Token(); claimerC <- tokenResult{tok, err} }()
	for deadline := time.Now().Add(5 * time.Second); readsInFlight()-before < 1; {
		if time.Now().After(deadline) {
			t.Fatal("the claimer's read never started")
		}
		time.Sleep(time.Millisecond)
	}
	secondC := make(chan tokenResult, 1)
	go func() { tok, err := f.Token(); secondC <- tokenResult{tok, err} }()
	select {
	case second = <-secondC:
		// Answered while the read was provably still parked.
		unwedge(t, fifo, content)
	case <-time.After(100 * time.Millisecond):
		unwedge(t, fifo, content)
		second = <-secondC
	}
	return <-claimerC, second
}

// Concurrent FIRST uses on a HEALTHY mount all get the token. A caller that
// found the first read already in flight answered errReadPending at once,
// without waiting the microseconds the read takes: promscrape spawns the
// cadvisor, /metrics and summary scrapes back to back, so the first cycle on
// every node failed all but one of them — and spent the once-per-process
// kubelet-token warn on a false alarm that then hid any real failure.
func TestFileConcurrentFirstUsesAllGetTheToken(t *testing.T) {
	fifo := wedge(t)
	f := NewFile(fifo, discard())
	claimer, second := firstUseRace(t, f, fifo, "tok")
	if claimer.err != nil || claimer.tok != "tok" {
		t.Errorf("claimer: Token() = %q, %v; want the token its read found", claimer.tok, claimer.err)
	}
	if second.err != nil || second.tok != "tok" {
		t.Errorf("concurrent first use: Token() = %q, %v; want the token the in-flight read found "+
			"(a caller with nothing to present must wait for that read, boundedly, rather than fail)", second.tok, second.err)
	}
}

// A caller that waited for a first read which FAILED reports that read's own
// error, not "the read has not returned" — by then it has.
func TestFileWaiterForAFailedFirstReadGetsItsError(t *testing.T) {
	fifo := wedge(t)
	f := NewFile(fifo, discard())
	claimer, second := firstUseRace(t, f, fifo, "")
	if claimer.err == nil {
		t.Fatalf("claimer: Token() = %q, nil; want the empty-file error", claimer.tok)
	}
	if second.err == nil || errors.Is(second.err, errReadPending) || !strings.Contains(second.err.Error(), "is empty") {
		t.Errorf("waiter: Token() = %q, %v; want the failed read's own error", second.tok, second.err)
	}
}

// The bound must not cost the healthy path its synchronous pickup: the call
// that TRIGGERS a due re-read still returns what that read found. This is what
// the claim-and-bounded-wait shape exists to keep (an async refresh answering
// every claimer from the old value would 401 a client that rotated first).
func TestClaimerStillGetsTheValueItsReadFound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	write(t, path, "first")
	c := newClock()
	f := NewFile(path, discard(), WithClock(c.now))
	if got, err := f.Token(); err != nil || got != "first" {
		t.Fatalf("Token = %q, %v", got, err)
	}
	r, err := NewRotating(path, discard(), WithClock(c.now))
	if err != nil {
		t.Fatal(err)
	}
	write(t, path, "second")
	c.advance(2 * DefaultReadInterval)
	if got, err := f.Token(); err != nil || got != "second" {
		t.Errorf("File.Token = %q, %v; the triggering call must present what its read found", got, err)
	}
	if got := r.Tokens(); len(got) != 2 || got[0] != "second" {
		t.Errorf("Rotating.Tokens = %v; the triggering call must accept what its read found", got)
	}
}

// waitForLog polls for a record written by a goroutine this test does not
// join (the stall report runs off the claimer's goroutine by design).
func waitForLog(t *testing.T, logs *syncBuffer, needle string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(logs.String(), needle) {
		if time.Now().After(deadline) {
			t.Fatalf("no %q record; got:\n%s", needle, logs.String())
		}
		time.Sleep(time.Millisecond)
	}
}
