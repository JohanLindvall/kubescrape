package bearer

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// clock is the injectable test clock: no sleeping to cross a read interval or
// a grace window.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)} }

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func discard() *slog.Logger { return slog.New(slog.DiscardHandler) }

func write(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// THE DRIFT this package exists to resolve: a token file that fails to re-read
// mid-rotation must keep serving the last good value. Two of the five previous
// implementations did (the agent's client-side cache, the metadata service's
// accept set) and two did not — otlpexport.Client.bearer returned an error that
// FAILED THE WHOLE EXPORT, and promscrape's kubelet path had no cache at all
// and failed the scrape. The same survivable Secret-swap window was survivable
// in one place and fatal in two.
func TestFileKeepsLastGoodTokenAcrossAFailedReread(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	write(t, path, "  first\n")
	c := newClock()
	f := NewFile(path, discard(), WithClock(c.now))

	got, err := f.Token()
	if err != nil || got != "first" {
		t.Fatalf("Token = %q, %v; want first (whitespace trimmed)", got, err)
	}

	// A rotated Secret is picked up without a restart, once the interval has
	// elapsed.
	write(t, path, "second")
	if got, err := f.Token(); err != nil || got != "first" {
		t.Fatalf("within the read interval: Token = %q, %v; want the cached first", got, err)
	}
	c.advance(2 * DefaultReadInterval)
	if got, err := f.Token(); err != nil || got != "second" {
		t.Fatalf("after rotation: Token = %q, %v; want second", got, err)
	}

	// The swap window: the file is briefly gone. The token must survive it,
	// and WITHOUT an error — the caller's export/scrape/request would
	// otherwise fail for a credential it still holds.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	c.advance(2 * DefaultReadInterval)
	got, err = f.Token()
	if err != nil {
		t.Fatalf("a failed re-read returned an error (%v); the last good token must be served instead", err)
	}
	if got != "second" {
		t.Fatalf("after a failed re-read: Token = %q, want the last good token", got)
	}
}

// The other half of the same decision: with NO last good value there is
// nothing to serve, so the failure IS the caller's. This is what keeps a
// missing kubelet token file a failed scrape rather than an unauthenticated
// one.
func TestFileWithNoGoodValueReportsTheError(t *testing.T) {
	f := NewFile(filepath.Join(t.TempDir(), "missing"), discard())
	if tok, err := f.Token(); err == nil {
		t.Fatalf("Token = %q, nil; an unreadable file with no cached value must be an error", tok)
	}
	if got := f.Get(); got != "" {
		t.Fatalf("Get = %q, want empty", got)
	}
}

// An empty file is an error, not a silently empty credential.
func TestFileRejectsAnEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	for _, blank := range []string{"", "\n", "  \n\t"} {
		write(t, path, blank)
		if _, err := NewFile(path, discard()).Read(); err == nil {
			t.Errorf("blank token file %q must be an error", blank)
		}
	}
}

// A broken file must not be re-read on every call: the interval is what keeps
// a hot path (every kubelet scrape) off the filesystem, and a failure is
// exactly when a retry storm would be least useful.
func TestFileDoesNotRetryPerCallWhileBroken(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	write(t, path, "tok")
	c := newClock()
	f := NewFile(path, discard(), WithClock(c.now))
	if _, err := f.Token(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	c.advance(2 * DefaultReadInterval)
	if _, err := f.Token(); err != nil {
		t.Fatal(err)
	}
	// Restore the file but do NOT advance the clock: the failed read must have
	// re-stamped the fetch time, so this call is served from cache.
	write(t, path, "new")
	if got, _ := f.Token(); got != "tok" {
		t.Fatalf("Token = %q, want the cached tok: a failed read must still consume the interval", got)
	}
	c.advance(2 * DefaultReadInterval)
	if got, _ := f.Token(); got != "new" {
		t.Fatalf("Token = %q, want new once the interval elapses again", got)
	}
}

// A changed file promotes the old token to a grace-window second value, and
// the window expires. Without the grace, clients (which re-read on their own
// cadence) would 401 until they happened to reload.
func TestRotatingGraceWindow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	write(t, path, "old-token\n")
	c := newClock()
	r, err := NewRotating(path, discard(), WithClock(c.now))
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Tokens(); len(got) != 1 || got[0] != "old-token" {
		t.Fatalf("fresh: %v, want [old-token]", got)
	}

	// Rotate the file; the cached value is still fresh, so nothing changes yet.
	// "Fresh" for a receiver is DefaultRefreshInterval, not the read interval —
	// the clock has not moved at all here, so neither cadence has elapsed.
	write(t, path, "new-token\n")
	if got := r.Tokens(); len(got) != 1 || got[0] != "old-token" {
		t.Fatalf("within the refresh cadence: %v, want the cached [old-token]", got)
	}

	// Past the read interval: both are accepted.
	c.advance(2 * DefaultReadInterval)
	if got := r.Tokens(); len(got) != 2 || got[0] != "new-token" || got[1] != "old-token" {
		t.Fatalf("during the grace window: %v, want [new-token old-token]", got)
	}

	// A re-read that finds no change must not re-arm the window.
	c.advance(2 * DefaultReadInterval)
	r.Tokens()
	c.advance(DefaultGrace)
	if got := r.Tokens(); len(got) != 1 || got[0] != "new-token" {
		t.Fatalf("past the window: %v, want [new-token] (an unchanged re-read must not extend it)", got)
	}
}

// The receiver's initial read is FATAL — deliberately unlike the client's.
func TestNewRotatingRefusesAMissingOrEmptyFile(t *testing.T) {
	dir := t.TempDir()
	if _, err := NewRotating("", discard()); err != ErrNoPath {
		t.Errorf("empty path = %v, want ErrNoPath", err)
	}
	if _, err := NewRotating("   ", discard()); err != ErrNoPath {
		t.Errorf("blank path = %v, want ErrNoPath", err)
	}
	if _, err := NewRotating(filepath.Join(dir, "missing"), discard()); err == nil {
		t.Error("an unreadable token file must refuse to construct")
	}
	path := filepath.Join(dir, "token")
	write(t, path, "\n\n")
	if _, err := NewRotating(path, discard()); err == nil {
		t.Error("an empty token file must refuse to construct")
	}
}

// THE OTHER DIRECTION of a rotation, which had no mechanism at all: the client
// re-read FIRST and presents a token the receiver has not seen. No grace window
// can cover that — a receiver cannot accept a value it has never read — so the
// only remedy is that the receiver reads soon, which is what
// DefaultRefreshInterval is for.
//
// The sequence is the friendliest possible case for the design: ONE shared
// token file, one clock, no kubelet projection skew. With the receiver on the
// same minute cadence as the client, the receiver's last read simply lands
// EARLIER in the minute than the client's, and every request in between is a
// hard 401 — measured at 57 consecutive seconds of rejection on
// /v1/scrape-auth, at the moment this package's doc called rotation a
// non-event.
func TestRotatingAcceptsATokenTheClientRotatedToFirst(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	write(t, path, "A")
	c := newClock()

	// t=0: the receiver reads A at construction.
	rt, err := NewRotating(path, discard(), WithClock(c.now))
	if err != nil {
		t.Fatal(err)
	}
	// t=1: the client reads A. Its cadence is now one second AHEAD of the
	// receiver's — which is the whole setup; nothing about it is unusual.
	c.advance(time.Second)
	f := NewFile(path, discard(), WithClock(c.now))
	if _, err := f.Read(); err != nil {
		t.Fatal(err)
	}
	// t=60: Run's ticker fires on the receiver. Nothing has changed.
	c.advance(DefaultReadInterval - time.Second)
	rt.Tokens()

	// t=61: the Secret is rotated. t=62: the client's own interval has elapsed,
	// so it presents the new token.
	c.advance(time.Second)
	write(t, path, "B")
	c.advance(time.Second)
	presented, err := f.Token()
	if err != nil {
		t.Fatal(err)
	}
	if presented != "B" {
		t.Fatalf("the client presents %q; the test needs it to have re-read first", presented)
	}
	if !Authorized("Bearer "+presented, rt.Tokens()) {
		t.Fatalf("the receiver rejected the rotated token the client already presents "+
			"(accepts %v); a client that re-reads first must not be 401'd", rt.Tokens())
	}
	// And the direction that DOES have a grace window is unaffected: a client
	// that has not re-read yet still authenticates with the predecessor.
	if !Authorized("Bearer A", rt.Tokens()) {
		t.Fatalf("the predecessor stopped being accepted (accepts %v); the grace window is what "+
			"covers every client that has not re-read yet", rt.Tokens())
	}
}

// The refresh cadence must not become a per-second retry of a BROKEN file: the
// warn it logs is per receiver, so a projection that stays unreadable would
// otherwise be one log line per second per process, fleet-wide, about a state
// that is not changing. A failed read backs off to the periodic interval.
func TestRotatingBacksOffToTheIntervalAfterAFailedRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	write(t, path, "tok")
	c := newClock()
	r, err := NewRotating(path, discard(), WithClock(c.now))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	c.advance(DefaultRefreshInterval)
	if got := r.Tokens(); len(got) != 1 || got[0] != "tok" {
		t.Fatalf("after a failed read: %v, want the last good [tok]", got)
	}
	// Restore the file. A read at the refresh cadence would pick it up
	// immediately; the backoff means it does not.
	write(t, path, "new")
	c.advance(2 * DefaultRefreshInterval)
	if got := r.Tokens(); len(got) != 1 || got[0] != "tok" {
		t.Fatalf("%v: a FAILED read must consume the periodic interval, not the refresh cadence — "+
			"otherwise an unreadable token file is re-read (and warned about) once a second", got)
	}
	c.advance(DefaultReadInterval)
	if got := r.Tokens(); len(got) != 2 || got[0] != "new" || got[1] != "tok" {
		t.Fatalf("past the interval: %v, want [new tok]", got)
	}
}

// A failed re-read on the ACCEPT side keeps the fleet authenticated.
func TestRotatingKeepsLastGoodTokenAcrossAFailedReread(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	write(t, path, "tok")
	c := newClock()
	r, err := NewRotating(path, discard(), WithClock(c.now))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	c.advance(2 * DefaultReadInterval)
	if got := r.Tokens(); len(got) != 1 || got[0] != "tok" {
		t.Fatalf("after a failed re-read: %v, want the last good [tok]", got)
	}
}

func TestParse(t *testing.T) {
	for _, tc := range []struct {
		header string
		want   string
		ok     bool
	}{
		{"Bearer s3cr3t", "s3cr3t", true},
		{"bearer s3cr3t", "s3cr3t", true},
		{"BEARER   s3cr3t  ", "s3cr3t", true},
		{"Basic s3cr3t", "", false},
		{"s3cr3t", "", false},
		{"Bearer ", "", false},
		{"", "", false},
	} {
		got, ok := Parse(tc.header)
		if got != tc.want || ok != tc.ok {
			t.Errorf("Parse(%q) = %q, %v; want %q, %v", tc.header, got, ok, tc.want, tc.ok)
		}
	}
}

func TestAuthorized(t *testing.T) {
	tokens := []string{"new", "old"}
	if !Authorized("Bearer new", tokens) {
		t.Error("the current token must authorize")
	}
	if !Authorized("Bearer old", tokens) {
		t.Error("the grace-window token must authorize")
	}
	if Authorized("Bearer other", tokens) {
		t.Error("an unlisted token must not authorize")
	}
	if Authorized("Bearer x", nil) {
		t.Error("no configured tokens must authorize nothing")
	}
	// An empty candidate must never authorize: an unconfigured receiver has to
	// reject, and "" would otherwise match an empty presented token.
	if Authorized("Bearer  ", []string{""}) {
		t.Error("an empty candidate must never authorize")
	}
}

// Run is what makes the rotation contract hold on a listener with NO traffic.
// Tokens() re-reads only when called, and it arms the grace window at that
// moment — so without a clock, a rotation on a quiet receiver is noticed by the
// first request AFTER it and the revoked token stays accepted far past the
// documented grace. That is not hypothetical: the ticker was hand-rolled at one
// call site and simply absent at another (the trace tier's authenticated hop),
// which is why the loop now belongs to the type rather than to whoever
// remembers.
func TestRotatingRunPicksUpRotationWithoutTraffic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("first"), 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := NewRotating(path, discard(), WithInterval(5*time.Millisecond), WithGrace(time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	// Observe the state WITHOUT calling Tokens(). Tokens() re-reads when stale
	// itself, so a test that polls through it passes with Run doing nothing at
	// all — which is exactly what the first version of this test did. Reading
	// the fields under the mutex is what makes the ticker the only thing that
	// can move them.
	state := func() (cur, prev string) {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.cur, r.prev
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	if err := os.WriteFile(path, []byte("second"), 0o600); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cur, prev := state(); cur == "second" {
			// The predecessor must still be accepted for the grace window, or
			// the two ends would have to flip in lockstep.
			if prev != "first" {
				t.Fatalf("the previous token was not retained (prev=%q); the grace window did not arm", prev)
			}
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("Run did not pick up the rotated token without request traffic")
}

// Run must stop with its context, or every receiver leaks a ticker goroutine.
func TestRotatingRunStopsWithContext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("tok"), 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := NewRotating(path, discard(), WithInterval(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return when its context was cancelled")
	}
}
