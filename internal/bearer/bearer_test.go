package bearer

import (
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
	write(t, path, "new-token\n")
	if got := r.Tokens(); len(got) != 1 || got[0] != "old-token" {
		t.Fatalf("within the read interval: %v, want the cached [old-token]", got)
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
