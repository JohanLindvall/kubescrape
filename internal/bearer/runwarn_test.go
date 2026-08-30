package bearer

// The first failure of a RUN warns; the repeats drop to Debug. The transition
// is what these pin: reWarnInterval documented it, and neither half
// implemented it — both consulted the throttle alone, whose zero value fires
// once per PROCESS. A mount that broke, was fixed, and broke again inside the
// re-warn window therefore opened its second outage at Debug, directly below
// the "succeeded again" Info line, which reads as a mount that got better and
// stayed better.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestClientSecondFailureRunWarnsAgain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	write(t, path, "first")
	log, buf := capture()
	c := newClock()
	f := NewFile(path, log, WithClock(c.now))

	if _, err := f.Token(); err != nil {
		t.Fatal(err)
	}
	// Run one: broken.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	c.advance(2 * DefaultReadInterval)
	if _, err := f.Token(); err != nil {
		t.Fatalf("Token = %v, want the last good value kept", err)
	}
	if n := strings.Count(buf.String(), "level=WARN"); n != 1 {
		t.Fatalf("the first run did not open on a WARN (%d):\n%s", n, buf.String())
	}

	// Fixed.
	write(t, path, "second")
	c.advance(2 * DefaultReadInterval)
	if _, err := f.Token(); err != nil {
		t.Fatal(err)
	}
	buf.Reset()

	// Run two, still inside reWarnInterval of the first run's warn: it is a
	// NEW outage and must be reported as one.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	c.advance(2 * DefaultReadInterval)
	if _, err := f.Token(); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "level=WARN") {
		t.Errorf("a second failure run opened at Debug, under the recovery line:\n%s", out)
	}
}

func TestReceiverSecondFailureRunWarnsAgain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	write(t, path, "first")
	log, buf := capture()
	c := newClock()
	r, err := NewRotating(path, log, WithClock(c.now))
	if err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	c.advance(2 * DefaultReadInterval)
	r.Tokens()
	if n := strings.Count(buf.String(), "level=WARN"); n != 1 {
		t.Fatalf("the first run did not open on a WARN (%d):\n%s", n, buf.String())
	}

	write(t, path, "second")
	c.advance(2 * DefaultReadInterval)
	r.Tokens()
	buf.Reset()

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	c.advance(2 * DefaultReadInterval)
	if got := r.Tokens(); len(got) == 0 {
		t.Fatal("Tokens = empty, want the last good accept set")
	}
	out := buf.String()
	if !strings.Contains(out, "level=WARN") {
		t.Errorf("a second failure run opened at Debug, under the recovery line:\n%s", out)
	}
}
