package bearer

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// capture returns a logger writing logfmt into buf at Debug, plus the buffer.
// The whole point of these assertions is the OPERATOR's view, so they read the
// rendered line rather than a structured record: a key that renders wrong is
// exactly the defect that would not be caught by inspecting slog.Attr values.
func capture() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})), &buf
}

// A read failure with NOTHING to fall back on used to be entirely silent here:
// the error goes to the caller, and Get — the shape a token PROVIDER must
// have — drops it. The symptom was a 401 storm with no line naming the file.
func TestClientWithNoTokenEverReadWarnsAndCounts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent")
	log, buf := capture()
	before := readErrors(t, roleClient)

	f := NewFile(path, log, WithClock(newClock().now))
	if got := f.Get(); got != "" {
		t.Fatalf("Get = %q, want empty", got)
	}
	line := buf.String()
	if !strings.Contains(line, "level=WARN") || !strings.Contains(line, "unauthenticated") {
		t.Errorf("want a WARN naming the consequence, got:\n%s", line)
	}
	if !strings.Contains(line, "path="+path) {
		t.Errorf("the warn must name the file; got:\n%s", line)
	}
	if got := readErrors(t, roleClient) - before; got != 1 {
		t.Errorf("kubescrape_bearer_token_read_errors_total{role=client} moved by %v, want 1", got)
	}
}

// A PERSISTING failure must not become a flood proportional to the request
// rate: the first is a Warn, the repeats drop to Debug (and every one of them
// still moves the counter, which is where the rate lives).
func TestRepeatedClientReadFailuresDropToDebug(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent")
	log, buf := capture()
	c := newClock()
	f := NewFile(path, log, WithClock(c.now))

	before := readErrors(t, roleClient)
	for range 5 {
		_ = f.Get()
	}
	out := buf.String()
	if n := strings.Count(out, "level=WARN"); n != 1 {
		t.Errorf("WARN lines = %d, want exactly 1 (the rest are throttled to Debug):\n%s", n, out)
	}
	if n := strings.Count(out, "level=DEBUG"); n != 4 {
		t.Errorf("DEBUG lines = %d, want 4:\n%s", n, out)
	}
	if got := readErrors(t, roleClient) - before; got != 5 {
		t.Errorf("counter moved by %v, want 5 — the throttle bounds the LOG, never the rate", got)
	}
}

// The recovery is the half a bare warn cannot express: without it an operator
// cannot tell a fixed mount from one nobody is watching any more.
func TestClientReportsRecoveryAndRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	log, buf := capture()
	c := newClock()
	f := NewFile(path, log, WithClock(c.now))

	if _, err := f.Token(); err == nil {
		t.Fatal("want an error while the file is absent")
	}
	write(t, path, "s3cret-value\n")
	if got, err := f.Token(); err != nil || got != "s3cret-value" {
		t.Fatalf("Token = %q, %v", got, err)
	}
	out := buf.String()
	if !strings.Contains(out, "succeeded again") {
		t.Errorf("want a recovery line, got:\n%s", out)
	}
	// The FIRST successful read is not a rotation.
	if strings.Contains(out, "token file changed") {
		t.Errorf("the first read must not report a change:\n%s", out)
	}

	buf.Reset()
	write(t, path, "second-value\n")
	c.advance(2 * DefaultReadInterval)
	if got, _ := f.Token(); got != "second-value" {
		t.Fatalf("Token = %q, want the rotated value", got)
	}
	out = buf.String()
	if !strings.Contains(out, "token file changed") || !strings.Contains(out, "bytes=12") {
		t.Errorf("want a change line carrying the LENGTH, got:\n%s", out)
	}
	if strings.Contains(out, "second-value") {
		t.Fatalf("THE TOKEN LEAKED INTO THE LOG:\n%s", out)
	}
}

// The receiver half: a failed re-read keeps the accept set, which is exactly
// why it needs saying — the 401 arrives later and somewhere else.
func TestReceiverReadFailureWarnsCountsAndRecovers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	write(t, path, "first")
	log, buf := capture()
	c := newClock()
	r, err := NewRotating(path, log, WithClock(c.now))
	if err != nil {
		t.Fatal(err)
	}
	before := readErrors(t, roleReceiver)

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	c.advance(2 * DefaultRefreshInterval)
	if got := r.Tokens(); len(got) != 1 || got[0] != "first" {
		t.Fatalf("Tokens = %v, want the last good set", got)
	}
	if got := readErrors(t, roleReceiver) - before; got != 1 {
		t.Errorf("kubescrape_bearer_token_read_errors_total{role=receiver} moved by %v, want 1", got)
	}
	out := buf.String()
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "401") {
		t.Errorf("want a WARN naming the delayed symptom, got:\n%s", out)
	}

	buf.Reset()
	write(t, path, "second")
	c.advance(2 * DefaultReadInterval)
	got := r.Tokens()
	if len(got) != 2 || got[0] != "second" || got[1] != "first" {
		t.Fatalf("Tokens = %v, want the new token plus its predecessor", got)
	}
	out = buf.String()
	if !strings.Contains(out, "succeeded again") {
		t.Errorf("want a recovery line, got:\n%s", out)
	}
	if !strings.Contains(out, "bytes=6") {
		t.Errorf("the rotation line must carry the length, got:\n%s", out)
	}
	if strings.Contains(out, "second\"") || strings.Contains(out, "=second") {
		t.Fatalf("THE TOKEN LEAKED INTO THE LOG:\n%s", out)
	}
}
