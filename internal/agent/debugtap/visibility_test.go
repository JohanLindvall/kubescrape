package debugtap

import (
	"bufio"
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
)

// syncBuffer is a log sink a test may read while the handler goroutine writes
// to it. The attach/detach lines are emitted on net/http's connection
// goroutine, so a bare bytes.Buffer here is a data race in the TEST — which the
// race detector reports as a failure of the code under test.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// captureLog installs a capturing default logger for the duration of a test.
func captureLog(t *testing.T) *syncBuffer {
	t.Helper()
	logged := &syncBuffer{}
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logged, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(old) })
	// The attach and refusal gates are package-level (a Throttle holds an
	// atomic, so they are pointers a test replaces rather than assigns over).
	// A test that captures the log wants to read the FIRST line of each
	// condition, not whatever an earlier test left of the window.
	streamAttaches = &logdedupe.Throttle{}
	streamRefusals = &logdedupe.Throttle{}
	return logged
}

// An attached stream is a STANDING cost on the exporting goroutine — for the
// tailer, the single sweep goroutine serving every log file on the node — that
// lasts until a human disconnects. A forgotten `curl` is therefore a
// performance incident whose only trace, before this, was the agent getting
// slower; the attach/detach pair is what makes it findable in the log.
func TestStreamAttachAndDetachAreLogged(t *testing.T) {
	logged := captureLog(t)
	tap := New(&fakeInner{})
	srv := httptest.NewServer(http.HandlerFunc(tap.ServeHTTP))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "?signal=logs&attr=k8s.namespace.name%3Dteam-a")
	if err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(resp.Body)
	if !sc.Scan() { // the banner: the handler is now past the attach line
		t.Fatal("no banner line")
	}
	if out := logged.String(); !strings.Contains(out, "stream attached") ||
		!strings.Contains(out, "signal=logs") || !strings.Contains(out, "filters=1") {
		t.Fatalf("attach was not reported with its shape:\n%s", out)
	}
	_ = resp.Body.Close()

	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(logged.String(), "stream detached") && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if out := logged.String(); !strings.Contains(out, "stream detached") || !strings.Contains(out, "dropped=0") {
		t.Fatalf("detach was not reported with its drop tally:\n%s", out)
	}
}

// The 503 tells the CALLER it was refused; it cannot tell them why, because
// the reason is the other four sessions they cannot see. The refusal belongs
// in the agent's log beside the attach lines that name them.
func TestStreamRefusalIsLogged(t *testing.T) {
	logged := captureLog(t)
	tap := New(&fakeInner{})
	for i := 0; i < maxSubscribers; i++ {
		sub, unsub := tap.subscribe(sigAll, nil, 100)
		if sub == nil {
			t.Fatalf("subscriber %d refused under the cap", i)
		}
		defer unsub()
	}
	rec := httptest.NewRecorder()
	tap.ServeHTTP(rec, httptest.NewRequest("GET", "/debug/otlp", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("over-cap stream = %d, want 503", rec.Code)
	}
	if out := logged.String(); !strings.Contains(out, "refusing a /debug/otlp stream") ||
		!strings.Contains(out, "streams=4") {
		t.Fatalf("the refusal was not reported:\n%s", out)
	}
}

// A marshal failure and "nothing matched this subscriber's filters" are the
// same thing on the wire — no line — so the tap has to tell them apart itself
// or an operator reads a broken render as an idle agent. The failure cannot be
// provoked through the public API (pdata this process built always marshals),
// so the seam is exercised directly.
func TestRenderFailureIsReportedAndNoMatchIsNot(t *testing.T) {
	logged := captureLog(t)
	tap := New(&fakeInner{})
	sub, unsub := tap.subscribe(sigAll, nil, 100)
	if sub == nil {
		t.Fatal("subscribe refused")
	}
	defer unsub()

	// No match: silent, and nothing queued.
	tap.offer(sigLogs, func(*subscriber) ([]byte, *renderFailure) { return nil, nil })
	if out := logged.String(); strings.Contains(out, "rendering a payload") {
		t.Fatalf("a no-match render was reported as a failure:\n%s", out)
	}

	// A failure: reported once, naming the signal and the error.
	fail := &renderFailure{signal: "metrics", err: context.DeadlineExceeded}
	// The gate is process-wide, so claim a fresh window rather than depending
	// on no sibling test having spent it.
	marshalWarns = &logdedupe.Throttle{}
	tap.offer(sigMetrics, func(*subscriber) ([]byte, *renderFailure) { return nil, fail })
	out := logged.String()
	if !strings.Contains(out, "rendering a payload") || !strings.Contains(out, "signal=metrics") {
		t.Fatalf("the render failure was not reported:\n%s", out)
	}
	// Throttled: a payload that fails to marshal fails on every export, so the
	// second one inside the window must stay quiet.
	before := strings.Count(out, "rendering a payload")
	tap.offer(sigMetrics, func(*subscriber) ([]byte, *renderFailure) { return nil, fail })
	if got := strings.Count(logged.String(), "rendering a payload"); got != before {
		t.Fatalf("the render failure was reported %d times inside one throttle window, want %d", got, before)
	}
}

// The renderers report a marshal error rather than folding it into the
// no-match answer; this pins the two-valued contract the reporting depends on.
func TestRenderersDistinguishNoMatchFromFailure(t *testing.T) {
	tap := New(&fakeInner{})
	sub := &subscriber{signals: sigAll, sample: 100, filters: []attrFilter{{Key: "nope", Value: "*"}}}
	if b, fail := tap.renderLogs(logsWithNamespaces("team-a"), sub); b != nil || fail != nil {
		t.Fatalf("no-match render = (%v, %v), want (nil, nil)", b, fail)
	}
	sub.filters = nil
	if b, fail := tap.renderMetrics(pmetric.NewMetrics(), sub); b != nil || fail != nil {
		t.Fatalf("empty render = (%v, %v), want (nil, nil)", b, fail)
	}
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	if b, fail := tap.renderLogs(ld, sub); b == nil || fail != nil {
		t.Fatalf("a matching render = (%v, %v), want bytes and no failure", b, fail)
	}
}

// The stream RESETS its own drop tally each time it tells its reader about it
// (Swap), so the detach line needs a tally nothing resets — otherwise a
// session that spent its life dropping payloads signs off with dropped=0,
// which is the opposite of what happened.
func TestDetachTallyIsNotResetByTheOnStreamReport(t *testing.T) {
	sub := &subscriber{signals: sigAll, sample: 100, ch: make(chan []byte, 1)}
	sub.drop()
	sub.drop()
	if got := sub.dropped.Swap(0); got != 2 { // what the stream reports and clears
		t.Fatalf("stream-side tally = %d, want 2", got)
	}
	if got := sub.droppedAll.Load(); got != 2 {
		t.Fatalf("lifetime tally = %d after the stream cleared its own, want 2", got)
	}
}

// A refusal is a STATE, not an event: the slots are taken and will stay taken
// until a human closes a session. Nothing rate-limits the requests being
// refused — a client retrying `curl` in a loop, or the built-in UI's Start
// button — so an unthrottled line at the cap is a flood proportional to the
// retry rate, and it buries the one line that says who is holding the slots.
func TestRepeatedStreamRefusalsAreThrottled(t *testing.T) {
	logged := captureLog(t)
	tap := New(&fakeInner{})
	for i := 0; i < maxSubscribers; i++ {
		sub, unsub := tap.subscribe(sigAll, nil, 100)
		if sub == nil {
			t.Fatalf("subscriber %d refused under the cap", i)
		}
		defer unsub()
	}
	for i := 0; i < 8; i++ {
		rec := httptest.NewRecorder()
		tap.ServeHTTP(rec, httptest.NewRequest("GET", "/debug/otlp", nil))
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("refusal %d = %d, want 503", i, rec.Code)
		}
	}
	out := logged.String()
	if n := strings.Count(out, "refusing a /debug/otlp stream"); n != 1 {
		t.Fatalf("want one throttled refusal line for eight refused requests, got %d:\n%s", n, out)
	}
	// One line per window has to carry the whole story, so it names how many
	// streams are held and how long the oldest has been held.
	if !strings.Contains(out, "streams=4") || !strings.Contains(out, "oldest=") {
		t.Errorf("the refusal does not say who is holding the slots:\n%s", out)
	}
}

// Same argument one level up: the cap bounds how many streams may be attached
// AT ONCE and says nothing about the RATE, so a reconnecting client would emit
// an Info pair per request. The pair is throttled TOGETHER — a "detached" whose
// "attached" was suppressed reads as a stream that was never there.
func TestRepeatedStreamAttachesAreThrottled(t *testing.T) {
	logged := captureLog(t)
	tap := New(&fakeInner{})
	srv := httptest.NewServer(http.HandlerFunc(tap.ServeHTTP))
	defer srv.Close()

	// connect opens a stream, waits until it is attached and the banner has
	// been written, then closes it and waits for the slot to come back. The
	// body must be closed before any assertion: srv.Close() waits for the
	// handler, so a Fatal with the stream still open hangs the test.
	connect := func() {
		t.Helper()
		resp, err := http.Get(srv.URL + "?signal=logs")
		if err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(resp.Body)
		if !sc.Scan() {
			_ = resp.Body.Close()
			t.Fatal("no banner line")
		}
		_ = resp.Body.Close()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if _, streams := tap.oldestStream(); streams == 0 {
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatal("the stream never detached")
	}
	for range 3 {
		connect()
	}

	out := logged.String()
	if n := strings.Count(out, "stream attached"); n != 1 {
		t.Errorf("want one throttled attach line for three sessions, got %d:\n%s", n, out)
	}
	if n := strings.Count(out, "stream detached"); n != 1 {
		t.Errorf("the pair does not match: %d detach lines for one attach:\n%s", n, out)
	}
}
