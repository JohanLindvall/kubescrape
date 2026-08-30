package otlpexport

// What an operator can SEE when the collector is not there, is there and
// refusing, or comes back. Every case here is a path that moved a counter and
// said nothing, on the configuration an operator meets FIRST: an endpoint that
// is wrong, a TLS pair that disagrees, a spool on a disk that is full.

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

func capturedLogger() (*slog.Logger, func() string) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(h), buf.String
}

// oneRecord is the smallest payload that reaches the wire.
func oneRecord() plog.Logs {
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("x")
	return ld
}

// The headline case: a collector that is not up yet. Before this, the ONLY
// evidence was kubescrape_export_requests_total{outcome="error"} — which named
// neither the endpoint nor the reason, and read identically for a collector
// that was rejecting every payload.
func TestUnreachableCollectorIsNamedOnceWithItsEndpointAndARemedy(t *testing.T) {
	// A port nothing listens on: httptest hands out a real one, then closes it.
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	endpoint := srv.URL
	srv.Close()

	log, dump := capturedLogger()
	c, err := New(Config{Endpoint: endpoint, Protocol: "http", Timeout: time.Second, Compression: "none"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	c.health = NewFailureReporter(log, "the OTLP collector", "endpoint", endpoint, "protocol", "http")

	before := obs.Exports.WithLabelValues("logs", "transient").Value()
	for i := 0; i < 5; i++ {
		if err := c.ExportLogs(context.Background(), oneRecord()); err == nil {
			t.Fatal("expected the export to fail against a closed port")
		}
	}
	if got := obs.Exports.WithLabelValues("logs", "transient").Value() - before; got != 5 {
		t.Errorf("kubescrape_export_requests_total{logs,transient} delta = %v, want 5", got)
	}
	out := dump()
	// ONE line for five failures: the condition is a state and a busy node
	// exports several times a second.
	if n := strings.Count(out, "exports to the OTLP collector are failing"); n != 1 {
		t.Errorf("want exactly one failure line for a persisting outage, got %d:\n%s", n, out)
	}
	for _, want := range []string{"signal=logs", "class=transient", "endpoint=" + endpoint, "nothing is listening"} {
		if !strings.Contains(out, want) {
			t.Errorf("failure line does not carry %q:\n%s", want, out)
		}
	}
}

// A collector that ANSWERS and rejects is a different problem with a different
// fix, and the class is what separates them: transient data is coming back,
// permanent data is being lost.
func TestRejectedPayloadIsClassifiedPermanentAndSaysSo(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "cannot parse", http.StatusBadRequest)
	}))
	defer srv.Close()

	log, dump := capturedLogger()
	c, err := New(Config{Endpoint: srv.URL, Protocol: "http", Timeout: time.Second, Compression: "none"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	c.health = NewFailureReporter(log, "the OTLP collector", "endpoint", srv.URL)

	before := obs.Exports.WithLabelValues("logs", "permanent").Value()
	if err := c.ExportLogs(context.Background(), oneRecord()); err == nil {
		t.Fatal("expected a 400 to fail the export")
	}
	if got := obs.Exports.WithLabelValues("logs", "permanent").Value() - before; got != 1 {
		t.Errorf("kubescrape_export_requests_total{logs,permanent} delta = %v, want 1", got)
	}
	out := dump()
	if !strings.Contains(out, "class=permanent") || !strings.Contains(out, "malformed") {
		t.Errorf("a permanent rejection must say so and say why:\n%s", out)
	}
}

// The narrative's three transitions, on the reporter itself so the clock is
// controllable: first failure warns, a persisting one is throttled, recovery
// says the outage is over and how long it lasted.
func TestFailureReporterWarnsOnceThenOnRecovery(t *testing.T) {
	log, dump := capturedLogger()
	r := NewFailureReporter(log, "the OTLP collector", "endpoint", "collector:4317")
	now := time.Unix(1000, 0)
	r.now = func() time.Time { return now }

	boom := status.Error(codes.Unavailable, "connection refused")
	for i := 0; i < 4; i++ {
		r.Note("metrics", boom)
		now = now.Add(10 * time.Second)
	}
	if n := strings.Count(dump(), "are failing"); n != 1 {
		t.Fatalf("want 1 warning while the condition persists, got %d:\n%s", n, dump())
	}
	// Past the window it re-warns, so a long outage is not silent forever.
	now = now.Add(failWarnEvery)
	r.Note("metrics", boom)
	if n := strings.Count(dump(), "are failing"); n != 2 {
		t.Fatalf("want a re-warn past the window, got %d lines:\n%s", n, dump())
	}
	if !strings.Contains(dump(), "failing=") {
		t.Errorf("a re-warn must carry how long this has been failing:\n%s", dump())
	}

	now = now.Add(time.Minute)
	r.Note("metrics", nil)
	out := dump()
	if !strings.Contains(out, "are being accepted again") || !strings.Contains(out, "outage=") {
		t.Errorf("recovery must be reported with the outage's length:\n%s", out)
	}
	// And a second success says nothing: steady state is quiet.
	r.Note("metrics", nil)
	if n := strings.Count(dump(), "accepted again"); n != 1 {
		t.Errorf("recovery must be reported once, got %d:\n%s", n, dump())
	}
}

// THE FLOOD THIS FILE EXISTS TO PREVENT, aimed at the report itself: a healthy
// collector that refuses ONE poison payload (a shape its pipeline will not
// parse) while accepting everything else. A rejection is a verdict on the
// PAYLOAD, so if it moves destination health the state re-arms on the very next
// accepted batch and the pair warn/recovery repeats per export — on the path an
// operator watches during an incident, at the rate a busy node exports.
func TestRejectedPayloadDoesNotFlapDestinationHealth(t *testing.T) {
	log, dump := capturedLogger()
	r := NewFailureReporter(log, "the OTLP collector", "endpoint", "collector:4317")
	now := time.Unix(1000, 0)
	r.now = func() time.Time { return now }

	poison := &HTTPStatusError{Code: 400, Body: "cannot parse"}
	for i := 0; i < 50; i++ {
		r.Note("logs", poison) // the poison batch
		r.Note("logs", nil)    // and everything else, accepted
		now = now.Add(time.Second)
	}

	out := dump()
	if n := strings.Count(out, "are failing"); n != 0 {
		t.Errorf("a rejected payload must not read as an unreachable destination, got %d lines:\n%s", n, out)
	}
	if n := strings.Count(out, "accepted again"); n != 0 {
		t.Errorf("a destination that never failed must not recover, got %d lines:\n%s", n, out)
	}
	// It IS reported — once per window, with the rate the throttle hides.
	if n := strings.Count(out, "rejected this telemetry outright"); n != 1 {
		t.Fatalf("want exactly one throttled rejection line for 50 rejections, got %d:\n%s", n, out)
	}
	for _, want := range []string{"class=permanent", "signal=logs", "endpoint=collector:4317", "malformed"} {
		if !strings.Contains(out, want) {
			t.Errorf("the rejection line does not carry %q:\n%s", want, out)
		}
	}
}

// The rejection line is throttled, not silenced: a destination that keeps
// refusing says so on every window, and carries how many payloads were dropped
// since the previous line rather than implying this one is the only one.
func TestRejectionsReWarnOnTheWindowWithTheirCount(t *testing.T) {
	log, dump := capturedLogger()
	r := NewFailureReporter(log, "the OTLP collector")
	now := time.Unix(0, 0)
	r.now = func() time.Time { return now }

	poison := status.Error(codes.InvalidArgument, "bad payload")
	r.Note("traces", poison)
	for i := 0; i < 3; i++ {
		now = now.Add(time.Minute)
		r.Note("traces", poison)
	}
	if n := strings.Count(dump(), "rejected this telemetry outright"); n != 1 {
		t.Fatalf("want the rejection line throttled inside the window, got %d:\n%s", n, dump())
	}
	now = now.Add(failWarnEvery)
	r.Note("traces", poison)
	out := dump()
	if n := strings.Count(out, "rejected this telemetry outright"); n != 2 {
		t.Fatalf("want a re-warn past the window, got %d:\n%s", n, out)
	}
	if !strings.Contains(out, "rejected=4") || !strings.Contains(out, "over=") {
		t.Errorf("the re-warn must carry how many payloads were dropped and over how long:\n%s", out)
	}
}

// The other half of the split: a transient failure still drives the
// reachability narrative exactly as before, even while payloads are being
// rejected. The two reports must not silence each other.
func TestRejectionsDoNotSuppressTheTransientNarrative(t *testing.T) {
	log, dump := capturedLogger()
	r := NewFailureReporter(log, "the OTLP collector")
	now := time.Unix(0, 0)
	r.now = func() time.Time { return now }

	r.Note("metrics", &HTTPStatusError{Code: 400, Body: "no"})
	r.Note("metrics", status.Error(codes.Unavailable, "connection refused"))
	now = now.Add(time.Second)
	r.Note("metrics", nil)

	out := dump()
	if n := strings.Count(out, "are failing"); n != 1 {
		t.Errorf("the transient outage must still warn once, got %d:\n%s", n, out)
	}
	if n := strings.Count(out, "accepted again"); n != 1 {
		t.Errorf("the transient outage must still report its recovery, got %d:\n%s", n, out)
	}
}

// Shutdown cancels every in-flight export at once. A "the collector is failing"
// line emitted on the way out is a false alarm on the one line an operator
// reads most carefully.
func TestCancelledExportIsNotReportedAsAFailure(t *testing.T) {
	log, dump := capturedLogger()
	r := NewFailureReporter(log, "the OTLP collector")
	r.Note("logs", context.Canceled)
	if strings.Contains(dump(), "are failing") {
		t.Errorf("a cancelled export must not read as a collector failure:\n%s", dump())
	}
}

// The `note` is the whole point of the line for a first live run: the raw error
// is complete and unreadable, and each of these has a different fix.
func TestDiagnoseNamesTheFirstRunMistakes(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"refused", errors.New("dial tcp 10.0.0.1:4317: connect: connection refused"), "nothing is listening"},
		{"dns", errors.New("dial tcp: lookup otel-collector: no such host"), "does not resolve"},
		{"tls to a plaintext port", errors.New("tls: first record does not look like a TLS handshake"), "-otlp-insecure"},
		{"untrusted ca", errors.New("x509: certificate signed by unknown authority"), "-otlp-ca-file"},
		{"unimplemented", status.Error(codes.Unimplemented, "unknown service"), "does not serve this signal"},
		{"auth", &HTTPStatusError{Code: 401}, "credentials"},
		{"too large", &HTTPStatusError{Code: 413}, "-otlp-max-send-bytes"},
		{"redirect", &HTTPStatusError{Code: 302}, "redirect"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Diagnose(tc.err); !strings.Contains(got, tc.want) {
				t.Errorf("Diagnose(%v) = %q, want it to mention %q", tc.err, got, tc.want)
			}
		})
	}
	// An unrecognised error gets no hint rather than a guessed one.
	if got := Diagnose(errors.New("something nobody has seen")); got != "" {
		t.Errorf("an unknown error must not be given a made-up remedy, got %q", got)
	}
}

// The size split fired on every over-cap payload and was reported by nothing:
// each extra part is its own round trip, auth build and gzip pass.
func TestSplitPartsAreCounted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c, err := New(Config{Endpoint: srv.URL, Protocol: "http", Timeout: 5 * time.Second,
		Compression: "none", MaxSendBytes: 4 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	before := obs.ExportSplitParts.WithLabelValues("logs").Value()
	if err := c.ExportLogs(context.Background(), buildLogs(1, 200, 200)); err != nil {
		t.Fatal(err)
	}
	if got := obs.ExportSplitParts.WithLabelValues("logs").Value() - before; got < 1 {
		t.Errorf("kubescrape_export_split_parts_total{logs} did not move for a payload that had to be split")
	}
	// A payload that fits is not a split.
	before = obs.ExportSplitParts.WithLabelValues("logs").Value()
	if err := c.ExportLogs(context.Background(), oneRecord()); err != nil {
		t.Fatal(err)
	}
	if got := obs.ExportSplitParts.WithLabelValues("logs").Value() - before; got != 0 {
		t.Errorf("a payload sent whole must not count as a split, got %v", got)
	}
}

// The disk buffer refusing a write was counted and never spoken about, and the
// two ways it happens need different responses. A FULL spool is the collector
// being down for longer than the cap covers; a spool that cannot be WRITTEN to
// is the node's disk, and the *os.PathError naming the directory — the whole
// diagnosis — was discarded.
func TestFullDiskBufferSaysSoWithItsDirectoryAndCap(t *testing.T) {
	log, dump := capturedLogger()
	dir := t.TempDir()
	ls, err := OpenBuffer(dir+"/logs", 600) // small cap: a few frames
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ls.Close() }()
	b := NewBuffered(&fakeSender{}, ls, nil, nil, 10*time.Millisecond, log)

	before := obs.BufferFull.WithLabelValues("logs").Value()
	filled := false
	for i := 0; i < 500 && !filled; i++ {
		if err := b.ExportLogs(context.Background(), logsWith("pad")); err != nil {
			filled = true
		}
	}
	if !filled {
		t.Fatal("cap too large: the spool never filled")
	}
	if obs.BufferFull.WithLabelValues("logs").Value() <= before {
		t.Error("kubescrape_buffer_full_total{logs} did not move")
	}
	out := dump()
	if !strings.Contains(out, "the disk buffer is refusing new batches") {
		t.Fatalf("a full spool must say so:\n%s", out)
	}
	for _, want := range []string{"signal=logs", "dir=", "maxBytes="} {
		if !strings.Contains(out, want) {
			t.Errorf("the line does not carry %q:\n%s", want, out)
		}
	}
	// The condition persists for as long as the collector is down, and every
	// producer on the node meets it on every flush: one line, not thousands.
	if n := strings.Count(out, "the disk buffer is refusing new batches"); n != 1 {
		t.Errorf("want the full-spool line throttled to one, got %d", n)
	}
}

// A spool whose directory has been taken away answers ErrClosed forever after
// the reopen fails. The enqueue counter moves and, until now, the reason the
// spool could not come back lived only in a discarded error.
func TestABrokenDiskBufferReportsWhyItCannotBeWrittenTo(t *testing.T) {
	log, dump := capturedLogger()
	ls, err := OpenBuffer(t.TempDir()+"/logs", 0)
	if err != nil {
		t.Fatal(err)
	}
	b := NewBuffered(&fakeSender{}, ls, nil, nil, 10*time.Millisecond, log)
	// Close is the reachable stand-in for a latched I/O failure: both leave a
	// dead handle that every later Add answers ErrClosed for.
	if err := ls.Close(); err != nil {
		t.Fatal(err)
	}
	before := obs.BufferEnqueueErrors.WithLabelValues("logs").Value()
	if err := b.ExportLogs(context.Background(), logsWith("x")); err == nil {
		t.Fatal("expected the enqueue to fail on a closed spool")
	}
	if obs.BufferEnqueueErrors.WithLabelValues("logs").Value() <= before {
		t.Error("kubescrape_buffer_enqueue_errors_total{logs} did not move")
	}
	out := dump()
	if !strings.Contains(out, "the disk buffer cannot be written to") || !strings.Contains(out, "error=") {
		t.Errorf("a broken spool must report the filesystem's own error:\n%s", out)
	}
}

// A reopen that FAILS leaves the signal with no durability at all, and the
// recovery then depends on a condition (the directory, the mount, the flock)
// that only this error names. It used to `return` in silence, so the symptom
// was kubescrape_buffer_enqueue_errors_total climbing forever with no cause
// anywhere.
func TestAFailedDiskBufferReopenIsReported(t *testing.T) {
	log, dump := capturedLogger()
	dir := t.TempDir()
	buf, err := OpenBuffer(dir+"/logs", 0)
	if err != nil {
		t.Fatal(err)
	}
	// NewBuffered is what gives the Buffer its signal name and logger (the
	// fields recover reports through).
	_ = NewBuffered(&fakeSender{}, buf, nil, nil, time.Millisecond, log)

	q, _ := buf.handles()
	// Make the reopen impossible: the queue's directory becomes a regular file.
	if err := os.RemoveAll(dir + "/logs"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/logs", []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	buf.recover(q)

	out := dump()
	if !strings.Contains(out, "could not be reopened") || !strings.Contains(out, "signal=logs") {
		t.Errorf("a failed reopen must name the signal and the error:\n%s", out)
	}
	// It is reached from every enqueue and every drain iteration while the
	// handle stays dead, so it must be throttled.
	buf.recover(q)
	if n := strings.Count(dump(), "could not be reopened"); n != 1 {
		t.Errorf("want the failed-reopen line throttled to one, got %d", n)
	}
}
