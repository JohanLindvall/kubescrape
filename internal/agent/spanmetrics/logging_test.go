package spanmetrics

// What New decides for itself, and says. Both cases silently changed what the
// operator configured: a dropped dimension is a label that never appears, and a
// fallback eviction age can turn the cardinality cap into a one-way latch.

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func capturedLog() (*slog.Logger, func() string) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(h), buf.String
}

// A dimension colliding with a built-in is DROPPED (it would blank the real
// label and render two series identically — cumagg.Builtins). Right, but the
// operator gets no error and finds the label missing from every series.
func TestDroppedDimensionsAreWarnedAbout(t *testing.T) {
	log, dump := capturedLog()
	g := New(Config{Dimensions: []string{"http.method", "span.name", "http.method"}, Logger: log})

	out := dump()
	if n := strings.Count(out, "ignoring a"); n != 2 {
		t.Errorf("want a line per dropped dimension (the built-in collision and the repeat), got %d:\n%s", n, out)
	}
	if !strings.Contains(out, "key=span.name") {
		t.Errorf("the warning does not name the dimension:\n%s", out)
	}
	// And the surviving dimension is still there: the report must not change
	// what is kept.
	if len(g.extra) != 1 || g.extra[0] != "http.method" {
		t.Errorf("extra dimensions = %v, want [http.method]", g.extra)
	}
}

func TestAcceptedDimensionsAreSilent(t *testing.T) {
	log, dump := capturedLog()
	New(Config{Dimensions: []string{"http.method", "http.route"}, Logger: log})
	if out := dump(); out != "" {
		t.Errorf("an ordinary dimension list logged:\n%s", out)
	}
}

// Validate refuses a bad staleAfter and -check-config runs it, so reaching New
// with one means validation was bypassed — and the fallback silently applies a
// DIFFERENT eviction policy than the one written down.
func TestUnparseableStaleAfterIsWarnedAbout(t *testing.T) {
	log, dump := capturedLog()
	New(Config{StaleAfter: "fifteen minutes", Logger: log})
	out := dump()
	if !strings.Contains(out, "staleAfter is unparseable") {
		t.Errorf("the fallback is silent:\n%s", out)
	}
	if !strings.Contains(out, "staleAfter=15m0s") {
		t.Errorf("the warning does not say what was used instead:\n%s", out)
	}
}
