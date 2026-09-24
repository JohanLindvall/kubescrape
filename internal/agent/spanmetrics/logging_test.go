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
// label and render two series identically — cumagg.Builtins), and so are an
// empty entry (an attribute with an empty KEY on every point) and a repeat.
// Right, but the operator gets no error and finds the label missing from every
// series — so each drop is one sentence from DimensionWarnings, the PURE
// function configWarnings emits from -check-config and a real start alike (New
// logging it was invisible to the dry run).
func TestDroppedDimensionsAreWarnedAbout(t *testing.T) {
	cfg := Config{Dimensions: []string{"http.method", "span.name", "http.method", "", "le"}}
	warns := cfg.DimensionWarnings()
	if len(warns) != 4 {
		t.Fatalf("want a sentence per dropped dimension (built-in, repeat, empty, le), got %d: %q", len(warns), warns)
	}
	all := strings.Join(warns, "\n")
	for _, want := range []string{
		`traceMetrics.dimensions[1] "span.name"`,
		`traceMetrics.dimensions[2] "http.method" is ignored: it repeats an earlier entry`,
		`traceMetrics.dimensions[3] "" is ignored: it is empty`,
		`traceMetrics.dimensions[4] "le"`,
	} {
		if !strings.Contains(all, want) {
			t.Errorf("the warnings do not name %s:\n%s", want, all)
		}
	}
	// And the generator drops exactly those: the report must describe what New
	// keeps, which is why both come from one walk.
	g := New(cfg)
	if len(g.extra) != 1 || g.extra[0] != "http.method" {
		t.Errorf("extra dimensions = %q, want [http.method]", g.extra)
	}
}

// New does not WARN about them itself: configWarnings does, on every start, so
// a Warn here would print each line twice. It keeps a Debug trace.
func TestNewLogsDroppedDimensionsOnlyAtDebug(t *testing.T) {
	log, dump := capturedLog()
	New(Config{Dimensions: []string{"span.name", ""}, Logger: log})
	out := dump()
	if strings.Contains(out, "level=WARN") {
		t.Errorf("New warned about a dropped dimension; configWarnings owns that line:\n%s", out)
	}
	if strings.Count(out, "level=DEBUG") != 2 {
		t.Errorf("want one Debug line per dropped dimension:\n%s", out)
	}
}

func TestAcceptedDimensionsAreSilent(t *testing.T) {
	log, dump := capturedLog()
	cfg := Config{Dimensions: []string{"http.method", "http.route"}, Logger: log}
	New(cfg)
	if out := dump(); out != "" {
		t.Errorf("an ordinary dimension list logged:\n%s", out)
	}
	if w := cfg.DimensionWarnings(); len(w) != 0 {
		t.Errorf("an ordinary dimension list warned: %q", w)
	}
}

// Validate refuses a bad staleAfter and -check-config runs it, so reaching New
// with one means validation was bypassed — and the fallback silently applies a
// DIFFERENT eviction policy than the one written down.
func TestUnparseableStaleAfterIsWarnedAbout(t *testing.T) {
	log, dump := capturedLog()
	New(Config{StaleAfter: "fifteen minutes", Logger: log})
	out := dump()
	if !strings.Contains(out, "staleAfter is invalid") {
		t.Errorf("the fallback is silent:\n%s", out)
	}
	if !strings.Contains(out, "staleAfter=15m0s") {
		t.Errorf("the warning does not say what was used instead:\n%s", out)
	}
}
