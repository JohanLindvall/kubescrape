package metrics

import (
	"log/slog"
	"strings"
	"testing"

	"github.com/JohanLindvall/haste/xxh3"
)

// Dump serves GET /metrics — the delivery path for this process's OWN
// telemetry whenever -self-metrics-interval=0 — and it used to answer a label
// string it could not parse by dropping the data point and moving on: no
// counter, no log line, and the series simply absent from the response. That is
// the one place where a silent skip degrades the signal an operator uses to
// diagnose everything else.

// corrupt installs a sample whose stored label string cannot be parsed back.
// The Registry's own label sets come from code, so this state is only reachable
// through memory corruption or a round-trip bug — which is exactly why its
// arrival must be reported rather than absorbed.
func corrupt(s *series, labels string, value float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.link(&expiringSample{sample: sample{value: value, labels: labels}},
		xxh3.Uint128{Hi: 1, Lo: uint64(s.count) + 1})
}

func TestDumpCountsAndNamesAnUnparseableLabelSet(t *testing.T) {
	r := NewRegistry()
	c := r.CounterVec("kubescrape_test_counter", "help", "reason")
	c.WithLabelValues("ok").Inc()
	corrupt(r.byName["kubescrape_test_counter"], "not-a-label-set", 7)

	before := r.DumpLabelErrors()
	got := r.Dump()
	if delta := r.DumpLabelErrors() - before; delta != 1 {
		t.Fatalf("DumpLabelErrors moved by %d, want 1", delta)
	}
	// The good point still renders: one bad label string must not cost the
	// whole series.
	if len(got) != 1 || len(got[0].Points) != 1 {
		t.Fatalf("dump = %+v, want the one parseable point", got)
	}
}

// A histogram takes the same skip on its own branch of dumpSeries; both had the
// defect and both need the count, or a corrupt histogram is the invisible half.
func TestDumpCountsAnUnparseableHistogramLabelSet(t *testing.T) {
	r := NewRegistry()
	h := r.HistogramVec("kubescrape_test_hist", "help", nil, "reason")
	h.WithLabelValues("ok").Observe(1)
	corrupt(r.byName["kubescrape_test_hist"], "{bad", 1)

	before := r.DumpLabelErrors()
	if got := r.Dump(); len(got) != 1 || len(got[0].Points) != 1 {
		t.Fatalf("dump = %+v, want the one parseable point", got)
	}
	if delta := r.DumpLabelErrors() - before; delta != 1 {
		t.Fatalf("DumpLabelErrors moved by %d, want 1", delta)
	}
}

// The counter carries the RATE; the line names the metric so the corruption can
// be reached. It is throttled because Dump runs once per scrape and the stored
// string does not repair itself — unthrottled, this is one line per series per
// scrape for the life of the process.
func TestDumpLabelParseWarningNamesTheMetricAndIsThrottled(t *testing.T) {
	log, buf := capture()
	prev := slog.Default()
	slog.SetDefault(log)
	defer slog.SetDefault(prev)

	r := NewRegistry()
	r.Counter("kubescrape_test_plain", "help").Inc()
	corrupt(r.byName["kubescrape_test_plain"], "}{", 3)

	for range 4 {
		r.Dump()
	}
	if got := r.DumpLabelErrors(); got != 4 {
		t.Fatalf("DumpLabelErrors = %d, want 4 (every skip counts)", got)
	}
	out := buf.String()
	if n := strings.Count(out, "a self-metric data point was skipped"); n != 1 {
		t.Fatalf("logged %d times, want 1 (throttle window %v)", n, dumpLabelWarnEvery)
	}
	for _, want := range []string{"level=WARN", "metric=kubescrape_test_plain", "labels=}{"} {
		if !strings.Contains(out, want) {
			t.Fatalf("log %q is missing %q", out, want)
		}
	}
}

// A corrupt label string is this process's own memory, not caller input, so it
// is safe to log — but its length is nobody's decision, so the line cuts it.
func TestCorruptLabelStringIsTruncatedInTheLog(t *testing.T) {
	long := strings.Repeat("x", 1000)
	if got := truncLabelString(long); len(got) >= len(long) || !strings.HasSuffix(got, "…") {
		t.Fatalf("truncLabelString kept %d bytes of %d", len(got), len(long))
	}
	if got := truncLabelString("{a=\"b\"}"); got != "{a=\"b\"}" {
		t.Fatalf("a short string was altered: %q", got)
	}
}

// Dump must stay non-mutating (TestDumpNonMutating's contract) even on the skip
// path: the counter is the only state it may touch.
func TestDumpSkipDoesNotDisturbTheExportPath(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("kubescrape_test_export", "help")
	c.Inc()
	corrupt(r.byName["kubescrape_test_export"], "oops", 5)
	r.Dump()

	// The good sample is still there with its value intact and still unsealed
	// for the push path.
	s := r.byName["kubescrape_test_export"]
	s.mu.Lock()
	defer s.mu.Unlock()
	var total float64
	for samp := range s.all() {
		if samp.labels == "oops" {
			continue
		}
		total += samp.value
	}
	if total != 1 {
		t.Fatalf("value after Dump = %v, want 1", total)
	}
}
