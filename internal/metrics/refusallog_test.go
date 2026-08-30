package metrics

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// syncBuf collects log output that may be written from a background goroutine
// while the test reads it.
type syncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func (s *syncBuf) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf.Reset()
}

// capture builds a Debug-level logfmt logger over a buffer.
func capture() (*slog.Logger, *syncBuf) {
	buf := &syncBuf{}
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})), buf
}

// A NaN or Inf observation is refused, and used to be refused SILENTLY apart
// from a process-wide counter — so "kubescrape_log_metrics_dropped_nan_total is
// climbing" arrived with no way to reach the rule whose value extraction was
// producing garbage.
func TestNonFiniteObservationNamesTheMetric(t *testing.T) {
	t0 := int64(1_700_600_000)
	setTimeForTest(time.Unix(t0, 0))
	defer testEpoch.Store(0)

	log, buf := capture()
	s := newTestSeries(seriesSpec{name: "latency", kind: kindCounter, expiration: time.Hour, log: log})
	res := pcommon.NewMap()
	for _, v := range []float64{math.NaN(), math.Inf(1)} {
		s.observe(labels{}.set("path", "/a"), v, resourceAccum(res), res, nil)
	}
	if got := s.drops.NaN(); got != 2 {
		t.Fatalf("dropped = %d, want 2", got)
	}
	line := buf.String()
	if !strings.Contains(line, "level=WARN") || !strings.Contains(line, "metric=latency") {
		t.Errorf("want a WARN naming the metric, got:\n%s", line)
	}
	if !strings.Contains(line, "value=NaN") {
		t.Errorf("want the offending value on the line, got:\n%s", line)
	}
	// Hourly, so a log full of bad lines cannot become a log full of warnings.
	if n := strings.Count(line, "level=WARN"); n != 1 {
		t.Errorf("WARN lines = %d, want 1 (the second refusal is inside the hour):\n%s", n, line)
	}
}

// The cardinality cap DROPS observations and frees slots only through
// idleness, so it is a Warn, not the Info it used to be — Info is reserved for
// lifecycle an operator reads without asking.
func TestCardinalityCapWarns(t *testing.T) {
	setTimeForTest(time.Unix(1_700_600_000, 0))
	defer testEpoch.Store(0)

	log, buf := capture()
	s := newTestSeries(seriesSpec{name: "c", kind: kindCounter, maxSize: 1, expiration: time.Hour, log: log})
	res := pcommon.NewMap()
	for _, p := range []string{"/a", "/b"} {
		s.observe(labels{}.set("path", p), 1, resourceAccum(res), res, nil)
	}
	line := buf.String()
	if !strings.Contains(line, "level=WARN") {
		t.Errorf("a refusal is a WARN: %s", line)
	}
	if !strings.Contains(line, "maxSeries=1") {
		t.Errorf("the cap must be on the line under a logfmt-safe key: %s", line)
	}
}

// A failed export used to warn once per interval per node with no counterpart,
// so an operator watching the lines stop could not tell a recovery from a
// process that had stopped exporting at all.
func TestExportFailureTransitionsAndRecovers(t *testing.T) {
	setTimeForTest(time.Unix(1_700_600_000, 0))
	defer testEpoch.Store(0)

	log, buf := capture()
	set, err := newTestSet([]Dynamic{{Name: "c", Type: CounterType, Value: "1", Match: []string{"m=1"}}}, WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		set.noteExport(errors.New("collector unavailable"))
	}
	out := buf.String()
	if n := strings.Count(out, "level=WARN"); n != 1 {
		t.Errorf("WARN lines = %d, want 1 (the repeats are Debug until the re-warn interval):\n%s", n, out)
	}
	if !strings.Contains(out, "attempts=3") {
		t.Errorf("the repeats must carry the cost so far:\n%s", out)
	}

	buf.Reset()
	set.noteExport(nil)
	out = buf.String()
	if !strings.Contains(out, "level=INFO") || !strings.Contains(out, "succeeded again") {
		t.Errorf("want a recovery line naming the outage:\n%s", out)
	}
	if !strings.Contains(out, "attempts=3") {
		t.Errorf("the recovery must say what the outage cost:\n%s", out)
	}
}

// A zero interval is a legal configuration that observes every line and
// exports nothing — indistinguishable, from outside, from a working one.
func TestZeroExportIntervalSaysSo(t *testing.T) {
	log, buf := capture()
	set, err := newTestSet([]Dynamic{{Name: "c", Type: CounterType, Value: "1", Match: []string{"m=1"}}}, WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); set.Run(ctx, failingExporter{}, 0, 0) }()
	// Run blocks on ctx; the line is written before it blocks.
	deadline := time.Now().Add(30 * time.Second)
	for !strings.Contains(buf.String(), "never exported") && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	out := buf.String()
	if !strings.Contains(out, "never exported") || !strings.Contains(out, "flag=-logs-metrics-interval") {
		t.Errorf("want a line naming the consequence and the flag:\n%s", out)
	}
}

// The re-offer buffer filling is REAL LOSS — the retention is what makes a
// failed export at-least-once, so an evicted resource is observations no export
// will ever carry. It was counted and never described, and the two bounds
// (resources, samples) bind for different reasons and are tuned separately, so
// the line has to say which one bound.
func TestRetentionEvictionIsDescribed(t *testing.T) {
	log, buf := capture()
	set := &DynamicMetricSet{log: log}

	t1 := time.Now().Add(-time.Minute)
	gen := func(ts time.Time, n int) seriesSamples {
		return seriesSamples{samples: make([]sample, n), ts: ts}
	}
	set.retain(map[string][]seriesSamples{
		"live":  {gen(t1.Add(30*time.Second), 15_000)},
		"quiet": {gen(t1, 40_000)},
	}, []string{"live", "quiet"})

	if set.DroppedUndelivered() == 0 {
		t.Fatal("nothing was evicted; the fixture no longer exceeds the sample cap")
	}
	out := buf.String()
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "observations are lost") {
		t.Errorf("want a WARN that says the observations are lost:\n%s", out)
	}
	if !strings.Contains(out, "resource=quiet") {
		t.Errorf("the line must name the evicted resource:\n%s", out)
	}
	if !strings.Contains(out, "maxSamples=") || !strings.Contains(out, "maxResources=") {
		t.Errorf("the line must quote BOTH bounds, since only one of them bound:\n%s", out)
	}
}
