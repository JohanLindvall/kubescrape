package metaclient

// reportCache runs once per lookup on the concurrent ingest and cadvisor paths,
// so WHICH of its two gates is consulted first is a hot-path decision. It used
// to consult the throttle first, on the stated grounds that the throttle is one
// atomic while Logger.Enabled is an interface call — but throttle.allow reads
// the CLOCK before it touches the atomic, and a clock read is ~7x an Enabled
// call. The order is now Enabled-first, and these pin both halves of that: the
// summary still appears at most once per interval, and no throttle slot is
// spent while Debug is off.

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// Raising the level mid-incident must produce a summary on the NEXT lookup.
//
// With the throttle consulted first, every lookup taken while Debug was off
// claimed the slot, so a level raised inside the first minute of a process was
// answered with silence for up to a full interval — exactly when an operator is
// looking. This is the behaviour the old ordering apologised for in a comment,
// and it is now a test rather than an accepted cost.
func TestCacheSummaryAppearsAsSoonAsDebugIsRaised(t *testing.T) {
	srv, _ := cachingServer(t, `"v1"`, `{"name":"pod1","namespace":"ns1"}`)

	level := &slog.LevelVar{}
	level.Set(slog.LevelInfo)
	buf := &syncBuf{}
	c := New(Config{Base: srv.URL, Timeout: time.Second,
		Log: slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: level}))})

	// A burst of lookups while Debug is off. Every one of these used to spend
	// the throttle slot for a line nobody could see.
	for range 50 {
		if _, err := c.PodByName(context.Background(), "ns1", "pod1"); err != nil {
			t.Fatal(err)
		}
	}
	if buf.String() != "" {
		t.Fatalf("nothing may be logged above Debug:\n%s", buf.String())
	}

	level.Set(slog.LevelDebug)
	if _, err := c.PodByName(context.Background(), "ns1", "pod1"); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(buf.String(), "metadata cache"); n != 1 {
		t.Errorf("cache summaries on the first lookup after raising the level = %d, want 1.\n"+
			"A throttle consulted before Logger.Enabled spends its slot while Debug is off, "+
			"which delays this by up to %s:\n%s", n, cacheReportInterval, buf.String())
	}
}

// The tallies are CUMULATIVE, as the summary's own comment says: they count
// every lookup, including the ones taken before anyone was listening. Skipping
// the counters when Debug is off would be a cheaper hot path and a different,
// undocumented statistic.
func TestCacheSummaryTalliesAreCumulativeAcrossTheLevelChange(t *testing.T) {
	srv, _ := cachingServer(t, `"v1"`, `{"name":"pod1","namespace":"ns1"}`)

	level := &slog.LevelVar{}
	level.Set(slog.LevelInfo)
	buf := &syncBuf{}
	c := New(Config{Base: srv.URL, Timeout: time.Second,
		Log: slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: level}))})

	// One fetch plus nine cache hits, all invisible.
	for range 10 {
		if _, err := c.PodByName(context.Background(), "ns1", "pod1"); err != nil {
			t.Fatal(err)
		}
	}
	level.Set(slog.LevelDebug)
	if _, err := c.PodByName(context.Background(), "ns1", "pod1"); err != nil {
		t.Fatal(err)
	}
	// 11 lookups: 1 fetch and 10 cache hits.
	if got := c.hits.Load(); got != 10 {
		t.Errorf("hits = %d, want 10 (every lookup counts, not only the observed ones)", got)
	}
	if got := c.fetched.Load(); got != 1 {
		t.Errorf("fetched = %d, want 1", got)
	}
	if out := buf.String(); !strings.Contains(out, "hits=10") {
		t.Errorf("the summary must carry the pre-Debug tallies:\n%s", out)
	}
}
