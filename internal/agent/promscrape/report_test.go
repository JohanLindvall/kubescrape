package promscrape

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// The empty target list is the most common first-run failure and it moves no
// other counter at all: no scrape runs, so no scrape fails.
func TestEmptyTargetListWarnsOnceAndRecovers(t *testing.T) {
	var buf strings.Builder
	s := debugScraper(Config{Node: "node1", Exporter: &captureExporter{}, Targets: staticTargets{}}, &buf)

	s.reportTargetSet(nil)
	if obs.ScrapeTargets.Value() != 0 {
		t.Errorf("gauge = %v, want 0", obs.ScrapeTargets.Value())
	}
	if n := strings.Count(buf.String(), "NO scrape targets"); n != 1 {
		t.Fatalf("warned %d times on the transition, want 1; log %q", n, buf.String())
	}
	s.reportTargetSet(nil)
	if n := strings.Count(buf.String(), "NO scrape targets"); n != 1 {
		t.Errorf("warned %d times, want 1 (the condition is unchanged)", n)
	}
	if !strings.Contains(buf.String(), "note=") {
		t.Error("the warning carries no remediation hint")
	}

	s.reportTargetSet([]kubemeta.ScrapeTarget{testTarget("http://a:1")})
	if !strings.Contains(buf.String(), "scrape targets discovered") {
		t.Errorf("the recovery was not reported; log %q", buf.String())
	}
	if obs.ScrapeTargets.Value() != 1 {
		t.Errorf("gauge = %v, want 1", obs.ScrapeTargets.Value())
	}
	// A SECOND outage says so again: the throttle is armed by the transition,
	// not by the clock, or an incident an hour after the last one is silent.
	s.reportTargetSet(nil)
	if n := strings.Count(buf.String(), "NO scrape targets"); n != 2 {
		t.Errorf("warned %d times, want 2 (a fresh outage re-warns)", n)
	}
}

// A failed FETCH must not be read as "this node has no targets": it has its own
// Error line, and blaming discovery for a metadata-service outage sends an
// operator to the wrong place.
func TestAFailedTargetFetchIsNotAnEmptyTargetSet(t *testing.T) {
	var buf strings.Builder
	s := debugScraper(Config{
		Node: "node1", Interval: time.Hour, Exporter: &captureExporter{},
		Targets: failingTargets{},
	}, &buf)
	s.cycle(context.Background())
	if strings.Contains(buf.String(), "NO scrape targets") {
		t.Errorf("a failed fetch was reported as an empty target set: %q", buf.String())
	}
	if !strings.Contains(buf.String(), "fetching scrape targets") {
		t.Errorf("the fetch failure was not reported: %q", buf.String())
	}
}

// A target dropped by the transforms file's hook is indistinguishable from one
// discovery never returned — same empty list, same silence — so the hook says
// what it took.
func TestTargetHookDropsAreReported(t *testing.T) {
	var buf strings.Builder
	s := debugScraper(Config{
		Node: "node1", Interval: time.Hour, Exporter: &captureExporter{},
		Targets:    staticTargets{testTarget("http://a:1"), testTarget("http://b:2")},
		TargetHook: func([]kubemeta.ScrapeTarget) []kubemeta.ScrapeTarget { return nil },
	}, &buf)
	s.cycle(context.Background())
	got := buf.String()
	if !strings.Contains(got, "targets: hook changed the target list") || !strings.Contains(got, "dropped=2") {
		t.Errorf("the hook's drops were not reported; log %q", got)
	}
}

// The by-source census is most of the diagnosis when the list is not what an
// operator expected: three different mechanisms produce targets.
func TestTargetSourcesCensus(t *testing.T) {
	pod := testTarget("http://a:1")
	pod.Source = "pod"
	mon := testTarget("http://b:2")
	mon.Source = "servicemonitor"
	bare := testTarget("http://c:3")
	if got := targetSources([]kubemeta.ScrapeTarget{mon, pod, bare}); got != "pod=1,servicemonitor=1,unknown=1" {
		t.Errorf("targetSources = %q", got)
	}
}
