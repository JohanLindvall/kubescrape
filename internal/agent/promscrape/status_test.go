package promscrape

import (
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// The /debug/targets snapshot MERGES across cycles: per-target cadences mean
// a cycle scrapes only what was due, and a snapshot of one cycle's outcomes
// made a long-interval target's standing failure invisible nine cycles out of
// ten. A known-but-unscraped target keeps its last outcome, a
// never-yet-scraped one shows as pending, a removed one is dropped — and a
// failed target-list fetch drops nothing.
func TestScrapeStatusMergesAcrossCycles(t *testing.T) {
	s := New(Config{
		Node: "n", Interval: time.Hour, Targets: staticTargets{},
		Exporter: &captureExporter{}, StartTime: time.Now(),
	})
	a := testTarget("http://a:1/metrics")
	b := testTarget("http://b:1/metrics")
	t0 := time.Now()

	// Cycle 1: a scraped ok, b discovered but not due yet.
	s.publishStatus([]scrapeOutcome{
		{pipeline: "targets", url: a.URL, target: &a, ok: true, samples: 3},
		{pipeline: "cadvisor", url: "https://kubelet/metrics/cadvisor", ok: true},
	}, []kubemeta.ScrapeTarget{a, b}, true, t0)
	st := s.Status()
	if len(st.Targets) != 3 {
		t.Fatalf("cycle 1: %d targets, want 3 (a, pending b, cadvisor)", len(st.Targets))
	}
	if st.Targets[0].URL != b.URL || !st.Targets[0].Pending {
		t.Fatalf("pending target must sort first among non-failures: %+v", st.Targets[0])
	}

	// Cycle 2: nothing due. Both targets and the kubelet entry carry, with
	// their original scrape times.
	s.publishStatus(nil, []kubemeta.ScrapeTarget{a, b}, true, t0.Add(time.Minute))
	st = s.Status()
	if len(st.Targets) != 3 {
		t.Fatalf("cycle 2: %d targets, want 3 carried", len(st.Targets))
	}
	for _, ts := range st.Targets {
		if ts.URL == a.URL && (!ts.Scraped.Equal(t0) || ts.Samples != 3) {
			t.Fatalf("carried outcome must keep its scrape time and result: %+v", ts)
		}
	}

	// Cycle 3: the target list could not be fetched — nothing is dropped.
	s.publishStatus(nil, nil, false, t0.Add(2*time.Minute))
	if st = s.Status(); len(st.Targets) != 3 {
		t.Fatalf("cycle 3 (fetch failed): %d targets, want 3 kept", len(st.Targets))
	}

	// Cycle 4: b is gone from a SUCCESSFUL listing — dropped; the kubelet
	// entry (not part of the target list) stays.
	s.publishStatus(nil, []kubemeta.ScrapeTarget{a}, true, t0.Add(3*time.Minute))
	st = s.Status()
	if len(st.Targets) != 2 {
		t.Fatalf("cycle 4: %d targets, want 2 (a + cadvisor)", len(st.Targets))
	}
	for _, ts := range st.Targets {
		if ts.URL == b.URL {
			t.Fatalf("removed target still present: %+v", ts)
		}
	}
}

// Two targets can share a URL (two hostNetwork pods on the node's IP with the
// same annotated port — the exact case scheduleKey exists for), so the status
// merge must key by the same identity: keyed by pipeline|URL, the two shared
// one row and alternately masked each other's standing failure on the debug
// page.
func TestScrapeStatusKeepsBothSameURLTargets(t *testing.T) {
	s := New(Config{
		Node: "n", Interval: time.Hour, Targets: staticTargets{},
		Exporter: &captureExporter{}, StartTime: time.Now(),
	})
	a := testTarget("http://10.0.0.1:9090/metrics")
	a.Pod.Name, a.Pod.UID = "pod-a", "uid-a"
	b := testTarget("http://10.0.0.1:9090/metrics")
	b.Pod.Name, b.Pod.UID = "pod-b", "uid-b"
	t0 := time.Now()

	// Cycle 1: both scraped — a ok, b with a standing failure.
	s.publishStatus([]scrapeOutcome{
		{pipeline: "targets", url: a.URL, target: &a, ok: true, samples: 3},
		{pipeline: "targets", url: b.URL, target: &b, ok: false, err: "connection refused"},
	}, []kubemeta.ScrapeTarget{a, b}, true, t0)
	st := s.Status()
	if len(st.Targets) != 2 {
		t.Fatalf("cycle 1: %d rows, want 2: same-URL targets must not share a status slot", len(st.Targets))
	}

	// Cycle 2: only a is due and scraped. b's failing row must carry, not be
	// masked by a's fresh success on the shared URL.
	s.publishStatus([]scrapeOutcome{
		{pipeline: "targets", url: a.URL, target: &a, ok: true, samples: 3},
	}, []kubemeta.ScrapeTarget{a, b}, true, t0.Add(time.Minute))
	st = s.Status()
	if len(st.Targets) != 2 {
		t.Fatalf("cycle 2: %d rows, want 2 (a's outcome + b's carried failure)", len(st.Targets))
	}
	var sawA, sawB bool
	for _, ts := range st.Targets {
		switch ts.Pod {
		case "pod-a":
			sawA = true
			if !ts.Up || !ts.Scraped.Equal(t0.Add(time.Minute)) {
				t.Fatalf("pod-a row: %+v, want this cycle's success", ts)
			}
		case "pod-b":
			sawB = true
			if ts.Up || ts.Error != "connection refused" || !ts.Scraped.Equal(t0) {
				t.Fatalf("pod-b row: %+v, want its cycle-1 failure carried", ts)
			}
		}
	}
	if !sawA || !sawB {
		t.Fatalf("rows = %+v, want one per pod", st.Targets)
	}
}
