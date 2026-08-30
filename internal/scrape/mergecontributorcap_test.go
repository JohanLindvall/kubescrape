package scrape

import (
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// THE ATTACK, at the seam it is spent against: N monitors that all resolve to
// one URL on one pod and each carry nothing but a finer `interval`.
//
// A contribution is that cheap — no metricRelabelings at all, so neither
// MaxRelabelChainRules nor MaxRelabelChainBytes is approached, and no target is
// added, so MaxPortsPerPod never sees it — and every contribution used to
// append a name to the served target's `monitors` list after scanning that list
// for it: O(n) bytes marshalled into the node document for every POD, O(n²)
// CPU, in the singleton that serves every agent's poll.
func TestContributorListStopsAtTheCeiling(t *testing.T) {
	const monitors = 2000
	// A realistic name; Kubernetes permits a good deal more.
	name := func(i int) string {
		return "default/sm-" + strconv.Itoa(1000000+i) + "-" + strings.Repeat("a", 40)
	}
	held := mergeHeld(name(0), servicemonitors.Endpoint{Interval: strconv.Itoa(monitors) + "s"})
	refused := 0
	for i := 1; i < monitors; i++ {
		// Strictly finer each time, so every one of them CONTRIBUTES: an
		// endpoint that merges nothing is never a contributor and would prove
		// nothing here.
		rep := MergeMonitorEndpoint(&held, name(i), &servicemonitors.Endpoint{
			Interval: strconv.Itoa(monitors-i) + "s",
		})
		if rep.ContributorsCapped {
			refused++
		}
		if len(held.Monitors) > MaxContributorsPerTarget {
			t.Fatalf("after %d merges the contributor list is %d long; the ceiling is %d",
				i, len(held.Monitors), MaxContributorsPerTarget)
		}
	}
	if len(held.Monitors) != MaxContributorsPerTarget {
		t.Fatalf("contributor list is %d long, want exactly the ceiling %d", len(held.Monitors), MaxContributorsPerTarget)
	}
	// Every refusal is REPORTED. Attribution is invisible in the data — the
	// scrape is unchanged and the series are identical — so an unreported drop
	// is a question /v1/explain and the counter could never answer.
	if want := monitors - MaxContributorsPerTarget; refused != want {
		t.Errorf("%d merges reported ContributorsCapped, want %d (one per monitor past the ceiling)", refused, want)
	}
	// The list that IS served is the holder plus the first contributors in
	// encounter order: a prefix, so the served document and its ETag do not
	// depend on which monitor arrived when.
	for i, m := range held.Monitors {
		if m != name(i) {
			t.Fatalf("contributor %d is %q, want %q (the list must be the encounter-order prefix)", i, m, name(i))
		}
	}
	// The bound that actually matters is the marshalled document.
	doc, err := json.Marshal(held)
	if err != nil {
		t.Fatal(err)
	}
	if len(doc) > 8<<10 {
		t.Errorf("one merged target marshals to %d bytes after %d monitors", len(doc), monitors)
	}
	t.Logf("%d monitors -> %d contributors, %d byte target", monitors, len(held.Monitors), len(doc))
}

// What the ceiling refuses is ATTRIBUTION, never CONFIGURATION: refusing the
// merge would change what is scraped in order to bound a diagnostic. Every
// documented merge rule still holds for a monitor past the ceiling.
func TestContributorCeilingRefusesAttributionNotConfiguration(t *testing.T) {
	held := mergeHeld("ns/holder", servicemonitors.Endpoint{Interval: "60s"})
	for i := range MaxContributorsPerTarget * 2 {
		MergeMonitorEndpoint(&held, "ns/filler-"+strconv.Itoa(i), &servicemonitors.Endpoint{
			Interval: strconv.Itoa(59-i%50) + "s",
		})
	}
	if len(held.Monitors) != MaxContributorsPerTarget {
		t.Fatalf("setup: contributor list is %d long, want the ceiling %d", len(held.Monitors), MaxContributorsPerTarget)
	}
	if slices.Contains(held.Monitors, "ns/late") {
		t.Fatal("setup: the late monitor is already listed")
	}

	// One-sided auth is still adopted whole, a finer interval still wins with
	// its own timeout, and the relabel chain still concatenates — all from a
	// monitor the list has no room to name.
	rep := MergeMonitorEndpoint(&held, "ns/late", &servicemonitors.Endpoint{
		Interval: "1s", ScrapeTimeout: "1s",
		BearerSecret:      "ns/tok/token",
		MetricRelabelings: []kubemeta.RelabelRule{keepRule},
	})
	if !rep.ContributorsCapped {
		t.Error("a contribution past the ceiling was not reported")
	}
	if !rep.AuthAdopted || held.AuthSecret != "ns/tok/token" {
		t.Errorf("one-sided auth was not adopted past the ceiling: adopted=%v secret=%q", rep.AuthAdopted, held.AuthSecret)
	}
	if held.Interval != "1s" || held.ScrapeTimeout != "1s" {
		t.Errorf("the finer cadence did not win past the ceiling: interval=%q timeout=%q", held.Interval, held.ScrapeTimeout)
	}
	if n := len(held.MetricRelabelings); n != 1 || held.MetricRelabelings[0].Regex != keepRule.Regex {
		t.Errorf("the relabel chain did not concatenate past the ceiling: %+v", held.MetricRelabelings)
	}
	if slices.Contains(held.Monitors, "ns/late") {
		t.Errorf("the refused monitor was listed anyway: %v", held.Monitors)
	}
}

// A monitor ALREADY on the list contributing again — its own second endpoint
// resolving to the same URL — is not a refusal: it contributed and it is on the
// wire. Reporting it would move the counter and warn about a monitor that lost
// nothing, on every targets request of every agent holding the pod.
func TestListedContributorIsNotRefusedByTheCeiling(t *testing.T) {
	held := mergeHeld("ns/holder", servicemonitors.Endpoint{Interval: "9000s"})
	for i := range MaxContributorsPerTarget * 2 {
		MergeMonitorEndpoint(&held, "ns/m-"+strconv.Itoa(i), &servicemonitors.Endpoint{
			Interval: strconv.Itoa(8999-i) + "s",
		})
	}
	if len(held.Monitors) != MaxContributorsPerTarget {
		t.Fatalf("setup: contributor list is %d long, want the ceiling %d", len(held.Monitors), MaxContributorsPerTarget)
	}
	listed := held.Monitors[MaxContributorsPerTarget-1]
	if rep := MergeMonitorEndpoint(&held, listed, &servicemonitors.Endpoint{Interval: "1s"}); rep.ContributorsCapped {
		t.Errorf("%q is on the contributor list and lost nothing, but its second endpoint was reported refused", listed)
	}
	// And the holder's own second endpoint stays the non-contributor it always
	// was: it is what Monitor already names.
	if rep := MergeMonitorEndpoint(&held, "ns/holder", &servicemonitors.Endpoint{Interval: "500ms"}); rep.ContributorsCapped {
		t.Error("the URL holder's own second endpoint was reported refused by the contributor ceiling")
	}
}
