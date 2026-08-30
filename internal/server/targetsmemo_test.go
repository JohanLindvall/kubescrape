package server

// Ceilings for the repeated work on GET /v1/nodes/{node}/targets.
//
// The route is re-derived by every agent in the fleet on every scrape cycle, so
// anything it recomputes is multiplied by node count. These pin the two pieces
// that were made to stop recomputing — the Services snapshot and the served
// list's order — as ceilings rather than as benchmark prose, because a win with
// no test defending it is undone by the next campaign.

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

// The Services snapshot is a pure function of the Services index, and the index
// publishes a change token — so a second request that changes nothing must not
// take the index lock at all. Index.Reads counts LOCKED reads, which is exactly
// the question.
//
// Before the memo this was one locked read, one copy of every Service in every
// namespace on the node and one sort per namespace, PER REQUEST: measured on a
// node spanning 20 namespaces of 200 Services at 1.09 ms, 38 KB and 64
// allocations, every byte of it identical to the previous request's.
func TestNodeTargetsSnapshotsTheServicesIndexOnlyWhenItChanges(t *testing.T) {
	f := targetsFixture{pods: 40, services: 20}
	s := f.build(t)
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	url := srv.URL + "/v1/nodes/node1/targets"

	getJSON(t, url, http.StatusOK, nil) // fills the memo
	before := s.services.Reads()
	for range 10 {
		getJSON(t, url, http.StatusOK, nil)
	}
	if got := s.services.Reads() - before; got != 0 {
		t.Errorf("locked Services-index reads across 10 unchanged requests = %d, want 0.\n"+
			"The snapshot is memoised against services.Index.Generation; if this is nonzero the "+
			"memo has stopped hitting and every agent in the fleet is paying the copy and the sort "+
			"again on every scrape cycle.", got)
	}
}

// …and it must still see a change. A memo that never invalidates serves a
// deleted Service as a live scrape target.
func TestNodeTargetsSnapshotIsRetakenAfterAServiceChanges(t *testing.T) {
	f := targetsFixture{pods: 5, services: 3}
	s := f.build(t)
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	url := srv.URL + "/v1/nodes/node1/targets"

	getJSON(t, url, http.StatusOK, nil)
	before := s.services.Reads()
	// Any real change to the index bumps its token.
	s.services.Delete("prod", "svc-uid-0")
	getJSON(t, url, http.StatusOK, nil)
	if got := s.services.Reads() - before; got == 0 {
		t.Error("the Services index was not re-read after a Delete: the memo is not invalidating")
	}
}

// The served order is produced by permuting an index rather than by moving
// 616-byte ScrapeTargets through a comparison sort. sort.Slice needs a reflect
// swapper, which is three allocations per call; the permutation needs one
// int32 slice. This pins the difference so a "simplification" back to sort.Slice
// is caught by a build rather than by a profile two campaigns later.
func TestSortTargetsDoesNotAllocateAReflectSwapper(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	f := fleetFixture{nodes: 4, podsPerNode: 110, namespaces: 4, otherServices: 5}
	s := f.build(t)
	targets, _ := s.nodeTargets("node-1")
	if len(targets) < 100 {
		t.Fatalf("fixture produced %d targets, want a sortable list", len(targets))
	}
	// One allocation: the int32 permutation. sort.Slice was three (the reflect
	// swapper closure and its scratch), and every one of its ~n log n swaps was
	// a write-barriered 616-byte typedmemmove besides.
	const budget = 1
	if got := testing.AllocsPerRun(100, func() { sortTargets(targets) }); got > budget {
		t.Errorf("sortTargets allocations = %v, want <= %d.\n"+
			"A comparison sort over the targets themselves reintroduces the reflect swapper "+
			"AND moves the pod document once per swap.", got, budget)
	}
}

// THE MARGINAL COST OF ONE MORE MONITOR RESOLVING TO A URL A POD ALREADY HOLDS.
//
// This is the shape a tenant can mint without limit — N ServiceMonitors that
// all resolve to one URL on one pod — so it is exactly the request the metadata
// service must answer most cheaply, and three separate things were making it
// dearer per monitor:
//
//   - the contributor-ceiling report built its dedupe key (`kind + "\x00" +
//     url`) and took the dedupe table's mutex once per REFUSED monitor, so the
//     guard allocated in proportion to the pile-up it exists to bound
//     (+68 allocations at N=100, exactly N − scrape.MaxContributorsPerTarget);
//   - the URL pre-check — the whole point of which is that a colliding monitor
//     costs "the URL resolution and nothing else" — resolved that URL in THREE
//     allocations (net.JoinHostPort, strconv.Itoa, the concatenation);
//   - the cadence merge re-parsed both interval strings through a REGEXP even
//     when they were spelled identically, which is what two charts' monitors
//     on one endpoint normally are.
//
// The endpoints here declare the SAME interval, which is the ordinary
// collision. What is left per monitor is the URL string and nothing else.
func TestCollidingMonitorsCostOneAllocationEachToRefuse(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	allocsAt := func(n int) float64 {
		mons := make([]unionMonitor, 0, n)
		for i := range n {
			// Mergeable (an interval is a cadence field, so the endpoint is not
			// BARE and does reach the contributor list) but identical, which is
			// two charts declaring the same scrape.
			mons = append(mons, unionMonitor{
				name:      fmt.Sprintf("sm-%03d", i),
				endpoints: []map[string]any{{"port": "http", "interval": "30s"}},
			})
		}
		s := unionServer(t, 1, mons)
		s.nodeTargets("node1") // warm the monitor→Service and Services memos
		return testing.AllocsPerRun(20, func() { targetSink, _ = s.nodeTargets("node1") })
	}
	a50, a100 := allocsAt(50), allocsAt(100)
	marginal := (a100 - a50) / 50
	// One allocation is the rendered URL, which is irreducible without a
	// per-derivation URL memo. The slack absorbs amortised map growth in the
	// offer dedup and the contributor list.
	const budget = 2.0
	t.Logf("allocations for one node's targets: %.0f at 50 colliding monitors, %.0f at 100 "+
		"(%.2f per monitor; it was 7.04 before the three fixes above)", a50, a100, marginal)
	if marginal > budget {
		t.Errorf("allocations per colliding monitor = %.2f (%.0f at 50, %.0f at 100), want <= %.1f.\n"+
			"Something on the merge path is allocating per REFUSED monitor again — the ceiling's "+
			"dedupe key, a multi-allocation URL render, or a re-parse of an interval both sides spell "+
			"the same way.", marginal, a50, a100, budget)
	}
}
