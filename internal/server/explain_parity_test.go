package server

// /v1/explain promises one thing above all: it walks the SAME decision chain
// nodeTargets walks, so the explanation cannot describe a target list the
// server does not serve. This file is that promise as a test, driven over the
// variable the two paths disagreed on — the number of matched SERVICES.
//
// Both paths sweep a monitor's endpoints once per Service selecting the pod,
// and scrape.MergeMonitorEndpoint is a FOLD. nodeTargets suppresses the repeat
// with monitorOffers; explain did not, so on the second Service a monitor whose
// chain equalled the holder's ORIGINAL chain no longer equalled the ACCUMULATED
// one, the merge counted it as a contribution, and explain reported a
// `monitors` list the served target did not have. With ONE Service the fold IS
// the union, which is why every earlier explain test agreed.
//
// The comparison is deliberately over the DERIVED targets, not over the
// document's summary of them: the divergence reaches the wire through the
// contributor list, but it starts in the merged relabel chain, which the
// document does not carry (explainPod returns both for exactly this reason).

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// summarise renders a served target the way the document does, so the two can
// be compared as the wire shows them.
func summarise(ts []kubemeta.ScrapeTarget) []explainTarget {
	out := make([]explainTarget, 0, len(ts))
	for _, t := range ts {
		out = append(out, explainTarget{URL: t.URL, Source: t.Source, Monitor: t.Monitor, Monitors: t.Monitors})
	}
	return out
}

// describe renders a target's merge-visible content: everything the fold
// touches (chain, cadence, auth) plus the identity it is keyed by.
func describe(t kubemeta.ScrapeTarget) string {
	chain := make([]string, len(t.MetricRelabelings))
	for i, r := range t.MetricRelabelings {
		chain[i] = r.Action + ":" + r.Regex
	}
	return fmt.Sprintf("url=%s source=%s monitor=%s monitors=%v chain=%v interval=%q timeout=%q auth=%q",
		t.URL, t.Source, t.Monitor, t.Monitors, chain,
		t.Interval, t.ScrapeTimeout, t.AuthSecret)
}

// assertParity compares what /v1/explain derived for the pod against what the
// node's target list serves for it, target for target.
func assertParity(t *testing.T, s *Server) {
	t.Helper()
	doc, explained := s.explainPod("default", "web-1")
	served, _ := s.nodeTargets("node1")

	if len(explained) != len(served) {
		t.Fatalf("explain derived %d targets, the node list serves %d:\nexplain %v\nserved  %v",
			len(explained), len(served), summarise(explained), summarise(served))
	}
	// nodeTargets sorts its result; explain lists in encounter order. Sort both
	// by URL — every fixture here collapses to one URL per pod, so this only
	// removes the ordering difference, never a content one.
	byURL := func(a, b kubemeta.ScrapeTarget) int { return strings.Compare(a.URL, b.URL) }
	e, sv := slices.Clone(explained), slices.Clone(served)
	slices.SortStableFunc(e, byURL)
	slices.SortStableFunc(sv, byURL)
	for i := range e {
		if got, want := describe(e[i]), describe(sv[i]); got != want {
			t.Errorf("explain and the served target list disagree about a target:\nexplain %s\nserved  %s", got, want)
		}
	}
	// And the document itself, which is what an operator reads. Sorted the same
	// way: the document lists in encounter order, the node response sorts, and
	// only the CONTENT is the promise.
	got := slices.Clone(doc.Targets)
	slices.SortStableFunc(got, func(a, b explainTarget) int { return strings.Compare(a.URL, b.URL) })
	if want := summarise(sv); !slices.EqualFunc(got, want, sameExplainTarget) {
		t.Errorf("explain document targets = %+v, served = %+v", got, want)
	}
	if len(doc.Targets) == 0 {
		t.Error("fixture explains no targets at all; it cannot show parity of anything")
	}
}

func sameExplainTarget(a, b explainTarget) bool {
	return a.URL == b.URL && a.Source == b.Source && a.Monitor == b.Monitor &&
		slices.Equal(a.Monitors, b.Monitors)
}

// The parity itself, over the Service counts that make the fold repeat, and
// over the three relations two monitors' chains can be in. EQUAL is the case
// the bug hid behind: the merge skips a chain identical to the holder's, so at
// one Service sm-b contributed nothing and was absent from `monitors` — while
// at two Services the holder's chain had already grown, the equality test
// failed, and explain listed a contributor the served target does not have.
func TestExplainAgreesWithTheServedTargetsAcrossMatchingServices(t *testing.T) {
	chains := map[string][]unionMonitor{
		"equal": {
			{"sm-a", []map[string]any{{"port": "http", "metricRelabelings": []any{rule("drop", "x_.*")}}}},
			{"sm-b", []map[string]any{{"port": "http", "metricRelabelings": []any{rule("drop", "x_.*")}}}},
		},
		"subset": {
			{"sm-a", []map[string]any{{"port": "http", "metricRelabelings": []any{rule("keep", "a_.*"), rule("drop", "b_.*")}}}},
			{"sm-b", []map[string]any{{"port": "http", "metricRelabelings": []any{rule("keep", "a_.*")}}}},
		},
		"disjoint": {
			{"sm-a", []map[string]any{{"port": "http", "metricRelabelings": []any{rule("keep", "a_.*")}}}},
			{"sm-b", []map[string]any{{"port": "http", "metricRelabelings": []any{rule("drop", "b_.*")}}}},
		},
		// Three monitors, one of them bare: a bare endpoint merges silently and
		// must stay out of the contributor list on BOTH paths.
		"with-a-bare-monitor": {
			{"sm-a", []map[string]any{{"port": "http", "metricRelabelings": []any{rule("keep", "a_.*")}}}},
			{"sm-b", []map[string]any{{"port": "http"}}},
			{"sm-c", []map[string]any{{"port": "http", "interval": "10s"}}},
		},
		// The one group a single scrape cannot honour twice. explain must not
		// serve a different credential than the node list does.
		"conflicting-auth": {
			{"sm-a", []map[string]any{{"port": "http", "bearerTokenSecret": map[string]any{"name": "tok-a", "key": "token"}}}},
			{"sm-b", []map[string]any{{"port": "http", "bearerTokenSecret": map[string]any{"name": "tok-b", "key": "token"}}}},
		},
		// One-sided auth: the holder has none, so the second monitor's group is
		// ADOPTED — a contribution, and one both paths must report identically.
		"adopted-auth": {
			{"sm-a", []map[string]any{{"port": "http"}}},
			{"sm-b", []map[string]any{{"port": "http", "bearerTokenSecret": map[string]any{"name": "tok-b", "key": "token"}}}},
		},
	}
	for name, mons := range chains {
		for _, n := range []int{1, 2, 3} {
			t.Run(fmt.Sprintf("%s/%d-services", name, n), func(t *testing.T) {
				assertParity(t, unionServer(t, n, mons))
			})
		}
	}
}

// Over HTTP, both routes as an operator meets them: the in-process assertions
// above compare what explainPod DERIVES, and this is what keeps the handler's
// projection of it honest — a future handler that re-derived its own targets
// instead of calling explainPod would pass every test above.
func TestExplainRouteAgreesWithTheTargetsRoute(t *testing.T) {
	s := unionServer(t, 2, []unionMonitor{
		{"sm-a", []map[string]any{{"port": "http", "metricRelabelings": []any{rule("keep", "a_.*"), rule("drop", "b_.*")}}}},
		{"sm-b", []map[string]any{{"port": "http", "metricRelabelings": []any{rule("keep", "a_.*")}}}},
	})
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)

	var targets struct {
		Targets []dedupTarget `json:"targets"`
	}
	getJSON(t, srv.URL+"/v1/nodes/node1/targets", http.StatusOK, &targets)
	var doc struct {
		Targets []explainTarget `json:"targets"`
	}
	getJSON(t, srv.URL+"/v1/explain/default/web-1", http.StatusOK, &doc)

	if len(targets.Targets) != 1 || len(doc.Targets) != 1 {
		t.Fatalf("want one target on each route, got %d served and %d explained", len(targets.Targets), len(doc.Targets))
	}
	served, explained := targets.Targets[0], doc.Targets[0]
	if served.URL != explained.URL || served.Source != explained.Source || served.Monitor != explained.Monitor {
		t.Errorf("explained %+v, served %+v", explained, served)
	}
	if !slices.Equal(served.Monitors, explained.Monitors) {
		t.Errorf("explained monitors = %v, served = %v", explained.Monitors, served.Monitors)
	}
}

// The contributor list is what reached the wire, so it is asserted on its own
// value rather than only against the other path: a shared bug would make both
// sides agree on the wrong answer. `equal` chains contribute nothing (the
// second monitor lost nothing), and the union of disjoint ones lists both.
func TestExplainContributorListIsTheUnionWhateverTheServiceCount(t *testing.T) {
	for _, tc := range []struct {
		name string
		mons []unionMonitor
		want []string // nil = the field must be absent
	}{
		{"equal chains contribute nothing", []unionMonitor{
			{"sm-a", []map[string]any{{"port": "http", "metricRelabelings": []any{rule("drop", "x_.*")}}}},
			{"sm-b", []map[string]any{{"port": "http", "metricRelabelings": []any{rule("drop", "x_.*")}}}},
		}, nil},
		{"disjoint chains list both", []unionMonitor{
			{"sm-a", []map[string]any{{"port": "http", "metricRelabelings": []any{rule("keep", "a_.*")}}}},
			{"sm-b", []map[string]any{{"port": "http", "metricRelabelings": []any{rule("drop", "b_.*")}}}},
		}, []string{"default/sm-a", "default/sm-b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, n := range []int{1, 2, 3} {
				doc, _ := unionServer(t, n, tc.mons).explainPod("default", "web-1")
				if len(doc.Targets) != 1 {
					t.Fatalf("%d services: want one explained target, got %+v", n, doc.Targets)
				}
				if got := doc.Targets[0].Monitors; !slices.Equal(got, tc.want) {
					t.Errorf("%d services: explained monitors = %v, want %v", n, got, tc.want)
				}
			}
		})
	}
}

// The per-endpoint verdicts must say what happened, and a repeat is not a
// merge: the second Service's offer of the SAME declaration is honoured through
// the first and folds nothing. Reporting "merged into the target already held"
// there described a fold the server does not perform.
func TestExplainReportsARepeatedOfferAsAlreadyFolded(t *testing.T) {
	doc, _ := unionServer(t, 2, []unionMonitor{
		{"sm-a", []map[string]any{{"port": "http", "metricRelabelings": []any{rule("keep", "a_.*")}}}},
		{"sm-b", []map[string]any{{"port": "http", "metricRelabelings": []any{rule("drop", "b_.*")}}}},
	}).explainPod("default", "web-1")

	if len(doc.Services) != 2 {
		t.Fatalf("want two matched Services, got %+v", doc.Services)
	}
	// First Service: sm-a claims the URL, sm-b merges into it.
	first := doc.Services[0].Monitors
	if len(first) != 2 {
		t.Fatalf("first service monitors = %+v, want two endpoint verdicts", first)
	}
	if first[0].Note != "" || !first[0].Resolved {
		t.Errorf("holder verdict = %+v, want a resolved endpoint with no note", first[0])
	}
	if want := "merged into the target already held"; !strings.Contains(first[1].Note, want) {
		t.Errorf("second monitor's verdict = %q, want it to say %q", first[1].Note, want)
	}
	// Second Service: both are repeats, and neither folds anything.
	for _, em := range doc.Services[1].Monitors {
		if !em.Resolved || em.URL == "" {
			t.Errorf("repeat verdict %+v: the endpoint still resolves through this Service", em)
		}
		if want := "already folded in through an earlier Service"; !strings.Contains(em.Note, want) {
			t.Errorf("repeat verdict note = %q, want it to say %q", em.Note, want)
		}
	}
}
