package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/scrape"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// contributorBombMonitors indexes n cluster-wide ServiceMonitors that all
// resolve to ONE URL on the fixture's pod and all CONTRIBUTE to the merged
// target, in the given upsert order.
//
// Each carries a strictly finer `interval` than the one before it in ENCOUNTER
// order (the index's (namespace, name) order, which is what the merge folds
// in) and nothing else: no metricRelabelings, no auth. That is the whole
// attack — a contribution is free — and it is why neither
// scrape.MaxRelabelChainRules nor the parse-time per-endpoint bound is reached
// by it.
func contributorBombMonitors(t *testing.T, n int, upsert []int) *servicemonitors.Index {
	t.Helper()
	monitors := servicemonitors.NewIndex()
	for _, i := range upsert {
		if err := monitors.Upsert(&unstructured.Unstructured{Object: map[string]any{
			"metadata": map[string]any{"name": contributorBombName(i), "namespace": "default"},
			"spec": map[string]any{
				"selector": map[string]any{"matchLabels": map[string]any{"team": "obs"}},
				"endpoints": []any{map[string]any{
					"port":     "http",
					"interval": strconv.Itoa(n-i) + "s",
				}},
			},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	return monitors
}

// contributorBombName is zero-padded so the index's lexical (namespace, name)
// order — the encounter order the merge folds in — is the numeric one, and long
// enough to be a realistic monitor name rather than a best case for the attack
// (Kubernetes permits 253 characters plus the namespace).
func contributorBombName(i int) string {
	return "sm-" + strconv.Itoa(1000000+i) + "-" + strings.Repeat("a", 40)
}

func forwardOrder(n int) []int {
	order := make([]int, 0, n)
	for i := range n {
		order = append(order, i)
	}
	return order
}

// THE ATTACK: a tenant with edit rights in ONE namespace creates N
// ServiceMonitors — `selector: {}` + `namespaceSelector.any: true` in the real
// thing, a matching label here — that all resolve to the same URL on the same
// pod, each carrying nothing but a finer `interval`.
//
// Every one of them MERGES into the single served target (a kubescrape target's
// exported identity has no monitor component, so the URL is scraped once), and
// each merge appended its monitor's name to that target's contributor list
// after scanning the list for it: O(n) bytes on the wire per POD and O(n²) CPU,
// with no relabel rule anywhere near either of the merge's chain bounds.
// Measured against the real derivation before the ceiling: 2,000 monitors put
// 2,000 names and ~122 KiB into ONE target (~622 KiB at the name length
// Kubernetes permits) in ~0.07 s — multiplied by the pods on the node,
// re-derived and re-marshalled on every agent poll, in the singleton the chart
// requests 128Mi for with no memory limit. scrape.MaxPortsPerPod cannot see it:
// no target is added.
func TestCollidingMonitorsCannotInflateOneTargetsContributorList(t *testing.T) {
	const monitorCount = 2000
	st, svcs := relabelBombFixture(t, 1)
	monitors := contributorBombMonitors(t, monitorCount, forwardOrder(monitorCount))

	before := obs.MonitorContributorsCapped.WithLabelValues("servicemonitor").Value()
	body, targets := fetchTargets(t, st, svcs, monitors)
	if len(targets) != 1 {
		t.Fatalf("got %d targets, want 1 (every monitor resolves to the same URL)", len(targets))
	}
	t.Logf("%d monitors -> %d contributors, %d byte response", monitorCount, len(targets[0].Monitors), len(body))
	if n := len(targets[0].Monitors); n > scrape.MaxContributorsPerTarget {
		t.Errorf("served target lists %d contributing monitors; the per-target ceiling is %d",
			n, scrape.MaxContributorsPerTarget)
	}
	// One pod document is ~1 KiB here and the bounded list a few KiB more, so
	// anything under 32 KiB is the list being bounded rather than accumulated.
	if len(body) > 32<<10 {
		t.Errorf("node targets document is %d bytes for ONE pod; a tenant-created monitor pile-up must not "+
			"multiply into it", len(body))
	}
	// Fail CLOSED and DIAGNOSABLE. What the ceiling refuses is ATTRIBUTION, and
	// attribution is exactly the thing no consumer of the document could
	// otherwise miss: the counter and the warning are the only report there is.
	if got := obs.MonitorContributorsCapped.WithLabelValues("servicemonitor").Value() - before; got == 0 {
		t.Error("the contributor list was capped without moving kubescrape_monitor_contributors_capped_total")
	}
	// And /v1/explain — the operator's "why is my monitor not contributing?" —
	// has to answer, since the served target no longer names the monitor at all.
	// It answers PER URL now, not per endpoint: the document is bounded too
	// (explainbomb_test.go), so it cannot name all 2,000 monitors — but naming
	// none of them and looking complete is the failure a cap introduces, which
	// is why the ceiling wording, the count it refused, and the number of
	// endpoint verdicts left out are all in there.
	doc := fetchExplain(t, st, svcs, monitors, "default", "web-0")
	if !strings.Contains(doc, "per-target ceiling of") || !strings.Contains(doc, "contributors") {
		t.Errorf("/v1/explain does not report the capped contributor list: %s", truncateDoc(doc))
	}
	var parsed explainDoc
	if err := json.Unmarshal([]byte(doc), &parsed); err != nil {
		t.Fatal(err)
	}
	if len(parsed.MergeCeilings) != 1 ||
		parsed.MergeCeilings[0].ContributorsCapped != monitorCount-scrape.MaxContributorsPerTarget {
		t.Errorf("/v1/explain does not say how many monitors the contributor ceiling refused: %+v", parsed.MergeCeilings)
	}
	if len(parsed.Services) != 1 || parsed.Services[0].MonitorsNotShown == 0 {
		t.Errorf("/v1/explain truncated its monitor list without saying so: %s", truncateDoc(doc))
	}
}

// The ceiling must not change WHAT IS SCRAPED, and it must not make the
// response depend on the order the monitors happened to be indexed in: the
// document is hashed into an ETag that every agent on the node revalidates
// against, so an order-sensitive body defeats the 304 path outright.
func TestContributorCeilingKeepsTheMergeOrderedAndTheBodyStable(t *testing.T) {
	const monitorCount = 200
	forward := forwardOrder(monitorCount)
	reverse := slices.Clone(forward)
	slices.Reverse(reverse)

	fetch := func(upsert []int) (string, string, kubemeta.ScrapeTarget) {
		st, svcs := relabelBombFixture(t, 1)
		monitors := contributorBombMonitors(t, monitorCount, upsert)
		srv := httptest.NewServer(New(Config{
			Store: st, Services: svcs, Monitors: monitors, Resolver: stubResolver{},
			MaxWait: 500 * time.Millisecond, CacheTTL: 10 * time.Second, Ready: closedChan(),
		}).Handler())
		t.Cleanup(srv.Close)
		resp, err := http.Get(srv.URL + "/v1/nodes/node1/targets")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		var out struct {
			Targets []kubemeta.ScrapeTarget `json:"targets"`
		}
		if err := json.Unmarshal(body, &out); err != nil {
			t.Fatal(err)
		}
		if len(out.Targets) != 1 {
			t.Fatalf("got %d targets, want 1", len(out.Targets))
		}
		return string(body), resp.Header.Get("ETag"), out.Targets[0]
	}

	fwdBody, fwdETag, fwd := fetch(forward)
	revBody, revETag, _ := fetch(reverse)
	if fwdBody != revBody || fwdETag != revETag {
		t.Errorf("the capped contributor list depends on upsert order; the body and its ETag must not\nforward %s\nreverse %s", fwdETag, revETag)
	}
	// The cadence rules still hold across the ceiling: the merge folds in
	// encounter order and the FINEST explicit interval wins, which is the last
	// monitor here — long past the point where its name stopped being listed.
	if fwd.Interval != "1s" {
		t.Errorf("served interval %q, want the finest (1s): the ceiling must refuse attribution, not merging", fwd.Interval)
	}
	// The list that IS served is the first contributors in encounter order:
	// the holder first, then the monitors that folded in, deterministically.
	if len(fwd.Monitors) != scrape.MaxContributorsPerTarget {
		t.Fatalf("contributor list has %d entries, want exactly the ceiling %d",
			len(fwd.Monitors), scrape.MaxContributorsPerTarget)
	}
	for i, m := range fwd.Monitors {
		if want := "default/" + contributorBombName(i); m != want {
			t.Fatalf("contributor %d is %q, want %q (the prefix must be encounter order)", i, m, want)
		}
	}
}

// fetchExplain fetches /v1/explain for one pod through a server built on the
// given indexes.
func fetchExplain(t *testing.T, st *store.Store, svcs *services.Index, monitors *servicemonitors.Index, ns, pod string) string {
	t.Helper()
	srv := httptest.NewServer(New(Config{
		Store: st, Services: svcs, Monitors: monitors, Resolver: stubResolver{},
		MaxWait: 500 * time.Millisecond, CacheTTL: 10 * time.Second, Ready: closedChan(),
	}).Handler())
	t.Cleanup(srv.Close)
	resp, err := http.Get(srv.URL + "/v1/explain/" + ns + "/" + pod)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	doc, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(doc)
}

func truncateDoc(s string) string {
	if len(s) > 2000 {
		return s[:2000] + "…"
	}
	return s
}
