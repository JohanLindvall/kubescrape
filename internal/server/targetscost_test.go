package server

// The cost side of GET /v1/nodes/{node}/targets. Every agent in the fleet
// re-fetches this route every scrape cycle and it re-serves the whole node's
// pod set, so its scaling properties are load-bearing: the ceilings here are
// what fail a build when one of them regresses. BenchmarkNodeTargets reports
// the same shapes.

import (
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"
	"unsafe"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// A conditional GET must be answered WITHOUT deriving the response.
//
// The ETag is a body hash, so a 304 otherwise cost everything a 200 cost —
// PodsOnNode, per-pod owner and namespace enrichment, target derivation, the
// sort and a full json.Marshal — and then threw the body away. With a 10s TTL
// under a 30s scrape interval, that is what EVERY agent poll after the first
// is.
func TestNodeTargetsRevalidationDoesNotRebuild(t *testing.T) {
	f := targetsFixture{pods: 20, services: 5, cacheTTL: 10 * time.Second}
	s := f.build(t)
	now := time.Now()
	s.now = func() time.Time { return now }
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	url := srv.URL + "/v1/nodes/node1/targets"

	etag := getETag(t, url, "")
	if etag == "" {
		t.Fatal("no ETag on the first response")
	}
	if n := s.targetBuilds.Load(); n != 1 {
		t.Fatalf("builds after the first request = %d, want 1", n)
	}

	// Revalidations inside the TTL: 304, and no derivation at all.
	for range 5 {
		status, _ := conditionalGet(t, url, etag)
		if status != http.StatusNotModified {
			t.Fatalf("revalidation status = %d, want 304", status)
		}
	}
	if n := s.targetBuilds.Load(); n != 1 {
		t.Errorf("builds after five revalidations = %d, want 1: a 304 must not derive the response", n)
	}

	// A request with no validator still gets a full 200 — the memo only ever
	// answers "unchanged", never serves a body.
	if got := getETag(t, url, ""); got != etag {
		t.Errorf("unconditional GET returned ETag %q, want %q", got, etag)
	}
	if n := s.targetBuilds.Load(); n != 2 {
		t.Errorf("builds after an unconditional GET = %d, want 2", n)
	}

	// A validator that does NOT match rebuilds, because the answer is a body.
	if status, _ := conditionalGet(t, url, `"stale"`); status != http.StatusOK {
		t.Errorf("stale validator status = %d, want 200", status)
	}
	if n := s.targetBuilds.Load(); n != 3 {
		t.Errorf("builds after a stale validator = %d, want 3", n)
	}

	// Past the TTL the memo is ignored: staleness stays bounded by exactly the
	// lifetime the response advertises, and a changed target list becomes a 200
	// there.
	now = now.Add(11 * time.Second)
	if status, _ := conditionalGet(t, url, etag); status != http.StatusNotModified {
		t.Errorf("status = %d, want 304 (the content is in fact unchanged)", status)
	}
	if n := s.targetBuilds.Load(); n != 4 {
		t.Errorf("builds after the TTL lapsed = %d, want 4: the memo must expire", n)
	}
}

// With caching off there is no validator to answer from, and nothing may be
// memoised: -metadata-cache-ttl=0 is an operator asking for a freshly derived
// answer every time.
func TestNodeTargetsNotMemoisedWithoutTTL(t *testing.T) {
	s := targetsFixture{pods: 5}.build(t)
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	for range 3 {
		getJSON(t, srv.URL+"/v1/nodes/node1/targets", http.StatusOK, nil)
	}
	if n := s.targetBuilds.Load(); n != 3 {
		t.Errorf("builds = %d, want 3", n)
	}
	if n := s.memoisedNodes(); n != 0 {
		t.Errorf("%d ETags memoised with caching disabled", n)
	}
}

// A node the store has no pods on must never mint a memo entry: node names come
// from the URL, so an unauthenticated caller could otherwise grow the map by
// inventing them. Building an empty list is a map lookup, so there is nothing
// to memoise anyway.
func TestUnknownNodeIsNotMemoised(t *testing.T) {
	s := targetsFixture{pods: 5, cacheTTL: 10 * time.Second}.build(t)
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	for i := range 50 {
		getJSON(t, srv.URL+"/v1/nodes/invented-"+string(rune('a'+i%26))+"/targets", http.StatusOK, nil)
	}
	if n := s.memoisedNodes(); n != 0 {
		t.Errorf("%d ETags memoised for nodes with no pods", n)
	}
	getJSON(t, srv.URL+"/v1/nodes/node1/targets", http.StatusOK, nil)
	if n := s.memoisedNodes(); n != 1 {
		t.Errorf("ETags memoised for a real node = %d, want 1", n)
	}
}

// The Services index must be read ONCE PER NAMESPACE per request, not once per
// pod: the per-pod call took the RLock and scanned every Service in the pod's
// namespace, so a 110-pod node in a 1,000-Service namespace did 110 lock round
// trips and 110,000 selector evaluations — on the default annotation path,
// contending with the informer's writes, while the two sibling lookups on the
// same request had both already been hoisted out of that loop.
func TestNodeTargetsReadsTheServicesIndexOncePerNamespace(t *testing.T) {
	f := targetsFixture{pods: 40, services: 20}
	s := f.build(t)
	svcs := s.services
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)

	before := svcs.Reads()
	getJSON(t, srv.URL+"/v1/nodes/node1/targets", http.StatusOK, nil)
	// One namespace on the node (the fixture puts every pod in "prod"), no
	// monitors indexed, so the monitor→services match reads nothing.
	if got := svcs.Reads() - before; got != 1 {
		t.Errorf("services index reads for one request = %d, want 1 (one namespace)", got)
	}
}

// A monitor endpoint that can only be SHADOWED must not be materialised: a
// ScrapeTarget embeds the whole pod document and allocates a Service view and a
// relabeling copy, so a cluster-wide monitor set built a target per (pod,
// monitor) and threw all but one away. The marginal cost of one more monitor
// selecting the same Services is now the URL resolution and nothing else.
func TestShadowedMonitorTargetsAreNotMaterialised(t *testing.T) {
	if raceEnabled {
		t.Skip("-race perturbs allocation counts")
	}
	const (
		pods     = 20
		monitors = 20
		// Bytes per (pod, monitor), which is what discriminates: the two paths
		// allocate a similar NUMBER of objects and wildly different amounts of
		// memory. Resolving the URL to compare it is a short string; building
		// the target instead is a 592-byte ScrapeTarget with the pod document
		// embedded, plus a Service view and a relabeling copy.
		budgetPerPair = 200
	)
	base := bytesPerTargetsBuild(t, targetsFixture{pods: pods, services: 1})
	with := bytesPerTargetsBuild(t, targetsFixture{pods: pods, services: 1, monitors: monitors})
	perPair := (with - base) / float64(pods*monitors)
	if perPair > budgetPerPair {
		t.Errorf("each additional monitor costs %.0f bytes per pod (budget %d): "+
			"targets that can only be shadowed are being built and discarded", perPair, budgetPerPair)
	}
	t.Logf("bytes per build: %.0f without monitors, %.0f with %d, %.0f per (pod, monitor)",
		base, with, monitors, perPair)
}

// bytesPerTargetsBuild is the byte-allocation counterpart of AllocsPerRun.
// TotalAlloc is exact and monotonic, so the average over a handful of builds is
// stable — no GC timing enters it.
func bytesPerTargetsBuild(t *testing.T, f targetsFixture) float64 {
	t.Helper()
	const runs = 10
	s := f.build(t)
	tg, _ := s.nodeTargets("node1") // warm any lazily-grown scratch
	targetSink = tg
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range runs {
		tg, _ := s.nodeTargets("node1")
		targetSink = tg
	}
	runtime.ReadMemStats(&after)
	return float64(after.TotalAlloc-before.TotalAlloc) / runs
}

var targetSink []kubemeta.ScrapeTarget

// Two monitors declaring DIFFERENT auth for one URL on one pod: one scrape
// cannot present two credentials, so the first monitor's is served — and the
// conflict is counted, because a scrape running with a credential one of its
// CRs did not choose must not be discoverable only by a packet capture. This
// is the ONE unmergeable endpoint group; everything else merges silently
// (TestTwoMonitorsOnOneURLMergeConfiguration).
func TestConflictingMonitorAuthIsCountedNotSilent(t *testing.T) {
	before := obs.MonitorTargetShadowed.WithLabelValues("podmonitor").Value()
	srv := twoPodMonitorsWithEndpoints(t, map[string]map[string]any{
		"pm-a": {"port": "metrics", "bearerTokenSecret": map[string]any{"name": "tok-a", "key": "token"}},
		"pm-b": {"port": "metrics", "bearerTokenSecret": map[string]any{"name": "tok-b", "key": "token"}},
	})

	var out struct {
		Targets []dedupTarget `json:"targets"`
	}
	getJSON(t, srv.URL+"/v1/nodes/node1/targets", http.StatusOK, &out)
	if len(out.Targets) != 1 {
		t.Fatalf("want exactly one target for one URL, got %+v", out.Targets)
	}
	// First by (namespace, name) — the same order the index publishes, so the
	// winner does not depend on map iteration.
	if out.Targets[0].Monitor != "default/pm-a" {
		t.Errorf("winner = %q, want default/pm-a", out.Targets[0].Monitor)
	}
	if got := out.Targets[0].AuthSecret; got != "default/tok-a/token" {
		t.Errorf("served auth = %q, want the first monitor's default/tok-a/token", got)
	}
	if got := obs.MonitorTargetShadowed.WithLabelValues("podmonitor").Value() - before; got != 1 {
		t.Errorf("auth conflicts counted = %v, want 1", got)
	}
}

// When the URL HOLDER is bare and a merged contributor's auth was adopted, the
// served credential belongs to the contributor — a later conflict must be
// judged (and attributed) against THAT material, and the served target proves
// whose it is: the old warn named the holder, a monitor with no auth at all.
func TestAdoptedAuthIsServedAndConflictCounted(t *testing.T) {
	before := obs.MonitorTargetShadowed.WithLabelValues("podmonitor").Value()
	srv := twoPodMonitorsWithEndpoints(t, map[string]map[string]any{
		"pm-a": {"port": "metrics"}, // the holder, bare
		"pm-b": {"port": "metrics", "bearerTokenSecret": map[string]any{"name": "tok-b", "key": "token"}},
		"pm-c": {"port": "metrics", "bearerTokenSecret": map[string]any{"name": "tok-c", "key": "token"}},
	})

	var out struct {
		Targets []dedupTarget `json:"targets"`
	}
	getJSON(t, srv.URL+"/v1/nodes/node1/targets", http.StatusOK, &out)
	if len(out.Targets) != 1 {
		t.Fatalf("want exactly one target for one URL, got %+v", out.Targets)
	}
	if out.Targets[0].Monitor != "default/pm-a" {
		t.Errorf("winner = %q, want default/pm-a", out.Targets[0].Monitor)
	}
	if got := out.Targets[0].AuthSecret; got != "default/tok-b/token" {
		t.Errorf("served auth = %q, want the adopted contributor's default/tok-b/token", got)
	}
	if got := obs.MonitorTargetShadowed.WithLabelValues("podmonitor").Value() - before; got != 1 {
		t.Errorf("auth conflicts counted = %v, want 1 (pm-c against the adopted material)", got)
	}
}

// The SAME auth declared by both monitors is not a conflict: the served
// material is exactly what each CR asked for, so nothing is lost and nothing
// may be counted — a false rate here would send an operator hunting for a
// credential mismatch that does not exist.
func TestIdenticalMonitorAuthMergesSilently(t *testing.T) {
	before := obs.MonitorTargetShadowed.WithLabelValues("podmonitor").Value()
	srv := twoPodMonitorsWithEndpoints(t, map[string]map[string]any{
		"pm-a": {"port": "metrics", "bearerTokenSecret": map[string]any{"name": "tok", "key": "token"}},
		"pm-b": {"port": "metrics", "bearerTokenSecret": map[string]any{"name": "tok", "key": "token"}},
	})

	var out struct {
		Targets []dedupTarget `json:"targets"`
	}
	getJSON(t, srv.URL+"/v1/nodes/node1/targets", http.StatusOK, &out)
	if len(out.Targets) != 1 || out.Targets[0].AuthSecret != "default/tok/token" {
		t.Fatalf("want one target carrying the shared auth, got %+v", out.Targets)
	}
	if got := obs.MonitorTargetShadowed.WithLabelValues("podmonitor").Value() - before; got != 0 {
		t.Errorf("identical auth counted %v times as a conflict", got)
	}
}

func getETag(t *testing.T, url, match string) string {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	if match != "" {
		req.Header.Set("If-None-Match", match)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.Header.Get("ETag")
}

func conditionalGet(t *testing.T, url, match string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("If-None-Match", match)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode, resp.Header.Get("ETag")
}

// The monitor→services memo holds one entry per (monitor, service, endpoint)
// pair and lives as long as the monitors and Services are unchanged. A
// cluster-wide 50 ServiceMonitors over 2,000 Services is 100,000 pairs, so the
// pair's SIZE is the memo's memory: by value the Endpoint made it 304 bytes and
// 40.2 MB resident, with the old and new maps both alive across a rebuild (an
// 80 MB transient peak) in a process with a 128Mi request. Indexed monitors are
// treat-as-immutable and the pair only ever reads the endpoint, so it holds a
// pointer.
func TestMonitorEndpointPairStaysSmall(t *testing.T) {
	const budget = 32
	if got := unsafe.Sizeof(monitorEndpoint{}); got > budget {
		t.Errorf("monitorEndpoint is %d bytes (budget %d); the memo holds one per "+
			"(monitor, service, endpoint) pair", got, budget)
	}
}

// memoisedNodes reads the ETag memo's size under its own lock: the handler
// writes it from the serving goroutine.
func (s *Server) memoisedNodes() int {
	s.targetsMu.Lock()
	defer s.targetsMu.Unlock()
	return len(s.targetsETags)
}

// One monitor reaching one pod port through two Services is the same scrape
// declaration arriving twice: the dedup collapses it, as it always has, and
// nothing is lost — so it must not be counted as a shadowed monitor. Counting
// it would put a permanent rate on the metric whose whole purpose is to say
// that an operator's second monitor is not being applied.
func TestOneMonitorViaTwoServicesIsNotReportedAsShadowed(t *testing.T) {
	before := obs.MonitorTargetShadowed.WithLabelValues("servicemonitor").Value()
	s := targetsFixture{pods: 3, services: 2, monitors: 1, sharedSelector: true}.build(t)
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)

	var out struct {
		Targets []dedupTarget `json:"targets"`
	}
	getJSON(t, srv.URL+"/v1/nodes/node1/targets", http.StatusOK, &out)
	if len(out.Targets) != 3 { // one per pod: both Services resolve to one URL
		t.Fatalf("want one target per pod, got %d: %+v", len(out.Targets), out.Targets)
	}
	if got := obs.MonitorTargetShadowed.WithLabelValues("servicemonitor").Value() - before; got != 0 {
		t.Errorf("shadowed counted %v times for one monitor reached twice", got)
	}
}
