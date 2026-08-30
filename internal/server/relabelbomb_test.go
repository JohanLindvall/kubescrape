package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/scrape"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// relabelBombFixture builds the shape the attack needs: pods on one node behind
// a Service that a cluster-wide ServiceMonitor selects. Anyone with edit rights
// in ONE namespace can create such a monitor — `selector: {}` plus
// `namespaceSelector.any: true` in the real thing, a matching label here — and
// -monitor-namespaces (the admin gate) is unset by default.
func relabelBombFixture(t *testing.T, pods int) (*store.Store, *services.Index) {
	t.Helper()
	st := store.New(time.Minute)
	for i := range pods {
		name := "web-" + strconv.Itoa(i)
		st.UpsertPod(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: "default",
				UID: types.UID("pod-" + name), ResourceVersion: "1",
				Labels: map[string]string{"app": "web"},
			},
			Spec: corev1.PodSpec{
				NodeName: "node1",
				Containers: []corev1.Container{{
					Name: "app", Image: "img",
					Ports: []corev1.ContainerPort{{Name: "metrics", ContainerPort: 9090}},
				}},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.9.9." + strconv.Itoa(i+1)},
		})
	}
	svcs := services.NewIndex()
	svcs.Upsert(&corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web", Namespace: "default", UID: types.UID("svc-uid"),
			Labels: map[string]string{"team": "obs"},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "web"},
			Ports:    []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromString("metrics")}},
		},
	})
	return st, svcs
}

// relabelRules renders n metricRelabelings whose regexes are `bytes` long.
func relabelRules(n, bytes int) []any {
	out := make([]any, 0, n)
	for i := range n {
		out = append(out, map[string]any{
			"action":       "drop",
			"sourceLabels": []any{"__name__"},
			"regex":        strconv.Itoa(i) + strings.Repeat("x", bytes),
		})
	}
	return out
}

func fetchTargets(t *testing.T, st *store.Store, svcs *services.Index, monitors *servicemonitors.Index) ([]byte, []kubemeta.ScrapeTarget) {
	t.Helper()
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
	return body, out.Targets
}

// THE ATTACK, one CR: a tenant with namespace edit rights creates a single
// ServiceMonitor carrying 20,000 keep/drop metricRelabelings — ~1.1 MiB of
// YAML, comfortably inside etcd's ~1.5 MB object limit — and selects every
// Service in the cluster.
//
// Unbounded, the chain is copied into EVERY target the monitor resolves to
// (scrape.stampEndpoint) and the whole node's targets are marshalled into ONE
// []byte per request, in a singleton whose chart requests 128Mi and sets no
// memory limit. Measured before the bound: 1.24 MiB per served target, 12.4 MiB
// for ten pods on one node — and every DaemonSet agent re-fetches this document
// on its scrape interval, which handlers.go documents as never hitting the ETag
// memo. scrape.MaxPortsPerPod does not see it: it bounds the target COUNT
// against a per-target cost it models as the pod document.
func TestOneMonitorsRelabelChainCannotInflateTheNodeTargetsDocument(t *testing.T) {
	const (
		pods  = 10
		rules = 20000
	)
	st, svcs := relabelBombFixture(t, pods)
	monitors := servicemonitors.NewIndex()
	if err := monitors.Upsert(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "sm-bomb", "namespace": "default"},
		"spec": map[string]any{
			"selector": map[string]any{"matchLabels": map[string]any{"team": "obs"}},
			"endpoints": []any{map[string]any{
				"port":              "http",
				"metricRelabelings": relabelRules(rules, 40),
			}},
		},
	}}); err != nil {
		t.Fatal(err)
	}

	body, targets := fetchTargets(t, st, svcs, monitors)
	t.Logf("%d rules x %d pods -> %d targets, %.2f MiB response", rules, pods, len(targets), float64(len(body))/(1<<20))
	if len(targets) != pods {
		t.Fatalf("got %d targets, want one per pod (%d)", len(targets), pods)
	}
	for _, tgt := range targets {
		if n := len(tgt.MetricRelabelings); n > scrape.MaxRelabelChainRules {
			t.Errorf("served target carries %d relabel rules; the per-target ceiling is %d",
				n, scrape.MaxRelabelChainRules)
		}
	}
	// The bound that actually matters is BYTES, which is what a rule count
	// cannot express (the lesson MaxPortsPerPod records). One pod document is
	// ~1 KiB here, so anything under a few hundred KiB is the chain being
	// bounded rather than copied.
	if len(body) > 512<<10 {
		t.Errorf("node targets document is %.2f MiB for %d pods; a tenant-supplied relabel chain must not "+
			"multiply into it", float64(len(body))/(1<<20), pods)
	}
	// Fail CLOSED and DIAGNOSABLE: the refused rules must be reported, not
	// silently dropped, or an operator sees the series they asked to drop
	// arriving with nothing anywhere saying why.
	ignored := strings.Join(servicemonitors.IgnoredFields(monitors.Endpoints("default", "sm-bomb")), ",")
	if !strings.Contains(ignored, "metricRelabelings") {
		t.Errorf("the refused rules are not reported: ignored fields = %q", ignored)
	}
}

// THE SAME ATTACK, spread over many CRs, which is what makes the per-endpoint
// bound insufficient on its own: every monitor resolving to ONE URL on one pod
// is served as ONE target whose chains CONCATENATE (scrape.MergeMonitorEndpoint),
// and nothing bounds how many ServiceMonitors a tenant may create. Each of
// these is individually inside the parse-time bound.
func TestCollidingMonitorsCannotConcatenateAnUnboundedRelabelChain(t *testing.T) {
	const (
		monitorCount    = 24
		rulesPerMonitor = 60
	)
	st, svcs := relabelBombFixture(t, 1)
	monitors := servicemonitors.NewIndex()
	for i := range monitorCount {
		// Distinct regexes, or relabelChainsEqual would merge them silently as
		// one identical declaration and prove nothing.
		rules := relabelRules(rulesPerMonitor, 60)
		for _, r := range rules {
			m := r.(map[string]any)
			m["regex"] = "m" + strconv.Itoa(i) + m["regex"].(string)
		}
		if err := monitors.Upsert(&unstructured.Unstructured{Object: map[string]any{
			"metadata": map[string]any{"name": "sm-" + strconv.Itoa(i), "namespace": "default"},
			"spec": map[string]any{
				"selector":  map[string]any{"matchLabels": map[string]any{"team": "obs"}},
				"endpoints": []any{map[string]any{"port": "http", "metricRelabelings": rules}},
			},
		}}); err != nil {
			t.Fatal(err)
		}
	}

	before := obs.MonitorRelabelChainCapped.WithLabelValues("servicemonitor").Value()
	body, targets := fetchTargets(t, st, svcs, monitors)
	if len(targets) != 1 {
		t.Fatalf("got %d targets, want 1 (every monitor resolves to the same URL)", len(targets))
	}
	chain := targets[0].MetricRelabelings
	bytes := 0
	for _, r := range chain {
		bytes += len(r.Regex)
		for _, l := range r.SourceLabels {
			bytes += len(l)
		}
	}
	t.Logf("%d monitors x %d rules -> merged chain %d rules / %d bytes, %d byte response",
		monitorCount, rulesPerMonitor, len(chain), bytes, len(body))
	if len(chain) > scrape.MaxRelabelChainRules {
		t.Errorf("merged chain has %d rules; the per-target ceiling is %d", len(chain), scrape.MaxRelabelChainRules)
	}
	if bytes > scrape.MaxRelabelChainBytes {
		t.Errorf("merged chain is %d bytes; the per-target ceiling is %d", bytes, scrape.MaxRelabelChainBytes)
	}
	// Refusing silently would be the worse half of the bug: the rules simply
	// stop filtering and the series arrive.
	if got := obs.MonitorRelabelChainCapped.WithLabelValues("servicemonitor").Value() - before; got == 0 {
		t.Error("the merged chain was capped without moving kubescrape_monitor_relabel_chain_capped_total")
	}
	// And /v1/explain — the operator's "why is this pod scraped like that?" —
	// has to say so too, since nothing in the data can.
	srv := httptest.NewServer(New(Config{
		Store: st, Services: svcs, Monitors: monitors, Resolver: stubResolver{},
		MaxWait: 500 * time.Millisecond, CacheTTL: 10 * time.Second, Ready: closedChan(),
	}).Handler())
	t.Cleanup(srv.Close)
	resp, err := http.Get(srv.URL + "/v1/explain/default/web-0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	doc, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(doc), "per-target ceiling") {
		t.Errorf("/v1/explain does not report the capped relabel chain: %s", doc)
	}
}
