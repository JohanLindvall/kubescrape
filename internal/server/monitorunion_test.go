package server

// The merge is exercised HERE, against the real nodeTargets loop, because the
// loop is the other half of the mechanism: scrape.MergeMonitorEndpoint is a
// FOLD and this file's subject is how many times each declaration is folded.
// The number of matched SERVICES is the variable, since the loop walks a
// monitor's endpoints once per Service that monitor selects — a pod behind a
// headless Service plus a ClusterIP Service (or a per-shard Service) has that
// shape in any real cluster. These properties were pinned once in
// internal/scrape against a hand-copied miniature of this loop, which is
// exactly the layer that cannot show the bug: with ONE Service the fold is
// already the union.

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// unionMonitor is one ServiceMonitor and the endpoints it declares.
type unionMonitor struct {
	name      string
	endpoints []map[string]any
}

func rule(action, regex string) map[string]any {
	return map[string]any{"action": action, "sourceLabels": []any{"__name__"}, "regex": regex}
}

// unionServer builds a pod selected by nServices Services that ALL translate
// the monitors' endpoint port to the same container port — so every monitor
// endpoint resolves to one URL through every Service, which is the collision
// the merge exists for and the repeat the offer dedup exists for.
func unionServer(t testing.TB, nServices int, mons []unionMonitor) *Server {
	t.Helper()
	st := store.New(time.Minute)
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web-1", Namespace: "default",
			UID: types.UID("pod-uid"), ResourceVersion: "1",
			Labels: map[string]string{"app": "web"},
		},
		Spec: corev1.PodSpec{
			NodeName: "node1",
			Containers: []corev1.Container{{
				Name: "app", Image: "img",
				Ports: []corev1.ContainerPort{{Name: "metrics", ContainerPort: 9090}},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.9.9.9"},
	})
	svcs := services.NewIndex()
	for i := range nServices {
		name := fmt.Sprintf("web-%d", i+1)
		svcs.Upsert(&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: "default", UID: types.UID(name + "-uid"),
				Labels: map[string]string{"team": "obs"},
			},
			Spec: corev1.ServiceSpec{
				Selector: map[string]string{"app": "web"},
				Ports:    []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromString("metrics")}},
			},
		})
	}
	monitors := servicemonitors.NewIndex()
	for _, m := range mons {
		eps := make([]any, 0, len(m.endpoints))
		for _, ep := range m.endpoints {
			eps = append(eps, ep)
		}
		if err := monitors.Upsert(&unstructured.Unstructured{Object: map[string]any{
			"metadata": map[string]any{"name": m.name, "namespace": "default"},
			"spec": map[string]any{
				"selector":  map[string]any{"matchLabels": map[string]any{"team": "obs"}},
				"endpoints": eps,
			},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	return New(Config{
		Store: st, Services: svcs, Monitors: monitors, Resolver: stubResolver{},
		MaxWait: 500 * time.Millisecond, CacheTTL: 10 * time.Second, Ready: closedChan(),
	})
}

// unionTarget serves the node's targets over HTTP and returns the single one
// every fixture here collapses to, plus the raw body and ETag (the merge must
// not make either depend on how the objects arrived).
func unionTarget(t testing.TB, s *Server) (dedupTarget, string, string) {
	t.Helper()
	srv := httptest.NewServer(s.Handler())
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
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	var out struct {
		Targets []dedupTarget `json:"targets"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Targets) != 1 {
		t.Fatalf("want one merged target for one URL, got %+v", out.Targets)
	}
	return out.Targets[0], string(body), resp.Header.Get("ETag")
}

// chainOf renders a served relabel chain compactly for comparison.
func chainOf(tg dedupTarget) []string {
	out := make([]string, len(tg.MetricRelabelings))
	for i, r := range tg.MetricRelabelings {
		out[i] = r.Action + ":" + r.Regex
	}
	return out
}

// The merged chain is the UNION of the monitors' chains, however many Services
// carry the collision into the merge — the API's promise, and what a single
// Service already produced. Two Services served it doubled and three tripled,
// because the loop offers each endpoint once per Service and the merge appends
// what it is given.
func TestMergedChainIsTheUnionAcrossMatchingServices(t *testing.T) {
	mons := []unionMonitor{
		{"sm-a", []map[string]any{{"port": "http", "metricRelabelings": []any{rule("keep", "a_.*")}}}},
		{"sm-b", []map[string]any{{"port": "http", "metricRelabelings": []any{rule("drop", "b_.*")}}}},
	}
	want := []string{"keep:a_.*", "drop:b_.*"}
	for _, n := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("%d-services", n), func(t *testing.T) {
			tg, body, etag := unionTarget(t, unionServer(t, n, mons))
			if got := chainOf(tg); !slices.Equal(got, want) {
				t.Errorf("chain over %d services = %v, want the union %v", n, got, want)
			}
			if wantMon := []string{"default/sm-a", "default/sm-b"}; !slices.Equal(tg.Monitors, wantMon) {
				t.Errorf("monitors = %v, want %v", tg.Monitors, wantMon)
			}
			// Neither the Service count nor the upsert order may reach the
			// wire: the ETag is a body hash, so a body that moved would defeat
			// every agent's 304 on the one route that re-sends the node's whole
			// pod set.
			reversed := slices.Clone(mons)
			slices.Reverse(reversed)
			_, body2, etag2 := unionTarget(t, unionServer(t, n, reversed))
			if body != body2 || etag != etag2 {
				t.Errorf("body/ETag depend on monitor upsert order:\n%s\n---\n%s", body, body2)
			}
		})
	}
}

// A monitor whose chain HAPPENS to appear inside the merged chain still made
// its own declaration, so it is still a contributor: `monitors` is documented
// as absent only when Monitor alone describes the target, and a consumer reads
// a missing entry as "that CR did not resolve here". Both shapes below were
// dropped from the list by a merge that tested whether the chain appeared as a
// contiguous run of the accumulated one.
func TestChainThatIsARunOfTheMergedChainStillContributes(t *testing.T) {
	t.Run("shorter-prefix", func(t *testing.T) {
		tg, _, _ := unionTarget(t, unionServer(t, 1, []unionMonitor{
			{"sm-a", []map[string]any{{"port": "http", "metricRelabelings": []any{rule("keep", "a_.*"), rule("drop", "b_.*")}}}},
			{"sm-b", []map[string]any{{"port": "http", "metricRelabelings": []any{rule("keep", "a_.*")}}}},
		}))
		if want := []string{"default/sm-a", "default/sm-b"}; !slices.Equal(tg.Monitors, want) {
			t.Errorf("monitors = %v, want %v", tg.Monitors, want)
		}
		if want := []string{"keep:a_.*", "drop:b_.*", "keep:a_.*"}; !slices.Equal(chainOf(tg), want) {
			t.Errorf("chain = %v, want %v", chainOf(tg), want)
		}
	})
	// The live shape: a third monitor whose two rules span the seam between the
	// first two monitors' chains.
	t.Run("spanning-the-seam", func(t *testing.T) {
		tg, _, _ := unionTarget(t, unionServer(t, 2, []unionMonitor{
			{"sm-a", []map[string]any{{"port": "http", "metricRelabelings": []any{rule("keep", "a1"), rule("keep", "a2")}}}},
			{"sm-b", []map[string]any{{"port": "http", "metricRelabelings": []any{rule("drop", "b1"), rule("drop", "b2")}}}},
			{"sm-c", []map[string]any{{"port": "http", "metricRelabelings": []any{rule("keep", "a2"), rule("drop", "b1")}}}},
		}))
		if want := []string{"default/sm-a", "default/sm-b", "default/sm-c"}; !slices.Equal(tg.Monitors, want) {
			t.Errorf("monitors = %v, want %v", tg.Monitors, want)
		}
		if want := []string{"keep:a1", "keep:a2", "drop:b1", "drop:b2", "keep:a2", "drop:b1"}; !slices.Equal(chainOf(tg), want) {
			t.Errorf("chain = %v, want %v", chainOf(tg), want)
		}
	})
}

// Two monitors that declare exactly the same chain get it ONCE and the second
// is not a contributor: nothing of its declaration was lost, which is the same
// rule a bare colliding endpoint follows.
func TestIdenticalChainsMergeSilently(t *testing.T) {
	tg, _, _ := unionTarget(t, unionServer(t, 2, []unionMonitor{
		{"sm-a", []map[string]any{{"port": "http", "metricRelabelings": []any{rule("drop", "x_.*")}}}},
		{"sm-b", []map[string]any{{"port": "http", "metricRelabelings": []any{rule("drop", "x_.*")}}}},
	}))
	if want := []string{"drop:x_.*"}; !slices.Equal(chainOf(tg), want) {
		t.Errorf("chain = %v, want %v", chainOf(tg), want)
	}
	if tg.Monitors != nil {
		t.Errorf("monitors = %v, want absent: nothing of the second declaration was lost", tg.Monitors)
	}
}

// ONE monitor's two endpoints on one URL are two declarations and both are
// honoured — the offer dedup keys on the DECLARATION, not on the monitor, or
// the second endpoint would vanish. It is still one monitor, so `monitors`
// stays absent, and a second Service changes nothing.
func TestOneMonitorTwoEndpointsOnOneURLContributeBoth(t *testing.T) {
	mon := []unionMonitor{{"sm-a", []map[string]any{
		{"port": "http", "metricRelabelings": []any{rule("keep", "a_.*")}},
		{"port": "http", "interval": "10s", "metricRelabelings": []any{rule("drop", "b_.*")}},
	}}}
	for _, n := range []int{1, 2} {
		t.Run(fmt.Sprintf("%d-services", n), func(t *testing.T) {
			tg, _, _ := unionTarget(t, unionServer(t, n, mon))
			if want := []string{"keep:a_.*", "drop:b_.*"}; !slices.Equal(chainOf(tg), want) {
				t.Errorf("chain = %v, want both of the monitor's endpoints %v", chainOf(tg), want)
			}
			if tg.Interval != "10s" {
				t.Errorf("interval = %q, want the second endpoint's 10s", tg.Interval)
			}
			if tg.Monitors != nil {
				t.Errorf("monitors = %v, want absent for a single-monitor target", tg.Monitors)
			}
		})
	}
}

// One monitor reaching the pod through N Services is ONE declaration offered N
// times: its own chain, once, and no contributor list.
func TestOneMonitorThroughManyServicesKeepsItsOwnChain(t *testing.T) {
	tg, _, _ := unionTarget(t, unionServer(t, 3, []unionMonitor{
		{"sm-a", []map[string]any{{"port": "http", "metricRelabelings": []any{rule("keep", "a_.*"), rule("drop", "b_.*")}}}},
	}))
	if want := []string{"keep:a_.*", "drop:b_.*"}; !slices.Equal(chainOf(tg), want) {
		t.Errorf("chain = %v, want %v", chainOf(tg), want)
	}
	if tg.Monitors != nil {
		t.Errorf("monitors = %v, want absent for a single-monitor target", tg.Monitors)
	}
}

// One shadowed monitor is ONE loss, however many Services carried its endpoint
// into the merge. The counter used to move once per matching Service (1/2/3 at
// 1/2/3 Services) while the throttled warning beside it fired once — so the
// metric disagreed with its own log line, and an alert on it read the number
// of Services selecting the pod as a number of conflicting CRs.
func TestShadowedMonitorIsCountedOncePerRequest(t *testing.T) {
	mons := []unionMonitor{
		{"sm-a", []map[string]any{{"port": "http", "bearerTokenSecret": map[string]any{"name": "tok-a", "key": "token"}}}},
		{"sm-b", []map[string]any{{"port": "http", "bearerTokenSecret": map[string]any{"name": "tok-b", "key": "token"}}}},
	}
	for _, n := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("%d-services", n), func(t *testing.T) {
			before := obs.MonitorTargetShadowed.WithLabelValues("servicemonitor").Value()
			tg, _, _ := unionTarget(t, unionServer(t, n, mons))
			if got := obs.MonitorTargetShadowed.WithLabelValues("servicemonitor").Value() - before; got != 1 {
				t.Errorf("one conflicting monitor counted %v times over %d services, want 1", got, n)
			}
			if tg.AuthSecret != "default/tok-a/token" {
				t.Errorf("authSecret = %q, want the first monitor's (the holder's is served)", tg.AuthSecret)
			}
		})
	}
}

// The primitive itself: a repeat of the same declaration at the same URL is
// suppressed, and NOTHING else is — a different URL and a different
// declaration are different offers, and a new pod starts over (two hostNetwork
// pods share the node IP and every URL on it).
func TestMonitorOffersSuppressesOnlyExactRepeats(t *testing.T) {
	epA := &servicemonitors.Endpoint{Port: "http"}
	epB := &servicemonitors.Endpoint{Port: "http"} // same VALUE, other declaration
	var o monitorOffers
	o.reset()
	for _, tc := range []struct {
		name string
		url  string
		ep   *servicemonitors.Endpoint
		want bool
	}{
		{"first", "u1", epA, true},
		{"repeat", "u1", epA, false},
		{"other url", "u2", epA, true},
		{"other declaration, same value", "u1", epB, true},
		{"repeat of the other declaration", "u1", epB, false},
	} {
		if got := o.first(tc.url, tc.ep); got != tc.want {
			t.Errorf("%s: first() = %v, want %v", tc.name, got, tc.want)
		}
	}
	o.reset()
	if !o.first("u1", epA) {
		t.Error("reset did not start the next pod over")
	}
}

// The offer dedup keys on (URL, declaration), and the URL half is load-bearing:
// ONE endpoint legitimately resolves to two different pod ports through two
// Services that spell its port differently, and those are two targets. Keying
// on the declaration alone would serve the first and silently drop the second
// — a pod scraped on one of the two ports its monitor asked for.
func TestOneEndpointResolvingToTwoURLsServesBoth(t *testing.T) {
	st := store.New(time.Minute)
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web-1", Namespace: "default",
			UID: types.UID("pod-uid"), ResourceVersion: "1",
			Labels: map[string]string{"app": "web"},
		},
		Spec: corev1.PodSpec{
			NodeName: "node1",
			Containers: []corev1.Container{{
				Name: "app", Image: "img",
				Ports: []corev1.ContainerPort{
					{Name: "metrics", ContainerPort: 9090},
					{Name: "admin", ContainerPort: 9091},
				},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.9.9.9"},
	})
	svcs := services.NewIndex()
	for name, target := range map[string]string{"web-a": "metrics", "web-b": "admin"} {
		svcs.Upsert(&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: "default", UID: types.UID(name + "-uid"),
				Labels: map[string]string{"team": "obs"},
			},
			Spec: corev1.ServiceSpec{
				Selector: map[string]string{"app": "web"},
				Ports:    []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromString(target)}},
			},
		})
	}
	monitors := servicemonitors.NewIndex()
	if err := monitors.Upsert(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "sm-a", "namespace": "default"},
		"spec": map[string]any{
			"selector":  map[string]any{"matchLabels": map[string]any{"team": "obs"}},
			"endpoints": []any{map[string]any{"port": "http"}},
		},
	}}); err != nil {
		t.Fatal(err)
	}
	s := New(Config{
		Store: st, Services: svcs, Monitors: monitors, Resolver: stubResolver{},
		MaxWait: 500 * time.Millisecond, Ready: closedChan(),
	})
	targets, _ := s.nodeTargets("node1")
	var urls []string
	for _, tg := range targets {
		urls = append(urls, tg.URL)
	}
	want := []string{"http://10.9.9.9:9090/metrics", "http://10.9.9.9:9091/metrics"}
	if !slices.Equal(urls, want) {
		t.Errorf("targets = %v, want both ports %v", urls, want)
	}
}

// The marginal cost of a colliding monitor is what the URL pre-check exists to
// bound, and the offer dedup runs on that same per-(pod, monitor, Service)
// path: this is the shape that regressed to 111µs at 50 monitors and 397µs at
// 100 when the merge scanned the accumulated chain for a matching run.
func BenchmarkNodeTargetsCollidingMonitors(b *testing.B) {
	for _, n := range []int{50, 100} {
		b.Run(fmt.Sprintf("%d-monitors", n), func(b *testing.B) {
			mons := make([]unionMonitor, 0, n)
			for i := range n {
				mons = append(mons, unionMonitor{
					name: fmt.Sprintf("sm-%03d", i),
					endpoints: []map[string]any{{
						"port":              "http",
						"metricRelabelings": []any{rule("drop", fmt.Sprintf("m%d_.*", i))},
					}},
				})
			}
			s := unionServer(b, 2, mons)
			s.nodeTargets("node1") // warm the monitor→services memo
			b.ReportAllocs()
			for b.Loop() {
				tg, _ := s.nodeTargets("node1")
				targetSink = tg
			}
		})
	}
}
