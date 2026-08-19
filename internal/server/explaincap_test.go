package server

import (
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

	"github.com/JohanLindvall/kubescrape/internal/scrape"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// The per-pod ceiling binds where targets ACCUMULATE (targetDedup.add), so no
// door can report its own refusals: with a pod annotation and a Service each
// individually under the ceiling, the accumulator still refuses what they want
// together. /v1/explain reported those refused targets as resolving — on the
// Service door with the note "the target is still served", an affirmative false
// statement — which is verbatim the answer ("why is my 17th port not scraped?"
// / "it is") the mirror exists to prevent, and the answer the capped counter's
// own help text sends the operator here to get.
//
// One helper per door below, because the doors reach the accumulator through
// different code and the ceiling was reported on none of them.

// capFixture is a pod on node1 whose doors the individual tests open.
func capFixture(t *testing.T, podAnnotations map[string]string, svc *corev1.Service, monitors *servicemonitors.Index) (*Server, *httptest.Server) {
	t.Helper()
	st := store.New(time.Minute)
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web-1", Namespace: "default",
			UID: types.UID("pod-uid"), ResourceVersion: "1",
			Labels:      map[string]string{"app": "web"},
			Annotations: podAnnotations,
		},
		Spec: corev1.PodSpec{
			NodeName: "node1",
			Containers: []corev1.Container{{
				Name: "app", Image: "img",
				Ports: []corev1.ContainerPort{{Name: "metrics", ContainerPort: 9090}},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.1.2.3"},
	})
	idx := services.NewIndex()
	if svc != nil {
		idx.Upsert(svc)
	}
	s := New(Config{
		Store: st, Services: idx, Monitors: monitors, Resolver: stubResolver{},
		MaxWait: 500 * time.Millisecond, CacheTTL: 10 * time.Second, Ready: closedChan(),
	})
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return s, srv
}

// assertCappedDocument is the shared shape of the promise: the document names
// as many refusals as the node response actually refused, and never claims a
// refused port resolves.
func assertCappedDocument(t *testing.T, s *Server, doc explainDoc, wantCapped int) {
	t.Helper()
	served, _ := s.nodeTargets("node1")
	if len(served) != scrape.MaxPortsPerPod {
		t.Fatalf("fixture serves %d targets; it must sit exactly at the ceiling %d to test it",
			len(served), scrape.MaxPortsPerPod)
	}
	if len(doc.Targets) != len(served) {
		t.Errorf("explain lists %d targets, the node response serves %d", len(doc.Targets), len(served))
	}
	if doc.CappedTargets != wantCapped {
		t.Errorf("doc.cappedTargets = %d, want %d (the targets the ceiling refused)", doc.CappedTargets, wantCapped)
	}
}

// Door 1+2 together: this is the case a per-door counter cannot fix. Ten pod
// annotation entries and ten Service ports are each far under the ceiling; the
// accumulator refuses four of the Service's.
func TestExplainNamesTheServicePortsTheCeilingRefused(t *testing.T) {
	var podPorts, svcPorts []string
	for i := 0; i < 10; i++ {
		podPorts = append(podPorts, strconv.Itoa(9000+i))
		svcPorts = append(svcPorts, strconv.Itoa(9100+i))
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web", Namespace: "default", UID: types.UID("svc-uid"), ResourceVersion: "1",
			Annotations: map[string]string{"prometheus.io/scrape": "true"},
		},
		Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "web"}},
	}
	for i, p := range svcPorts {
		n, _ := strconv.Atoi(p)
		svc.Spec.Ports = append(svc.Spec.Ports, corev1.ServicePort{
			Name: "p" + strconv.Itoa(i), Port: int32(n), TargetPort: intstr.FromInt32(int32(n)),
		})
	}
	s, srv := capFixture(t, map[string]string{
		"prometheus.io/scrape": "true",
		"prometheus.io/port":   strings.Join(podPorts, ","),
	}, svc, nil)

	var doc explainDoc
	getJSON(t, srv.URL+"/v1/explain/default/web-1", http.StatusOK, &doc)
	assertCappedDocument(t, s, doc, 4)

	if len(doc.Services) != 1 {
		t.Fatalf("services = %+v", doc.Services)
	}
	entries := doc.Services[0].PortEntries
	if len(entries) != 10 {
		t.Fatalf("service portEntries = %+v", entries)
	}
	// The doors are consumed in nodeTargets' order — pod annotation first, then
	// this Service — so the accumulator fills after six of these ten and the
	// last four are exactly the refused ones. Asserting the SPLIT, not just a
	// count, is what pins the verdict to the right entry.
	for i, v := range entries {
		refused := i >= 6
		if got := strings.Contains(v.Note, "over the per-pod ceiling"); got != refused {
			t.Errorf("entry %d = %+v, ceiling note %v want %v", i, v, got, refused)
		}
		if refused {
			if len(v.Ports) != 0 {
				t.Errorf("entry %d was refused but still claims resolving ports: %+v", i, v)
			}
			// The affirmative lie: "the target is still served" on a target the
			// server refused, pointing the operator at a listener problem their
			// app does not have.
			if strings.Contains(v.Note, "still served") {
				t.Errorf("entry %d claims a refused target is still served: %+v", i, v)
			}
		} else if len(v.Ports) != 1 {
			t.Errorf("entry %d should resolve one port, got %+v", i, v)
		}
	}
	// And the same on the wire, not only through the in-package struct.
	resp, err := http.Get(srv.URL + "/v1/explain/default/web-1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"cappedTargets":4`) {
		t.Errorf("response body carries no cappedTargets count (%d bytes)", len(body))
	}
	if !strings.Contains(string(body), "over the per-pod ceiling") {
		t.Errorf("response body never mentions the ceiling (%d bytes)", len(body))
	}
}

// monitorSelectedService is the Service a ServiceMonitor door needs between the
// monitor and the pod: selected by the monitor's label selector, selecting the
// pod, and NOT scrape-annotated — the monitor is the only opt-in.
func monitorSelectedService() *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web", Namespace: "default", UID: types.UID("svc-uid"), ResourceVersion: "1",
			Labels: map[string]string{"team": "obs"},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "web"},
			Ports:    []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromString("metrics")}},
		},
	}
}

// newMonitorIndex holds one ServiceMonitor with the given endpoints. Distinct
// paths mean distinct URLs, so the URL dedup collapses none of them and each
// one reaches the accumulator on its own.
func newMonitorIndex(t *testing.T, name string, eps []any) *servicemonitors.Index {
	t.Helper()
	monitors := servicemonitors.NewIndex()
	if err := monitors.Upsert(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": name, "namespace": "default"},
		"spec": map[string]any{
			"selector":  map[string]any{"matchLabels": map[string]any{"team": "obs"}},
			"endpoints": eps,
		},
	}}); err != nil {
		t.Fatal(err)
	}
	return monitors
}

// Door 3: a ServiceMonitor endpoint list needs no annotation on the pod at all,
// and every refused endpoint rendered `resolved: true` with a URL and an EMPTY
// note — byte-identical to a served one.
func TestExplainNamesTheMonitorEndpointsTheCeilingRefused(t *testing.T) {
	const endpoints = 20
	eps := make([]any, 0, endpoints)
	for i := 0; i < endpoints; i++ {
		eps = append(eps, map[string]any{"port": "http", "path": "/m" + strconv.Itoa(i)})
	}
	s, srv := capFixture(t, nil, monitorSelectedService(), newMonitorIndex(t, "sm-many", eps))

	var doc explainDoc
	getJSON(t, srv.URL+"/v1/explain/default/web-1", http.StatusOK, &doc)
	assertCappedDocument(t, s, doc, endpoints-scrape.MaxPortsPerPod)

	if len(doc.Services) != 1 || len(doc.Services[0].Monitors) != endpoints {
		t.Fatalf("monitor verdicts = %+v", doc.Services)
	}
	for i, em := range doc.Services[0].Monitors {
		capped := strings.Contains(em.Note, "over the per-pod ceiling")
		if want := i >= scrape.MaxPortsPerPod; capped != want {
			t.Errorf("endpoint %d verdict = %+v, capped=%v want %v", i, em, capped, want)
		}
		// The endpoint still RESOLVES — the refusal is downstream of that, and
		// blanking the resolution would hide which URL went unscraped.
		if !em.Resolved || em.URL == "" {
			t.Errorf("endpoint %d lost its resolution: %+v", i, em)
		}
	}
}

// Door 4: PodMonitors reach the accumulator through their own loop, which had
// the same silent refusal.
func TestExplainNamesThePodMonitorEndpointsTheCeilingRefused(t *testing.T) {
	const endpoints = 20
	eps := make([]any, 0, endpoints)
	for i := 0; i < endpoints; i++ {
		eps = append(eps, map[string]any{"port": "metrics", "path": "/pm" + strconv.Itoa(i)})
	}
	monitors := servicemonitors.NewIndex()
	if err := monitors.UpsertPodMonitor(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "pm-many", "namespace": "default"},
		"spec": map[string]any{
			"selector":            map[string]any{"matchLabels": map[string]any{"app": "web"}},
			"podMetricsEndpoints": eps,
		},
	}}); err != nil {
		t.Fatal(err)
	}
	s, srv := capFixture(t, nil, nil, monitors)

	var doc explainDoc
	getJSON(t, srv.URL+"/v1/explain/default/web-1", http.StatusOK, &doc)
	assertCappedDocument(t, s, doc, endpoints-scrape.MaxPortsPerPod)

	if len(doc.PodMonitors) != endpoints {
		t.Fatalf("podMonitor verdicts = %+v", doc.PodMonitors)
	}
	for i, em := range doc.PodMonitors {
		capped := strings.Contains(em.Note, "over the per-pod ceiling")
		if want := i >= scrape.MaxPortsPerPod; capped != want {
			t.Errorf("podMonitor endpoint %d verdict = %+v, capped=%v want %v", i, em, capped, want)
		}
	}
}
