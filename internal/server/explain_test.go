package server

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// The explain endpoint walks the same decision chain as the targets response,
// so its verdicts must line up with what /v1/nodes/{node}/targets serves: an
// annotated pod explains into the same one target, and the entry-by-entry
// port verdicts carry the diagnostics the target list cannot (an undeclared
// numeric port, a name resolving to nothing).
func TestExplainAnnotatedPod(t *testing.T) {
	st := store.New(time.Minute)
	addPod(st) // prometheus.io/scrape=true, port=9090, no declared container ports
	srv := testServer(t, st, closedChan())

	var doc explainDoc
	getJSON(t, srv.URL+"/v1/explain/default/web-abc-xyz", 200, &doc)
	if !doc.Found || !doc.Scrapeable || !doc.PodAnnotated {
		t.Fatalf("doc = %+v", doc)
	}
	if len(doc.Targets) != 1 || doc.Targets[0].URL != "http://10.1.2.3:9090/metrics" || doc.Targets[0].Source != "pod" {
		t.Fatalf("targets = %+v", doc.Targets)
	}
	// The port resolves numerically but no container declares it — the
	// "unexposed container port" caveat must be spelled out.
	if len(doc.PortEntries) != 1 || doc.PortEntries[0].Entry != "9090" {
		t.Fatalf("portEntries = %+v", doc.PortEntries)
	}
	if note := doc.PortEntries[0].Note; !strings.Contains(note, "no container declares port 9090") {
		t.Errorf("undeclared-port note missing: %q", note)
	}
}

func TestExplainNotFoundAndNotScrapeable(t *testing.T) {
	st := store.New(time.Minute)
	srv := testServer(t, st, closedChan())

	// Unknown pod: still a 200, with the hint carrying the next step.
	var doc explainDoc
	getJSON(t, srv.URL+"/v1/explain/default/nope", 200, &doc)
	if doc.Found || !strings.Contains(doc.Hint, "not found") {
		t.Fatalf("doc = %+v", doc)
	}

	// A finished pod is found but not scrapeable, with the reason named.
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "done", Namespace: "default", UID: types.UID("done-uid"), ResourceVersion: "1",
			Annotations: map[string]string{"prometheus.io/scrape": "true", "prometheus.io/port": "8080"},
		},
		Spec:   corev1.PodSpec{NodeName: "node1", Containers: []corev1.Container{{Name: "c"}}},
		Status: corev1.PodStatus{Phase: corev1.PodSucceeded, PodIP: "10.0.0.9"},
	})
	getJSON(t, srv.URL+"/v1/explain/default/done", 200, &doc)
	if !doc.Found || doc.Scrapeable || len(doc.Targets) != 0 {
		t.Fatalf("doc = %+v", doc)
	}
	if len(doc.NotScrapeableWhy) == 0 || !strings.Contains(doc.NotScrapeableWhy[0], "Succeeded") {
		t.Errorf("notScrapeableWhy = %v", doc.NotScrapeableWhy)
	}
}

// A port annotation naming a container port that does not exist must explain
// itself entry by entry, and a Service whose targetPort name resolves to
// nothing must say so — the two silent-nothing cases the endpoint exists for.
func TestExplainUnresolvedPorts(t *testing.T) {
	st := store.New(time.Minute)
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "app-1", Namespace: "prod", UID: types.UID("app-uid"), ResourceVersion: "1",
			Labels:      map[string]string{"app": "app"},
			Annotations: map[string]string{"prometheus.io/scrape": "true", "prometheus.io/port": "metrics,70000"},
		},
		Spec: corev1.PodSpec{NodeName: "node1", Containers: []corev1.Container{{
			Name:  "c",
			Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}},
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.7"},
	})
	idx := services.NewIndex()
	idx.Upsert(&corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "app", Namespace: "prod", UID: types.UID("svc-uid"), ResourceVersion: "1",
			Annotations: map[string]string{"prometheus.io/scrape": "true"},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "app"},
			Ports: []corev1.ServicePort{{
				Name: "web", Port: 80, TargetPort: intstr.FromString("nosuchport"),
			}},
		},
	})
	srv := testServerWithServices(t, st, idx, closedChan())

	var doc explainDoc
	getJSON(t, srv.URL+"/v1/explain/prod/app-1", 200, &doc)
	if len(doc.Targets) != 0 {
		t.Fatalf("targets = %+v", doc.Targets)
	}
	if len(doc.PortEntries) != 2 {
		t.Fatalf("portEntries = %+v", doc.PortEntries)
	}
	if !strings.Contains(doc.PortEntries[0].Note, `no container declares a port named "metrics"`) {
		t.Errorf("name entry note = %q", doc.PortEntries[0].Note)
	}
	if !strings.Contains(doc.PortEntries[1].Note, "not a valid port number") {
		t.Errorf("out-of-range entry note = %q", doc.PortEntries[1].Note)
	}
	if len(doc.Services) != 1 || len(doc.Services[0].PortEntries) != 1 {
		t.Fatalf("services = %+v", doc.Services)
	}
	if note := doc.Services[0].PortEntries[0].Note; !strings.Contains(note, `targets container port name "nosuchport"`) {
		t.Errorf("service port note = %q", note)
	}
	if !strings.Contains(doc.Hint, "no port resolved") {
		t.Errorf("hint = %q", doc.Hint)
	}
	// The declared ports are listed, so the fix is one glance away.
	if len(doc.DeclaredPorts) != 1 || doc.DeclaredPorts[0].Name != "http" || doc.DeclaredPorts[0].Port != 8080 {
		t.Errorf("declaredPorts = %+v", doc.DeclaredPorts)
	}
}

// A pod with no scrape annotation whose only selector match is an UNannotated
// Service is not opted into scraping by anything: doc.Services lists every
// selector match regardless of annotation, so the hint must not read a mere
// match as an opt-in and claim "an opt-in exists but no port resolved".
func TestExplainUnannotatedServiceGetsNothingOptsInHint(t *testing.T) {
	st := store.New(time.Minute)
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "plain-1", Namespace: "prod", UID: types.UID("plain-uid"), ResourceVersion: "1",
			Labels: map[string]string{"app": "plain"},
		},
		Spec: corev1.PodSpec{NodeName: "node1", Containers: []corev1.Container{{
			Name:  "c",
			Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}},
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.8"},
	})
	idx := services.NewIndex()
	idx.Upsert(&corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			// No prometheus.io/scrape annotation: the Service matches the pod
			// but opts nothing into scraping.
			Name: "plain", Namespace: "prod", UID: types.UID("plain-svc-uid"), ResourceVersion: "1",
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "plain"},
			Ports:    []corev1.ServicePort{{Name: "web", Port: 80, TargetPort: intstr.FromInt(8080)}},
		},
	})
	srv := testServerWithServices(t, st, idx, closedChan())

	var doc explainDoc
	getJSON(t, srv.URL+"/v1/explain/prod/plain-1", 200, &doc)
	if len(doc.Targets) != 0 {
		t.Fatalf("targets = %+v", doc.Targets)
	}
	// The matched Service is still listed (with annotated=false) — the hint
	// just must not count it as an opt-in.
	if len(doc.Services) != 1 || doc.Services[0].Annotated {
		t.Fatalf("services = %+v", doc.Services)
	}
	if !strings.Contains(doc.Hint, "nothing opts this pod into scraping") {
		t.Errorf("hint = %q", doc.Hint)
	}
}
