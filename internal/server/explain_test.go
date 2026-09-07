package server

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
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

// The per-entry verdicts must not contradict the head of their own document.
//
// Scrapeable is a THIRD short-circuit — monitorEndpoint checks it beside the
// port resolution and the size refusal, and PodTargets/ServiceTargets never run
// at all — while the notes beside it knew only about the other two. So a
// terminating pod behind a perfectly good ServiceMonitor was explained as
// "endpoint resolves to no pod port", and its port entries were listed as
// resolving: an operator reading past the head is sent to fix a port name that
// is fine, or concludes the annotation is working when the pod is excluded
// outright.
func TestExplainOfANotScrapeablePodDoesNotBlameItsPorts(t *testing.T) {
	st := store.New(time.Minute)
	dt := metav1.NewTime(time.Now())
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web-abc", Namespace: "prod", UID: types.UID("web-uid"), ResourceVersion: "1",
			Labels:            map[string]string{"app": "web"},
			Annotations:       map[string]string{"prometheus.io/scrape": "true", "prometheus.io/port": "8080"},
			DeletionTimestamp: &dt, // draining: phase stays Running for the whole grace period
		},
		Spec: corev1.PodSpec{NodeName: "node1", Containers: []corev1.Container{{
			Name:  "c",
			Ports: []corev1.ContainerPort{{Name: "metrics", ContainerPort: 8080}},
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.5"},
	})
	svcs := services.NewIndex()
	svcs.Upsert(&corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web", Namespace: "prod", UID: types.UID("web-svc-uid"), ResourceVersion: "1",
			Labels: map[string]string{"team": "obs"},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "web"},
			Ports:    []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromString("metrics")}},
		},
	})
	idx := servicemonitors.NewIndex()
	if err := idx.Upsert(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "sm-web", "namespace": "prod"},
		"spec": map[string]any{
			"selector":  map[string]any{"matchLabels": map[string]any{"team": "obs"}},
			"endpoints": []any{map[string]any{"port": "http"}},
		},
	}}); err != nil {
		t.Fatal(err)
	}

	var doc explainDoc
	raw := fetchExplain(t, st, svcs, idx, "prod", "web-abc")
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatal(err)
	}
	// The head is the part that was always right; the rest has to agree with it.
	if doc.Scrapeable || len(doc.NotScrapeableWhy) == 0 || len(doc.Targets) != 0 {
		t.Fatalf("setup: scrapeable=%v why=%v targets=%+v", doc.Scrapeable, doc.NotScrapeableWhy, doc.Targets)
	}
	if len(doc.PortEntries) != 1 {
		t.Fatalf("portEntries = %+v", doc.PortEntries)
	}
	if note := doc.PortEntries[0].Note; !strings.Contains(note, "excluded from scraping") {
		t.Errorf("the pod-annotation port entry does not name the pod's exclusion: %q "+
			"(it resolves, and the pod is what yields no target)", note)
	}
	if len(doc.PortEntries[0].Ports) != 0 {
		t.Errorf("a port entry of a pod that yields no target still lists ports %v, which reads as "+
			"'this port is scraped'", doc.PortEntries[0].Ports)
	}
	if len(doc.Services) != 1 || len(doc.Services[0].Monitors) != 1 {
		t.Fatalf("services = %+v", doc.Services)
	}
	if note := doc.Services[0].Monitors[0].Note; !strings.Contains(note, "excluded from scraping") {
		t.Errorf("the monitor endpoint is explained as %q; the endpoint is fine and the POD is "+
			"excluded, so this sends an operator to fix a port name that is not the problem", note)
	}
	if note := doc.Services[0].PortEntries[0].Note; !strings.Contains(note, "excluded from scraping") {
		t.Errorf("the Service port entry does not name the pod's exclusion: %q", note)
	}
}

// The "nothing opts this pod in" hint must be derived from the DERIVATION, not
// from the budget-clipped document: doc.Services holds at most
// maxExplainServices entries, so a pod opted in by its 17th matching Service
// was told, flatly, that no scrape-annotated Service selects it — the exact
// false negative the ...NotShown counters exist to prevent, in the field an
// operator reads first.
func TestExplainHintSeesAnOptInTheBudgetClippedAway(t *testing.T) {
	st := store.New(time.Minute)
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web-1", Namespace: "prod", UID: types.UID("web-uid"), ResourceVersion: "1",
			Labels: map[string]string{"app": "web"},
		},
		Spec: corev1.PodSpec{NodeName: "node1", Containers: []corev1.Container{{
			Name:  "c",
			Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}},
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.6"},
	})
	idx := services.NewIndex()
	// One more than the document lists. Only the LAST by name is annotated, and
	// its ports resolve to nothing, so the pod is opted in and has no targets —
	// which is exactly when the hint is written.
	for i := range maxExplainServices + 1 {
		name := "svc-" + strconv.Itoa(10+i)
		ann := map[string]string(nil)
		target := intstr.FromInt(8080)
		if i == maxExplainServices {
			ann = map[string]string{"prometheus.io/scrape": "true"}
			target = intstr.FromString("nosuchport")
		}
		idx.Upsert(&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: "prod", UID: types.UID(name + "-uid"), ResourceVersion: "1",
				Annotations: ann,
			},
			Spec: corev1.ServiceSpec{
				Selector: map[string]string{"app": "web"},
				Ports:    []corev1.ServicePort{{Name: "web", Port: 80, TargetPort: target}},
			},
		})
	}
	srv := testServerWithServices(t, st, idx, closedChan())

	var doc explainDoc
	getJSON(t, srv.URL+"/v1/explain/prod/web-1", 200, &doc)
	if len(doc.Targets) != 0 {
		t.Fatalf("targets = %+v", doc.Targets)
	}
	// The clip must actually have happened, or the assertion below is vacuous.
	if doc.ServicesNotShown == 0 {
		t.Fatalf("no Service was clipped out of the document (%d listed); the fixture no longer "+
			"reaches the budget", len(doc.Services))
	}
	for _, es := range doc.Services {
		if es.Annotated {
			t.Fatalf("the annotated Service %q is still listed; the fixture must clip it away", es.Name)
		}
	}
	if strings.Contains(doc.Hint, "nothing opts this pod into scraping") {
		t.Errorf("hint = %q — an annotated Service selects this pod and the hint reads it off the "+
			"clipped list, so the operator is told to add an opt-in that already exists", doc.Hint)
	}
	if !strings.Contains(doc.Hint, "no port resolved") {
		t.Errorf("hint = %q, want the opt-in-exists-but-no-port wording", doc.Hint)
	}
}
