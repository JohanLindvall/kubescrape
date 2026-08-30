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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"

	"github.com/JohanLindvall/kubescrape/internal/owners"
	"github.com/JohanLindvall/kubescrape/internal/scrape"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// mapLister serves PartialObjectMetadata out of a map, standing in for the
// metadata informer's lister so this test drives the REAL owners.Resolver
// rather than server_test.go's stub — the cap and the annotation budget both
// live below the server, and a stub would prove neither.
type mapLister map[string]*metav1.PartialObjectMetadata

func (l mapLister) List(labels.Selector) ([]runtime.Object, error) { return nil, nil }
func (l mapLister) Get(name string) (runtime.Object, error)        { return l.get(name) }
func (l mapLister) ByNamespace(ns string) cache.GenericNamespaceLister {
	return mapNSLister{l: l, ns: ns}
}
func (l mapLister) get(key string) (runtime.Object, error) {
	if m, ok := l[key]; ok {
		return m, nil
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "objects"}, key)
}

type mapNSLister struct {
	l  mapLister
	ns string
}

func (n mapNSLister) List(labels.Selector) ([]runtime.Object, error) { return nil, nil }
func (n mapNSLister) Get(name string) (runtime.Object, error)        { return n.l.get(n.ns + "/" + name) }

// THE ATTACK the pod-document ceilings exist for, driven end to end.
//
// A tenant with edit rights in ONE namespace creates 100 ReplicaSets each
// carrying a 200 KiB annotation and points every pod's ownerReferences at all
// of them. Kubernetes caps neither the ownerReferences count nor the annotation
// per owner, and one fat owner is shared by every pod that names it, so the pod
// DOCUMENT — which /v1/nodes/{node}/targets carries once per scrapeable pod on
// the node, re-derived and re-marshalled on every agent poll, in the singleton
// the chart requests 128Mi for with no memory limit — grew to ~25 MB per pod.
// scrape.MaxTargetBytesPerPod cannot see it: the pod's FIRST target is
// unconditional by design, and this is entirely inside that first target.
func TestFatOwnerChainCannotInflateThePodDocument(t *testing.T) {
	const fatOwners = 100
	const podsOnNode = 5

	rs := mapLister{}
	refs := make([]metav1.OwnerReference, 0, fatOwners)
	for i := range fatOwners {
		name := "rs-" + strconv.Itoa(i)
		rs["default/"+name] = &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{
			UID:         types.UID("uid-" + name),
			Labels:      map[string]string{"app": "web"},
			Annotations: map[string]string{"team.example.com/inventory": strings.Repeat("x", 200<<10)},
		}}
		refs = append(refs, metav1.OwnerReference{
			APIVersion: "apps/v1", Kind: "ReplicaSet", Name: name, UID: types.UID("uid-" + name),
		})
	}
	ns := mapLister{"prod": {ObjectMeta: metav1.ObjectMeta{
		UID:         "ns-uid",
		Annotations: map[string]string{"team.example.com/runbook": strings.Repeat("y", 200<<10)},
	}}}
	resolver := owners.NewFromListers(map[schema.GroupVersionResource]cache.GenericLister{
		owners.ReplicaSetGVR: rs,
		owners.NamespaceGVR:  ns,
	})

	st := store.New(time.Minute)
	for i := range podsOnNode {
		st.UpsertPod(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "web-" + strconv.Itoa(i), Namespace: "default",
				UID: types.UID("pod-" + strconv.Itoa(i)), ResourceVersion: "1",
				Labels:          map[string]string{"app": "web"},
				Annotations:     map[string]string{scrape.AnnotationScrape: "true", scrape.AnnotationPort: "9090"},
				OwnerReferences: refs,
			},
			Spec: corev1.PodSpec{NodeName: "node1", Containers: []corev1.Container{{
				Name: "app", Image: "img",
				Ports: []corev1.ContainerPort{{Name: "metrics", ContainerPort: 9090}},
			}}},
			Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.9.9." + strconv.Itoa(i+1)},
		})
	}

	srv := httptest.NewServer(New(Config{
		Store: st, Services: services.NewIndex(), Monitors: servicemonitors.NewIndex(), Resolver: resolver,
		MaxWait: 500 * time.Millisecond, CacheTTL: 10 * time.Second, Ready: closedChan(),
	}).Handler())
	t.Cleanup(srv.Close)

	body := httpGet(t, srv.URL+"/v1/nodes/node1/targets")
	t.Logf("%d owners x 200 KiB + a 200 KiB namespace annotation, %d pods -> %d byte targets document "+
		"(unbounded: ~%d MB)", fatOwners, podsOnNode, len(body), podsOnNode*fatOwners*(200<<10)>>20)

	// MaxOwners owners plus the namespace plus the pod itself, each bounded by
	// kubemeta's per-object annotation budget, times the pods on the node —
	// plus generous slack for labels, containers and framing. Unbounded this
	// was ~125 MB.
	perPod := (owners.MaxOwners + 2) * (kubemeta.MaxAnnotationBytes + 4096)
	if want := podsOnNode*perPod + (16 << 10); len(body) > want {
		t.Errorf("node targets document is %d bytes for %d pods, want at most %d", len(body), podsOnNode, want)
	}

	// TRUTHFULNESS: a document that is short must say so, or the next reader
	// takes it for the whole truth. /v1/pods serves the same pod one at a time.
	var pod kubemeta.Pod
	if err := json.Unmarshal(httpGet(t, srv.URL+"/v1/pods/default/web-0"), &pod); err != nil {
		t.Fatal(err)
	}
	if pod.OwnersOmitted != fatOwners-len(pod.Owners) {
		t.Errorf("pod reports ownersOmitted=%d with %d owners served, want %d",
			pod.OwnersOmitted, len(pod.Owners), fatOwners-len(pod.Owners))
	}
	if len(pod.Owners) == 0 {
		t.Fatal("the cap refused the whole chain; the pod's controller must always be described")
	}
	if !kubemeta.AnnotationsOmitted(pod.Owners[0].Annotations) {
		t.Errorf("the owner's annotations were cut without saying so: %v", pod.Owners[0].Annotations)
	}
	for _, o := range pod.Owners {
		if _, ok := o.Annotations["team.example.com/inventory"]; ok {
			t.Fatal("a 200 KiB owner annotation was served")
		}
	}
}

// The namespace document is copied into EVERY pod document — the enrich memo
// shares one value per request in memory, but the marshalled response repeats
// it once per pod, and /v1/pods must serve it standalone. Deduplicating it on
// the wire would be a response-format change that helps only one of the two
// routes, so the honest bound is the same per-object annotation budget.
func TestANamespacesAnnotationsAreBoundedInEveryPodDocument(t *testing.T) {
	ns := mapLister{"default": {ObjectMeta: metav1.ObjectMeta{
		UID:         "ns-uid",
		Annotations: map[string]string{"team.example.com/runbook": strings.Repeat("y", 200<<10)},
	}}}
	resolver := owners.NewFromListers(map[schema.GroupVersionResource]cache.GenericLister{
		owners.NamespaceGVR: ns,
	})
	st := store.New(time.Minute)
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web-0", Namespace: "default", UID: "p", ResourceVersion: "1",
			Labels: map[string]string{"app": "web"},
		},
		Spec:   corev1.PodSpec{NodeName: "node1", Containers: []corev1.Container{{Name: "app", Image: "img"}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.9.9.1"},
	})
	srv := httptest.NewServer(New(Config{
		Store: st, Services: services.NewIndex(), Resolver: resolver,
		MaxWait: 500 * time.Millisecond, CacheTTL: 10 * time.Second, Ready: closedChan(),
	}).Handler())
	t.Cleanup(srv.Close)

	var pod kubemeta.Pod
	if err := json.Unmarshal(httpGet(t, srv.URL+"/v1/pods/default/web-0"), &pod); err != nil {
		t.Fatal(err)
	}
	if pod.NamespaceMetadata == nil {
		t.Fatal("no namespace metadata")
	}
	n := 0
	for k, v := range pod.NamespaceMetadata.Annotations {
		n += len(k) + len(v)
	}
	if n > kubemeta.MaxAnnotationBytes+2048 {
		t.Errorf("the namespace contributes %d annotation bytes to every pod document", n)
	}
	if !kubemeta.AnnotationsOmitted(pod.NamespaceMetadata.Annotations) {
		t.Error("the namespace's annotations were cut without saying so")
	}
}

func httpGet(t *testing.T, url string) []byte {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// /v1/explain answers "why is this pod not scraped?", and an annotation refused
// at the SOURCE is the one case where every field below the head describes a
// pod the derivation never saw: podAnnotated reads false because
// prometheus.io/scrape is genuinely absent from the served document. The head
// has to say what went, or the endpoint is confidently wrong.
func TestExplainSaysWhenThePodsAnnotationsWereRefused(t *testing.T) {
	st := store.New(time.Minute)
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web-0", Namespace: "default", UID: "p", ResourceVersion: "1",
			Labels: map[string]string{"app": "web"},
			Annotations: map[string]string{
				// Over kubemeta.MaxAnnotationValueBytes: a port list nobody
				// could use, refused whole rather than truncated into a
				// half-parseable one.
				scrape.AnnotationScrape: "true",
				scrape.AnnotationPort:   strings.Repeat("9090,", (kubemeta.MaxAnnotationValueBytes/5)+1),
			},
		},
		Spec: corev1.PodSpec{NodeName: "node1", Containers: []corev1.Container{{
			Name: "app", Image: "img",
			Ports: []corev1.ContainerPort{{Name: "metrics", ContainerPort: 9090}},
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.9.9.1"},
	})
	doc := fetchExplain(t, st, services.NewIndex(), servicemonitors.NewIndex(), "default", "web-0")
	var parsed struct {
		AnnotationsOmitted string `json:"annotationsOmitted"`
	}
	if err := json.Unmarshal([]byte(doc), &parsed); err != nil {
		t.Fatal(err)
	}
	if parsed.AnnotationsOmitted == "" {
		t.Fatalf("/v1/explain does not say the pod's annotations were refused: %s", truncateDoc(doc))
	}
	if !strings.Contains(parsed.AnnotationsOmitted, scrape.AnnotationPort) {
		t.Errorf("the note does not name the refused annotation: %q", parsed.AnnotationsOmitted)
	}
}
