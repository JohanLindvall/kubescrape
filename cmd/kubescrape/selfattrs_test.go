package main

import (
	"context"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/store"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// stubResolver is the owner/namespace enrichment the real informer-backed
// resolver provides.
type stubResolver struct{}

func (stubResolver) Resolve(_ string, refs []metav1.OwnerReference) []kubemeta.Owner {
	out := make([]kubemeta.Owner, 0, len(refs))
	for _, r := range refs {
		out = append(out, kubemeta.Owner{APIVersion: r.APIVersion, Kind: r.Kind, Name: r.Name, UID: string(r.UID)})
	}
	return out
}

func (stubResolver) Namespace(name string) *kubemeta.ObjectMeta {
	return &kubemeta.ObjectMeta{UID: "ns-" + name, Labels: map[string]string{"kubernetes.io/metadata.name": name}}
}

func (stubResolver) Node(string) *kubemeta.ObjectMeta { return nil }

// addSelfPod puts a pod named like this process's hostname into the store,
// standing in for the service's own pod.
func addSelfPod(t *testing.T, st *store.Store, namespace string) string {
	t.Helper()
	host, err := os.Hostname()
	if err != nil {
		t.Skip("no hostname")
	}
	ctrl := true
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            host,
			Namespace:       namespace,
			UID:             types.UID("svc-uid"),
			ResourceVersion: "1",
			Labels:          map[string]string{"app": "kubescrape"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "kubescrape-abc", UID: "rs-uid", Controller: &ctrl,
			}},
		},
		Spec:   corev1.PodSpec{NodeName: "node1", Containers: []corev1.Container{{Name: "kubescrape"}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.1.2.9"},
	})
	return host
}

// The service finds its own pod in its own store — no API traffic, no
// downward-API env beyond the namespace it already has from the ServiceAccount
// projection — and enriches it like the HTTP handlers do.
func TestSelfResolverReadsOwnStore(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "monitoring")
	st := store.New(time.Minute)
	host := addSelfPod(t, st, "monitoring")

	pod, err := selfResolver(st, stubResolver{})(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if pod.Name != host || pod.Namespace != "monitoring" || pod.UID != "svc-uid" {
		t.Fatalf("pod = %+v", pod)
	}
	if len(pod.Owners) != 1 || pod.Owners[0].Kind != "ReplicaSet" {
		t.Fatalf("owners = %+v; the owner chain must be resolved, as on the HTTP path", pod.Owners)
	}
	if pod.NamespaceMetadata == nil || pod.NamespaceMetadata.UID != "ns-monitoring" {
		t.Fatalf("namespace metadata = %+v", pod.NamespaceMetadata)
	}
}

// Before the pod informer has delivered this pod — and forever, for a process
// that is not a pod of that name — the lookup fails and the caller keeps the
// bare identity. It must never invent one.
func TestSelfResolverUnresolved(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "monitoring")
	st := store.New(time.Minute)
	if pod, err := selfResolver(st, stubResolver{})(context.Background()); err == nil {
		t.Fatalf("resolved %+v from an empty store", pod)
	}

	// No namespace at all (no env, no ServiceAccount projection): also an
	// error, never a lookup in the wrong namespace.
	t.Setenv("POD_NAMESPACE", "")
	addSelfPod(t, st, "monitoring")
	if _, err := os.Stat("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); os.IsNotExist(err) {
		if pod, err := selfResolver(st, stubResolver{})(context.Background()); err == nil {
			t.Fatalf("resolved %+v with no namespace available", pod)
		}
	}
}

// The service's build is the built-in mapping: pod attributes plus the derived
// Mimir identity. It carries no resourceAttributes config — that is the
// agent's surface.
func TestServiceSelfBuild(t *testing.T) {
	res := pcommon.NewResource()
	selfBuild(res, &kubemeta.Pod{
		Name: "kubescrape-abc-xyz", Namespace: "monitoring", UID: "svc-uid", NodeName: "node1",
		Owners: []kubemeta.Owner{{Kind: "Deployment", Name: "kubescrape"}},
	})
	a := res.Attributes()
	for _, tc := range []struct{ key, want string }{
		{"k8s.namespace.name", "monitoring"},
		{"k8s.pod.name", "kubescrape-abc-xyz"},
		{"k8s.pod.uid", "svc-uid"},
		{"k8s.node.name", "node1"},
		{"k8s.deployment.name", "kubescrape"},
		{"service.namespace", "monitoring"},
	} {
		v, ok := a.Get(tc.key)
		if !ok || v.AsString() != tc.want {
			t.Errorf("%s = %q, want %q", tc.key, v.AsString(), tc.want)
		}
	}
}
