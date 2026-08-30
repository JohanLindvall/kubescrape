package services

// InNamespaces is called once per GET /v1/nodes/{node}/targets — the route
// every agent in the fleet re-fetches every scrape cycle — and it COPIES and
// SORTS every Service of every namespace the node's pods live in. That work is
// a pure function of the index, so its cost is repeated per request and per
// node: a namespace's Service population is the same list for all 200 nodes
// asking about it.
//
// The shapes bracket what a real cluster does to it: a node whose pods span
// many small namespaces (the common multi-tenant shape) and a node with pods in
// one very large namespace (kube-system, or a monolith namespace).

import (
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func benchIndex(namespaces, perNamespace int) (*Index, []string) {
	ix := NewIndex()
	names := make([]string, 0, namespaces)
	for n := range namespaces {
		ns := fmt.Sprintf("team-%d", n)
		names = append(names, ns)
		for i := range perNamespace {
			ix.Upsert(&corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					// Reverse-ordered names so the sort has real work to do:
					// map iteration is arbitrary, but a fixture inserted in
					// sorted order can still flatter an adaptive sort.
					Name: fmt.Sprintf("svc-%06d", perNamespace-i), Namespace: ns,
					UID:             types.UID(fmt.Sprintf("uid-%s-%d", ns, i)),
					ResourceVersion: "1",
					Labels:          map[string]string{"team": "obs"},
				},
				Spec: corev1.ServiceSpec{
					Selector: map[string]string{"app": fmt.Sprintf("app-%d", i)},
					Ports:    []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromString("metrics")}},
				},
			})
		}
	}
	return ix, names
}

var inNamespacesSink map[string][]*Service

func BenchmarkInNamespaces(b *testing.B) {
	for _, tc := range []struct{ namespaces, perNamespace int }{
		{1, 20},
		{20, 20},
		{20, 200},
		{4, 1000},
	} {
		ix, names := benchIndex(tc.namespaces, tc.perNamespace)
		b.Run(fmt.Sprintf("%dns_%dsvc", tc.namespaces, tc.perNamespace), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				inNamespacesSink = ix.InNamespaces(names)
			}
		})
	}
}

// The invalidating shape: a Service changes between every call, so the memo (if
// any) can never hit. It is what an index under churn costs, and it must not be
// DEARER than no memo at all.
func BenchmarkInNamespacesChurn(b *testing.B) {
	ix, names := benchIndex(20, 200)
	rv := 1
	b.ReportAllocs()
	for b.Loop() {
		rv++
		ix.Upsert(&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name: "svc-000001", Namespace: "team-0", UID: types.UID("uid-team-0-199"),
				ResourceVersion: fmt.Sprintf("%d", rv), Labels: map[string]string{"team": "obs"},
			},
			Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "app-199"}},
		})
		inNamespacesSink = ix.InNamespaces(names)
	}
}
