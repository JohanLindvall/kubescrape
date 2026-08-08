package services

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func nsService(ns, name, uid string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID(uid)},
		Spec:       corev1.ServiceSpec{Selector: map[string]string{"app": name}},
	}
}

// A ServiceMonitor's namespaceSelector.matchNames is user input the CRD does
// not constrain to be unique, and it reaches All verbatim: a namespace repeated
// N times used to return every Service in it N times, multiplying the server's
// monitor→services memo and the per-request target loop by N. InNamespaces
// already deduped; All must too.
func TestAllDedupesRepeatedNamespaces(t *testing.T) {
	ix := NewIndex()
	ix.Upsert(nsService("prod", "a", "uid-a"))
	ix.Upsert(nsService("prod", "b", "uid-b"))
	ix.Upsert(nsService("stage", "c", "uid-c"))

	got := ix.All([]string{"prod", "prod", "stage", "prod"})
	if len(got) != 3 {
		t.Fatalf("All returned %d services for a namespace list repeating %q, want 3", len(got), "prod")
	}
	seen := map[string]bool{}
	for _, s := range got {
		if seen[s.UID] {
			t.Errorf("service %s/%s returned twice", s.Namespace, s.Name)
		}
		seen[s.UID] = true
	}
	// The single-occurrence result is unchanged.
	if n := len(ix.All([]string{"prod", "stage"})); n != 3 {
		t.Errorf("All over distinct namespaces returned %d, want 3", n)
	}
}
