package services

// A Service deleted and recreated under the SAME name gets a fresh UID. The
// index is keyed by UID, so if the Delete event is ever missed — which
// DeltaFIFO.Replace makes possible, since it synthesizes Deleted only for keys
// ABSENT from a relist — the old record becomes unreachable and immortal: it
// keeps matching pods and keeps yielding targets from a configuration that no
// longer exists. Same failure, same guard and same test shape as
// internal/store/samename_regression_test.go.

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func svcNamed(uid types.UID, name string, annotations map[string]string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns", Name: name, UID: uid,
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "web"},
			Ports:    []corev1.ServicePort{{Name: "http", Port: 80}},
		},
	}
}

func TestRecreatedServiceReplacesItsPredecessor(t *testing.T) {
	ix := NewIndex()
	// The original opted into scraping.
	ix.Upsert(svcNamed("uid-old", "web", map[string]string{"prometheus.io/scrape": "true"}))
	// Recreated under the same name with the annotation REMOVED, and with no
	// Delete for the predecessor — the relist-gap case.
	ix.Upsert(svcNamed("uid-new", "web", nil))

	matched := ix.Matching("ns", map[string]string{"app": "web"})
	if len(matched) != 1 {
		t.Fatalf("want exactly one record for a recreated Service, got %d: %+v", len(matched), matched)
	}
	if matched[0].UID != "uid-new" {
		t.Errorf("live record UID = %q, want uid-new", matched[0].UID)
	}
	if got := matched[0].Annotations["prometheus.io/scrape"]; got != "" {
		t.Errorf("stale annotation %q survived the recreation: the old record is still being served", got)
	}
	if all := ix.All(nil); len(all) != 1 {
		t.Errorf("All() returned %d records, want 1 (the predecessor leaked)", len(all))
	}
}

// A Delete for the PREDECESSOR arriving after the successor already claimed
// the name must not unindex the successor.
func TestLateDeleteOfPredecessorKeepsSuccessor(t *testing.T) {
	ix := NewIndex()
	ix.Upsert(svcNamed("uid-old", "web", nil))
	ix.Upsert(svcNamed("uid-new", "web", nil))
	ix.Delete("ns", "uid-old")

	if matched := ix.Matching("ns", map[string]string{"app": "web"}); len(matched) != 1 || matched[0].UID != "uid-new" {
		t.Fatalf("successor lost to a late predecessor delete: %+v", matched)
	}
	// And deleting the successor really does empty the index.
	ix.Delete("ns", "uid-new")
	if matched := ix.Matching("ns", map[string]string{"app": "web"}); len(matched) != 0 {
		t.Fatalf("index not empty after deleting the live service: %+v", matched)
	}
}

// The DELETE side of the guard has to clean the name index, or byName grows by
// one permanently-stale entry per deleted Service — and the next Service to
// take that name is then treated as a replacement of a record that no longer
// exists. Nothing else in the suite reads byName after a delete, so removing
// the cleanup entirely left every test green.
func TestDeleteClearsTheNameIndex(t *testing.T) {
	ix := NewIndex()
	ix.Upsert(svcNamed("uid-a", "web", nil))
	ix.Upsert(svcNamed("uid-b", "api", nil))
	ix.Delete("ns", "uid-a")

	ix.mu.RLock()
	n, stillThere := len(ix.byName), ix.byName["ns/web"]
	ix.mu.RUnlock()
	if stillThere != "" {
		t.Errorf("byName still maps ns/web to %q after its Service was deleted", stillThere)
	}
	if n != 1 {
		t.Errorf("byName holds %d entries, want 1 (only the live Service)", n)
	}

	// And deleting the last Service in the namespace must leave nothing behind
	// at all — that path also removes the namespace map, and an early return
	// there would skip the name cleanup.
	ix.Delete("ns", "uid-b")
	ix.mu.RLock()
	n, nsGone := len(ix.byName), ix.byNamespace["ns"] == nil
	ix.mu.RUnlock()
	if n != 0 || !nsGone {
		t.Errorf("after deleting every Service: byName=%d entries, namespace map gone=%v; want 0, true", n, nsGone)
	}
}

// The name key is namespaced. Without the namespace component, two Services
// sharing a name in DIFFERENT namespaces would evict each other — and no test
// in the package put the same name in two namespaces.
func TestSameNameInDifferentNamespacesCoexist(t *testing.T) {
	ix := NewIndex()
	a := svcNamed("a1", "web", nil)
	a.Namespace = "a"
	b := svcNamed("b1", "web", nil)
	b.Namespace = "b"
	ix.Upsert(a)
	ix.Upsert(b)

	for _, tc := range []struct{ ns, uid string }{{"a", "a1"}, {"b", "b1"}} {
		got := ix.Matching(tc.ns, map[string]string{"app": "web"})
		if len(got) != 1 || got[0].UID != tc.uid {
			t.Fatalf("namespace %q: %+v, want exactly uid %s", tc.ns, got, tc.uid)
		}
	}

	// Replacing the one in namespace "a" must not disturb namespace "b".
	a2 := svcNamed("a2", "web", nil)
	a2.Namespace = "a"
	ix.Upsert(a2)
	if got := ix.Matching("a", map[string]string{"app": "web"}); len(got) != 1 || got[0].UID != "a2" {
		t.Errorf("namespace a: %+v, want exactly uid a2", got)
	}
	if got := ix.Matching("b", map[string]string{"app": "web"}); len(got) != 1 || got[0].UID != "b1" {
		t.Errorf("namespace b was disturbed by a replacement in namespace a: %+v", got)
	}
}

// Distinct names in one namespace must be unaffected by the name guard.
func TestDistinctNamesCoexist(t *testing.T) {
	ix := NewIndex()
	ix.Upsert(svcNamed("uid-a", "web", nil))
	ix.Upsert(svcNamed("uid-b", "api", nil))
	if matched := ix.Matching("ns", map[string]string{"app": "web"}); len(matched) != 2 {
		t.Fatalf("want both services, got %d: %+v", len(matched), matched)
	}
}

// An ordinary update (same UID, same name) must replace in place, not
// duplicate or self-evict.
func TestUpdateInPlaceKeepsOneRecord(t *testing.T) {
	ix := NewIndex()
	ix.Upsert(svcNamed("uid-a", "web", map[string]string{"prometheus.io/scrape": "true"}))
	ix.Upsert(svcNamed("uid-a", "web", map[string]string{"prometheus.io/scrape": "false"}))
	matched := ix.Matching("ns", map[string]string{"app": "web"})
	if len(matched) != 1 {
		t.Fatalf("want 1 record, got %d", len(matched))
	}
	if got := matched[0].Annotations["prometheus.io/scrape"]; got != "false" {
		t.Errorf("update did not take effect: annotation = %q", got)
	}
}
