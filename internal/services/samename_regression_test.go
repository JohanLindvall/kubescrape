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
