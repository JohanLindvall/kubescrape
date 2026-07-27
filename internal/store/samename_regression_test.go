package store

import (
	"testing"
	"time"
)

// A pod name reused by a NEW UID means the old pod is gone. Its own Delete
// event normally handles that — client-go even synthesizes one from a
// DeletedFinalStateUnknown tombstone across a relist gap — but every index
// except byPodName is keyed by UID, so if that delete is ever missed the old
// record keeps its byNode entry and is served as a live scrape target forever.
// StatefulSets make the collision routine: stable name, fresh UID per
// recreation.
func TestSameNameReplacementRetiresOldRecord(t *testing.T) {
	s, _ := newTestStore(5 * time.Minute)

	old := makePod("uid-old", "web-0", "node-a", "1", map[string]string{"app": "aaa"})
	s.UpsertPod(old)
	if got := len(s.PodsOnNode("node-a")); got != 1 {
		t.Fatalf("setup: PodsOnNode = %d, want 1", got)
	}

	// Replacement under the SAME name with a new UID, and NO delete for the
	// old one — the case a lost delete produces.
	repl := makePod("uid-new", "web-0", "node-a", "1", map[string]string{"app": "bbb"})
	s.UpsertPod(repl)

	pods := s.PodsOnNode("node-a")
	if len(pods) != 1 {
		t.Fatalf("PodsOnNode = %d, want 1 — the replaced pod is still a live scrape target", len(pods))
	}
	if pods[0].Pod.UID != "uid-new" {
		t.Errorf("PodsOnNode returned UID %q, want uid-new", pods[0].Pod.UID)
	}

	// The retired pod is tombstoned, not erased: its containers must stay
	// resolvable for the TTL so a draining pod's last logs are attributable.
	if res := mustGet(t, s, "aaa"); res.Pod.UID != "uid-old" {
		t.Errorf("old container resolved to UID %q, want uid-old", res.Pod.UID)
	}
	if res := mustGet(t, s, "bbb"); res.Pod.UID != "uid-new" {
		t.Errorf("new container resolved to UID %q, want uid-new", res.Pod.UID)
	}
}
