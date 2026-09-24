package server

import (
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/scrape"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// The collision warning's key names the colliding CONFIGURATION — job, port and
// the set of declarations — so one mistake is one line whatever its replica
// count. It was written member by member, and identical hostNetwork replicas of
// one workload on one node each contribute one identical member: the key
// changed (and grew) with the per-node replica count, so nodes running
// different counts, and every reschedule that changed a count, minted a fresh
// key and a fresh warning inside the throttle window for one annotation.
func TestHostNetworkReplicaCountDoesNotMintANewCollisionKey(t *testing.T) {
	h := &recordingHandler{}
	build := func(replicas int) (*Server, []scrape.InstanceCollision) {
		st := store.New(time.Minute)
		for i := range replicas {
			st.UpsertPod(replicaPod("web-abc-"+strconv.Itoa(i), "10.0.0.5", "9100", true))
		}
		s := New(Config{
			Store: st, Services: services.NewIndex(), Log: slog.New(h),
			Resolver: deploymentResolver{}, MaxWait: 500 * time.Millisecond, Ready: closedChan(),
		})
		targets, _ := s.nodeTargets("node1")
		if len(targets) != replicas {
			t.Fatalf("%d replicas served %d targets", replicas, len(targets))
		}
		var scan scrape.InstanceScan
		return s, scan.Collisions(targets)
	}

	two, c2 := build(2)
	three, c3 := build(3)
	if len(c2) != 1 || len(c3) != 1 {
		t.Fatalf("fixture: %d and %d collision groups, want one each", len(c2), len(c3))
	}
	if k2, k3 := collisionWarnKey(c2[0]), collisionWarnKey(c3[0]); k2 != k3 {
		t.Errorf("two and three identical replicas key differently (%d vs %d bytes): the key must name the SET "+
			"of colliding declarations, not one entry per replica", len(k2), len(k3))
	}

	// End to end: one process, one table, two replica counts — one line.
	h.mu.Lock()
	h.lines = nil
	h.mu.Unlock()
	three.warnCollide = two.warnCollide
	two.nodeTargets("node1")
	three.nodeTargets("node1")
	if n := two.warnCollide.Len(); n != 1 {
		t.Errorf("throttle table holds %d keys for one misconfigured workload, want 1", n)
	}
	if n := len(h.matching("export the same series identity")); n != 0 {
		t.Errorf("the replica count changing re-warned %d times inside the throttle window", n)
	}
}
