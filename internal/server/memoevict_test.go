package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// seedNodeTargetsMemo writes memo entries for nodes the store holds no pods on,
// each tagged `"seeded"` and last built or revalidated age before now. valid
// is the entry's change token, derived from the node's CURRENT one by adjust.
func seedNodeTargetsMemo(s *Server, now time.Time, age time.Duration, names []string, adjust func(*targetsValidity)) {
	s.targetsMu.Lock()
	defer s.targetsMu.Unlock()
	if s.targetsETags == nil {
		s.targetsETags = make(map[string]nodeTargetsETag)
	}
	for _, n := range names {
		v := s.targetsValidity(n)
		if adjust != nil {
			adjust(&v)
		}
		s.targetsETags[n] = nodeTargetsETag{etag: `"seeded"`, builtAt: now.Add(-age), valid: v}
	}
}

func memoNames(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s-%d", prefix, i)
	}
	return out
}

func (s *Server) memoHolds(node string) (nodeTargetsETag, bool) {
	s.targetsMu.Lock()
	defer s.targetsMu.Unlock()
	e, ok := s.targetsETags[node]
	return e, ok
}

// At its cap the memo must spend a rebuild only where one is OWED. The sweep
// used to drop every entry older than the TTL as "useless: they can only
// produce a rebuild" — true of the wall-clock design, false once the change
// token was added, which answers 304 for an entry of ANY age while its token
// matches. builtAt moves only when a node revalidates or rebuilds, so with a
// 10s TTL and 30s agent polls nearly every LIVE entry is past the TTL at any
// instant, and one insert at the cap evicted the whole fleet's valid memos
// while the departed nodes' dead entries it was meant to reclaim went with
// them. The provably unusable go first now; the least-recently-used rule is
// only the fallback for a cluster with nothing unusable to reclaim.
func TestNodeTargetsMemoSweepAtTheCapEvictsTheUnusableFirst(t *testing.T) {
	const ttl, idle = 10 * time.Second, time.Minute

	t.Run("dead entries go, live ones stay", func(t *testing.T) {
		var ownerGen atomic.Uint64
		ownerGen.Store(5)
		s := targetsFixture{pods: 3, services: 1, cacheTTL: ttl, ownerGen: ownerGen.Load}.build(t)
		now := time.Now()
		s.now = func() time.Time { return now }
		srv := httptest.NewServer(s.Handler())
		t.Cleanup(srv.Close)

		live := memoNames("live", maxNodeTargetETags-2)
		seedNodeTargetsMemo(s, now, idle, live, nil)
		// An owner change landed after this entry was built.
		seedNodeTargetsMemo(s, now, idle, []string{"dead-owners"}, func(v *targetsValidity) { v.owners-- })
		// This node's pods changed after its entry was built (a node that
		// left the cluster is exactly this: removing its pods moved its token).
		seedNodeTargetsMemo(s, now, idle, []string{"dead-pods"}, func(v *targetsValidity) { v.pods-- })
		if got := s.memoisedNodes(); got != maxNodeTargetETags {
			t.Fatalf("seeded %d entries, want the cap %d", got, maxNodeTargetETags)
		}

		getETag(t, srv.URL+"/v1/nodes/node1/targets", "") // a node not yet memoised: the sweep runs
		if _, ok := s.memoHolds("node1"); !ok {
			t.Fatal("node1 was not memoised: the sweep freed no room although two entries were provably stale")
		}
		for _, dead := range []string{"dead-owners", "dead-pods"} {
			if _, ok := s.memoHolds(dead); ok {
				t.Errorf("%s survived the sweep: its token is older than the current one, so it can only rebuild", dead)
			}
		}
		if got, want := s.memoisedNodes(), len(live)+1; got != want {
			t.Errorf("memo holds %d entries after the sweep, want %d (every live entry plus node1): "+
				"%d entries whose token still matches were evicted for being older than the TTL",
				got, want, want-got)
		}
		// And the entry kept is genuinely usable, however old: its next
		// revalidation is a 304 without a derivation.
		builds := s.targetBuilds.Load()
		if status, _ := conditionalGet(t, srv.URL+"/v1/nodes/"+live[0]+"/targets", `"seeded"`); status != http.StatusNotModified {
			t.Errorf("revalidating a kept entry last used %s ago (TTL %s) answered %d, want 304", idle, ttl, status)
		}
		if got := s.targetBuilds.Load() - builds; got != 0 {
			t.Errorf("revalidating a kept entry cost %d derivations, want 0", got)
		}
	})

	t.Run("a static cluster larger than the cap still rotates", func(t *testing.T) {
		s := targetsFixture{pods: 3, services: 1, cacheTTL: ttl}.build(t)
		now := time.Now()
		s.now = func() time.Time { return now }
		srv := httptest.NewServer(s.Handler())
		t.Cleanup(srv.Close)
		// Nothing is provably stale, and every entry is idle past the TTL:
		// the least-recently-used fallback must still make room.
		seedNodeTargetsMemo(s, now, idle, memoNames("live", maxNodeTargetETags), nil)
		getETag(t, srv.URL+"/v1/nodes/node1/targets", "")
		if _, ok := s.memoHolds("node1"); !ok {
			t.Error("node1 was not memoised: with nothing provably stale the idle entries must make room")
		}
	})

	t.Run("replacing an entry the memo holds sweeps nothing", func(t *testing.T) {
		s := targetsFixture{pods: 3, services: 1, cacheTTL: ttl}.build(t)
		now := time.Now()
		s.now = func() time.Time { return now }
		srv := httptest.NewServer(s.Handler())
		t.Cleanup(srv.Close)
		seedNodeTargetsMemo(s, now, idle, memoNames("live", maxNodeTargetETags-1), nil)
		seedNodeTargetsMemo(s, now, idle, []string{"node1"}, nil)

		// An unconditional GET rebuilds node1 and re-memoises it. Replacing a
		// held key does not grow the map, so nothing may be evicted for it.
		etag := getETag(t, srv.URL+"/v1/nodes/node1/targets", "")
		if e, ok := s.memoHolds("node1"); !ok || e.etag != etag {
			t.Fatalf("node1's entry after the rebuild = %+v (held %v), want the new tag %s", e, ok, etag)
		}
		if got := s.memoisedNodes(); got != maxNodeTargetETags {
			t.Errorf("memo holds %d entries after re-memoising a held node, want %d: "+
				"replacing an entry ran the at-cap sweep", got, maxNodeTargetETags)
		}
	})

	t.Run("wall-clock fallback: only entries past the TTL are unusable", func(t *testing.T) {
		s := targetsFixture{pods: 3, services: 1, cacheTTL: ttl, noOwnerGen: true}.build(t)
		now := time.Now()
		s.now = func() time.Time { return now }
		srv := httptest.NewServer(s.Handler())
		t.Cleanup(srv.Close)
		young := memoNames("young", maxNodeTargetETags-1)
		seedNodeTargetsMemo(s, now, time.Second, young, nil)
		seedNodeTargetsMemo(s, now, idle, []string{"expired"}, nil)
		getETag(t, srv.URL+"/v1/nodes/node1/targets", "")
		if _, ok := s.memoHolds("expired"); ok {
			t.Error("an unwired entry past the TTL survived the sweep: the wall clock never consults it again")
		}
		if got, want := s.memoisedNodes(), len(young)+1; got != want {
			t.Errorf("memo holds %d entries, want %d (every entry inside its TTL plus node1)", got, want)
		}
	})
}
