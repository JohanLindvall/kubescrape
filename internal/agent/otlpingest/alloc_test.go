package otlpingest

// Allocation budgets for the ingest request path, ENFORCED here rather than
// only reported by a benchmark (a benchmark cannot fail a build). They skip
// under -race, whose bookkeeping allocations would make any ceiling either
// meaningless or wide enough to let a real regression through.

import (
	"context"
	"fmt"
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

// autoDecisionPush is the shape the auto-mode decision walks: one resource
// naming itself, N data points naming the same object by a different id kind
// (an SDK labelling its own metrics), so every point is visited and none is
// foreign.
func autoDecisionPush(points int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.id", "cafe01")
	dps := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().
		SetEmptyGauge().DataPoints()
	for i := 0; i < points; i++ {
		dp := dps.AppendEmpty()
		dp.SetIntValue(1)
		dp.Attributes().PutStr("k8s.pod.uid", "pod-uid-1")
		dp.Attributes().PutStr("le", fmt.Sprint(i))
	}
	return md
}

// The auto-mode decision walks EVERY data point of the default mode's default
// path, and it built a kind-tagged token per point purely to compare it against
// the resource's — one heap allocation each, the largest non-pdata allocator
// there (a 4 MiB gRPC push is ~67k points, a 16 MiB HTTP push ~270k). The
// comparison and the memo lookups are in-place now; a token is materialised
// only when one has to be STORED, i.e. once per distinct id.
func TestAutoDecisionWalkAllocationBudget(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector's bookkeeping allocations make the ceiling meaningless")
	}
	const points = 2000
	e := NewEnricher(Config{Meta: newMeta(), MetricsMode: MetricsAuto})
	md := autoDecisionPush(points)
	ctx := context.Background()

	got := testing.AllocsPerRun(20, func() {
		cache := newReqCache()
		if !e.resourceModeSuffices(ctx, cache, md) {
			t.Fatal("this payload must take the resource branch; the budget below measures the wrong walk")
		}
	})
	// The per-request cache and its one materialised token, not a per-point cost.
	if want := 20.0; got > want {
		t.Errorf("decision over %d data points allocates %.0f times, want <= %.0f (it was one per point)", points, got, want)
	}
}

// The peer-IP fallback is OPT-IN and off by default, yet its not-applicable
// answer minted a fresh empty map — two allocations — for every id-less
// resource of every push, and memoised nothing, so each one paid again.
func TestPeerFallbackNotApplicableIsAllocationFree(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector's bookkeeping allocations make the ceiling meaningless")
	}
	e := NewEnricher(Config{Meta: newMeta()}) // PeerIPFallback off: the default
	ctx := context.Background()
	cache := newReqCache()

	got := testing.AllocsPerRun(100, func() {
		if _, resolved, rejected := e.peerAttrs(ctx, cache); resolved || rejected {
			t.Fatal("the fallback is disabled; it must resolve nothing")
		}
	})
	if got != 0 {
		t.Errorf("peerAttrs allocates %.1f times per id-less resource, want 0", got)
	}
}

var reqCacheSink *reqCache

// Most pushes write to ONE of the request cache's maps. The probe memo and the
// counted-outcome marker are filled only on their own paths, so allocating them
// up front cost every push the maps it does not use.
func TestNewReqCacheDoesNotPreallocateUnusedMaps(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector's bookkeeping allocations make the ceiling meaningless")
	}
	got := testing.AllocsPerRun(100, func() { reqCacheSink = newReqCache() })
	if want := 2.0; got > want {
		t.Errorf("newReqCache allocates %.0f times, want <= %.0f (the struct and the ids memo)", got, want)
	}
}
