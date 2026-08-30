package server

import (
	"strconv"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/JohanLindvall/kubescrape/internal/scrape"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
)

// The MERGE arm used to charge NOTHING against the per-pod byte budget, on the
// grounds that a merge copies no pod document. That is true of the pod document
// and incomplete about the target: a fold grows the held target by its merged
// relabel chain (up to scrape.MaxRelabelChainBytes) and its contributor list
// (scrape.MaxContributorsPerTarget names of up to 317 bytes) — ~26 KiB per
// target, up to ~400 KiB per pod, entirely on TOP of a budget whose whole claim
// is that it charges the WHOLE target document.
//
// Bounded and deterministic, so it was never the unbounded shape the other
// ceilings answer — but a comment that claims something narrower than the truth
// is how the next sibling hides, so the merge arm now charges what it grows.
// Nothing is refused BY the charge (a refused merge would drop relabel rules a
// monitor asked for, changing what is exported to bound a response); the pod's
// budget is simply spent, so the next NEW url is measured against what is
// really being served.
func TestMergedChainsAreChargedAgainstThePerPodByteBudget(t *testing.T) {
	// A moderately fat pod, so the BYTE ceiling is the one that binds (the
	// count ceiling would otherwise refuse first and hide the arithmetic).
	const podBulk = 16 << 10
	st, svcs := fatPodFixture(t, 1, podBulk, 1)

	// Endpoint PAIRS, interleaved: each new URL is immediately followed by a
	// fat-chained endpoint that MERGES into it. Interleaving is the point — a
	// charge only ever refuses a LATER new url, so a fixture that adds every
	// url before merging anything would prove nothing.
	endpoints := make([]any, 0, 2*scrape.MaxPortsPerPod)
	for i := range scrape.MaxPortsPerPod {
		path := "/m" + strconv.Itoa(i)
		endpoints = append(endpoints, map[string]any{"port": "http", "path": path})
		endpoints = append(endpoints, map[string]any{
			"port": "http", "path": path,
			// Just under the per-endpoint parse bound, so the whole chain
			// folds into the holder and the merged ceiling is what stops it.
			"metricRelabelings": relabelRules(60, 200),
		})
	}
	idx := servicemonitors.NewIndex()
	if err := idx.Upsert(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "sm-merge", "namespace": "default"},
		"spec": map[string]any{
			"selector":  map[string]any{"matchLabels": map[string]any{"team": "obs"}},
			"endpoints": endpoints,
		},
	}}); err != nil {
		t.Fatal(err)
	}

	body, targets := fetchTargets(t, st, svcs, idx)
	t.Logf("%d endpoint pairs on a %d KiB pod -> %d targets, %d byte response",
		scrape.MaxPortsPerPod, podBulk>>10, len(targets), len(body))

	// The same invariant the new-URL arm is held to: the whole budget plus the
	// pod's unconditional first target, with a pod document of slack for the
	// response framing. Uncharged, the merged chains rode entirely outside it.
	if want := scrape.MaxTargetBytesPerPod + 2*podBulk; len(body) > want {
		t.Errorf("node targets document is %d bytes for ONE pod (budget %d + the unconditional first target "+
			"= %d); merged relabel chains must be charged against the pod's budget",
			len(body), scrape.MaxTargetBytesPerPod, want)
	}
	if len(targets) == 0 {
		t.Error("the charge refused every target; it must spend the budget, never bound the workload")
	}
}
