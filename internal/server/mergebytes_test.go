package server

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/JohanLindvall/kubescrape/internal/scrape"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
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

// The UPGRADE arm — a monitor target displacing an annotation target on the
// same URL — had the same hole the merge arm had, and it swaps in more: a
// merged relabel chain, the auth references, and a SERVICE VIEW carrying up to
// kubemeta.MaxAnnotationBytes of the Service's annotations. The pod door
// charged the small annotation target it built and the swap was free, so a pod
// behind a fat Service could serve ~400 KiB against a 256 KiB budget with
// d.capped at 0 — the budget understating what is served, which is precisely
// what the byte ceiling exists not to do.
//
// Charging only, never refusing, exactly as the merge arm does: the URL is
// being served either way and refusing here would drop a monitor's declaration
// rather than bound a response. What it buys is that the pod's LATER new urls
// are measured against the truth, which is what this asserts.
func TestUpgradedTargetsAreChargedAgainstThePerPodByteBudget(t *testing.T) {
	// A SMALL pod document, so nothing but the swapped-in material can spend
	// the budget: a fat pod would run the ceiling down through the new-URL arm
	// and prove nothing about the upgrade.
	const (
		podBulk  = 1 << 10
		upgrades = 11 // x ~24 KiB of swapped-in material: just over the budget
		extra    = 5  // and 11+5 = MaxPortsPerPod, so the COUNT ceiling cannot bind
	)
	st, svcs := fatPodFixture(t, 1, podBulk, upgrades)
	// The Service the monitor selects carries the annotations every monitor
	// target copies into its Service view.
	fattenService(t, svcs, kubemeta.MaxAnnotationBytes)

	endpoints := make([]any, 0, upgrades+extra)
	for i := range upgrades {
		// targetPort resolves to the SAME url the pod annotation already holds
		// (default path /metrics), so each of these takes the upgrade arm.
		endpoints = append(endpoints, map[string]any{
			"targetPort":        9000 + i,
			"metricRelabelings": relabelRules(60, 130),
		})
	}
	for i := range extra {
		// Distinct paths: NEW urls, offered after the upgrades have spent the
		// budget. Uncharged upgrades left room for all of them.
		endpoints = append(endpoints, map[string]any{"port": "http", "path": "/late" + strconv.Itoa(i)})
	}
	idx := servicemonitors.NewIndex()
	if err := idx.Upsert(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "sm-upgrade", "namespace": "default"},
		"spec": map[string]any{
			"selector":  map[string]any{"matchLabels": map[string]any{"team": "obs"}},
			"endpoints": endpoints,
		},
	}}); err != nil {
		t.Fatal(err)
	}

	body, targets := fetchTargets(t, st, svcs, idx)
	t.Logf("%d upgraded + %d later urls -> %d targets, %d byte response", upgrades, extra, len(targets), len(body))

	// The upgrades must actually have happened, or the rest is vacuous.
	upgraded := 0
	for i := range targets {
		if targets[i].Monitor != "" && targets[i].Path == "/metrics" {
			upgraded++
		}
	}
	if upgraded == 0 {
		t.Fatalf("no annotation target was upgraded by a monitor endpoint; the fixture no longer "+
			"reaches add's upgrade arm (%d targets)", len(targets))
	}
	if len(targets) == upgrades+extra {
		t.Errorf("all %d targets were served: the upgrade arm swapped in ~%d KiB per target without "+
			"charging it, so the %d later urls were measured against a budget that understates what "+
			"is being served", len(targets), (kubemeta.MaxAnnotationBytes>>10)+8, extra)
	}
	if len(targets) == 0 {
		t.Error("the charge refused every target; it must spend the budget, never bound the workload")
	}
	// And the refusal must read as a SIZE refusal: 11 targets is nowhere near
	// MaxPortsPerPod, so "over the per-pod ceiling of 16 targets" would send an
	// operator counting ports that are not the problem.
	var parsed explainDoc
	doc := fetchExplain(t, st, svcs, idx, "default", "web-0")
	if err := json.Unmarshal([]byte(doc), &parsed); err != nil {
		t.Fatal(err)
	}
	if parsed.CappedTargetsBySize == 0 {
		t.Errorf("/v1/explain reports %d capped targets but none by SIZE: %s",
			parsed.CappedTargets, truncateDoc(doc))
	}

	// What this deliberately does NOT assert is a bound on the body, and the
	// reason is the arm's own semantics: the upgrades are never refused (that
	// would drop a monitor's declaration to bound a response), and the pod door
	// admits all of its urls before the first one arrives — so a pod's served
	// bytes can still exceed the budget by what its upgrades swap in. That
	// excess is bounded and deterministic (MaxPortsPerPod targets x one Service
	// view plus one merged chain each) and it is the same trade the merge arm
	// makes; the property the charge buys is the one above, that nothing
	// AFTERWARDS is measured against a budget that understates the truth.
	t.Logf("served body %d bytes against a %d byte budget: the upgrades themselves are charged, "+
		"not refused", len(body), scrape.MaxTargetBytesPerPod)
}

// fattenService gives the fixture's Service `bulk` bytes of annotations — the
// half of a monitor target's cost that arrives on the UPGRADE rather than
// through the pod document.
func fattenService(t *testing.T, svcs *services.Index, bulk int) {
	t.Helper()
	const perAnnotation = 24 + 128 + 6
	ann := map[string]string{}
	for i := range bulk / perAnnotation {
		ann["bulk.example.com/svc-"+strconv.Itoa(10000+i)] = strings.Repeat("y", 128)
	}
	svcs.Upsert(&corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web", Namespace: "default", UID: types.UID("svc-uid"),
			Labels: map[string]string{"team": "obs"}, Annotations: ann,
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "web"},
			Ports:    []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromString("metrics")}},
		},
	})
}
