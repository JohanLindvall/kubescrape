package owners

import (
	"bytes"
	"log/slog"
	"strconv"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// fatRefs builds n ReplicaSet ownerReferences and the cached metadata for them,
// each carrying an annotation of annBytes.
//
// THE ATTACK this cap exists for: Kubernetes bounds neither how many
// ownerReferences an object may carry nor what an owner may annotate, and the
// owner objects can be SHARED by every pod on the node. A tenant with edit
// rights in ONE namespace creates N fat ReplicaSets and points every pod's
// ownerReferences at all of them; the resolver attached one labels+annotations
// copy per reference (non-controller ones included — only `follow` was ever
// restricted), and /v1/nodes/{node}/targets carries the resulting document once
// per scrapeable pod on the node, re-derived and re-marshalled on every agent
// poll, in the singleton the chart requests 128Mi for with no memory limit.
func fatRefs(t *testing.T, n, annBytes int) (*Resolver, []metav1.OwnerReference) {
	t.Helper()
	objects := map[string]*metav1.PartialObjectMetadata{}
	refs := make([]metav1.OwnerReference, 0, n)
	for i := range n {
		name := "rs-" + strconv.Itoa(i)
		m := obj("uid-"+name, map[string]string{"app": "web"})
		m.Annotations = map[string]string{"team.example.com/inventory": strings.Repeat("x", annBytes)}
		objects["replicasets/default/"+name] = m
		refs = append(refs, metav1.OwnerReference{
			APIVersion: "apps/v1", Kind: "ReplicaSet", Name: name, UID: types.UID("uid-" + name),
		})
	}
	return fakeResolver(objects), refs
}

// annotationsOmittedCount reads the annotation-budget counter for one kind.
func annotationsOmittedCount(kind string) float64 {
	return obs.MetadataAnnotationsOmitted.WithLabelValues(kind).Value()
}

func TestOwnerChainIsCappedCountedAndReported(t *testing.T) {
	const refs = 100
	before := counter("ReplicaSet", reasonOwnersCapped)
	r, list := fatRefs(t, refs, 200<<10)
	var buf bytes.Buffer
	r.log = slog.New(slog.NewTextHandler(&buf, nil))

	got, omitted := r.Resolve("default", list)

	if len(got) > MaxOwners {
		t.Errorf("resolved %d owners from %d references; the ceiling is %d and every entry carries the "+
			"owner's whole labels+annotations into every pod document that names it", len(got), refs, MaxOwners)
	}
	if want := refs - len(got); omitted != want {
		t.Errorf("reported %d omitted owners, want %d; a truncated chain that reports 0 reads as a complete one",
			omitted, want)
	}
	if delta := counter("ReplicaSet", reasonOwnersCapped) - before; delta != float64(omitted) {
		t.Errorf("counted %v refused owners, want %d", delta, omitted)
	}
	line := buf.String()
	if !strings.Contains(line, "level=WARN") || !strings.Contains(line, "omitted="+strconv.Itoa(omitted)) {
		t.Errorf("the cap was not reported: %q", line)
	}
	if n := strings.Count(line, "level=WARN"); n != 1 {
		t.Errorf("logged %d lines for ONE resolution of %d references; the cap must not log per refused "+
			"reference (and must not spend the keyed throttle tables the RBAC warnings depend on)", n, refs)
	}
}

// The cap must bound the DOCUMENT, which is the whole reason it exists. This is
// the same measurement the finding was written from: with the two annotation
// ceilings and this one in place, a pod naming 100 fat owners is a constant.
func TestOwnerCapBoundsThePodDocument(t *testing.T) {
	r, list := fatRefs(t, 100, 200<<10)
	owners, omitted := r.Resolve("default", list)
	pod := kubemeta.Pod{Name: "web-0", Namespace: "default", Owners: owners, OwnersOmitted: omitted}
	n := 0
	for i := range pod.Owners {
		for k, v := range pod.Owners[i].Annotations {
			n += len(k) + len(v)
		}
	}
	// MaxOwners x kubemeta's per-object annotation budget, plus the omission
	// notes. Unbounded, this measured 100 x 200 KiB = ~20 MB in ONE document.
	if want := MaxOwners * (kubemeta.MaxAnnotationBytes + 2048); n > want {
		t.Errorf("the owner chain contributes %d annotation bytes to one pod document, want at most %d", n, want)
	}
	if pod.OwnersOmitted == 0 {
		t.Error("the served document does not say the chain is truncated")
	}
}

// The legitimate case is ONE controller reference and the one parent this
// resolver follows. Nothing about the cap may touch it.
func TestTheOrdinaryChainIsUntouchedByTheCap(t *testing.T) {
	ctrl := true
	r := fakeResolver(map[string]*metav1.PartialObjectMetadata{
		"replicasets/default/web-abc": obj("rs-uid", map[string]string{"app": "web"}, metav1.OwnerReference{
			APIVersion: "apps/v1", Kind: "Deployment", Name: "web", UID: "dep-uid", Controller: &ctrl,
		}),
		"deployments/default/web": obj("dep-uid", map[string]string{"team": "core"}),
	})
	got, omitted := r.Resolve("default", []metav1.OwnerReference{{
		APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web-abc", UID: "rs-uid", Controller: &ctrl,
	}})
	if len(got) != 2 || omitted != 0 {
		t.Fatalf("the ReplicaSet -> Deployment chain resolved to %d owners (omitted %d), want 2 and 0", len(got), omitted)
	}
}

// An owner whose own annotations are over the budget still resolves — the
// object is served, its blob is not — and the refusal is counted with the
// OBJECT's kind, never the ownerReference's (which names arbitrary CRDs).
func TestAnOwnersOversizedAnnotationsAreOmittedAndCounted(t *testing.T) {
	before := annotationsOmittedCount("ReplicaSet")
	r, list := fatRefs(t, 1, 200<<10)
	got, _ := r.Resolve("default", list)
	if len(got) != 1 {
		t.Fatalf("got %d owners, want 1", len(got))
	}
	if _, ok := got[0].Annotations["team.example.com/inventory"]; ok {
		t.Error("a 200 KiB owner annotation was served")
	}
	if !kubemeta.AnnotationsOmitted(got[0].Annotations) {
		t.Errorf("the owner document does not say it is short: %v", got[0].Annotations)
	}
	if delta := annotationsOmittedCount("ReplicaSet") - before; delta != 1 {
		t.Errorf("counted %v annotation omissions, want 1", delta)
	}
}
