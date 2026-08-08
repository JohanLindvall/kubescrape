package attrs

import (
	"slices"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// TestIdentityKeysCoverEveryFixedEmission pins IdentityKeys against the actual
// emissions: it builds a resource through every fixed-key path the built-in
// mapping has — Pod (with an owner of every kindTable kind), Container,
// Service, Identity — and asserts the emitted key set and IdentityKeys are
// EQUAL. The ingest splitter strips a sender's resource by this list before
// re-labelling it as a described object's, so a key emitted but not listed is
// a sender-identity leak onto every split-described object (the drift that
// already happened once, when Service grew k8s.service.name/uid and the
// splitter's then-hand-copied list did not).
//
// The pod deliberately carries no labels: k8s.pod.label.* /
// k8s.namespace.label.* keys are data-derived and documented as outside the
// list.
func TestIdentityKeysCoverEveryFixedEmission(t *testing.T) {
	res := pcommon.NewResource()
	pod := kubemeta.Pod{
		Name:      "web-1",
		Namespace: "default",
		UID:       "uid-1",
		NodeName:  "node1",
		PodIP:     "10.0.0.1",
	}
	for kind := range kindTable {
		pod.Owners = append(pod.Owners, kubemeta.Owner{Kind: kind, Name: "owner-" + kind})
	}
	Pod(res, pod)
	Container(res, kubemeta.Container{Name: "app", ID: "cafe01", Image: "nginx:1", RestartCount: 3})
	Service(res, &kubemeta.Service{Name: "web", UID: "svc-uid"})
	Identity(res)

	keys := IdentityKeys()
	if !slices.IsSorted(keys) {
		t.Errorf("IdentityKeys is not sorted: %v", keys)
	}
	emitted := map[string]bool{}
	res.Attributes().Range(func(k string, _ pcommon.Value) bool {
		emitted[k] = true
		if !slices.Contains(keys, k) {
			t.Errorf("the built-in mapping emitted %q, which IdentityKeys does not list: add it to identityKeys in attrs.go (the ingest splitter strips senders' resources by that list)", k)
		}
		return true
	})
	// The reverse: no dead entries claiming keys nothing can emit.
	for _, k := range keys {
		if !emitted[k] {
			t.Errorf("IdentityKeys lists %q, which even a fully-populated build never emits: remove it, or fix the build in this test to emit it", k)
		}
	}
}

// IdentityKeys hands out a copy: the backing list is package state, and a
// caller appending to the returned slice (the ingest splitter does exactly
// that) must not extend or clobber it for everyone else.
func TestIdentityKeysReturnsACopy(t *testing.T) {
	a := IdentityKeys()
	a[0] = "mutated"
	_ = append(a, "appended")
	if b := IdentityKeys(); !slices.Equal(b, identityKeys) || b[0] == "mutated" {
		t.Fatalf("mutating IdentityKeys' result leaked into the package list: %v", b)
	}
}
