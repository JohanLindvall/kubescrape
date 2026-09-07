package main

// The owner change token feeds the node-targets ETag memo, and what it counts
// as a "change" decides whether that memo survives contact with a real cluster.
// The token is SHARED across every node, so one spurious bump invalidates every
// agent's memo at once — which makes over-bumping not a small inefficiency but
// the difference between the memo working at fleet scale and not.

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/JohanLindvall/kubescrape/internal/owners"
)

func partial(rv string, labels, annotations map[string]string) *metav1.PartialObjectMetadata {
	return &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{
		Name: "obj", Namespace: "prod", UID: "uid-1", ResourceVersion: rv,
		Labels: labels, Annotations: annotations,
	}}
}

// owned is partial() plus the owner references the object itself carries — the
// field owners.Resolve FOLLOWS to append a pod's grandparent (a ReplicaSet's
// Deployment, a Job's CronJob).
func owned(rv string, refs ...metav1.OwnerReference) *metav1.PartialObjectMetadata {
	p := partial(rv, map[string]string{"app": "web"}, nil)
	p.OwnerReferences = refs
	return p
}

func ownerRef(kind, name, uid string, controller bool) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: "apps/v1", Kind: kind, Name: name, UID: types.UID(uid),
		Controller: &controller,
	}
}

// The one that matters. Everything these informers serve is UID + labels +
// annotations + owner references (owners.Resolver.clusterScoped/Resolve via
// kubemeta.CopyMeta), and UID is immutable — so an update touching none of
// those cannot change any response, and must not advance the token.
//
// A resourceVersion comparison is NOT sufficient for that, which is the trap
// this test exists to hold shut: the API server changes the resourceVersion on
// every write including status-only ones, and the objects behind AllGVRs are
// written constantly for reasons the metadata never sees. A kubelet rewrites
// its Node's status on nodeStatusReportFrequency (5 minutes by default), so a
// 200-node cluster produces a node write about every 1.5 seconds; a Deployment
// or ReplicaSet's status moves on every scale and rollout; a Job's active and
// succeeded counts move throughout its life. Against agents polling every 30s,
// an RV-keyed token would be bumped tens of times between one agent's polls and
// the memo would never validate — while a benchmark, which has no such churn,
// would still report the full win.
func TestStatusOnlyUpdatesDoNotAdvanceTheOwnerToken(t *testing.T) {
	var c owners.Changes
	h := ownerChangeHandler(&c, func() {})

	for _, tc := range []struct {
		name     string
		old, new *metav1.PartialObjectMetadata
	}{
		{
			// The shape a kubelet, a Deployment controller or a Job controller
			// produces: a new resourceVersion, identical metadata.
			name: "status write bumps the resourceVersion only",
			old:  partial("100", map[string]string{"app": "web"}, map[string]string{"team": "obs"}),
			new:  partial("101", map[string]string{"app": "web"}, map[string]string{"team": "obs"}),
		},
		{
			// A resync re-delivering the same object. Covered by the same rule,
			// so the token needs no separate resync special case.
			name: "resync re-delivers an identical object",
			old:  partial("100", map[string]string{"app": "web"}, nil),
			new:  partial("100", map[string]string{"app": "web"}, nil),
		},
		{
			name: "both maps empty, spelled two ways",
			old:  partial("100", nil, nil),
			new:  partial("101", map[string]string{}, map[string]string{}),
		},
		{
			// The owner-reference compare must not be a resourceVersion
			// compare in disguise: an identical chain, re-delivered.
			name: "an unchanged owner chain",
			old:  owned("100", ownerRef("Deployment", "web", "dep-uid", true)),
			new:  owned("101", ownerRef("Deployment", "web", "dep-uid", true)),
		},
		{
			// blockOwnerDeletion is not served by anything, so a
			// garbage-collector rewrite of it must not invalidate the fleet's
			// memos.
			name: "blockOwnerDeletion flipped",
			old:  owned("100", withBlock(ownerRef("Deployment", "web", "dep-uid", true), false)),
			new:  owned("101", withBlock(ownerRef("Deployment", "web", "dep-uid", true), true)),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := c.Generation()
			h.UpdateFunc(tc.old, tc.new)
			if got := c.Generation(); got != before {
				t.Errorf("the token advanced (%d -> %d) for an update that changes nothing "+
					"this package serves. Every node's targets memo is invalidated by that, "+
					"cluster-wide, on every such write.", before, got)
			}
		})
	}
}

// The other direction, which is the safety property: anything that CAN change a
// served response must advance the token. Failing this way serves a stale
// target list, so these cases are the ones to be conservative about.
func TestServedChangesAdvanceTheOwnerToken(t *testing.T) {
	for _, tc := range []struct {
		name     string
		old, new *metav1.PartialObjectMetadata
	}{
		{"a label changed", partial("100", map[string]string{"app": "web"}, nil),
			partial("101", map[string]string{"app": "api"}, nil)},
		{"a label added", partial("100", map[string]string{"app": "web"}, nil),
			partial("101", map[string]string{"app": "web", "tier": "fe"}, nil)},
		{"a label removed", partial("100", map[string]string{"app": "web", "tier": "fe"}, nil),
			partial("101", map[string]string{"app": "web"}, nil)},
		{"an annotation changed", partial("100", nil, map[string]string{"team": "obs"}),
			partial("101", nil, map[string]string{"team": "sre"})},
		{"an annotation added", partial("100", nil, nil),
			partial("101", nil, map[string]string{"team": "obs"})},
		// The owner-reference cases. owners.Resolve reads the CACHED OWNER
		// OBJECT's OwnerReferences to append the followed parent, so an
		// ownerReferences-only rewrite of a ReplicaSet or Job changes the
		// Owners chain of every pod it owns — and with it attrs.ServiceName,
		// hence half the Prometheus job of every series the fleet exports for
		// that workload. Nothing else bumps: the pods are untouched, so the
		// store's generation does not move either, and the node-targets memo
		// answered 304 with a full max-age indefinitely.
		{"an owner reference removed", owned("100", ownerRef("Deployment", "web", "dep-uid", true)),
			owned("101")},
		{"an owner reference added (re-adoption)", owned("100"),
			owned("101", ownerRef("Deployment", "web", "dep-uid", true))},
		{"the owner was recreated under its old name (new UID)",
			owned("100", ownerRef("Deployment", "web", "dep-uid", true)),
			owned("101", ownerRef("Deployment", "web", "dep-uid-2", true))},
		{"the owner reference was renamed",
			owned("100", ownerRef("Deployment", "web", "dep-uid", true)),
			owned("101", ownerRef("Deployment", "api", "dep-uid", true))},
		{"the owner reference changed kind",
			owned("100", ownerRef("Deployment", "web", "dep-uid", true)),
			owned("101", ownerRef("StatefulSet", "web", "dep-uid", true))},
		{"the controller flag was cleared",
			owned("100", ownerRef("Deployment", "web", "dep-uid", true)),
			owned("101", ownerRef("Deployment", "web", "dep-uid", false))},
		{"two references were reordered (the chain is emitted in their order)",
			owned("100", ownerRef("Deployment", "web", "a", true), ownerRef("Deployment", "api", "b", false)),
			owned("101", ownerRef("Deployment", "api", "b", false), ownerRef("Deployment", "web", "a", true))},
		{
			// Neither side is the type the metadata informer delivers. The
			// token must move rather than assume: an unrecognised shape is not
			// evidence that nothing changed.
			name: "an unexpected object type",
			old:  nil, new: nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var c owners.Changes
			h := ownerChangeHandler(&c, func() {})
			var oldObj, newObj any = tc.old, tc.new
			if tc.old == nil {
				oldObj, newObj = "not-metadata", "not-metadata either"
			}
			h.UpdateFunc(oldObj, newObj)
			if c.Generation() == 0 {
				t.Errorf("the token did not advance for %q — a change this package serves "+
					"went unnoticed, which is how the targets memo serves a stale list", tc.name)
			}
		})
	}

	// Add and delete always move it: an object appearing or disappearing
	// changes what every pod owned by it resolves to.
	var c owners.Changes
	h := ownerChangeHandler(&c, func() {})
	h.AddFunc(partial("1", nil, nil))
	if c.Generation() == 0 {
		t.Error("add did not advance the token")
	}
	before := c.Generation()
	h.DeleteFunc(partial("1", nil, nil))
	if c.Generation() == before {
		t.Error("delete did not advance the token")
	}
}

// withBlock sets blockOwnerDeletion, which nothing serves.
func withBlock(ref metav1.OwnerReference, block bool) metav1.OwnerReference {
	ref.BlockOwnerDeletion = &block
	return ref
}
