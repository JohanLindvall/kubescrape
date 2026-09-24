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

func withAPIVersion(ref metav1.OwnerReference, apiVersion string) metav1.OwnerReference {
	ref.APIVersion = apiVersion
	return ref
}

// The one that matters. Everything these informers serve is UID + labels +
// annotations + owner references (owners.Resolver.clusterScoped/Resolve via
// kubemeta.CopyMeta) — so an update touching none of those cannot change any
// response, and must not advance the token. (The UID is immutable per OBJECT,
// but the informer is keyed by namespace/name, so a recreated object CAN arrive
// as an update with a new one; TestServedChangesAdvanceTheOwnerToken holds that.)
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
		// The object's OWN UID. Immutable per object, but the informer is
		// keyed by namespace/name: an owner deleted and recreated under the
		// same name inside a relist gap arrives as an UPDATE with a new UID
		// and, from a template, identical maps. Resolve's answer changes —
		// the uid_mismatch arm stops lending its labels to pods naming the
		// old UID, and a Namespace's UID is served verbatim.
		{"the object itself was recreated under its name (new UID only)",
			partial("100", map[string]string{"app": "web"}, nil),
			withUID(partial("101", map[string]string{"app": "web"}, nil), "uid-2")},
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
		// The apiVersion is served verbatim AND decides whether Resolve
		// recognises the reference at all (ownerRow matches on its group), so
		// moving it to a group this service does not watch stops the parent
		// being enriched or followed. No other field changes here, which is
		// what makes this the case that pins the comparison's APIVersion term.
		{"the owner reference changed apiVersion",
			owned("100", ownerRef("Deployment", "web", "dep-uid", true)),
			owned("101", withAPIVersion(ownerRef("Deployment", "web", "dep-uid", true), "example.com/v1"))},
		{"the owner reference changed version within its group",
			owned("100", ownerRef("Deployment", "web", "dep-uid", true)),
			owned("101", withAPIVersion(ownerRef("Deployment", "web", "dep-uid", true), "apps/v1beta2"))},
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

// withUID sets the object's own UID (partial() hard-codes "uid-1").
func withUID(p *metav1.PartialObjectMetadata, uid string) *metav1.PartialObjectMetadata {
	p.UID = types.UID(uid)
	return p
}

// The Node informer feeds NO owner token. No node-targets derivation reads Node
// metadata (only GET /v1/nodes/{node}/metadata does, and its ETag is a body
// hash), so a Node add, delete or label edit — autoscaler churn is routine —
// lapsed every node's targets memo for a change no targets document can show.
// Every other resource main wires is read by the derivation and must feed it.
func TestNodeInformerFeedsNoOwnerToken(t *testing.T) {
	var c owners.Changes
	for _, gvr := range owners.AllGVRs {
		got := ownerTokenFor(gvr, &c)
		switch {
		case gvr == owners.NodeGVR && got != nil:
			t.Errorf("%s feeds the owner token: every node's targets memo lapses on node churn, "+
				"which no targets document can show", gvr.Resource)
		case gvr != owners.NodeGVR && got != &c:
			t.Errorf("%s does not feed the owner token: a change to it would leave the targets memo stale", gvr.Resource)
		}
	}
	// And the handler it gets is still safe to drive: a nil token accepts Bump.
	h := ownerChangeHandler(ownerTokenFor(owners.NodeGVR, &c), func() {})
	h.AddFunc(partial("1", nil, nil))
	h.UpdateFunc(partial("1", nil, nil), partial("2", map[string]string{"a": "b"}, nil))
	h.DeleteFunc(partial("2", nil, nil))
	if c.Generation() != 0 {
		t.Fatalf("Node events moved the shared owner token to %d", c.Generation())
	}
}
