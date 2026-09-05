package main

// The owner change token feeds the node-targets ETag memo, and what it counts
// as a "change" decides whether that memo survives contact with a real cluster.
// The token is SHARED across every node, so one spurious bump invalidates every
// agent's memo at once — which makes over-bumping not a small inefficiency but
// the difference between the memo working at fleet scale and not.

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/JohanLindvall/kubescrape/internal/owners"
)

func partial(rv string, labels, annotations map[string]string) *metav1.PartialObjectMetadata {
	return &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{
		Name: "obj", Namespace: "prod", UID: "uid-1", ResourceVersion: rv,
		Labels: labels, Annotations: annotations,
	}}
}

// The one that matters. Everything these informers serve is UID + labels +
// annotations (owners.Resolver.clusterScoped/Resolve via kubemeta.CopyMeta),
// and UID is immutable — so an update touching neither map cannot change any
// response, and must not advance the token.
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
