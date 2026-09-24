package owners

import (
	"strconv"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// THE ATTACK the owner-label bound closes, one door over from fatRefs': the
// annotation budget bounded what each owner ADDS in annotations and MaxOwners
// bounded how many owners a pod document carries — but an owner's LABELS were
// copied verbatim, and the API server bounds their count only by the object's
// ~1.5 MiB ceiling. A tenant creates MaxOwners ReplicaSets each carrying ~1 MiB
// of labels and points its pods at all of them: ~8 MiB of owner labels in EVERY
// pod document, and the node-targets response carries one per pod on the first,
// unconditional target — six such pods pushed it past the agent's 64 MiB read
// cap, and the agent then scheduled no annotation or monitor target for ANY pod
// on that node. Nothing selects on an owner's labels, so they are bounded where
// a pod's and a Service's cannot be.
//
// Reverse-patch check: restoring kubemeta.CopyMeta in Resolve serves the full
// ~8 MiB and this fails on the byte bound.
func TestLabelFatOwnersAreBoundedInThePodDocument(t *testing.T) {
	objects := map[string]*metav1.PartialObjectMetadata{}
	refs := make([]metav1.OwnerReference, 0, MaxOwners)
	for i := range MaxOwners {
		name := "rs-" + strconv.Itoa(i)
		labels := make(map[string]string, 15000)
		for j := range 15000 {
			labels["tenant.example.com/l"+strconv.Itoa(j)] = strings.Repeat("v", 63)
		}
		objects["replicasets/default/"+name] = obj("uid-"+name, labels)
		refs = append(refs, metav1.OwnerReference{
			APIVersion: "apps/v1", Kind: "ReplicaSet", Name: name, UID: types.UID("uid-" + name),
		})
	}
	before := obs.MetadataLabelsOmitted.WithLabelValues("ReplicaSet").Value()

	got, _ := fakeResolver(objects).Resolve("default", refs)

	if len(got) != MaxOwners {
		t.Fatalf("resolved %d owners, want %d", len(got), MaxOwners)
	}
	total := 0
	for _, o := range got {
		for k, v := range o.Labels {
			total += len(k) + len(v)
		}
		if !kubemeta.LabelsOmitted(o.Annotations) {
			t.Errorf("owner %s serves a short label set and does not say so: %v", o.Name, o.Annotations)
		}
	}
	if ceiling := MaxOwners * kubemeta.MaxOwnerLabelBytes; total > ceiling {
		t.Fatalf("the pod document carries %d bytes of owner labels, over the %d-byte ceiling "+
			"(MaxOwners x MaxOwnerLabelBytes)", total, ceiling)
	}
	if delta := obs.MetadataLabelsOmitted.WithLabelValues("ReplicaSet").Value() - before; delta != MaxOwners {
		t.Errorf("counted %v label-short owners, want %d", delta, MaxOwners)
	}
}
