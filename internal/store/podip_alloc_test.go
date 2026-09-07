package store

// The address enumeration runs on the informer goroutine holding the store's
// EXCLUSIVE write lock — twice per upsert (the record's old addresses and the
// pod's new ones), again on every delete and once per claimant a promotion
// scans — so every allocation it makes is paid by every container, pod-uid,
// pod-ip and node-targets reader waiting on that lock. It used to make four per
// ordinary upsert (rawIPs' unconditional copy and podAddresses' append-built
// second one, twice) for addresses that needed neither canonicalising nor
// filtering, which is every address on every cluster that spells them the
// ordinary way.

import (
	"slices"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

func TestAddressEnumerationIsAllocationFreeForOrdinaryPods(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race changes escape analysis and adds bookkeeping allocations")
	}
	for _, tc := range []struct {
		name string
		pod  kubemeta.Pod
	}{
		{"single address", kubemeta.Pod{PodIP: "10.0.0.5", PodIPs: []string{"10.0.0.5"}, HostIP: "192.168.1.1", Phase: "Running"}},
		{"dual stack", kubemeta.Pod{PodIP: "10.0.0.5", PodIPs: []string{"10.0.0.5", "fd00::7"}, HostIP: "192.168.1.1", Phase: "Running"}},
		{"no addresses", kubemeta.Pod{HostIP: "192.168.1.1", Phase: "Pending"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var sink []string
			if got := testing.AllocsPerRun(200, func() { sink = podAddresses(tc.pod) }); got != 0 {
				t.Errorf("podAddresses allocates %.1f times per call under the store's write lock; "+
					"an address that needs neither canonicalising nor filtering must be returned as it "+
					"is (sink=%v)", got, sink)
			}
		})
	}
}

// And the copy is still made — correctly — when an address is NOT canonical, or
// when one has to be filtered out. The fast path is a shortcut, never a change
// of answer: the index is keyed in peerip's form and a claim taken from an
// address one enumeration sees and another does not is never released.
func TestAddressEnumerationStillNormalisesAndFilters(t *testing.T) {
	for _, tc := range []struct {
		name string
		pod  kubemeta.Pod
		want []string
	}{
		{"mapped v4 is canonicalised", kubemeta.Pod{PodIPs: []string{"::ffff:10.1.2.3"}, Phase: "Running"}, []string{"10.1.2.3"}},
		{"the second address is canonicalised", kubemeta.Pod{PodIPs: []string{"10.0.0.5", "FD00::0:7"}, Phase: "Running"}, []string{"10.0.0.5", "fd00::7"}},
		{"a zone is stripped", kubemeta.Pod{PodIPs: []string{"fd00::7%eth0"}, Phase: "Running"}, []string{"fd00::7"}},
		{"the host address is dropped", kubemeta.Pod{PodIPs: []string{"192.168.1.1", "10.0.0.5"}, HostIP: "192.168.1.1", Phase: "Running"}, []string{"10.0.0.5"}},
		{"the host address is dropped however it is spelled", kubemeta.Pod{PodIPs: []string{"::ffff:192.168.1.1"}, HostIP: "192.168.1.1", Phase: "Running"}, []string{}},
		{"an empty address is dropped", kubemeta.Pod{PodIPs: []string{"", "10.0.0.5"}, Phase: "Running"}, []string{"10.0.0.5"}},
		{"podIP backstop when podIPs is empty", kubemeta.Pod{PodIP: "::ffff:10.1.2.3", Phase: "Running"}, []string{"10.1.2.3"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The pod's own slice must survive the walk untouched: the fast
			// path returns it BY ALIAS, and it is the value the store serves.
			before := append([]string(nil), tc.pod.PodIPs...)
			got := podAddresses(tc.pod)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("podAddresses = %v, want %v", got, tc.want)
			}
			if !slices.Equal(tc.pod.PodIPs, before) {
				t.Errorf("podAddresses rewrote the pod's own PodIPs: %v, was %v", tc.pod.PodIPs, before)
			}
		})
	}
}
