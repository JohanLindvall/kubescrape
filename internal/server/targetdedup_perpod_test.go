package server

// Two pods on one node can legitimately produce the SAME scrape URL: hostNetwork
// pods share the node address and every port on it, and so — for the length of
// the recycle window the store's whole ipSeq machinery exists for — do two
// Running pods reporting one recycled pod IP.
//
// That shape is what the per-pod half of the dedup and the last tiebreak of the
// target sort are for, and neither was pinned: `clear(d.urlOwner)` could be
// deleted (one of the two pods silently stops being scraped) and so could the
// Pod.UID tiebreak (the response body, hence its ETag, becomes a function of
// PodsOnNode's map iteration). Both are silent — the first has no counter and no
// up=0, because the target never appears at all; the second just turns every
// agent poll into a full 200 forever.

import (
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// sharedAddressServer puts n annotated hostNetwork pods on node1, all reporting
// the node's address, so every one of them resolves to the identical URL,
// Source and (empty) Monitor.
func sharedAddressServer(t *testing.T, n int) *Server {
	t.Helper()
	st := store.New(time.Minute)
	for i := range n {
		st.UpsertPod(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:            fmt.Sprintf("hostnet-%d", i),
				Namespace:       "default",
				UID:             types.UID(fmt.Sprintf("pod-uid-%d", i)),
				ResourceVersion: "1",
				Annotations: map[string]string{
					"prometheus.io/scrape": "true",
					"prometheus.io/port":   "9090",
				},
			},
			Spec: corev1.PodSpec{NodeName: "node1", HostNetwork: true},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning, PodIP: "192.168.1.1", HostIP: "192.168.1.1",
			},
		})
	}
	return New(Config{
		Store: st, Services: services.NewIndex(), Resolver: stubResolver{},
		MaxWait: 500 * time.Millisecond, Ready: closedChan(),
	})
}

// The dedup is PER POD. Carrying urlOwner across pods makes the second pod's
// target collide with the first one's — and an annotation target cannot replace
// an annotation target (configuredTarget is false for both), so it is neither
// appended nor promoted: the pod simply stops being scraped, with no counter, no
// log and no up=0, because no target for it is ever served.
func TestTargetsOfTwoPodsSharingOneURLAreBothServed(t *testing.T) {
	s := sharedAddressServer(t, 2)
	targets, _ := s.nodeTargets("node1")
	if len(targets) != 2 {
		t.Fatalf("targets = %d, want 2: both annotated pods on the shared address must be scraped", len(targets))
	}
	uids := map[string]bool{}
	for _, tg := range targets {
		if tg.URL != "http://192.168.1.1:9090/metrics" {
			t.Errorf("URL = %q, want the shared address", tg.URL)
		}
		uids[tg.Pod.UID] = true
	}
	if len(uids) != 2 {
		t.Fatalf("the two targets name %d distinct pods, want 2: %v", len(uids), uids)
	}
}

// With the URL, Monitor and Source all tied, the Pod.UID tiebreak is the only
// thing making the sort a TOTAL order — and sort.Slice is not stable, so
// without it the served order is PodsOnNode's map iteration order. The body is
// hashed into the ETag, so an order that shuffles per request mints a fresh
// ETag every time and every agent poll on that node re-downloads the whole
// pod set (measured elsewhere in this repo at 2.21 MB for 110 pods) instead of
// getting a 304, forever.
//
// The Monitor and Source tiebreaks above it order more finely than stability
// needs — two targets can only tie on all of URL, Monitor and Source when they
// belong to different pods, since two monitors at one URL on ONE pod merge into
// a single target. They group a URL's targets by declaration; the UID is what
// makes the comparator separate every pair.
func TestNodeTargetsOrderIsStableAcrossRebuilds(t *testing.T) {
	s := sharedAddressServer(t, 4)
	seq := func() string {
		targets, _ := s.nodeTargets("node1")
		out := ""
		for _, tg := range targets {
			out += fmt.Sprintf("%s|%s|%s|%s\n", tg.URL, tg.Source, tg.Monitor, tg.Pod.UID)
		}
		return out
	}
	first := seq()
	for i := range 200 {
		if got := seq(); got != first {
			t.Fatalf("rebuild %d served a different order for an unchanged node:\n%s\nwant\n%s\n"+
				"the body is the ETag, so a per-request order defeats 304 revalidation entirely",
				i, got, first)
		}
	}
}
