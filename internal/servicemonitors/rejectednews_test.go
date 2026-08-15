package servicemonitors

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// The bool the Upsert*Changed methods return is what the metadata service gates
// its event-shaped reports on — the ignored-fields WARN and counter, and the
// parse-error WARN and counter. On the SUCCESS path "news" and "the index
// changed" are the same statement. On the ERROR path they are not, and that is
// the whole hazard: a monitor that never parsed is not in the index either, so
// the first sighting of a broken monitor and its ten-thousandth resync
// re-delivery are the same map lookup.
//
// Get it wrong in one direction and an operator who applies a malformed monitor
// is told nothing at all, while the resync keeps re-reporting the ones already
// known. Get it wrong in the other and one broken monitor logs and increments
// forever at the resync period.
func TestARejectedMonitorIsNewsOncePerVersion(t *testing.T) {
	ix := NewIndex()
	upsert := func(what string, u *unstructured.Unstructured, wantNews, wantErr bool) {
		t.Helper()
		news, err := ix.UpsertChanged(u)
		if (err != nil) != wantErr {
			t.Fatalf("%s: err = %v, wantErr %v", what, err, wantErr)
		}
		if news != wantNews {
			t.Fatalf("%s: news = %v, want %v", what, news, wantNews)
		}
	}

	upsert("the first delivery of a monitor that does not parse", brokenMonitor("1"), true, true)
	upsert("a resync re-delivering it byte-identical", brokenMonitor("1"), false, true)
	upsert("another resync", brokenMonitor("1"), false, true)
	upsert("an EDIT that is still broken", brokenMonitor("2"), true, true)
	upsert("that edit re-delivered", brokenMonitor("2"), false, true)

	// Fixed, then broken again: the rejection record must have been cleared by
	// the successful parse, or the operator's second mistake is silent.
	upsert("a fix", monitorWithSecret("sm", 0), true, false)
	upsert("broken again at the same version the last rejection used", brokenMonitor("2"), true, true)

	// Deleted while broken, then re-created identically. Without the clear in
	// deleteMonitor the record outlives the object and the re-creation reports
	// nothing — and, since a rejected key is never in the monitor map, that
	// clear is the one that has to happen before deleteMonitor's early return.
	ix.Delete("sm-0", "mon")
	upsert("a re-created monitor, broken the same way", brokenMonitor("2"), true, true)

	// A hand-built object carries no resourceVersion, and for those the version
	// says nothing about the content — the success path's rule, applied here too.
	upsert("an unversioned broken object", brokenMonitor(""), true, true)
	upsert("the same unversioned broken object again", brokenMonitor(""), true, true)
}

// The two kinds share a key space ("namespace/name") and must not share
// rejection state: a broken PodMonitor named like an already-rejected
// ServiceMonitor is its own first sighting.
func TestRejectionStateIsPerMonitorKind(t *testing.T) {
	ix := NewIndex()
	if news, err := ix.UpsertChanged(brokenMonitor("1")); err == nil || !news {
		t.Fatalf("ServiceMonitor: news=%v err=%v; want the first rejection reported", news, err)
	}
	news, err := ix.UpsertPodMonitorChanged(brokenMonitor("1"))
	if err == nil {
		t.Fatal("the broken fixture parsed as a PodMonitor")
	}
	if !news {
		t.Fatal("a broken PodMonitor was treated as already reported because a ServiceMonitor of the same " +
			"namespace/name had been rejected: the two kinds share a key space, not a rejection table")
	}
}

// brokenMonitor is monitorWithSecret's shape with a selector that cannot parse,
// at resourceVersion rv.
func brokenMonitor(rv string) *unstructured.Unstructured {
	u := monitorWithSecret("sm", 0)
	u.SetResourceVersion(rv)
	if err := unstructured.SetNestedField(u.Object, "not-a-selector", "spec", "selector"); err != nil {
		panic(err)
	}
	return u
}
