package services

// InNamespaces memoises its per-namespace sorted snapshot against the index's
// change token. A memo on a discovery path is exactly the kind of optimisation
// that fails silently and expensively — a deleted Service kept in the memo is a
// scrape target the fleet keeps hitting, and a missed update is a port scraped
// at up=0 forever — so every way the index can change has a test that the next
// call reflects it, and the concurrent shape has one too.

import (
	"fmt"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func memoService(ns, name, uid, rv string, port int32) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: ns, UID: types.UID(uid), ResourceVersion: rv,
			Annotations: map[string]string{"prometheus.io/scrape": "true"},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": name},
			Ports:    []corev1.ServicePort{{Name: "http", Port: port, TargetPort: intstr.FromInt32(port)}},
		},
	}
}

func memoNames(t *testing.T, ix *Index, ns string) []string {
	t.Helper()
	out := []string{}
	for _, svc := range ix.InNamespaces([]string{ns})[ns] {
		out = append(out, svc.Name)
	}
	return out
}

func eq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// The memo must be transparent: repeated calls answer exactly what a fresh
// build answers, and a hit takes no lock (Reads counts locked reads, so a hit
// leaves it still).
func TestInNamespacesMemoIsTransparentAndTakesNoLock(t *testing.T) {
	ix := NewIndex()
	for i := range 5 {
		ix.Upsert(memoService("prod", fmt.Sprintf("svc-%d", 5-i), fmt.Sprintf("uid-%d", i), "1", 80))
	}
	want := []string{"svc-1", "svc-2", "svc-3", "svc-4", "svc-5"}
	if got := memoNames(t, ix, "prod"); !eq(got, want) {
		t.Fatalf("first call = %v, want %v", got, want)
	}
	before := ix.Reads()
	for range 10 {
		if got := memoNames(t, ix, "prod"); !eq(got, want) {
			t.Fatalf("memoised call = %v, want %v", got, want)
		}
	}
	if got := ix.Reads() - before; got != 0 {
		t.Errorf("locked reads across 10 memoised calls = %d, want 0", got)
	}
}

// Every mutation kind must invalidate. An Upsert that CHANGES nothing must not
// (that is the informer-resync case the memo exists to survive).
func TestInNamespacesMemoTracksEveryMutation(t *testing.T) {
	ix := NewIndex()
	ix.Upsert(memoService("prod", "a", "uid-a", "1", 80))
	ix.Upsert(memoService("prod", "b", "uid-b", "1", 80))
	if got := memoNames(t, ix, "prod"); !eq(got, []string{"a", "b"}) {
		t.Fatalf("baseline = %v", got)
	}

	// ADD
	ix.Upsert(memoService("prod", "c", "uid-c", "1", 80))
	if got := memoNames(t, ix, "prod"); !eq(got, []string{"a", "b", "c"}) {
		t.Errorf("after an add = %v, want [a b c]", got)
	}

	// UPDATE: same UID and name, new resourceVersion and a changed port. The
	// memo holds *Service POINTERS, so a rebuild that reused the old record
	// would serve the old port with the right name.
	ix.Upsert(memoService("prod", "b", "uid-b", "2", 9090))
	svcs := ix.InNamespaces([]string{"prod"})["prod"]
	for _, svc := range svcs {
		if svc.Name == "b" && (len(svc.Ports) != 1 || svc.Ports[0].Port != 9090) {
			t.Errorf("after an update b has ports %+v, want port 9090", svc.Ports)
		}
	}

	// RESYNC: identical resourceVersion, which Upsert ignores. The memo must
	// still be valid — a resync re-delivers every object, and invalidating on
	// it would mean the memo never survives one.
	before := ix.Reads()
	ix.Upsert(memoService("prod", "b", "uid-b", "2", 9090))
	if got := memoNames(t, ix, "prod"); !eq(got, []string{"a", "b", "c"}) {
		t.Errorf("after a resync = %v, want [a b c]", got)
	}
	if got := ix.Reads() - before; got != 0 {
		t.Errorf("locked reads after an unchanged re-delivery = %d, want 0: a resync must not empty the memo", got)
	}

	// DELETE
	ix.Delete("prod", "uid-b")
	if got := memoNames(t, ix, "prod"); !eq(got, []string{"a", "c"}) {
		t.Errorf("after a delete = %v, want [a c]", got)
	}
}

// A namespace with no Services is remembered as a FACT — otherwise every
// request rebuilds it, which is most namespaces on most nodes — and that
// memory must still yield to a Service arriving in it.
func TestInNamespacesMemoRemembersEmptyAndYieldsToTheFirstService(t *testing.T) {
	ix := NewIndex()
	ix.Upsert(memoService("prod", "a", "uid-a", "1", 80))
	if got := ix.InNamespaces([]string{"empty"}); len(got) != 0 {
		t.Fatalf("an empty namespace must be absent from the result, got %v", got)
	}
	before := ix.Reads()
	for range 5 {
		if got := ix.InNamespaces([]string{"empty"}); len(got) != 0 {
			t.Fatalf("got %v", got)
		}
	}
	if got := ix.Reads() - before; got != 0 {
		t.Errorf("locked reads for 5 repeats of an empty namespace = %d, want 0", got)
	}
	ix.Upsert(memoService("empty", "z", "uid-z", "1", 80))
	if got := memoNames(t, ix, "empty"); !eq(got, []string{"z"}) {
		t.Errorf("after the first Service = %v, want [z]", got)
	}
}

// A namespace named twice in one call must appear once and correctly — the
// pre-memo code had an explicit guard for it.
func TestInNamespacesHandlesARepeatedNamespace(t *testing.T) {
	ix := NewIndex()
	ix.Upsert(memoService("prod", "a", "uid-a", "1", 80))
	ix.Upsert(memoService("prod", "b", "uid-b", "1", 80))
	got := ix.InNamespaces([]string{"prod", "prod", "prod"})
	if len(got) != 1 {
		t.Fatalf("result namespaces = %d, want 1", len(got))
	}
	if len(got["prod"]) != 2 {
		t.Errorf("prod list = %v, want 2 services", got["prod"])
	}
	// …and the NEXT call must not be answered from a poisoned memo. The build
	// loop writes one slot per entry of the missing list, so a repeat that took
	// the "already built" branch without filling its slot memoised the
	// namespace as EMPTY — a populated namespace serving no scrape targets at
	// all until something unrelated changed the index. The first call looks
	// perfect; only the second is wrong.
	for range 3 {
		again := ix.InNamespaces([]string{"prod"})
		if len(again["prod"]) != 2 {
			t.Fatalf("after a repeated-namespace call the memo answers %v, want 2 services", again["prod"])
		}
	}
	// The same shape with the repeat NOT first, so the ordering of the two
	// branches is exercised both ways.
	ix2 := NewIndex()
	ix2.Upsert(memoService("a", "s1", "uid-1", "1", 80))
	ix2.Upsert(memoService("b", "s2", "uid-2", "1", 80))
	ix2.InNamespaces([]string{"a", "b", "b", "a"})
	for _, ns := range []string{"a", "b"} {
		if n := len(ix2.InNamespaces([]string{ns})[ns]); n != 1 {
			t.Errorf("namespace %q memoised as %d services, want 1", ns, n)
		}
	}
}

// Readers racing writers must never see a Service the index no longer holds.
// Run under -race this also pins the lock discipline (sortMu → mu, never the
// other way).
func TestInNamespacesMemoUnderConcurrentChange(t *testing.T) {
	ix := NewIndex()
	for i := range 20 {
		ix.Upsert(memoService("prod", fmt.Sprintf("svc-%02d", i), fmt.Sprintf("uid-%d", i), "1", 80))
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				last := ""
				for _, svc := range ix.InNamespaces([]string{"prod", "other"})["prod"] {
					if svc.Name < last {
						t.Errorf("list is not sorted: %q after %q", svc.Name, last)
						return
					}
					last = svc.Name
				}
			}
		}()
	}
	for i := range 500 {
		ix.Upsert(memoService("prod", fmt.Sprintf("svc-%02d", i%20), fmt.Sprintf("uid-%d", i%20),
			fmt.Sprintf("%d", i+2), 80))
		if i%7 == 0 {
			ix.Delete("prod", types.UID(fmt.Sprintf("uid-%d", i%20)))
		}
	}
	close(stop)
	wg.Wait()

	// And after the churn the memo agrees with the index it memoises.
	ix.Upsert(memoService("prod", "final", "uid-final", "1", 80))
	got := ix.InNamespaces([]string{"prod"})["prod"]
	found := false
	for _, svc := range got {
		if svc.Name == "final" {
			found = true
		}
	}
	if !found {
		t.Errorf("the Service added after the churn is missing from %d results", len(got))
	}
}
