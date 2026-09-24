package route

// match used to glob every pattern of every route against every resource's
// namespace on every export: linear in the route count, re-done per resource,
// and for a namespace no route claims (the commonest answer) a walk of EVERY
// pattern. The memo in front of it answers the repeats; these pin that it
// answers exactly what the scan would, stays bounded, and is never consulted
// by a Router that has nothing to route to.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"unsafe"
)

func memoRouter() *Router {
	return New(&capDest{}, []Destination{
		{Name: "tenant-a", Namespaces: []string{"team-a-*", "prod"}, Exporter: &capDest{}},
		{Name: "tenant-b", Namespaces: []string{"team-?", "[xy]-ns"}, Exporter: &capDest{}},
		{Name: "catch", Namespaces: []string{"team-*"}, Exporter: &capDest{}},
	})
}

func TestNamespaceMemoAnswersWhatTheGlobScanAnswers(t *testing.T) {
	r := memoRouter()
	names := []string{"team-a-web", "prod", "team-b", "x-ns", "y-ns", "z-ns", "team-zz", "kube-system", "", "team-a-"}
	for pass := range 2 { // the first pass learns, the second is answered by the memo
		for _, ns := range names {
			want, _ := r.globNamespace(ns)
			if got := r.namespaceDest(ns); got != want {
				t.Errorf("pass %d: namespaceDest(%q) = %d, the scan says %d", pass, ns, got, want)
			}
		}
	}
	if _, ok := r.nsMemo.get("kube-system"); !ok {
		t.Error("a namespace no route claims was not remembered; that is the answer that costs the whole scan")
	}
}

func TestNamespaceMemoStopsLearningAtItsBound(t *testing.T) {
	r := memoRouter()
	for i := range maxNamespaceMemo + 50 {
		r.namespaceDest(fmt.Sprintf("ns-%d", i))
	}
	if n := len(*r.nsMemo.m.Load()); n != maxNamespaceMemo {
		t.Errorf("memo holds %d entries, want exactly the %d bound", n, maxNamespaceMemo)
	}
	// Past the bound a name is still answered — by the scan.
	if got := r.namespaceDest("team-a-late"); got != 0 {
		t.Errorf("a namespace seen past the bound resolved to %d, want route 0", got)
	}
}

// A name longer than a Kubernetes namespace can be is matched, not remembered,
// and a remembered name must not share memory with the payload it came from.
func TestNamespaceMemoKeepsOnlyNamespaceShapedCopies(t *testing.T) {
	r := memoRouter()
	long := "team-a-" + strings.Repeat("x", maxMemoNamespaceBytes)
	if got := r.namespaceDest(long); got != 0 {
		t.Fatalf("a long name resolved to %d, want route 0", got)
	}
	if _, ok := r.nsMemo.get(long); ok {
		t.Error("a name longer than a namespace was remembered")
	}

	buf := []byte("team-a-web-and-then-more")
	name := unsafe.String(&buf[0], len("team-a-web"))
	r.namespaceDest(name)
	for k := range *r.nsMemo.m.Load() {
		if k == name && unsafe.StringData(k) == unsafe.StringData(name) {
			t.Error("the memo key aliases the payload's string; it would pin the buffer the value was decoded from")
		}
	}
}

// The destination-less Router exists only to strip the script marker, and must
// not pay for a namespace nothing can route on.
func TestDestinationlessRouterNeverReadsTheNamespace(t *testing.T) {
	r := New(&capDest{}, nil)
	if err := r.ExportLogs(context.Background(), nsLogs("team-a", "team-b")); err != nil {
		t.Fatal(err)
	}
	if m := r.nsMemo.m.Load(); m != nil {
		t.Errorf("a destination-less Router resolved %d namespaces; it has no route to match them against", len(*m))
	}
}

// Learning races with reading on the concurrent export paths (ingest handlers,
// scrape goroutines, the tailer's sweep). Run under -race.
func TestNamespaceMemoConcurrentLearning(t *testing.T) {
	r := memoRouter()
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 200 {
				ns := fmt.Sprintf("team-a-%d", (i*7+g)%64)
				if got := r.namespaceDest(ns); got != 0 {
					t.Errorf("namespaceDest(%q) = %d, want 0", ns, got)
					return
				}
			}
		})
	}
	wg.Wait()
}
