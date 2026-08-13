package server

// An informer RESYNC re-delivers every object it holds, byte-identical, on the
// operator's `-resync` period. The monitor→Service cross product is memoised
// behind the two indexes' change tokens precisely because it is expensive
// (measured at 19.8 ms, 9.67 MB and 14,734 allocations per build for 50
// monitors over 2,000 Services), and both indexes used to move their token for
// every delivery rather than every change — so with `-resync` set, a
// 2,000-Service cluster invalidated the memo continuously and essentially every
// agent poll paid a rebuild. The store's pod path has always short-circuited on
// resourceVersion; these two did not.

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

func TestResyncOfUnchangedObjectsDoesNotRebuildTheMonitorMemo(t *testing.T) {
	st := store.New(time.Minute)
	addPod(st)
	monitors := servicemonitors.NewIndex()
	svcs := services.NewIndex()
	s := New(Config{
		Store: st, Services: svcs, Monitors: monitors, Resolver: stubResolver{},
		MaxWait: 500 * time.Millisecond, Ready: closedChan(),
	})

	// What the informers hold: one Service and one ServiceMonitor, each with the
	// resourceVersion the API server stamped.
	svc := func(rv, port string) *corev1.Service {
		return &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name: "web", Namespace: "default", UID: types.UID("svc-uid"), ResourceVersion: rv,
			},
			Spec: corev1.ServiceSpec{
				Selector: map[string]string{"app": "web"},
				Ports:    []corev1.ServicePort{{Name: port, Port: 80, TargetPort: intstr.FromInt32(9090)}},
			},
		}
	}
	mon := func(rv, port string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"metadata": map[string]any{"name": "m", "namespace": "default", "resourceVersion": rv},
			"spec": map[string]any{
				"selector":  map[string]any{"matchLabels": map[string]any{"app": "web"}},
				"endpoints": []any{map[string]any{"port": port}},
			},
		}}
	}
	svcs.Upsert(svc("7", "http"))
	if err := monitors.Upsert(mon("11", "http")); err != nil {
		t.Fatal(err)
	}
	s.nodeTargets("node1")
	if n := s.monBuilds.Load(); n != 1 {
		t.Fatalf("builds after the first request = %d, want 1", n)
	}

	// The resync: both objects re-delivered unchanged, several times over.
	for range 3 {
		svcs.Upsert(svc("7", "http"))
		if err := monitors.Upsert(mon("11", "http")); err != nil {
			t.Fatal(err)
		}
		s.nodeTargets("node1")
	}
	if n := s.monBuilds.Load(); n != 1 {
		t.Fatalf("builds after three resyncs = %d, want 1: a re-delivery that changes nothing must "+
			"not invalidate the monitor→Service memo", n)
	}

	// A genuine edit on either side still rebuilds immediately.
	svcs.Upsert(svc("8", "metrics"))
	s.nodeTargets("node1")
	if n := s.monBuilds.Load(); n != 2 {
		t.Fatalf("builds after a Service edit = %d, want 2", n)
	}
	if err := monitors.Upsert(mon("12", "metrics")); err != nil {
		t.Fatal(err)
	}
	s.nodeTargets("node1")
	if n := s.monBuilds.Load(); n != 3 {
		t.Fatalf("builds after a monitor edit = %d, want 3", n)
	}
}
