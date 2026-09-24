package server

import (
	"log/slog"
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

// A ServiceMonitor that selects TWO Services in front of one pod offers each
// endpoint through both — the ordinary shape is a ClusterIP Service and a
// headless one sharing labels — and an endpoint naming a port only one of them
// declares still yields the pod's target. The unresolved-endpoint warning used
// to fire per (Service, endpoint), inside the per-Service loop, so it said "the
// pod is simply absent from the target list" about a pod the same response was
// serving, and took the monitor's 30-minute throttle slot away from a pod that
// genuinely was absent. The verdict is per POD: only an endpoint that resolved
// through NONE of the pod's Services is reported. Both orders, since the
// matched Services are walked by name and the failing one may come first.
func TestEndpointResolvingThroughAnotherSelectedServiceIsNotWarned(t *testing.T) {
	for _, sibling := range []string{"a-headless", "web-headless"} {
		t.Run(sibling, func(t *testing.T) {
			s, h := capWarnFixture(t, []any{map[string]any{"port": "http"}})
			headless := monitorSelectedService()
			headless.Name, headless.UID = sibling, types.UID(sibling+"-uid")
			headless.Spec.ClusterIP = corev1.ClusterIPNone
			headless.Spec.Ports = []corev1.ServicePort{{Name: "grpc", Port: 9000, TargetPort: intstr.FromInt32(9000)}}
			s.services.Upsert(headless)

			targets, _ := s.nodeTargets("node1")
			if len(targets) != 1 || targets[0].Monitor != "default/sm-many" {
				t.Fatalf("the endpoint resolves through Service web, so the pod must be served once: %+v", targets)
			}
			if lines := h.matching("names a port the selected pod does not declare"); len(lines) != 0 {
				t.Errorf("warned that a SERVED pod yields no target: %v", lines)
			}
			if n := s.warnUnresolved.Len(); n != 0 {
				t.Errorf("the false report claimed %d throttle slot(s), suppressing a genuine one for 30 minutes", n)
			}
		})
	}

	// And an endpoint that resolves through NEITHER Service is still reported,
	// once, for the pod.
	s, h := capWarnFixture(t, []any{map[string]any{"port": "typo-metrics"}})
	headless := monitorSelectedService()
	headless.Name, headless.UID = "web-headless", "web-headless-uid"
	s.services.Upsert(headless)
	if targets, _ := s.nodeTargets("node1"); len(targets) != 0 {
		t.Fatalf("the fixture must resolve to nothing; got %+v", targets)
	}
	if lines := h.matching("names a port the selected pod does not declare"); len(lines) != 1 {
		t.Errorf("want one warning for a pod no Service resolves the endpoint on, got %v", lines)
	}
}

// unresolvedFleet is pods replicas behind monitorSelectedService, selected by
// one ServiceMonitor with the given endpoints.
func unresolvedFleet(t *testing.T, pods int, eps []any) *Server {
	t.Helper()
	st := store.New(time.Minute)
	for i := range pods {
		name := "web-" + strconv.Itoa(i)
		st.UpsertPod(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: "default", UID: types.UID("pod-" + name), ResourceVersion: "1",
				Labels: map[string]string{"app": "web"},
			},
			Spec: corev1.PodSpec{NodeName: "node1", Containers: []corev1.Container{{
				Name: "app", Ports: []corev1.ContainerPort{{Name: "metrics", ContainerPort: 9090}},
			}}},
			Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.1.3." + strconv.Itoa(i+1)},
		})
	}
	idx := services.NewIndex()
	idx.Upsert(monitorSelectedService())
	return New(Config{
		Store: st, Services: idx, Monitors: newMonitorIndex(t, "sm-broken", eps),
		Resolver: stubResolver{}, MaxWait: time.Second, Ready: closedChan(), Log: slog.New(&recordingHandler{}),
	})
}

// The warning's throttle key is (kind, monitor) — no pod, no port — so inside
// one derivation every question after the first for a key has a known answer.
// Asking the dedupe table anyway built the key, read the clock and took the
// table's mutex (shared by every derivation in the fleet) once per pod per
// unresolved endpoint: measured at 128 broken endpoints x 110 pods, 14,098
// allocations and about half the derivation's time. The derivation's cost on
// this path must not grow with pods x endpoints.
func TestUnresolvedEndpointCostDoesNotGrowWithPodsTimesEndpoints(t *testing.T) {
	if testrace.Enabled {
		t.Skip("allocation counts are meaningless under -race")
	}
	const pods = 30
	allocs := func(endpoints int) float64 {
		eps := make([]any, 0, endpoints)
		for i := range endpoints {
			eps = append(eps, map[string]any{"port": "typo-" + strconv.Itoa(i)})
		}
		s := unresolvedFleet(t, pods, eps)
		// Warm: the monitor memo, and the one line the throttle allows.
		if targets, _ := s.nodeTargets("node1"); len(targets) != 0 {
			t.Fatalf("the fixture must resolve to nothing; got %d targets", len(targets))
		}
		return testing.AllocsPerRun(10, func() { s.nodeTargets("node1") })
	}
	one, many := allocs(1), allocs(64)
	// The per-pod scratch grows once per derivation (a slice of the failed
	// endpoints), never once per (pod, endpoint): 30 x 63 extra questions put
	// ~1,900 allocations here.
	if many > one+16 {
		t.Errorf("a derivation over %d pods costs %.0f allocations with 64 unresolved endpoints against %.0f with one: "+
			"the throttle is being asked per (pod, endpoint) rather than once per monitor", pods, many, one)
	}
}
