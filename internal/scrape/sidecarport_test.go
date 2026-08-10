package scrape

// A NATIVE SIDECAR — an initContainer with restartPolicy: Always, the
// recommended sidecar shape since Kubernetes 1.29 — declaring the same port
// NAME as the app container. Service meshes do exactly this ("metrics" on the
// proxy's 15020 beside the app's 9090), so it is not a corner case.
//
// kubemeta.Pod lists the containers in the SPEC's order, which puts
// spec.initContainers FIRST (kubeconvert.FromPod), while
// k8s.io/kubernetes/pkg/api/v1/pod.FindPort — what the endpoints/EndpointSlice
// controller resolves a named targetPort with, and hence what Prometheus'
// endpoints role and prometheus-operator's generated jobs scrape — iterates
// spec.Containers ONLY. So a plain first-in-document-order walk scraped the
// sidecar's port where the whole rest of the stack scrapes the app's: one pod,
// one name, and kubescrape the only component with a different answer.
//
// The rule these tests pin: regular containers first, the init/ephemeral ones
// only as a fallback for a name no regular container declares (containerPortByName's
// doc carries the reasoning for both halves).

import (
	"slices"
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// sidecarPod is the mesh shape: the native sidecar comes FIRST in the document
// (it is an initContainer) and declares "metrics" on 15020; the app declares
// "metrics" on 9090.
func sidecarPod() kubemeta.Pod {
	pod := basePod()
	pod.Containers = []kubemeta.Container{
		{Name: "mesh-proxy", Type: "init", Ports: []kubemeta.ContainerPort{{Name: "metrics", Port: 15020}}},
		{Name: "app", Type: "container", Ports: []kubemeta.ContainerPort{{Name: "metrics", Port: 9090}}},
	}
	pod.Annotations[AnnotationPort] = "metrics"
	return pod
}

// sidecarService reaches that name through a targetPort, like any Service in
// front of a meshed workload.
func sidecarService() *services.Service {
	svc := baseService()
	svc.Ports = []services.Port{{Name: "http", Port: 80, TargetPortName: "metrics"}}
	svc.Annotations[AnnotationPort] = "http"
	return svc
}

// EVERY path resolves the APP's port, because that is the one FindPort returns
// and therefore the one an EndpointSlice carries.
func TestNativeSidecarDoesNotWinAPortNameFromTheAppContainer(t *testing.T) {
	pod := sidecarPod()
	svc := sidecarService()
	const want = int32(9090)

	for _, tc := range []struct {
		path  string
		ports []int32
	}{
		{"pod annotation", targetPorts(PodTargets(pod))},
		{"service annotation targetPort", targetPorts(ServiceTargets(pod, svc))},
		{"servicemonitor endpoint port", targetPorts(MonitorTargets(pod, svc, "ns/sm", servicemonitors.Endpoint{Port: "http"}))},
		{"servicemonitor endpoint targetPort", targetPorts(MonitorTargets(pod, svc, "ns/sm", servicemonitors.Endpoint{TargetPort: ptr(intstr.FromString("metrics"))}))},
		{"podmonitor endpoint port", targetPorts(PodMonitorTargets(pod, "ns/pm", servicemonitors.Endpoint{Port: "metrics"}))},
		{"podmonitor endpoint targetPort", targetPorts(PodMonitorTargets(pod, "ns/pm", servicemonitors.Endpoint{TargetPort: ptr(intstr.FromString("metrics"))}))},
	} {
		if !slices.Equal(tc.ports, []int32{want}) {
			t.Errorf("%s resolved %v, want the app container's %d: podutil.FindPort walks spec.containers only, so the sidecar's declaration is not what the rest of the stack scrapes", tc.path, tc.ports, want)
		}
	}
}

// The regular container wins wherever it sits in the document — the fix is not
// "skip the first container", it is "prefer the regular ones".
func TestRegularContainerWinsWhicheverOrderTheDocumentHasThem(t *testing.T) {
	pod := sidecarPod()
	slices.Reverse(pod.Containers) // app first, sidecar second
	if got := targetPorts(PodTargets(pod)); !slices.Equal(got, []int32{9090}) {
		t.Errorf("app-first document resolved %v, want [9090]", got)
	}
}

// A name NO regular container declares still resolves from the sidecar: that
// is the case FindPort has no answer for at all, so nothing is contradicted,
// and refusing it would silently stop scraping a metrics port a sidecar
// legitimately owns.
func TestSidecarOnlyPortNameStillResolves(t *testing.T) {
	pod := basePod()
	pod.Containers = []kubemeta.Container{
		{Name: "mesh-proxy", Type: "init", Ports: []kubemeta.ContainerPort{{Name: "mesh", Port: 15020}}},
		{Name: "app", Type: "container", Ports: []kubemeta.ContainerPort{{Name: "http", Port: 8080}}},
	}
	pod.Annotations[AnnotationPort] = "mesh"
	if got := targetPorts(PodTargets(pod)); !slices.Equal(got, []int32{15020}) {
		t.Errorf("sidecar-only name resolved %v, want [15020]", got)
	}
	// Through a Service's targetPort too: one resolver, one answer.
	svc := baseService()
	svc.Ports = []services.Port{{Name: "http", Port: 80, TargetPortName: "mesh"}}
	svc.Annotations[AnnotationPort] = "http"
	if got := targetPorts(ServiceTargets(pod, svc)); !slices.Equal(got, []int32{15020}) {
		t.Errorf("sidecar-only name through a Service resolved %v, want [15020]", got)
	}
	// And an EPHEMERAL container's declaration is the same fallback (a debug
	// container attached to a running pod must not change what is scraped, but
	// a name only it declares is still better than no target at all).
	pod.Containers = []kubemeta.Container{
		{Name: "debug", Type: "ephemeral", Ports: []kubemeta.ContainerPort{{Name: "dbg", Port: 7777}}},
		{Name: "app", Type: "container", Ports: []kubemeta.ContainerPort{{Name: "http", Port: 8080}}},
	}
	pod.Annotations[AnnotationPort] = "dbg"
	if got := targetPorts(PodTargets(pod)); !slices.Equal(got, []int32{7777}) {
		t.Errorf("ephemeral-only name resolved %v, want [7777]", got)
	}
}

// A Pod model whose containers carry no Type — a hand-built one, or a future
// producer that leaves the field empty — keeps the plain document order it
// always had. Sorting an unstamped container into the FALLBACK pass would be a
// silent port change on exactly the pods nothing else can classify.
func TestUntypedContainersKeepDocumentOrder(t *testing.T) {
	pod := basePod()
	pod.Containers = []kubemeta.Container{
		{Name: "first", Ports: []kubemeta.ContainerPort{{Name: "dup", Port: 1111}}},
		{Name: "second", Ports: []kubemeta.ContainerPort{{Name: "dup", Port: 2222}}},
	}
	pod.Annotations[AnnotationPort] = "dup"
	if got := targetPorts(PodTargets(pod)); !slices.Equal(got, []int32{1111}) {
		t.Errorf("untyped containers resolved %v, want the first declaration [1111]", got)
	}
}

// explain must say the same thing the derivation does, and say WHY the other
// declaration lost — the target list shows one port and nothing in it mentions
// the sidecar.
func TestExplainNamesTheRegularContainersPortForASidecarCollision(t *testing.T) {
	pod := sidecarPod()
	verdicts, annotated := ExplainPodPorts(pod)
	if !annotated || len(verdicts) != 1 {
		t.Fatalf("verdicts = %+v (annotated=%v), want one", verdicts, annotated)
	}
	if !slices.Equal(verdicts[0].Ports, []int32{9090}) {
		t.Errorf("explain resolves %v, the derivation serves %v", verdicts[0].Ports, targetPorts(PodTargets(pod)))
	}
	for _, want := range []string{`2 containers declare a port named "metrics"`, "port 9090 resolves", "prefers a REGULAR container's declaration"} {
		if !containsNote(verdicts[0].Note, want) {
			t.Errorf("note = %q, want it to mention %q", verdicts[0].Note, want)
		}
	}
}
