package scrape

// A pod declaring ONE container-port name on two containers. Kubernetes only
// WARNS about that at admission, so it reaches the store, and the resolution
// paths one request runs disagreed about it: the pod annotation resolved the
// name to EVERY declaration (two targets) while the Service's named targetPort
// and a monitor endpoint resolved it to the FIRST (one target) — the same pod,
// the same name, two answers in one response.
//
// They now share ONE resolver (containerPortByName, whose doc carries the
// first-declaration rule and the EndpointSlice/prometheus-operator fidelity
// argument for it), and these tests are what hold every path to it. The
// asymmetry that made the disagreement harmful is the point: the monitor path
// resolves ONE URL by contract — MonitorTargetURL is the identity the server
// dedups and merges on — so a path resolving a second port produced a target
// no monitor endpoint could upgrade, i.e. a bare service-source target next to
// a monitor-derived one carrying the CR's bearer token and drop rules.

import (
	"slices"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// dupPortNamePod declares "dup" twice, on two containers.
func dupPortNamePod() kubemeta.Pod {
	pod := basePod()
	pod.Containers = []kubemeta.Container{
		{Name: "app", Ports: []kubemeta.ContainerPort{{Name: "dup", Port: 9100}}},
		{Name: "sidecar", Ports: []kubemeta.ContainerPort{{Name: "dup", Port: 9200}}},
	}
	pod.Annotations[AnnotationPort] = "dup"
	return pod
}

// dupPortNameService selects it by that name.
func dupPortNameService() *services.Service {
	svc := baseService()
	svc.Ports = []services.Port{{Name: "metrics", Port: 80, TargetPortName: "dup"}}
	return svc
}

func targetPorts(ts []kubemeta.ScrapeTarget) []int32 {
	out := make([]int32, len(ts))
	for i, t := range ts {
		out[i] = t.Port
	}
	return out
}

// EVERY path that turns a container-port NAME into a pod port must give one
// answer for one pod: the first declaration. A path resolving a second port is
// not merely inconsistent — the monitor path cannot follow it (one endpoint,
// one URL), so the extra target is served with none of the configuration of
// the CR that selected the pod.
func TestEveryPortPathAgreesOnADuplicateName(t *testing.T) {
	pod := dupPortNamePod()
	svc := dupPortNameService()
	want := []int32{9100}

	for _, tc := range []struct {
		path  string
		ports []int32
	}{
		{"pod annotation", targetPorts(PodTargets(pod))},
		{"service annotation targetPort", targetPorts(ServiceTargets(pod, svc))},
		{"servicemonitor endpoint port", targetPorts(MonitorTargets(pod, svc, "ns/sm", servicemonitors.Endpoint{Port: "metrics"}))},
		{"servicemonitor endpoint targetPort", targetPorts(MonitorTargets(pod, svc, "ns/sm", servicemonitors.Endpoint{TargetPort: ptr(intstr.FromString("dup"))}))},
		{"podmonitor endpoint port", targetPorts(PodMonitorTargets(pod, "ns/pm", servicemonitors.Endpoint{Port: "dup"}))},
		{"podmonitor endpoint targetPort", targetPorts(PodMonitorTargets(pod, "ns/pm", servicemonitors.Endpoint{TargetPort: ptr(intstr.FromString("dup"))}))},
	} {
		if !slices.Equal(tc.ports, want) {
			t.Errorf("%s resolved %v, want %v: one name on one pod must resolve the same however the target was discovered", tc.path, tc.ports, want)
		}
	}
}

// The other declarations are still reachable — by NUMBER, and without a port
// annotation at all. Nothing here is a target that cannot be asked for.
func TestDuplicateNameOtherDeclarationsAreReachableByNumber(t *testing.T) {
	pod := dupPortNamePod()
	pod.Annotations[AnnotationPort] = "9100,9200"
	if got, want := targetPorts(PodTargets(pod)), []int32{9100, 9200}; !slices.Equal(got, want) {
		t.Errorf("numeric entries = %v, want %v", got, want)
	}

	delete(pod.Annotations, AnnotationPort)
	if got, want := targetPorts(PodTargets(pod)), []int32{9100, 9200}; !slices.Equal(got, want) {
		t.Errorf("no annotation = %v, want every declared container port %v", got, want)
	}
}

// explain mirrors the derivation, so its verdict must resolve the same one
// port — and must SAY that the name is declared twice, which is the one fact
// the target list cannot show.
func TestExplainSaysWhichDuplicateDeclarationResolved(t *testing.T) {
	pod := dupPortNamePod()
	svc := dupPortNameService()

	podVerdicts, annotated := ExplainPodPorts(pod)
	if !annotated {
		t.Errorf("explain reports annotated=false for a present port annotation")
	}
	if len(podVerdicts) != 1 || !slices.Equal(podVerdicts[0].Ports, []int32{9100}) {
		t.Fatalf("pod verdicts = %+v, want one resolving 9100", podVerdicts)
	}
	svcVerdicts, _ := ExplainServicePorts(pod, svc)
	if len(svcVerdicts) != 1 || !slices.Equal(svcVerdicts[0].Ports, []int32{9100}) {
		t.Fatalf("service verdicts = %+v, want one resolving 9100", svcVerdicts)
	}
	for _, v := range []PortVerdict{podVerdicts[0], svcVerdicts[0]} {
		if !strings.Contains(v.Note, `2 containers declare a port named "dup"`) {
			t.Errorf("verdict %+v does not say the name is declared twice", v)
		}
	}
	// Parity with the derivation, entry by entry.
	if got, want := resolvedPorts(podVerdicts), len(PodTargets(pod)); got != want {
		t.Errorf("explain resolves %d pod-annotation ports, the derivation serves %d targets", got, want)
	}
	if got, want := resolvedPorts(svcVerdicts), len(ServiceTargets(pod, svc)); got != want {
		t.Errorf("explain resolves %d service ports, the derivation serves %d targets", got, want)
	}
}

// A name declared ONCE earns no note: the caveat must not become noise on
// every named port in the cluster.
func TestSingleDeclarationHasNoDuplicateNote(t *testing.T) {
	pod := basePod()
	pod.Annotations[AnnotationPort] = "metrics"
	verdicts, _ := ExplainPodPorts(pod)
	if len(verdicts) != 1 || verdicts[0].Note != "" {
		t.Errorf("verdicts = %+v, want one with no note", verdicts)
	}
	svcVerdicts, _ := ExplainServicePorts(pod, baseService())
	for _, v := range svcVerdicts {
		if strings.Contains(v.Note, "containers declare a port named") {
			t.Errorf("verdict %+v carries a duplicate-name note for a name declared once", v)
		}
	}
}

// The seen-port dedup still spans the whole resolution: a second service port
// targeting the same name adds no target, and says so.
func TestDuplicateNameDedupsAcrossServicePorts(t *testing.T) {
	pod := dupPortNamePod()
	svc := dupPortNameService()
	svc.Ports = append(svc.Ports, services.Port{Name: "second", Port: 81, TargetPortName: "dup"})

	if got, want := targetPorts(ServiceTargets(pod, svc)), []int32{9100}; !slices.Equal(got, want) {
		t.Errorf("targets = %v, want %v (the second service port adds nothing)", got, want)
	}
	verdicts, _ := ExplainServicePorts(pod, svc)
	if len(verdicts) != 2 || resolvedPorts(verdicts) != 1 {
		t.Fatalf("want 2 verdicts resolving 1 port, got %+v", verdicts)
	}
	if !strings.Contains(verdicts[1].Note, "pod port 9100, already claimed by service port 80") {
		t.Errorf("second service port note = %q, want the already-claimed note", verdicts[1].Note)
	}
}

func ptr[T any](v T) *T { return &v }
