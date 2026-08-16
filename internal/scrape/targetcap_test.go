package scrape

import (
	"strconv"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// A pod with a pathological prometheus.io/port list must produce at most
// MaxPortsPerPod targets: every target embeds the whole pod, so an unbounded
// list is an O(N²) response that OOMs the singleton service. Regression for the
// per-pod target cap.
func TestPodTargetsCappedPerPod(t *testing.T) {
	pod := basePod()
	var b strings.Builder
	for p := 1; p <= 20000; p++ { // far more distinct ports than the cap
		if p > 1 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.Itoa(p))
	}
	pod.Annotations[AnnotationPort] = b.String()

	targets := PodTargets(pod)
	if len(targets) != MaxPortsPerPod {
		t.Fatalf("got %d targets from a %d-port annotation; want the cap %d", len(targets), 20000, MaxPortsPerPod)
	}
}

// The same cap applies to the declared-container-ports fallback (a pod that
// declares thousands of ports with no annotation).
func TestPodTargetsCapsDeclaredPorts(t *testing.T) {
	pod := basePod()
	delete(pod.Annotations, AnnotationPort)
	pod.Containers[0].Ports = pod.Containers[0].Ports[:0]
	for p := 1; p <= 20000; p++ {
		pod.Containers[0].Ports = append(pod.Containers[0].Ports, kubemeta.ContainerPort{Port: int32(p)})
	}
	if got := len(PodTargets(pod)); got != MaxPortsPerPod {
		t.Fatalf("declared-ports fallback produced %d targets; want the cap %d", got, MaxPortsPerPod)
	}
}
