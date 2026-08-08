package scrape

import (
	"testing"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// The explain half must resolve the same PORTS the derivation does, dedupe
// included: podPorts routes every resolution through an `add` that admits a
// number once, and the explanation did not — so an entry list or a container
// set naming one port twice affirmed two targets where the server serves one.
// Parity here is the package's own notion: resolvedPorts(verdicts) ==
// len(PodTargets(pod)).
func TestExplainPodPortsParityOnDuplicateResolutions(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func() kubemeta.Pod
	}{
		{"repeated-annotation-number", func() kubemeta.Pod {
			pod := basePod()
			pod.Annotations[AnnotationPort] = "8080,8080"
			return pod
		}},
		{"two-names-one-number", func() kubemeta.Pod {
			pod := basePod()
			// Two names on one containerPort: legal, and both entries resolve
			// to 8080.
			pod.Containers[0].Ports = append(pod.Containers[0].Ports, kubemeta.ContainerPort{Name: "admin", Port: 8080})
			pod.Annotations[AnnotationPort] = "web,admin"
			return pod
		}},
		{"name-and-its-number", func() kubemeta.Pod {
			pod := basePod()
			pod.Annotations[AnnotationPort] = "web,8080"
			return pod
		}},
		{"declared-twice-no-annotation", func() kubemeta.Pod {
			pod := basePod()
			// One container may declare a number twice (two names, or two
			// protocols); the API server validates only name uniqueness within
			// a container.
			pod.Containers[0].Ports = append(pod.Containers[0].Ports, kubemeta.ContainerPort{Name: "admin", Port: 8080})
			delete(pod.Annotations, AnnotationPort)
			return pod
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := tc.build()
			targets := PodTargets(pod)
			verdicts, _ := ExplainPodPorts(pod)
			if got, want := resolvedPorts(verdicts), len(targets); got != want {
				t.Errorf("explain claims %d resolving ports, the derivation serves %d targets: %+v",
					got, want, verdicts)
			}
			// Every explained port must be one the derivation actually serves.
			served := map[int32]bool{}
			for _, p := range podPorts(pod) {
				served[p] = true
			}
			for _, v := range verdicts {
				for _, p := range v.Ports {
					if !served[p] {
						t.Errorf("verdict %+v claims port %d, which podPorts does not resolve", v, p)
					}
				}
			}
		})
	}
}
