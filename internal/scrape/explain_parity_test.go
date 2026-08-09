package scrape

import (
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/services"
)

// The explain functions MUST give the same verdict as the derivation they
// mirror (the package's header contract). The degenerate port annotations —
// present but splitting to zero entries (","), or present but all-blank
// (" ") — are where the two predicates once disagreed: the derivation falls
// back to declared ports only for an ABSENT or all-blank annotation, while
// explain fell back whenever the split was empty and so affirmed targets the
// server never serves for "prometheus.io/port: \",\"".

// resolvedPorts counts the pod ports the explain verdicts claim resolve.
func resolvedPorts(verdicts []PortVerdict) int {
	n := 0
	for _, v := range verdicts {
		n += len(v.Ports)
	}
	return n
}

func TestExplainPodPortsParityOnDegenerateAnnotations(t *testing.T) {
	// Entry-less but non-blank: the derivation serves ZERO targets, so explain
	// must claim zero resolving entries and say why.
	for _, ann := range []string{",", ", ,", ",,,", " , "} {
		pod := basePod()
		pod.Annotations[AnnotationPort] = ann

		if targets := PodTargets(pod); len(targets) != 0 {
			t.Fatalf("ann %q: derivation served %d targets, want 0: %+v", ann, len(targets), targets)
		}
		verdicts, annotated := ExplainPodPorts(pod)
		if !annotated {
			t.Errorf("ann %q: explain reports annotated=false for a present annotation", ann)
		}
		if got := resolvedPorts(verdicts); got != 0 {
			t.Errorf("ann %q: explain claims %d resolving ports, derivation serves none: %+v", ann, got, verdicts)
		}
		if len(verdicts) != 1 || !strings.Contains(verdicts[0].Note, "no entries") {
			t.Errorf("ann %q: want one no-entries verdict, got %+v", ann, verdicts)
		}
	}

	// Absent or all-blank: BOTH fall back to the declared container ports.
	for _, tc := range []struct {
		name string
		ann  *string
	}{
		{"absent", nil},
		{"blank", strptr(" ")},
		{"tabs", strptr(" \t ")},
	} {
		pod := basePod()
		if tc.ann != nil {
			pod.Annotations[AnnotationPort] = *tc.ann
		}
		targets := PodTargets(pod)
		verdicts, annotated := ExplainPodPorts(pod)
		if annotated {
			t.Errorf("%s: explain reports annotated=true where the derivation falls back", tc.name)
		}
		if len(targets) != 2 || resolvedPorts(verdicts) != 2 {
			t.Errorf("%s: derivation served %d targets, explain resolved %d ports; want 2/2 (the declared ports)",
				tc.name, len(targets), resolvedPorts(verdicts))
		}
	}
}

func TestExplainServicePortsParityOnDegenerateAnnotations(t *testing.T) {
	pod := basePod()

	// Entry-less but non-blank: selectServicePorts selects NOTHING.
	for _, ann := range []string{",", ", ,", ",,,"} {
		svc := baseService()
		svc.Annotations[AnnotationPort] = ann

		if targets := ServiceTargets(pod, svc); len(targets) != 0 {
			t.Fatalf("ann %q: derivation served %d targets, want 0: %+v", ann, len(targets), targets)
		}
		verdicts, annotated := ExplainServicePorts(pod, svc)
		if !annotated {
			t.Errorf("ann %q: explain reports annotated=false for a present annotation", ann)
		}
		if got := resolvedPorts(verdicts); got != 0 {
			t.Errorf("ann %q: explain claims %d resolving ports, derivation serves none: %+v", ann, got, verdicts)
		}
		if len(verdicts) != 1 || !strings.Contains(verdicts[0].Note, "no entries") {
			t.Errorf("ann %q: want one no-entries verdict, got %+v", ann, verdicts)
		}
	}

	// Absent or all-blank: BOTH select every service port.
	for _, tc := range []struct {
		name string
		ann  *string
	}{
		{"absent", nil},
		{"blank", strptr(" ")},
	} {
		svc := baseService()
		if tc.ann != nil {
			svc.Annotations[AnnotationPort] = *tc.ann
		}
		targets := ServiceTargets(pod, svc)
		verdicts, annotated := ExplainServicePorts(pod, svc)
		if annotated {
			t.Errorf("%s: explain reports annotated=true where the derivation selects all ports", tc.name)
		}
		if len(targets) != 2 || resolvedPorts(verdicts) != 2 {
			t.Errorf("%s: derivation served %d targets, explain resolved %d ports; want 2/2 (every service port)",
				tc.name, len(targets), resolvedPorts(verdicts))
		}
	}
}

func TestExplainServicePortsParityOnSharedPodPort(t *testing.T) {
	// Two service ports translating to the SAME pod port: ServiceTargets'
	// seen-port dedup serves ONE target, so explain must claim exactly one
	// resolving entry and say why the later one adds nothing.
	pod := basePod()
	svc := baseService()
	svc.Ports = []services.Port{
		{Name: "http", Port: 80, TargetPortNum: 8080},
		{Name: "https", Port: 443, TargetPortNum: 8080},
	}

	if targets := ServiceTargets(pod, svc); len(targets) != 1 || targets[0].Port != 8080 {
		t.Fatalf("derivation served %+v, want one target on pod port 8080", targets)
	}
	verdicts, annotated := ExplainServicePorts(pod, svc)
	if annotated {
		t.Errorf("explain reports annotated=true with no port annotation")
	}
	if len(verdicts) != 2 || resolvedPorts(verdicts) != 1 {
		t.Fatalf("want 2 verdicts resolving 1 port, got %+v", verdicts)
	}
	if len(verdicts[0].Ports) != 1 || verdicts[0].Ports[0] != 8080 || verdicts[0].Note != "" {
		t.Errorf("first entry = %+v, want it resolving 8080 with no note", verdicts[0])
	}
	if len(verdicts[1].Ports) != 0 || !strings.Contains(verdicts[1].Note, "pod port 8080, already claimed by service port 80") {
		t.Errorf("second entry = %+v, want no ports and the already-claimed note", verdicts[1])
	}

	// The dedup spans annotation entries too — same two ports, selected
	// explicitly.
	svc.Annotations[AnnotationPort] = "80, 443"
	if targets := ServiceTargets(pod, svc); len(targets) != 1 {
		t.Fatalf("annotated: derivation served %+v, want one target", targets)
	}
	verdicts, annotated = ExplainServicePorts(pod, svc)
	if !annotated {
		t.Errorf("annotated: explain reports annotated=false for a present annotation")
	}
	if len(verdicts) != 2 || resolvedPorts(verdicts) != 1 {
		t.Fatalf("annotated: want 2 verdicts resolving 1 port, got %+v", verdicts)
	}
	if !strings.Contains(verdicts[1].Note, "already claimed by service port 80") {
		t.Errorf("annotated: second entry = %+v, want the already-claimed note", verdicts[1])
	}
}

func strptr(s string) *string { return &s }
