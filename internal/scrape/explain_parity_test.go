package scrape

import (
	"strings"
	"testing"
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

func strptr(s string) *string { return &s }
