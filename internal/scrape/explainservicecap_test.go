package scrape

import (
	"strconv"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/services"
)

// ServiceTargets BREAKS at MaxPortsPerPod, and ExplainServicePorts must stop
// where it stops. It did not: servicePortFilter mirrored only the seen-port
// dedup, so a Service declaring more ports than the ceiling was explained with
// a resolving verdict for every one of them — and, on a numeric targetPort no
// container declares, with the note "the target is still served", an
// affirmative false statement about a target the derivation refused. That is
// the exact inversion the package header's "MUST give the same verdict"
// contract exists to prevent, and the one question /v1/explain answers.
func TestExplainServicePortsStopsWhereServiceTargetsStops(t *testing.T) {
	const ports = 20 // comfortably past MaxPortsPerPod

	pod := basePod()
	delete(pod.Annotations, AnnotationScrape) // the Service door alone
	svc := baseService()
	svc.Ports = nil
	for i := 0; i < ports; i++ {
		n := int32(9000 + i)
		svc.Ports = append(svc.Ports, services.Port{
			Name: "p" + strconv.Itoa(int(n)), Port: n, TargetPortNum: n,
		})
	}

	targets := ServiceTargets(pod, svc)
	if len(targets) != MaxPortsPerPod {
		t.Fatalf("derivation served %d targets from %d service ports; want the ceiling %d",
			len(targets), ports, MaxPortsPerPod)
	}
	verdicts, _ := ExplainServicePorts(pod, svc)
	if len(verdicts) != ports {
		t.Fatalf("want one verdict per service port, got %d", len(verdicts))
	}
	if got := resolvedPorts(verdicts); got != len(targets) {
		t.Errorf("explain claims %d resolving ports, the derivation serves %d: %+v", got, len(targets), verdicts)
	}
	// And the refused ones must SAY so, rather than merely resolving to
	// nothing: a silent empty verdict is the same dead end.
	for i := MaxPortsPerPod; i < ports; i++ {
		if !strings.Contains(verdicts[i].Note, "over the per-pod ceiling") {
			t.Errorf("verdict %d (past the ceiling) = %+v, want the ceiling note", i, verdicts[i])
		}
		if strings.Contains(verdicts[i].Note, "still served") {
			t.Errorf("verdict %d claims a refused target is still served: %+v", i, verdicts[i])
		}
	}

	// The break is BEFORE the resolution in the derivation, so an entry past
	// the ceiling that would not have resolved either is still the ceiling's
	// verdict — explaining it as an unresolvable name would describe a
	// resolution ServiceTargets never attempts.
	svc.Ports = append(svc.Ports, services.Port{Name: "late", Port: 9999, TargetPortName: "nosuchport"})
	verdicts, _ = ExplainServicePorts(pod, svc)
	last := verdicts[len(verdicts)-1]
	if !strings.Contains(last.Note, "over the per-pod ceiling") {
		t.Errorf("unresolvable entry past the ceiling = %+v, want the ceiling verdict", last)
	}
}

// The pod-annotation mirror keeps its own ceiling note (podPorts pre-caps), and
// both doors must spell a refusal the SAME way — an operator greps one string.
func TestEveryDoorSpellsACeilingRefusalTheSameWay(t *testing.T) {
	pod := basePod()
	pod.Containers[0].Ports = nil
	var b strings.Builder
	for p := 9000; p < 9000+MaxPortsPerPod+2; p++ {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.Itoa(p))
	}
	pod.Annotations[AnnotationPort] = b.String()
	verdicts, _ := ExplainPodPorts(pod)
	last := verdicts[len(verdicts)-1]
	if want := CeilingNote("port " + strconv.Itoa(9000+MaxPortsPerPod+1)); last.Note != want {
		t.Errorf("pod-annotation ceiling note = %q, want %q", last.Note, want)
	}
	if !strings.Contains(CeilingNote("this endpoint"), "is NOT scraped") {
		t.Errorf("CeilingNote must say the target is not scraped: %q", CeilingNote("this endpoint"))
	}
}
