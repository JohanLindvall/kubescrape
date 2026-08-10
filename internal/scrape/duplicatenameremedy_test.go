package scrape

// A note's REMEDY has to fit the verdict it hangs on. The duplicate-name
// caveat is attached on two paths and they are fixed differently: the pod
// annotation can name the other declaration by NUMBER, but a Service's named
// targetPort cannot — numbers in the pod annotation are a different discovery
// path (source "pod"), and they yield nothing at all unless the pod itself
// carries prometheus.io/scrape. Telling an operator behind a Service to "name
// the ports by number" sends them to edit a field that cannot change the
// outcome, on the one endpoint whose whole job is to end that guessing.

import (
	"strings"
	"testing"
)

// containsNote is strings.Contains, named for what the assertions mean.
func containsNote(note, want string) bool { return strings.Contains(note, want) }

func TestDuplicateNameRemedyFitsThePathThatResolvedIt(t *testing.T) {
	pod := dupPortNamePod()
	svc := dupPortNameService()

	podVerdicts, _ := ExplainPodPorts(pod)
	if len(podVerdicts) != 1 {
		t.Fatalf("pod verdicts = %+v, want one", podVerdicts)
	}
	if want := "Name the ports by number in prometheus.io/port"; !containsNote(podVerdicts[0].Note, want) {
		t.Errorf("pod-annotation note = %q, want the remedy %q", podVerdicts[0].Note, want)
	}

	svcVerdicts, _ := ExplainServicePorts(pod, svc)
	if len(svcVerdicts) != 1 {
		t.Fatalf("service verdicts = %+v, want one", svcVerdicts)
	}
	note := svcVerdicts[0].Note
	if want := "second port whose targetPort is another declaration's NUMBER"; !containsNote(note, want) {
		t.Errorf("service-port note = %q, want the Service-side remedy %q", note, want)
	}
	// The remedy that does NOT work here must not appear: a Service port's
	// named targetPort reaches exactly one declaration however the pod
	// annotation is spelled.
	if containsNote(note, "prometheus.io/port") {
		t.Errorf("service-port note = %q, want no pod-annotation remedy: it cannot reach a second declaration through this Service", note)
	}
	// Both still state the fact the target list cannot show.
	for _, v := range []string{podVerdicts[0].Note, note} {
		if !containsNote(v, `2 containers declare a port named "dup"`) {
			t.Errorf("note = %q, want it to say the name is declared twice", v)
		}
	}
}

// The remedy is advice; the VERDICT is the contract. Whatever the note says,
// both paths resolve the port the derivation serves.
func TestRemedyDoesNotChangeTheVerdict(t *testing.T) {
	pod := dupPortNamePod()
	svc := dupPortNameService()
	podVerdicts, _ := ExplainPodPorts(pod)
	svcVerdicts, _ := ExplainServicePorts(pod, svc)
	if got, want := resolvedPorts(podVerdicts), len(PodTargets(pod)); got != want {
		t.Errorf("explain resolves %d pod ports, the derivation serves %d targets", got, want)
	}
	if got, want := resolvedPorts(svcVerdicts), len(ServiceTargets(pod, svc)); got != want {
		t.Errorf("explain resolves %d service ports, the derivation serves %d targets", got, want)
	}
}
