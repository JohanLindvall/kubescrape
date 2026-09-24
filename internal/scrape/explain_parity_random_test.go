package scrape

import (
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// The explain port mirrors (ExplainPodPorts, ExplainServicePorts) MUST agree
// with the derivation (PodTargets, ServiceTargets) about which pod ports a door
// resolves — the header contract of explain.go. Hand-picked examples are how
// that contract drifted, repeatedly: every earlier parity fixture was built on
// basePod()/baseService(), both scrape-annotated, which is exactly the shape
// that hid explain listing an UNANNOTATED door's ports as resolving.
//
// So this checks the contract over GENERATED configurations — the port
// annotation's entries (names, numbers, the signed/zero-padded/overflowing
// spellings parsePort has to get exactly right, blanks), the container port
// declarations (name, number, regular/init/ephemeral), the Service ports with
// numeric, named and absent targetPorts, and the scrape annotation on both the
// pod and the Service — through ONE checker, driven by a seeded loop that runs
// in every `go test` and by FuzzExplainPortsMatchDerivation for deeper runs.
//
// What the checker asserts is the division of labour the two layers actually
// have, stated so neither half can drift silently:
//
//   - the mirror is blind to the OPT-IN gate by contract: it resolves ports
//     as the door would if it were opted in, so its claimed ports equal the
//     derivation's for the same door WITH prometheus.io/scrape=true (the
//     generated pods are all Scrapeable — that gate the mirror applies
//     itself, see TestExplainPortsOfAnExcludedPodClaimNoPort);
//   - the derivation applies the gate: a door that is not opted in yields no
//     target at all — and the caller retracts the mirror's ports for it
//     (NoteNotOptedIn, pinned end to end by internal/server's
//     TestExplainPortsMatchTheServedTargetsOverGeneratedPods).

// parityChoices decodes a configuration from a byte stream, so the seeded loop
// and the fuzz target share ONE generator. An exhausted stream reads as zeros.
type parityChoices struct {
	b []byte
	i int
}

func (c *parityChoices) pick(n int) int {
	if n <= 1 || c.i >= len(c.b) {
		return 0
	}
	v := int(c.b[c.i]) % n
	c.i++
	return v
}

var (
	// Port NAMES as Kubernetes admits them: never all-digit.
	parityNames = []string{"metrics", "http", "web", "admin"}
	parityNums  = []int32{80, 8080, 9090, 9100}
	// Port-annotation entries: names (declared and not), numbers (declared and
	// not), and every spelling parsePort must accept or refuse exactly as
	// strconv.ParseInt(entry, 10, 32) plus the 1-65535 range would.
	parityEntries = []string{
		"metrics", "http", "web", "admin", "nope",
		"80", "8080", "9090", "9100", "9999",
		"+80", "0080", "-0", "0", "-1", "70000", "4294967297", "8_0", "0x50",
		" 9090 ", " ", "",
	}
	parityTypes = []string{"", "init", "ephemeral"}
)

// genParityCase builds one pod and one Service selecting it.
func genParityCase(c *parityChoices) (kubemeta.Pod, *services.Service) {
	pod := kubemeta.Pod{
		Name: "pod1", Namespace: "default", PodIP: "10.0.0.5", Phase: "Running",
		Annotations: map[string]string{},
	}
	for ci := range c.pick(4) {
		ctr := kubemeta.Container{Name: "c" + string(rune('0'+ci)), Type: parityTypes[c.pick(len(parityTypes))]}
		for range c.pick(4) {
			p := kubemeta.ContainerPort{Port: parityNums[c.pick(len(parityNums))]}
			if c.pick(3) > 0 {
				p.Name = parityNames[c.pick(len(parityNames))]
			}
			ctr.Ports = append(ctr.Ports, p)
		}
		pod.Containers = append(pod.Containers, ctr)
	}
	svc := &services.Service{
		Name: "svc1", Namespace: "default", UID: "svc-uid",
		Annotations: map[string]string{}, Selector: map[string]string{"app": "web"},
	}
	for range c.pick(5) {
		sp := services.Port{Port: parityNums[c.pick(len(parityNums))]}
		if c.pick(3) > 0 {
			sp.Name = parityNames[c.pick(len(parityNames))]
		}
		switch c.pick(3) {
		case 1:
			sp.TargetPortName = parityNames[c.pick(len(parityNames))]
		case 2:
			sp.TargetPortNum = parityNums[c.pick(len(parityNums))]
		}
		svc.Ports = append(svc.Ports, sp)
	}
	for _, ann := range []map[string]string{pod.Annotations, svc.Annotations} {
		switch c.pick(3) {
		case 1:
			ann[AnnotationScrape] = "true"
		case 2:
			ann[AnnotationScrape] = "false"
		}
		switch c.pick(4) {
		case 0: // absent: the fallback to every declared port
		case 1:
			ann[AnnotationPort] = " "
		default:
			entries := make([]string, 0, 6)
			for range 1 + c.pick(6) {
				entries = append(entries, parityEntries[c.pick(len(parityEntries))])
			}
			ann[AnnotationPort] = strings.Join(entries, ",")
		}
		if c.pick(8) == 0 {
			ann[AnnotationPath] = "/" + strings.Repeat("p", MaxTargetPathBytes)
		}
	}
	return pod, svc
}

// claimedPorts is every pod port a door's explain verdicts claim resolve,
// sorted.
func claimedPorts(verdicts []PortVerdict) []int32 {
	var out []int32
	for _, v := range verdicts {
		out = append(out, v.Ports...)
	}
	slices.Sort(out)
	return out
}

// derivedPorts is the pod ports of a door's targets, sorted.
func derivedPorts(targets []kubemeta.ScrapeTarget) []int32 {
	out := make([]int32, 0, len(targets))
	for _, t := range targets {
		out = append(out, t.Port)
	}
	slices.Sort(out)
	return out
}

// optedIn is a copy of annotations with the door opted in.
func optedIn(annotations map[string]string) map[string]string {
	out := make(map[string]string, len(annotations)+1)
	maps.Copy(out, annotations)
	out[AnnotationScrape] = "true"
	return out
}

// checkPortParity asserts the mirror/derivation contract (see the file
// comment) for both doors of one configuration. It reports whether it failed,
// so a loop can stop at the first counterexample instead of burying it.
func checkPortParity(t *testing.T, pod kubemeta.Pod, svc *services.Service) bool {
	t.Helper()
	ok := true

	verdicts, _ := ExplainPodPorts(pod)
	opted := pod
	opted.Annotations = optedIn(pod.Annotations)
	if got, want := claimedPorts(verdicts), derivedPorts(PodTargets(opted)); !slices.Equal(got, want) {
		t.Errorf("pod door: explain claims ports %v, the derivation resolves %v\npod annotations %q\ncontainers %+v\nverdicts %+v",
			got, want, pod.Annotations, pod.Containers, verdicts)
		ok = false
	}
	if pod.Annotations[AnnotationScrape] != "true" {
		if n := len(PodTargets(pod)); n != 0 {
			t.Errorf("pod door: not opted in (%q) and still derived %d targets", pod.Annotations[AnnotationScrape], n)
			ok = false
		}
	}

	verdicts, _ = ExplainServicePorts(pod, svc)
	optedSvc := *svc
	optedSvc.Annotations = optedIn(svc.Annotations)
	if got, want := claimedPorts(verdicts), derivedPorts(ServiceTargets(pod, &optedSvc)); !slices.Equal(got, want) {
		t.Errorf("service door: explain claims ports %v, the derivation resolves %v\nservice annotations %q\nservice ports %+v\ncontainers %+v\nverdicts %+v",
			got, want, svc.Annotations, svc.Ports, pod.Containers, verdicts)
		ok = false
	}
	if svc.Annotations[AnnotationScrape] != "true" {
		if n := len(ServiceTargets(pod, svc)); n != 0 {
			t.Errorf("service door: not opted in (%q) and still derived %d targets", svc.Annotations[AnnotationScrape], n)
			ok = false
		}
	}
	return ok
}

// A deterministic seed, so a failure reproduces; 20,000 configurations run in
// well under a second.
func TestExplainPortsMatchDerivationOverGeneratedConfigurations(t *testing.T) {
	rng := rand.New(rand.NewPCG(0x6b75626573, 0x7061726974))
	buf := make([]byte, 96)
	for i := range 20000 {
		for j := range buf {
			buf[j] = byte(rng.Uint32())
		}
		pod, svc := genParityCase(&parityChoices{b: buf})
		if !checkPortParity(t, pod, svc) {
			t.Fatalf("configuration %d disagrees (stopping at the first counterexample)", i)
		}
	}
}

func FuzzExplainPortsMatchDerivation(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{1, 0, 2, 1, 0, 1, 2, 1, 2, 3, 3, 1, 3, 5, 7, 11, 13, 1, 2, 3, 0, 4, 5, 6})
	f.Add([]byte{3, 2, 3, 1, 2, 0, 1, 3, 2, 3, 2, 1, 1, 0, 2, 4, 9, 14, 21, 0, 1, 1, 3, 6, 10, 15})
	f.Fuzz(func(t *testing.T, b []byte) {
		pod, svc := genParityCase(&parityChoices{b: b})
		checkPortParity(t, pod, svc)
	})
}
