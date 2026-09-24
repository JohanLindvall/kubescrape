package server

import (
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/JohanLindvall/kubescrape/internal/scrape"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// /v1/explain's port entries answer "which of my ports are scraped?", and the
// answer must be the SERVED one: the set of pod ports the document claims
// resolve — over the pod's own entries and every listed Service's — must equal
// the set of ports the node's target list actually serves for the pod.
//
// The per-door mirrors in internal/scrape are checked against the derivation
// there (TestExplainPortsMatchDerivationOverGeneratedConfigurations), and they
// are blind to the opt-in gate by contract. What only THIS layer can get wrong
// is everything on top of them: the door-level opt-in (scrape.NoteNotOptedIn,
// applied here), the ceiling notes, and the pod-level exclusion reaching the
// document (the mirrors apply it; this layer must not undo it). Every earlier
// fixture opted both doors in, which is exactly how explain came to list an
// unannotated door's ports as resolving beside an empty target list — so this
// varies the scrape annotation on both doors, the pod's phase and deletion, the
// port annotations and the declarations, over generated configurations.
func TestExplainPortsMatchTheServedTargetsOverGeneratedPods(t *testing.T) {
	st := store.New(time.Minute)
	svcs := services.NewIndex()
	s := New(Config{
		Store: st, Services: svcs, Monitors: servicemonitors.NewIndex(), Resolver: stubResolver{},
		MaxWait: 500 * time.Millisecond, CacheTTL: 10 * time.Second, Ready: closedChan(),
	})
	rng := rand.New(rand.NewPCG(0x6578706c61696e, 0x706f727473))
	names := []string{"metrics", "http", "web", "admin"}
	nums := []int32{80, 8080, 9090, 9100}
	entries := []string{"metrics", "http", "web", "nope", "80", "8080", "9090", "9999", "+80", "0080", "0", "70000", " "}
	pick := func(n int) int { return rng.IntN(n) }
	portList := func() (string, bool) {
		switch pick(4) {
		case 0:
			return "", false // absent: every declared port
		case 1:
			return " ", true // all-blank: the same fallback
		}
		e := make([]string, 0, 5)
		for range 1 + pick(5) {
			e = append(e, entries[pick(len(entries))])
		}
		return strings.Join(e, ","), true
	}
	scrapeAnn := func(ann map[string]string) {
		switch pick(3) {
		case 1:
			ann[scrape.AnnotationScrape] = "true"
		case 2:
			ann[scrape.AnnotationScrape] = "false"
		}
	}

	const cases = 3000
	servedCases := 0
	for i := range cases {
		rv := strconv.Itoa(i + 1)
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "web-0", Namespace: "default", UID: types.UID("pod-web-0"), ResourceVersion: rv,
				Labels: map[string]string{"app": "web"}, Annotations: map[string]string{},
			},
			Spec:   corev1.PodSpec{NodeName: "node1"},
			Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.9.9.1"},
		}
		switch pick(8) {
		case 0:
			pod.Status.Phase = corev1.PodSucceeded
		case 1:
			now := metav1.Now()
			pod.DeletionTimestamp = &now
		}
		scrapeAnn(pod.Annotations)
		if v, ok := portList(); ok {
			pod.Annotations[scrape.AnnotationPort] = v
		}
		for c := range 1 + pick(3) {
			ctr := corev1.Container{Name: "c" + strconv.Itoa(c), Image: "img"}
			for range pick(4) {
				p := corev1.ContainerPort{ContainerPort: nums[pick(len(nums))]}
				if pick(3) > 0 {
					p.Name = names[pick(len(names))]
				}
				ctr.Ports = append(ctr.Ports, p)
			}
			// A native sidecar or init container declaring a port is the shape
			// the regular-first name resolution exists for.
			if c > 0 && pick(2) == 0 {
				pod.Spec.InitContainers = append(pod.Spec.InitContainers, ctr)
			} else {
				pod.Spec.Containers = append(pod.Spec.Containers, ctr)
			}
		}

		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name: "web", Namespace: "default", UID: types.UID("svc-web"), ResourceVersion: rv,
				Annotations: map[string]string{},
			},
			Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "web"}},
		}
		scrapeAnn(svc.Annotations)
		if v, ok := portList(); ok {
			svc.Annotations[scrape.AnnotationPort] = v
		}
		for range pick(4) {
			sp := corev1.ServicePort{Port: nums[pick(len(nums))], Name: names[pick(len(names))]}
			if pick(3) == 0 {
				sp.Name = "" // an unnamed port must never match an empty name
			}
			switch pick(3) {
			case 1:
				sp.TargetPort = intstr.FromString(names[pick(len(names))])
			case 2:
				sp.TargetPort = intstr.FromInt32(nums[pick(len(nums))])
			}
			svc.Spec.Ports = append(svc.Spec.Ports, sp)
		}

		st.UpsertPod(pod)
		svcs.Upsert(svc)

		doc, _ := s.explainPod("default", "web-0")
		if !doc.Found {
			t.Fatalf("case %d: explain did not find the pod", i)
		}
		served, _ := s.nodeTargets("node1")
		if len(served) > 0 {
			servedCases++
		}
		var claimed []int32
		for _, v := range doc.PortEntries {
			claimed = append(claimed, v.Ports...)
		}
		for _, es := range doc.Services {
			for _, v := range es.PortEntries {
				claimed = append(claimed, v.Ports...)
			}
		}
		var got []int32
		for _, tg := range served {
			got = append(got, tg.Port)
		}
		slices.Sort(claimed)
		claimed = slices.Compact(claimed)
		slices.Sort(got)
		got = slices.Compact(got)
		if !slices.Equal(claimed, got) {
			t.Fatalf("case %d: /v1/explain claims ports %v resolve, the node serves %v\npod annotations %q phase %s deleting %v\n"+
				"containers %+v\ninit containers %+v\nservice annotations %q\nservice ports %+v\nportEntries %+v\nservices %+v",
				i, claimed, got, pod.Annotations, pod.Status.Phase, pod.DeletionTimestamp != nil,
				pod.Spec.Containers, pod.Spec.InitContainers, svc.Annotations, svc.Spec.Ports, doc.PortEntries, doc.Services)
		}
	}
	// Both halves must actually be exercised: a generator that stopped
	// producing served targets would agree on the empty set forever.
	if servedCases < cases/10 || servedCases > cases-cases/10 {
		t.Errorf("%d of %d generated cases serve a target; the generator no longer covers both halves", servedCases, cases)
	}
}
