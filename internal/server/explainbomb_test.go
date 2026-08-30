package server

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/JohanLindvall/kubescrape/internal/scrape"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// maxExplainBytes is the size THIS document is asserted to stay under. It is
// not a bound in the code — the bounds are on ENTRIES, and this is the
// arithmetic they imply, asserted end to end because "a bound on entries is
// not a bound on bytes" is the lesson every one of these findings has been.
// Generous by ~3x over what the fixtures below actually produce, so an added
// field does not fail it and a lost bound cannot pass it.
const maxExplainBytes = 64 << 10

// THE ATTACK, and it needs no credential: /v1/explain is unauthenticated by
// design (it hands out no secret material and an operator debugging a pod that
// is not being scraped must be able to ask), it walks the same accumulator the
// node-targets document is built from, and it is re-derived and re-marshalled
// per request — writeJSON, no ETag, no cache. Every list it materialises is
// grown by input a namespace tenant supplies.
//
// Measured through the real handler, before the bounds, with the SERVED
// document beside it:
//
//	2,000 colliding ServiceMonitors  targets     2,777 B   explain   750,369 B
//	200 matching Services x 50 ports targets        29 B   explain 1,169,615 B
//	a 20,000-entry prometheus.io/port                       explain 2,099,738 B
//
// The middle row is the one that says this is explain's own defect rather than
// a leak from the targets path: NOTHING there is annotated, so the server
// serves no target at all and still renders 1.1 MB per request.
func TestExplainDocumentIsBoundedUnderTenantSuppliedInput(t *testing.T) {
	t.Run("colliding monitors", func(t *testing.T) {
		const monitors = 2000
		st, svcs := relabelBombFixture(t, 1)
		idx := contributorBombMonitors(t, monitors, forwardOrder(monitors))
		body := fetchExplain(t, st, svcs, idx, "default", "web-0")
		doc := decodeExplain(t, body)
		t.Logf("%d colliding monitors -> /v1/explain %d bytes", monitors, len(body))
		assertExplainBounded(t, body, doc)

		if len(doc.Services) != 1 {
			t.Fatalf("services = %+v", doc.Services)
		}
		es := doc.Services[0]
		// Every endpoint is accounted for: the ones listed, plus the ones the
		// bound refused, add up to the ones that exist. A short list that
		// looks complete is the failure mode a cap introduces, and it is
		// worse than the flood it fixes.
		if got := len(es.Monitors) + es.MonitorsNotShown; got != monitors {
			t.Errorf("%d listed + %d notShown = %d monitor endpoints, want %d",
				len(es.Monitors), es.MonitorsNotShown, got, monitors)
		}
		if len(es.Monitors) != maxExplainMonitors {
			t.Errorf("listed %d monitor endpoints, want the ceiling %d", len(es.Monitors), maxExplainMonitors)
		}
		// And the ceiling that refused the ATTRIBUTION still has to be
		// explained — that is the whole question ("which of my monitors
		// stopped contributing?"), and it is now answered ONCE per URL
		// instead of once per refused endpoint.
		if len(doc.MergeCeilings) != 1 {
			t.Fatalf("mergeCeilings = %+v, want one entry for the one URL", doc.MergeCeilings)
		}
		mc := doc.MergeCeilings[0]
		if want := monitors - scrape.MaxContributorsPerTarget; mc.ContributorsCapped != want {
			t.Errorf("mergeCeilings[0].contributorsCapped = %d, want %d", mc.ContributorsCapped, want)
		}
		if !strings.Contains(mc.Note, "per-target ceiling of") || !strings.Contains(mc.Note, "contributors") {
			t.Errorf("the per-URL ceiling note does not name the ceiling: %q", mc.Note)
		}
		// ONCE. The ~370-byte wording used to be appended to every refused
		// endpoint, so the fix that bounded the served document enlarged this
		// one on exactly the input the bound exists for.
		if n := strings.Count(body, "per-target ceiling of"); n != 1 {
			t.Errorf("the contributor-ceiling wording appears %d times, want exactly 1", n)
		}
	})

	t.Run("matching services", func(t *testing.T) {
		const svcCount, ports = 200, 50
		st, _ := relabelBombFixture(t, 1)
		idx := services.NewIndex()
		for i := range svcCount {
			svcPorts := make([]corev1.ServicePort, 0, ports)
			for p := range ports {
				svcPorts = append(svcPorts, corev1.ServicePort{
					Name: "p" + strconv.Itoa(p), Port: int32(8000 + p),
					TargetPort: intstr.FromInt32(int32(8000 + p)),
				})
			}
			idx.Upsert(&corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "svc-" + strconv.Itoa(i) + "-" + strings.Repeat("a", 40),
					Namespace: "default", UID: types.UID("svc-" + strconv.Itoa(i)),
				},
				Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "web"}, Ports: svcPorts},
			})
		}
		body := fetchExplain(t, st, idx, nil, "default", "web-0")
		doc := decodeExplain(t, body)
		t.Logf("%d matching services x %d ports -> /v1/explain %d bytes", svcCount, ports, len(body))
		assertExplainBounded(t, body, doc)

		if len(doc.Services) != maxExplainServices {
			t.Errorf("listed %d services, want the ceiling %d", len(doc.Services), maxExplainServices)
		}
		if got := len(doc.Services) + doc.ServicesNotShown; got != svcCount {
			t.Errorf("%d listed + %d notShown = %d services, want %d",
				len(doc.Services), doc.ServicesNotShown, got, svcCount)
		}
		// The port verdicts are bounded per list AND across the document, so
		// sixteen listed Services cannot carry 800 verdicts between them.
		listed := 0
		for _, es := range doc.Services {
			listed += len(es.PortEntries)
			if len(es.PortEntries) > maxExplainPortEntries {
				t.Errorf("service %s lists %d port verdicts, want at most %d",
					es.Name, len(es.PortEntries), maxExplainPortEntries)
			}
		}
		if listed > maxExplainPortEntriesPerDoc {
			t.Errorf("document lists %d port verdicts, want at most %d", listed, maxExplainPortEntriesPerDoc)
		}
	})

	t.Run("port annotation", func(t *testing.T) {
		// Sized to sit just under kubemeta.MaxAnnotationValueBytes: past that
		// the annotation is refused at the SOURCE (and named in the pod's own
		// kubescrape.io/annotations-omitted), so the derivation never sees it
		// and this bound would be exercised by nothing. 1365 entries of
		// "9000," is 8189 bytes — still 85x maxExplainPortEntries, which is
		// what this test is about.
		const entries = 1365
		list := make([]string, 0, entries)
		for i := range entries {
			list = append(list, strconv.Itoa(9000+i%50000))
		}
		st := store.New(time.Minute)
		st.UpsertPod(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "web-0", Namespace: "default", UID: types.UID("p"), ResourceVersion: "1",
				Labels: map[string]string{"app": "web"},
				Annotations: map[string]string{
					"prometheus.io/scrape": "true",
					"prometheus.io/port":   strings.Join(list, ","),
				},
			},
			Spec: corev1.PodSpec{NodeName: "node1", Containers: []corev1.Container{{
				Name: "app", Image: "img",
				Ports: []corev1.ContainerPort{{Name: "metrics", ContainerPort: 9090}},
			}}},
			Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.9.9.1"},
		})
		body := fetchExplain(t, st, services.NewIndex(), nil, "default", "web-0")
		doc := decodeExplain(t, body)
		t.Logf("%d port-annotation entries -> /v1/explain %d bytes", entries, len(body))
		assertExplainBounded(t, body, doc)

		if got := len(doc.PortEntries) + doc.PortEntriesNotShown; got != entries {
			t.Errorf("%d listed + %d notShown = %d port verdicts, want %d",
				len(doc.PortEntries), doc.PortEntriesNotShown, got, entries)
		}
		// The annotation itself is echoed, and echoing a megabyte of it per
		// unauthenticated request is the same defect one field over.
		if len(doc.PortAnnotation) > maxExplainValueBytes+len("…(truncated)") {
			t.Errorf("the echoed port annotation is %d bytes; it must be clipped", len(doc.PortAnnotation))
		}
	})
}

// assertExplainBounded is the shared half: the document stays within its size,
// and it SAYS it is short rather than looking complete.
func assertExplainBounded(t *testing.T, body string, doc explainDoc) {
	t.Helper()
	if len(body) > maxExplainBytes {
		t.Errorf("/v1/explain returned %d bytes for one pod; the per-pod document must be bounded "+
			"(any pod in the cluster can issue this ~100-byte GET in a loop, unauthenticated)", len(body))
	}
	if doc.NotShown == 0 {
		t.Error("the document truncated nothing, or truncated it silently: notShown must account for " +
			"every entry the bounds refused")
	}
}

func decodeExplain(t *testing.T, body string) explainDoc {
	t.Helper()
	var doc explainDoc
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("decoding the explain document: %v", err)
	}
	return doc
}
