package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/scrape"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// fatPodFixture builds a store holding `pods` pods whose DOCUMENT measures
// roughly bulkBytes, each with a prometheus.io/port list of `ports` distinct
// numeric entries — the shape scrape.MaxTargetBytesPerPod exists for.
//
// The bulk rides on LABELS, and that is the point of this fixture rather than an
// incidental choice. Annotations used to be the lever and no longer are:
// kubemeta.MaxAnnotationBytes bounds one object's annotations at 16 KiB, so the
// 200 KiB inventory blob this file was written around is now refused at the
// source and named in the pod's own kubescrape.io/annotations-omitted. Labels
// are NOT bounded and deliberately so — they are selection input (Service
// selectors, PodMonitor selectors), and no filter can know which label a
// selector needs, so refusing one would silently change what is scraped rather
// than how large the answer is. Each label here is a shape the API server
// accepts (a ≤63-byte value under a qualified key); only their COUNT is large,
// which Kubernetes bounds only through the object's ~1.5 MiB ceiling. So this is
// still something a tenant with edit rights in ONE namespace can create, and it
// is what the byte ceiling has left to bind on.
func fatPodFixture(t *testing.T, pods, bulkBytes, ports int) (*store.Store, *services.Index) {
	t.Helper()
	var portList strings.Builder
	for p := range ports {
		if p > 0 {
			portList.WriteByte(',')
		}
		portList.WriteString(strconv.Itoa(9000 + p))
	}
	// ~90 bytes of document per label: a 21-byte key, a 63-byte value (the API
	// server's maximum) and the JSON framing between them.
	const perLabel = 21 + 63 + 6
	labels := map[string]string{"app": "web"}
	for i := range bulkBytes / perLabel {
		labels["bulk.example.com/"+strconv.Itoa(10000+i)] = strings.Repeat("x", 63)
	}
	st := store.New(time.Minute)
	for i := range pods {
		name := "web-" + strconv.Itoa(i)
		st.UpsertPod(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: "default",
				UID: types.UID("pod-" + name), ResourceVersion: "1",
				Labels: labels,
				Annotations: map[string]string{
					scrape.AnnotationScrape: "true",
					scrape.AnnotationPort:   portList.String(),
				},
			},
			Spec: corev1.PodSpec{
				NodeName: "node1",
				Containers: []corev1.Container{{
					Name: "app", Image: "img",
					Ports: []corev1.ContainerPort{{Name: "metrics", ContainerPort: 9090}},
				}},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.9.9." + strconv.Itoa(i+1)},
		})
	}
	svcs := services.NewIndex()
	svcs.Upsert(&corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web", Namespace: "default", UID: types.UID("svc-uid"),
			Labels: map[string]string{"team": "obs"},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "web"},
			Ports:    []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromString("metrics")}},
		},
	})
	return st, svcs
}

// THE ATTACK: a tenant with edit rights in ONE namespace puts 200 KiB of
// legitimate-looking metadata on a pod, sets prometheus.io/scrape=true and
// lists 16 ports. Every ScrapeTarget embeds the WHOLE pod document by value, so
// the node-targets response re-marshals those 200 KiB once per target.
//
// The metadata is LABELS here (see fatPodFixture): the annotation route into a
// fat document is closed at the source by kubemeta.MaxAnnotationBytes, and
// labels are the honest remainder — unbounded on purpose, because they are
// selection input. So this ceiling still has work to do, and this is the shape
// that reaches it.
//
// MEASURED through the real derivation with every OTHER ceiling on this path
// fully respected (MaxTargetPathBytes, the per-endpoint and merged relabel
// chains, MaxContributorsPerTarget): 16 targets, a 3,283,798-byte body. Ten such
// pods on a node is ~33 MB per GET /v1/nodes/{node}/targets, re-derived and
// re-marshalled on every agent poll of that node, in the singleton the chart
// requests 128Mi for and deliberately gives no memory limit — and writeCached
// must BUILD the body to hash its ETag, so the 304 path does not save it.
// scrape.MaxPortsPerPod cannot see it: 16 targets is exactly what it admits,
// and its own comment models the per-target cost as "the ~2 KiB pod document".
func TestFatPodAnnotationCannotMultiplyIntoTheNodeTargetsDocument(t *testing.T) {
	const bulk = 200 << 10
	st, svcs := fatPodFixture(t, 1, bulk, scrape.MaxPortsPerPod)

	before := obs.ScrapeTargetsCapped.Value()
	body, targets := fetchTargets(t, st, svcs, servicemonitors.NewIndex())
	t.Logf("%d KiB of pod metadata, %d ports -> %d targets, %d byte response (unbounded: %d targets, ~%d bytes)",
		bulk>>10, scrape.MaxPortsPerPod, len(targets), len(body),
		scrape.MaxPortsPerPod, scrape.MaxPortsPerPod*len(body))

	// At LEAST one: the pod's annotations are legitimate data and the ceiling
	// bounds the MULTIPLIER, never the workload (see MaxTargetBytesPerPod).
	if len(targets) == 0 {
		t.Fatal("a pod with large but honest annotations was refused every target; the byte ceiling must " +
			"bound the multiplier, not stop scraping a workload that did nothing wrong")
	}
	// The budget plus ONE unconditional first target is the whole bound. Two
	// pod documents of slack covers the first target and the response framing.
	if want := scrape.MaxTargetBytesPerPod + 2*bulk; len(body) > want {
		t.Errorf("node targets document is %d bytes for ONE pod (ceiling %d + the unconditional first target "+
			"= %d); a fat pod document must not multiply by the pod's port count", len(body), scrape.MaxTargetBytesPerPod, want)
	}
	// Fail CLOSED and DIAGNOSABLE: a refused target is an endpoint that is NOT
	// scraped, and it is indistinguishable from one that was never configured.
	if got := obs.ScrapeTargetsCapped.Value() - before; got == 0 {
		t.Error("targets were refused without moving kubescrape_scrape_targets_capped_total")
	}
	doc := fetchExplain(t, st, svcs, servicemonitors.NewIndex(), "default", "web-0")
	var parsed explainDoc
	if err := json.Unmarshal([]byte(doc), &parsed); err != nil {
		t.Fatal(err)
	}
	// /v1/explain has to say it was refused for SIZE, not for count: "over the
	// per-pod ceiling of 16 targets" against a pod serving one would send an
	// operator looking for fifteen missing ports that do not exist.
	if parsed.CappedTargetsBySize == 0 {
		t.Errorf("/v1/explain reports %d capped targets but none by SIZE: %s", parsed.CappedTargets, truncateDoc(doc))
	}
	if parsed.PodDocumentBytes < bulk {
		t.Errorf("/v1/explain reports podDocumentBytes=%d for a pod carrying %d bytes of metadata; the measured "+
			"size is what explains the refusal", parsed.PodDocumentBytes, bulk)
	}
	if !strings.Contains(doc, "per-pod ceiling of "+strconv.Itoa(scrape.MaxTargetBytesPerPod)+" bytes") {
		t.Errorf("/v1/explain does not carry the size-ceiling wording: %s", truncateDoc(doc))
	}
	// And it must not become the unbounded path itself: it walks the same
	// accumulator, is unauthenticated, and any pod in the cluster can issue
	// that ~100-byte GET in a loop. It carries no pod document at all, so the
	// bound is far below the served response.
	if len(doc) > 64<<10 {
		t.Errorf("/v1/explain document is %d bytes for one fat pod; it echoes clipped annotations only", len(doc))
	}
}

// The pod's FIRST target is unconditional, even when its document ALONE is over
// the whole budget. Annotations are legitimate attribution data that operators
// rely on, and a ceiling that silently stopped scraping a workload for being
// verbose would be a worse failure than the one it prevents — the multiplier is
// the problem, not the pod.
func TestAPodOverTheWholeByteBudgetStillGetsOneTarget(t *testing.T) {
	// Larger than the entire per-pod budget, so even the first target's own
	// cost exceeds it: only the unconditional arm can serve this pod.
	st, svcs := fatPodFixture(t, 1, scrape.MaxTargetBytesPerPod+(64<<10), 4)
	_, targets := fetchTargets(t, st, svcs, servicemonitors.NewIndex())
	if len(targets) != 1 {
		t.Fatalf("got %d targets for a pod whose document alone is over the budget; want exactly 1 (the "+
			"unconditional first target)", len(targets))
	}
}

// A pod under the byte ceiling is untouched by it: the ordinary case must reach
// scrape.MaxPortsPerPod exactly as before. At the ~2 KiB document
// MaxPortsPerPod models, all 16 targets fit eight times over.
func TestOrdinaryPodIsUnaffectedByTheByteCeiling(t *testing.T) {
	st, svcs := fatPodFixture(t, 1, 512, scrape.MaxPortsPerPod)
	_, targets := fetchTargets(t, st, svcs, servicemonitors.NewIndex())
	if len(targets) != scrape.MaxPortsPerPod {
		t.Fatalf("an ordinary pod got %d targets; the byte ceiling must not bind below the count one (%d)",
			len(targets), scrape.MaxPortsPerPod)
	}
	doc := fetchExplain(t, st, svcs, servicemonitors.NewIndex(), "default", "web-0")
	var parsed explainDoc
	if err := json.Unmarshal([]byte(doc), &parsed); err != nil {
		t.Fatal(err)
	}
	if parsed.CappedTargetsBySize != 0 || parsed.PodDocumentBytes != 0 {
		t.Errorf("/v1/explain reports a size refusal for an ordinary pod: %+v", parsed)
	}
}

// The COUNT ceiling must still read as a count ceiling: a small pod declaring
// more ports than MaxPortsPerPod gets the ports wording, no size wording, and
// nothing on the size fields. The two refusals have different remedies.
func TestCountCeilingIsNotReportedAsASizeCeiling(t *testing.T) {
	// A SMALL pod document, so only the count ceiling can bind — and two doors,
	// because the pod-annotation door pre-caps at MaxPortsPerPod itself: the
	// annotation fills the sixteen slots and the monitors arrive at a full
	// accumulator.
	st, svcs := fatPodFixture(t, 1, 512, scrape.MaxPortsPerPod)
	idx := servicemonitors.NewIndex()
	for i := range 4 {
		if err := idx.Upsert(&unstructured.Unstructured{Object: map[string]any{
			"metadata": map[string]any{"name": "sm-" + strconv.Itoa(1000+i), "namespace": "default"},
			"spec": map[string]any{
				"selector":  map[string]any{"matchLabels": map[string]any{"team": "obs"}},
				"endpoints": []any{map[string]any{"port": "http", "path": "/m" + strconv.Itoa(i)}},
			},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	doc := fetchExplain(t, st, svcs, idx, "default", "web-0")
	var parsed explainDoc
	if err := json.Unmarshal([]byte(doc), &parsed); err != nil {
		t.Fatal(err)
	}
	if parsed.CappedTargets == 0 {
		t.Fatalf("expected the count ceiling to refuse targets: %s", truncateDoc(doc))
	}
	if parsed.CappedTargetsBySize != 0 {
		t.Errorf("count-ceiling refusals were reported as size refusals: %+v", parsed)
	}
	if !strings.Contains(doc, "over the per-pod ceiling of "+strconv.Itoa(scrape.MaxPortsPerPod)+" targets") {
		t.Errorf("/v1/explain lost the count-ceiling wording: %s", truncateDoc(doc))
	}
	if strings.Contains(doc, "bytes and it is") {
		t.Errorf("/v1/explain reports a byte refusal for a pod that hit the count ceiling: %s", truncateDoc(doc))
	}
}

// The byte ceiling must not make the response depend on the order the monitors
// happened to be indexed in: the document is hashed into an ETag every agent on
// the node revalidates against, so an order-sensitive body defeats the 304 path
// outright — the property the contributor ceiling established and every later
// ceiling inherits.
func TestByteCeilingKeepsTheBodyStableAcrossUpsertOrder(t *testing.T) {
	const monitors = 24
	fetch := func(reverse bool) (string, string, int) {
		st, svcs := fatPodFixture(t, 1, 40<<10, 1)
		idx := servicemonitors.NewIndex()
		order := make([]int, 0, monitors)
		for i := range monitors {
			order = append(order, i)
		}
		if reverse {
			for l, r := 0, len(order)-1; l < r; l, r = l+1, r-1 {
				order[l], order[r] = order[r], order[l]
			}
		}
		for _, i := range order {
			// Distinct paths, so each monitor resolves to its OWN url and
			// materialises a target rather than merging into a held one: the
			// byte budget binds on the new-URL arm.
			if err := idx.Upsert(&unstructured.Unstructured{Object: map[string]any{
				"metadata": map[string]any{"name": "sm-" + strconv.Itoa(1000+i), "namespace": "default"},
				"spec": map[string]any{
					"selector":  map[string]any{"matchLabels": map[string]any{"team": "obs"}},
					"endpoints": []any{map[string]any{"port": "http", "path": "/m" + strconv.Itoa(i)}},
				},
			}}); err != nil {
				t.Fatal(err)
			}
		}
		srv := httptest.NewServer(New(Config{
			Store: st, Services: svcs, Monitors: idx, Resolver: stubResolver{},
			MaxWait: 500 * time.Millisecond, CacheTTL: 10 * time.Second, Ready: closedChan(),
		}).Handler())
		t.Cleanup(srv.Close)
		resp, err := http.Get(srv.URL + "/v1/nodes/node1/targets")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		var out struct {
			Targets []kubemeta.ScrapeTarget `json:"targets"`
		}
		if err := json.Unmarshal(body, &out); err != nil {
			t.Fatal(err)
		}
		return string(body), resp.Header.Get("ETag"), len(out.Targets)
	}

	fwdBody, fwdETag, fwdN := fetch(false)
	revBody, revETag, revN := fetch(true)
	// The BYTE ceiling has to actually bind, or the stability assertion is
	// vacuous — and "fewer than the monitors offered" is not enough, since the
	// COUNT ceiling would refuse them too. A 40 KiB document admits six targets
	// against the 256 KiB budget, well short of MaxPortsPerPod.
	if fwdN >= scrape.MaxPortsPerPod {
		t.Fatalf("the byte ceiling did not bind: %d targets served from %d monitors, which is the count "+
			"ceiling (%d) doing the work", fwdN, monitors, scrape.MaxPortsPerPod)
	}
	if fwdBody != revBody || fwdETag != revETag {
		t.Errorf("the byte ceiling makes the body depend on upsert order; the body and its ETag must not\nforward %s (%d targets)\nreverse %s (%d targets)",
			fwdETag, fwdN, revETag, revN)
	}
}

// The budget charges the WHOLE target document, not only the pod it embeds —
// which is the difference between a fifth ceiling and a backstop. Every
// per-target string is inside it the day it is added, whether or not anyone
// gives it a bound of its own: two pods with IDENTICAL documents must serve
// different numbers of targets when one pod's targets each carry a 2 KiB path
// and a 7 KiB relabel chain — both individually legal, both far under their own
// per-field ceilings.
func TestPerTargetStringsAreChargedNotJustThePodDocument(t *testing.T) {
	const monitors = 24
	serve := func(fat bool) int {
		st, svcs := fatPodFixture(t, 1, 20<<10, 1)
		idx := servicemonitors.NewIndex()
		for i := range monitors {
			ep := map[string]any{"port": "http", "path": "/m" + strconv.Itoa(i)}
			if fat {
				// Inside every per-field ceiling: the path under
				// scrape.MaxTargetPathBytes, the chain under the per-endpoint
				// parse bound. The point is precisely that legal fields still
				// multiply.
				ep["path"] = "/m" + strconv.Itoa(i) + "/" + strings.Repeat("p", 1900)
				ep["metricRelabelings"] = relabelRules(28, 240)
			}
			if err := idx.Upsert(&unstructured.Unstructured{Object: map[string]any{
				"metadata": map[string]any{"name": "sm-" + strconv.Itoa(1000+i), "namespace": "default"},
				"spec": map[string]any{
					"selector":  map[string]any{"matchLabels": map[string]any{"team": "obs"}},
					"endpoints": []any{ep},
				},
			}}); err != nil {
				t.Fatal(err)
			}
		}
		_, targets := fetchTargets(t, st, svcs, idx)
		return len(targets)
	}
	bare, fat := serve(false), serve(true)
	t.Logf("same pod document, %d monitors: %d targets bare, %d with a 2 KiB path and a 7 KiB chain each",
		monitors, bare, fat)
	if bare >= scrape.MaxPortsPerPod || fat < 1 {
		t.Fatalf("the byte ceiling is not the binding one (bare=%d fat=%d, count ceiling %d)",
			bare, fat, scrape.MaxPortsPerPod)
	}
	if fat >= bare {
		t.Errorf("targets carrying %d bytes of their own fields cost the same as bare ones (%d vs %d): the "+
			"budget charges only the embedded pod, so any per-target string nobody has bounded is free",
			2000+28*240, fat, bare)
	}
}
