package server

import (
	"encoding/json"
	"io"
	"log/slog"
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

	"github.com/JohanLindvall/kubescrape/internal/scrape"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// fatServiceLabels is ~600 KB of Service labels: 63-byte values (the API
// server's per-value maximum), bounded in COUNT by nothing but the object's
// size limit — which is why no filter can refuse them and why they are the
// part of a Service view no per-field ceiling reaches.
func fatServiceLabels() map[string]string {
	labels := map[string]string{"team": "obs"}
	for i := range 6500 {
		labels["bulk.example.com/"+strconv.Itoa(100000+i)] = strings.Repeat("x", 63)
	}
	return labels
}

// swapViewPod is one pod on node1 declaring sixteen container ports.
func swapViewPod(annotations map[string]string) *corev1.Pod {
	ports := make([]corev1.ContainerPort, 0, scrape.MaxPortsPerPod)
	for i := range scrape.MaxPortsPerPod {
		ports = append(ports, corev1.ContainerPort{Name: "p" + strconv.Itoa(i), ContainerPort: int32(9000 + i)})
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web-0", Namespace: "default", UID: types.UID("pod-web-0"), ResourceVersion: "1",
			Labels: map[string]string{"app": "web"}, Annotations: annotations,
		},
		Spec:   corev1.PodSpec{NodeName: "node1", Containers: []corev1.Container{{Name: "app", Ports: ports}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.9.9.1"},
	}
}

// swapViewService is the fat Service in front of that pod, one Service port
// per container port.
func swapViewService(annotations map[string]string) *corev1.Service {
	ports := make([]corev1.ServicePort, 0, scrape.MaxPortsPerPod)
	for i := range scrape.MaxPortsPerPod {
		ports = append(ports, corev1.ServicePort{
			Name: "s" + strconv.Itoa(i), Port: int32(8000 + i), TargetPort: intstr.FromInt32(int32(9000 + i)),
		})
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web", Namespace: "default", UID: types.UID("svc-uid"), ResourceVersion: "1",
			Labels: fatServiceLabels(), Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "web"}, Ports: ports},
	}
}

// The per-pod byte budget refuses on the NEW-URL arm, and the two SWAP arms of
// targetDedup.add — a monitor target upgrading an annotation target, and a
// holder keeping its URL while taking on the displaced target's Service view —
// only charged. That is right for configuration (refusing a monitor's relabel
// or auth declaration would change what is exported to bound a response), and
// wrong for the Service VIEW, which is tenant-sized attribution: its labels are
// bounded by nothing but the object's size, so one fat Service in front of a
// 16-port pod rode sixteen swaps into the response — measured 9.7 MB against
// the 256 KiB budget, where the new-URL arm would have held the same view to
// one target.
//
// Both arms, both shapes. The targets are all still served and all still name
// their Service; only the labels past the budget are left off.
func TestAFatServiceViewCannotRideTheSwapArmsPastTheByteBudget(t *testing.T) {
	endpoints := make([]any, 0, scrape.MaxPortsPerPod)
	portList := make([]string, 0, scrape.MaxPortsPerPod)
	for i := range scrape.MaxPortsPerPod {
		endpoints = append(endpoints, map[string]any{"targetPort": 9000 + i})
		portList = append(portList, strconv.Itoa(9000+i))
	}
	for _, tc := range []struct {
		name   string
		pod    *corev1.Pod
		svc    *corev1.Service
		source string // what the served targets must be
		eps    []any
	}{
		{
			// Holder-keeps: the pod door takes all sixteen URLs first; the
			// scrape-annotated Service offers the same sixteen and loses the
			// URL but donates its view.
			name:   "holder keeps the url",
			pod:    swapViewPod(map[string]string{scrape.AnnotationScrape: "true"}),
			svc:    swapViewService(map[string]string{scrape.AnnotationScrape: "true"}),
			source: "pod",
		},
		{
			// Upgrade: an unannotated Service, and a ServiceMonitor whose
			// sixteen endpoints resolve to the pod annotation's sixteen URLs.
			name: "monitor upgrades the annotation target",
			pod: swapViewPod(map[string]string{
				scrape.AnnotationScrape: "true", scrape.AnnotationPort: strings.Join(portList, ","),
			}),
			svc:    swapViewService(nil),
			source: "servicemonitor",
			eps:    endpoints,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := store.New(time.Minute)
			st.UpsertPod(tc.pod)
			svcs := services.NewIndex()
			svcs.Upsert(tc.svc)
			idx := servicemonitors.NewIndex()
			if tc.eps != nil {
				if err := idx.Upsert(&unstructured.Unstructured{Object: map[string]any{
					"metadata": map[string]any{"name": "sm-web", "namespace": "default"},
					"spec": map[string]any{
						"selector":  map[string]any{"matchLabels": map[string]any{"team": "obs"}},
						"endpoints": tc.eps,
					},
				}}); err != nil {
					t.Fatal(err)
				}
			}
			h := &recordingHandler{}
			srv := httptest.NewServer(New(Config{
				Store: st, Services: svcs, Monitors: idx, Resolver: stubResolver{}, Log: slog.New(h),
				MaxWait: 500 * time.Millisecond, CacheTTL: 10 * time.Second, Ready: closedChan(),
			}).Handler())
			t.Cleanup(srv.Close)
			body := getBody(t, srv.URL+"/v1/nodes/node1/targets")
			var out struct {
				Targets []kubemeta.ScrapeTarget `json:"targets"`
			}
			if err := json.Unmarshal(body, &out); err != nil {
				t.Fatal(err)
			}

			view := scrape.ServiceViewBytes(&kubemeta.Service{
				Name: "web", Namespace: "default", UID: "svc-uid", Labels: tc.svc.Labels,
			})
			// The budget, plus the ONE full view the pod's unconditional first
			// target keeps (it is exempt from every byte refusal).
			if want := scrape.MaxTargetBytesPerPod + view; len(body) > want {
				t.Errorf("node targets document is %d bytes for ONE pod: the budget is %d plus one %d-byte Service "+
					"view for the unconditional first target (%d) — the swap arms carried the view past it",
					len(body), scrape.MaxTargetBytesPerPod, view, want)
			}
			// Nothing is unscraped, and nothing loses its Service's identity.
			if len(out.Targets) != scrape.MaxPortsPerPod {
				t.Fatalf("served %d targets, want all %d: bounding a view must never refuse a target",
					len(out.Targets), scrape.MaxPortsPerPod)
			}
			full := 0
			for _, tg := range out.Targets {
				if tg.Source != tc.source {
					t.Errorf("target %s has source %q, want %q: the fixture no longer takes this arm", tg.URL, tg.Source, tc.source)
				}
				if tg.Service == nil || tg.Service.Name != "web" || tg.Service.UID != "svc-uid" {
					t.Errorf("target %s lost its Service identity: %+v", tg.URL, tg.Service)
					continue
				}
				if len(tg.Service.Labels) > 0 {
					full++
				}
			}
			if full != 1 {
				t.Errorf("%d targets carry the full Service view, want exactly the pod's first", full)
			}

			// Said where an operator looks: one warning naming the Service...
			lines := h.matching("did not fit the pod's scrape-target byte budget")
			if len(lines) != 1 || !strings.Contains(lines[0], "service=web") {
				t.Errorf("want one warning naming the Service, got %v", lines)
			}
			// ...and /v1/explain, per target.
			var doc explainDoc
			if err := json.Unmarshal(getBody(t, srv.URL+"/v1/explain/default/web-0"), &doc); err != nil {
				t.Fatal(err)
			}
			marked := 0
			for _, et := range doc.Targets {
				if et.ServiceViewTrimmed {
					marked++
				}
			}
			if doc.ServiceViewsTrimmed != scrape.MaxPortsPerPod-1 || marked != doc.ServiceViewsTrimmed {
				t.Errorf("explain: serviceViewsTrimmed=%d with %d targets marked, want %d of each",
					doc.ServiceViewsTrimmed, marked, scrape.MaxPortsPerPod-1)
			}
		})
	}
}

// A target refused a view is ONE refusal however many swaps re-offer it: an
// annotated Service donates its view to the pod door's target (trimmed), and a
// ServiceMonitor on the same Service then upgrades that target, bringing the
// view again (trimmed again). Counting refusals rather than targets reported
// two trimmed views for one target.
func TestAServiceViewRefusedTwiceOnOneTargetIsCountedOnce(t *testing.T) {
	st := store.New(time.Minute)
	st.UpsertPod(swapViewPod(map[string]string{scrape.AnnotationScrape: "true"}))
	svcs := services.NewIndex()
	svcs.Upsert(swapViewService(map[string]string{scrape.AnnotationScrape: "true"}))
	endpoints := make([]any, 0, scrape.MaxPortsPerPod)
	for i := range scrape.MaxPortsPerPod {
		endpoints = append(endpoints, map[string]any{"targetPort": 9000 + i})
	}
	idx := servicemonitors.NewIndex()
	if err := idx.Upsert(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "sm-web", "namespace": "default"},
		"spec": map[string]any{
			"selector":  map[string]any{"matchLabels": map[string]any{"team": "obs"}},
			"endpoints": endpoints,
		},
	}}); err != nil {
		t.Fatal(err)
	}
	var doc explainDoc
	if err := json.Unmarshal([]byte(fetchExplain(t, st, svcs, idx, "default", "web-0")), &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Targets) != scrape.MaxPortsPerPod {
		t.Fatalf("explain lists %d targets, want %d", len(doc.Targets), scrape.MaxPortsPerPod)
	}
	for _, et := range doc.Targets {
		if et.Source != "servicemonitor" {
			t.Fatalf("target %s has source %q: the fixture no longer reaches the upgrade arm", et.URL, et.Source)
		}
	}
	if doc.ServiceViewsTrimmed != scrape.MaxPortsPerPod-1 {
		t.Errorf("serviceViewsTrimmed = %d for %d affected targets", doc.ServiceViewsTrimmed, scrape.MaxPortsPerPod-1)
	}
}

// swapViewNamedService is a Service called name in front of swapViewPod, one
// Service port per container port, annotated to scrape the Service ports in
// scrapePorts (none: not scrape-annotated).
func swapViewNamedService(name string, labels map[string]string, scrapePorts ...int) *corev1.Service {
	svc := swapViewService(nil)
	svc.Name, svc.UID, svc.Labels = name, types.UID(name+"-uid"), labels
	if len(scrapePorts) > 0 {
		list := make([]string, 0, len(scrapePorts))
		for _, p := range scrapePorts {
			list = append(list, strconv.Itoa(p))
		}
		svc.Annotations = map[string]string{
			scrape.AnnotationScrape: "true", scrape.AnnotationPort: strings.Join(list, ","),
		}
	}
	return svc
}

// servicePorts is the Service ports 8000+from ..= 8000+to, which target the
// container ports 9000+from ..= 9000+to.
func servicePorts(from, to int) []int {
	out := make([]int, 0, to-from+1)
	for i := from; i <= to; i++ {
		out = append(out, 8000+i)
	}
	return out
}

// The trimmed-view warning names the Services whose views are trimmed on the
// SERVED targets, each with its own count. The pod used to remember only the
// FIRST Service refused, and untrimming a URL (a later swap replacing the
// view) never re-derived it: fat Service a trimmed on 9001-9007, a monitor on
// thin Service b then upgrading those seven targets to b's full view, and fat
// Service c trimmed on 9008-9015 warned "service=a" — a Service trimmed on no
// target — while c, the one actually trimmed, went unnamed and its own
// throttle key unspent. And two Services both still trimmed were one line
// naming the first with the other's targets added to its count.
func TestTheTrimmedViewWarningNamesTheServicesStillTrimmed(t *testing.T) {
	for _, tc := range []struct {
		name     string
		services []*corev1.Service
		monitor  bool // sm-b, selecting mon=yes, on container ports 9001-9007
		want     map[string]int
	}{
		{
			name: "a later swap untrims the first Service",
			services: []*corev1.Service{
				swapViewNamedService("a", fatServiceLabels(), servicePorts(1, 7)...),
				swapViewNamedService("b", map[string]string{"mon": "yes"}),
				swapViewNamedService("c", fatServiceLabels(), servicePorts(8, 15)...),
			},
			monitor: true,
			want:    map[string]int{"c": 8},
		},
		{
			name: "each Service still trimmed is named with its own count",
			services: []*corev1.Service{
				swapViewNamedService("a", fatServiceLabels(), servicePorts(1, 7)...),
				swapViewNamedService("c", fatServiceLabels(), servicePorts(8, 15)...),
			},
			want: map[string]int{"a": 7, "c": 8},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := store.New(time.Minute)
			st.UpsertPod(swapViewPod(map[string]string{scrape.AnnotationScrape: "true"}))
			svcs := services.NewIndex()
			for _, svc := range tc.services {
				svcs.Upsert(svc)
			}
			idx := servicemonitors.NewIndex()
			if tc.monitor {
				eps := make([]any, 0, 7)
				for i := 1; i <= 7; i++ {
					eps = append(eps, map[string]any{"targetPort": 9000 + i})
				}
				if err := idx.Upsert(&unstructured.Unstructured{Object: map[string]any{
					"metadata": map[string]any{"name": "sm-b", "namespace": "default"},
					"spec": map[string]any{
						"selector":  map[string]any{"matchLabels": map[string]any{"mon": "yes"}},
						"endpoints": eps,
					},
				}}); err != nil {
					t.Fatal(err)
				}
			}
			h := &recordingHandler{}
			s := New(Config{
				Store: st, Services: svcs, Monitors: idx, Resolver: stubResolver{}, Log: slog.New(h),
				MaxWait: 500 * time.Millisecond, CacheTTL: 10 * time.Second, Ready: closedChan(),
			})
			targets, _ := s.nodeTargets("node1")

			// What is actually served: the identity-only views per Service.
			served := map[string]int{}
			for _, tg := range targets {
				if tg.Service != nil && len(tg.Service.Labels) == 0 && tg.Service.Name != "b" {
					served[tg.Service.Name]++
				}
			}
			if len(served) != len(tc.want) {
				t.Fatalf("identity-only views served per Service = %v, want %v: the fixture no longer takes these arms",
					served, tc.want)
			}
			for name, n := range tc.want {
				if served[name] != n {
					t.Fatalf("identity-only views served per Service = %v, want %v: the fixture no longer takes these arms",
						served, tc.want)
				}
			}

			lines := h.matching("did not fit the pod's scrape-target byte budget")
			if len(lines) != len(tc.want) {
				t.Errorf("want one warning per trimmed Service %v, got %d: %v", tc.want, len(lines), lines)
			}
			for _, l := range lines {
				named := false
				for name, n := range tc.want {
					if strings.Contains(l, " service="+name+" ") {
						named = true
						if !strings.Contains(l, " targets="+strconv.Itoa(n)+" ") {
							t.Errorf("warning for Service %s does not count its %d trimmed targets: %s", name, n, l)
						}
					}
				}
				if !named {
					t.Errorf("warning names a Service trimmed on no served target (served: %v): %s", served, l)
				}
			}

			// And explain agrees with what is served.
			doc, _ := s.explainPod("default", "web-0")
			total := 0
			for _, n := range tc.want {
				total += n
			}
			if doc.ServiceViewsTrimmed != total {
				t.Errorf("explain serviceViewsTrimmed = %d, want %d", doc.ServiceViewsTrimmed, total)
			}
		})
	}
}

func getBody(t *testing.T, url string) []byte {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return body
}
