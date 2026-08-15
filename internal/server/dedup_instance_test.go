package server

// The URL dedup keeps two targets apart that the EXPORTED identity cannot:
// one host:port, two paths. scrape/instance.go carries the argument for
// serving both rather than merging one away; this file is the behaviour —
// both served, and the collision named where an operator will find it.

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/JohanLindvall/kubescrape/internal/scrape"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// recordingHandler keeps every emitted record's message and attributes.
type recordingHandler struct {
	mu    sync.Mutex
	lines []string
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder
	b.WriteString(r.Message)
	r.Attrs(func(a slog.Attr) bool {
		b.WriteString(" " + a.Key + "=" + a.Value.String())
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lines = append(h.lines, b.String())
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordingHandler) matching(substr string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, l := range h.lines {
		if strings.Contains(l, substr) {
			out = append(out, l)
		}
	}
	return out
}

// twoPathsOnOnePort builds the CRD-free shape: a pod annotated for scraping on
// container port 9090 with the default /metrics path, selected by a Service
// annotated for scraping with a DIFFERENT path whose targetPort is that same
// container port. Two URLs, one host:port — and no monitors involved, so this
// is reachable on any cluster with no prometheus-operator CRDs at all.
func twoPathsOnOnePort(t *testing.T, log *slog.Logger) *Server {
	t.Helper()
	st := store.New(time.Minute)
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web-1", Namespace: "default",
			UID: types.UID("pod-uid"), ResourceVersion: "1",
			Labels: map[string]string{"app": "web"},
			Annotations: map[string]string{
				"prometheus.io/scrape": "true",
				"prometheus.io/port":   "9090",
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "node1",
			Containers: []corev1.Container{{
				Name: "app", Image: "img",
				Ports: []corev1.ContainerPort{{Name: "metrics", ContainerPort: 9090}},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.9.9.9"},
	})
	svcs := services.NewIndex()
	svcs.Upsert(&corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web", Namespace: "default", UID: types.UID("svc-uid"),
			Annotations: map[string]string{
				"prometheus.io/scrape": "true",
				"prometheus.io/path":   "/actuator/prometheus",
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "web"},
			Ports:    []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromString("metrics")}},
		},
	})
	return New(Config{
		Store: st, Services: svcs, Log: log,
		Resolver: stubResolver{}, MaxWait: 500 * time.Millisecond, Ready: closedChan(),
	})
}

// Both targets are SERVED. Dropping one would be silent data loss —
// indistinguishable from an application that stopped exporting those series —
// and both declarations were written on purpose. This pins the decision, so a
// later "fix" that dedupes by host:port has to argue with a test.
func TestCollidingTargetsAreBothServed(t *testing.T) {
	h := &recordingHandler{}
	srv := httptest.NewServer(twoPathsOnOnePort(t, slog.New(h)).Handler())
	t.Cleanup(srv.Close)

	var out struct {
		Targets []struct {
			URL     string `json:"url"`
			Address string `json:"address"`
		} `json:"targets"`
	}
	getJSON(t, srv.URL+"/v1/nodes/node1/targets", http.StatusOK, &out)
	if len(out.Targets) != 2 {
		t.Fatalf("targets = %+v, want both paths served", out.Targets)
	}
	if out.Targets[0].Address != out.Targets[1].Address {
		t.Fatalf("fixture no longer collides: addresses %q and %q",
			out.Targets[0].Address, out.Targets[1].Address)
	}
}

// ...and the collision is NAMED. Every other symptom is anonymous: `up`
// alternating 0 and 1 at one timestamp, or a backend rejecting a duplicate
// sample — neither of which names the pod or the two paths.
func TestCollidingTargetsAreWarnedAbout(t *testing.T) {
	h := &recordingHandler{}
	srv := httptest.NewServer(twoPathsOnOnePort(t, slog.New(h)).Handler())
	t.Cleanup(srv.Close)

	getJSON(t, srv.URL+"/v1/nodes/node1/targets", http.StatusOK, &struct{}{})

	lines := h.matching("export the same series identity")
	if len(lines) != 1 {
		t.Fatalf("logged %d collision warnings, want exactly 1: %v", len(lines), h.lines)
	}
	for _, want := range []string{
		"job=default/web-1", // no workload owner in this fixture: service.name is the pod's
		"instance=10.9.9.9:9090",
		"http://10.9.9.9:9090/metrics",
		"http://10.9.9.9:9090/actuator/prometheus",
		"on default/web-1", // every member names its pod
	} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("warning does not name %q, so it cannot be acted on: %s", want, lines[0])
		}
	}
}

// The collision is a STEADY state re-derived on every targets request of every
// agent whose node holds one of the pods, so the line is throttled — otherwise
// the notice is a flood proportional to fleet size and gets filtered out, which
// is the same as not warning at all.
func TestCollisionWarningIsThrottledAcrossRequests(t *testing.T) {
	h := &recordingHandler{}
	s := twoPathsOnOnePort(t, slog.New(h))
	for range 8 {
		if _, built := s.nodeTargets("node1"); !built {
			t.Fatal("no targets built")
		}
	}
	if n := len(h.matching("export the same series identity")); n != 1 {
		t.Errorf("logged %d warnings over 8 target derivations, want 1", n)
	}
}

// An ordinary multi-port pod must not be warned about: two container ports are
// two exported instances, which is what host:port buys.
func TestTwoPortsAreNotWarnedAbout(t *testing.T) {
	h := &recordingHandler{}
	st := store.New(time.Minute)
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web-1", Namespace: "default",
			UID: types.UID("pod-uid"), ResourceVersion: "1",
			Annotations: map[string]string{
				"prometheus.io/scrape": "true",
				"prometheus.io/port":   "8080,9100",
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "node1",
			Containers: []corev1.Container{{
				Name: "app", Image: "img",
				Ports: []corev1.ContainerPort{{ContainerPort: 8080}, {ContainerPort: 9100}},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.9.9.9"},
	})
	s := New(Config{
		Store: st, Services: services.NewIndex(), Log: slog.New(h),
		Resolver: stubResolver{}, MaxWait: 500 * time.Millisecond, Ready: closedChan(),
	})
	targets, _ := s.nodeTargets("node1")
	if len(targets) != 2 {
		t.Fatalf("fixture built %d targets, want 2", len(targets))
	}
	if n := len(h.matching("export the same series identity")); n != 0 {
		t.Errorf("warned %d times about two DISTINCT ports: %v", n, h.lines)
	}
}

// /v1/explain is the operator's "why is this pod (not) scraped?" — it listed
// both targets and said nothing about their collapsing onto one identity, which
// is the one thing about this pod's targets that is wrong.
func TestExplainNamesTheCollidingTargets(t *testing.T) {
	srv := httptest.NewServer(twoPathsOnOnePort(t, slog.New(slog.DiscardHandler)).Handler())
	t.Cleanup(srv.Close)

	var doc struct {
		Targets []struct {
			URL          string   `json:"url"`
			CollidesWith []string `json:"collidesWith"`
		} `json:"targets"`
	}
	getJSON(t, srv.URL+"/v1/explain/default/web-1", http.StatusOK, &doc)
	if len(doc.Targets) != 2 {
		t.Fatalf("explain listed %d targets, want 2: %+v", len(doc.Targets), doc.Targets)
	}
	for _, tg := range doc.Targets {
		if len(tg.CollidesWith) != 1 {
			t.Fatalf("target %s reports collidesWith=%v, want the one other target", tg.URL, tg.CollidesWith)
		}
		if tg.CollidesWith[0] == tg.URL {
			t.Errorf("target %s reports colliding with itself", tg.URL)
		}
	}
	if doc.Targets[0].CollidesWith[0] != doc.Targets[1].URL ||
		doc.Targets[1].CollidesWith[0] != doc.Targets[0].URL {
		t.Errorf("the two targets do not name each other: %+v", doc.Targets)
	}
}

// ...and it reports the SAME collisions the served list has, because it reads
// them off the same dedup. explain exists to explain what nodeTargets serves;
// a second implementation shaped like the first is the drift
// explain_parity_test.go was written after, and sameExplainTarget there does
// not compare this field.
func TestExplainCollisionsMatchTheServedList(t *testing.T) {
	s := twoPathsOnOnePort(t, slog.New(slog.DiscardHandler))
	doc, _ := s.explainPod("default", "web-1")
	served, _ := s.nodeTargets("node1")

	var scan scrape.InstanceScan
	want := map[string][]string{}
	for _, c := range scan.Collisions(served) {
		for _, ct := range c.Targets {
			for _, other := range c.Targets {
				if other.URL != ct.URL {
					want[ct.URL] = append(want[ct.URL], other.URL)
				}
			}
		}
	}
	if len(want) != 2 {
		t.Fatalf("the served list holds %d colliding targets, want 2: %+v", len(want), want)
	}
	for _, tg := range doc.Targets {
		if !slices.Equal(tg.CollidesWith, want[tg.URL]) {
			t.Errorf("explain says %s collides with %v, the served list says %v",
				tg.URL, tg.CollidesWith, want[tg.URL])
		}
	}
}

// A pod with one target must not carry the field at all — omitempty, so the
// overwhelming majority of documents are unchanged.
func TestExplainOmitsCollidesWithWhenNothingCollides(t *testing.T) {
	st := store.New(time.Minute)
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web-1", Namespace: "default",
			UID: types.UID("pod-uid"), ResourceVersion: "1",
			Annotations: map[string]string{"prometheus.io/scrape": "true", "prometheus.io/port": "9090"},
		},
		Spec: corev1.PodSpec{
			NodeName:   "node1",
			Containers: []corev1.Container{{Name: "app", Image: "img", Ports: []corev1.ContainerPort{{ContainerPort: 9090}}}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.9.9.9"},
	})
	srv := httptest.NewServer(New(Config{
		Store: st, Services: services.NewIndex(),
		Resolver: stubResolver{}, MaxWait: 500 * time.Millisecond, Ready: closedChan(),
	}).Handler())
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/v1/explain/default/web-1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	buf := make([]byte, 8192)
	n, _ := resp.Body.Read(buf)
	if body := string(buf[:n]); strings.Contains(body, "collidesWith") {
		t.Errorf("a non-colliding pod's document carries collidesWith: %s", body)
	}
}

// A pod-annotation target displacing a service-annotation one for the same URL
// carries strictly LESS than what it displaces: it has no Service, so
// promscrape's fillTargetResource stamps no k8s.service.name and no
// k8s.service.uid, and every sample of that endpoint loses the service join —
// while the SAME cluster with only the Service annotated keeps both attributes.
// This is the loss TestPodMonitorUpgradeKeepsTheServiceMetadata diagnosed on
// the replace path, on the arm that keeps the holder instead.
func TestPodAnnotationWinnerKeepsTheServiceMetadata(t *testing.T) {
	st := store.New(time.Minute)
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web-1", Namespace: "default",
			UID: types.UID("pod-uid"), ResourceVersion: "1",
			Labels: map[string]string{"app": "web"},
			Annotations: map[string]string{
				"prometheus.io/scrape": "true",
				"prometheus.io/port":   "9090",
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "node1",
			Containers: []corev1.Container{{
				Name: "app", Image: "img",
				Ports: []corev1.ContainerPort{{Name: "metrics", ContainerPort: 9090}},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.9.9.9"},
	})
	svcs := services.NewIndex()
	svcs.Upsert(&corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web", Namespace: "default", UID: types.UID("svc-uid"),
			Annotations: map[string]string{"prometheus.io/scrape": "true"},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "web"},
			Ports:    []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromString("metrics")}},
		},
	})
	srv := httptest.NewServer(New(Config{
		Store: st, Services: svcs,
		Resolver: stubResolver{}, MaxWait: 500 * time.Millisecond, Ready: closedChan(),
	}).Handler())
	t.Cleanup(srv.Close)

	var out struct {
		Targets []struct {
			URL     string `json:"url"`
			Source  string `json:"source"`
			Service *struct {
				Name string `json:"name"`
				UID  string `json:"uid"`
			} `json:"service"`
		} `json:"targets"`
	}
	getJSON(t, srv.URL+"/v1/nodes/node1/targets", http.StatusOK, &out)
	if len(out.Targets) != 1 {
		t.Fatalf("want one deduped target, got %+v", out.Targets)
	}
	got := out.Targets[0]
	if got.Source != "pod" {
		t.Fatalf("the pod-annotation target did not win the dedup: %+v", got)
	}
	if got.Service == nil || got.Service.Name != "web" || got.Service.UID != "svc-uid" {
		t.Errorf("the surviving target lost the Service the service-annotation target carried: %+v", got.Service)
	}
}

// deploymentResolver resolves a ReplicaSet owner into the chain the real
// owners.Resolver returns — [ReplicaSet, Deployment] — because that chain is
// what makes attrs.ServiceName derive ONE service.name, and therefore one job,
// for every replica of a workload. stubResolver returns the refs verbatim, so a
// fixture using it gives each replica a job of its own and cannot exercise
// anything below.
type deploymentResolver struct{ stubResolver }

func (r deploymentResolver) Resolve(ns string, refs []metav1.OwnerReference) []kubemeta.Owner {
	out := r.stubResolver.Resolve(ns, refs)
	for i := range out {
		if out[i].Kind != "ReplicaSet" {
			continue
		}
		return append(out, kubemeta.Owner{
			APIVersion: "apps/v1", Kind: "Deployment",
			Name: strings.TrimSuffix(out[i].Name, "-abc"), UID: "deploy-uid",
		})
	}
	return out
}

// replicaPod is one replica of the "web" Deployment, annotated for scraping on
// port. With hostNetwork set, ip is the NODE's address and every replica on
// that node reports the same one — which is the whole point of the pairs below.
func replicaPod(name, ip, port string, hostNetwork bool) *corev1.Pod {
	ctrl := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "default",
			UID: types.UID("uid-" + name), ResourceVersion: "1",
			Labels: map[string]string{"app": "web"},
			Annotations: map[string]string{
				"prometheus.io/scrape": "true",
				"prometheus.io/port":   port,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web-abc", UID: "rs-uid", Controller: &ctrl,
			}},
		},
		Spec: corev1.PodSpec{
			NodeName:    "node1",
			HostNetwork: hostNetwork,
			Containers: []corev1.Container{{
				Name: "app", Image: "img",
				Ports: []corev1.ContainerPort{{Name: "metrics", ContainerPort: 9090}},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: ip},
	}
}

// THE STRONGEST FORM of the collision, and the one a pod-scoped scan could not
// see: two hostNetwork REPLICAS of one workload, annotated for one port. They
// share the node's address, and being replicas they share the Deployment, so
// they share the job — and their URLs are identical, so unlike two paths on one
// port not even url.full distinguishes the two exports.
//
// The port needs no bind: the target's port is the ANNOTATION's. Both targets
// are served (the URL dedup is per pod) and the agent scrapes both — its
// scheduleKey comment says so — so `up` for this workload arrives twice per
// cycle at one (job, instance) with disagreeing values.
func TestTwoHostNetworkReplicasOfOneWorkloadAreWarnedAbout(t *testing.T) {
	h := &recordingHandler{}
	st := store.New(time.Minute)
	st.UpsertPod(replicaPod("web-abc-1", "10.0.0.5", "9100", true))
	st.UpsertPod(replicaPod("web-abc-2", "10.0.0.5", "9100", true))
	s := New(Config{
		Store: st, Services: services.NewIndex(), Log: slog.New(h),
		Resolver: deploymentResolver{}, MaxWait: 500 * time.Millisecond, Ready: closedChan(),
	})

	targets, _ := s.nodeTargets("node1")
	if len(targets) != 2 {
		t.Fatalf("served %d targets, want both replicas': %+v", len(targets), targets)
	}
	if targets[0].URL != targets[1].URL {
		t.Fatalf("fixture no longer collides: %q vs %q", targets[0].URL, targets[1].URL)
	}
	lines := h.matching("export the same series identity")
	if len(lines) != 1 {
		t.Fatalf("logged %d collision warnings, want exactly 1: %v", len(lines), h.lines)
	}
	for _, want := range []string{
		"job=default/web", // the Deployment, shared by both replicas
		"instance=10.0.0.5:9100",
		"on default/web-abc-1",
		"on default/web-abc-2",
	} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("warning does not name %q, so the pair cannot be found: %s", want, lines[0])
		}
	}
}

// ...and the bound on that: two DIFFERENT workloads sharing the node's address
// on one annotated port collide on `instance` and NOT on `job`, so their series
// stay distinct and nothing is warned about. Warning here would be false in the
// message's own words, and it is the shape with real false positives — two
// hostNetwork DaemonSets annotated for one well-known port.
func TestTwoHostNetworkWorkloadsSharingAnAddressAreNotWarnedAbout(t *testing.T) {
	h := &recordingHandler{}
	st := store.New(time.Minute)
	st.UpsertPod(replicaPod("web-abc-1", "10.0.0.5", "9100", true))
	other := replicaPod("other-def-1", "10.0.0.5", "9100", true)
	other.OwnerReferences[0].Name = "other-abc"
	other.OwnerReferences[0].UID = "rs2-uid"
	st.UpsertPod(other)
	s := New(Config{
		Store: st, Services: services.NewIndex(), Log: slog.New(h),
		Resolver: deploymentResolver{}, MaxWait: 500 * time.Millisecond, Ready: closedChan(),
	})

	targets, _ := s.nodeTargets("node1")
	if len(targets) != 2 || targets[0].URL != targets[1].URL {
		t.Fatalf("fixture must serve two targets on one URL: %+v", targets)
	}
	if n := len(h.matching("export the same series identity")); n != 0 {
		t.Errorf("warned %d times about two workloads whose jobs differ: %v", n, h.lines)
	}
}

// misconfiguredWorkload is n replicas of ONE Deployment, each annotated for
// /metrics on 9090 and each selected by a Service annotated for
// /actuator/prometheus on the same container port: n pods, 2n targets, n
// collisions, ONE configuration mistake.
func misconfiguredWorkload(t *testing.T, log *slog.Logger, n int, gen string) *Server {
	t.Helper()
	st := store.New(time.Minute)
	for i := range n {
		st.UpsertPod(replicaPod("web-"+gen+"-"+strconv.Itoa(i), "10.244."+gen+"."+strconv.Itoa(i), "9090", false))
	}
	svcs := services.NewIndex()
	svcs.Upsert(&corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web", Namespace: "default", UID: types.UID("svc-uid"),
			Annotations: map[string]string{
				"prometheus.io/scrape": "true",
				"prometheus.io/path":   "/actuator/prometheus",
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "web"},
			Ports:    []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromString("metrics")}},
		},
	})
	return New(Config{
		Store: st, Services: svcs, Log: log,
		Resolver: deploymentResolver{}, MaxWait: 500 * time.Millisecond, Ready: closedChan(),
	})
}

// ONE mistake is ONE line, whatever the replica count and however often the
// pods are replaced. Keying the throttle on the pod and its address made this
// 20 lines, and 20 more the instant a rollout gave the replicas new names and
// new IPs — a table of 40 for one annotation to fix. That is the flood the
// throttle exists to prevent, and it is the third time this repo has recorded
// the mistake (see collisionWarnKey).
//
// The workload owner is what collapses them: it is in the job, it is not in the
// pod's name and it outlives every pod under it — the same property that makes
// the agent's warnTarget key on a Deployment rather than on a URL.
func TestOneMisconfiguredWorkloadIsOneWarningAcrossReplicasAndRollouts(t *testing.T) {
	h := &recordingHandler{}
	s := misconfiguredWorkload(t, slog.New(h), 20, "1")

	targets, _ := s.nodeTargets("node1")
	if len(targets) != 40 {
		t.Fatalf("served %d targets, want 40 (20 replicas x 2 paths)", len(targets))
	}
	// The DETECTION must find all twenty; only the reporting is throttled.
	var scan scrape.InstanceScan
	if n := len(scan.Collisions(targets)); n != 20 {
		t.Fatalf("found %d collisions in the served list, want 20 — the throttle assertion below would be vacuous", n)
	}
	if n := len(h.matching("export the same series identity")); n != 1 {
		t.Errorf("logged %d warnings for 20 replicas of one misconfigured Deployment, want 1", n)
	}
	if n := s.warnCollide.Len(); n != 1 {
		t.Errorf("throttle table holds %d keys for one mistake, want 1", n)
	}

	// The rollout: 20 new pod names, 20 new pod IPs, same annotations. Nothing
	// an operator has to fix has changed, so there is nothing new to say.
	rolled := misconfiguredWorkload(t, slog.New(h), 20, "2")
	rolled.warnCollide = s.warnCollide // the same process, the same table
	if _, built := rolled.nodeTargets("node1"); !built {
		t.Fatal("no targets built after the rollout")
	}
	if n := len(h.matching("export the same series identity")); n != 1 {
		t.Errorf("logged %d warnings in total after a rollout, want the original 1", n)
	}
	if n := s.warnCollide.Len(); n != 1 {
		t.Errorf("throttle table holds %d keys after a rollout, want 1", n)
	}
}

// A SECOND, genuinely different mistake still gets its own line: the key is the
// configuration, so a different port under the same job is a different key.
func TestASecondCollidingPortIsItsOwnWarning(t *testing.T) {
	h := &recordingHandler{}
	st := store.New(time.Minute)
	// One pod annotated for two ports, each of which also carries a Service
	// path — two independent collisions on one pod.
	p := replicaPod("web-abc-1", "10.244.1.1", "9090,9100", false)
	p.Spec.Containers[0].Ports = append(p.Spec.Containers[0].Ports,
		corev1.ContainerPort{Name: "second", ContainerPort: 9100})
	st.UpsertPod(p)
	svcs := services.NewIndex()
	for _, port := range []struct{ name, target string }{{"a", "metrics"}, {"b", "second"}} {
		svcs.Upsert(&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name: "web-" + port.name, Namespace: "default", UID: types.UID("svc-" + port.name),
				Annotations: map[string]string{
					"prometheus.io/scrape": "true",
					"prometheus.io/path":   "/actuator/prometheus",
				},
			},
			Spec: corev1.ServiceSpec{
				Selector: map[string]string{"app": "web"},
				Ports:    []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromString(port.target)}},
			},
		})
	}
	s := New(Config{
		Store: st, Services: svcs, Log: slog.New(h),
		Resolver: deploymentResolver{}, MaxWait: 500 * time.Millisecond, Ready: closedChan(),
	})
	if _, built := s.nodeTargets("node1"); !built {
		t.Fatal("no targets built")
	}
	if n := len(h.matching("export the same series identity")); n != 2 {
		t.Errorf("logged %d warnings for two colliding ports, want 2: %v", n, h.lines)
	}
}

// The repo's own corpus holds a colliding shape, and it is worth saying so
// deliberately rather than leaving it to be rediscovered: server_test.go's
// TestNodeTargetsFromServiceMonitor puts two ServiceMonitor endpoints — /stats
// and /metrics — on one container port of default/mon-web-1, which is this
// defect with monitors instead of annotations. Two CRs, neither of them wrong
// on its own, are all it takes.
func TestTwoMonitorEndpointsOnOnePortCollide(t *testing.T) {
	var scan scrape.InstanceScan
	p := kubemeta.Pod{Namespace: "default", Name: "mon-web-1"}
	got := scan.Collisions([]kubemeta.ScrapeTarget{
		{URL: "http://10.1.2.5:9090/stats", Address: "10.1.2.5:9090", Source: "servicemonitor", Monitor: "monitoring/web", Pod: p},
		{URL: "http://10.1.2.5:9090/metrics", Address: "10.1.2.5:9090", Source: "servicemonitor", Monitor: "monitoring/web-direct", Pod: p},
	})
	if len(got) != 1 || len(got[0].Targets) != 2 {
		t.Fatalf("collisions = %+v, want the two monitors' endpoints as one group", got)
	}
	if got[0].Targets[0].Monitor != "monitoring/web" || got[0].Targets[1].Monitor != "monitoring/web-direct" {
		t.Errorf("group does not name both monitors: %+v", got[0].Targets)
	}
}
