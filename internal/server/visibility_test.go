package server

// The metadata service is a fleet-wide dependency, so every refusal it makes
// has to be visible in its own logs and metrics: the agent on the other end
// sees a status code and nothing else, and most of these refusals end the same
// way — a target scraped without the credential its CR declares, or a pod that
// is simply absent from a target list.
//
// These tests pin the two halves the campaign asks for: a COUNTER for the rate
// and a LOG LINE for the context a counter cannot carry (which ref, which
// monitor, which pod), throttled where the condition can repeat per agent per
// cycle.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/scrape"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// authFixture builds a scrape-auth server with a recording logger. secrets or
// monitors may be nil, which is what the two "the feature is off" refusals
// need.
func authFixture(t *testing.T, sec SecretReader, monitors *servicemonitors.Index) (*httptest.Server, *recordingHandler) {
	t.Helper()
	h := &recordingHandler{}
	s := New(Config{
		Store:           store.New(time.Minute),
		Services:        services.NewIndex(),
		Monitors:        monitors,
		Resolver:        stubResolver{},
		MaxWait:         500 * time.Millisecond,
		Ready:           closedChan(),
		Secrets:         sec,
		ScrapeAuthToken: testScrapeToken,
		Log:             slog.New(h),
	})
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return srv, h
}

// An agent only asks for a credential because a monitor target THIS SERVICE
// served it names one, so a service that cannot serve credentials at all is a
// two-sided misconfiguration whose whole consequence is up=0 on the other end.
func TestScrapeAuthDisabledIsCountedAndNamed(t *testing.T) {
	srv, h := authFixture(t, nil, nil)
	before := obs.ScrapeAuthFailures.WithLabelValues("disabled").Value()

	if status, _ := get(t, srv.URL+"/v1/scrape-auth/monitoring/tok/token", "Bearer "+testScrapeToken); status != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", status)
	}
	if moved := obs.ScrapeAuthFailures.WithLabelValues("disabled").Value() - before; moved != 1 {
		t.Errorf("kubescrape_scrape_auth_failures_total{reason=\"disabled\"} moved by %v, want 1", moved)
	}
	lines := h.matching("-scrape-auth-secrets")
	if len(lines) != 1 {
		t.Fatalf("want exactly one line naming the flag, got %d: %v", len(lines), h.lines)
	}
	if !strings.Contains(lines[0], "flag=-scrape-auth-secrets") {
		t.Errorf("the line does not carry the flag an operator would change: %s", lines[0])
	}
}

// The 401 is the shape a -scrape-auth-token-file mismatch takes, and it was the
// one refusal on this route with neither a counter nor a line. What it must
// NEVER carry is the credential itself.
func TestScrapeAuthUnauthorizedIsCountedWithoutLeakingTheToken(t *testing.T) {
	const wrong = "definitely-not-the-token"
	for _, tc := range []struct {
		name, authorization, credential string
	}{
		{"no header at all", "", "missing"},
		{"a token that does not match", "Bearer " + wrong, "mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, h := authFixture(t, erroringSecrets{value: "v"}, servicemonitors.NewIndex())
			before := obs.ScrapeAuthFailures.WithLabelValues("unauthorized").Value()

			if status, _ := get(t, srv.URL+"/v1/scrape-auth/monitoring/tok/token", tc.authorization); status != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", status)
			}
			if moved := obs.ScrapeAuthFailures.WithLabelValues("unauthorized").Value() - before; moved != 1 {
				t.Errorf("kubescrape_scrape_auth_failures_total{reason=\"unauthorized\"} moved by %v, want 1", moved)
			}
			lines := h.matching("bearer token")
			if len(lines) != 1 {
				t.Fatalf("want one warning, got %d: %v", len(lines), h.lines)
			}
			if !strings.Contains(lines[0], "credential="+tc.credential) {
				t.Errorf("line does not distinguish a missing credential from a wrong one: %s", lines[0])
			}
			// The security boundary: neither the presented token nor the
			// configured one may appear anywhere in the logs.
			for _, l := range h.lines {
				if strings.Contains(l, wrong) || strings.Contains(l, testScrapeToken) {
					t.Fatalf("a bearer token reached the log: %s", l)
				}
			}
		})
	}
}

// The allowlist miss is the one an operator cannot guess: it means the INDEX
// disagrees with the target the agent was served (a monitor that failed to
// parse, or one refused by -monitor-namespaces), not that the Secret is
// missing.
func TestScrapeAuthAllowlistMissIsCountedAndNamesTheRef(t *testing.T) {
	srv, h := authFixture(t, erroringSecrets{value: "v"}, servicemonitors.NewIndex())
	before := obs.ScrapeAuthFailures.WithLabelValues("not_allowed").Value()

	if status, _ := get(t, srv.URL+"/v1/scrape-auth/tenant/creds/token", "Bearer "+testScrapeToken); status != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", status)
	}
	if moved := obs.ScrapeAuthFailures.WithLabelValues("not_allowed").Value() - before; moved != 1 {
		t.Errorf("kubescrape_scrape_auth_failures_total{reason=\"not_allowed\"} moved by %v, want 1", moved)
	}
	lines := h.matching("no indexed monitor endpoint")
	if len(lines) != 1 {
		t.Fatalf("want one warning, got %d: %v", len(lines), h.lines)
	}
	for _, want := range []string{"namespace=tenant", "name=creds", "key=token"} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("line is missing %q: %s", want, lines[0])
		}
	}
	// Throttled per ref: an agent re-asks every scrape cycle.
	for range 20 {
		get(t, srv.URL+"/v1/scrape-auth/tenant/creds/token", "Bearer "+testScrapeToken)
	}
	if got := len(h.matching("no indexed monitor endpoint")); got != 1 {
		t.Errorf("21 requests for one ref logged %d times; the throttle is not holding", got)
	}
	// …and a DIFFERENT ref is not masked by the first.
	get(t, srv.URL+"/v1/scrape-auth/tenant/other/token", "Bearer "+testScrapeToken)
	if got := len(h.matching("no indexed monitor endpoint")); got != 2 {
		t.Errorf("a second ref logged %d lines total, want 2", got)
	}
}

// -scrape-auth-secrets without -servicemonitors can never serve anything: the
// allowlist is built from indexed monitors.
func TestScrapeAuthWithoutMonitorsIsCounted(t *testing.T) {
	srv, h := authFixture(t, erroringSecrets{value: "v"}, nil)
	before := obs.ScrapeAuthFailures.WithLabelValues("no_monitors").Value()

	if status, _ := get(t, srv.URL+"/v1/scrape-auth/monitoring/tok/token", "Bearer "+testScrapeToken); status != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", status)
	}
	if moved := obs.ScrapeAuthFailures.WithLabelValues("no_monitors").Value() - before; moved != 1 {
		t.Errorf("kubescrape_scrape_auth_failures_total{reason=\"no_monitors\"} moved by %v, want 1", moved)
	}
	if got := len(h.matching("-servicemonitors")); got != 1 {
		t.Fatalf("want one line naming the flag, got %d: %v", got, h.lines)
	}
}

// The path-segment refusal is the re-cutting guard. No agent this repo ships
// can produce one, which is exactly why a rate on it is worth seeing — and why
// the caller-supplied value must be clipped before it reaches a log line.
func TestScrapeAuthBadSegmentIsCountedAndClipped(t *testing.T) {
	srv, h := authFixture(t, erroringSecrets{value: "v"}, servicemonitors.NewIndex())
	before := obs.ScrapeAuthFailures.WithLabelValues("bad_request").Value()

	status, _ := get(t, srv.URL+"/v1/scrape-auth/"+strings.Repeat("x", 600)+"..%2Fx/creds/token",
		"Bearer "+testScrapeToken)
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", status)
	}
	if moved := obs.ScrapeAuthFailures.WithLabelValues("bad_request").Value() - before; moved != 1 {
		t.Errorf("kubescrape_scrape_auth_failures_total{reason=\"bad_request\"} moved by %v, want 1", moved)
	}
	lines := h.matching("cannot name a Kubernetes object")
	if len(lines) != 1 {
		t.Fatalf("want one warning, got %d: %v", len(lines), h.lines)
	}
	if !strings.Contains(lines[0], "(truncated)") {
		t.Errorf("a 600-byte path segment reached the log unclipped: %s", lines[0])
	}
}

func TestClipSegmentMarksWhatItCut(t *testing.T) {
	if got := clipSegment("short"); got != "short" {
		t.Errorf("clipSegment(short) = %q, want it unchanged", got)
	}
	long := strings.Repeat("a", 1000)
	got := clipSegment(long)
	if len(got) >= len(long) || !strings.HasSuffix(got, "(truncated)") {
		t.Errorf("clipSegment did not clip and mark: len=%d suffix=%q", len(got), got[max(0, len(got)-16):])
	}
}

// A container lookup that BLOCKED for its whole budget and one that missed
// instantly are the same 404 to the HTTP counter, and they mean opposite
// things: the second says the store never learned about a container an agent is
// holding log lines for right now.
func TestContainerLookupTimeoutIsCountedAndNamed(t *testing.T) {
	h := &recordingHandler{}
	s := New(Config{
		Store: store.New(time.Minute), Services: services.NewIndex(), Resolver: stubResolver{},
		MaxWait: 50 * time.Millisecond, Ready: closedChan(), Log: slog.New(h),
	})
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)

	before := obs.ContainerLookupTimeouts.Value()
	if status, _ := get(t, srv.URL+"/v1/containers/containerd://abc123", ""); status != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", status)
	}
	if moved := obs.ContainerLookupTimeouts.Value() - before; moved != 1 {
		t.Errorf("kubescrape_container_lookup_timeouts_total moved by %v, want 1", moved)
	}
	lines := h.matching("container lookup timed out")
	if len(lines) != 1 {
		t.Fatalf("want one warning naming the id, got %d: %v", len(lines), h.lines)
	}
	if !strings.Contains(lines[0], "id=abc123") {
		t.Errorf("the line does not name the container id: %s", lines[0])
	}

	// A ?wait=0 poll — the cadvisor and ingest shape — never blocked, so it
	// must not be counted as a timeout however often it misses.
	before = obs.ContainerLookupTimeouts.Value()
	for range 5 {
		get(t, srv.URL+"/v1/containers/containerd://def456?wait=0", "")
	}
	if moved := obs.ContainerLookupTimeouts.Value() - before; moved != 0 {
		t.Errorf("five ?wait=0 misses moved the timeout counter by %v; they never blocked", moved)
	}
}

// selfServer serves /v1/self for a store holding one pod at 10.1.2.3.
func selfServer(t *testing.T) (*httptest.Server, *recordingHandler) {
	t.Helper()
	h := &recordingHandler{}
	st := store.New(time.Minute)
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "agent-1", Namespace: "monitoring", UID: types.UID("u1"), ResourceVersion: "1",
		},
		Spec:   corev1.PodSpec{NodeName: "node1"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.1.2.3"},
	})
	s := New(Config{
		Store: st, Services: services.NewIndex(), Resolver: stubResolver{},
		MaxWait: time.Second, Ready: closedChan(), Log: slog.New(h),
	})
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return srv, h
}

// The forwarding-header refusal is deliberate and it costs a working
// deployment (a mesh sidecar in the caller's own netns lands here), so the
// service must say that it happened — the agent's side only records that the
// by-name fallback ran.
func TestSelfForwardedRefusalIsCountedAndNamed(t *testing.T) {
	srv, h := selfServer(t)
	before := obs.SelfLookupRefused.WithLabelValues("forwarded").Value()

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/v1/self", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Forwarded-For", "10.9.9.9")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", res.StatusCode)
	}
	if moved := obs.SelfLookupRefused.WithLabelValues("forwarded").Value() - before; moved != 1 {
		t.Errorf("kubescrape_self_lookups_refused_total{reason=\"forwarded\"} moved by %v, want 1", moved)
	}
	lines := h.matching("forwarding header")
	if len(lines) != 1 {
		t.Fatalf("want one warning, got %d: %v", len(lines), h.lines)
	}
	if !strings.Contains(lines[0], "header=X-Forwarded-For") {
		t.Errorf("the line does not name the header that caused the refusal: %s", lines[0])
	}
}

// A caller whose address owns no live pod (hostNetwork, SNAT) is EXPECTED and
// recovers through the by-name fallback, so it is counted and deliberately not
// logged — the counter is how an operator tells that population from an
// attribution outage.
func TestSelfUnresolvedIsCountedAndNotLogged(t *testing.T) {
	srv, h := selfServer(t)
	before := obs.SelfLookupRefused.WithLabelValues("no_pod").Value()

	// The test client connects over loopback, which owns no pod in the store.
	if status, _ := get(t, srv.URL+"/v1/self", ""); status != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", status)
	}
	if moved := obs.SelfLookupRefused.WithLabelValues("no_pod").Value() - before; moved != 1 {
		t.Errorf("kubescrape_self_lookups_refused_total{reason=\"no_pod\"} moved by %v, want 1", moved)
	}
	if got := len(h.lines); got != 0 {
		t.Errorf("an expected hostNetwork/SNAT miss logged %d lines: %v", got, h.lines)
	}
}

// capWarnFixture is capFixture with a logger: one pod, one Service, one
// ServiceMonitor whose endpoints all resolve to distinct URLs on it.
func capWarnFixture(t *testing.T, eps []any) (*Server, *recordingHandler) {
	t.Helper()
	h := &recordingHandler{}
	st := store.New(time.Minute)
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web-1", Namespace: "default", UID: types.UID("pod-uid"), ResourceVersion: "1",
			Labels: map[string]string{"app": "web"},
		},
		Spec: corev1.PodSpec{
			NodeName: "node1",
			Containers: []corev1.Container{{
				Name: "app", Image: "img",
				Ports: []corev1.ContainerPort{{Name: "metrics", ContainerPort: 9090}},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.1.2.3"},
	})
	idx := services.NewIndex()
	idx.Upsert(monitorSelectedService())
	s := New(Config{
		Store: st, Services: idx, Monitors: newMonitorIndex(t, "sm-many", eps),
		Resolver: stubResolver{}, MaxWait: time.Second, Ready: closedChan(), Log: slog.New(h),
	})
	return s, h
}

// kubescrape_scrape_targets_capped_total's help sends the operator to
// /v1/explain — which needs a pod NAME the counter cannot supply. A refused
// endpoint is otherwise indistinguishable from one nobody configured.
func TestPerPodCeilingWarnsWithThePodToInspect(t *testing.T) {
	const endpoints = scrape.MaxPortsPerPod + 4
	eps := make([]any, 0, endpoints)
	for i := range endpoints {
		eps = append(eps, map[string]any{"port": "http", "path": "/m" + strconv.Itoa(i)})
	}
	s, h := capWarnFixture(t, eps)

	if _, ok := s.nodeTargets("node1"); !ok {
		t.Fatal("nodeTargets returned not-ok")
	}
	lines := h.matching("per-pod ceiling")
	if len(lines) != 1 {
		t.Fatalf("want one warning, got %d: %v", len(lines), h.lines)
	}
	for _, want := range []string{"namespace=default", "pod=web-1", "dropped=4",
		"limit=" + strconv.Itoa(scrape.MaxPortsPerPod), "/v1/explain/default/web-1"} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("line is missing %q: %s", want, lines[0])
		}
	}

	// Throttled by WORKLOAD: every agent poll re-derives this, and so does
	// every replica of the same misconfiguration.
	for range 10 {
		s.nodeTargets("node1")
	}
	if got := len(h.matching("per-pod ceiling")); got != 1 {
		t.Errorf("eleven derivations logged %d lines; the throttle is not holding", got)
	}
}

// An endpoint naming a port the pod does not declare is the commonest
// ServiceMonitor mistake and was the least visible outcome on the whole
// derivation: prometheus-operator emits no config for it either, so there is no
// failing scrape, no up=0 and no counter — the pod is just missing.
func TestUnresolvedMonitorEndpointIsWarnedAndThrottled(t *testing.T) {
	s, h := capWarnFixture(t, []any{map[string]any{"port": "typo-metrics"}})

	targets, _ := s.nodeTargets("node1")
	if len(targets) != 0 {
		t.Fatalf("the fixture must resolve to nothing; got %d targets", len(targets))
	}
	lines := h.matching("names a port the selected pod does not declare")
	if len(lines) != 1 {
		t.Fatalf("want one warning, got %d: %v", len(lines), h.lines)
	}
	for _, want := range []string{"kind=servicemonitor", "monitor=default/sm-many",
		"port=typo-metrics", "pod=web-1"} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("line is missing %q: %s", want, lines[0])
		}
	}
	for range 10 {
		s.nodeTargets("node1")
	}
	if got := len(h.matching("names a port the selected pod does not declare")); got != 1 {
		t.Errorf("eleven derivations logged %d lines; the throttle is not holding", got)
	}
}

// An endpoint naming NEITHER port nor targetPort already rides
// Endpoint.Ignored ("port(unset)"), which the metadata service logs once per
// changed monitor and counts. Reporting it here as well would say the same
// thing per pod per cycle.
func TestEndpointNamingNoPortAtAllIsNotWarnedTwice(t *testing.T) {
	s, h := capWarnFixture(t, []any{map[string]any{"path": "/metrics"}})
	s.nodeTargets("node1")
	if got := h.matching("names a port the selected pod does not declare"); len(got) != 0 {
		t.Errorf("the port(unset) case was reported here too: %v", got)
	}
}

// /v1/explain derives through the same accumulator and must stay read-only:
// the two warnings it would otherwise duplicate are the ones an operator
// following the metric's own help text would trigger by hand, on the very pods
// they are investigating.
func TestExplainEmitsNeitherNewWarning(t *testing.T) {
	const endpoints = scrape.MaxPortsPerPod + 4
	eps := make([]any, 0, endpoints)
	for i := range endpoints {
		eps = append(eps, map[string]any{"port": "http", "path": "/m" + strconv.Itoa(i)})
	}
	eps = append(eps, map[string]any{"port": "typo-metrics"})
	s, h := capWarnFixture(t, eps)

	for range 3 {
		s.explainPod("default", "web-1")
	}
	if got := len(h.lines); got != 0 {
		t.Errorf("/v1/explain emitted %d warnings: %v", got, h.lines)
	}
}

// The scrape-auth throttles that are keyless are keyless because the condition
// is a property of the PROCESS. They must still be independent of each other,
// or fixing one would hide the next.
func TestKeylessScrapeAuthThrottlesDoNotMaskEachOther(t *testing.T) {
	h := &recordingHandler{}
	s := New(Config{Log: slog.New(h)})
	if !s.warnAuthOff.Allow(scrapeAuthWarnEvery) {
		t.Fatal("the first disabled report was suppressed")
	}
	if s.warnAuthOff.Allow(scrapeAuthWarnEvery) {
		t.Error("a second disabled report inside the window was allowed")
	}
	for name, th := range map[string]interface{ Allow(time.Duration) bool }{
		"no-monitors": &s.warnAuthNoMonitors,
		"token":       &s.warnAuthToken,
		"segment":     &s.warnAuthSegment,
	} {
		if !th.Allow(scrapeAuthWarnEvery) {
			t.Errorf("the %s throttle was masked by the disabled one", name)
		}
	}
}

// The store's two write-lock anomalies are counters and not logs (nothing may
// log under that lock), so the counters are the whole signal and have to move.
func TestStoreCountsAMissedDeleteAndAContestedPodIP(t *testing.T) {
	st := store.New(time.Minute)
	pod := func(name, uid, ip string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: "default", UID: types.UID(uid), ResourceVersion: uid,
			},
			Spec:   corev1.PodSpec{NodeName: "node1"},
			Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: ip},
		}
	}
	// A recreation whose predecessor's Delete never arrived: the same
	// namespace/name under a new UID, with the old record still live.
	st.UpsertPod(pod("web-0", "uid-a", "10.1.2.3"))
	if n := st.NameReuses(); n != 0 {
		t.Fatalf("a first upsert counted %d name reuses", n)
	}
	st.UpsertPod(pod("web-0", "uid-b", "10.1.2.4"))
	if n := st.NameReuses(); n != 1 {
		t.Errorf("NameReuses = %d, want 1", n)
	}

	// Two LIVE pods reporting one address: the recycle race. The terminating
	// hand-off — the ordinary way an address is released — is deliberately not
	// counted, so this must be the only one.
	before := st.ContestedPodIPs()
	st.UpsertPod(pod("other", "uid-c", "10.1.2.4"))
	if moved := st.ContestedPodIPs() - before; moved != 1 {
		t.Errorf("ContestedPodIPs moved by %d, want 1", moved)
	}
	// A pod merely re-asserting an address it already holds is not a contest.
	before = st.ContestedPodIPs()
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "other", Namespace: "default", UID: types.UID("uid-c"), ResourceVersion: "9",
		},
		Spec:   corev1.PodSpec{NodeName: "node1"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.1.2.4"},
	})
	if moved := st.ContestedPodIPs() - before; moved != 0 {
		t.Errorf("re-asserting a held address moved ContestedPodIPs by %d, want 0", moved)
	}
}

// The Service index carries the same missed-delete guard, and the same
// invisibility before this.
func TestServicesCountsAMissedDelete(t *testing.T) {
	ix := services.NewIndex()
	svc := func(uid string) *corev1.Service {
		return &corev1.Service{ObjectMeta: metav1.ObjectMeta{
			Name: "web", Namespace: "default", UID: types.UID(uid), ResourceVersion: uid,
		}}
	}
	ix.Upsert(svc("uid-a"))
	if n := ix.NameReuses(); n != 0 {
		t.Fatalf("a first upsert counted %d name reuses", n)
	}
	ix.Upsert(svc("uid-b"))
	if n := ix.NameReuses(); n != 1 {
		t.Errorf("NameReuses = %d, want 1", n)
	}
}

// A response this process cannot serialise is a bug that answers 500 forever,
// and it used to be visible only as an HTTP status counter.
func TestEncodeFailureIsReportedOnce(t *testing.T) {
	h := &recordingHandler{}
	s := New(Config{Log: slog.New(h)})
	for range 5 {
		s.reportEncodeFailure("metadata response", fmt.Errorf("json: unsupported type: chan int"))
	}
	lines := h.matching("encoding a response failed")
	if len(lines) != 1 {
		t.Fatalf("want exactly one throttled report, got %d: %v", len(lines), h.lines)
	}
	if !strings.Contains(lines[0], "what=metadata response") {
		t.Errorf("the report does not say which response: %s", lines[0])
	}
}

// failingMarshaler fails the way a broken model field would.
type failingMarshaler struct{}

func (failingMarshaler) MarshalJSON() ([]byte, error) { return nil, errors.New("nope") }

func TestUnencodableTellsTheValueFromTheConnection(t *testing.T) {
	if unencodable(context.Canceled) {
		t.Error("a connection error was classified as an unserialisable value")
	}
	// A REAL encoding/json failure, not a hand-built error value: what is
	// being pinned is that errors.As reaches the types this package asks for,
	// and encoding/json wraps a marshaller's own error in *json.MarshalerError.
	_, err := json.Marshal(failingMarshaler{})
	if err == nil {
		t.Fatal("encoding a failing marshaller succeeded")
	}
	if !unencodable(err) {
		t.Errorf("an unserialisable value was not classified as one: %v", err)
	}
}
