package main

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/JohanLindvall/kubescrape/internal/server"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/metaclient"
)

// /v1/self refuses a request that carries a forwarding header, because a hop
// that sets one is saying the connection is not its caller's. That refusal is
// paid for by a deployment shape where the connection IS the caller's: a
// same-network-namespace sidecar (a mesh proxy on the agent's own pod) appends
// X-Forwarded-For without changing the source address at all, so an agent that
// was answered 200 — correctly — is now answered 404.
//
// The whole argument for accepting that cost is that the agent falls back to a
// lookup BY NAME and ends up with the same pod. This test is that claim
// VERIFIED rather than assumed: it runs the REAL metadata-service handler over
// a real store, with a transport standing in for the sidecar, and requires the
// agent's resolver to come out the other side holding its own identity.
//
// Both halves are here on purpose. The first is the deployment the refusal
// regresses (no header, answered by peer address) — without it a future change
// could satisfy the second by refusing everything. The second is the sidecar.
func TestSelfResolveSurvivesASidecarThatAppendsForwardedFor(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "monitoring")
	t.Setenv("POD_NAME", "kubescrape-agent-xyz")

	// The pod owns the loopback address, because that is the source address an
	// httptest connection actually arrives from: the peer-IP lookup has to be
	// able to succeed here, or the "correct 200 today" half proves nothing.
	st := store.New(time.Minute)
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "kubescrape-agent-xyz", Namespace: "monitoring",
			UID: types.UID("agent-uid"), ResourceVersion: "1",
		},
		Spec:   corev1.PodSpec{NodeName: "node1", Containers: []corev1.Container{{Name: "agent", Image: "img"}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "127.0.0.1"},
	})
	ready := make(chan struct{})
	close(ready)
	api := server.New(server.Config{
		Store: st, Services: services.NewIndex(), Resolver: nullResolver{},
		MaxWait: time.Second, CacheTTL: 10 * time.Second, Ready: ready,
	})

	var mu sync.Mutex
	hits := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits[r.URL.Path]++
		mu.Unlock()
		api.Handler().ServeHTTP(w, r)
	}))
	defer srv.Close()
	count := func(path string) int {
		mu.Lock()
		defer mu.Unlock()
		return hits[path]
	}

	direct := metaclient.New(metaclient.Config{Base: srv.URL, Timeout: 5 * time.Second})
	pod, err := selfResolve(direct)(t.Context())
	if err != nil {
		t.Fatalf("a direct agent could not resolve itself at all: %v", err)
	}
	if pod.Name != "kubescrape-agent-xyz" || pod.UID != "agent-uid" {
		t.Fatalf("direct resolve returned %s/%s (uid %q)", pod.Namespace, pod.Name, pod.UID)
	}
	if n := count("/v1/pods/monitoring/kubescrape-agent-xyz"); n != 0 {
		t.Fatalf("the direct agent fell back to the by-name lookup %d times: the peer-address answer this "+
			"route exists for is not being produced, so the sidecar half below proves nothing", n)
	}

	// …and now the same agent, behind a sidecar that appends the header and
	// changes nothing else.
	viaSidecar := metaclient.New(metaclient.Config{
		Base: srv.URL, Timeout: 5 * time.Second,
		Transport: appendForwardedFor{http.DefaultTransport},
	})
	pod, err = selfResolve(viaSidecar)(t.Context())
	if err != nil {
		t.Fatalf("an agent whose sidecar appends X-Forwarded-For cannot resolve itself: %v\n"+
			"/v1/self refuses it by design, so this is the fallback that refusal is justified by, and it "+
			"did not cover the case", err)
	}
	if pod.Name != "kubescrape-agent-xyz" || pod.Namespace != "monitoring" || pod.UID != "agent-uid" {
		t.Fatalf("behind a sidecar the agent resolved %s/%s (uid %q) — not itself", pod.Namespace, pod.Name, pod.UID)
	}
	if n := count("/v1/pods/monitoring/kubescrape-agent-xyz"); n != 1 {
		t.Fatalf("by-name lookups = %d, want exactly 1: the identity above did not come from the fallback, "+
			"so this test is not measuring the path it names", n)
	}
	if n := count("/v1/self"); n != 2 {
		t.Fatalf("/v1/self was asked %d times, want 2 (once per resolve): the refusal is not being reached", n)
	}
}

// nullResolver is the enrichment the pod routes call for. Owners and namespace
// metadata are not what this test is about; the identity is.
type nullResolver struct{}

func (nullResolver) Resolve(string, []metav1.OwnerReference) []kubemeta.Owner { return nil }
func (nullResolver) Namespace(string) *kubemeta.ObjectMeta                    { return nil }
func (nullResolver) Node(string) *kubemeta.ObjectMeta                         { return nil }

// appendForwardedFor is the sidecar: it adds the header a mesh proxy adds and
// touches nothing else — in particular NOT the source address, which stays the
// caller's own, which is exactly why the refusal costs this deployment its
// peer-address answer.
type appendForwardedFor struct{ inner http.RoundTripper }

func (a appendForwardedFor) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("X-Forwarded-For", "10.42.0.7")
	return a.inner.RoundTrip(r)
}
