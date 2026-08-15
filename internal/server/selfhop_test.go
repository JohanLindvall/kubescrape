package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// /v1/self hands a caller an IDENTITY based on the address its connection came
// from, and a wrong answer is not an error the caller can see: it is an agent
// stamping another pod's k8s.pod.name, uid, namespace and service.instance.id
// onto everything it exports about itself, with a 200 and nothing counted
// anywhere.
//
// The guarantee the route is documented with — a hop gets a 404 rather than
// someone else's identity — held for the hops that motivated it (hostNetwork,
// SNAT, off-cluster), all of which resolve to no live pod at all. It did NOT
// hold for the ordinary in-cluster shape: an egress gateway or proxy POD has a
// perfectly good pod IP, so re-originating the request handed the caller the
// PROXY's document, cached private, max-age.
//
// A hop that sets a forwarding header is a hop saying so, and that is refused
// now. The header is still never read for an ADDRESS — see the sibling test
// below, which is the property it must not cost.
func TestSelfRefusesAConnectionAHopDeclares(t *testing.T) {
	for _, header := range []string{"X-Forwarded-For", "Forwarded", "X-Real-Ip"} {
		t.Run(header, func(t *testing.T) {
			st := store.New(time.Minute)
			addNamedPod(st, "real-caller", "uid-caller", "10.0.0.5")
			addNamedPod(st, "egress-proxy", "uid-proxy", "10.0.0.9")
			api := New(Config{
				Store: st, Services: services.NewIndex(), Resolver: stubResolver{},
				MaxWait: time.Second, CacheTTL: 10 * time.Second, Ready: closedChan(),
			})

			req := httptest.NewRequest(http.MethodGet, "/v1/self", nil)
			req.RemoteAddr = "10.0.0.9:41234" // the proxy re-originated it
			req.Header.Set(header, "10.0.0.5")
			w := httptest.NewRecorder()
			api.Handler().ServeHTTP(w, req)

			if w.Code == http.StatusOK {
				var pod kubemeta.Pod
				_ = json.Unmarshal(w.Body.Bytes(), &pod)
				t.Fatalf("a request forwarded by a proxy POD was answered 200 with %s/%s (uid %s), "+
					"Cache-Control %q: the caller now stamps the proxy's identity on its own telemetry, "+
					"and /v1/self promises a 404 instead of someone else's identity",
					pod.Namespace, pod.Name, pod.UID, w.Header().Get("Cache-Control"))
			}
			if w.Code != http.StatusNotFound {
				t.Fatalf("status %d, want 404 (metaclient's documented fallback to PodByName is keyed on it); "+
					"body %q", w.Code, w.Body.String())
			}
			if body := w.Body.String(); !strings.Contains(body, header) {
				t.Errorf("the refusal does not name the header that caused it: %q", body)
			}
			// A refusal is never cached: the next request from this address may
			// be a direct one.
			if cc := w.Header().Get("Cache-Control"); !strings.Contains(cc, "no-store") {
				t.Errorf("Cache-Control = %q on the refusal, want no-store", cc)
			}
		})
	}
}

// The refusal must not become a way to be told about SOMEBODY ELSE. The header
// is evidence that the connection is a hop's; it is never an address to look
// up, so a caller naming another pod in it gets the same 404 whether or not the
// pod it named exists.
func TestSelfNeverResolvesTheForwardedForAddress(t *testing.T) {
	st := store.New(time.Minute)
	addNamedPod(st, "victim", "uid-victim", "10.0.0.5")
	api := New(Config{
		Store: st, Services: services.NewIndex(), Resolver: stubResolver{},
		MaxWait: time.Second, Ready: closedChan(),
	})
	req := httptest.NewRequest(http.MethodGet, "/v1/self", nil)
	req.RemoteAddr = "192.168.9.9:5000" // no pod owns this
	req.Header.Set("X-Forwarded-For", "10.0.0.5")
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status %d, want 404", w.Code)
	}
	if strings.Contains(w.Body.String(), "victim") {
		t.Fatalf("the header was used as an address: %q", w.Body.String())
	}
}

// The honest limit, pinned so it is not rediscovered as a bug and not read out
// of the doc as a stronger promise than it is: a hop that re-originates a
// request WITHOUT declaring itself is indistinguishable from the caller, and
// this route answers with the hop's own pod. Nothing in an HTTP request can
// separate the two — the connection is the only evidence there is — so the
// deployments that consume /v1/self must reach the service directly, which is
// what the shipped agent does (metaclient sets Proxy: nil).
func TestASilentReOriginatingHopIsAnsweredWithTheHopsOwnIdentity(t *testing.T) {
	st := store.New(time.Minute)
	addNamedPod(st, "egress-proxy", "uid-proxy", "10.0.0.9")
	api := New(Config{
		Store: st, Services: services.NewIndex(), Resolver: stubResolver{},
		MaxWait: time.Second, Ready: closedChan(),
	})
	req := httptest.NewRequest(http.MethodGet, "/v1/self", nil)
	req.RemoteAddr = "10.0.0.9:41234"
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status %d, want 200: a connection from a pod IP with nothing saying it is a hop is "+
			"served, and this test exists to say so out loud", w.Code)
	}
	var pod kubemeta.Pod
	if err := json.Unmarshal(w.Body.Bytes(), &pod); err != nil {
		t.Fatal(err)
	}
	if pod.Name != "egress-proxy" {
		t.Fatalf("pod = %s, want the hop's own pod", pod.Name)
	}
}

// addNamedPod adds a running, non-hostNetwork pod owning ip.
func addNamedPod(st *store.Store, name, uid, ip string) {
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "default", UID: types.UID(uid), ResourceVersion: "1",
		},
		Spec:   corev1.PodSpec{NodeName: "node1", Containers: []corev1.Container{{Name: "c", Image: "img"}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: ip},
	})
}
