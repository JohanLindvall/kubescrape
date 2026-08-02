// Tests for GET /v1/self: the CALLER's own pod, attributed by the connection's
// source address. The httptest client connects over loopback, so a pod whose
// PodIP is 127.0.0.1 is "the caller".
package server

import (
	"net/http"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/JohanLindvall/kubescrape/internal/store"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// addAgentPod adds a pod owning ip, standing in for the caller's own pod.
func addAgentPod(st *store.Store, ip string, hostNetwork bool) {
	ctrl := true
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "kubescrape-agent-xyz",
			Namespace:       "monitoring",
			UID:             types.UID("agent-uid"),
			ResourceVersion: "1",
			Labels:          map[string]string{"app.kubernetes.io/name": "kubescrape-agent"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "DaemonSet", Name: "kubescrape-agent", UID: "ds-uid", Controller: &ctrl,
			}},
		},
		Spec: corev1.PodSpec{
			NodeName:    "node1",
			HostNetwork: hostNetwork,
			Containers:  []corev1.Container{{Name: "agent", Image: "img"}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: ip},
	})
}

func TestSelfEndpointResolvesTheCaller(t *testing.T) {
	st := store.New(time.Minute)
	addAgentPod(st, "127.0.0.1", false)
	srv := testServer(t, st, closedChan())

	var pod kubemeta.Pod
	getJSON(t, srv.URL+"/v1/self", http.StatusOK, &pod)
	if pod.Name != "kubescrape-agent-xyz" || pod.Namespace != "monitoring" || pod.UID != "agent-uid" {
		t.Fatalf("pod = %+v; want the loopback caller's own pod", pod)
	}
	// The owner chain and namespace metadata must be enriched like every other
	// pod endpoint — they are what the caller stamps as k8s.daemonset.name.
	if len(pod.Owners) != 1 || pod.Owners[0].Kind != "DaemonSet" {
		t.Fatalf("owners = %+v", pod.Owners)
	}
}

// The response identifies the CALLER, so it is cached PRIVATE: a per-client
// cache (metaclient, one per process, always asking about the same pod) may
// hold it, a shared cache must not — that marker is the only thing standing
// between "cheap re-reads" and one caller being handed another's identity.
// The ETag is what makes a re-read a 304.
func TestSelfEndpointIsPrivatelyCached(t *testing.T) {
	st := store.New(time.Minute)
	addAgentPod(st, "127.0.0.1", false)
	srv := cachingServer(t, st, 10*time.Second)

	resp, err := http.Get(srv.URL + "/v1/self")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	cc := resp.Header.Get("Cache-Control")
	if !strings.HasPrefix(cc, "private,") || !strings.Contains(cc, "max-age=10") {
		t.Fatalf("Cache-Control = %q; want private + max-age", cc)
	}
	etag := resp.Header.Get("ETag")
	if etag == "" {
		t.Fatal("no ETag; a re-read could not be answered 304")
	}
	// The conditional re-read a poll makes: 304, no body.
	condGet(t, srv.URL+"/v1/self", etag, http.StatusNotModified)
}

// Turning the metadata cache off (-metadata-cache-ttl=0) removes the freshness
// lifetime — but NOT the private marker. A 200 with no freshness information is
// heuristically cacheable by a shared cache (RFC 9111 4.2.2), and this is the
// one response that names its caller, so at a zero TTL it must say so
// explicitly rather than fall silent.
func TestSelfEndpointStaysPrivateWithZeroTTL(t *testing.T) {
	st := store.New(time.Minute)
	addAgentPod(st, "127.0.0.1", false)
	srv := testServer(t, st, closedChan()) // CacheTTL unset

	resp, err := http.Get(srv.URL + "/v1/self")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if cc := resp.Header.Get("Cache-Control"); cc != "private, no-store" {
		t.Fatalf("Cache-Control = %q; want private, no-store with the metadata cache off", cc)
	}
	// The shared-cache routes keep their existing behaviour: no headers at all.
	pod, err := http.Get(srv.URL + "/v1/pods/monitoring/kubescrape-agent-xyz")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pod.Body.Close() }()
	if cc := pod.Header.Get("Cache-Control"); cc != "" {
		t.Fatalf("shared-cache route Cache-Control = %q; want none at a zero TTL", cc)
	}
}

// The peer address comes from the CONNECTION. A caller that sets
// X-Forwarded-For must not be handed the identity of the pod it names.
func TestSelfEndpointIgnoresForwardedForHeader(t *testing.T) {
	st := store.New(time.Minute)
	addAgentPod(st, "10.1.2.3", false) // some OTHER pod
	srv := testServer(t, st, closedChan())

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/v1/self", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Forwarded-For", "10.1.2.3")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status %d; want 404 — a spoofable header must not attribute the caller", resp.StatusCode)
	}
}

// A caller no live pod owns (outside the cluster, hostNetwork, or behind an
// address-rewriting hop) gets a 404, never someone else's identity.
func TestSelfEndpointUnknownCaller(t *testing.T) {
	st := store.New(time.Minute)
	srv := testServer(t, st, closedChan())
	getJSON(t, srv.URL+"/v1/self", http.StatusNotFound, nil)

	// hostNetwork pods share the node IP and are not indexed, even when their
	// reported PodIP happens to be the caller's address.
	st2 := store.New(time.Minute)
	addAgentPod(st2, "127.0.0.1", true)
	srv2 := testServer(t, st2, closedChan())
	getJSON(t, srv2.URL+"/v1/self", http.StatusNotFound, nil)
}
