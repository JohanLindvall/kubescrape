// Tests for GET /v1/self: the CALLER's own pod, attributed by the connection's
// source address. The httptest client connects over loopback, so a pod whose
// PodIP is 127.0.0.1 is "the caller".
package server

import (
	"net/http"
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

// The response depends on WHO asked, so it must never be cached — neither by
// the metaclient (which caches on Cache-Control/ETag) nor by a proxy that
// caches a header-less 200 heuristically.
func TestSelfEndpointIsNeverCached(t *testing.T) {
	st := store.New(time.Minute)
	addAgentPod(st, "127.0.0.1", false)
	srv := cachingServer(t, st, 10*time.Second) // metadata cache ON

	resp, err := http.Get(srv.URL + "/v1/self")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-store" {
		t.Fatalf("Cache-Control = %q; want no-store", cc)
	}
	if etag := resp.Header.Get("ETag"); etag != "" {
		t.Fatalf("ETag = %q; want none on a caller-dependent response", etag)
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

func TestPeerIP(t *testing.T) {
	for _, tc := range []struct{ addr, want string }{
		{"10.0.0.1:34512", "10.0.0.1"},
		{"[fd00::1]:34512", "fd00::1"},
		{"[fe80::1%eth0]:34512", "fe80::1"}, // zone stripped: the store keys bare IPs
		{"10.0.0.1", "10.0.0.1"},            // no port
		{"", ""},
		{"not-an-ip:80", ""},
		{"kubescrape.monitoring:80", ""},
	} {
		if got := peerIP(tc.addr); got != tc.want {
			t.Errorf("peerIP(%q) = %q, want %q", tc.addr, got, tc.want)
		}
	}
}
