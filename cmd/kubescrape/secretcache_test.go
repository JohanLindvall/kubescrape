package main

// The k8sSecretReader caches FAILURES so one broken monitor ref cannot turn
// into a permanent `secrets get` stream against the API server. That cache is
// shared by every agent in the fleet, and Get receives the inbound REQUEST's
// context — so what it may remember is constrained: a failure that describes
// the CLUSTER is cacheable, a failure that describes ONE CALLER is not.

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/JohanLindvall/kubescrape/internal/server"
)

// countingClient counts Secret GETs and returns a scripted outcome.
func countingClient(t *testing.T, calls *int, react func() (runtime.Object, error)) *fake.Clientset {
	t.Helper()
	c := fake.NewSimpleClientset()
	c.PrependReactor("get", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
		*calls++
		obj, err := react()
		return true, obj, err
	})
	return c
}

func secret(ns, name string, data map[string][]byte) *corev1.Secret {
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}, Data: data}
}

// A cluster-caused failure IS cached: that is the whole point of the negative
// cache — one misconfigured monitor ref must not become one API-server GET per
// agent per scrape cycle, forever.
func TestSecretReaderCachesClusterFailures(t *testing.T) {
	var calls int
	forbidden := apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, "tok", errors.New("nope"))
	r := &k8sSecretReader{client: countingClient(t, &calls, func() (runtime.Object, error) {
		return nil, forbidden
	})}
	for range 5 {
		if _, err := r.Get(context.Background(), "ns", "tok", "token"); err == nil {
			t.Fatal("want an error")
		}
	}
	if calls != 1 {
		t.Errorf("API-server GETs = %d, want 1 (the failure must be cached)", calls)
	}
}

// A CALLER-caused failure must NOT be cached. Get takes the inbound request's
// context, so an agent that disconnects mid-flight — a restart, a rolling
// update, a client timeout — yields context.Canceled. Remembering that would
// make one agent going away break that credential for the WHOLE FLEET until
// the entry expired: every other agent would get the cached error, which
// handleScrapeAuth reports as a 502.
func TestSecretReaderDoesNotCacheCallerCancellation(t *testing.T) {
	var calls int
	r := &k8sSecretReader{client: countingClient(t, &calls, func() (runtime.Object, error) {
		return nil, context.Canceled
	})}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Get(cancelled, "ns", "tok", "token"); err == nil {
		t.Fatal("want an error from the cancelled request")
	}

	// A DIFFERENT, healthy caller must still reach the API server rather than
	// inherit the first one's cancellation.
	r.client = countingClient(t, &calls, func() (runtime.Object, error) {
		return secret("ns", "tok", map[string][]byte{"token": []byte("v")}), nil
	})
	val, err := r.Get(context.Background(), "ns", "tok", "token")
	if err != nil {
		t.Fatalf("healthy caller inherited the cancelled caller's error: %v", err)
	}
	if val != "v" {
		t.Errorf("value = %q, want v", val)
	}
}

// The success path still caches, and the two TTLs differ: a failure is held far
// more briefly than a success, so a fixed RBAC grant takes effect in seconds.
func TestSecretReaderTTLsDiffer(t *testing.T) {
	if secretFailureTTL >= secretCacheTTL {
		t.Fatalf("failure TTL %v must be shorter than the success TTL %v", secretFailureTTL, secretCacheTTL)
	}
	ok := secretCacheEntry{value: "v", fetched: time.Now()}
	bad := secretCacheEntry{err: errors.New("x"), fetched: time.Now()}
	if ok.ttl() != secretCacheTTL || bad.ttl() != secretFailureTTL {
		t.Errorf("ttl() = %v / %v, want %v / %v", ok.ttl(), bad.ttl(), secretCacheTTL, secretFailureTTL)
	}
}

// Get releases the lock across the API call, so two concurrent misses for one
// ref race on the way back. A slower FAILURE must not displace the success that
// already landed: handleScrapeAuth serves a cached error as a 502 to every agent
// in the fleet, for the whole failure TTL, for a credential this process holds.
func TestSecretReaderFailureDoesNotOverwriteALiveSuccess(t *testing.T) {
	r := &k8sSecretReader{}
	now := time.Now()
	r.remember("ns/tok/token", secretCacheEntry{value: "v", fetched: now})
	r.remember("ns/tok/token", secretCacheEntry{err: errors.New("transient"), fetched: now.Add(time.Second)})

	val, err := r.Get(context.Background(), "ns", "tok", "token")
	if err != nil {
		t.Fatalf("a resolved credential was replaced by a slower failure: %v", err)
	}
	if val != "v" {
		t.Errorf("value = %q, want v", val)
	}

	// A success still replaces a cached failure — recovery must not wait out
	// the failure TTL when the value is already in hand.
	r = &k8sSecretReader{}
	r.remember("ns/tok/token", secretCacheEntry{err: errors.New("transient"), fetched: now})
	r.remember("ns/tok/token", secretCacheEntry{value: "v", fetched: now})
	if val, err := r.Get(context.Background(), "ns", "tok", "token"); err != nil || val != "v" {
		t.Fatalf("Get = %q, %v; a success must displace a cached failure", val, err)
	}
}

// A missing KEY is the client's mistake and stays cacheable, but it must remain
// matchable through the cache — handleScrapeAuth uses errors.Is to answer 404
// rather than the retryable 502 it gives cluster failures.
func TestSecretReaderCachedKeyMissStaysMatchable(t *testing.T) {
	var calls int
	r := &k8sSecretReader{client: countingClient(t, &calls, func() (runtime.Object, error) {
		return secret("ns", "tok", map[string][]byte{"other": []byte("v")}), nil
	})}
	for range 3 {
		_, err := r.Get(context.Background(), "ns", "tok", "token")
		if !errors.Is(err, server.ErrSecretKeyNotFound) {
			t.Fatalf("err = %v, want it to wrap ErrSecretKeyNotFound (a cached error must stay matchable)", err)
		}
	}
	if calls != 1 {
		t.Errorf("API-server GETs = %d, want 1", calls)
	}
}
