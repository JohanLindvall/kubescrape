package main

// The Secret reader behind /v1/scrape-auth (-scrape-auth-secrets), with its
// TTL cache, and the bearer token set that authenticates that route.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/JohanLindvall/kubescrape/internal/bearer"
	"github.com/JohanLindvall/kubescrape/internal/server"
)

// k8sSecretReader resolves Secret keys on demand with a short cache (tokens
// rotate; per-scrape-cycle lookups must not hammer the API server).
type k8sSecretReader struct {
	client kubernetes.Interface
	mu     sync.Mutex
	cache  map[string]secretCacheEntry
}

type secretCacheEntry struct {
	value   string
	err     error
	fetched time.Time
}

// secretCacheTTL bounds how long a resolved Secret value is reused. Entries
// past it are not merely re-fetched but DROPPED (see evictExpiredLocked): this
// process holds cluster-wide `secrets: get`, so every value it has ever
// resolved — bearer tokens, CA bundles, client private keys — was otherwise
// pinned in its heap for the lifetime of the pod, growing by one permanent
// entry per distinct ns/name/key ever seen. Monitor churn alone (GitOps
// renames, per-release secret names) makes that unbounded.
const secretCacheTTL = time.Minute

// secretFailureTTL bounds how long a FAILED resolution is remembered. Failures
// were not cached at all, so a single monitor referencing a key that does not
// exist — allowlisted by AuthSecretRefs, so it passes the handler's check and
// reaches the API server — turned into one `secrets get` per agent per scrape
// cycle, indefinitely, against the client's QPS=50 budget and one audit entry
// apiece. The reader's own doc comment ("per-scrape-cycle lookups must not
// hammer the API server") was true only on the success path.
//
// Much shorter than secretCacheTTL, deliberately: a cached failure DELAYS
// recovery after the operator fixes the RBAC grant or creates the key, and
// that repair is the moment responsiveness matters most.
const secretFailureTTL = 10 * time.Second

// secretCacheKey identifies one Secret key in the reader's cache.
//
// NUL-separated, not "/"-joined: a Secret namespace, name and key cannot
// contain a NUL, so two distinct triples can never share one cached VALUE. The
// "/" join could — ("a/b","c","d") and ("a","b/c","d") render identically —
// and this cache is what a repeated scrape-auth lookup reads instead of the
// API server. The same ambiguity in the /v1/scrape-auth allowlist was the
// security half of it; this is the caching half.
func secretCacheKey(namespace, name, key string) string {
	return namespace + "\x00" + name + "\x00" + key
}

func (r *k8sSecretReader) Get(ctx context.Context, namespace, name, key string) (string, error) {
	ck := secretCacheKey(namespace, name, key)
	r.mu.Lock()
	if e, ok := r.cache[ck]; ok && time.Since(e.fetched) < e.ttl() {
		r.mu.Unlock()
		return e.value, e.err
	}
	r.evictExpiredLocked()
	r.mu.Unlock()
	sec, err := r.client.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		// Cache the failure — but only when it says something about the
		// CLUSTER. ctx here is the inbound REQUEST's context, so a caller that
		// disconnects mid-flight (an agent restart, a rolling update, a client
		// timeout) produces context.Canceled/DeadlineExceeded that describes
		// that one caller and nothing else. Remembering it would let a single
		// agent going away make its credential unresolvable — a 502 — for
		// every OTHER agent in the fleet for the whole failure TTL.
		if ctx.Err() == nil {
			r.remember(ck, secretCacheEntry{err: err, fetched: time.Now()})
		}
		return "", err
	}
	val, ok := sec.Data[key]
	if !ok {
		// Wrapped, not bare: handleScrapeAuth distinguishes this client-caused
		// miss (404) from a cluster-caused failure like a forbidden read (502,
		// retryable). See server.ErrSecretKeyNotFound.
		err := fmt.Errorf("%w: %q", server.ErrSecretKeyNotFound, key)
		r.remember(ck, secretCacheEntry{err: err, fetched: time.Now()})
		return "", err
	}
	r.remember(ck, secretCacheEntry{value: string(val), fetched: time.Now()})
	return string(val), nil
}

// ttl is how long this entry stays usable: failures are held far more briefly
// than successes, so a fixed RBAC grant or a created key takes effect within
// seconds rather than a minute.
func (e secretCacheEntry) ttl() time.Duration {
	if e.err != nil {
		return secretFailureTTL
	}
	return secretCacheTTL
}

func (r *k8sSecretReader) remember(ck string, e secretCacheEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cache == nil {
		r.cache = map[string]secretCacheEntry{}
	}
	// A failure never displaces a LIVE success. Fetches happen on a miss only,
	// so a usable success can be sitting here just one way: the lock is
	// released across the API call, and two concurrent misses for one ref raced
	// back in the other order. Storing the slower error would serve every agent
	// in the fleet a 502 for the whole failure TTL, for a credential this
	// process has in hand.
	if cur, ok := r.cache[ck]; ok && e.err != nil && cur.err == nil && time.Since(cur.fetched) < cur.ttl() {
		return
	}
	r.cache[ck] = e
}

// evictExpiredLocked drops every entry past the TTL. It runs on the miss path
// only: a hit is the hot path and an expired entry is about to be replaced
// anyway, so the cost lands where an API round-trip is already being paid. The
// map is bounded by the AuthSecretRefs allowlist at any instant, and this is
// what keeps it bounded over TIME as monitors come and go.
func (r *k8sSecretReader) evictExpiredLocked() {
	for k, e := range r.cache {
		if time.Since(e.fetched) >= e.ttl() {
			delete(r.cache, k)
		}
	}
}

// newScrapeAuthTokens opens the shared bearer token guarding /v1/scrape-auth.
//
// Every failure mode is fatal by design: -scrape-auth-secrets turns the service
// into a reader of every Secret key a monitor references, so "no token file",
// "unreadable file" and "empty file" must all stop the process rather than
// quietly leave the endpoint open to the whole cluster. That is exactly
// bearer.NewRotating's contract; only the messages are ours, because they name
// the flag the operator set.
//
// Past startup the two sides re-read on DIFFERENT cadences, and the asymmetry
// is deliberate. This RECEIVER refreshes its accept set on
// bearer.DefaultRefreshInterval (a second, at request time and on
// Rotating.Run's ticker when idle; only a read failure that PERSISTS backs
// off to bearer.DefaultReadInterval, the first being retried a second later),
// because a receiver that has not yet
// read a rotated token has no grace to fall back on — it is a hard 401 for as
// long as its copy lags. The other direction, an agent still presenting the
// PREVIOUS token, is what bearer.DefaultGrace covers: agents re-read the copy
// they present on their own bearer.DefaultReadInterval minute and the
// predecessor stays accepted for five, so nobody has to flip in lockstep with
// the service.
func newScrapeAuthTokens(path string, log *slog.Logger) (*bearer.Rotating, error) {
	rt, err := bearer.NewRotating(path, log)
	switch {
	case errors.Is(err, bearer.ErrNoPath):
		return nil, errors.New("-scrape-auth-secrets requires -scrape-auth-token-file: " +
			"/v1/scrape-auth serves monitor Secret keys and must not be reachable unauthenticated")
	case err != nil:
		return nil, fmt.Errorf("reading -scrape-auth-token-file: %w", err)
	}
	return rt, nil
}
