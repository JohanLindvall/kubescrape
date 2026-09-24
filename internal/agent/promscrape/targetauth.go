package promscrape

// Per-target authentication and TLS beyond a bearer token.
//
// kube-prometheus-stack's own control-plane monitors (etcd, kube-scheduler,
// kube-controller-manager) authenticate with CLIENT CERTIFICATES, mesh-fronted
// targets need mTLS, and anything behind a private CA was previously scrapeable
// only by turning verification off entirely (tlsConfig.insecureSkipVerify was
// the sole TLS field interpreted). All of it arrives as "namespace/name/key"
// secret references resolved through the same /v1/scrape-auth channel as the
// bearer token, so it is served only when the metadata service runs
// -scrape-auth-secrets.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/metaclient"
)

// applyAuth sets the request's Authorization header from the target's
// bearerTokenSecret, authorization or basicAuth (in that order of precedence —
// prometheus-operator rejects combining them, and preferring the most specific
// keeps a partially-migrated CR working).
func (s *Scraper) applyAuth(ctx context.Context, req *http.Request, t kubemeta.ScrapeTarget) error {
	switch {
	case t.AuthSecret != "":
		token, err := s.authToken(ctx, t.AuthSecret)
		if err != nil {
			return fmt.Errorf("scrape auth %s: %w", t.AuthSecret, err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
	case t.AuthCredentials != "":
		cred, err := s.authToken(ctx, t.AuthCredentials)
		if err != nil {
			return fmt.Errorf("scrape auth %s: %w", t.AuthCredentials, err)
		}
		typ := t.AuthType
		if typ == "" {
			typ = "Bearer" // prometheus-operator's default
		}
		req.Header.Set("Authorization", typ+" "+cred)
	case t.BasicAuthUser != "" || t.BasicAuthPass != "":
		var user, pass string
		var err error
		if t.BasicAuthUser != "" {
			if user, err = s.authToken(ctx, t.BasicAuthUser); err != nil {
				return fmt.Errorf("scrape basic-auth user %s: %w", t.BasicAuthUser, err)
			}
		}
		if t.BasicAuthPass != "" {
			if pass, err = s.authToken(ctx, t.BasicAuthPass); err != nil {
				return fmt.Errorf("scrape basic-auth password %s: %w", t.BasicAuthPass, err)
			}
		}
		req.Header.Set("Authorization", "Basic "+
			base64.StdEncoding.EncodeToString([]byte(user+":"+pass)))
	}
	return nil
}

// authCacheTTL is how long a resolved secret is served without asking the
// metadata service again (tokens rotate; per-cycle lookups must not hammer the
// service).
const authCacheTTL = time.Minute

// authIdleRelease is how long an entry nothing asks for survives — the hygiene
// bound on secret material whose monitor was deleted or whose pod left the
// node. It is deliberately NOT authCacheTTL: an entry is also authToken's
// fallback, and a ref asked for once a minute (a monitor's `interval: 1m`, the
// commonest explicit cadence) is asked again ~60s after its last use, give or
// take the tick's jitter and dueNow's early slack — so a one-minute release
// raced that target's own next scrape and threw its fallback away about half
// the time. Twice the TTL covers every target scraped at least once a minute; a
// slower one re-resolves on each scrape with no fallback, as all of them did
// before the grace existed.
const authIdleRelease = 2 * authCacheTTL

// authStaleGrace bounds how long a value past authCacheTTL may still be SERVED
// when its refresh fails without a verdict (see authToken). It exists because
// the metadata service reads secrets with a direct API-server GET behind its
// own one-minute cache: during an API-server outage — or a metadata-service
// rollout — every secret-ref-credentialed target otherwise failed with
// reason=auth within a minute or two, although the credential it already held
// was still valid. The same rule internal/bearer applies to a file-backed
// token: a FAILED re-read keeps the last good value. Ten minutes is past any
// ordinary rollout while still bounding how long revoked-by-outage material can
// be presented; a DEFINITIVE refusal drops the value at once.
const authStaleGrace = 10 * time.Minute

// authStaleWarnEvery throttles the retained-credential warning.
const authStaleWarnEvery = 5 * time.Minute

type authCacheEntry struct {
	token string
	// fetched is when the metadata service last SERVED the value, used when a
	// scrape last ASKED for it. The two bound different things: fetched decides
	// freshness and the stale-serving grace, used decides hygiene (a ref
	// nobody asks for any more is released after authIdleRelease).
	fetched, used time.Time
}

// authToken resolves a monitor endpoint's bearer token (or basicAuth half, or
// TLS material) via the metadata service, cached for authCacheTTL.
//
// A value past its TTL is re-fetched, and when that refresh fails WITHOUT a
// verdict — a transport error, a timeout, a 5xx (the service's `upstream`, i.e.
// the API server could not be asked) — the retained value is served for up to
// authStaleGrace, with a throttled warning. A refresh the service ANSWERS with a
// refusal (401/403/404, any status below 500: the secret was deleted, the
// monitor no longer references it, this agent's token was refused) drops the
// value and fails as before, so revocation takes effect immediately rather than
// after the grace.
func (s *Scraper) authToken(ctx context.Context, ref string) (string, error) {
	now := time.Now()
	s.authMu.Lock()
	s.sweepAuthCacheLocked(now)
	retained, have := s.authCache[ref]
	if have {
		retained.used = now
		s.authCache[ref] = retained
		if now.Sub(retained.fetched) < authCacheTTL {
			s.authMu.Unlock()
			return retained.token, nil
		}
	}
	s.authMu.Unlock()
	if s.cfg.Auth == nil {
		return "", errors.New("no auth source configured")
	}
	fetchCtx := ctx
	if have {
		// With a value to fall back on, the refresh may not spend the whole
		// scrape: a HUNG metadata service would otherwise hold it to the scrape
		// deadline, and the retained value would then be served into a request
		// that has no time left to use it.
		if dl, ok := ctx.Deadline(); ok {
			var cancel context.CancelFunc
			fetchCtx, cancel = context.WithTimeout(ctx, time.Until(dl)/2)
			defer cancel()
		}
	}
	token, err := s.cfg.Auth.ScrapeAuth(fetchCtx, ref)
	if err != nil {
		if !have || ctx.Err() != nil {
			// Nothing to fall back on — or the SCRAPE's own context is gone
			// (shutdown, its budget spent), in which case the retained value
			// could not be used either and the warning below would blame the
			// metadata service for a cancellation.
			return "", err
		}
		if definitiveAuthRefusal(err) {
			s.authMu.Lock()
			delete(s.authCache, ref)
			s.authMu.Unlock()
			return "", err
		}
		if s.authStaleWarn.Allow(authStaleWarnEvery) {
			s.log.Warn("could not refresh a scrape credential; serving the value last resolved until the metadata service answers",
				"key", ref, "error", err, "outage", time.Since(retained.fetched).Round(time.Second), "grace", authStaleGrace)
		}
		return retained.token, nil
	}
	s.authMu.Lock()
	now = time.Now()
	s.sweepAuthCacheLocked(now)
	// The cap is the second bound, because expiry alone bounds nothing under
	// churn: a monitor edited in a loop mints a fresh ref each time, and every
	// entry inside one TTL window survives the sweep. An arbitrary victim costs
	// the next scrape of that ref one metadata request.
	for k := range s.authCache {
		if len(s.authCache) < maxAuthCacheEntries {
			break
		}
		delete(s.authCache, k)
	}
	s.authCache[ref] = authCacheEntry{token: token, fetched: now, used: now}
	s.authMu.Unlock()
	return token, nil
}

// definitiveAuthRefusal reports whether a failed secret lookup is the metadata
// service's VERDICT (a status below 500 — the secret or key is gone, no
// monitor references it any more, this agent's token was refused) as opposed
// to its absence (a transport failure, a timeout, a 5xx). Only the absence may
// be bridged with a retained value.
func definitiveAuthRefusal(err error) bool {
	var se *metaclient.StatusError
	return errors.As(err, &se) && se.Code < 500
}

// maxAuthCacheEntries bounds the resolved-secret cache. One monitor endpoint
// can name up to six refs (bearer, basicAuth user and password, authorization
// credentials, CA, cert, key), so this is a few dozen monitored targets' worth
// on one node — far above any real node, and a hard bound against ref churn.
const maxAuthCacheEntries = 256

// sweepAuthCacheLocked drops entries nothing has asked for in authIdleRelease,
// and entries fetched longer ago than authStaleGrace however busy. Caller holds
// authMu.
//
// It is called on EVERY path into the cache — the hit, the insert, and the
// scrape cycle — because these entries hold bearer tokens, CA bundles, client
// certificates and client PRIVATE KEYS, and a sweep that ran only inside a
// cache-miss insert (as this one did) never reaches the entry that matters: a
// ref that stops being fetched is exactly the one whose monitor was deleted or
// whose pod moved off this node, and if EVERY ref goes away the sweep stops
// running at all, leaving the whole map resident for the process lifetime —
// the opposite of what the comment here promised. The material must not
// outlive its use: an entry is retained past its freshness only while scrapes
// keep asking for it, and then only as authToken's fallback.
func (s *Scraper) sweepAuthCacheLocked(now time.Time) {
	for k, e := range s.authCache {
		if now.Sub(e.used) >= authIdleRelease || now.Sub(e.fetched) >= authStaleGrace {
			delete(s.authCache, k)
		}
	}
}

// sweepAuthCache releases aged-out secret material even when nothing asks for
// a token any more — the cycle always runs, an authToken call may never come
// again.
func (s *Scraper) sweepAuthCache() {
	s.authMu.Lock()
	s.sweepAuthCacheLocked(time.Now())
	s.authMu.Unlock()
}

// needsTLSClient reports whether the target needs its own transport rather than
// the shared default or skip-verify clients.
func needsTLSClient(t kubemeta.ScrapeTarget) bool {
	return t.TLSCA != "" || t.TLSCert != "" || t.TLSKey != "" || t.TLSServerName != ""
}

// noRedirect refuses to follow redirects on a scrape.
//
// Credentials and client certificates are attached per request; Go's stdlib
// strips the Authorization header only across a HOSTNAME change, so a same-host
// https->http redirect would put a bearer token on the wire in cleartext, and a
// per-target client presents its CLIENT CERTIFICATE to whatever host a redirect
// names. A metrics endpoint has no legitimate reason to redirect. The OTLP
// exporter already refuses redirects for the same reason.
func noRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

// scrapeMaxIdlePerHost is the idle-connection pool per host of the discovered
// targets' clients: a target is one host scraped once per cycle, so a small
// pool is all reuse needs.
const scrapeMaxIdlePerHost = 2

// newScrapeClient builds a scrape HTTP client. Every one of them — the shared
// default, the shared skip-verify one, a target's own TLS client and the
// kubelet's — goes through here, so none can be the one that follows a
// redirect (noRedirect) or keeps idle connections past 90s.
//
// No client Timeout: the per-request context carries the effective (possibly
// per-target) budget, and it is set before the client is even chosen, so a
// client Timeout — which starts only at Do — could never fire first, while a
// fixed one silently capped every target that asked for longer. The kubelet's
// client sets one anyway, as a backstop (newKubeletHTTPClient).
func newScrapeClient(tlsCfg *tls.Config, maxIdlePerHost int) *http.Client {
	return &http.Client{
		CheckRedirect: noRedirect,
		Transport: &http.Transport{
			TLSClientConfig:     tlsCfg,
			MaxIdleConnsPerHost: maxIdlePerHost,
			IdleConnTimeout:     90 * time.Second,
		},
	}
}

// clientFor returns the HTTP client for a target: the shared default, the
// shared skip-verify one, or a per-target client built from its CA, client
// certificate and serverName. Built clients are cached by their material, so a
// hundred targets sharing one CA share one transport (and its connection pool);
// the cache is refreshed when the underlying secrets change, since the key
// includes the resolved bytes.
func (s *Scraper) clientFor(ctx context.Context, t kubemeta.ScrapeTarget) (*http.Client, error) {
	if !needsTLSClient(t) {
		if t.InsecureSkipVerify {
			return s.insecureHTTP, nil
		}
		return s.http, nil
	}

	// A ref that will not RESOLVE is reason=auth, exactly as a bearer or
	// basicAuth ref is (applyAuth's caller): the scrape never left this agent,
	// and the remedy is -scrape-auth-secrets and the shared token, not the
	// target's certificate. classify keeps the innermost reason, so the
	// caller's outer reason=tls still labels material that resolved but is
	// unusable (tlsConfigFromPEM: an empty CA, certificate or key, a PEM
	// holding no certificate, a key that does not match) — whose note is the
	// right one for it.
	var caPEM, certPEM, keyPEM string
	var err error
	if t.TLSCA != "" {
		if caPEM, err = s.authToken(ctx, t.TLSCA); err != nil {
			return nil, classify(reasonAuth, fmt.Errorf("scrape tls ca %s: %w", t.TLSCA, err))
		}
	}
	if t.TLSCert != "" {
		if certPEM, err = s.authToken(ctx, t.TLSCert); err != nil {
			return nil, classify(reasonAuth, fmt.Errorf("scrape tls cert %s: %w", t.TLSCert, err))
		}
	}
	if t.TLSKey != "" {
		if keyPEM, err = s.authToken(ctx, t.TLSKey); err != nil {
			return nil, classify(reasonAuth, fmt.Errorf("scrape tls key %s: %w", t.TLSKey, err))
		}
	}
	// Keyed by the resolved material, so a rotated secret yields a new client
	// rather than silently reusing the old credentials. Length-prefixed, so two
	// configurations cannot serialise to one key.
	//
	// Deliberately NOT keyed by the target's timeout, and the client carries
	// none (newScrapeClient): the scrape context's deadline is set before the
	// client is even chosen, so a client Timeout could never fire first — it
	// only split the cache, giving a 10s monitor and a default-15s one sharing a
	// CA two transports, two pools and two handshakes, and two of maxTLSClients.
	key := lp(t.TLSServerName) + lp(caPEM) + lp(certPEM) + lp(keyPEM) +
		lp(strconv.FormatBool(t.InsecureSkipVerify))

	s.tlsMu.Lock()
	if e, ok := s.tlsClients[key]; ok {
		e.used = time.Now()
		s.tlsClients[key] = e
		s.tlsMu.Unlock()
		return e.client, nil
	}
	s.tlsMu.Unlock()

	cfg, err := tlsConfigFromPEM(t, caPEM, certPEM, keyPEM)
	if err != nil {
		return nil, err
	}
	client := newScrapeClient(cfg, scrapeMaxIdlePerHost)

	s.tlsMu.Lock()
	// Re-check under the write lock: two scrape goroutines missing the cache
	// for one key both build. Without the re-check the second insert
	// OVERWROTE the first — whose transport the winner's scrape was already
	// using, and whose pooled connections nothing would ever close. The loser
	// built here adopts the cached client instead; it has served no request,
	// so there is nothing of its own to close.
	if e, ok := s.tlsClients[key]; ok {
		e.used = time.Now()
		s.tlsClients[key] = e
		s.tlsMu.Unlock()
		return e.client, nil
	}
	now := time.Now()
	// Retire what nothing has used for a while, BEFORE the cap is consulted
	// (the cycle's sweepTLSClients does the same on every cycle, which is what
	// reaches a superseded entry when no NEW material ever arrives).
	retired := s.retireIdleTLSClientsLocked(now)
	// Bound the cache: the key includes the secret bytes, so a rotating
	// credential would otherwise accumulate a transport per rotation faster than
	// the TTL retires them. Evict ONE entry (and close its idle connections)
	// rather than clearing the map: a steady population above the cap would
	// otherwise rebuild every transport each cycle, paying a fresh TCP+TLS
	// handshake per target while the orphans held their pooled connections open
	// for the full idle timeout.
	capped := false
	if len(s.tlsClients) >= maxTLSClients {
		for k, victim := range s.tlsClients {
			retired = append(retired, victim.client)
			delete(s.tlsClients, k)
			capped = true
			break
		}
	}
	s.tlsClients[key] = tlsClientEntry{client: client, used: now}
	s.tlsMu.Unlock()
	// Done with the lock DROPPED: closing idle connections walks a victim's
	// whole connection pool and the warn renders and writes a slog record, and
	// every scrape goroutine on the node contends for tlsMu.
	closeRetiredTLSClients(retired)
	if capped {
		// The eviction is correct and the scrape still works, so this is a Warn
		// about COST rather than about loss: past the cap every cycle pays a
		// fresh TCP+TLS handshake for the targets that keep missing. The key
		// includes the resolved secret bytes, so the realistic cause is
		// credentials rotating faster than the cache holds them. The TTL sweep
		// above is ordinary hygiene and deliberately says nothing.
		s.warnCacheEviction(&s.tlsEvictWarn, "per-target TLS clients", maxTLSClients,
			"more than the cache holds are in use: targets are rotating their CA or client certificate, or too many distinct tlsConfigs are in play")
	}
	return client, nil
}

// retireIdleTLSClientsLocked removes the clients no scrape has selected in
// tlsClientIdleTTL and returns them for closeRetiredTLSClients, which the
// caller runs AFTER dropping tlsMu. Caller holds tlsMu.
//
// Retired by LAST USE and not by age: a client scraped every cycle must not be
// rebuilt on a timer, while one nothing selects any more is exactly the
// rotated-away credential to release. The key includes the resolved PEM, so
// every cert-manager rotation mints a new entry and leaves the old one — which
// holds the PREVIOUS client PRIVATE KEY — untouched; so does a TLS target that
// leaves the node.
func (s *Scraper) retireIdleTLSClientsLocked(now time.Time) []*http.Client {
	var retired []*http.Client
	for k, e := range s.tlsClients {
		if now.Sub(e.used) >= tlsClientIdleTTL {
			retired = append(retired, e.client)
			delete(s.tlsClients, k)
		}
	}
	return retired
}

// sweepTLSClients retires idle per-target clients on the cycle's cadence.
//
// The retirement used to run only inside clientFor's post-miss insert, which is
// the one path that does NOT follow the ordinary rotation: after v1 → v2 every
// later cycle hits v2, nothing new is inserted, and v1 — its parsed private key
// and its Transport — stayed resident until some other distinct material
// arrived, typically the next rotation weeks later (or never, for a target that
// left the node). The same trap sweepAuthCacheLocked's comment describes for
// the resolved-secret cache. At most maxTLSClients entries, once per cycle.
func (s *Scraper) sweepTLSClients() {
	s.tlsMu.Lock()
	retired := s.retireIdleTLSClientsLocked(time.Now())
	s.tlsMu.Unlock()
	closeRetiredTLSClients(retired)
}

// closeRetiredTLSClients closes the idle connections of clients already removed
// from the cache. Called with tlsMu DROPPED: it walks each victim's whole
// connection pool. A victim is unreachable from the map by now, so nothing can
// adopt it while this works on it — a scrape already holding it keeps its live
// connections either way (CloseIdleConnections closes only idle ones).
func closeRetiredTLSClients(retired []*http.Client) {
	for _, victim := range retired {
		if tr, ok := victim.Transport.(*http.Transport); ok {
			tr.CloseIdleConnections()
		}
	}
}

// maxTLSClients bounds the per-target transport cache.
const maxTLSClients = 64

// tlsClientEntry is a cached per-target client and when a scrape last selected
// it. The stamp is what lets a SUPERSEDED entry — whose *http.Client holds the
// previous client certificate's private key — be released without waiting for
// the size cap.
type tlsClientEntry struct {
	client *http.Client
	used   time.Time
}

// tlsClientIdleTTL is how long an unselected client is kept. Comfortably above
// the transport's own 90s IdleConnTimeout, past which the cached client's
// pooled connections are gone anyway and all it still buys is the tls.X509KeyPair
// parse — so retiring it costs a rarely-scraped target one handshake it was
// going to pay regardless, and buys the release of retired key material.
const tlsClientIdleTTL = 5 * time.Minute

// tlsConfigFromPEM builds a target's TLS config from its resolved secret
// material. The target's TLSCA/TLSCert/TLSKey are the "ns/name/key" references
// (empty = none asked for), used both in errors and to detect the
// empty-resolution case: the metadata service answers a present-but-empty key
// as a 200 with an empty value, so an empty PEM behind a set ref is material
// that resolved and is unusable, never "none".
//
// It is the same PEM→config pattern as otlpexport's client TLS but not the same
// contract, which is why it is not shared: that one reads FILE PATHS for the
// agent's own collector, this one works from material already resolved through
// /v1/scrape-auth, names the monitor's REFS in its errors and refuses an empty
// resolution. Sharing the dozen lines would couple the scraper to the
// exporter's API for no common behaviour.
func tlsConfigFromPEM(t kubemeta.ScrapeTarget, caPEM, certPEM, keyPEM string) (*tls.Config, error) {
	cfg := &tls.Config{
		InsecureSkipVerify: t.InsecureSkipVerify, //nolint:gosec // explicit per-endpoint opt-in
		ServerName:         t.TLSServerName,
	}
	// A client certificate is a PAIR. A tlsConfig naming `cert` without
	// `keySecret` (or the reverse) is a CR mistake, and handing the lone half
	// to X509KeyPair failed with "tls: failed to find any PEM data in key
	// input", which names neither the field that is missing nor the monitor's
	// ref — otlpexport refuses the same shape up front for its own collector.
	if (t.TLSCert == "") != (t.TLSKey == "") {
		have, missing, ref := "cert", "keySecret", t.TLSCert
		if t.TLSCert == "" {
			have, missing, ref = "keySecret", "cert", t.TLSKey
		}
		return nil, fmt.Errorf("scrape tls: tlsConfig.%s %s has no tlsConfig.%s; a client certificate needs both", have, ref, missing)
	}
	if t.TLSCA != "" && caPEM == "" {
		// A resolvable-but-EMPTY ca.crt (mid-rotation, or a secret an init
		// container has not populated yet) must not silently degrade to the
		// system trust store: the endpoint asked to be pinned to a private CA.
		return nil, fmt.Errorf("scrape tls ca %s: empty", t.TLSCA)
	}
	// The client-certificate counterpart of the guard above. With BOTH refs
	// resolving empty, the certificate branch below was skipped altogether and
	// the target was contacted with NO client certificate and no error: against
	// a server requiring one the refusal read as a handshake failure with no
	// hint that the material was missing, and against optional client auth the
	// scrape SUCCEEDED under an anonymous identity the monitor never asked for.
	// One empty half with the other present already failed in X509KeyPair, but
	// with a parse error naming neither ref.
	if t.TLSCert != "" && certPEM == "" {
		return nil, fmt.Errorf("scrape tls cert %s: empty", t.TLSCert)
	}
	if t.TLSKey != "" && keyPEM == "" {
		return nil, fmt.Errorf("scrape tls key %s: empty", t.TLSKey)
	}
	if caPEM != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(caPEM)) {
			return nil, fmt.Errorf("scrape tls ca %s: no certificates found", t.TLSCA)
		}
		cfg.RootCAs = pool
	}
	if certPEM != "" || keyPEM != "" {
		pair, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
		if err != nil {
			return nil, fmt.Errorf("scrape tls client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return cfg, nil
}

// lp length-prefixes a key component so two different configurations cannot
// serialise to the same cache key — the string form of appendLP (the package's
// one injective-join rule; see cadvisorbatch.go).
func lp(s string) string { return string(appendLP(nil, s)) }
