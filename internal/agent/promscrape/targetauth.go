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
	"fmt"
	"net/http"
	"time"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
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

// clientFor returns the HTTP client for a target: the shared default, the
// shared skip-verify one, or a per-target client built from its CA, client
// certificate and serverName. Built clients are cached by their material, so a
// hundred targets sharing one CA share one transport (and its connection pool);
// the cache is refreshed when the underlying secrets change, since the key
// includes the resolved bytes.
func (s *Scraper) clientFor(ctx context.Context, t kubemeta.ScrapeTarget, timeout time.Duration) (*http.Client, error) {
	if !needsTLSClient(t) {
		if t.InsecureSkipVerify {
			return s.insecureHTTP, nil
		}
		return s.http, nil
	}

	var caPEM, certPEM, keyPEM string
	var err error
	if t.TLSCA != "" {
		if caPEM, err = s.authToken(ctx, t.TLSCA); err != nil {
			return nil, fmt.Errorf("scrape tls ca %s: %w", t.TLSCA, err)
		}
	}
	if t.TLSCert != "" {
		if certPEM, err = s.authToken(ctx, t.TLSCert); err != nil {
			return nil, fmt.Errorf("scrape tls cert %s: %w", t.TLSCert, err)
		}
	}
	if t.TLSKey != "" {
		if keyPEM, err = s.authToken(ctx, t.TLSKey); err != nil {
			return nil, fmt.Errorf("scrape tls key %s: %w", t.TLSKey, err)
		}
	}
	// Keyed by the resolved material, so a rotated secret yields a new client
	// rather than silently reusing the old credentials.
	// Length-prefixed, and including the timeout: the client bakes the timeout
	// in, so omitting it made the effective timeout depend on which target
	// happened to build the cached client first.
	key := lp(t.TLSServerName) + lp(caPEM) + lp(certPEM) + lp(keyPEM) +
		lp(fmt.Sprint(t.InsecureSkipVerify)) + lp(timeout.String())

	s.tlsMu.Lock()
	if c, ok := s.tlsClients[key]; ok {
		s.tlsMu.Unlock()
		return c, nil
	}
	s.tlsMu.Unlock()

	cfg, err := tlsConfigFromPEM(t.TLSServerName, t.InsecureSkipVerify, t.TLSCA, caPEM, certPEM, keyPEM)
	if err != nil {
		return nil, err
	}
	client := &http.Client{
		Timeout:       timeout,
		CheckRedirect: noRedirect,
		Transport: &http.Transport{
			TLSClientConfig:     cfg,
			MaxIdleConnsPerHost: 2,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	s.tlsMu.Lock()
	// Re-check under the write lock: two scrape goroutines missing the cache
	// for one key both build. Without the re-check the second insert
	// OVERWROTE the first — whose transport the winner's scrape was already
	// using, and whose pooled connections nothing would ever close. The loser
	// built here adopts the cached client instead; it has served no request,
	// so there is nothing of its own to close.
	if c, ok := s.tlsClients[key]; ok {
		s.tlsMu.Unlock()
		return c, nil
	}
	// Bound the cache: the key includes the secret bytes, so a rotating
	// credential would otherwise accumulate a transport per rotation. Evict ONE
	// entry (and close its idle connections) rather than clearing the map: a
	// steady population above the cap would otherwise rebuild every transport
	// each cycle, paying a fresh TCP+TLS handshake per target while the orphans
	// held their pooled connections open for the full idle timeout.
	if len(s.tlsClients) >= maxTLSClients {
		for k, victim := range s.tlsClients {
			if tr, ok := victim.Transport.(*http.Transport); ok {
				tr.CloseIdleConnections()
			}
			delete(s.tlsClients, k)
			break
		}
	}
	s.tlsClients[key] = client
	s.tlsMu.Unlock()
	return client, nil
}

// maxTLSClients bounds the per-target transport cache.
const maxTLSClients = 64

// tlsConfigFromPEM builds a target's TLS config from its resolved secret
// material; caRef is the CA's "ns/name/key" reference (empty = none asked
// for), used both in errors and to detect the empty-resolution case. The same
// PEM→config pattern as otlpexport's client TLS, kept here because pkg-boundary
// direction forbids the import.
func tlsConfigFromPEM(serverName string, insecureSkipVerify bool, caRef, caPEM, certPEM, keyPEM string) (*tls.Config, error) {
	cfg := &tls.Config{
		InsecureSkipVerify: insecureSkipVerify, //nolint:gosec // explicit per-endpoint opt-in
		ServerName:         serverName,
	}
	if caRef != "" && caPEM == "" {
		// A resolvable-but-EMPTY ca.crt (mid-rotation, or a secret an init
		// container has not populated yet) must not silently degrade to the
		// system trust store: the endpoint asked to be pinned to a private CA.
		return nil, fmt.Errorf("scrape tls ca %s: empty", caRef)
	}
	if caPEM != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(caPEM)) {
			return nil, fmt.Errorf("scrape tls ca %s: no certificates found", caRef)
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
