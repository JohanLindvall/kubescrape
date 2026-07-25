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
	return t.TLSCA != "" || t.TLSCert != "" || t.TLSServerName != ""
}

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
	key := t.TLSServerName + "\x00" + caPEM + "\x00" + certPEM + "\x00" + keyPEM +
		"\x00" + fmt.Sprint(t.InsecureSkipVerify)

	s.tlsMu.Lock()
	if c, ok := s.tlsClients[key]; ok {
		s.tlsMu.Unlock()
		return c, nil
	}
	s.tlsMu.Unlock()

	cfg := &tls.Config{
		InsecureSkipVerify: t.InsecureSkipVerify, //nolint:gosec // explicit per-endpoint opt-in
		ServerName:         t.TLSServerName,
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
	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			TLSClientConfig:     cfg,
			MaxIdleConnsPerHost: 2,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	s.tlsMu.Lock()
	// Bound the cache: the key includes the secret bytes, so a rotating
	// credential would otherwise accumulate a transport per rotation.
	if len(s.tlsClients) >= maxTLSClients {
		s.tlsClients = map[string]*http.Client{}
	}
	s.tlsClients[key] = client
	s.tlsMu.Unlock()
	return client, nil
}

// maxTLSClients bounds the per-target transport cache.
const maxTLSClients = 64
