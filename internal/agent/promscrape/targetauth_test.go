package promscrape

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mapAuth resolves secret refs to fixed values.
type mapAuth struct{ vals map[string]string }

func (m *mapAuth) ScrapeAuth(_ context.Context, ref string) (string, error) {
	return m.vals[ref], nil
}

// kube-prometheus-stack monitors commonly use basicAuth; before this the only
// interpreted credential was a bearer token.
func TestScrapeTargetBasicAuth(t *testing.T) {
	var got atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.Store(r.Header.Get("Authorization"))
		_, _ = w.Write([]byte("m 1\n"))
	}))
	t.Cleanup(srv.Close)

	tgt := testTarget(srv.URL)
	tgt.BasicAuthUser = "ns/creds/user"
	tgt.BasicAuthPass = "ns/creds/pass"

	exp := &captureExporter{}
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets:  staticTargets{tgt},
		Auth:     &mapAuth{vals: map[string]string{"ns/creds/user": "alice", "ns/creds/pass": "s3cret"}},
		Exporter: exp, StartTime: time.Now(),
	})
	s.cycle(context.Background())

	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("alice:s3cret"))
	if h, _ := got.Load().(string); h != want {
		t.Fatalf("Authorization = %q, want %q", h, want)
	}
	if exp.points() != 1 {
		t.Fatalf("points = %d, want 1", exp.points())
	}
}

// `authorization: {type, credentials}` is the modern spelling of a bearer
// token and supports other schemes.
func TestScrapeTargetAuthorizationHeader(t *testing.T) {
	var got atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.Store(r.Header.Get("Authorization"))
		_, _ = w.Write([]byte("m 1\n"))
	}))
	t.Cleanup(srv.Close)

	tgt := testTarget(srv.URL)
	tgt.AuthType = "Token"
	tgt.AuthCredentials = "ns/creds/tok"

	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets:  staticTargets{tgt},
		Auth:     &mapAuth{vals: map[string]string{"ns/creds/tok": "abc123"}},
		Exporter: &captureExporter{}, StartTime: time.Now(),
	})
	s.cycle(context.Background())

	if h, _ := got.Load().(string); h != "Token abc123" {
		t.Fatalf("Authorization = %q, want %q", h, "Token abc123")
	}
}

// A private CA must be usable WITHOUT turning verification off: previously
// insecureSkipVerify was the only interpreted TLS field, so the choice was
// "trust everything" or "cannot scrape".
func TestScrapeTargetPrivateCA(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("m 1\n"))
	}))
	t.Cleanup(srv.Close)

	caPEM := pemOf(srv)
	tgt := testTarget(srv.URL)
	tgt.TLSCA = "ns/tls/ca.crt"
	tgt.TLSServerName = "example.com" // httptest's cert is issued for example.com

	exp := &captureExporter{}
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets:  staticTargets{tgt},
		Auth:     &mapAuth{vals: map[string]string{"ns/tls/ca.crt": caPEM}},
		Exporter: exp, StartTime: time.Now(),
	})
	s.cycle(context.Background())

	if exp.points() != 1 {
		t.Fatalf("points = %d, want 1: the target's own CA was not used to verify it", exp.points())
	}

	// A wrong CA must FAIL rather than silently fall back to skip-verify.
	bad := testTarget(srv.URL)
	bad.TLSCA = "ns/tls/other.crt"
	bad.TLSServerName = "example.com"
	exp2 := &captureExporter{}
	s2 := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets:  staticTargets{bad},
		Auth:     &mapAuth{vals: map[string]string{"ns/tls/other.crt": otherCAPEM}},
		Exporter: exp2, StartTime: time.Now(),
	})
	s2.cycle(context.Background())
	if exp2.points() != 0 {
		t.Fatalf("points = %d, want 0: an untrusted certificate must not be accepted", exp2.points())
	}
}

// Clients are cached by their resolved material, so many targets sharing a CA
// share one transport — and a ROTATED secret yields a new client rather than
// silently reusing the old credentials.
func TestTLSClientCacheKeyedByMaterial(t *testing.T) {
	s := New(Config{Node: "n1", Interval: time.Hour, Timeout: time.Second,
		Auth: &mapAuth{vals: map[string]string{"ns/tls/ca.crt": otherCAPEM}}})
	tgt := testTarget("https://x/metrics")
	tgt.TLSCA = "ns/tls/ca.crt"

	c1, err := s.clientFor(context.Background(), tgt, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := s.clientFor(context.Background(), tgt, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if c1 != c2 {
		t.Fatal("identical TLS material must reuse one client (and its connection pool)")
	}

	// Rotate the secret: a different client must be built.
	s.cfg.Auth = &mapAuth{vals: map[string]string{"ns/tls/ca.crt": rotatedCAPEM}}
	s.authCache = map[string]authCacheEntry{} // bypass the 1-minute token cache
	c3, err := s.clientFor(context.Background(), tgt, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if c3 == c1 {
		t.Fatal("a rotated CA must not keep using the client built from the old one")
	}
}

// Two goroutines missing the cache for one key both build; the insert must
// re-check under the write lock so the second adopts the first's client
// instead of overwriting it — the overwritten client was already serving the
// winner's scrape, and nothing would ever close its pooled connections.
func TestTLSClientCacheConcurrentBuildYieldsOneClient(t *testing.T) {
	s := New(Config{Node: "n1", Interval: time.Hour, Timeout: time.Second,
		Auth: &mapAuth{vals: map[string]string{"ns/tls/ca.crt": otherCAPEM}}})
	tgt := testTarget("https://x/metrics")
	tgt.TLSCA = "ns/tls/ca.crt"

	const goroutines = 8
	clients := make([]*http.Client, goroutines)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			c, err := s.clientFor(context.Background(), tgt, time.Second)
			if err != nil {
				t.Error(err)
				return
			}
			clients[i] = c
		}(i)
	}
	close(start)
	wg.Wait()
	for i := 1; i < goroutines; i++ {
		if clients[i] != clients[0] {
			t.Fatal("concurrent builds for one key returned distinct clients: the losing insert overwrote the one in use")
		}
	}
	if len(s.tlsClients) != 1 {
		t.Fatalf("cache holds %d entries for one key, want 1", len(s.tlsClients))
	}
}

// A target with no TLS material keeps using the shared clients.
func TestNoTLSMaterialUsesSharedClients(t *testing.T) {
	s := New(Config{Node: "n1", Interval: time.Hour, Timeout: time.Second})
	plain := testTarget("http://x/metrics")
	if c, _ := s.clientFor(context.Background(), plain, time.Second); c != s.http {
		t.Error("a plain target must use the shared default client")
	}
	skip := testTarget("https://x/metrics")
	skip.InsecureSkipVerify = true
	if c, _ := s.clientFor(context.Background(), skip, time.Second); c != s.insecureHTTP {
		t.Error("insecureSkipVerify alone must use the shared skip-verify client")
	}
}

// pemOf renders a test server's certificate as PEM, for use as a target CA.
func pemOf(srv *httptest.Server) string {
	return string(pem.EncodeToMemory(&pem.Block{
		Type: "CERTIFICATE", Bytes: srv.Certificate().Raw,
	}))
}

// A syntactically valid CA that signed nothing here, for the negative cases.
var otherCAPEM, rotatedCAPEM = mustSelfSigned("other"), mustSelfSigned("rotated")

func mustSelfSigned(cn string) string {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}
