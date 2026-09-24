package promscrape

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
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

	c1, err := s.clientFor(context.Background(), tgt)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := s.clientFor(context.Background(), tgt)
	if err != nil {
		t.Fatal(err)
	}
	if c1 != c2 {
		t.Fatal("identical TLS material must reuse one client (and its connection pool)")
	}

	// Rotate the secret: a different client must be built.
	s.cfg.Auth = &mapAuth{vals: map[string]string{"ns/tls/ca.crt": rotatedCAPEM}}
	s.authCache = map[string]authCacheEntry{} // bypass the 1-minute token cache
	c3, err := s.clientFor(context.Background(), tgt)
	if err != nil {
		t.Fatal(err)
	}
	if c3 == c1 {
		t.Fatal("a rotated CA must not keep using the client built from the old one")
	}
}

// A target's timeout travels on its scrape CONTEXT, whose deadline is set
// before the client is even chosen — so it must not key the per-target TLS
// client. It used to: the client baked a Timeout in (one that could never fire
// first) and the cache key carried it, so two monitors sharing TLS material but
// asking for different scrapeTimeouts got two transports, two connection pools
// and two handshakes, and spent two slots of maxTLSClients.
func TestTLSClientIsSharedAcrossTargetTimeouts(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("m 1\n"))
	}))
	t.Cleanup(srv.Close)

	var targets staticTargets
	for _, c := range []struct{ monitor, timeout string }{{"ns/fast", "2s"}, {"ns/slow", "3s"}} {
		tgt := testTarget(srv.URL)
		tgt.Source, tgt.Monitor, tgt.ScrapeTimeout = "servicemonitor", c.monitor, c.timeout
		tgt.TLSCA = "ns/tls/ca.crt"
		tgt.TLSServerName = "example.com" // httptest's cert is issued for example.com
		targets = append(targets, tgt)
	}
	exp := &captureExporter{}
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets:  targets,
		Auth:     &mapAuth{vals: map[string]string{"ns/tls/ca.crt": pemOf(srv)}},
		Exporter: exp, StartTime: time.Now(),
	})
	s.cycle(context.Background())

	if exp.points() != 2 {
		t.Fatalf("points = %d, want 2: both targets must scrape", exp.points())
	}
	s.tlsMu.Lock()
	defer s.tlsMu.Unlock()
	if len(s.tlsClients) != 1 {
		t.Fatalf("%d per-target TLS clients for ONE set of TLS material, want 1: the effective timeout is splitting the cache", len(s.tlsClients))
	}
	for _, e := range s.tlsClients {
		if e.client.Timeout != 0 {
			t.Errorf("the per-target client carries Timeout=%v; the scrape context is the budget", e.client.Timeout)
		}
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
	for i := range goroutines {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			c, err := s.clientFor(context.Background(), tgt)
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
	if c, _ := s.clientFor(context.Background(), plain); c != s.http {
		t.Error("a plain target must use the shared default client")
	}
	skip := testTarget("https://x/metrics")
	skip.InsecureSkipVerify = true
	if c, _ := s.clientFor(context.Background(), skip); c != s.insecureHTTP {
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

// errAuth fails every secret lookup, as a metadata service without
// -scrape-auth-secrets (or an agent without the matching token) does.
type errAuth struct{ err error }

func (e errAuth) ScrapeAuth(context.Context, string) (string, error) { return "", e.err }

// A tlsConfig secret ref that will not RESOLVE is the same failure as a bearer
// or basicAuth ref that will not: the scrape never left this agent, and the
// remedy is -scrape-auth-secrets and the shared token. It was counted
// reason=tls, whose note steers the operator toward insecureSkipVerify — which
// cannot help, since the ref is still required.
func TestUnresolvableTLSRefIsAnAuthFailure(t *testing.T) {
	for _, field := range []string{"ca", "cert", "key"} {
		t.Run(field, func(t *testing.T) {
			tgt := testTarget("https://127.0.0.1:1/metrics")
			switch field {
			case "ca":
				tgt.TLSCA = "ns/tls/ca.crt"
			case "cert":
				tgt.TLSCert = "ns/tls/tls.crt"
			case "key":
				tgt.TLSKey = "ns/tls/tls.key"
			}
			s := New(Config{
				Node: "n1", Interval: time.Hour, Timeout: time.Second,
				Auth: errAuth{errors.New("metadata service returned 404")},
			})
			_, err := s.scrapeTarget(context.Background(), tgt, time.Second)
			if got := failureReason(err); got != reasonAuth {
				t.Errorf("reason = %q (%v), want %q", got, err, reasonAuth)
			}
		})
	}
	// Material that RESOLVED but is unusable is still the TLS failure it was:
	// an empty CA must not degrade to the system trust store, and its note is
	// the right one.
	tgt := testTarget("https://127.0.0.1:1/metrics")
	tgt.TLSCA = "ns/tls/ca.crt"
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: time.Second,
		Auth: &mapAuth{vals: map[string]string{"ns/tls/ca.crt": ""}},
	})
	if _, err := s.scrapeTarget(context.Background(), tgt, time.Second); failureReason(err) != reasonTLS {
		t.Errorf("an empty CA: reason = %q (%v), want %q", failureReason(err), err, reasonTLS)
	}
}

// A target refusing this agent's CLIENT CERTIFICATE does it with a TLS alert,
// which crypto/tls surfaces as &net.OpError{Op: "remote error"} around an
// unexported alert type. It matched none of the TLS types and fell into the
// generic *net.OpError arm — reason=connect, no note — pointing the operator
// at networking for the main per-target mTLS failure.
func TestClientCertificateRefusalIsATLSFailure(t *testing.T) {
	if got := failureReason(fmt.Errorf("get: %w", &net.OpError{Op: "remote error", Err: errors.New("tls: certificate required")})); got != reasonTLS {
		t.Errorf("a remote TLS alert: reason = %q, want %q", got, reasonTLS)
	}

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("m 1\n"))
	}))
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAnyClientCert}
	srv.Config.ErrorLog = log.New(io.Discard, "", 0) // the refused handshake is the point
	srv.StartTLS()
	t.Cleanup(srv.Close)

	tgt := testTarget(srv.URL)
	tgt.TLSCA = "ns/tls/ca.crt"
	tgt.TLSServerName = "example.com" // httptest's cert is issued for example.com
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		Auth: &mapAuth{vals: map[string]string{"ns/tls/ca.crt": pemOf(srv)}},
	})
	_, err := s.scrapeTarget(context.Background(), tgt, 5*time.Second)
	if err == nil {
		t.Fatal("a target requiring a client certificate accepted a scrape presenting none")
	}
	if got := failureReason(err); got != reasonTLS {
		t.Errorf("reason = %q (%v), want %q", got, err, reasonTLS)
	}
	if !strings.Contains(failureNote(pipelineTargets, reasonTLS), "client certificate") {
		t.Errorf("the tls note does not name the client certificate: %q", failureNote(pipelineTargets, reasonTLS))
	}
}

// testCA is a throwaway certificate authority for the client-certificate
// tests: the server trusts it for client auth, and issue mints a leaf under it.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func newTestCA(t *testing.T, cn string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &testCA{cert: cert, key: key}
}

func (ca *testCA) pool() *x509.CertPool {
	p := x509.NewCertPool()
	p.AddCert(ca.cert)
	return p
}

// issue mints a client certificate under the CA, returning its certificate and
// key PEM (what the tls.crt and tls.key secret keys hold) and the leaf's DER,
// which is what the server sees as the peer certificate.
func (ca *testCA) issue(t *testing.T, cn string, serial int64) (certPEM, keyPEM string, leaf []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	pk, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pk})), der
}

// clientAuthServer is a TLS target that authenticates its CLIENTS against ca
// under the given policy and records the certificate each request presented.
func clientAuthServer(t *testing.T, ca *testCA, policy tls.ClientAuthType) (*httptest.Server, *atomic.Value) {
	t.Helper()
	var peer atomic.Value // []byte: the presented leaf, nil for an anonymous client
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var leaf []byte
		if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
			leaf = r.TLS.PeerCertificates[0].Raw
		}
		peer.Store(leaf)
		_, _ = w.Write([]byte("m 1\n"))
	}))
	srv.TLS = &tls.Config{ClientAuth: policy, ClientCAs: ca.pool()}
	srv.Config.ErrorLog = log.New(io.Discard, "", 0) // a refused handshake is some cases' point
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv, &peer
}

// mtlsTarget points a target at srv, pinned to its serving certificate, with
// the client-certificate refs every case below resolves through mapAuth.
func mtlsTarget(srv *httptest.Server) kubemeta.ScrapeTarget {
	tgt := testTarget(srv.URL)
	tgt.TLSCA = "ns/tls/ca.crt"
	tgt.TLSServerName = "example.com" // httptest's cert is issued for example.com
	tgt.TLSCert = "ns/tls/tls.crt"
	tgt.TLSKey = "ns/tls/tls.key"
	return tgt
}

// Nothing scraped with a client certificate anywhere in this package's tests,
// so the cert/key resolution, the X509KeyPair parse, the certificate actually
// reaching the target and the cache turning over on a rotated key could all
// regress with the suite green — kube-prometheus-stack's etcd and control-plane
// monitors authenticate exactly this way.
func TestScrapeTargetPresentsItsClientCertificate(t *testing.T) {
	ca := newTestCA(t, "client-ca")
	srv, peer := clientAuthServer(t, ca, tls.RequireAndVerifyClientCert)
	certPEM, keyPEM, leaf := ca.issue(t, "scraper", 2)
	tgt := mtlsTarget(srv)

	exp := &captureExporter{}
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: staticTargets{tgt}, Exporter: exp, StartTime: time.Now(),
		Auth: &mapAuth{vals: map[string]string{
			"ns/tls/ca.crt": pemOf(srv), "ns/tls/tls.crt": certPEM, "ns/tls/tls.key": keyPEM,
		}},
	})
	if _, err := s.scrapeTarget(context.Background(), tgt, 5*time.Second); err != nil {
		t.Fatalf("an mTLS target refused the scrape: %v", err)
	}
	if exp.points() != 1 {
		t.Fatalf("points = %d, want 1", exp.points())
	}
	if got, _ := peer.Load().([]byte); !bytes.Equal(got, leaf) {
		t.Fatal("the target did not see the client certificate the monitor's tlsConfig names")
	}

	// A rotated key pair must build a NEW client and be the one presented: the
	// cache is keyed by the resolved material, so the old client (holding the
	// old private key) is never reused for the new credential.
	before, err := s.clientFor(context.Background(), tgt)
	if err != nil {
		t.Fatal(err)
	}
	certPEM2, keyPEM2, leaf2 := ca.issue(t, "scraper", 3)
	s.cfg.Auth = &mapAuth{vals: map[string]string{
		"ns/tls/ca.crt": pemOf(srv), "ns/tls/tls.crt": certPEM2, "ns/tls/tls.key": keyPEM2,
	}}
	s.authCache = map[string]authCacheEntry{} // bypass the 1-minute secret cache
	after, err := s.clientFor(context.Background(), tgt)
	if err != nil {
		t.Fatal(err)
	}
	if after == before {
		t.Fatal("a rotated client key kept using the client built from the old one")
	}
	if _, err := s.scrapeTarget(context.Background(), tgt, 5*time.Second); err != nil {
		t.Fatalf("the rotated certificate was refused: %v", err)
	}
	if got, _ := peer.Load().([]byte); !bytes.Equal(got, leaf2) {
		t.Fatal("after rotation the target still saw the OLD client certificate")
	}
}

// Material that RESOLVED but is empty — the metadata service answers a
// present-but-empty secret key as a 200 with "" — must be refused, never read
// as "none asked for". For the CA that is the guard against falling back to the
// system trust store; for the client certificate it is the guard against
// scraping under an ANONYMOUS identity: against a target whose client auth is
// optional, both refs resolving empty used to scrape successfully with no
// certificate at all, and against one requiring it the refusal named neither
// ref. mapAuth answers "" for an unknown ref too, so every ref is listed with an
// explicit "" — otherwise this would only prove the lookup miss.
func TestEmptyTLSMaterialIsRefused(t *testing.T) {
	ca := newTestCA(t, "client-ca")
	srv, peer := clientAuthServer(t, ca, tls.VerifyClientCertIfGiven)
	certPEM, keyPEM, _ := ca.issue(t, "scraper", 2)

	for _, tc := range []struct {
		name  string
		empty []string // the refs that resolve to ""
	}{
		{"ca", []string{"ns/tls/ca.crt"}},
		{"cert and key", []string{"ns/tls/tls.crt", "ns/tls/tls.key"}},
		{"cert", []string{"ns/tls/tls.crt"}},
		{"key", []string{"ns/tls/tls.key"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vals := map[string]string{"ns/tls/ca.crt": pemOf(srv), "ns/tls/tls.crt": certPEM, "ns/tls/tls.key": keyPEM}
			for _, ref := range tc.empty {
				vals[ref] = ""
			}
			tgt := mtlsTarget(srv)
			exp := &captureExporter{}
			s := New(Config{
				Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
				Targets: staticTargets{tgt}, Exporter: exp, StartTime: time.Now(),
				Auth: &mapAuth{vals: vals},
			})
			untouched := []byte("untouched")
			peer.Store(untouched)
			_, err := s.scrapeTarget(context.Background(), tgt, 5*time.Second)
			if err == nil {
				t.Fatalf("scraped with %v resolving empty (points=%d): the target was contacted without the material the monitor declares", tc.empty, exp.points())
			}
			if got := failureReason(err); got != reasonTLS {
				t.Errorf("reason = %q (%v), want %q: the material resolved and is unusable", got, err, reasonTLS)
			}
			if !strings.Contains(err.Error(), tc.empty[0]) || !strings.Contains(err.Error(), "empty") {
				t.Errorf("the error %q does not name the empty ref %s", err, tc.empty[0])
			}
			if exp.points() != 0 {
				t.Errorf("points = %d, want 0", exp.points())
			}
			if got, _ := peer.Load().([]byte); !bytes.Equal(got, untouched) {
				t.Error("the target was contacted: empty material must be refused before the handshake")
			}
		})
	}
}

// A client certificate is a pair, and a tlsConfig naming only one half is a CR
// mistake. Handed to X509KeyPair the lone half failed with "failed to find any
// PEM data in key input", naming neither the missing field nor the monitor's
// ref; the refusal must name both, and the target must never be contacted.
func TestUnpairedClientCertificateIsRefusedByName(t *testing.T) {
	ca := newTestCA(t, "client-ca")
	srv, peer := clientAuthServer(t, ca, tls.VerifyClientCertIfGiven)
	certPEM, keyPEM, _ := ca.issue(t, "scraper", 2)

	for _, tc := range []struct {
		name         string
		unset        func(*kubemeta.ScrapeTarget)
		ref, missing string
	}{
		{"cert without keySecret", func(tgt *kubemeta.ScrapeTarget) { tgt.TLSKey = "" }, "ns/tls/tls.crt", "tlsConfig.keySecret"},
		{"keySecret without cert", func(tgt *kubemeta.ScrapeTarget) { tgt.TLSCert = "" }, "ns/tls/tls.key", "tlsConfig.cert"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tgt := mtlsTarget(srv)
			tc.unset(&tgt)
			exp := &captureExporter{}
			s := New(Config{
				Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
				Targets: staticTargets{tgt}, Exporter: exp, StartTime: time.Now(),
				Auth: &mapAuth{vals: map[string]string{"ns/tls/ca.crt": pemOf(srv), "ns/tls/tls.crt": certPEM, "ns/tls/tls.key": keyPEM}},
			})
			untouched := []byte("untouched")
			peer.Store(untouched)
			_, err := s.scrapeTarget(context.Background(), tgt, 5*time.Second)
			if err == nil {
				t.Fatalf("scraped with only one half of a client certificate declared (points=%d)", exp.points())
			}
			if got := failureReason(err); got != reasonTLS {
				t.Errorf("reason = %q (%v), want %q", got, err, reasonTLS)
			}
			if msg := err.Error(); !strings.Contains(msg, tc.ref) || !strings.Contains(msg, tc.missing) {
				t.Errorf("the error %q does not name the declared ref %s and the missing %s", msg, tc.ref, tc.missing)
			}
			if got, _ := peer.Load().([]byte); !bytes.Equal(got, untouched) {
				t.Error("the target was contacted: an unpaired certificate must be refused before the handshake")
			}
		})
	}
}

// No test pinned redirect refusal on any of the four scrape clients, and the
// kubelet client — the one carrying the node's ServiceAccount token — once
// shipped without it: deleting every `CheckRedirect: noRedirect` left the
// package green. Go copies Authorization across a redirect to the same host
// (only a hostname change strips it), and a per-target client presents its
// client certificate wherever a redirect points, so following one is a
// credential leak. Every client must stop at the 302.
func TestScrapeClientsRefuseRedirects(t *testing.T) {
	var reached atomic.Int64
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached.Add(1)
		_, _ = w.Write([]byte("m 1\n"))
	}))
	t.Cleanup(sink.Close)
	redirect := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, sink.URL+"/metrics", http.StatusFound)
	})
	plain := httptest.NewServer(redirect)
	t.Cleanup(plain.Close)
	tlsRedirector := httptest.NewTLSServer(redirect)
	t.Cleanup(tlsRedirector.Close)

	auth := &mapAuth{vals: map[string]string{"ns/s/token": "t0k3n"}}
	withBearer := func(url string) kubemeta.ScrapeTarget {
		tgt := testTarget(url)
		tgt.AuthSecret = "ns/s/token"
		return tgt
	}
	perTarget := withBearer(tlsRedirector.URL)
	perTarget.TLSServerName = "example.com" // forces needsTLSClient
	perTarget.InsecureSkipVerify = true     // …without needing a CA ref
	skipVerify := withBearer(tlsRedirector.URL)
	skipVerify.InsecureSkipVerify = true
	if !needsTLSClient(perTarget) || needsTLSClient(skipVerify) {
		t.Fatal("the fixtures no longer select the per-target and the shared skip-verify clients")
	}

	cases := []struct {
		name   string
		scrape func(*Scraper) error
	}{
		{"default client", func(s *Scraper) error {
			_, err := s.scrapeTarget(context.Background(), withBearer(plain.URL), 5*time.Second)
			return err
		}},
		{"per-target TLS client", func(s *Scraper) error {
			_, err := s.scrapeTarget(context.Background(), perTarget, 5*time.Second)
			return err
		}},
		{"skip-verify client", func(s *Scraper) error {
			_, err := s.scrapeTarget(context.Background(), skipVerify, 5*time.Second)
			return err
		}},
		{"kubelet client", func(s *Scraper) error {
			resp, err := s.kubeletGet(context.Background(), plain.URL+"/metrics/cadvisor", acceptExposition)
			if err == nil {
				drainClose(resp.Body)
			}
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := reached.Load()
			s := newKubeletScraper(t, plain.URL, &fakeMetaSource{}, &captureExporter{}, false)
			s.cfg.Auth = auth
			err := tc.scrape(s)
			var se *statusError
			if !errors.As(err, &se) || se.code != http.StatusFound {
				t.Errorf("err = %v, want the 302 itself: the redirect was followed or failed otherwise", err)
			}
			if n := reached.Load() - before; n != 0 {
				t.Errorf("the redirect target received %d request(s): the client followed a redirect with the scrape's credentials attached", n)
			}
		})
	}
}
