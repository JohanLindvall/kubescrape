// Tests for the per-signal destination mux (persignal.go).
package otlpexport

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
)

// testLogsPayload is one log record, enough to exercise a send.
func testLogsPayload() plog.Logs {
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("x")
	return ld
}

// countingCollector records hits per path plus the last tenancy header.
func countingCollector(hits *atomic.Int32, tenant *atomic.Value) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		tenant.Store(r.Header.Get("X-Scope-OrgID"))
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	}))
}

// A logs override routes ONLY logs to its endpoint; metrics stay on the
// default — and headers merge (section base + override, override winning).
func TestPerSignalRoutesAndMergesHeaders(t *testing.T) {
	var defHits, logHits atomic.Int32
	var defTenant, logTenant atomic.Value
	defSrv := countingCollector(&defHits, &defTenant)
	defer defSrv.Close()
	logSrv := countingCollector(&logHits, &logTenant)
	defer logSrv.Close()

	ps, err := BuildExporter(Config{
		Endpoint: defSrv.URL, Protocol: "http", Compression: "none", Timeout: 5 * time.Second, RetryAttempts: 1,
	}, &ExportConfig{
		Headers: map[string]string{"X-Scope-OrgID": "base-tenant"},
		Logs:    &ExportOverride{Endpoint: logSrv.URL, Headers: map[string]string{"X-Scope-OrgID": "logs-tenant"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ps.Close() }()

	if err := ps.ExportLogs(context.Background(), testLogsPayload()); err != nil {
		t.Fatal(err)
	}
	if err := ps.ExportMetrics(context.Background(), testMetrics()); err != nil {
		t.Fatal(err)
	}
	if logHits.Load() != 1 || defHits.Load() != 1 {
		t.Fatalf("hits: logs=%d default=%d, want 1/1", logHits.Load(), defHits.Load())
	}
	if logTenant.Load() != "logs-tenant" {
		t.Fatalf("logs tenant = %v, want the override", logTenant.Load())
	}
	if defTenant.Load() != "base-tenant" {
		t.Fatalf("default tenant = %v, want the section base header on the DEFAULT chain", defTenant.Load())
	}
}

// Shape validation is dry (no files touched) and rejects half a client cert —
// the section's own (Validate) and an override's (on the merged destination,
// ValidateAgainst) — and unknown protocols.
func TestExportConfigValidate(t *testing.T) {
	base := Config{Endpoint: "collector:4317", Protocol: "grpc"}
	if err := (&ExportConfig{ClientCertFile: "c.pem"}).Validate(); err == nil {
		t.Fatal("half a client cert pair must be rejected")
	}
	// A lone KEY is the half the merge used to drop silently (a pair was taken
	// only when its certificate was set), so it must reach Config.Validate.
	for _, o := range []*ExportOverride{
		{ClientKeyFile: "k.pem"},
		{Endpoint: "https://loki.example.com:443", Protocol: "http", ClientKeyFile: "k.pem"},
		{ClientCertFile: "c.pem"},
	} {
		if err := (&ExportConfig{Logs: o}).ValidateAgainst(base); err == nil || !strings.Contains(err.Error(), "export.logs") {
			t.Errorf("half an override client pair %+v must be refused naming the signal, got %v", o, err)
		}
	}
	if err := (&ExportConfig{Logs: &ExportOverride{Protocol: "carrier-pigeon"}}).ValidateAgainst(base); err == nil {
		t.Fatal("unknown protocol must be rejected")
	}
	var nilCfg *ExportConfig
	if err := nilCfg.Validate(); err != nil {
		t.Fatalf("nil section: %v", err)
	}
}

// A client certificate on a plaintext destination must refuse startup, not
// silently skip the handshake — for the flag-built base and per-signal
// overrides alike.
func TestClientCertOnPlaintextRefused(t *testing.T) {
	dir := t.TempDir()
	// New checks plaintext-ness BEFORE loading the pair, so paths suffice.
	cert, key := dir+"/c.crt", dir+"/c.key"

	if _, err := New(Config{Endpoint: "h:4317", Protocol: "grpc", Insecure: true,
		ClientCertFile: cert, ClientKeyFile: key}); err == nil {
		t.Fatal("client cert on plaintext gRPC must be refused")
	}
	if _, err := New(Config{Endpoint: "http://h:4318", Protocol: "http",
		ClientCertFile: cert, ClientKeyFile: key}); err == nil {
		t.Fatal("client cert on plain http must be refused")
	}
	// The per-signal path surfaces the same refusal.
	if _, err := BuildExporter(Config{Endpoint: "h:4317", Protocol: "grpc", Insecure: true},
		&ExportConfig{ClientCertFile: cert, ClientKeyFile: key}); err == nil {
		t.Fatal("BuildExporter must refuse a base client cert on a plaintext base")
	}
	// The dry run (shape-only) catches a scheme-less http override endpoint.
	if err := (&ExportConfig{Logs: &ExportOverride{Protocol: "http", Endpoint: "loki:3100"}}).ValidateAgainst(Config{Endpoint: "h:4317"}); err == nil {
		t.Fatal("http override endpoint without a scheme must fail ValidateAgainst")
	}
}

// The collectorless shape — a client certificate plus an override for EVERY
// signal — must start under the default flags (-otlp-protocol=grpc,
// -otlp-insecure=true). The base is then unreachable, so building it (and
// refusing it for carrying a cert on a plaintext connection) would reject the
// documented config for a destination nothing can ever send to.
func TestAllSignalsOverriddenSkipsUnreachableDefault(t *testing.T) {
	cert, key := writeKeyPair(t)
	base := Config{Endpoint: "otel-collector.monitoring:4317", Protocol: "grpc", Insecure: true}
	no := false
	ps, err := BuildExporter(base, &ExportConfig{
		ClientCertFile: cert, ClientKeyFile: key,
		Logs:    &ExportOverride{Endpoint: "https://loki.example.com/otlp", Protocol: "http"},
		Metrics: &ExportOverride{Endpoint: "https://mimir.example.com/otlp", Protocol: "http"},
		Traces:  &ExportOverride{Endpoint: "tempo.example.com:4317", Insecure: &no},
	})
	if err != nil {
		t.Fatalf("fully-overridden config must start: %v", err)
	}
	defer func() { _ = ps.Close() }()
	if ps.Default != nil {
		t.Fatal("no signal can reach the default; it must not be built")
	}
	// Every signal still resolves to a non-nil client.
	if ps.logsClient() == nil || ps.metricsClient() == nil || ps.tracesClient() == nil {
		t.Fatal("a signal resolved to a nil client")
	}

	// But leave ONE signal on the base and the cert genuinely would be unused
	// there — that must still be refused rather than silently ignored.
	if _, err := BuildExporter(base, &ExportConfig{
		ClientCertFile: cert, ClientKeyFile: key,
		Logs:    &ExportOverride{Endpoint: "https://loki.example.com/otlp", Protocol: "http"},
		Metrics: &ExportOverride{Endpoint: "https://mimir.example.com/otlp", Protocol: "http"},
	}); err == nil {
		t.Fatal("a reachable plaintext default carrying a client cert must be refused")
	}
}

// writeKeyPair writes a throwaway self-signed certificate and key, for the
// TLS-carrying destinations that actually load the pair.
func writeKeyPair(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "kubescrape-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile, keyFile = dir+"/c.crt", dir+"/c.key"
	writePEM(t, certFile, &pem.Block{Type: "CERTIFICATE", Bytes: der})
	writePEM(t, keyFile, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	return certFile, keyFile
}

func writePEM(t *testing.T, path string, b *pem.Block) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := pem.Encode(f, b); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// ValidateAgainst walks the three signals in a FIXED order, so a section with
// two mistakes is refused for the same one on every run (the walk used to
// range a map, and a test of the wording could only pin whichever came first).
func TestExportConfigValidateRefusesSignalsInOrder(t *testing.T) {
	cfg := &ExportConfig{
		Logs:   &ExportOverride{Protocol: "carrier-pigeon"},
		Traces: &ExportOverride{Compression: "brotli"},
	}
	for range 20 {
		err := cfg.ValidateAgainst(Config{Endpoint: "collector:4317"})
		if err == nil || !strings.Contains(err.Error(), "export.logs: protocol") {
			t.Fatalf("ValidateAgainst = %v, want the logs override refused first", err)
		}
	}
}

// A per-signal override naming its OWN endpoint names a DIFFERENT host, so it
// must not inherit the flag base's collector credentials — the bearer token,
// the CA bundle and the skip-verify trust decision that -otlp-* configure for
// the deployment's own collector. Copying the base wholesale presented all
// three to whatever backend the override named (a third-party SaaS host in the
// documented collectorless shape), with no per-signal field to opt out with.
//
// The section's OWN base additions still apply, and so does plaintext-ness:
// both are argued at signalConfig.
func TestOwnEndpointSignalDoesNotInheritBaseCredentials(t *testing.T) {
	base := Config{
		Endpoint:           "otel-collector.monitoring:4317",
		Protocol:           "grpc",
		Insecure:           true,
		InsecureSkipVerify: true,
		CAFile:             "/etc/certs/collector-ca.crt",
		BearerTokenFile:    "/var/run/secrets/collector-token",
		Headers:            map[string]string{"X-Flag-Header": "collector"},
		Timeout:            9 * time.Second,
		RetryAttempts:      4,
		MaxSendBytes:       123456,
	}
	cfg := &ExportConfig{
		Headers:        map[string]string{"X-Scope-OrgID": "platform"},
		ClientCertFile: "/etc/certs/client.crt",
		ClientKeyFile:  "/etc/certs/client.key",
		Traces:         &ExportOverride{Endpoint: "https://tempo-prod-04.grafana.net/otlp", Protocol: "http"},
	}
	got := cfg.signalConfig(cfg.Traces, base)

	if got.Endpoint != "https://tempo-prod-04.grafana.net/otlp" {
		t.Fatalf("endpoint = %q, want the override's", got.Endpoint)
	}
	for _, tc := range []struct{ what, got string }{
		{"bearerTokenFile", got.BearerTokenFile},
		{"caFile", got.CAFile},
	} {
		if tc.got != "" {
			t.Errorf("%s = %q crossed to the override's own endpoint; the flag base's credential is the COLLECTOR's", tc.what, tc.got)
		}
	}
	if got.InsecureSkipVerify {
		t.Error("insecureSkipVerify crossed to the override's own endpoint: the third party's certificate would not be verified")
	}
	if _, ok := got.Headers["X-Flag-Header"]; ok {
		t.Error("a flag-base header crossed to the override's own endpoint")
	}

	// The section's own additions are declared beside the endpoint, at
	// every-signal scope, and still apply — the documented collectorless
	// example depends on it.
	if got.Headers["X-Scope-OrgID"] != "platform" {
		t.Errorf("export.headers = %v, want the section's tenancy header applied", got.Headers)
	}
	if got.ClientCertFile != cfg.ClientCertFile || got.ClientKeyFile != cfg.ClientKeyFile {
		t.Errorf("client cert = %q/%q, want the section's mTLS identity", got.ClientCertFile, got.ClientKeyFile)
	}
	// Plaintext-ness is transport to the named host, not a credential.
	if !got.Insecure {
		t.Error("insecure was not carried; an own-endpoint signal written against a plaintext collector would flip to TLS on upgrade")
	}
	// Transport tuning is the inheritable half, by definition.
	if got.Timeout != base.Timeout || got.RetryAttempts != base.RetryAttempts || got.MaxSendBytes != base.MaxSendBytes {
		t.Errorf("transport tuning not inherited: %+v", got)
	}

	// The override's own credentials are still honoured — dropping the base's
	// is not a refusal to authenticate, it is a refusal to REUSE.
	cfg.Traces.BearerTokenFile = "/var/run/secrets/tempo-token"
	cfg.Traces.CAFile = "/etc/certs/tempo-ca.crt"
	yes := true
	cfg.Traces.InsecureSkipVerify = &yes
	got = cfg.signalConfig(cfg.Traces, base)
	if got.BearerTokenFile != "/var/run/secrets/tempo-token" || got.CAFile != "/etc/certs/tempo-ca.crt" || !got.InsecureSkipVerify {
		t.Errorf("the override's own credentials were not applied: %+v", got)
	}
}

// The reflective half of the rule above: with a bare section, NOTHING
// destination-scoped may reach an own-endpoint signal except the endpoint
// itself and the plaintext decision. A new credential field on Config fails
// this test until signalConfig is taught about it — the same job
// TestConfigFieldsAreClassified does for TransportOnly.
func TestNoDestinationFieldCrossesToAnOwnEndpointSignal(t *testing.T) {
	base := fillConfig(t)
	base.Endpoint = "otel-collector.monitoring:4317"
	cfg := &ExportConfig{Logs: &ExportOverride{Endpoint: "https://loki.example.com/otlp"}}
	got := reflect.ValueOf(cfg.signalConfig(cfg.Logs, base))
	typ := got.Type()
	// Endpoint is the override's; Insecure is transport to the named host.
	carried := map[string]bool{"Endpoint": true, "Insecure": true}
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		if !destinationFields[name] || carried[name] {
			continue
		}
		if !got.Field(i).IsZero() {
			t.Errorf("Config.%s = %v reached a signal naming its own endpoint; a destination-scoped field must come from the override or stay unset",
				name, got.Field(i).Interface())
		}
	}
}

// A signal whose override has no endpoint of its own — or repeats the flag
// base's — is still aimed at the base destination, so the base's credentials
// still belong to it. Dropping them there would break every deployment that
// overrides only headers or a protocol.
func TestSignalWithoutItsOwnEndpointStillInheritsTheBase(t *testing.T) {
	base := Config{
		Endpoint:        "otel-collector.monitoring:4317",
		BearerTokenFile: "/var/run/secrets/collector-token",
		CAFile:          "/etc/certs/collector-ca.crt",
	}
	cfg := &ExportConfig{
		Logs:    &ExportOverride{Headers: map[string]string{"X-Scope-OrgID": "logs"}},
		Metrics: &ExportOverride{Endpoint: base.Endpoint, Compression: "none"},
	}
	for _, tc := range []struct {
		name string
		o    *ExportOverride
	}{{"logs", cfg.Logs}, {"metrics", cfg.Metrics}} {
		got := cfg.signalConfig(tc.o, base)
		if got.Endpoint != base.Endpoint {
			t.Errorf("%s endpoint = %q, want the base's", tc.name, got.Endpoint)
		}
		if got.BearerTokenFile != base.BearerTokenFile || got.CAFile != base.CAFile {
			t.Errorf("%s lost the base credentials for a destination that IS the base: %+v", tc.name, got)
		}
	}
}

// The warning behind the rule fires only where behaviour actually differs from
// a naive merge: a credential the base does not carry, one the override
// replaces, or a destination that IS the base has nothing to report, and a
// warning on those would train an operator to ignore the one that matters.
func TestDroppedBaseCredentialsNamesOnlyWhatActuallyStopped(t *testing.T) {
	base := Config{Endpoint: "collector:4317", BearerTokenFile: "/tok", CAFile: "/ca", InsecureSkipVerify: true}
	own := "https://tempo.example.com/otlp"

	got := DroppedBaseCredentials(&ExportOverride{Endpoint: own}, base)
	want := []string{"-otlp-bearer-token-file", "-otlp-tls-ca-file", "-otlp-tls-insecure-skip-verify"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("dropped = %v, want %v (in flag order, so two runs name them the same way)", got, want)
	}

	no := false
	for _, tc := range []struct {
		name string
		o    *ExportOverride
		base Config
	}{
		{"no endpoint of its own", &ExportOverride{}, base},
		{"the base's own endpoint", &ExportOverride{Endpoint: base.Endpoint}, base},
		{"its own credentials throughout", &ExportOverride{
			Endpoint: own, BearerTokenFile: "/t2", CAFile: "/ca2", InsecureSkipVerify: &no}, base},
		{"a base carrying no credential", &ExportOverride{Endpoint: own}, Config{Endpoint: base.Endpoint}},
	} {
		if got := DroppedBaseCredentials(tc.o, tc.base); got != nil {
			t.Errorf("%s: dropped = %v, want nothing to warn about", tc.name, got)
		}
	}
}

// A routing route and an export.<signal> override are ONE derivation
// (destinationConfig): carrying the same destination fields, they derive the
// same client config on every arm — no endpoint, the base's endpoint
// repeated, an endpoint of their own — with the single deliberate difference
// that the section's client pair never reaches a route naming its own
// endpoint. Two copies used to exist, and they disagreed on which endpoint is
// another host, on whether skip-verify crossed to it, and on what an
// endpoint-less route did with its own credentials.
func TestRouteAndSignalDeriveTheSameDestination(t *testing.T) {
	base := Config{
		Endpoint:           "otel-collector.monitoring:4317",
		Protocol:           "grpc",
		Insecure:           true,
		InsecureSkipVerify: true,
		CAFile:             "/etc/certs/collector-ca.crt",
		BearerTokenFile:    "/var/run/secrets/collector-token",
		Timeout:            9 * time.Second,
		RetryAttempts:      4,
	}
	no := false
	creds := func(o ExportOverride) ExportOverride {
		o.Headers = map[string]string{"X-Scope-OrgID": "tenant"}
		o.BearerTokenFile, o.CAFile, o.InsecureSkipVerify = "/own/token", "/own/ca.crt", &no
		return o
	}
	overrides := map[string]ExportOverride{
		"no endpoint":                        {},
		"no endpoint, own credentials":       creds(ExportOverride{}),
		"the base endpoint repeated":         {Endpoint: base.Endpoint},
		"the base repeated, own credentials": creds(ExportOverride{Endpoint: base.Endpoint}),
		"an endpoint of its own":             {Endpoint: "tenant.example.com:4317"},
		"its own endpoint and credentials":   creds(ExportOverride{Endpoint: "tenant.example.com:4317"}),
		"its own client pair":                {Endpoint: "tenant.example.com:4317", ClientCertFile: "/own/c.crt", ClientKeyFile: "/own/c.key"},
		"a lone key":                         {ClientKeyFile: "/own/c.key"},
	}
	sections := map[string]*ExportConfig{
		"no section":            nil,
		"section headers":       {Headers: map[string]string{"X-Scope-OrgID": "platform", "X-Base": "1"}},
		"section mTLS identity": {Headers: map[string]string{"X-Base": "1"}, ClientCertFile: "/sec/c.crt", ClientKeyFile: "/sec/c.key"},
	}
	for sname, sec := range sections {
		for oname, o := range overrides {
			route, signal := sec.RouteConfig(&o, base), sec.signalConfig(&o, base)
			if sec != nil && sec.ClientCertFile != "" && OwnEndpoint(o.Endpoint, base) && o.ClientCertFile == "" && o.ClientKeyFile == "" {
				// The one deliberate difference: the section's mTLS identity is
				// the export: block author's to present, not a route's.
				if route.ClientCertFile != "" || route.ClientKeyFile != "" {
					t.Errorf("%s / %s: the section's client pair reached a route naming its own endpoint: %+v", sname, oname, route)
				}
				if signal.ClientCertFile != sec.ClientCertFile {
					t.Errorf("%s / %s: an export.<signal> naming its own endpoint lost the section's client pair: %+v", sname, oname, signal)
				}
				signal.ClientCertFile, signal.ClientKeyFile = "", ""
			}
			if !reflect.DeepEqual(route, signal) {
				t.Errorf("%s / %s: a route and an export.<signal> with the same fields derived different destinations:\n route: %+v\nsignal: %+v", sname, oname, route, signal)
			}
		}
	}
}

// Overrides is the one walk over the three signals, on both sides of the
// package boundary: fixed order, nil entries kept, nil for no section.
func TestOverridesWalkTheSignalsInAFixedOrder(t *testing.T) {
	var none *ExportConfig
	if got := none.Overrides(); got != nil {
		t.Errorf("no section: Overrides = %v, want nil", got)
	}
	cfg := &ExportConfig{Metrics: &ExportOverride{Endpoint: "m:4317"}}
	var names []string
	for _, s := range cfg.Overrides() {
		names = append(names, s.Name)
		if (s.Override != nil) != (s.Name == "metrics") {
			t.Errorf("%s: override = %v", s.Name, s.Override)
		}
	}
	if want := []string{"logs", "metrics", "traces"}; !reflect.DeepEqual(names, want) {
		t.Errorf("names = %v, want %v", names, want)
	}
}

// The flag base is unused only when every signal is overridden with an
// endpoint of its OWN: an override that inherits the base endpoint (none set)
// or repeats it still dials the base.
func TestBaseEndpointUnusedOnlyWhenEverySignalNamesItsOwnEndpoint(t *testing.T) {
	base := Config{Endpoint: "collector:4317"}
	own := func(ep string) *ExportOverride { return &ExportOverride{Endpoint: ep} }
	for _, tc := range []struct {
		name string
		cfg  *ExportConfig
		want bool
	}{
		{"no section", nil, false},
		{"one signal left to the base", &ExportConfig{Logs: own("l:4317"), Metrics: own("m:4317")}, false},
		{"an override inheriting the base endpoint", &ExportConfig{
			Logs: own("l:4317"), Metrics: own("m:4317"), Traces: &ExportOverride{Compression: "none"}}, false},
		{"an override repeating the base endpoint", &ExportConfig{
			Logs: own("l:4317"), Metrics: own("m:4317"), Traces: own(base.Endpoint)}, false},
		{"every signal elsewhere", &ExportConfig{
			Logs: own("l:4317"), Metrics: own("m:4317"), Traces: own("t:4317")}, true},
	} {
		if got := tc.cfg.BaseEndpointUnused(base); got != tc.want {
			t.Errorf("%s: BaseEndpointUnused = %v, want %v", tc.name, got, tc.want)
		}
	}
}
