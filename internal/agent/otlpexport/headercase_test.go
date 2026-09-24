package otlpexport

// Header names are case-insensitive on BOTH transports — gRPC lowercases
// metadata keys, HTTP canonicalises header names — so every layer that merges
// or validates a header map must be too. It was not: MergeHeaders compared
// the exact string, so an override spelled `x-scope-orgid` sat BESIDE a base
// `X-Scope-OrgID` instead of replacing it, Validate passed the pair, and the
// wire carried one header with two values — gRPC sent both, HTTP whichever
// its map iteration visited last, so the tenant changed at random between
// exports.

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// tenantSink records every x-scope-orgid value set a gRPC export carried.
type tenantSink struct {
	plogotlp.UnimplementedGRPCServer
	mu   sync.Mutex
	seen [][]string
}

func (s *tenantSink) Export(ctx context.Context, _ plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen = append(s.seen, slices.Clone(md.Get("x-scope-orgid")))
	return plogotlp.NewExportResponse(), nil
}

func TestHeaderOverrideIsCaseInsensitive(t *testing.T) {
	onlyOverride := func(t *testing.T, where string, h map[string]string) {
		t.Helper()
		if len(h) != 2 || h["x-scope-orgid"] != "team-a" || h["X-Keep"] != "1" {
			t.Errorf("%s: headers = %v, want the lowercased override REPLACING the base spelling, beside the untouched key", where, h)
		}
	}
	base := map[string]string{"X-Scope-OrgID": "default-tenant", "X-Keep": "1"}
	over := map[string]string{"x-scope-orgid": "team-a"}

	onlyOverride(t, "MergeHeaders", MergeHeaders(base, over))
	if base["X-Scope-OrgID"] != "default-tenant" || len(base) != 2 {
		t.Fatalf("MergeHeaders wrote through the shared base: %v", base)
	}

	// ApplyBase: the section's headers over the flag base's.
	onlyOverride(t, "ApplyBase", (&ExportConfig{Headers: over}).ApplyBase(Config{Headers: base}).Headers)

	// signalConfig: both arms — the override inheriting the base destination
	// and the override naming its own endpoint (whose base is the section's).
	sec := &ExportConfig{Headers: base, Logs: &ExportOverride{Headers: over}}
	onlyOverride(t, "signalConfig (inherits)", sec.signalConfig(sec.Logs, Config{Endpoint: "collector:4317"}).Headers)
	own := &ExportConfig{Headers: base, Logs: &ExportOverride{Endpoint: "tenant:4317", Headers: over}}
	onlyOverride(t, "signalConfig (own endpoint)", own.signalConfig(own.Logs, Config{Endpoint: "collector:4317"}).Headers)

	// And on the wire, both protocols: every export of many carries exactly
	// the override. Random map order made one export in several the base's.
	const exports = 32
	t.Run("http", func(t *testing.T) {
		var mu sync.Mutex
		var seen [][]string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			seen = append(seen, r.Header.Values("X-Scope-OrgID"))
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()
		wireExports(t, Config{Endpoint: srv.URL, Protocol: "http", Compression: "none", Timeout: 5 * time.Second, RetryAttempts: 1}, exports)
		mu.Lock()
		defer mu.Unlock()
		checkTenants(t, seen, exports)
	})
	t.Run("grpc", func(t *testing.T) {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		sink := &tenantSink{}
		srv := grpc.NewServer()
		plogotlp.RegisterGRPCServer(srv, sink)
		go func() { _ = srv.Serve(lis) }()
		defer srv.Stop()
		wireExports(t, Config{Endpoint: lis.Addr().String(), Protocol: "grpc", Insecure: true, Timeout: 5 * time.Second, RetryAttempts: 1}, exports)
		sink.mu.Lock()
		defer sink.mu.Unlock()
		checkTenants(t, sink.seen, exports)
	})
}

// wireExports builds the per-signal stack the agent builds — section base
// header, logs override spelling it in lowercase — validates it the way
// -check-config does, and exports n log payloads through it.
func wireExports(t *testing.T, base Config, n int) {
	t.Helper()
	cfg := &ExportConfig{
		Headers: map[string]string{"X-Scope-OrgID": "default-tenant"},
		Logs:    &ExportOverride{Headers: map[string]string{"x-scope-orgid": "team-a"}},
	}
	if err := cfg.ValidateAgainst(base); err != nil {
		t.Fatalf("ValidateAgainst: %v", err)
	}
	ps, err := BuildExporter(base, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ps.Close() }()
	for range n {
		if err := ps.ExportLogs(context.Background(), testLogsPayload()); err != nil {
			t.Fatal(err)
		}
	}
}

func checkTenants(t *testing.T, seen [][]string, n int) {
	t.Helper()
	if len(seen) != n {
		t.Fatalf("collector saw %d exports, want %d", len(seen), n)
	}
	for i, v := range seen {
		if !slices.Equal(v, []string{"team-a"}) {
			t.Fatalf("export %d carried X-Scope-OrgID %q, want exactly the override [team-a]", i, v)
		}
	}
}

// The static headers neither transport can send are refused at Validate —
// -check-config — rather than failing every export: net/http refuses them
// client-side ("invalid header field value"), grpc-go with codes.Internal,
// both TRANSIENT, so the spool retried them forever and filled. So is a
// static Host (net/http takes it from the URL and ignores the map) and a
// pair differing only in case (one header, two values). The error names the
// KEY and never echoes the value: header values are routinely credentials.
func TestStaticHeadersNeitherTransportCanSendAreRefused(t *testing.T) {
	const secret = "s3cret-tenant-token"
	for _, tc := range []struct {
		name    string
		headers map[string]string
		key     string // the key the refusal must name
	}{
		{"trailing newline from a YAML block", map[string]string{"X-Scope-OrgID": secret + "\n"}, "X-Scope-OrgID"},
		{"carriage return", map[string]string{"X-Scope-OrgID": secret + "\r\nX-Evil: 1"}, "X-Scope-OrgID"},
		{"non-ASCII value", map[string]string{"X-Scope-OrgID": secret + "é"}, "X-Scope-OrgID"},
		{"NUL in the value", map[string]string{"X-Token": secret + "\x00"}, "X-Token"},
		{"space in the name", map[string]string{"bad key": secret}, "bad key"},
		{"colon in the name", map[string]string{"X-Scope:OrgID": secret}, "X-Scope:OrgID"},
		{"empty name", map[string]string{"": secret}, "empty name"},
		{"static Host", map[string]string{"Host": secret}, "Host"},
		{"lowercase host", map[string]string{"host": secret}, "host"},
		{"two spellings of one header", map[string]string{"X-Scope-OrgID": secret, "x-scope-orgid": "other"}, "x-scope-orgid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, proto := range []string{"grpc", "http"} {
				cfg := Config{Endpoint: "collector:4317", Protocol: proto, Insecure: true, Headers: tc.headers}
				if proto == "http" {
					cfg.Endpoint = "http://collector:4318"
				}
				err := cfg.Validate()
				if err == nil {
					t.Fatalf("%s: Validate accepted %q — a header no transport can send, or one that is silently dropped", proto, tc.headers)
				}
				if !strings.Contains(err.Error(), tc.key) {
					t.Errorf("%s: refusal %q does not name %q", proto, err, tc.key)
				}
				if strings.Contains(err.Error(), secret) {
					t.Errorf("%s: refusal echoes the header VALUE: %q", proto, err)
				}
			}
		})
	}

	// What stays legal: every byte the intersection admits, in the name and in
	// the value.
	ok := Config{Endpoint: "collector:4317", Protocol: "grpc", Insecure: true, Headers: map[string]string{
		"X-Scope-OrgID":      "platform",
		"x_custom.header-09": "a value ~ with spaces, punctuation: and =",
		"Authorization":      "Basic Zm9vOmJhcg==",
	}}
	if err := ok.Validate(); err != nil {
		t.Errorf("Validate refused headers both transports send: %v", err)
	}
}
