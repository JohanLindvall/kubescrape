package otlpexport

// Static headers vs the headers the transport sets itself. The two protocols
// resolved the same config differently — HTTP's Set replaced the OTLP framing
// and the rotating bearer token, gRPC appended a second value — so the same
// ConfigMap authenticated (or 415'd) differently depending on -otlp-protocol,
// with nothing refused at startup.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The framing keys are the transport's and are refused outright; Authorization
// is refused only BESIDE a bearer token file, because a static Authorization
// header is the only way to spell Basic auth here and must stay usable alone.
func TestReservedStaticHeadersAreRefused(t *testing.T) {
	base := func(h map[string]string) Config {
		return Config{Endpoint: "collector:4317", Protocol: "grpc", Insecure: true, Headers: h}
	}
	for _, k := range []string{"Content-Type", "content-encoding", "CONTENT-LENGTH"} {
		err := base(map[string]string{k: "x"}).Validate()
		if err == nil || !strings.Contains(err.Error(), k) {
			t.Errorf("Validate with header %q = %v, want a refusal naming it", k, err)
		}
	}

	withToken := base(map[string]string{"authorization": "Basic Zm9v"})
	withToken.BearerTokenFile = "/var/run/secrets/token"
	if err := withToken.Validate(); err == nil {
		t.Error("an Authorization header beside a bearer token file must be refused: two credentials for one destination")
	}
	// Alone it is legitimate — a collector wanting Basic auth has no other
	// spelling, and -otlp-bearer-token-file only ever writes Bearer.
	if err := base(map[string]string{"Authorization": "Basic Zm9v"}).Validate(); err != nil {
		t.Errorf("a static Authorization header without a bearer token file: %v, want accepted", err)
	}
	// An ordinary tenancy header is untouched.
	if err := base(map[string]string{"X-Scope-OrgID": "platform"}).Validate(); err != nil {
		t.Errorf("X-Scope-OrgID: %v, want accepted", err)
	}
}

// And the ordering behind the refusal: the transport's own headers are applied
// AFTER the static map on the HTTP arm, matching the gRPC arm where the
// credential is appended last. A key that ever slipped past Validate must not
// be able to replace the protobuf content type.
func TestTransportHeadersWinOverStaticOnes(t *testing.T) {
	var ct, enc, auth, tenant atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ct.Store(r.Header.Get("Content-Type"))
		enc.Store(r.Header.Get("Content-Encoding"))
		auth.Store(r.Header.Get("Authorization"))
		tenant.Store(r.Header.Get("X-Scope-OrgID"))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c, err := New(Config{
		Endpoint: srv.URL, Protocol: "http", Compression: "gzip", Timeout: 5 * time.Second,
		Headers: map[string]string{"X-Scope-OrgID": "platform", "Authorization": "Basic Zm9v"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	// Validate refuses these, so plant them behind its back: this pins the
	// ORDER, which is the second line of defence rather than the first. Into
	// headerKV, the flattening BOTH arms send from (the HTTP arm used to range
	// over cfg.Headers, in random order).
	c.headerKV = append(c.headerKV, "Content-Type", "text/plain", "Content-Encoding", "identity")

	if err := c.ExportLogs(context.Background(), testLogsPayload()); err != nil {
		t.Fatal(err)
	}
	if got := ct.Load(); got != "application/x-protobuf" {
		t.Errorf("Content-Type = %v, want the transport's application/x-protobuf", got)
	}
	if got := enc.Load(); got != "gzip" {
		t.Errorf("Content-Encoding = %v, want the transport's gzip", got)
	}
	// With no bearer token file the static credential is the only one, and it
	// still ships — the ordering must not silently delete it.
	if got := auth.Load(); got != "Basic Zm9v" {
		t.Errorf("Authorization = %v, want the configured static credential", got)
	}
	if got := tenant.Load(); got != "platform" {
		t.Errorf("X-Scope-OrgID = %v, want the static header", got)
	}
}
