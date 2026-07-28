package azurediag

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWorkloadIdentityFetch(t *testing.T) {
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenFile, []byte("federated-jwt\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var gotForm map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/tenant-1/oauth2/v2.0/token" {
			t.Errorf("path = %s", r.URL.Path)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		gotForm = map[string]string{}
		for k := range r.PostForm {
			gotForm[k] = r.PostForm.Get(k)
		}
		w.Header().Set("Content-Type", "application/json")
		// The real endpoint returns expires_in as a NUMBER.
		_, _ = w.Write([]byte(`{"token_type":"Bearer","expires_in":3599,"access_token":"entra-token"}`))
	}))
	defer srv.Close()

	fetch := workloadIdentityFetch(srv.URL, "tenant-1", "client-1", tokenFile, "https://ns.servicebus.windows.net/.default", srv.Client())
	tok, ttl, err := fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tok != "entra-token" {
		t.Fatalf("token = %q", tok)
	}
	if ttl != 3599*time.Second {
		t.Fatalf("ttl = %v", ttl)
	}
	for k, want := range map[string]string{
		"grant_type":            "client_credentials",
		"client_id":             "client-1",
		"scope":                 "https://ns.servicebus.windows.net/.default",
		"client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
		"client_assertion":      "federated-jwt", // trimmed
	} {
		if gotForm[k] != want {
			t.Errorf("form[%s] = %q, want %q", k, gotForm[k], want)
		}
	}
}

func TestIMDSFetch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata") != "true" {
			t.Error("missing Metadata: true header")
		}
		q := r.URL.Query()
		if q.Get("resource") != "https://ns.servicebus.windows.net" {
			t.Errorf("resource = %q", q.Get("resource"))
		}
		if q.Get("client_id") != "uami-1" {
			t.Errorf("client_id = %q", q.Get("client_id"))
		}
		w.Header().Set("Content-Type", "application/json")
		// IMDS returns expires_in as a STRING.
		_, _ = w.Write([]byte(`{"access_token":"imds-token","expires_in":"3599","token_type":"Bearer"}`))
	}))
	defer srv.Close()

	fetch := imdsFetch(srv.URL, "https://ns.servicebus.windows.net", "uami-1", srv.Client())
	tok, ttl, err := fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tok != "imds-token" || ttl != 3599*time.Second {
		t.Fatalf("token = %q, ttl = %v", tok, ttl)
	}
}

func TestTokenSourceCachesAndRefreshes(t *testing.T) {
	fetches := 0
	now := time.Unix(1000, 0)
	ts := &tokenSource{
		now: func() time.Time { return now },
		fetch: func(context.Context) (string, time.Duration, error) {
			fetches++
			return "tok", time.Hour, nil
		},
	}
	for i := 0; i < 5; i++ {
		if tok, err := ts.get(context.Background()); err != nil || tok != "tok" {
			t.Fatalf("get: %q, %v", tok, err)
		}
	}
	if fetches != 1 {
		t.Fatalf("fetches = %d, want 1 (cached)", fetches)
	}
	// Inside the refresh margin: a fetch happens.
	now = now.Add(time.Hour - refreshMargin + time.Second)
	if _, err := ts.get(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fetches != 2 {
		t.Fatalf("fetches = %d, want 2 (refreshed ahead of expiry)", fetches)
	}
}

// A refresh failure while the old token is still VALID serves the old token:
// a token-endpoint blip must not fail every new Kafka connection.
func TestTokenSourceServesStaleOnRefreshFailure(t *testing.T) {
	calls := 0
	now := time.Unix(1000, 0)
	ts := &tokenSource{
		now: func() time.Time { return now },
		fetch: func(context.Context) (string, time.Duration, error) {
			calls++
			if calls == 1 {
				return "tok", time.Hour, nil
			}
			return "", 0, errors.New("endpoint down")
		},
	}
	if _, err := ts.get(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Inside the margin, still before expiry: stale token served.
	now = now.Add(time.Hour - time.Minute)
	tok, err := ts.get(context.Background())
	if err != nil || tok != "tok" {
		t.Fatalf("stale-serve: %q, %v", tok, err)
	}
	// Past expiry: the failure surfaces.
	now = now.Add(2 * time.Minute)
	if _, err := ts.get(context.Background()); err == nil {
		t.Fatal("an expired token with a failing refresh must error")
	}
}

func TestParseTokenResponseRejectsGarbage(t *testing.T) {
	for _, bad := range []string{``, `{}`, `{"access_token":""}`, `not json`} {
		if _, _, err := parseTokenResponse([]byte(bad), "test"); err == nil {
			t.Errorf("%q: want an error", bad)
		}
	}
	// Absent expires_in falls back to a sane default.
	tok, ttl, err := parseTokenResponse([]byte(`{"access_token":"t"}`), "test")
	if err != nil || tok != "t" || ttl != time.Hour {
		t.Fatalf("default ttl: %q %v %v", tok, ttl, err)
	}
}

func TestConnectionStringMechanismReadsPerSession(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cs")
	if err := os.WriteFile(path, []byte("Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKey=one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	mech := connectionStringMechanism(path)
	if mech.Name() != "PLAIN" {
		t.Fatalf("mechanism = %s", mech.Name())
	}
	_, first, err := mech.Authenticate(context.Background(), "host")
	if err != nil {
		t.Fatal(err)
	}
	// Rotate the secret; the next session must carry the new string.
	if err := os.WriteFile(path, []byte("Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKey=two"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, second, err := mech.Authenticate(context.Background(), "host")
	if err != nil {
		t.Fatal(err)
	}
	if string(first) == string(second) {
		t.Fatal("rotated connection string was not picked up")
	}
	if !bytes.Contains(first, []byte("$ConnectionString")) {
		t.Fatal("PLAIN payload lacks the $ConnectionString user")
	}
}
