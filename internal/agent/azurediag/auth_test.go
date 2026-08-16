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

// parseConnectionString handles both the namespace-scoped and the
// ENTITY-scoped shapes, over realistic strings — including a base64
// SharedAccessKey ending in '=' padding, which sits BETWEEN the two fields
// that are read and so must not disturb them.
//
// Note what this does NOT pin: swapping the first-'=' cut for a naive
// split-on-every-'=' leaves every case here passing, because neither Endpoint
// nor EntityPath has a '=' in its value. That is a property of what is read,
// not of the parse (see parseConnectionString) — do not read this test as
// proof the cut is load-bearing.
func TestParseConnectionString(t *testing.T) {
	// Never put a real key in a test file: obviously fake, but the right shape.
	const key = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="

	for _, tc := range []struct {
		name           string
		in             string
		wantNS, wantEP string
	}{
		{
			name:   "entity scoped",
			in:     "Endpoint=sb://mydiag-we-0a1b2c3d.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=" + key + ";EntityPath=azure",
			wantNS: "mydiag-we-0a1b2c3d.servicebus.windows.net",
			wantEP: "azure",
		},
		{
			name:   "namespace scoped",
			in:     "Endpoint=sb://myns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=" + key,
			wantNS: "myns.servicebus.windows.net",
		},
		{
			name:   "keys are case insensitive and space tolerant",
			in:     " endpoint=sb://myns.servicebus.windows.net/ ; entitypath=hub-1 ",
			wantNS: "myns.servicebus.windows.net",
			wantEP: "hub-1",
		},
		{
			name:   "endpoint without a trailing slash or scheme prefix",
			in:     "Endpoint=myns.servicebus.windows.net;EntityPath=h",
			wantNS: "myns.servicebus.windows.net",
			wantEP: "h",
		},
		{
			name: "no endpoint at all",
			in:   "SharedAccessKeyName=test;SharedAccessKey=" + key,
		},
		{name: "empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := parseConnectionString(tc.in)
			if got.Namespace != tc.wantNS {
				t.Errorf("Namespace = %q, want %q", got.Namespace, tc.wantNS)
			}
			if got.EntityPath != tc.wantEP {
				t.Errorf("EntityPath = %q, want %q", got.EntityPath, tc.wantEP)
			}
		})
	}
}

// The SASL password is the connection string VERBATIM, EntityPath and all:
// the operator's credential goes on the wire exactly as issued. Rewriting a
// secret in flight would make a failed authentication a puzzle whose answer
// is in our code rather than in their portal.
func TestConnectionStringPasswordIsVerbatim(t *testing.T) {
	const cs = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=AAAA=;EntityPath=azure"
	path := filepath.Join(t.TempDir(), "cs")
	if err := os.WriteFile(path, []byte(cs+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, payload, err := connectionStringMechanism(path).Authenticate(context.Background(), "host")
	if err != nil {
		t.Fatal(err)
	}
	// SASL PLAIN is \x00user\x00pass.
	parts := bytes.Split(payload, []byte{0})
	if len(parts) != 3 || string(parts[1]) != "$ConnectionString" {
		t.Fatalf("unexpected PLAIN payload shape: %q", payload)
	}
	if string(parts[2]) != cs {
		t.Fatalf("password = %q, want the connection string verbatim %q", parts[2], cs)
	}
}

// A non-finite expires_in ("Inf"/"+Inf") must not become a negative TTL (which
// would force a token refresh on every connection). Regression.
func TestParseTokenResponseRejectsNonFiniteExpiry(t *testing.T) {
	for _, s := range []string{`"Inf"`, `"+Inf"`, `"Infinity"`, `"NaN"`} {
		_, ttl, err := parseTokenResponse([]byte(`{"access_token":"t","expires_in":`+s+`}`), "test")
		if err != nil {
			t.Fatalf("expires_in=%s errored: %v", s, err)
		}
		if ttl <= 0 {
			t.Errorf("expires_in=%s gave ttl=%v; want the positive default (a non-finite value must not land in the past)", s, ttl)
		}
	}
	// A normal numeric value is still honoured.
	if _, ttl, _ := parseTokenResponse([]byte(`{"access_token":"t","expires_in":"3599"}`), "test"); ttl != 3599*time.Second {
		t.Errorf("numeric expires_in ttl = %v", ttl)
	}
}
