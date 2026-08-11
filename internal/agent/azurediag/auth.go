package azurediag

// Event Hubs authentication over the Kafka surface, two ways:
//
//   - a CONNECTION STRING (SASL PLAIN, user "$ConnectionString", password =
//     the whole string) — the "plain" path, read from a mounted secret file
//     per SASL session so a rotated secret applies without a restart;
//   - MANAGED IDENTITY (SASL OAUTHBEARER): a Microsoft Entra token for the
//     namespace, via AKS workload identity (the federated-token exchange —
//     the projected token file plus AZURE_CLIENT_ID/AZURE_TENANT_ID env the
//     webhook injects) when configured, else IMDS.
//
// Both token protocols are two small, stable HTTP exchanges, implemented
// directly rather than through the Azure SDK — the same trade as rejecting
// OAuth2/SigV4 SDKs in otlpexport: a heavyweight dependency for two POSTs.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"

	ljson "github.com/JohanLindvall/lightning/pkg/json"

	"github.com/JohanLindvall/kubescrape/internal/agent/logchain"
	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

const (
	defaultAuthority = "https://login.microsoftonline.com"
	imdsEndpoint     = "http://169.254.169.254"
	// refreshMargin is how long before expiry a cached token is renewed;
	// Entra tokens live ~1h, so refreshes are rare and never on the deadline.
	refreshMargin = 5 * time.Minute
)

// connectionStringMechanism authenticates with the Event Hubs connection
// string: SASL PLAIN, the string itself as the password. The file is re-read
// per session, so rotation needs no restart (the bearer-token-file pattern).
//
// The string is sent VERBATIM, including an `EntityPath=` on an entity-scoped
// one. Do not be tempted to strip that: a SAS rule's scope is the resource it
// was created on, never a token in the string, so stripping cannot widen
// access — while an entity-level rule NAME is only unique within its entity,
// which is plausibly what EntityPath lets the service resolve. Stripping
// therefore risks breaking authentication to buy nothing. (Microsoft's .NET
// Kafka quickstart claims an entity-level string "will not work" at all, but
// that text is from 2018, was posted alongside a promise to enable it, and
// has never been revisited; entity-scoped strings are shown working over the
// Kafka surface by several vendors since. If one is genuinely refused, the
// answer is a NAMESPACE-level policy, not a doctored string.)
func connectionStringMechanism(path string) sasl.Mechanism {
	return plain.Plain(func(context.Context) (plain.Auth, error) {
		b, err := os.ReadFile(path)
		if err != nil {
			return plain.Auth{}, fmt.Errorf("reading the event hubs connection string: %w", err)
		}
		return plain.Auth{User: "$ConnectionString", Pass: strings.TrimSpace(string(b))}, nil
	})
}

// connString is the parsed form of an Event Hubs connection string. Only the
// two fields this consumer routes on are kept — the CREDENTIAL halves
// (SharedAccessKeyName/SharedAccessKey) are deliberately never extracted:
// the whole string is the SASL password, so pulling the secret into a struct
// field would only add places for it to be logged from.
type connString struct {
	// Namespace is the host from Endpoint=sb://host/ — "" when absent.
	Namespace string
	// EntityPath is the single hub an ENTITY-SCOPED connection string names
	// (the shape you get by copying a shared access policy from one Event Hub
	// rather than from the namespace). "" for a namespace-scoped string.
	EntityPath string
}

// parseConnectionString splits the `Key=Value;...` form.
//
// Each part is cut at its FIRST '=' — not split on every one. That is free
// insurance rather than a fix for a live bug: the two fields read here
// (Endpoint, EntityPath) carry no '=' in their values, so a naive split would
// behave identically today. It matters the moment anything reads a field that
// DOES — a base64 SharedAccessKey ends in '=' padding, a SharedAccessSignature
// is full of them — which is why the cut is here rather than left as a trap
// for whoever adds the third field. (Reading the credential is deliberately
// not done at all; see connectionStringMechanism.) Keys are matched
// case-insensitively, as the Azure SDKs' own parsers do.
func parseConnectionString(cs string) connString {
	var out connString
	for _, part := range strings.Split(cs, ";") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		v = strings.TrimSpace(v)
		switch strings.ToLower(strings.TrimSpace(k)) {
		case "endpoint":
			v = strings.TrimPrefix(v, "sb://")
			v = strings.TrimPrefix(v, "amqps://")
			out.Namespace = strings.Trim(v, "/")
		case "entitypath":
			out.EntityPath = strings.Trim(v, "/")
		}
	}
	return out
}

// tokenSource caches one Entra token and refreshes it ahead of expiry. get
// is called from franz-go's per-connection SASL sessions, hence the mutex.
type tokenSource struct {
	mu    sync.Mutex
	fetch func(ctx context.Context) (string, time.Duration, error)
	now   func() time.Time

	token  string
	expiry time.Time
}

func (t *tokenSource) get(ctx context.Context) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.now == nil {
		t.now = time.Now
	}
	now := t.now()
	if t.token != "" && now.Before(t.expiry.Add(-refreshMargin)) {
		return t.token, nil
	}
	tok, ttl, err := t.fetch(ctx)
	if err != nil {
		obs.AzureTokenRefreshes.WithLabelValues("error").Inc()
		if t.token != "" && now.Before(t.expiry) {
			// Inside the refresh margin but not yet expired: the stale token
			// still works; a transient token-endpoint blip must not fail
			// every new Kafka connection.
			return t.token, nil
		}
		return "", err
	}
	obs.AzureTokenRefreshes.WithLabelValues("ok").Inc()
	t.token, t.expiry = tok, now.Add(ttl)
	return t.token, nil
}

// mechanism wraps the source as SASL OAUTHBEARER.
func (t *tokenSource) mechanism() sasl.Mechanism {
	return oauth.Oauth(func(ctx context.Context) (oauth.Auth, error) {
		tok, err := t.get(ctx)
		if err != nil {
			return oauth.Auth{}, err
		}
		return oauth.Auth{Token: tok}, nil
	})
}

// managedIdentitySource picks the token protocol: workload identity when the
// AKS webhook's federation env is present (or flags supply it), else IMDS.
// namespaceHost scopes the token to the Event Hubs namespace.
func managedIdentitySource(namespaceHost, clientID, tenantID string, hc *http.Client) *tokenSource {
	if clientID == "" {
		clientID = os.Getenv("AZURE_CLIENT_ID")
	}
	if tenantID == "" {
		tenantID = os.Getenv("AZURE_TENANT_ID")
	}
	tokenFile := os.Getenv("AZURE_FEDERATED_TOKEN_FILE")
	authority := os.Getenv("AZURE_AUTHORITY_HOST")
	if authority == "" {
		authority = defaultAuthority
	}
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Second}
	}
	var fetch func(ctx context.Context) (string, time.Duration, error)
	if tokenFile != "" && tenantID != "" && clientID != "" {
		fetch = workloadIdentityFetch(authority, tenantID, clientID, tokenFile, "https://"+namespaceHost+"/.default", hc)
	} else {
		fetch = imdsFetch(imdsEndpoint, "https://"+namespaceHost, clientID, hc)
	}
	return &tokenSource{fetch: fetch}
}

// workloadIdentityFetch exchanges the projected federated token for an Entra
// access token (client_credentials with a client_assertion). The token file
// is read per fetch — kubelet rotates it.
func workloadIdentityFetch(authority, tenant, clientID, tokenFile, scope string, hc *http.Client) func(ctx context.Context) (string, time.Duration, error) {
	return func(ctx context.Context) (string, time.Duration, error) {
		assertion, err := os.ReadFile(tokenFile)
		if err != nil {
			return "", 0, fmt.Errorf("reading the federated token: %w", err)
		}
		form := url.Values{
			"grant_type":            {"client_credentials"},
			"client_id":             {clientID},
			"scope":                 {scope},
			"client_assertion_type": {"urn:ietf:params:oauth:client-assertion-type:jwt-bearer"},
			"client_assertion":      {strings.TrimSpace(string(assertion))},
		}
		endpoint := strings.TrimSuffix(authority, "/") + "/" + tenant + "/oauth2/v2.0/token"
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
		if err != nil {
			return "", 0, err
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		return roundTripToken(hc, req, "entra token exchange")
	}
}

// imdsFetch asks the instance metadata service for a managed-identity token.
// clientID selects a user-assigned identity; empty uses the system-assigned.
func imdsFetch(endpoint, resource, clientID string, hc *http.Client) func(ctx context.Context) (string, time.Duration, error) {
	return func(ctx context.Context) (string, time.Duration, error) {
		q := url.Values{"api-version": {"2018-02-01"}, "resource": {resource}}
		if clientID != "" {
			q.Set("client_id", clientID)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/metadata/identity/oauth2/token?"+q.Encode(), nil)
		if err != nil {
			return "", 0, err
		}
		req.Header.Set("Metadata", "true")
		return roundTripToken(hc, req, "imds token")
	}
}

func roundTripToken(hc *http.Client, req *http.Request, what string) (string, time.Duration, error) {
	resp, err := hc.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("%s: %w", what, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", 0, fmt.Errorf("%s: %w", what, err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("%s: HTTP %d: %s", what, resp.StatusCode, truncate(string(body), 200))
	}
	return parseTokenResponse(body, what)
}

// parseTokenResponse pulls access_token and expires_in out of the response.
// IMDS returns expires_in as a STRING ("3599"), the token endpoint as a
// number; both are handled.
func parseTokenResponse(body []byte, what string) (string, time.Duration, error) {
	raws, err := ljson.GetMany(body, []string{"access_token", "expires_in"}, nil)
	if err != nil || len(raws) < 2 || len(raws[0]) == 0 {
		return "", 0, fmt.Errorf("%s: response carries no access_token", what)
	}
	tok, ok := logline.RawScalarString(raws[0])
	if !ok || tok == "" {
		return "", 0, fmt.Errorf("%s: malformed access_token", what)
	}
	ttl := time.Hour // a sane default when expires_in is absent
	if len(raws[1]) > 0 {
		if s, ok := logline.RawScalarString(raws[1]); ok {
			if secs, err := strconv.ParseFloat(s, 64); err == nil && secs > 0 {
				ttl = time.Duration(secs * float64(time.Second))
			}
		}
	}
	return tok, ttl, nil
}

// truncate caps an HTTP body copied into an error, cutting on a rune
// boundary (a bare s[:n] could split a multibyte rune and put invalid UTF-8
// into the error string).
func truncate(s string, n int) string {
	if len(s) > n {
		return logchain.TruncateRunes(s, n) + "…"
	}
	return s
}
