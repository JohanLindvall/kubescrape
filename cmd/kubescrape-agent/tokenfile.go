package main

import (
	"crypto/subtle"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
)

// tokenReadInterval bounds how often a token file is re-read. Kubernetes
// projects rotated Secret contents into the mounted file, so the value must be
// re-read rather than captured once — but not on every request.
const tokenReadInterval = time.Minute

// tokenFile serves a bearer token from a mounted file, re-reading it at most
// once per interval. A failed re-read keeps serving the last good value: a
// transient read error during a ConfigMap/Secret swap must not turn every
// subsequent request into an authentication failure.
type tokenFile struct {
	path string
	log  *slog.Logger

	mu      sync.Mutex
	token   string
	fetched time.Time
}

func newTokenFile(path string, log *slog.Logger) *tokenFile {
	return &tokenFile{path: path, log: log}
}

func (t *tokenFile) read() (string, error) {
	data, err := os.ReadFile(t.path)
	if err != nil {
		return "", err
	}
	tok := strings.TrimSpace(string(data))
	if tok == "" {
		return "", fmt.Errorf("token file %s is empty", t.path)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.token = tok
	t.fetched = time.Now()
	return tok, nil
}

// get returns the current token, refreshing it when the cached value is stale.
func (t *tokenFile) get() string {
	t.mu.Lock()
	tok, fresh := t.token, time.Since(t.fetched) < tokenReadInterval
	t.mu.Unlock()
	if fresh {
		return tok
	}
	next, err := t.read()
	if err != nil {
		// Keep the last good token; the caller's request may still succeed.
		t.log.Warn("re-reading token file", "path", t.path, "error", err)
		return tok
	}
	return next
}

// --- verifying a presented token (the service-graph shard's receiver) ---
//
// tokenFile above is the CLIENT half: the token this process presents. The
// rest of this file is the SERVER half, and it is the metadata service's
// /v1/scrape-auth contract reproduced (cmd/kubescrape/main.go's rotatingToken
// plus internal/server/auth.go): per-minute re-read, the previous value
// accepted for a grace window after a rotation, constant-time comparison
// against every candidate. Reproduced rather than shared because both live in
// package main of their own binary — but deliberately identical, so an
// operator rotating either Secret does the same thing and gets the same
// behaviour, and there is one auth model in this repo rather than two.

const (
	// tokenGrace keeps the PREVIOUS token accepted after a rotation. The two
	// sides re-read their copy of the file on independent per-minute cadences,
	// so without a grace window a rotated Secret would 401 every agent that had
	// not re-read yet — a fleet-wide gap in the graph for no reason. With it,
	// rotation is a non-event: update the Secret and both sides converge.
	tokenGrace = 5 * time.Minute
)

// rotatingToken serves the token set a receiver ACCEPTS: the current file
// contents plus, for tokenGrace after a change, its predecessor.
type rotatingToken struct {
	path string
	log  *slog.Logger

	mu        sync.Mutex
	cur, prev string
	prevUntil time.Time
	fetched   time.Time
}

// newRotatingToken reads the token file once, fatally.
//
// Every failure mode here is fatal for the same reason the metadata service's
// is: the caller is about to open a listener that is reachable from every pod
// in the cluster, and "no file", "unreadable" and "empty" must all stop the
// process rather than leave it running with an empty accept set. (An empty
// candidate can never authorize either — see authorizedBearer — so the failure
// would be a receiver that refuses everything, which is only marginally better
// than one that accepts everything.)
func newRotatingToken(path string, log *slog.Logger) (*rotatingToken, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("no token file configured")
	}
	r := &rotatingToken{path: path, log: log}
	tok, err := readTokenFile(path)
	if err != nil {
		return nil, err
	}
	r.cur, r.fetched = tok, time.Now()
	return r, nil
}

// tokens returns the accepted set, re-reading the file when stale. A failed or
// empty re-read keeps the last good value: a transient error during a Secret
// swap must not 401 the whole fleet.
func (r *rotatingToken) tokens() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if time.Since(r.fetched) >= tokenReadInterval {
		r.fetched = time.Now()
		if next, err := readTokenFile(r.path); err != nil {
			r.log.Warn("re-reading the service-graph token file; keeping the last good token", "path", r.path, "error", err)
		} else if next != r.cur {
			r.prev, r.prevUntil = r.cur, time.Now().Add(tokenGrace)
			r.cur = next
			r.log.Info("service-graph token rotated; the previous token stays accepted for the grace window", "grace", tokenGrace)
		}
	}
	if r.prev != "" && time.Now().Before(r.prevUntil) {
		return []string{r.cur, r.prev}
	}
	return []string{r.cur}
}

// readTokenFile reads and trims a token file. Trimming matters: a Secret
// mounted from `echo` or a here-doc carries a trailing newline, and every
// client sends the trimmed value in the header.
func readTokenFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	tok := strings.TrimSpace(string(b))
	if tok == "" {
		return "", fmt.Errorf("token file %s is empty", path)
	}
	return tok, nil
}

// authorizedBearer reports whether an `Authorization: Bearer <token>` header
// carries one of the accepted tokens.
//
// Constant time (crypto/subtle), and EVERY candidate is compared with no early
// exit: a byte-at-a-time compare leaks the shared token to anyone who can time
// responses, and an early exit would leak which of the rotation pair matched.
// This is internal/server/authorizedForScrapeAuth, applied to the one other
// listener in this repo that a shared token guards.
func authorizedBearer(header string, tokens []string) bool {
	got, ok := bearerToken(header)
	if !ok {
		return false
	}
	matched := 0
	for _, tok := range tokens {
		if tok == "" {
			continue // an empty candidate must never authorize
		}
		matched |= subtle.ConstantTimeCompare([]byte(got), []byte(tok))
	}
	return matched == 1
}

// bearerToken extracts the credentials of an `Authorization: Bearer <token>`
// header. The scheme is matched case-insensitively (RFC 9110 §11.1); the token
// is returned verbatim.
func bearerToken(header string) (string, bool) {
	scheme, token, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "bearer") {
		return "", false
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return "", false
	}
	return token, true
}
