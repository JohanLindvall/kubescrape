package server

// Bearer-token authentication for GET /v1/scrape-auth — the ONE route that
// serves Secret material.
//
// The rest of the v1 API is deliberately unauthenticated: agents poll it for
// pod/container metadata and scrape targets, none of which is secret. The
// scrape-auth route is different in kind — the service holds cluster-wide
// `secrets: get`, so an unauthenticated endpoint there hands every Secret key
// any monitor endpoint references to anything that can open a TCP connection
// to the service. Hence a shared token: cheap, stateless across replicas
// (every replica reads the same file), and no dependency on cluster-internal
// mTLS or a NetworkPolicy being in place.

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// scrapeAuthRealm is sent on 401s so a client can tell "wrong credentials"
// from "wrong URL".
const scrapeAuthRealm = `Bearer realm="kubescrape scrape-auth"`

// authorizedForScrapeAuth reports whether r carries the configured
// scrape-auth bearer token.
//
// Fail-closed: with no token configured NOTHING is authorized. The startup
// path (Config.Validate) refuses that combination outright, so this is the
// second line of defense for an embedder building a Server directly — an
// endpoint that serves Secrets must never be reachable because a flag was
// forgotten.
//
// The comparison is constant time (crypto/subtle): a byte-at-a-time compare
// leaks the shared token to anyone who can time responses, and this endpoint
// is reachable from every pod in the cluster. subtle.ConstantTimeCompare also
// returns 0 for differing lengths, which leaks only the token's length —
// unavoidable without hashing and not worth the extra machinery here.
func (s *Server) authorizedForScrapeAuth(r *http.Request) bool {
	if s.scrapeAuthToken == "" {
		return false
	}
	got, ok := bearerToken(r.Header.Get("Authorization"))
	if !ok {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(s.scrapeAuthToken)) == 1
}

// bearerToken extracts the credentials of an `Authorization: Bearer <token>`
// header. The scheme is matched case-insensitively (RFC 9110 §11.1); the
// token itself is returned verbatim.
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

// writeUnauthorized rejects an unauthenticated scrape-auth request. The body
// never echoes the presented token or says whether the referenced secret
// exists.
func writeUnauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", scrapeAuthRealm)
	w.Header().Set("Cache-Control", "no-store")
	writeError(w, http.StatusUnauthorized, "missing or invalid bearer token (-scrape-auth-token-file)")
}
