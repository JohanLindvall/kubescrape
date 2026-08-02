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
	"net/http"

	"github.com/JohanLindvall/kubescrape/internal/bearer"
)

// scrapeAuthRealm is sent on 401s so a client can tell "wrong credentials"
// from "wrong URL".
const scrapeAuthRealm = `Bearer realm="kubescrape scrape-auth"`

// authorizedForScrapeAuth reports whether r carries one of the configured
// scrape-auth bearer tokens (more than one only during a rotation grace
// window — the file's previous value stays valid briefly so agents and the
// service never have to flip in lockstep).
//
// Fail-closed: with no token source configured NOTHING is authorized. The
// startup path (Config.Validate) refuses that combination outright, so this
// is the second line of defense for an embedder building a Server directly —
// an endpoint that serves Secrets must never be reachable because a flag was
// forgotten.
//
// The comparison itself lives in internal/bearer (constant time, every
// candidate compared with no early exit, an empty candidate never authorizing)
// — the same routine the trace tier's internal listener uses, so this repo has
// one bearer-auth model rather than two byte-identical ones that can drift.
func (s *Server) authorizedForScrapeAuth(r *http.Request) bool {
	if s.scrapeAuthTokens == nil {
		return false
	}
	return bearer.Authorized(r.Header.Get("Authorization"), s.scrapeAuthTokens())
}

// writeUnauthorized rejects an unauthenticated scrape-auth request. The body
// never echoes the presented token or says whether the referenced secret
// exists.
func writeUnauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", scrapeAuthRealm)
	w.Header().Set("Cache-Control", "no-store")
	writeError(w, http.StatusUnauthorized, "missing or invalid bearer token (-scrape-auth-token-file)")
}
