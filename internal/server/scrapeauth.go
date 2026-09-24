package server

// The GET /v1/scrape-auth handler — the one route that serves Secret
// material. The bearer-token check it runs first, and why this route alone is
// authenticated, are auth.go.

import (
	"errors"
	"fmt"
	"net/http"
	"time"
	"unicode/utf8"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/validate/content"

	"github.com/JohanLindvall/kubescrape/internal/clip"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/peerip"
)

// scrapeAuthWarnEvery bounds how often one secret ref may log a resolution
// failure. An RBAC grant that was never added is a STEADY state, not an event:
// every agent on every node re-asks each scrape cycle, so an unthrottled line
// is a permanent flood proportional to fleet size. The counter carries the rate;
// the log only has to name the ref often enough to be found.
const scrapeAuthWarnEvery = 5 * time.Minute

// maxScrapeAuthWarnRefs bounds the throttle table. Keys come from the
// AuthSecretRefs allowlist, so they are already bounded by the indexed monitors
// — this is belt and braces against a monitor set that churns.
const maxScrapeAuthWarnRefs = 1024

// maxScrapeAuthDeniedRefs bounds the SEPARATE table the allowlist miss uses
// (see Server.warnAuthDenied). Same size, different blast radius: those keys
// are caller-chosen, so that is the table a mint may saturate and it must not
// be the one carrying the operator-facing failures.
const maxScrapeAuthDeniedRefs = 1024

// handleScrapeAuth serves GET /v1/scrape-auth/{namespace}/{name}/{key}: the
// bearer token a monitor endpoint's bearerTokenSecret references. Disabled
// (404) unless the service runs with -scrape-auth-secrets. Responses are
// never cacheable — a rotated token must not be re-served from a cache.
//
// This is the only AUTHENTICATED route: it is the only one serving Secret
// material, and the service holds cluster-wide `secrets: get`. Clients send
// the shared token from -scrape-auth-token-file as
// `Authorization: Bearer <token>`; anything else is a 401 (see auth.go).
func (s *Server) handleScrapeAuth(w http.ResponseWriter, r *http.Request) {
	// Set once, at the top, so EVERY exit inherits it. The 404s here are
	// heuristically storable (RFC 9111 4.2.2 over RFC 9110 15.1), and this
	// route's whole point is that a rotated or newly-granted credential takes
	// effect now — a cached "secret not found" from the startup window, or from
	// before an RBAC fix, outlives the condition that caused it and shows up
	// only as up=0. cachePolicy.noStore says the same thing for the pod routes;
	// this one was the exception.
	w.Header().Set("Cache-Control", "no-store")
	if s.secrets == nil {
		// The feature is off, so there is nothing to protect; keep the
		// pre-existing "not enabled" 404 rather than a misleading 401.
		//
		// Counted and warned, because this is a two-sided configuration
		// mismatch that nothing else reports: an agent only asks because a
		// monitor endpoint THIS SERVICE served it names a credential, so every
		// such scrape is about to run without one and sit at up=0 — and on the
		// agent the 404 is indistinguishable from "that ref does not exist".
		// The ref is not logged: it has not been through IsPathSegmentName yet
		// and the condition is a property of this process, not of the request.
		obs.ScrapeAuthFailures.WithLabelValues("disabled").Inc()
		if s.warnAuthOff.Allow(scrapeAuthWarnEvery) {
			s.log().Warn("a scrape-auth credential was requested but this service does not serve them; "+
				"every monitor endpoint declaring auth or TLS material will be scraped without it",
				"flag", "-scrape-auth-secrets",
				"note", "the agents were served monitor targets carrying secret refs, so enable "+
					"-scrape-auth-secrets (plus its secrets RBAC and -scrape-auth-token-file) or remove the "+
					"auth/TLS clauses from those monitors; further reports are suppressed for "+
					scrapeAuthWarnEvery.String())
		}
		writeError(w, http.StatusNotFound, "scrape auth secrets are not enabled (-scrape-auth-secrets)")
		return
	}
	// Authenticate BEFORE any lookup: an unauthenticated client must not be
	// able to probe which secret refs a monitor names (403 vs 404) either.
	if !s.authorizedForScrapeAuth(r) {
		// NEITHER the presented token NOR the Authorization header is ever
		// logged; what an operator has to fix is one agent's
		// -scrape-auth-token-file, so the line carries the peer address (the
		// only thing on the request that names that agent) and whether a
		// credential was presented at all. Those two cases have different
		// remedies: `missing` is an agent that was never given the flag,
		// `mismatch` is a token file that does not match this service's — the
		// shape a rotation gets wrong, which the service's own 5-minute grace
		// window is meant to cover.
		credential := "missing"
		if r.Header.Get("Authorization") != "" {
			credential = "mismatch"
		}
		obs.ScrapeAuthFailures.WithLabelValues("unauthorized").Inc()
		// Keyless: the condition is one misconfiguration, and a per-peer table
		// would be keyed by something that grows with the fleet. The counter
		// carries the rate; the line only has to name one example.
		if s.warnAuthToken.Allow(scrapeAuthWarnEvery) {
			s.log().Warn("scrape-auth request rejected: the caller did not present an accepted bearer token",
				"peer", peerip.From(r.RemoteAddr), "credential", credential,
				"tokenFile", "-scrape-auth-token-file",
				"note", "the agent's -scrape-auth-token-file must hold the same token as this service's; "+
					"a rotation is covered for five minutes on both sides, so a persistent rate is a "+
					"mismatch and not a rotation. Further reports are suppressed for "+scrapeAuthWarnEvery.String())
		}
		writeUnauthorized(w)
		return
	}
	ns, name, key := r.PathValue("namespace"), r.PathValue("name"), r.PathValue("key")
	// Scope to secrets a monitor endpoint actually references — the endpoint
	// must not become a read-any-cluster-secret oracle for anything that can
	// reach the (unauthenticated, cluster-internal) service.
	// The readiness gate sits AFTER the disabled-feature 404 and the anonymous
	// 401 (so neither changes), and BEFORE the allowlist — this is the route
	// whose missing gate is requireReady's war story. Retry-After: 1 because
	// unlike the metadata routes this 503 replaced a DEFINITIVE-looking 403,
	// so it says out loud when to come back.
	if !s.requireReady(w, "1") {
		return
	}
	if s.monitors == nil {
		// -scrape-auth-secrets without -servicemonitors: the allowlist that
		// bounds this route is built from indexed monitors, so nothing can
		// ever be served and every credential-bearing scrape 401s. Same shape
		// as the disabled case above, and just as silent before this.
		obs.ScrapeAuthFailures.WithLabelValues("no_monitors").Inc()
		if s.warnAuthNoMonitors.Allow(scrapeAuthWarnEvery) {
			s.log().Warn("a scrape-auth credential was requested but no monitors are indexed, so no secret ref "+
				"can be allowlisted",
				"flag", "-servicemonitors",
				"note", "-scrape-auth-secrets serves only Secret keys an indexed ServiceMonitor/PodMonitor "+
					"endpoint references; further reports are suppressed for "+scrapeAuthWarnEvery.String())
		}
		writeError(w, http.StatusNotFound, "no monitors indexed")
		return
	}
	// The allowlist key is a flat "ns/name/key" join checked against three
	// SEPARATELY-CHOSEN path segments, and Go's ServeMux unescapes %2F inside a
	// single wildcard segment — so without this, three segments could be
	// re-cut: GET /v1/scrape-auth/tenant%2Fvictim/creds/token matches the entry
	// a monitor in namespace `tenant` mints for a bearerTokenSecret named
	// "victim/creds", and reaches SecretReader.Get with namespace
	// "tenant/victim". The shipped client-go reader rejects that namespace
	// before it sends anything, but Secrets is a pluggable interface and a
	// reader implemented over a lister keyed by "ns/name" — the obvious
	// optimisation for a per-scrape-cycle path — would perform the read.
	//
	// So both ends refuse the ambiguity: servicemonitors' secretRef.ref
	// declines to MINT such an entry, and this declines to match one. The check
	// is the API server's own for a name used as a path segment (content.
	// IsPathSegmentName, which validation/path now merely aliases): "/", "%",
	// "." and ".." are exactly what re-cutting needs.
	// In PATH order, never off a map: a request with two bad segments must be
	// refused for the same one every time, so the 400 body an operator reads
	// (and a test of it) names one segment rather than whichever the map
	// yielded first.
	for _, seg := range [...]struct{ what, v string }{{"namespace", ns}, {"name", name}, {"key", key}} {
		what, v := seg.what, seg.v
		if errs := content.IsPathSegmentName(v); len(errs) > 0 {
			// Counted with the other refusals: no agent this repo ships can
			// produce one (the ref comes from a monitor CR, whose fields are
			// already Kubernetes names), so a rate here is either a hand-built
			// request or the re-cutting attack this check exists for. The
			// value is CLIPPED before it reaches the log — it is a raw path
			// segment, bounded only by the header limit.
			obs.ScrapeAuthFailures.WithLabelValues("bad_request").Inc()
			if s.warnAuthSegment.Allow(scrapeAuthWarnEvery) {
				s.log().Warn("scrape-auth request rejected: a path segment cannot name a Kubernetes object",
					"segment", what, "value", clipSegment(v), "error", errs[0],
					"note", "further reports are suppressed for "+scrapeAuthWarnEvery.String())
			}
			writeError(w, http.StatusBadRequest,
				fmt.Sprintf("invalid %s %q: %s", what, v, errs[0]))
			return
		}
	}
	if ref := ns + "/" + name + "/" + key; !s.monitors.AuthSecretRefs().Has(ref) {
		// The allowlist is derived from the INDEXED monitors, so a miss is
		// usually not a hostile probe but the index disagreeing with what the
		// agent was served: the monitor failed to parse (and was therefore
		// DELETED from the index, dropping its targets with it), its namespace
		// is outside -monitor-namespaces, or the ref really is a typo. All
		// three end the same way — the scrape runs unauthenticated — and none
		// of them was visible here.
		//
		// Per-ref, because two broken credentials must not mask each other —
		// but through the miss's OWN table, and under a CLIPPED key. These
		// three segments are the caller's, not the operators' configuration:
		// they have passed IsPathSegmentName by now and nothing bounds their
		// length or their number, so keying the shared warnRefs table by them
		// handed anyone holding the scrape-auth token a way to saturate it and
		// suppress the RBAC-failure and non-UTF-8 warnings for every real ref
		// (see Server.warnAuthDenied).
		obs.ScrapeAuthFailures.WithLabelValues("not_allowed").Inc()
		if s.allowKeyed(s.warnAuthDenied, clipSegment(ns)+"\x00"+clipSegment(name)+"\x00"+clipSegment(key),
			"scrape-auth allowlist-miss", "refs", maxScrapeAuthDeniedRefs,
			"note", "the rate stays on kubescrape_scrape_auth_failures_total") {
			s.log().Warn("scrape-auth request refused: no indexed monitor endpoint references this secret key",
				"namespace", clipSegment(ns), "name", clipSegment(name), "key", clipSegment(key),
				"note", "check that the monitor naming it parsed (kubescrape_monitor_parse_errors_total) and "+
					"that its namespace is permitted by -monitor-namespaces; the target is scraped without the "+
					"credential meanwhile. Further failures for this ref are suppressed for "+
					scrapeAuthWarnEvery.String())
		}
		writeError(w, http.StatusForbidden, "secret is not referenced by any monitor endpoint")
		return
	}
	val, err := s.secrets.Get(r.Context(), ns, name, key)
	if err != nil {
		// Classify. This is the one route that hard-fails on EXTERNAL state, so
		// collapsing every cause into 404 made an RBAC denial — the likeliest
		// real failure, since -scrape-auth-secrets needs a `secrets get` grant
		// the operator adds by hand — read as "no such secret", and put a
		// permissions bug into the metadata_requests_total{outcome="not_found"}
		// stream that obs documents as the container-attribution signal.
		//
		// A missing key or a genuinely absent Secret is the client's 404;
		// anything else (forbidden, timeout, apiserver down) is ours, and is
		// retryable.
		status, reason := http.StatusBadGateway, "upstream"
		if apierrors.IsNotFound(err) || errors.Is(err, ErrSecretKeyNotFound) {
			status, reason = http.StatusNotFound, "not_found"
		}
		if status != http.StatusNotFound {
			// The service is uniquely positioned to explain this one: the agent
			// sees only the status code. Log it, and count it apart from the
			// client-caused misses.
			//
			// THROTTLED, because this is the steady state of the failure the
			// route's own doc calls the likeliest: an RBAC grant that was never
			// added means every agent on every node re-asks each scrape cycle
			// and each one would log a line, forever. The counter is the
			// alerting signal; the log only has to say it once in a while, and
			// per REF so a second broken credential is not masked by the first.
			if s.allowKeyed(s.warnRefs, "read:"+ns+"/"+name+"/"+key, "scrape-auth", "refs", maxScrapeAuthWarnRefs) {
				s.log().Warn("resolving scrape-auth secret",
					"namespace", ns, "name", name, "key", key, "error", err,
					"note", "further failures for this ref are suppressed for "+scrapeAuthWarnEvery.String())
			}
			w.Header().Set("Retry-After", "5")
		}
		obs.ScrapeAuthFailures.WithLabelValues(reason).Inc()
		writeError(w, status, fmt.Sprintf("secret %s/%s key %s: %v", ns, name, key, err))
		return
	}
	// The value is about to be marshalled into a JSON string. encoding/json
	// replaces every invalid UTF-8 byte with U+FFFD and reports no error, so a
	// credential created from raw bytes (kubectl create secret
	// --from-file=password=<binary>) would reach the agent silently corrupted,
	// with a 200 and up=0 as the only evidence. Refuse loudly instead: the
	// alternative — base64 on the wire — is a format change every deployed
	// agent would have to learn in lockstep.
	if !utf8.ValidString(val) {
		obs.ScrapeAuthFailures.WithLabelValues("not_utf8").Inc()
		// Throttled through the same table as the failure above, but under its
		// OWN key prefix: sharing the bare ref let a class transition (an RBAC
		// gap fixed, revealing a non-UTF-8 value) be swallowed for the whole
		// window, reporting neither condition.
		if s.allowKeyed(s.warnRefs, "utf8:"+ns+"/"+name+"/"+key, "scrape-auth", "refs", maxScrapeAuthWarnRefs) {
			s.log().Warn("scrape-auth secret value is not valid UTF-8 and cannot be served as JSON",
				"namespace", ns, "name", name, "key", key,
				"note", "further failures for this ref are suppressed for "+scrapeAuthWarnEvery.String())
		}
		writeError(w, http.StatusUnprocessableEntity, fmt.Sprintf(
			"secret %s/%s key %s is not valid UTF-8; kubescrape serves credentials as JSON strings",
			ns, name, key))
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]string{"value": val})
}

// clipSegment bounds a caller-supplied path segment before it reaches a log
// line. A namespace, a Secret name and a Secret key are all DNS-subdomain-ish
// in practice (253 bytes at most), but nothing on the wire enforces that here:
// the segment arrives from a URL path bounded only by the request-head limit,
// and a log line is the one place a 16 KB value costs something in every
// direction at once. The truncation is marked, so a clipped value is never
// mistaken for the whole one, and made on a rune boundary (internal/clip): the
// segment is the caller's bytes, and a bare byte cut put half a rune on the
// line.
func clipSegment(v string) string {
	const maxLoggedSegmentBytes = 253
	return clip.Marked(v, maxLoggedSegmentBytes, "…(truncated)")
}
