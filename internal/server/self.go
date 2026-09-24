package server

// GET /v1/self: the pod-IP lookup applied to the caller's own connection.

import (
	"fmt"
	"net/http"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/peerip"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// handleSelf serves GET /v1/self: full metadata for the pod the CALLER runs
// in, attributed by the connection's source address. It exists so an agent can
// stamp its own pod's Kubernetes attributes onto the telemetry it generates
// about ITSELF (self-metrics, span metrics) without a downward-API env var
// wired into every deployment that runs the binary.
//
// The address comes from the connection (r.RemoteAddr) and NEVER from a
// header for the LOOKUP: X-Forwarded-For is caller-controlled, and this
// endpoint hands out whatever pod owns the address it is given. It resolves
// through the same live-only pod-IP index as /v1/pod-ips, so a caller on
// hostNetwork (sharing the node IP), one behind SNAT, and one from outside the
// cluster all get a 404 rather than someone else's identity.
//
// WHAT THIS ROUTE CANNOT DO, stated plainly because the answer is an identity:
// a hop that RE-ORIGINATES the request is itself usually a pod, and its address
// is a perfectly good pod IP, so the answer names THE HOP. The connection is
// all there is — nothing distinguishes "this address is my caller" from "this
// address is a proxy in front of my caller" — and the corroboration the agent
// does on its side (selfmeta.verified, pod name against hostname) needs
// something the caller would have to already know, which is the very thing
// this route exists to supply.
//
// So the one hop that CAN be recognised is refused: a request carrying a
// forwarding header (Forwarded, Via, X-Forwarded-For, X-Real-Ip —
// peerip.ForwardingHeader owns the list, shared with the agent's debug guard,
// and Via is the one RFC 9110 REQUIRES a proxy to add) says, in the hop's own words, that the connection is not the caller's.
// The header is still never READ for an address — it selects nothing and names
// nobody, so no caller can use one to be told about somebody else; its mere
// PRESENCE is the whole effect, empty value included, and the refusal it
// produces is one a caller can only inflict on itself.
//
// THAT COSTS SOMETHING, and it is paid by a deployment that works today: a
// forwarding header is evidence of a HOP, not evidence of address REWRITING,
// and a sidecar in the caller's OWN network namespace — a mesh proxy on the
// agent's pod — appends X-Forwarded-For while leaving the source address
// exactly as it was. Such a caller was answered 200, correctly, and is now
// answered 404. The trade is taken because the two outcomes are not symmetric.
// The 404 is loud and recoverable: the agent falls back to a lookup BY NAME and
// ends up with the SAME pod, at one extra request per -self-attributes-refresh
// and a visible kubescrape_self_metadata_lookups_total{outcome="by_name"}. The
// answer it prevents is silent and permanent: a proxy pod's name, uid and
// namespace stamped on every self-metric the caller ever exports, 200, cached,
// nothing counted anywhere. The fallback is not ASSUMED to cover the sidecar —
// cmd/kubescrape-agent's TestSelfResolveSurvivesASidecarThatAppendsForwardedFor
// drives this handler with that header through a real store and requires the
// agent to come out holding its own identity.
//
// A hop that adds no header remains indistinguishable from the caller, and
// TestASilentReOriginatingHopIsAnsweredWithTheHopsOwnIdentity pins that limit
// rather than leaving it to be rediscovered as a bug.
//
// The response is cached like any other metadata 200, but PRIVATE: it names
// the caller, so only a per-client cache may hold it. That is what lets a
// caller re-read its own pod cheaply — the poll becomes a conditional GET, and
// a 304 says "your labels and namespace metadata are unchanged" — instead of
// choosing between stale attributes and a full document every interval.
func (s *Server) handleSelf(w http.ResponseWriter, r *http.Request) {
	ip := peerip.From(r.RemoteAddr)
	if ip == "" {
		// net/http builds RemoteAddr from the accepted connection, so this is
		// a "cannot happen" branch — which is exactly why it is reported
		// rather than left as a bare 400: reaching it means the listener is
		// not what this code assumes (a custom net.Listener, a Unix socket),
		// and every self-attribution on it fails identically and silently.
		obs.SelfLookupRefused.WithLabelValues("unparseable_peer").Inc()
		if s.warnSelfPeer.Allow(selfWarnEvery) {
			s.log().Warn("/v1/self cannot read the connection's source address, so no caller can be attributed",
				"peer", clipSegment(r.RemoteAddr),
				"note", "further reports are suppressed for "+selfWarnEvery.String())
		}
		writeError(w, http.StatusBadRequest, fmt.Sprintf("unparseable peer address %q", r.RemoteAddr))
		return
	}
	via := peerip.ForwardingHeader(r.Header)
	if via != "" {
		// The refusal is deliberate and it COSTS something (see the doc above:
		// a mesh sidecar in the caller's own network namespace lands here),
		// so it is counted and named. Without this the only trace was a 404 an
		// operator cannot tell from "this agent is on hostNetwork", on a route
		// whose whole job is to hand out an identity.
		obs.SelfLookupRefused.WithLabelValues("forwarded").Inc()
		if s.warnSelfForwarded.Allow(selfWarnEvery) {
			s.log().Warn("/v1/self refused: the request carries a forwarding header, so the connection is a "+
				"hop's and not the caller's",
				"header", via, "peer", ip,
				"note", "the caller falls back to a lookup by name ($POD_NAMESPACE/$POD_NAME), which resolves "+
					"to the same pod at one extra request per -self-attributes-refresh; if a service mesh adds "+
					"the header on the caller's own pod this is the only cost. Further reports are suppressed "+
					"for "+selfWarnEvery.String())
		}
	}
	s.servePod(w, r, cachePrivate,
		func() (store.NodePod, bool) {
			if via != "" {
				return store.NodePod{}, false
			}
			np, ok := s.store.GetPodByIP(ip)
			if !ok {
				// EXPECTED for a hostNetwork agent (it shares the node
				// address) and for one behind SNAT, so it is counted and not
				// logged: the remedy is the by-name fallback the agent
				// already runs, and this rate is how an operator tells that
				// fallback's population from a genuine attribution outage.
				obs.SelfLookupRefused.WithLabelValues("no_pod").Inc()
			}
			return np, ok
		},
		func() string {
			if via != "" {
				return fmt.Sprintf("request carries %s, so this connection belongs to a hop and not to the "+
					"caller; /v1/self can only attribute a direct connection", via)
			}
			return fmt.Sprintf("no live pod with peer IP %q", ip)
		})
}

// selfWarnEvery bounds the /v1/self refusal warnings. Every agent re-reads its
// own pod on -self-attributes-refresh (1m), so both conditions repeat once per
// agent per minute for as long as they last.
const selfWarnEvery = 15 * time.Minute
