package main

// Who may READ the agent's data-bearing debug surfaces on -listen.
//
// /debug/otlp streams, verbatim and post-transform, every log record, metric
// and span this process exports: on the DaemonSet that is every container log
// line on the node, from every namespace scheduled there, selectable by
// resource-attribute glob; on the trace tier it is every application's spans.
// /debug/tailer enumerates the files behind them. That is a different class of
// data from everything else this port serves — /healthz, /readyz, the homepage,
// /debug/targets and /debug/transforms are process state, and the pod and
// target metadata /debug/targets shows is what the metadata service's /v1
// routes already serve to anyone by design — and it was reachable, with no
// credential of any kind, by any pod that could open a TCP connection to the
// agent's pod IP. The package's own header called that "the same exposure
// profile [as] any pod on the collector path", which is true of a collector and
// false of a DaemonSet whose port every pod in the cluster can reach.
//
// So these three are gated, and the gate has TWO keys because the two ways
// people legitimately read them are not alike:
//
//   - a LOCAL connection. `kubectl port-forward` is how every doc in this repo
//     reaches these surfaces, and the kubelet implements it by dialling
//     127.0.0.1 inside the pod's own network namespace — so the loopback
//     address IS the port-forward, and reaching it already requires
//     pods/portforward on the namespace. A pod on the cluster network cannot
//     forge it: the source address of an accepted TCP connection is not the
//     sender's to choose. (A container in the agent's own pod shares that
//     namespace, and an agent deliberately put on hostNetwork shares the
//     node's — both are far narrower sets than "any pod", and both are stated
//     in the flag help.)
//
//     That key has a SECOND requirement, because a loopback connection is not
//     reachable only by a process in the netns. `kubectl port-forward` binds a
//     loopback port on the OPERATOR's own machine, and every page their browser
//     loads can reach it: a page served from evil.example.com whose DNS answer
//     is rebound to 127.0.0.1 makes the browser open a genuinely local
//     connection AND, being same-origin with the page, lets the script READ the
//     response — the classic rebinding shape against a local debug UI. (A plain
//     cross-origin fetch at http://127.0.0.1 is not that shape: the browser
//     sends it, but CORS keeps the page from reading the answer. Rebinding is
//     what turns a write primitive into a read one.) The address is loopback,
//     no relay is involved, and nothing in the request is the attacker's to
//     choose EXCEPT the one thing rebinding cannot change: the Host header,
//     which is still the attacker's own name — the browser sends the name the
//     page was loaded from, not the address it resolved to. So the local arm
//     additionally requires Host to be a loopback literal (localhost,
//     127.0.0.0/8, ::1) or absent. Every port-forward client sends exactly
//     that, so the check costs an operator nothing and closes the rebinding
//     shape; a token-bearing request skips it entirely, because a credential
//     answers the question the Host was standing in for.
//   - a BEARER TOKEN, -debug-token-file, through the same internal/bearer as
//     /v1/scrape-auth and the trace tier's internal hop: one auth model in this
//     repo rather than three. That is the key for reading an agent from
//     somewhere else — a central debug pod, a proxied UI.
//
// The default is therefore local-only rather than a hard refusal: a hard
// refusal would break the documented workflow of every operator to close a hole
// only a neighbour pod can walk through, and this repo's rule is to take the
// narrower fix. A refusal names the flag and is counted, because a refusal that
// looks like a bug gets configured away.
//
// ACCEPTED RESIDUAL, written down rather than papered over: a token cannot be
// presented by the /debug/otlp/ui page's own fetch, so with -debug-token-file
// set the UI is reachable through port-forward (local) or through a proxy that
// adds the header — never by pasting a token into a query parameter, which
// would put a credential in every access log and in this process's own.

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/bearer"
	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/peerip"
)

// debugAuthRealm is sent on a 401 so a client can tell "wrong credentials" from
// "wrong URL".
const debugAuthRealm = `Bearer realm="kubescrape debug"`

// debugWarnEvery bounds the refusal warnings. The condition is a persisting
// one — a scanner, or an operator whose curl is missing the token — and the
// useful information in it is one line.
const debugWarnEvery = 15 * time.Minute

// debugForwardedHeaders are the headers a hop sets when it re-originates a
// request. Their presence is the hop saying, in its own words, that the
// connection is not the caller's — which is exactly the claim the local
// exemption rests on, so a local connection carrying one is refused. The same
// evidence /v1/self refuses on, for the same reason.
var debugForwardedHeaders = []string{"Forwarded", "X-Forwarded-For", "X-Real-Ip"}

// debugGuard decides who may read the data-bearing debug surfaces.
type debugGuard struct {
	// tokens is the accepted set (more than one only during a rotation grace
	// window), or nil when no -debug-token-file is configured.
	tokens func() []string
	log    *slog.Logger
	// warn throttles PER REASON: a scanner hammering the port must not
	// suppress the one line that tells an operator their token is wrong.
	warn *logdedupe.Table
}

// newDebugGuard reads -debug-token-file, if any, and starts its re-read.
//
// The read is FATAL when a path was given, which is bearer.NewRotating's
// contract and the right one here too: an operator who asked for a token wants
// to know their mount is wrong, not to discover months later that the agent
// quietly fell back to local-only.
func newDebugGuard(ctx context.Context, path string, log *slog.Logger) (*debugGuard, error) {
	g := &debugGuard{log: log, warn: logdedupe.New(8, debugWarnEvery)}
	if strings.TrimSpace(path) == "" {
		return g, nil
	}
	tok, err := bearer.NewRotating(path, log)
	if err != nil {
		return nil, err
	}
	// Clock-driven, never request-driven: Tokens() re-reads only when called,
	// so on a listener nobody is watching a rotation would go unnoticed until
	// the next request and arm the revoked token's grace window at THAT moment.
	// Same call pattern as the trace tier's receiver, which learned it the hard
	// way.
	go tok.Run(ctx)
	g.tokens = tok.Tokens
	return g, nil
}

// debugAccessMode names, in one word, who may read the data-bearing debug
// surfaces: the startup summary and -check-config print it, so an operator can
// see it without diffing flags. Read from the FLAG rather than from a built
// guard because -check-config builds nothing.
func debugAccessMode() string {
	if strings.TrimSpace(*debugToken) != "" {
		return "token"
	}
	if *listen == "" {
		return "(no -listen)"
	}
	return "local-only"
}

// authenticated reports whether a -debug-token-file is configured at all.
func (g *debugGuard) authenticated() bool { return g != nil && g.tokens != nil }

// protect gates one data-bearing handler.
func (g *debugGuard) protect(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if reason := g.refuse(r); reason != "" {
			g.reject(w, r, reason)
			return
		}
		h(w, r)
	}
}

// refuse returns "" when r may read, or the reason it may not. The reasons are
// the label values of kubescrape_debug_refused_total and each names a different
// operator action, which is why they are not one "denied".
func (g *debugGuard) refuse(r *http.Request) string {
	// A valid token authorizes from anywhere: the credential is the proof, so
	// neither the source address nor a forwarding header can add to it or take
	// from it.
	if g.tokens != nil && bearer.Authorized(r.Header.Get("Authorization"), g.tokens()) {
		return ""
	}
	if !debugLocalPeer(r.RemoteAddr) {
		if g.tokens == nil {
			// Nothing to present: the operator must set the flag.
			return "no_token"
		}
		// A credential exists and this request did not carry a valid one.
		return "unauthenticated"
	}
	if debugForwardedVia(r) != "" {
		// Local address, relayed request: the address is the relay's, so it
		// proves nothing about the caller, and the local exemption is the only
		// thing that would have admitted it.
		return "forwarded"
	}
	if !debugLocalHost(r.Host) {
		// Local address, someone else's name. A client that dialled this port
		// directly says so in the Host it sends; a browser rebound onto
		// 127.0.0.1 keeps sending the page's own name, which is the only part
		// of a rebinding attack the attacker cannot launder.
		return "host"
	}
	return ""
}

// reject answers a refused request, counts it and (throttled) says so.
func (g *debugGuard) reject(w http.ResponseWriter, r *http.Request, reason string) {
	obs.DebugRefused.WithLabelValues(reason).Inc()
	// The table holds far more keys than there are reasons, so it cannot
	// saturate and the truncation notice is unreachable.
	if allow, _ := g.warn.Allow(reason); allow {
		note := "these surfaces are served to a local connection (kubectl port-forward, or a container in " +
			"this pod) and to a request carrying the -debug-token-file bearer token"
		if reason == "host" {
			// A DIFFERENT operator action, so a different line: the caller was
			// local and still refused, which reads as a broken port-forward
			// unless the reason says what to look at.
			note = "the connection was local but named someone else in its Host header, which is what a browser " +
				"page on a rebound DNS name reaching a kubectl port-forward looks like; a client dialling this " +
				"port directly sends localhost or 127.0.0.1"
		}
		// The peer, the path and the Host NAME — never the presented credential.
		// The Host is the evidence here, and it is the attacker's own domain
		// rather than anything of this cluster's.
		g.log.Warn("refused a request for a data-bearing /debug surface; it streams this node's exported telemetry",
			"path", r.URL.Path, "peer", peerip.From(r.RemoteAddr), "host", debugLogHost(r.Host), "reason", reason,
			"note", note+"; further reports of this reason are suppressed for "+debugWarnEvery.String())
	}
	w.Header().Set("Cache-Control", "no-store")
	if reason == "host" {
		// Always a 403, even with a token file configured: this request came
		// from a client that cannot add a header (that is what makes it a
		// browser), so a challenge would only invite it to retry.
		http.Error(w, "this request's Host header names something other than localhost, so it did not come from "+
			"a client that dialled this port directly — a page in a browser reaching a kubectl port-forward "+
			"through a rebound DNS name looks exactly like this. Reach these surfaces as localhost or "+
			"127.0.0.1, or present the -debug-token-file bearer token", http.StatusForbidden)
		return
	}
	if g.tokens != nil {
		w.Header().Set("WWW-Authenticate", debugAuthRealm)
		http.Error(w, "missing or invalid bearer token (-debug-token-file); this surface streams the telemetry "+
			"this agent exports, so it is served only to a local connection (kubectl port-forward) or to a "+
			"request carrying that token", http.StatusUnauthorized)
		return
	}
	http.Error(w, "this surface streams the telemetry this agent exports and is served only to a local "+
		"connection (kubectl port-forward, or a container in this pod); set -debug-token-file on the agent to "+
		"read it from elsewhere with Authorization: Bearer <token>", http.StatusForbidden)
}

// maxLoggedHostBytes bounds the Host header on the refusal line. The value is
// the refused caller's to choose and net/http admits a header block far larger
// than anything a name needs; the throttle already bounds the RATE, this bounds
// the SIZE. Quoting is the handler's (internal/cli renders logfmt through
// TextHandler, which quotes and escapes a value holding spaces, '=' or
// newlines), so the only thing left to worry about is length.
const maxLoggedHostBytes = 128

// debugLogHost renders the Host for the refusal line, cut to a sane length with
// the cut made visible.
func debugLogHost(host string) string {
	if len(host) <= maxLoggedHostBytes {
		return host
	}
	return host[:maxLoggedHostBytes] + "...(truncated)"
}

// debugLocalPeer reports whether the connection came from the pod's own network
// namespace. Loopback is what `kubectl port-forward` produces: the kubelet
// dials 127.0.0.1 inside the target pod's namespace, which is the same property
// that makes port-forward work for a process bound to localhost.
func debugLocalPeer(remoteAddr string) bool {
	addr, err := netip.ParseAddr(peerip.From(remoteAddr))
	return err == nil && addr.IsLoopback()
}

// debugLocalHost reports whether the request's Host names loopback, which is
// what every client that dialled this port directly sends and what a DNS-rebound
// browser page does not (see the header comment).
//
// Absent is accepted: an HTTP/1.0 client may omit Host entirely, and a request
// that claims no name cannot have had one rebound — a browser always sends one.
// The name `localhost` is accepted alongside the literals because RFC 6761
// reserves it for loopback, so it is not a name an attacker's DNS can answer
// for.
func debugLocalHost(host string) bool {
	if host == "" {
		return true
	}
	h := host
	if hostOnly, _, err := net.SplitHostPort(h); err == nil {
		h = hostOnly
	}
	// A bracketed literal with no port ("[::1]") never reaches SplitHostPort's
	// success path, so strip the brackets here too.
	h = strings.TrimSuffix(strings.TrimPrefix(h, "["), "]")
	// A trailing dot is the same name, fully qualified.
	h = strings.TrimSuffix(h, ".")
	if strings.EqualFold(h, "localhost") {
		return true
	}
	// Through peerip.Canonical so the zone and the IPv4-mapped form are read the
	// same way here as they are on the peer address two lines up: one
	// normalisation in this repo, not two that agree until they do not.
	addr, err := netip.ParseAddr(peerip.Canonical(h))
	return err == nil && addr.IsLoopback()
}

// debugForwardedVia names the forwarding header the request carries, or "".
func debugForwardedVia(r *http.Request) string {
	for _, h := range debugForwardedHeaders {
		if r.Header.Get(h) != "" {
			return h
		}
	}
	return ""
}
