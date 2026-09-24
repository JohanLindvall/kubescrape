package peerip

import (
	"net/http"
	"slices"
)

// forwardingHeaders are the headers a hop sets when it re-originates a request.
// Any one of them present is the hop declaring, in its own words, that the
// connection's address is not its caller's — the only evidence a server holding
// that address can have of it. Two decisions rest on it, and they must refuse
// on the SAME evidence: the metadata service's /v1/self (which would otherwise
// hand the hop's own pod identity to the caller, stamped on every self-metric it
// exports) and the agent's debug guard (whose local-connection exemption would
// otherwise admit a relay on the pod's loopback as a port-forward). They were
// two copies, and the agent's once lacked Via.
//
// Via is in the list because it is the one a forwarding proxy is REQUIRED to add
// (RFC 9110 §7.6.3), where the other three are conventions: a spec-following
// proxy that adds only Via — leaving the address it rewrote unannounced — is
// exactly the hop this refusal exists for.
//
// Names must be in net/http's canonical form, since ForwardingHeader indexes the
// header map directly (TestForwardingHeaderNamesAreCanonical).
var forwardingHeaders = []string{"Forwarded", "Via", "X-Forwarded-For", "X-Real-Ip"}

// ForwardingHeaders returns the forwarding-header names ForwardingHeader tests
// for, in its order. A copy: the list is a security decision and callers only
// ever need to read it (a test's coverage table).
func ForwardingHeaders() []string { return slices.Clone(forwardingHeaders) }

// ForwardingHeader names the first forwarding header h carries, or "".
//
// PRESENCE, not value: Header.Get cannot tell an absent header from a
// present-but-empty one, and an empty X-Forwarded-For is a hop that declared
// itself and wrote nothing — no less evidence than a populated one, since the
// value is never read for an address anyway. A caller can therefore only ever
// refuse ITSELF with one; no header selects or names anybody else.
func ForwardingHeader(h http.Header) string {
	for _, name := range forwardingHeaders {
		if _, ok := h[name]; ok {
			return name
		}
	}
	return ""
}
