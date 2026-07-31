package kubemeta

import (
	"net"
	"net/netip"
	"strings"
	"unicode"
)

// NormalizeContainerID strips the runtime scheme prefix from a container ID,
// so "containerd://abc", "docker://abc" and "abc" all normalize to "abc".
// It also tolerates a collapsed "containerd:/abc" form (as produced by HTTP
// path cleaning). Container runtime IDs themselves never contain a colon.
//
// It is idempotent — the same ID may be normalized again on another path
// (agent, HTTP handler), which must be a no-op. Hence the cut at the LAST
// colon (the result can never retain one) and the space trim after the
// slashes (a malformed "scheme:// id" must not need two passes).
func NormalizeContainerID(id string) string {
	id = strings.TrimSpace(id)
	if i := strings.LastIndexByte(id, ':'); i >= 0 {
		id = strings.TrimLeftFunc(id[i+1:], func(r rune) bool { return r == '/' || unicode.IsSpace(r) })
	}
	return id
}

// PeerIP extracts the bare, CANONICAL IP from a connection's remote address
// ("10.0.0.1:34512", "[fe80::1%eth0]:34512", or a bare address), returning ""
// when it does not hold one.
//
// Canonical means what the pod-IP index is keyed by: the zone is dropped and
// an IPv4-mapped IPv6 address is unmapped, so `::ffff:10.0.0.1` and
// `10.0.0.1` are the same key. Both users — the metadata service attributing
// a /v1/self caller and the agent's peer-IP ingest fallback — look pods up in
// that one index, so they must agree byte for byte on the form; they used to
// canonicalise differently, which only stayed harmless because no transport
// in use produced the divergent form.
func PeerIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr // no port (a unix socket peer will not parse anyway)
	}
	if i := strings.IndexByte(host, '%'); i >= 0 {
		host = host[:i]
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return ""
	}
	return addr.Unmap().String()
}
