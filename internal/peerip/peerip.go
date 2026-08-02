// Package peerip canonicalises a connection's remote address into the form the
// store's pod-IP index is keyed by.
//
// It lived in pkg/kubemeta, which is the JSON model of the metadata API and has
// nothing to say about HTTP RemoteAddr strings — a public package carrying a
// transport helper for two internal callers. What it must NOT become is two
// copies: the metadata service attributing a /v1/self caller and the agent's
// peer-IP ingest fallback look pods up in the same index and have to agree byte
// for byte on the form. They used to canonicalise differently, which only
// stayed harmless because no transport in use produced the divergent form.
package peerip

import (
	"net"
	"net/netip"
	"strings"
)

// From extracts the bare, CANONICAL IP from a connection's remote address
// ("10.0.0.1:34512", "[fe80::1%eth0]:34512", or a bare address), returning ""
// when it does not hold one.
//
// Canonical means what the pod-IP index is keyed by: the zone is dropped and an
// IPv4-mapped IPv6 address is unmapped, so `::ffff:10.0.0.1` and `10.0.0.1` are
// the same key.
func From(remoteAddr string) string {
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
