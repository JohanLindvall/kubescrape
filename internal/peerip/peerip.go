// Package peerip canonicalises a connection's remote address into the form the
// store's pod-IP index is keyed by — and, through Canonical, is what the store
// keys that index WITH, so the two cannot disagree.
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
// Canonical means what the pod-IP index is keyed by: the store runs every
// address a pod reports through Canonical before indexing it, so the two forms
// meet.
func From(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr // no port (a unix socket peer will not parse anyway)
	}
	ip, ok := canonical(host)
	if !ok {
		return ""
	}
	return ip
}

// Canonical is that same normalisation applied to a bare address: the zone is
// dropped, an IPv4-mapped IPv6 address is unmapped and an IPv6 address is
// lower-cased and compressed, so `::ffff:10.0.0.1` and `10.0.0.1`, or `FD00::7`
// and `fd00::7`, are one key.
//
// It is what the pod-IP index keys on, and it has to be: the addresses come
// from a kubelet's status.podIPs while every lookup comes through From or
// through a client that formed its argument the same way. An address that does
// NOT parse is returned unchanged — it is still a key, and index and lookup
// have to agree on it too.
func Canonical(ip string) string {
	if c, ok := canonical(ip); ok {
		return c
	}
	return ip
}

func canonical(host string) (string, bool) {
	if i := strings.IndexByte(host, '%'); i >= 0 {
		host = host[:i]
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return "", false
	}
	return addr.Unmap().String(), true
}
