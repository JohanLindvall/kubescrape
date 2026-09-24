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
//
// It also owns the one other fact both binaries read off an incoming request to
// decide whether its address is the caller's: the forwarding headers a
// re-originating hop declares itself with (ForwardingHeader).
package peerip

import (
	"net"
	"net/netip"
	"strings"

	"github.com/JohanLindvall/kubescrape/internal/clip"
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

// maxLogPeerBytes bounds a RAW peer address in a log line — the repo's clip for
// a log attribute (internal/clip). A RemoteAddr that parses never gets near it.
const maxLogPeerBytes = 96

// ForLog renders a connection's remote address as the value of the `peer` log
// key (internal/cli's vocabulary): the bare canonical IP From extracts, so one
// grep for a sender finds every line about it whichever listener wrote it —
// never "10.0.0.5:34512" on one line and "10.0.0.5" on the next, nor an IPv6
// peer bracketed here and bare there. The ephemeral port is dropped on purpose:
// it names one connection, and the question a peer= line answers is WHO.
//
// An address that does not parse (a Unix-socket peer, a custom listener) is
// logged raw, clipped, because it is then the only evidence of who connected.
func ForLog(remoteAddr string) string {
	if ip := From(remoteAddr); ip != "" {
		return ip
	}
	return clip.Ellipsis(remoteAddr, maxLogPeerBytes)
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

// addrTextMax bounds the canonical text of any address netip renders: the
// longest form is an IPv4-mapped IPv6 address ("::ffff:255.255.255.255", 22
// bytes) or a full IPv6 address (39), and the zone is stripped before we get
// here. A stack buffer this size therefore never spills to the heap.
const addrTextMax = 48

func canonical(host string) (string, bool) {
	if i := strings.IndexByte(host, '%'); i >= 0 {
		host = host[:i]
	}
	if host == "" {
		// Short-circuited BEFORE ParseAddr, which boxes a parseAddrError for an
		// error this function discards. Every pod with no HostIP takes this
		// path, twice per upsert, under the store's exclusive write lock.
		return "", false
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return "", false
	}
	// Rendered into a stack buffer and returned as the ORIGINAL string when it
	// matches: the overwhelmingly common input is already canonical (a
	// kubelet's status.podIP, an accept()ed peer address), and Addr.String
	// allocates a fresh one every time. `string(buf) == host` compiles to a
	// comparison with no copy.
	var buf [addrTextMax]byte
	rendered := addr.Unmap().AppendTo(buf[:0])
	if string(rendered) == host {
		return host, true
	}
	return string(rendered), true
}
