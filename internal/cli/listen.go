package cli

// The listen-address checks both binaries run in -check-config (and at every
// real start, through the same validateConfig). Both register -listen,
// -metrics-listen and -pprof-listen, and each used to judge them with its own
// rule: the service compared raw strings, so `:9090` beside `0.0.0.0:9090`
// passed the dry run and then lost the bind, while the agent normalised the
// port and understood wildcards but let an unparseable address through to fail
// inside net.Listen with an error naming the value and not the flag.

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// Listener is one address a process will bind: the flag that named it
// (spelled with its dash) and the address as the flag holds it. Note, when set,
// is appended to a collision message this listener is part of — for a pair
// where losing the bind race is not the only thing wrong.
type Listener struct {
	Flag string
	Addr string
	Note string
}

// PprofNote is the Note for -pprof-listen: a collision there is also a
// configuration that puts profiles on a port meant for something else.
const PprofNote = " -pprof-listen is its own port on purpose: profiles expose goroutine stacks and heap contents, so it is the port to firewall or bind to localhost."

// CheckListeners refuses, by flag name, what a bind would otherwise refuse
// later and less legibly: a non-empty address that is not host:port, and two
// listeners that would contend for one socket (SameListenAddr). An empty
// address disables its listener and is never refused here — whether empty is
// legal for a given flag is the caller's to decide.
func CheckListeners(ls []Listener) error {
	for _, l := range ls {
		a := strings.TrimSpace(l.Addr)
		if a == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(a); err != nil {
			return fmt.Errorf("%s %q is not a listen address: %w (want host:port, or :port for every interface)", l.Flag, l.Addr, err)
		}
	}
	for i := range ls {
		for _, other := range ls[i+1:] {
			if !SameListenAddr(ls[i].Addr, other.Addr) {
				continue
			}
			// One copy of a note the two sides share.
			note := ls[i].Note
			if other.Note != note {
				note += other.Note
			}
			both := fmt.Sprintf("are both %q", ls[i].Addr)
			if ls[i].Addr != other.Addr {
				both = fmt.Sprintf("(%q and %q) name one socket", ls[i].Addr, other.Addr)
			}
			return fmt.Errorf("%s and %s %s: whichever binds second fails with `address already in use` and takes the process down.%s",
				ls[i].Flag, other.Flag, both, note)
		}
	}
	return nil
}

// SameListenAddr reports whether two listen addresses would contend for one
// socket. Empty disables a listener, so it collides with nothing; an address
// that is not host:port collides with nothing either (CheckListeners refuses
// it by name first).
//
// Hosts must match or one must be a WILDCARD: two different loopback or pod
// addresses on one port are legitimate, while 0.0.0.0 (or "", or ::) covers
// every address on that port and so contends with all of them.
func SameListenAddr(a, b string) bool {
	ha, pa, ok := splitListen(a)
	if !ok {
		return false
	}
	hb, pb, ok := splitListen(b)
	if !ok || pa != pb {
		return false
	}
	return ha == hb || wildcardHost(ha) || wildcardHost(hb)
}

// splitListen splits a listen address into host and normalised port.
func splitListen(addr string) (host, port string, ok bool) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", "", false
	}
	h, p, err := net.SplitHostPort(addr)
	if err != nil {
		return "", "", false
	}
	// ":04317" and ":4317" are one port; a NAMED port (":http") is compared as
	// written, which is exact for the equality this is used for.
	if n, err := strconv.Atoi(p); err == nil {
		p = strconv.Itoa(n)
	}
	return h, p, true
}

// wildcardHost reports whether a listen host covers every local address.
// net.SplitHostPort has already stripped the brackets from "[::]:4319", so the
// bracketed spelling never reaches here.
func wildcardHost(h string) bool {
	switch h {
	case "", "0.0.0.0", "::":
		return true
	}
	return false
}
