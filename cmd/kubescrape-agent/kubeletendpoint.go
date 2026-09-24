package main

// -kubelet-endpoint normalisation: the one place the kubelet base URL is
// parsed, called once by compileConfig — so -check-config refuses an unusable
// endpoint, and startScraper scrapes exactly the value that passed.

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

// kubeletBase normalises -kubelet-endpoint into a base URL net/http will
// accept, and reports the ones it cannot. Empty stays empty (the kubelet
// scrapes are then simply not scheduled — configWarnings names that).
//
// The one repair it makes is BRACKETING an IPv6 literal, because the shipped
// default cannot spell both address families and the manifests cannot fix it:
// NODE_IP is status.hostIP, so `https://$(NODE_IP):10250` expands to
// `https://fd00:10::5:10250` on an IPv6 node — which net/url has refused since
// Go 1.26 enforced strict colons for http/https (`invalid port ":10::5:10250"
// after host`, go.dev/issue/75223), so every request the three kubelet
// pipelines build fails before it is issued, on every node, forever. Writing
// `https://[$(NODE_IP)]:10250` instead is NOT the fix: that renders
// `https://[10.0.0.5]:10250` on an IPv4 cluster, which the same parser rejects
// as an invalid IP-literal. One static value cannot be right for both families,
// so the bracketing has to happen where the family is known — here, once, at
// the one place the endpoint is read.
//
// It is net.JoinHostPort's treatment, the same one internal/scrape/targets.go
// already gives pod addresses; only a host that genuinely parses as an IP
// literal is bracketed, so an IPv4 address and a DNS name are returned
// untouched and never acquire brackets Go would then reject.
func kubeletBase(ep string) (string, error) {
	// Exactly empty, never trimmed: a whitespace-only value is a mistake, and
	// reading it as "not configured" would disable all three kubelet pipelines
	// silently — the one outcome configWarnings exists to prevent. It falls
	// through to the refusal below instead.
	if ep == "" {
		return "", nil
	}
	u, err := url.Parse(ep)
	if err == nil && u.Host != "" {
		// An authority is not enough: `//node:10250` (no scheme) and
		// `ftp://node` both parse with a host, and http.Client refuses every
		// request built from them as an unsupported protocol scheme — the
		// failure this function's refusal exists to move to -check-config.
		if err = kubeletScheme(u.Scheme); err == nil {
			return ep, nil
		}
	} else if fixed, ok := bracketIPLiteralHost(ep); ok {
		// Re-parsed rather than trusted: the repair must produce something the
		// request path accepts, or it has merely moved the failure.
		if v, verr := url.Parse(fixed); verr == nil && v.Host != "" {
			if serr := kubeletScheme(v.Scheme); serr != nil {
				err = serr
			} else {
				return fixed, nil
			}
		}
	}
	if err == nil {
		// Parsed, but with no authority — a scheme-less endpoint like
		// `10.0.0.5:10250`, which url.Parse reads as scheme+opaque and
		// http.NewRequest then refuses as an unsupported protocol scheme.
		err = errors.New("no scheme://host — a bare host:port is read as a URL scheme, not an address")
	}
	return "", fmt.Errorf("-kubelet-endpoint=%q is not a base URL the agent can request (%v): all three kubelet scrapes build every request from it, so each would fail before it is issued. "+
		"Write it as scheme://host[:port] — an IPv6 host may be bare (https://fd00:10::5:10250, which is what $(NODE_IP) expands to on an IPv6 node) or bracketed; an IPv4 host or a name must NOT be bracketed", ep, err)
}

// kubeletScheme refuses a scheme http.Client cannot request. url.Parse has
// already lowercased it, so HTTPS:// is accepted.
func kubeletScheme(scheme string) error {
	switch scheme {
	case "http", "https":
		return nil
	case "":
		return errors.New("no scheme — a //host:port endpoint names no protocol for the request to use")
	default:
		return fmt.Errorf("scheme %q is not http or https", scheme)
	}
}

// bracketIPLiteralHost re-forms scheme://host[:port] with the host bracketed
// when it is an unbracketed IPv6 literal, returning false for everything else.
//
// The two-colon test is what keeps this narrow: an IPv4 authority has at most
// one colon (its port separator) and a DNS name has none, so neither can reach
// net.ParseIP here — which matters, because bracketing either is exactly the
// "invalid IP-literal" refusal this whole function exists to avoid.
//
// The ORDER of the two readings is the load-bearing part, and it used to be the
// other way round. Both can succeed on one string: `fd00::1:8443` is a legal
// IPv6 address AND a legal `fd00::1` plus port 8443, because a 1-4 digit port is
// also a legal hextet. Taking the address-only reading first therefore swallowed
// the port into the address for every port below 10000 — 8443, 9090, 4317, 443 —
// returning `https://[fd00::1:8443]` with err=nil, so -check-config passed and
// all three kubelet scrapes then dialled a wrong host on the scheme's default
// port, forever. Only a 5-digit port survived it, which is the sole reason the
// shipped :10250 ever worked. A trailing all-decimal group is a port far more
// often than it is the last hextet of an address someone wrote unbracketed, and
// the address-only reading is still reachable both from this fallback (its last
// group is not all digits, or the remainder is not an address) and from the
// bracketed spelling, which url.Parse accepts without coming here at all.
func bracketIPLiteralHost(ep string) (string, bool) {
	scheme, rest, ok := strings.Cut(ep, "://")
	if !ok {
		return "", false
	}
	authority, tail := rest, ""
	if i := strings.IndexAny(rest, "/?#"); i >= 0 {
		authority, tail = rest[:i], rest[i:]
	}
	// Userinfo would make the host split ambiguous, and a kubelet endpoint
	// never carries one (the credential is a bearer token file). Leave it to
	// the error rather than guess.
	if strings.Contains(authority, "@") || strings.Count(authority, ":") < 2 {
		return "", false
	}
	if i := strings.LastIndex(authority, ":"); i >= 0 {
		if host, port := authority[:i], authority[i+1:]; allDigits(port) && net.ParseIP(host) != nil {
			return scheme + "://" + net.JoinHostPort(host, port) + tail, true
		}
	}
	if ip := net.ParseIP(authority); ip != nil { // an address with no port
		return scheme + "://[" + authority + "]" + tail, true
	}
	return "", false
}

// allDigits reports whether s is a non-empty run of decimal digits — the shape
// of a port, and the only trailing group bracketIPLiteralHost will read as one.
func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
