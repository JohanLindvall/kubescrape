package scrape

// appendTargetURL restates net.JoinHostPort's rule rather than calling it, to
// save the strconv.Itoa allocation JoinHostPort's string port forces. A URL is
// a scrape target's IDENTITY — the server dedups on it and the agent keys a
// scrape by it — so a restatement that differs from the original in ANY shape
// silently splits one target into two, or merges two into one. This is the test
// that keeps the copy honest.

import (
	"net"
	"strconv"
	"testing"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

func TestTargetURLMatchesNetJoinHostPort(t *testing.T) {
	ips := []string{
		"10.1.2.3",
		"0.0.0.0",
		"255.255.255.255",
		"", // an unscheduled pod: no IP yet
		"fd00::7",
		"::1",
		"::",
		"2001:db8:85a3::8a2e:370:7334",
		"fe80::1%eth0",    // a zoned literal still contains ':' and brackets
		"::ffff:10.1.2.3", // IPv4-mapped
		"not-an-ip",       // whatever the kubelet reported
		"host.with.dots",  //
	}
	ports := []int32{0, 1, 80, 9090, 8443, 65535, -1}
	paths := []string{"/metrics", "", "/", "/a/very/long/path/that/pushes/past/the/stack/scratch/buffer/" +
		"0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"}
	schemes := []string{"http", "https"}

	for _, ip := range ips {
		for _, port := range ports {
			for _, path := range paths {
				for _, scheme := range schemes {
					want := scheme + "://" + net.JoinHostPort(ip, strconv.Itoa(int(port))) + path
					pod := kubemeta.Pod{PodIP: ip}
					if got := targetURL(pod, scheme, path, port); got != want {
						t.Fatalf("targetURL(%q, %q, %q, %d) = %q, want %q", ip, scheme, path, port, got, want)
					}
					// makeTarget must name the same URL AND expose the
					// host:port half of it as Address.
					tgt := makeTarget(pod, scheme, path, port)
					if tgt.URL != want {
						t.Fatalf("makeTarget URL = %q, want %q", tgt.URL, want)
					}
					if wantAddr := net.JoinHostPort(ip, strconv.Itoa(int(port))); tgt.Address != wantAddr {
						t.Fatalf("makeTarget(%q, %d).Address = %q, want %q", ip, port, tgt.Address, wantAddr)
					}
				}
			}
		}
	}
}

// The URL render is the MARGINAL cost of one more monitor endpoint resolving to
// a pod — the whole reason the server's merge resolves a URL before building a
// target — so it is held to one allocation: the returned string.
func TestTargetURLIsOneAllocation(t *testing.T) {
	pod := kubemeta.Pod{PodIP: "10.244.3.17"}
	if got := testing.AllocsPerRun(200, func() {
		urlSink = targetURL(pod, "http", "/metrics", 9090)
	}); got != 1 {
		t.Errorf("targetURL allocations = %v, want 1.\n"+
			"net.JoinHostPort + strconv.Itoa + concatenation is three; an alloc profile of a "+
			"colliding-monitor derivation found those at 78%% of every object it allocated.", got)
	}
	// A pod address alone (an IPv6 literal, which takes the bracketing branch)
	// is one allocation too.
	pod6 := kubemeta.Pod{PodIP: "fd00::7"}
	if got := testing.AllocsPerRun(200, func() {
		urlSink = targetURL(pod6, "https", "/metrics", 8443)
	}); got != 1 {
		t.Errorf("targetURL allocations for an IPv6 pod = %v, want 1", got)
	}
}

var urlSink string
