// Tests for the canonical form the pod-IP index is keyed by. Both users — the
// metadata service attributing a /v1/self caller and the agent's peer-IP ingest
// fallback — look the result up in that one index, so they have to agree byte
// for byte; this is the single implementation they share.
package peerip

import "testing"

func TestPeerIPCanonicalises(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ addr, want string }{
		{"10.0.0.1:34512", "10.0.0.1"},
		{"[fd00::1]:34512", "fd00::1"},
		{"[fe80::1%eth0]:34512", "fe80::1"},       // zone stripped: the store keys bare IPs
		{"[::ffff:10.1.2.3]:34512", "10.1.2.3"},   // 4-in-6 unmapped to the form status.podIP reports
		{"[2001:0db8::0:1]:34512", "2001:db8::1"}, // canonicalised, as the API server reports it
		{"10.0.0.1", "10.0.0.1"},                  // no port
		{"", ""},
		{"not-an-ip:80", ""},
		{"kubescrape.monitoring:80", ""},
	} {
		if got := From(tc.addr); got != tc.want {
			t.Errorf("From(%q) = %q, want %q", tc.addr, got, tc.want)
		}
	}
}
