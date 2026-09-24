// Tests for the canonical form the pod-IP index is keyed by. Both users — the
// metadata service attributing a /v1/self caller and the agent's peer-IP ingest
// fallback — look the result up in that one index, so they have to agree byte
// for byte; this is the single implementation they share.
package peerip

import (
	"net/http"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

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

// Canonical is the same normalisation on a bare address — it is what the store
// keys the index WITH, so From's output and a kubelet's status.podIP land on
// one key. An address that does not parse stays itself: it is still a key, and
// both sides have to agree on it too.
// The `peer` log key's value: the same canonical IP the index keys on, so a
// grep for one sender matches every line about it — and a raw address, clipped,
// only when there is no IP to extract.
func TestForLogRendersTheCanonicalIPOrTheClippedRawAddress(t *testing.T) {
	t.Parallel()
	long := "@" + strings.Repeat("x", 200)
	for _, tc := range []struct{ addr, want string }{
		{"10.0.0.1:34512", "10.0.0.1"},
		{"10.0.0.1", "10.0.0.1"},
		{"[FD00::0:7%eth0]:34512", "fd00::7"},
		{"[::ffff:10.1.2.3]:1", "10.1.2.3"},
		{"@", "@"}, // a Unix-socket peer: raw
		{"", ""},
		{long, long[:maxLogPeerBytes] + "…"},
	} {
		if got := ForLog(tc.addr); got != tc.want {
			t.Errorf("ForLog(%q) = %q, want %q", tc.addr, got, tc.want)
		}
	}
}

func TestCanonical(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ ip, want string }{
		{"10.1.2.3", "10.1.2.3"},
		{"::ffff:10.1.2.3", "10.1.2.3"},
		{"FD00::7", "fd00::7"},
		{"fd00:0:0:0:0:0:0:7", "fd00::7"},
		{"fe80::1%eth0", "fe80::1"},
		{"", ""},
		{"not-an-ip", "not-an-ip"},
	} {
		if got := Canonical(tc.ip); got != tc.want {
			t.Errorf("Canonical(%q) = %q, want %q", tc.ip, got, tc.want)
		}
	}

	// The two must agree: what From hands a lookup is what Canonical made the
	// key.
	if got, want := From("[::ffff:10.1.2.3]:34512"), Canonical("::ffff:10.1.2.3"); got != want {
		t.Errorf("From = %q but the index key is %q", got, want)
	}
}

// Canonical runs twice per pod upsert inside the store's exclusive write lock
// (podAddresses, for every address plus the host IP), so it must not allocate
// on the shapes that path actually sees: an already-canonical address, and the
// empty string every pod with no status.hostIP hands it.
// Deliberately NOT t.Parallel, unlike the rest of this package: an
// AllocsPerRun measurement must never run beside another test.
func TestCanonicalIsAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	for _, ip := range []string{"10.1.2.3", "fd00::7", "", "2001:db8::1"} {
		got := testing.AllocsPerRun(200, func() {
			sink = Canonical(ip)
		})
		if got != 0 {
			t.Errorf("Canonical(%q) allocates %v times per call, want 0", ip, got)
		}
	}
}

// sink defeats the dead-store elimination that would let an allocation-free
// claim hold vacuously.
var sink string

// ForwardingHeader indexes the header map directly, so a name that is not in
// net/http's canonical form would never match and its refusal would never fire.
func TestForwardingHeaderNamesAreCanonical(t *testing.T) {
	t.Parallel()
	names := ForwardingHeaders()
	if len(names) == 0 {
		t.Fatal("no forwarding headers: every refusal built on them passes vacuously")
	}
	for _, h := range names {
		if got := http.CanonicalHeaderKey(h); got != h {
			t.Errorf("forwarding header %q is stored by net/http as %q: the refusal never fires for it", h, got)
		}
	}
}

// PRESENCE is the evidence: an empty value is a hop that declared itself and
// wrote nothing, and must be refused like a populated one.
func TestForwardingHeaderIsDecidedByPresence(t *testing.T) {
	t.Parallel()
	if got := ForwardingHeader(http.Header{"Accept": {"*/*"}}); got != "" {
		t.Errorf("ForwardingHeader(no forwarding header) = %q, want \"\"", got)
	}
	for _, h := range ForwardingHeaders() {
		for _, v := range []string{"10.0.0.9", ""} {
			hdr := http.Header{}
			hdr.Set(h, v)
			if got := ForwardingHeader(hdr); got != h {
				t.Errorf("ForwardingHeader(%s: %q) = %q, want %q", h, v, got, h)
			}
		}
	}
}
