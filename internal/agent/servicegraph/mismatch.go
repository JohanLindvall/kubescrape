package servicegraph

// Agent/shard configuration drift, detected rather than documented.
//
// The agent's serviceGraphShards.dimensions and .peerAttributes must match the
// shard's serviceGraph.dimensions and .virtualNodePeerAttributes. Trim keeps
// exactly the configured keys, so a dimension the shard reads but the agent
// does not forward renders as an EMPTY label on every edge, and a peer
// attribute it does not forward means no virtual node is ever synthesized —
// calls to uninstrumented dependencies vanish from the graph instead of
// appearing as their far side. Neither failure is visible in the output: the
// graph renders, it is just wrong, which is the one outcome worse than a
// missing graph.
//
// This used to be a rule stated in three documents and enforced by nothing. The
// forwarder now declares its effective lists on every forwarded resource
// (ForwardedDimensions), and this compares them with the shard's own.

import (
	"log/slog"
	"slices"
	"strings"
	"sync"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// maxMismatchClaims bounds the distinct agent claims remembered for the
// log-once bookkeeping.
//
// The key is data from the network — a claim is whatever an agent stamped — so
// it is a key space this process does not control, and an unbounded map keyed
// on such a thing is the bug class this repo has now fixed twice (the disk
// buffer's stuck-payload map, the spanmetrics cardinality latch). A cluster has
// as many distinct claims as it has distinct agent configurations, which is one
// during steady state and two mid-rollout; sixteen is far past any legitimate
// fleet, and at the cap the watch says so once and stops remembering rather
// than growing or falling back to logging every batch.
const maxMismatchClaims = 16

// mismatchWatch compares each agent's declared trim lists against this shard's
// own configuration, logging once per DISTINCT disagreement and counting every
// occurrence.
//
// Once per distinct claim, never per span: a fleet-wide misconfiguration means
// every batch from every node carries the same wrong claim, and a line per
// batch would bury the one line that says what is wrong under thousands that
// repeat it. The counter is what carries the volume — it also answers "how
// much of my graph traffic is affected", which the log line cannot.
type mismatchWatch struct {
	// dims/peers are this shard's own effective lists, canonically encoded, and
	// are immutable after construction — which is what lets the hot path
	// compare against them without taking the mutex.
	dims, peers       string
	dimList, peerList []string
	log               *slog.Logger
	mu                sync.Mutex
	seen              map[string]bool
	saturated         bool
}

func newMismatchWatch(dims, peers []string, log *slog.Logger) *mismatchWatch {
	if log == nil {
		log = slog.Default()
	}
	return &mismatchWatch{
		dims:     encodeAttrList(dims),
		peers:    encodeAttrList(peers),
		dimList:  canonicalAttrList(dims),
		peerList: canonicalAttrList(peers),
		log:      log,
		seen:     make(map[string]bool, 2),
	}
}

// check reads one forwarded resource's claim and reports a disagreement.
//
// A resource that declares NOTHING is not a disagreement: the spans were pushed
// straight to the shard (a test, another sender), or they came from an agent
// too old to declare. Both are indistinguishable from here, and warning about
// an in-progress rollout on every batch would make the signal worthless
// exactly when a fleet is being upgraded.
func (w *mismatchWatch) check(attrs pcommon.Map) {
	if w == nil {
		return
	}
	v, ok := attrs.Get(ForwardedDimensions)
	if !ok {
		return
	}
	dims := v.Str()
	peers := ""
	if pv, ok := attrs.Get(ForwardedPeerAttributes); ok {
		peers = pv.Str()
	}
	if dims == w.dims && peers == w.peers {
		return // the common case: one string compare each, no lock
	}
	obs.ServiceGraphConfigMismatch.Inc()
	w.report(dims, peers)
}

// report logs the disagreement the first time this exact claim is seen.
func (w *mismatchWatch) report(dims, peers string) {
	key := dims + "\x00" + peers
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.seen[key] {
		return
	}
	if len(w.seen) >= maxMismatchClaims {
		if !w.saturated {
			w.saturated = true
			w.log.Warn("service-graph agent/shard config mismatches from too many distinct agent configurations; not logging further shapes (the counter keeps moving)",
				"distinctClaims", len(w.seen))
		}
		return
	}
	w.seen[key] = true
	// Both directions are reported because they fail differently: what the
	// shard reads and the agent does not forward is LOST, while the reverse is
	// only wasted bytes on the hop. Naming the two sides' whole lists as well
	// makes the line self-contained — an operator should not have to fetch two
	// ConfigMaps to act on it.
	w.log.Warn("service-graph agent/shard config mismatch: this shard reads a different dimension/peer-attribute set than an agent forwards, so the agent trims away exactly what the shard looks for — missing dimensions render as EMPTY labels on every edge from that agent, and missing peer attributes mean no virtual node is synthesized, so calls to uninstrumented dependencies do not appear at all",
		"agentDimensions", dims,
		"shardDimensions", w.dims,
		"dimensionsNotForwarded", strings.Join(missingFrom(w.dimList, dims), ","),
		"agentPeerAttributes", peers,
		"shardPeerAttributes", w.peers,
		"peerAttributesNotForwarded", strings.Join(missingFrom(w.peerList, peers), ","),
	)
}

// missingFrom returns the shard's keys that the agent's encoded claim does not
// contain — the ones that will be lost.
func missingFrom(want []string, claim string) []string {
	have := strings.Split(claim, attrListSep)
	var out []string
	for _, k := range want {
		if !slices.Contains(have, k) {
			out = append(out, k)
		}
	}
	return out
}

// attrListSep joins an encoded attribute list. A comma is readable in a log
// line and in a resource attribute, and an OTel attribute KEY containing one is
// pathological enough that the only cost is a contrived false negative (["a,b"]
// encoding identically to ["a","b"]); an unprintable separator would trade that
// for an unreadable log line on the path whose entire purpose is being read.
const attrListSep = ","

// encodeAttrList renders an attribute-key list for comparison: deduplicated,
// empties dropped, SORTED and joined.
//
// Sorted because the two sides' lists mean the same thing in any order — order
// matters to neither the trimmer's allow-list nor (dimensions) the metric
// layer, which takes its ordering from elsewhere — so an order-sensitive
// encoding would report a mismatch where there is none. Peer attributes ARE
// order-sensitive in one respect (precedence), but that is the agent's
// business only insofar as it forwards them: the shard resolves precedence
// itself, so a reordered agent list changes nothing and must not warn.
func encodeAttrList(keys []string) string {
	return strings.Join(canonicalAttrList(keys), attrListSep)
}

// canonicalAttrList is encodeAttrList's list form.
func canonicalAttrList(keys []string) []string {
	if len(keys) == 0 {
		return nil
	}
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		if k == "" || slices.Contains(out, k) {
			continue
		}
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}
