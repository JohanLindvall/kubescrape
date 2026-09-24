package server

// HTTP caching for the metadata routes: Cache-Control and ETag rendering, the
// 304 path, and the node-targets memo validated by change tokens.

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/JohanLindvall/haste/xxh3"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// cachePolicy selects the cache headers a pod response carries.
type cachePolicy int

const (
	// cacheNone sends no freshness lifetime: the pod-IP index exists for
	// IMMEDIACY (IPs recycle; deleted pods drop out at once) and a cached 200
	// would let metaclient re-serve the OLD owner of a recycled IP for up to
	// the TTL. It says so EXPLICITLY (`no-store`) rather than by omission:
	// a response carrying no freshness information may still be stored and
	// heuristically freshened by a shared cache (RFC 9111 4.2.2), which is
	// precisely the staleness this route exists to avoid.
	cacheNone cachePolicy = iota
	// cacheShared is the standard metadata caching: max-age + ETag, so repeat
	// lookups are served locally and revalidate as 304s.
	cacheShared
	// cachePrivate is cacheShared plus `private`, for a response that
	// identifies the CALLER (/v1/self). A per-client cache — metaclient, one
	// per process, always asking about the same pod — may hold it; a SHARED
	// cache must not, or one caller's identity would be handed to the next.
	cachePrivate
)

// noStore is the Cache-Control this policy needs on a response that carries no
// freshness lifetime — the cacheNone 200s and every 404. 404 is one of the
// statuses a cache may store and heuristically freshen when nothing says
// otherwise (RFC 9111 4.2.2 over RFC 9110 15.1), so silence is not the same as
// "do not store": a cached "no live pod with IP x" outlives the pod that took
// the recycled address, and a cached /v1/self answer names whoever asked
// first. cacheShared keeps its historical silence — those 404s are the
// container/pod/uid/node lookups, whose 200s are cached on purpose and whose
// misses are cheap to re-ask.
func (p cachePolicy) noStore() string {
	switch p {
	case cacheNone:
		return "no-store"
	case cachePrivate:
		// The answer identifies the CALLER; a shared cache must not hold it
		// even for the time a heuristic would grant.
		return "private, no-store"
	}
	return ""
}

// writeCached serves a 200 metadata response with standard HTTP cache headers
// (Cache-Control max-age + ETag), so the agent's client can serve repeat
// lookups locally and revalidate cheaply with If-None-Match (304). With a zero
// TTL it falls back to a plain uncached JSON write.
//
// private marks a response that identifies the CALLER (/v1/self): per-client
// caches may store it, shared ones must not.
func (s *Server) writeCached(w http.ResponseWriter, r *http.Request, v any, private bool) {
	if s.cacheTTL <= 0 {
		if private {
			// The caching knob is off, but the response still names its caller:
			// without a freshness lifetime a shared cache MAY store a 200
			// heuristically (RFC 9111 4.2.2), which is exactly the response
			// that must never be handed to the next caller.
			w.Header().Set("Cache-Control", "private, no-store")
		}
		s.writeJSON(w, http.StatusOK, v)
		return
	}
	body, etag, ok := s.encodeCached(w, v, "metadata response")
	if !ok {
		return
	}
	s.writeCachedBody(w, r, body, etag, private)
}

// encodeCached marshals a cached response and names it: the body and its entity
// tag. A value that cannot be encoded is reported (reportEncodeFailure) and
// answered 500 here, and ok is false — nothing has been written otherwise, which
// is why a cached response is marshalled whole rather than streamed. The one
// sequence writeCached and handleNodeTargets both need; handleNodeTargets
// encodes for itself only because it memoises the tag before the write.
func (s *Server) encodeCached(w http.ResponseWriter, v any, what string) (body []byte, etag string, ok bool) {
	body, err := json.Marshal(v)
	if err != nil {
		s.reportEncodeFailure(what, encodeAnswered500, err)
		writeError(w, http.StatusInternalServerError, "encoding response")
		return nil, "", false
	}
	return body, entityTag(body), true
}

// writeCachedBody serves an already-encoded body under its entity tag: the
// cache headers, then either the 304 a matching validator asks for or the body.
// handleNodeTargets encodes for itself, because it memoises the tag and has to
// do so before the client can act on it.
func (s *Server) writeCachedBody(w http.ResponseWriter, r *http.Request, body []byte, etag string, private bool) {
	cc := cacheControlFor(s.maxAgeSeconds(), private)
	if match := r.Header.Get("If-None-Match"); match != "" && etagMatches(match, etag) {
		writeNotModified(w, etag, cc)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Cache-Control", cc)
	h.Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// writeNotModified answers a matching revalidation: 304 under the validator
// and the freshness lifetime the client may now reuse its copy for. The ONE
// spelling of a 304 here — writeCachedBody's and the node-targets memo's
// (nodeTargetsNotModified) — so the two cannot come to disagree about its
// headers; the pair had already drifted once, on max-age rounding (see
// maxAgeSeconds).
func writeNotModified(w http.ResponseWriter, etag, cacheControl string) {
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Cache-Control", cacheControl)
	h.Set("ETag", etag)
	w.WriteHeader(http.StatusNotModified)
}

// entityTag is the ETag of a response body: the 128-bit digest as 32 hex
// digits, high half first, in quotes.
//
// Rendered into a fixed-size array rather than with fmt or two FormatUint
// calls: the width is constant, so the whole tag is one allocation (the
// returned string) on a path that runs for every cached response including
// every 304 revalidation. Zero-PADDING is the part that matters for
// correctness — FormatUint drops leading zeros, which would let two distinct
// digests render as the same tag whenever one half is small.
func entityTag(body []byte) string { return formatTag(bodyHash(body)) }

// formatTag renders one digest. Split from entityTag so the padding can be
// asserted on a CHOSEN digest (TestEntityTagIsFixedWidthHex) — searching for a
// body that hashes to a half with leading zeros is not a test's job.
func formatTag(h xxh3.Uint128) string {
	var half [8]byte
	var out [2 + 32]byte
	out[0] = '"'
	binary.BigEndian.PutUint64(half[:], h.Hi)
	hex.Encode(out[1:17], half[:])
	binary.BigEndian.PutUint64(half[:], h.Lo)
	hex.Encode(out[17:33], half[:])
	out[33] = '"'
	return string(out[:])
}

// etagMatches evaluates an If-None-Match header against the current entity
// tag per RFC 9110: a comma-separated list of entity tags compared weakly (a
// W/ prefix is ignored), with "*" matching any current representation. Our
// ETags are quoted hex with no embedded commas, so splitting on commas is
// exact.
//
// SplitSeq, not Split: the header is the client's, it is read on every cached
// revalidation of every unauthenticated metadata route, and Split materialised
// the whole list first — one allocation per request, and a header of commas up
// to MaxHeaderBytes made that a ~200 KB transient slice. SplitSeq yields
// exactly Split's substrings, empty ones included, and allocates nothing
// (TestETagMatchesIsAllocationFree).
func etagMatches(header, etag string) bool {
	for candidate := range strings.SplitSeq(header, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "*" {
			return true
		}
		if strings.TrimPrefix(candidate, "W/") == etag {
			return true
		}
	}
	return false
}

// bodyHash is the ETag digest: xxh3, 128 bits wide.
//
// Not hash/fnv, because FNV-1a is a byte-at-a-time loop (~1.2 GB/s) and this
// runs over the FULL body of every cached response, including every 304
// revalidation — most visibly on /v1/nodes/{node}/targets, which re-serializes
// every pod document on the node each scrape cycle.
//
// 128 bits rather than 64 because of what a collision COSTS here, not because
// one is likely: two bodies sharing a digest make writeCached answer a
// revalidation with 304, and the agent then keeps serving the PREVIOUS node's
// target list — a wrong answer that persists until the body changes again and
// looks exactly like a correctly-cached response while it lasts. A digest is
// also the one place where the input is attacker-influenced in principle (a pod
// annotation lands in the body), and 64-bit xxhash is not collision-resistant
// against a chosen input. The width is close to free: xxh3 computes the 128-bit
// result in the same pass as the 64-bit one.
//
// ETags stay opaque to clients (etagMatches only string-compares, W/ prefix
// stripped), so widening the digest costs one full 200 per cached client at the
// upgrade boundary, exactly as any other ETag change would.
func bodyHash(b []byte) xxh3.Uint128 { return xxh3.Sum128(b) }

// maxAgeSeconds is the whole TTL rendered in the seconds max-age is expressed
// in.
//
// max-age has second granularity: a sub-second TTL truncates to 0, which tells
// the client not to cache AT ALL — the opposite of a short cache, and silently
// (the ETag is still computed on every response). Round up so any non-zero TTL
// caches for at least a second; 0 disables caching before we get here.
//
// The node-targets memo grants through this too, and the two MUST agree. While
// the memo floored where the 200's max-age rounded up, a sub-second TTL made the
// memo unreachable by arithmetic: the 200 advertised max-age=1 and the memo
// then refused every revalidation it seeded, so a conforming agent re-derived,
// re-sorted, re-marshalled and re-hashed the whole node on every poll while
// kubescrape_node_targets_builds_total reported "built" forever — the same
// symptom as an unwired change token, with nothing to tell the two apart.
func (s *Server) maxAgeSeconds() int {
	return max(1, int(s.cacheTTL/time.Second))
}

// cacheControlFor renders a freshness lifetime of maxAge seconds. It takes the
// lifetime rather than reading the TTL because the node-targets memo's
// wall-clock fallback grants only the REMAINING window, not the whole one
// (nodeTargetsNotModified); every other response grants s.maxAgeSeconds().
func cacheControlFor(maxAge int, private bool) string {
	cc := "max-age=" + strconv.Itoa(maxAge)
	if private {
		cc = "private, " + cc
	}
	return cc
}

// nodeTargetsETag remembers what one node's target list was called, and when.
type nodeTargetsETag struct {
	etag    string
	builtAt time.Time
	valid   targetsValidity
}

// targetsValidity is the composite change token of every source nodeTargets
// reads. Component-wise rather than hashed or summed on purpose: four uint64s
// compare in four instructions, and any folding of them into one introduces an
// aliasing chance for the one thing this must never do — call a changed cluster
// unchanged.
//
// wired reports that EVERY source publishes a token. It is not a formality: an
// unwired source is indistinguishable from one that never changes, so a missing
// one would silently turn this into "always valid" and serve a frozen target
// list until the TTL fallback happened to save it. Absent by CONFIGURATION is a
// different thing and is fine — a nil Monitors index means monitor-derived
// targets cannot exist, so its constant zero is the truth.
type targetsValidity struct {
	pods, owners, services, monitors uint64
	wired                            bool
}

// targetsValidity samples every source nodeTargets derives from. Callers load
// it BEFORE reading the data (see store.Store's gen field): a change landing
// mid-derivation then leaves the memo tagged with the older token and the next
// revalidation rebuilds, which is the safe direction to be wrong in.
//
// The pod half is the NODE's token (store.NodeGeneration), not the store-wide
// one: nodeTargets reads only PodsOnNode(node), and the store-wide token lapsed
// every node's memo on a pod event anywhere in the cluster and on every sweep
// that removed a tombstone — so under ordinary pod churn the memo rarely hit.
func (s *Server) targetsValidity(node string) targetsValidity {
	if s.ownerGeneration == nil || s.store == nil || s.services == nil {
		return targetsValidity{}
	}
	v := targetsValidity{
		pods:     s.store.NodeGeneration(node),
		owners:   s.ownerGeneration(),
		services: s.services.Generation(),
		wired:    true,
	}
	if s.monitors != nil {
		v.monitors = s.monitors.Generation()
	}
	return v
}

// maxNodeTargetETags bounds the memo. An entry is a node name, a quoted 32-hex
// tag (the 128-bit body digest, formatTag) and its change token, and one is
// minted only for a node the store HAS pods on — a caller inventing node names
// gets an empty list, which costs a map lookup to produce and never reaches the
// memo. Nothing deletes an entry except the sweep below, so a node that left
// the cluster keeps its entry until the cap is reached; the cap is what bounds
// that residue (autoscaler churn over the process lifetime) and a cluster far
// larger than this service is built for.
//
// At the cap, and only when the node is not already memoised (replacing an
// entry does not grow the map), the sweep runs in two passes:
//
//   - First, entries that are PROVABLY unusable: under the change token, one
//     whose recorded token is older than the current one in any component (its
//     next revalidation would fail the equality check and rebuild anyway — a
//     departed node's entry is always one, since removing the node's pods moved
//     its pod token); under the wall-clock fallback, one older than the TTL.
//   - Only if that frees nothing — a cluster that is genuinely static and
//     larger than the cap — entries not revalidated for a whole TTL. builtAt is
//     refreshed on every token-validated 304, so this is the least recently
//     USED, not the stale: their memo may still be valid, and evicting one
//     trades that node a rebuild for room. It is an approximation, and it is
//     here so the memo keeps rotating rather than freezing on the first 8,192
//     nodes while every other node pays the sweep on every rebuild.
//
// If neither frees room the new tag is simply not memoised — that node's
// revalidations rebuild until room appears, which is the correct answer, just
// the slow one.
const maxNodeTargetETags = 8192

// nodeTargetsNotModified answers a conditional GET for a node's targets from
// the ETag memo, without building the response — reporting whether it did.
//
// The ETag is a body hash, so a 304 otherwise costs EVERYTHING a 200 costs:
// PodsOnNode, per-pod owner and namespace enrichment, target derivation, the
// sort and a full json.Marshal, and then the body is discarded (measured at
// 110 pods: 1.87 ms, 1.90 MB and 7,553 allocations to send an empty 304, with
// json.Marshal at 48.6% of the request's CPU).
//
// TWO WAYS TO VALIDATE, and which one runs decides whether this is worth
// having at all.
//
// THE CHANGE TOKEN is the one that matters. Every source nodeTargets reads —
// the pod store, the owner and namespace informer caches, services.Index and
// servicemonitors.Index — publishes a generation that advances only on a real
// change, and the memo records what they read at build time. If all four still
// agree, the client's copy is not merely young: it is PROVABLY current, so the
// 304 grants a full max-age measured from now, exactly as the 200 does. No
// clock is consulted.
//
// THE WALL CLOCK is the fallback for when some source is not wired (see
// targetsValidity.wired) — it must not be entered by assuming an unwired token
// means "unchanged". It bounds staleness by builtAt+TTL, the last instant the
// store is known to have agreed with the tag, and hands out only the REMAINING
// window floored to whole seconds. Less than the 200's window, deliberately:
// the revalidating client is not necessarily the requester whose build stamped
// builtAt (any other caller rebuilds the unchanged list and refreshes the
// memo), so re-stamping a full max-age on an up-to-TTL-old memo would entitle a
// lapsed client to its copy until builtAt+2×TTL while the list may have changed
// just after builtAt. Once nothing remains the memo is ignored and the response
// is rebuilt, which is where a changed target list becomes a 200.
//
// WHY THE TOKEN WAS ADDED: with only the clock, this was unreachable by the
// caller it exists for. The memo lived for cacheTTL from the build and cacheTTL
// is also the max-age the 200 advertises, so a client honouring its own cache
// asked again exactly when — or after — the memo lapsed; the two windows were
// the same window. A DaemonSet agent polling on its scrape interval therefore
// NEVER hit it and re-paid the whole derivation, sort and marshal on every
// poll (468 µs / 165 KiB / 1,259 allocations at 5,000 pods; 1.85 ms / 650 KiB /
// 4,945 at 22,000, with marshal 54-71% of it), multiplied by every node in the
// fleet, in the singleton the chart requests 128Mi for. What reached it was
// only a caller asking FASTER than the max-age it was given.
// TestNodeTargetsMemoServesAConformingClient pins the new behaviour,
// TestNodeTargetsMemoRebuildsWhenAnySourceChanges pins the safety property, and
// BenchmarkNodeTargetsRevalidation reports both shapes side by side.
//
// Only the tag is memoised, never the body: the body is the whole node's pod
// set (2.21 MB at 110 pods), and holding one per node would put hundreds of
// megabytes of response bodies in a process with a 128Mi request.
func (s *Server) nodeTargetsNotModified(w http.ResponseWriter, r *http.Request, node string) bool {
	if s.cacheTTL <= 0 {
		return false
	}
	match := r.Header.Get("If-None-Match")
	if match == "" {
		return false
	}
	// Sampled BEFORE the memo is read, for the same reason a build samples it
	// before reading the store: if a change lands between the two, the token
	// compared is the older one and the answer is a rebuild.
	cur := s.targetsValidity(node)

	s.targetsMu.Lock()
	e, ok := s.targetsETags[node]
	s.targetsMu.Unlock()
	if !ok || !etagMatches(match, e.etag) {
		return false
	}

	var maxAge int
	if cur.wired && e.valid.wired {
		// The token is AUTHORITATIVE when it is available, in both directions.
		// A mismatch means rebuild — falling through to the clock here would
		// answer 304 for a list that provably changed, purely because the memo
		// happened to be young, which is the one thing this must never do.
		if e.valid != cur {
			return false
		}
		// Unchanged, so the copy is current as of NOW and the grant is the full
		// window — the same one a 200 hands out, through the same function and
		// therefore with the same rounding. builtAt moves with it so the
		// fallback below stays truthful if a source is later unwired.
		maxAge = s.maxAgeSeconds()
		now := s.now()
		s.targetsMu.Lock()
		if cached, still := s.targetsETags[node]; still && cached.etag == e.etag {
			cached.builtAt = now
			s.targetsETags[node] = cached
		}
		s.targetsMu.Unlock()
	} else {
		// No token: the wall-clock fallback. The remaining window, floored —
		// the client's expiry must not pass builtAt+TTL (the bound argued
		// above). A remainder under a second grants nothing, so the memo counts
		// as expired (which subsumes the age >= TTL check) and the store is
		// consulted.
		//
		// EXCEPT when the TTL is itself sub-second, where flooring can never
		// grant anything and the memo would be dead by arithmetic. That bound
		// is not expressible on this wire at all there — the 200 that seeded
		// the memo already rounded its own window up to a second — so matching
		// the 200 is both the honest reading and the only one under which this
		// branch does anything.
		remaining := s.cacheTTL - s.now().Sub(e.builtAt)
		maxAge = int(remaining / time.Second)
		if maxAge < 1 && remaining > 0 && s.cacheTTL < time.Second {
			maxAge = s.maxAgeSeconds()
		}
	}
	if maxAge < 1 {
		return false
	}
	obs.NodeTargetsBuilds.WithLabelValues("memo_hit").Inc()
	writeNotModified(w, e.etag, cacheControlFor(maxAge, false))
	return true
}

// rememberNodeTargets records what a node's freshly built target list is
// called, so the next revalidation can be answered without building it again
// (while its change token still matches — or, on the wall-clock fallback,
// inside the TTL).
func (s *Server) rememberNodeTargets(node, etag string, valid targetsValidity) {
	if etag == "" { // caching disabled, or the response failed to encode
		return
	}
	now := s.now()
	s.targetsMu.Lock()
	defer s.targetsMu.Unlock()
	if s.targetsETags == nil {
		s.targetsETags = make(map[string]nodeTargetsETag)
	}
	if _, held := s.targetsETags[node]; !held && len(s.targetsETags) >= maxNodeTargetETags {
		// The two-pass sweep maxNodeTargetETags describes. If it frees
		// nothing, skip the memo rather than grow.
		if !s.evictNodeTargetsLocked(func(k string, e nodeTargetsETag) bool {
			return s.nodeTargetsUnusable(k, e, valid, now)
		}) {
			s.evictNodeTargetsLocked(func(_ string, e nodeTargetsETag) bool {
				return now.Sub(e.builtAt) >= s.cacheTTL
			})
		}
		if len(s.targetsETags) >= maxNodeTargetETags {
			return
		}
	}
	s.targetsETags[node] = nodeTargetsETag{etag: etag, builtAt: now, valid: valid}
}

// evictNodeTargetsLocked deletes every memo entry drop selects, reporting
// whether it deleted any. Called with targetsMu held.
func (s *Server) evictNodeTargetsLocked(drop func(node string, e nodeTargetsETag) bool) bool {
	freed := false
	for k, e := range s.targetsETags {
		if drop(k, e) {
			delete(s.targetsETags, k)
			freed = true
		}
	}
	return freed
}

// nodeTargetsUnusable reports whether a memo entry can only ever produce a
// rebuild — the sweep's first pass (see maxNodeTargetETags).
//
// cur is the token the triggering build sampled. Tokens only advance, so an
// entry OLDER than cur in any component is provably stale whatever happened
// since; comparing by order rather than equality is what keeps an entry built
// after cur was sampled (a newer token, which is current) from being mistaken
// for a stale one. The pod component is the entry's OWN node's, read now: cur's
// is the triggering node's and says nothing about another node's pods.
// Called with targetsMu held; NodeGeneration takes the store's read lock, and
// nothing takes targetsMu under the store's lock, so the order is safe.
func (s *Server) nodeTargetsUnusable(node string, e nodeTargetsETag, cur targetsValidity, now time.Time) bool {
	if !cur.wired || !e.valid.wired {
		// The wall-clock fallback: past the TTL the memo is never consulted.
		return now.Sub(e.builtAt) >= s.cacheTTL
	}
	if e.valid.owners < cur.owners || e.valid.services < cur.services || e.valid.monitors < cur.monitors {
		return true
	}
	return e.valid.pods < s.store.NodeGeneration(node)
}
