package servicegraph

// The shard ring: which shard owns a trace.
//
// Pairing a request's two halves is only local if both halves reach the same
// process, so the routing key must be the TRACE ID — the one thing a client
// span and its server span always agree on. Everything here exists to turn a
// trace id into exactly one shard name, identically in every agent, without
// any coordination between them.

import (
	"sort"
)

// DefaultTokensPerShard is how many virtual tokens each shard places on the
// circle. Cortex/Tempo use 128 per ingester; this is higher because the shard
// tier here is small (single digits, not dozens) and its tokens are FIXED, so
// a bad draw cannot be re-registered away — it is that tier's permanent skew.
// The imbalance of a consistent-hash ring falls as ~1/sqrt(tokens), and the
// measured spread across 4/8/16 shards (TestTokensPerShardTightensDistribution
// and the sweep behind it) is: 128 tokens → 12-35% between the lightest and
// heaviest shard, 512 → 12-20%, 1024 → 3-9%. Tokens cost one uint32 plus one
// int32 each, built once at startup and then read lock-free forever (8 shards
// = 64 KiB), so buying the variance down is nearly free here in a way it is
// not for a 200-ingester Cortex ring.
const DefaultTokensPerShard = 1024

// FNV-1 32-bit parameters (NOT FNV-1a: the multiply comes first). This is
// hash/fnv's New32; it is spelled out here because TokenFor is on the
// per-span path and hash.Hash32 costs an allocation and an interface call per
// use. TestTokenForMatchesStdlib pins the two together.
const (
	fnvOffset32 = 2166136261
	fnvPrime32  = 16777619
)

// TokenFor is Grafana Tempo's util.TokenFor (pkg/util/hash.go): FNV-1 32-bit
// over the tenant bytes followed by the trace-id bytes.
//
// It is reproduced rather than approximated because the DISTRIBUTION is the
// contract: an operator moving a workload from Tempo's metrics-generator to
// this shard tier should see the same traces land in the same relative
// proportions, and any future comparison against Tempo (why does that shard
// run hot?) is only meaningful if the key function is identical. It is also
// the hash Tempo has field-validated on real trace-id generators, several of
// which are far from uniform in their low bytes.
//
// The tenant argument is Tempo's multi-tenancy: a tenant's traces hash into
// their own positions on the shared ring. kubescrape has no tenant of its own
// (routing tenancy is a header on the way OUT, applied after pairing), so
// Ring.Owner passes "" — but the parameter stays, both for fidelity with
// Tempo and because a per-tenant shard tier is expressible without changing
// the key function.
func TokenFor(tenant string, traceID []byte) uint32 {
	h := uint32(fnvOffset32)
	for i := 0; i < len(tenant); i++ {
		h *= fnvPrime32
		h ^= uint32(tenant[i])
	}
	for _, c := range traceID {
		h *= fnvPrime32
		h ^= uint32(c)
	}
	return h
}

// Ring maps a trace id to the shard that owns it: each shard places
// tokensPerShard virtual tokens on the uint32 circle, and a key is owned by
// the first token at or after it, walking clockwise and wrapping past 2^32-1.
// The replication factor is 1 — one owner per trace, no quorum, no
// replication. A replica would not buy availability here: the pairing state is
// in memory and lost with the process either way, so a second copy of every
// span would double the wire cost to shorten one Wait window's outage.
//
// # Deliberate difference from Tempo
//
// Tempo's instances generate RANDOM tokens at startup and publish them to
// each other over memberlist gossip; the ring is a shared, mutable,
// eventually-consistent document. Here the shard set is a fixed ordinal
// StatefulSet, so each shard's tokens are derived DETERMINISTICALLY from its
// name (a PRNG seeded by a hash of the name — statistically the same random
// placement Tempo gets, just reproducible). Same ring structure, same lookup,
// same distribution; no gossip layer at all.
//
// Why that is strictly better in this deployment:
//
//   - No gossip means no membership protocol to run, no ports to open between
//     shards, no RBAC, and no window where two agents hold different rings and
//     send the two halves of one request to two different shards. With derived
//     tokens every agent computes the identical ring from the identical config
//     — disagreement is impossible unless the CONFIG disagrees.
//   - It is testable and reviewable: the ring is a pure function of (names,
//     tokens), so a distribution regression is a unit test rather than a
//     production observation.
//   - A restarted shard resumes its own token positions, so a pod restart
//     shuffles nothing. In Tempo a restarted instance draws fresh random
//     tokens and re-partitions the circle.
//
// What it costs:
//
//   - No rebalancing beyond scale changes. Tokens are fixed by name, so a
//     shard that runs hot (one enormous trace-id-skewed tenant) cannot be
//     re-weighted by re-registering; the only levers are tokensPerShard
//     (which re-derives EVERY shard's positions, moving a large fraction of
//     the keys once) and the replica count.
//   - The membership is whatever the config says, not what is alive. A dead
//     shard keeps its arc of the circle and its traces are simply not paired
//     until it returns — where gossip would hand its range to a neighbour.
//     That is the intended trade for a best-effort, opt-in graph: a resharding
//     stampede on every rolling restart would corrupt more edges (every
//     in-flight half-edge on both the losing and gaining shard) than a brief
//     gap loses.
//   - Scaling the StatefulSet is a config change agents must see. Until they
//     do, they address the old shard set — correctly, just at the old width.
//     A shard added or removed moves ~1/N of the keys and nothing else, which
//     is the consistent-hashing property this structure exists for.
type Ring struct {
	shards []string // sorted, deduplicated
	tokens []uint32 // ascending, deduplicated
	owner  []int32  // owner[i] indexes shards, for tokens[i]
}

// NewRing builds the ring for shards. Input order does not matter (names are
// sorted and deduplicated first) — the ring must not depend on how a caller
// happened to render its config, since two agents disagreeing about the ring
// silently splits traces. Empty names are ignored; tokensPerShard <= 0 takes
// DefaultTokensPerShard. An empty shard set yields a ring whose Owner is
// always "" (the caller forwards nothing).
func NewRing(shards []string, tokensPerShard int) *Ring {
	if tokensPerShard <= 0 {
		tokensPerShard = DefaultTokensPerShard
	}
	names := append([]string(nil), shards...)
	sort.Strings(names)
	names = dedupeStrings(names)
	r := &Ring{shards: names}
	if len(names) == 0 {
		return r
	}
	type entry struct {
		token uint32
		owner int32
	}
	all := make([]entry, 0, len(names)*tokensPerShard)
	for i, name := range names {
		for _, t := range shardTokens(name, tokensPerShard) {
			all = append(all, entry{token: t, owner: int32(i)})
		}
	}
	// Ties broken by owner index (i.e. by sorted shard name) so that two
	// shards colliding on a token — a 1-in-2^32 event per pair, but a certainty
	// somewhere across a fleet's lifetime — still yield one deterministic ring
	// everywhere. The duplicate is then dropped: a token that appears twice
	// would make the binary search's answer depend on which copy it landed on.
	sort.Slice(all, func(i, j int) bool {
		if all[i].token != all[j].token {
			return all[i].token < all[j].token
		}
		return all[i].owner < all[j].owner
	})
	r.tokens = make([]uint32, 0, len(all))
	r.owner = make([]int32, 0, len(all))
	for i, e := range all {
		if i > 0 && e.token == all[i-1].token {
			continue
		}
		r.tokens = append(r.tokens, e.token)
		r.owner = append(r.owner, e.owner)
	}
	return r
}

// Owner returns the shard owning traceID, or "" when the ring is empty.
func (r *Ring) Owner(traceID []byte) string {
	return r.OwnerForTenant("", traceID)
}

// OwnerForTenant is Owner under Tempo's tenant-qualified key (see TokenFor).
func (r *Ring) OwnerForTenant(tenant string, traceID []byte) string {
	if len(r.tokens) == 0 {
		return ""
	}
	return r.shards[r.owner[r.index(TokenFor(tenant, traceID))]]
}

// index is the ring position owning key: the first token >= key, wrapping to
// the first token when key is past the last one. sort.Search is a branch-light
// binary search over a slice that never changes after construction, so this is
// lock-free and allocation-free on the per-span path.
func (r *Ring) index(key uint32) int {
	i := sort.Search(len(r.tokens), func(i int) bool { return r.tokens[i] >= key })
	if i == len(r.tokens) {
		return 0 // wrapped past the largest token
	}
	return i
}

// Shards returns the ring's shard names, sorted and deduplicated.
func (r *Ring) Shards() []string {
	return append([]string(nil), r.shards...)
}

// Ownership reports the EXACT fraction of the key space each shard owns,
// summing to 1. It is computed from the token arcs rather than by sampling, so
// a test (or a startup log line) sees the ring's real balance rather than one
// draw from it — the sampled distribution converges to this, and any gap
// between them is the sample's noise, not the ring's.
func (r *Ring) Ownership() map[string]float64 {
	out := make(map[string]float64, len(r.shards))
	for _, s := range r.shards {
		out[s] = 0
	}
	n := len(r.tokens)
	if n == 0 {
		return out
	}
	const space = float64(1 << 32)
	if n == 1 {
		out[r.shards[r.owner[0]]] = 1
		return out
	}
	for i := 0; i < n; i++ {
		prev := r.tokens[(i-1+n)%n]
		// Token i owns (tokens[i-1], tokens[i]]; the subtraction wraps for i=0,
		// which is exactly the arc crossing zero.
		arc := uint64(r.tokens[i] - prev)
		out[r.shards[r.owner[i]]] += float64(arc) / space
	}
	return out
}

// shardTokens derives a shard's virtual tokens from its NAME alone.
//
// The tokens are a PRNG stream (SplitMix32) seeded with a hash of the name:
// statistically indistinguishable from the random tokens Tempo's instances
// register, but a pure function of the name, so every agent and every restart
// computes the same positions. A plain hash of name+index was the obvious
// alternative and is worse: consecutive indices differ in one low byte, which
// FNV mixes only through the final multiply, so the tokens of one shard come
// out visibly correlated and the arcs cluster.
func shardTokens(name string, n int) []uint32 {
	state := fnv1a32(name)
	out := make([]uint32, n)
	for i := range out {
		state += 0x9e3779b9 // the 32-bit golden-ratio increment; the Weyl sequence SplitMix walks
		out[i] = mix32(state)
	}
	return out
}

// mix32 is SplitMix32's finalizer: an avalanche mix, so the low-entropy
// counter above becomes uniformly distributed positions.
func mix32(z uint32) uint32 {
	z ^= z >> 16
	z *= 0x21f0aaad
	z ^= z >> 15
	z *= 0x735a2d97
	z ^= z >> 15
	return z
}

// fnv1a32 seeds the token stream. FNV-1a (xor first) rather than the FNV-1 of
// TokenFor purely to keep the two uses visibly distinct: this one is a seed
// derivation and has no compatibility contract with Tempo.
func fnv1a32(s string) uint32 {
	h := uint32(fnvOffset32)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= fnvPrime32
	}
	return h
}

func dedupeStrings(sorted []string) []string {
	out := sorted[:0]
	for i, s := range sorted {
		if s == "" || (i > 0 && s == sorted[i-1]) {
			continue
		}
		out = append(out, s)
	}
	return out
}
