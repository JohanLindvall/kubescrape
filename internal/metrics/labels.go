package metrics

import (
	"errors"
	"math/bits"
	"slices"
	"strings"

	"github.com/JohanLindvall/haste/xxh3"
)

// A metric's label set is a plain slice of key-value pairs. Order does not
// matter: series are keyed by an order-independent hash (see labels.hash). The
// slice representation is simple and, for the handful of labels a metric
// carries, at least as fast as a map.
type kv struct{ key, value string }

type labels []kv

// get returns the value for key and whether it was present.
func (l labels) get(key string) (string, bool) {
	for _, e := range l {
		if e.key == key {
			return e.value, true
		}
	}
	return "", false
}

// maxLabelValueBytes bounds ONE retained label value. The cardinality cap
// counts label COMBINATIONS, never bytes, so without this a single stream could
// retain megabytes: log-derived label values come from the LINE (a captured
// regex group, a JSON field, the synthetic __line__ key is the whole body), and
// they are held for maxAge — 24h by default. Ten thousand streams each holding
// a 1 MiB value is 10 GB inside a cap that reads as "10000 series".
//
// It is applied in set(), which is the one door every label value comes
// through, so the value that is HASHED is the value that is RENDERED. Cutting
// only the retained copy would be worse than not cutting: two long values
// differing past the cut would hash apart and render identical, i.e. one
// payload carrying the same label set twice. Same bound and same reasoning as
// cumagg.MaxLabelBytes, which the two span aggregators already apply.
const maxLabelValueBytes = 256

// truncLabelCut returns the byte length a value is bounded to — the exact cut
// truncLabelValue applies. It exists separately so the HASH paths
// (resourceAccum, resLabelsAccum) can fold v[:truncLabelCut(v)] — a reslice,
// no copy — and stay in step with what set() retains and renders: hashed and
// rendered identity must agree (see maxLabelValueBytes), and hashing the
// untruncated value while rendering the truncated one split one rendered
// identity into two live samples — duplicate same-timestamp points every
// export, and for histograms one merged point whose buckets summed both
// variants.
func truncLabelCut(v string) int {
	if len(v) <= maxLabelValueBytes {
		return len(v)
	}
	end := maxLabelValueBytes
	// Back off to the start of the rune that straddles the cut, so the value
	// stays valid UTF-8 rather than ending in a half-encoded code point.
	for end > 0 && v[end]&0xC0 == 0x80 {
		end--
	}
	if end == 0 {
		// The whole prefix is continuation bytes, i.e. the value was ALREADY
		// invalid UTF-8 and has no rune boundary to back off to. Cut at the
		// byte bound instead: nothing here can make it valid, and returning ""
		// would DROP the label — set() treats an empty value as absent — so a
		// series would silently lose a dimension and merge with another.
		end = maxLabelValueBytes
	}
	return end
}

// truncLabelValue bounds a value, cutting on a rune boundary and COPYING.
//
// The copy is the point. Slicing a Go string keeps its whole backing array
// alive, so a 256-byte view of a 1 MiB log line pins the megabyte for as long
// as the series lives — the exact leak the bound exists to prevent, arrived at
// through the fix for it. Only the truncating branch allocates; the
// overwhelmingly common short value is returned untouched.
func truncLabelValue(v string) string {
	if len(v) <= maxLabelValueBytes {
		return v
	}
	return string([]byte(v[:truncLabelCut(v)]))
}

// set appends or replaces key with value, returning the (possibly grown)
// slice. Empty keys and values are ignored, matching label semantics.
func (l labels) set(key, value string) labels {
	if key == "" || value == "" {
		return l
	}
	value = truncLabelValue(value)
	for i := range l {
		if l[i].key == key {
			l[i].value = value
			return l
		}
	}
	return append(l, kv{key, value})
}

// without returns the labels with key removed, order preserved. It allocates
// only when key is present.
func (l labels) without(key string) labels {
	for i := range l {
		if l[i].key == key {
			out := make(labels, 0, len(l)-1)
			out = append(out, l[:i]...)
			return append(out, l[i+1:]...)
		}
	}
	return l
}

// strHash is one string's 128-bit xxh3 hash — the input to every accumulator
// in this package.
//
// A series is keyed on ONE 128-bit accumulator, and this is why that is enough.
// The store used to key on a PAIR (a primary hash plus a "check" hash compared
// on every map hit, refusing a mismatch), because the primary was 64 bits and a
// 64-bit key genuinely can collide: at the hard cap of 10000 label combinations
// per metric the birthday bound is n^2/2^65 ~ 2.7e-12 per series map, small but
// not nothing across a fleet over years. At 128 bits the same bound is ~1.5e-31
// — multiplied by a million series maps, still 1e-25, which is many orders of
// magnitude below the rate at which the machine silently corrupts the value in
// RAM. A second accumulator guarding that is not defence in depth; it is a
// doubling of the per-label fold for an event that cannot happen.
//
// So the check went when the primary got wide, and the two changes belong
// together: keeping both would have been carrying the compensation and the fix
// at the same time. (The pair was also weaker than it looked — both halves were
// projections of the SAME two 64-bit xxhash values per label, so a collision in
// the STRING hash fed both an identical contribution and the check saw nothing.
// The claimed ~2^-128 only ever covered a collision arising in the combine.)
//
// xxh3 rather than xxhash64 because it is the faster algorithm on exactly the
// input this hashes — short strings, one per label key and value per
// observation — so the wider hash is cheaper than the narrow one it replaced,
// and dropping the second accumulator halves what remains: measured, the fold
// is 11.4% faster than the 64-bit pair it replaced.
//
// The observe path as a whole is nonetheless 2.2-6.2% SLOWER, and the cost is
// not here: it is series.db's key going from uint64 to a 16-byte struct. Go
// specialises map[uint64] to mapaccess*_fast64, and a comparable struct key
// falls back to the generic path. BenchmarkDynamicAddBound is the proof —
// observePreHashed uses a hash precomputed at construction and folds nothing,
// yet slowed 6.2%. Do not go looking for it in the hashing.
func strHash(s string) xxh3.Uint128 { return xxh3.Sum128String(s) }

// hashAccum is the order-independent accumulator of the label set: every entry
// contributes combineHash(hash(key), hash(value)) and they are XOR-folded.
//
// XOR rather than a wrapping sum. Both are order-independent and both are
// exactly uniform over uniform independent contributions — folding in any
// finite abelian group is — so this is not a distribution choice; measured over
// 400k realistic label sets x 8 populations x 2 bit windows, chi-square on the
// FINALIZED key (mixHash applied, which is what the map sees) gives mean |z|
// 0.65 for XOR against 0.51 for the sum, both at the ~0.80 a uniform
// distribution predicts, with the ordering inverting against the raw un-mixed
// fold. It is not a speed choice either: swapping the two moves nothing
// measurable (interleaved, n=8: LabelsAccums, ResourceAccum, DynamicAddAttrs,
// DynamicAddHistogram, DynamicAddBound all ~, geomean 0.13%). XOR is chosen for
// being SELF-INVERSE: the fold-out that resLabelsAccum needs is the same
// operation as the fold-in, so there is one primitive instead of a pair, and no
// carry/borrow to get right.
//
// THE PRICE, and it is the thing to know before touching any caller: XOR is
// blind to EVEN MULTIPLICITY. A contribution folded twice cancels to zero, so
// {k=v, k=v} hashes identically to {} — where a sum merely hashed it distinctly
// from {k=v}. The history here is not hypothetical: this fold WAS XOR, a pair
// reaching it twice silently merged distinct series, and it was changed to a
// sum for that reason. Note that a sum is not the fix it looks like — it stops
// the merge with {} and still hashes {k=v, k=v} apart from the {k="v"} it
// RENDERS, which is the milder historical failure (duplicate points every
// export, merged corrupt points for histograms) rather than none.
//
// What makes XOR safe is that no contribution can be folded twice, and every
// fold in this package now establishes that itself rather than asking its
// caller to:
//
//   - labels.set() replaces by key, so a label set cannot hold one key twice.
//     Every label fold — hashAccum here, resAccum, the registry's bound
//     wrappers, EmitDirect's label map — builds through it.
//   - resourceAccum ranges a pcommon.Map, whose keys are unique when the map is
//     agent-built and NOT when it arrived on the wire (OTLP encodes attributes
//     as a repeated KeyValue and pdata does not dedupe on decode). It proves
//     uniqueness as it folds and materializes the identity through set() when
//     it cannot — see there; it used to state the requirement as a contract on
//     the caller instead, and the contract had already leaked.
//
// Pinned by TestXorFoldIsSafeBecauseNoFoldCanRepeatAPair and, for the resource
// half, by FuzzResourceIdentityMatchesItsRender.
func (l labels) hashAccum() xxh3.Uint128 {
	var h xxh3.Uint128
	for _, e := range l {
		h = xor128(h, combineHash(strHash(e.key), strHash(e.value)))
	}
	return h
}

// hash is the finalized order-independent hash of the label set.
func (l labels) hash() xxh3.Uint128 { return mixHash(l.hashAccum()) }

// String serializes the labels into a normalized, key-sorted form such as
// `{a="1", b="2"}`. Empty values are dropped; quotes, backslashes and newlines
// are escaped so the result stays a single valid line. It is the inverse of
// parseLabels.
func (l labels) String() string {
	sorted := make(labels, 0, len(l))
	size := 2
	for _, e := range l {
		if e.value != "" {
			size += len(e.key) + len(e.value) + 5
			sorted = append(sorted, e)
		}
	}
	slices.SortFunc(sorted, func(a, b kv) int { return strings.Compare(a.key, b.key) })

	var sb strings.Builder
	sb.Grow(size)
	sb.WriteByte('{')
	for i, e := range sorted {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(escapeKey(e.key))
		sb.WriteString(`="`)
		sb.WriteString(escapeValue(e.value))
		sb.WriteByte('"')
	}
	sb.WriteByte('}')
	return sb.String()
}

func escapeValue(v string) string {
	if !strings.ContainsAny(v, "\"\\\n") {
		return v
	}
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `"`, `\"`)
	return strings.ReplaceAll(v, "\n", `\n`)
}

// escapeKey escapes a label key: the value escapes plus the key-terminating
// '=', the pair-separating ',' and an EDGE space. Data-point keys are
// DSL-restricted identifiers and never need it, but RESOURCE keys are arbitrary
// pcommon map keys from config (attrs templates, logattrs) — an unescaped '='
// in one made parseLabels cut the pair at the wrong place, silently renaming
// the exported attribute and mangling its value, and an unescaped edge space
// was eaten by the separator (String writes ", " between pairs, so parseLabels
// has to skip it). The hashed identity is the key as given while the rendered
// one had the space gone, so " env" and "env" were two series exporting
// byte-identical attributes.
func escapeKey(k string) string {
	if !strings.ContainsAny(k, "=,\"\\\n") && !hasEdgeSpace(k) {
		return k
	}
	var sb strings.Builder
	sb.Grow(len(k) + 4)
	for i := 0; i < len(k); i++ {
		switch k[i] {
		case '\\', '=', ',', '"':
			sb.WriteByte('\\')
			sb.WriteByte(k[i])
		case '\n':
			sb.WriteString(`\n`)
		case ' ':
			// Only at the edges: an interior space is unambiguous, and the
			// escape exists solely to keep the separator skip off the key.
			if i == 0 || i == len(k)-1 {
				sb.WriteByte('\\')
			}
			sb.WriteByte(' ')
		default:
			sb.WriteByte(k[i])
		}
	}
	return sb.String()
}

// hasEdgeSpace reports whether k starts or ends with the separator byte.
func hasEdgeSpace(k string) bool {
	return k != "" && (k[0] == ' ' || k[len(k)-1] == ' ')
}

// unescapeKey reverses escapeKey. Keys without a backslash return unchanged.
func unescapeKey(k string) string {
	if !strings.Contains(k, `\`) {
		return k
	}
	var sb strings.Builder
	sb.Grow(len(k))
	for i := 0; i < len(k); i++ {
		if k[i] == '\\' && i+1 < len(k) {
			i++
			if k[i] == 'n' {
				sb.WriteByte('\n')
			} else {
				sb.WriteByte(k[i])
			}
			continue
		}
		sb.WriteByte(k[i])
	}
	return sb.String()
}

// indexUnescapedEq returns the index of the first '=' not preceded by an
// (unconsumed) backslash escape, or -1.
func indexUnescapedEq(s string) int {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			i++ // skip the escaped byte
		case '=':
			return i
		}
	}
	return -1
}

var errBadLabelString = errors.New("invalid label string")

// parseLabels parses a `{k="v", ...}` string back into a key-sorted label set.
func parseLabels(in string) (labels, error) {
	if len(in) < 2 || in[0] != '{' || in[len(in)-1] != '}' {
		return nil, errBadLabelString
	}
	var out labels
	s := in[1 : len(in)-1]
	for s != "" {
		// Skip exactly the separator String writes (", "), not every leading
		// space: TrimSpace here ate a key's own leading space — and its trailing
		// one, leaving a dangling backslash when the key was escaped — so the
		// parsed key no longer equalled the key that was hashed.
		s = strings.TrimPrefix(s, " ")
		eq := indexUnescapedEq(s)
		if eq < 0 {
			return nil, errBadLabelString
		}
		key, rest := unescapeKey(s[:eq]), s[eq+1:]
		if key == "" {
			return nil, errBadLabelString
		}
		var value string
		value, s = scanLabelValue(rest)
		if value != "" {
			out = append(out, kv{key, value})
		}
	}
	slices.SortFunc(out, func(a, b kv) int { return strings.Compare(a.key, b.key) })
	return out, nil
}

// scanLabelValue reads one value off the front of s (starting just after the
// '='), returning the unescaped value and the remainder after its separating
// comma. Unquoted values run to the next comma; quoted values honour \\, \" and
// \n escapes.
func scanLabelValue(s string) (value, rest string) {
	// Fast path: no quote or escape before the next comma.
	if i := strings.IndexAny(s, "\\\","); i == -1 {
		return s, ""
	} else if s[i] == ',' {
		return s[:i], s[i+1:]
	}

	var sb strings.Builder
	var quoted, escaped bool
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case escaped:
			escaped = false
			if c == 'n' {
				c = '\n'
			}
		case c == '\\':
			escaped = true
			continue
		case c == '"':
			quoted = !quoted
			continue
		case !quoted && c == ',':
			return sb.String(), s[i+1:]
		}
		sb.WriteByte(c)
	}
	return sb.String(), ""
}

// xxhash finalization primes.
const (
	prime1 uint64 = 11400714785074694791
	prime2 uint64 = 14029467366897019727
	prime3 uint64 = 1609587929392839161
	prime4 uint64 = 9650029242287828579
	prime5 uint64 = 2870177450012600261
)

// xor128 is the fold. It is its OWN INVERSE, which is why there is no separate
// cancel operation: resLabelsAccum folds a replaced resource pair back out by
// folding it in again.
//
// The cost of that convenience is stated at hashAccum and is real: XOR is blind
// to even multiplicity, so any contribution folded twice vanishes. Nothing in
// this package may fold the same (domain, key, value) twice — every fold builds
// through set(), or proves the input key-unique before taking a shortcut past
// it (resourceAccum). That is the invariant, and it is the folds' own to keep;
// it used to be a contract on their callers.
func xor128(a, b xxh3.Uint128) xxh3.Uint128 {
	return xxh3.Uint128{Hi: a.Hi ^ b.Hi, Lo: a.Lo ^ b.Lo}
}

// combineHash folds a key's and a value's 128-bit hashes into one 128-bit
// contribution for the outer XOR fold.
//
// Both output halves depend on all 256 input bits: bits.Mul64 widens each
// 64x64 product to 128 bits and the two products are cross-folded, so a
// difference anywhere in either input reaches both halves. That is the point of
// carrying 128-bit string hashes at all — an intermediate form gave the (then
// two) accumulators one half of each hash each, which is only 64 bits of input
// per accumulator however wide the sum.
//
// The distinct primes break the (h1,h2)/(h2,h1) symmetry: multiplication is
// commutative, so XOR-ing a different prime into each operand is what keeps
// key=value from colliding with value=key. They also keep an all-zero pair from
// folding to zero — a contribution of 0 is invisible in a wrapping sum, merging
// two label sets that differ only by it. Unreachable in practice (it needs both
// string hashes to be exactly 0), but the seed is free.
//
// Series hashes are in-memory only (the db is rebuilt on restart; export
// identity is the labels themselves), so this formula carries no persistence
// constraint and can be changed whenever the arithmetic argues for it.
func combineHash(h1, h2 xxh3.Uint128) xxh3.Uint128 {
	loHi, loLo := bits.Mul64(h1.Lo^prime1, h2.Lo^prime2)
	hiHi, hiLo := bits.Mul64(h1.Hi^prime3, h2.Hi^prime4)
	return mixHash(xxh3.Uint128{Hi: loHi ^ hiLo, Lo: loLo ^ hiHi})
}

// A series key is the XOR fold of its resource pairs and its data-point label
// pairs. Folded with one combine, those two contributions would live in the
// SAME hash domain — and an order-independent fold cannot tell where a term
// came from. Two series built from the same multiset of (key, value) pairs,
// split differently between resource and labels, would then hash identically,
// silently merging distinct series. Concretely, with a rule lifting a line
// field named like a resource attribute: pod A (resource k8s.pod.name=a)
// logging peer=b and pod B (resource k8s.pod.name=b) logging peer=a collapse
// into one sample.
//
// This is the ONE separation the width does not make redundant, and the reason
// is worth keeping straight: it does not defend against a chance collision (128
// bits already does that) but against two genuinely different series producing
// the SAME multiset of terms, which no amount of width fixes because the inputs
// really are equal. combineResHash therefore folds a resource pair under
// different primes, giving the resource its own domain. resourceAccum and
// resLabelsAccum use it (a resourceLabel is lifted ONTO the resource, so it
// belongs to the resource domain and must cancel a resource key it overrides);
// data-point labels keep combineHash.
func combineResHash(h1, h2 xxh3.Uint128) xxh3.Uint128 {
	loHi, loLo := bits.Mul64(h1.Lo^prime4, h2.Lo^prime5)
	hiHi, hiLo := bits.Mul64(h1.Hi^prime1, h2.Hi^prime2)
	return mixHash(xxh3.Uint128{Hi: loHi ^ hiLo, Lo: loLo ^ hiHi})
}

// mixHash is the final avalanche over the whole 128-bit value. Each half is
// mixed with material from the other, so the two halves of a contribution
// cannot drift apart into what would effectively be two independent 64-bit
// hashes glued together.
func mixHash(h xxh3.Uint128) xxh3.Uint128 {
	h.Lo ^= h.Hi >> 33
	h.Lo *= prime2
	h.Hi ^= h.Lo >> 29
	h.Hi *= prime3
	h.Lo ^= h.Hi >> 32
	h.Lo *= prime1
	h.Hi ^= h.Lo >> 31
	h.Hi *= prime4
	h.Lo ^= h.Hi >> 27
	return h
}
