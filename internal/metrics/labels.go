package metrics

import (
	"errors"
	"math/bits"
	"slices"
	"strings"

	"github.com/cespare/xxhash/v2"
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

// set appends or replaces key with value, returning the (possibly grown)
// slice. Empty keys and values are ignored, matching label semantics.
func (l labels) set(key, value string) labels {
	if key == "" || value == "" {
		return l
	}
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

// hashAccum is the order-independent accumulator of the label set: every
// entry contributes combineHash(hash(key), hash(value)) and they are summed
// (wrapping). Summation is order-independent like XOR but not self-inverse
// under duplication — with XOR, the same key=value pair contributed twice
// (e.g. in both the data-point labels and the resource labels) cancelled to
// zero, silently merging distinct series. Addition still supports folding a
// single label in or out (subtract its combined hash), which the histogram
// observe path uses for the per-bucket "le".
func (l labels) hashAccum() uint64 {
	var h uint64
	for _, e := range l {
		h += combineHash(xxhash.Sum64String(e.key), xxhash.Sum64String(e.value))
	}
	return h
}

// checkAccum is a second, independently-mixed accumulator over the same label
// set. Series lookups compare it on every hash hit, so a residual 64-bit
// collision between different label multisets is detected instead of silently
// merging their data (both sums colliding at once is ~2^-128).
func (l labels) checkAccum() uint64 {
	var h uint64
	for _, e := range l {
		h += combineCheck(xxhash.Sum64String(e.key), xxhash.Sum64String(e.value))
	}
	return h
}

// accums returns both accumulators in one pass. Callers that need the pair —
// every series lookup does, to key the sample and to carry its collision check
// — must use this rather than calling hashAccum and checkAccum in turn: the
// expensive part of each entry is hashing the key and value strings, and the
// two accumulators fold the very same two hashes. Computing them once halves
// the string hashing on the observe path. The results are identical to calling
// the two separately (TestAccumsMatchSeparate).
func (l labels) accums() (h, check uint64) {
	for _, e := range l {
		hk, hv := xxhash.Sum64String(e.key), xxhash.Sum64String(e.value)
		h += combineHash(hk, hv)
		check += combineCheck(hk, hv)
	}
	return h, check
}

// hash is the finalized order-independent hash of the label set.
func (l labels) hash() uint64 { return mixHash(l.hashAccum()) }

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
		sb.WriteString(e.key)
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

var errBadLabelString = errors.New("invalid label string")

// parseLabels parses a `{k="v", ...}` string back into a key-sorted label set.
func parseLabels(in string) (labels, error) {
	if len(in) < 2 || in[0] != '{' || in[len(in)-1] != '}' {
		return nil, errBadLabelString
	}
	var out labels
	s := in[1 : len(in)-1]
	for s != "" {
		key, rest, ok := strings.Cut(s, "=")
		if !ok {
			return nil, errBadLabelString
		}
		key = strings.TrimSpace(key)
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

// combineHash folds two 64-bit hashes into one. The inputs are already xxhash
// outputs (uniformly mixed), so a linear projection — distinct odd multipliers
// plus a rotation break the (h1,h2)/(h2,h1) symmetry — followed by one
// avalanche disperses the pair for the outer wrapping sum. Series hashes are
// in-memory only (the db is rebuilt on restart; export identity is the labels
// themselves), so this formula carries no persistence constraint.
func combineHash(h1, h2 uint64) uint64 {
	return mixHash(h1*prime1 + bits.RotateLeft64(h2, 29)*prime2)
}

// combineCheck is a second projection of the same pair for the collision-check
// accumulator, independent of combineHash (different multipliers, swapped
// operands, different rotation): a pair collision in one projection is not a
// collision in the other, keeping the double-collision odds at ~2^-128.
func combineCheck(h1, h2 uint64) uint64 {
	return mixHash(h2*prime3 + bits.RotateLeft64(h1, 47)*prime4)
}

// mixHash performs the final xxhash avalanche on h.
func mixHash(h uint64) uint64 {
	h ^= h >> 33
	h *= prime2
	h ^= h >> 29
	h *= prime3
	h ^= h >> 32
	return h
}
