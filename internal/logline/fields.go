package logline

import (
	"strings"
	"unsafe"

	ljson "github.com/JohanLindvall/lightning/pkg/json"
	"github.com/JohanLindvall/logfmt"

	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
)

// LineKey is the synthetic label key that resolves to the whole raw line, so
// selectors and labels can reference the line contents directly.
const LineKey = "__line__"

// ResolveKey is THE attrs-then-line-fields resolution tier: LineKey is the
// raw body, the caller's attribute lookup (record/resource attributes) wins
// when it returns non-empty, and the line's own parsed fields are the
// fallback. It was spelled once here (the rules engine's filterCtx) and once
// in internal/metrics (the log-metrics engine's addContext) — resolution-
// order policy with two places for a future synthetic key or a change to the
// empty-means-absent rule to land, the exact drift mode the logchain resolver
// consolidation ended for the tier above this one.
//
// VALUE-based, not presence-based: a present-but-empty attribute cannot
// shadow a line field. That is inherent in the func(string) string lookup
// signature the zero-alloc paths require, and deliberately unlike the
// resolver's internal record→lifted→resource ranking.
//
// A plain function over pieces both callers already hold — not a struct, not
// a closure — so the pooled contexts keep their shapes and the pinned
// per-line budgets (TestDynamicAddAllocationBudget, the tailer's flush
// budget) are untouched.
func ResolveKey(key, raw string, lookup func(string) string, ki *KeyIndex, lf *Fields) string {
	if key == LineKey {
		return raw
	}
	if lookup != nil {
		if v := lookup(key); v != "" {
			return v
		}
	}
	return ki.Get(lf, key)
}

// Fields lazily extracts the referenced fields from a JSON or logfmt log
// line so metric label/value keys can read straight from the line, with no
// separate logAttributes config. Only the keys the set references are parsed
// (paths held on the set); parsing happens once per line, on the first miss.
//
// Extracted values live in a SLOT SLICE parallel to the owning KeyIndex's keys,
// not in a map: a map keyed by the line's own bytes cost one string allocation
// per referenced key per line (`values[string(key)] = ...` allocates, unlike
// the lookup form), which the JSON path never paid because it indexes by the
// key's position. Both formats now write by slot.
type Fields struct {
	line   string
	vals   []string // per-slot extracted values (see KeyIndex.want)
	raws   [][]byte // reused GetPaths output buffer
	parsed bool
}

func (lf *Fields) Reset(line string) {
	lf.line = line
	lf.parsed = false
	clear(lf.vals)
}

// Release drops every reference into the last line before the holder is
// pooled: the line string itself, the extracted values (which may alias it)
// and the reused GetPaths buffer's raw views. Clearing lookup state alone
// released nothing — the pooled holder pinned the last-processed line (up to
// one max-size multiline entry) until its next use or a GC pool eviction.
func (lf *Fields) Release() {
	lf.line = ""
	lf.parsed = false
	clear(lf.vals)
	clear(lf.raws)
}

// NewKeyIndex builds an empty index; callers Add every key their rules read.
func NewKeyIndex() KeyIndex {
	return KeyIndex{want: map[string]int{}}
}

// KeyIndex holds, for a DynamicMetricSet, the distinct line-field keys its
// rules reference and their dotted JSON paths (parallel slices). want maps each
// key to its SLOT — its index in keys/paths, and hence in Fields.vals — so the
// logfmt scan can store a value found under the line's own bytes without
// materializing them as a string. Precomputed once, not per line.
type KeyIndex struct {
	keys  []string
	paths [][]string
	want  map[string]int
}

// Add registers one referenced field key (idempotent; the empty key and the
// synthetic LineKey are skipped — LineKey is never a line FIELD). The literal
// "1" is NOT special here: it is a value-DSL constant (metricRule.readValue
// short-circuits it before consulting values), and skipping it in the shared
// index silently deadened a selector or label legitimately reading a field
// named "1" — the classification belongs at the value call site
// (buildKeyIndex), not in the index every key kind shares.
func (ki *KeyIndex) Add(key string) {
	if key == "" || key == LineKey {
		return
	}
	if _, ok := ki.want[key]; ok {
		return
	}
	ki.want[key] = len(ki.keys)
	ki.keys = append(ki.keys, key)
	ki.paths = append(ki.paths, strings.Split(key, "."))
}

// Empty reports whether no rule reads any line field.
func (ki KeyIndex) Empty() bool { return len(ki.keys) == 0 }

// Get returns the value of key from the line, parsing it on first use.
func (ki KeyIndex) Get(lf *Fields, key string) string {
	if ki.Empty() {
		return ""
	}
	slot, ok := ki.want[key]
	if !ok {
		return "" // not a key any rule registered; the line was never scanned for it
	}
	if !lf.parsed {
		if len(lf.vals) < len(ki.keys) {
			lf.vals = make([]string, len(ki.keys))
		}
		ki.Parse(lf)
		lf.parsed = true
	}
	return lf.vals[slot]
}

// Parse fills lf.vals (by slot) with the referenced keys from the line: JSON
// when it starts with '{', otherwise logfmt (flat keys only).
//
// DUPLICATE KEYS resolve differently by format, and deliberately stay that way:
// JSON keeps the FIRST occurrence (lightning's GetPaths fills a path's slot
// only while it is still nil — its documented contract, and re-resolving it
// would mean a second pass over the line) while the logfmt scan below keeps the
// LAST (the callback writes the slot on every match). So `{"level":"info",
// "level":"warn"}` reads info and `level=info level=warn` reads warn. Both
// inputs are malformed, and neither format's reader dictates an answer — the
// logfmt reader's own Get resolves first-NON-EMPTY-wins, a third rule again —
// so unifying it would mean inventing a rule here and keeping the twin
// extractor in pkg/logattrs (which has the identical asymmetry, for the
// identical reason) in step with it forever. It is written down instead.
func (ki KeyIndex) Parse(lf *Fields) {
	if t := strings.TrimSpace(lf.line); strings.HasPrefix(t, "{") {
		// Read-only view: GetPaths only reads the buffer; its outputs alias it.
		buf := unsafe.Slice(unsafe.StringData(t), len(t))
		raws, err := ljson.GetPaths(buf, ki.paths, lf.raws)
		lf.raws = raws
		if err != nil {
			return
		}
		for i, raw := range raws {
			if len(raw) == 0 {
				continue
			}
			if s, ok := RawScalarString(raw); ok {
				lf.vals[i] = s // raws is parallel to ki.paths, hence to the slots
			}
		}
		return
	}
	// A line with no '=' carries no logfmt pair; the scan is skipped as a pure
	// fast path, which is only sound because bare keys resolve to nothing (see
	// the IsBareKey guard below).
	if strings.IndexByte(lf.line, '=') < 0 {
		return
	}
	buf := unsafe.Slice(unsafe.StringData(lf.line), len(lf.line))
	_ = logfmt.Iterate(buf, func(key, val []byte) bool {
		// map[string(bytes)] lookup: no allocation, and the slot it yields is
		// where the value goes — storing under a materialized key string was an
		// allocation per referenced key per line.
		slot, ok := ki.want[string(key)]
		if !ok {
			return true
		}
		// A BARE key yields the sentinel "true", which is prose, not a field:
		// `weight=10 disk error` would resolve error="true" and fire a selector
		// written as `error=true`. Worse, whether the words are seen at all
		// depends on an unrelated '=' elsewhere on the line (the fast path
		// above), so the same sentence resolves differently depending on its
		// neighbours.
		if logfmt.IsBareKey(val) {
			return true
		}
		// Iterate yields RAW values (quotes stripped, escapes intact).
		// Decode them so `msg="a \"b\""` reads as `a "b"` — the JSON path
		// unescapes, and the same logical value must not match selectors
		// or mint label values differently depending on the line format.
		// QUOTED values only: an unquoted `path=C:\logs\app.log` holds no
		// escapes, and decoding it deleted the backslashes — or, for a
		// recognised letter, minted a label value with a real newline in
		// it. logattrs.DecodeLogfmtValue is the shared decision and stays
		// the one that makes it.
		//
		// Its no-escape answer is the raw bytes verbatim, whatever the
		// quoting, so that case is served with a read-only view into the
		// line instead of a copy — the same aliasing the JSON arm above
		// relies on (lightning never mutates its input, and lf.line owns
		// the backing array for as long as these values are valid).
		if !logfmt.NeedsUnescape(val) {
			lf.vals[slot] = unsafe.String(unsafe.SliceData(val), len(val))
		} else {
			lf.vals[slot] = logattrs.DecodeLogfmtValue(buf, val)
		}
		return true
	})
}

// RawScalarString renders a raw JSON scalar token as a string; objects, arrays
// and null are rejected. It matches what DecodeAny + a type switch produced
// (numbers round-trip through float64) without boxing the value in an any.
// Integral-token classification is logattrs.IsIntegerToken, shared with the
// twin decoder there (logattrs.decodeScalar) so one line field cannot classify
// differently as a label and as a lifted attribute.
func RawScalarString(raw []byte) (string, bool) {
	if len(raw) == 0 {
		return "", false // shape check at the parse seam; GetPaths never yields this
	}
	switch raw[0] {
	case '"':
		if len(raw) < 2 || raw[len(raw)-1] != '"' {
			return "", false
		}
		s, err := ljson.UnescapeString(raw[1 : len(raw)-1])
		return s, err == nil
	case 't':
		if string(raw) == "true" { // comparison does not allocate
			return "true", true
		}
		return "", false
	case 'f':
		if string(raw) == "false" {
			return "false", true
		}
		return "", false
	case '{', '[', 'n':
		return "", false
	default: // number
		// Integral numbers render EXACTLY. float64 cannot hold a 64-bit id, so
		// routing every number through it collapsed adjacent snowflake ids into
		// one logMetrics series while the record attribute lifted from the same
		// field (pkg/logattrs.decodeScalar, fixed for this) stayed exact. A
		// number token is already the decimal text: if it has no fraction or
		// exponent, it IS the value. (Known residual divergence, accepted: a
		// token exceeding int64 renders verbatim here but rounds through
		// float64 in decodeScalar — see IsIntegerToken's doc.)
		if logattrs.IsIntegerToken(raw) {
			return string(raw), true
		}
		f, err := ljson.ParseFloat(raw)
		if err != nil {
			return "", false
		}
		// logattrs.FloatString, not FormatFloat('f'): the SAME field reaches an
		// exported string through three paths — this one (a line field read
		// straight off the line), a record attribute (put on pdata, read back by
		// AsString) and a lifted resource attribute (logchain's attrString) — and
		// the other two render ES6. Fixed-point here meant "0.0000005" where they
		// said "5e-7", so adding or removing an unrelated logAttributes rule for
		// that key silently renamed every series labelled by it, and a selector
		// written for one spelling stopped matching.
		return logattrs.FloatString(f), true
	}
}
