package logline

import (
	"strconv"
	"strings"
	"unsafe"

	ljson "github.com/JohanLindvall/lightning/pkg/json"
	"github.com/JohanLindvall/logfmt"

	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
)

// LineKey is the synthetic label key that resolves to the whole raw line, so
// selectors and labels can reference the line contents directly.
const LineKey = "__line__"

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

// KeyIndex holds, for a DynamicMetricSet, the distinct line-field keys its
// rules reference and their dotted JSON paths (parallel slices). want maps each
// key to its SLOT — its index in keys/paths, and hence in Fields.vals — so the
// logfmt scan can store a value found under the line's own bytes without
// materializing them as a string. Precomputed once, not per line.
// NewKeyIndex builds an empty index; callers Add every key their rules read.
func NewKeyIndex() KeyIndex {
	return KeyIndex{want: map[string]int{}}
}

type KeyIndex struct {
	keys  []string
	paths [][]string
	want  map[string]int
}

// add registers one referenced field key (idempotent; synthetic keys and
// literals are skipped).
func (ki *KeyIndex) Add(key string) {
	if key == "" || key == "1" || key == LineKey {
		return
	}
	if _, ok := ki.want[key]; ok {
		return
	}
	ki.want[key] = len(ki.keys)
	ki.keys = append(ki.keys, key)
	ki.paths = append(ki.paths, strings.Split(key, "."))
}

// empty reports whether no rule reads any line field.
func (ki KeyIndex) Empty() bool { return len(ki.keys) == 0 }

// get returns the value of key from the line, parsing it on first use.
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

// parse fills lf.vals (by slot) with the referenced keys from the line: JSON
// when it starts with '{', otherwise logfmt (flat keys only).
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

// isIntegerToken reports whether a JSON number token is a plain integer (no
// fraction, no exponent), so its text can be used verbatim.
func isIntegerToken(raw []byte) bool {
	if len(raw) == 0 {
		return false
	}
	i := 0
	if raw[0] == '-' {
		i = 1
	}
	if i == len(raw) {
		return false
	}
	for ; i < len(raw); i++ {
		if raw[i] < '0' || raw[i] > '9' {
			return false // '.', 'e', 'E' — not integral
		}
	}
	return true
}

// RawScalarString renders a raw JSON scalar token as a string; objects, arrays
// and null are rejected. It matches what DecodeAny + a type switch produced
// (numbers round-trip through float64) without boxing the value in an any.
func RawScalarString(raw []byte) (string, bool) {
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
		// exponent, it IS the value.
		if isIntegerToken(raw) {
			return string(raw), true
		}
		f, err := ljson.ParseFloat(raw)
		if err != nil {
			return "", false
		}
		return strconv.FormatFloat(f, 'f', -1, 64), true
	}
}
