package logline

import (
	"strconv"
	"strings"
	"unsafe"

	ljson "github.com/JohanLindvall/lightning/pkg/json"
	"github.com/JohanLindvall/logfmt"
)

// LineKey is the synthetic label key that resolves to the whole raw line, so
// selectors and labels can reference the line contents directly.
const LineKey = "__line__"

// Fields lazily extracts the referenced fields from a JSON or logfmt log
// line so metric label/value keys can read straight from the line, with no
// separate logAttributes config. Only the keys the set references are parsed
// (paths held on the set); parsing happens once per line, on the first miss.
type Fields struct {
	line   string
	values map[string]string
	raws   [][]byte // reused GetPaths output buffer
	parsed bool
}

func (lf *Fields) Reset(line string) {
	lf.line = line
	lf.parsed = false
	for k := range lf.values {
		delete(lf.values, k)
	}
}

// KeyIndex holds, for a DynamicMetricSet, the distinct line-field keys its
// rules reference and their dotted JSON paths (parallel slices). want mirrors
// keys as a set for the logfmt scan (precomputed once, not per line).
// NewKeyIndex builds an empty index; callers Add every key their rules read.
func NewKeyIndex() KeyIndex {
	return KeyIndex{want: map[string]bool{}}
}

type KeyIndex struct {
	keys  []string
	paths [][]string
	want  map[string]bool
}

// add registers one referenced field key (idempotent; synthetic keys and
// literals are skipped).
func (ki *KeyIndex) Add(key string) {
	if key == "" || key == "1" || key == LineKey || ki.want[key] {
		return
	}
	ki.want[key] = true
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
	if !lf.parsed {
		if lf.values == nil {
			lf.values = make(map[string]string, len(ki.keys))
		}
		ki.Parse(lf)
		lf.parsed = true
	}
	return lf.values[key]
}

// parse fills lf.values with the referenced keys from the line: JSON when it
// starts with '{', otherwise logfmt (flat keys only).
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
				lf.values[ki.keys[i]] = s
			}
		}
		return
	}
	if strings.IndexByte(lf.line, '=') < 0 {
		return
	}
	buf := unsafe.Slice(unsafe.StringData(lf.line), len(lf.line))
	_ = logfmt.Iterate(buf, func(key, val []byte) bool {
		if ki.want[string(key)] {
			lf.values[string(key)] = string(val)
		}
		return true
	})
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
		f, err := ljson.ParseFloat(raw)
		if err != nil {
			return "", false
		}
		return strconv.FormatFloat(f, 'f', -1, 64), true
	}
}
