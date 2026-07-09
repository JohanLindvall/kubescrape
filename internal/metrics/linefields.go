package metrics

import (
	"strconv"
	"strings"
	"unsafe"

	ljson "github.com/JohanLindvall/lightning/pkg/json"
	"github.com/JohanLindvall/logfmt"
)

// lineFields lazily extracts the referenced fields from a JSON or logfmt log
// line so metric label/value keys can read straight from the line, with no
// separate logAttributes config. Only the keys the set references are parsed
// (paths held on the set); parsing happens once per line, on the first miss.
type lineFields struct {
	line   string
	values map[string]string
	parsed bool
}

func (lf *lineFields) reset(line string) {
	lf.line = line
	lf.parsed = false
	for k := range lf.values {
		delete(lf.values, k)
	}
}

// keyIndex holds, for a DynamicMetricSet, the distinct line-field keys its
// rules reference and their dotted JSON paths (parallel slices).
type keyIndex struct {
	keys  []string
	paths [][]string
}

// newKeyIndex collects the distinct field keys referenced across rules: label
// getters, the observed value, and selector labels.
func newKeyIndex(rules []*metricRule) keyIndex {
	seen := map[string]bool{}
	var ki keyIndex
	add := func(key string) {
		if key == "" || key == "1" || key == lineKey || seen[key] {
			return
		}
		seen[key] = true
		ki.keys = append(ki.keys, key)
		ki.paths = append(ki.paths, strings.Split(key, "."))
	}
	for _, r := range rules {
		add(r.value)
		for _, lt := range r.labels {
			add(lt.getKey)
		}
		for _, key := range r.match.labelKeys() {
			add(key)
		}
	}
	return ki
}

// empty reports whether no rule reads any line field.
func (ki keyIndex) empty() bool { return len(ki.keys) == 0 }

// get returns the value of key from the line, parsing it on first use.
func (ki keyIndex) get(lf *lineFields, key string) string {
	if ki.empty() {
		return ""
	}
	if !lf.parsed {
		if lf.values == nil {
			lf.values = make(map[string]string, len(ki.keys))
		}
		ki.parse(lf)
		lf.parsed = true
	}
	return lf.values[key]
}

// parse fills lf.values with the referenced keys from the line: JSON when it
// starts with '{', otherwise logfmt (flat keys only).
func (ki keyIndex) parse(lf *lineFields) {
	if t := strings.TrimSpace(lf.line); strings.HasPrefix(t, "{") {
		raws, err := ljson.GetPaths([]byte(t), ki.paths, nil)
		if err != nil {
			return
		}
		for i, raw := range raws {
			if raw == nil {
				continue
			}
			if v, err := ljson.DecodeAny(raw); err == nil {
				if s, ok := scalarString(v); ok {
					lf.values[ki.keys[i]] = s
				}
			}
		}
		return
	}
	if strings.IndexByte(lf.line, '=') < 0 {
		return
	}
	want := map[string]bool{}
	for _, k := range ki.keys {
		want[k] = true
	}
	buf := unsafe.Slice(unsafe.StringData(lf.line), len(lf.line))
	_ = logfmt.Iterate(buf, func(key, val []byte) bool {
		if want[string(key)] {
			lf.values[string(key)] = string(val)
		}
		return true
	})
}

// scalarString renders a lightning-decoded scalar as a string; objects, arrays
// and null are rejected.
func scalarString(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case bool:
		return strconv.FormatBool(t), true
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), true
	default:
		return "", false
	}
}
