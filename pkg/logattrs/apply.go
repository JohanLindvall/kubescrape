package logattrs

import (
	"strconv"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Put sets each attribute on m with its decoded type. Existing keys are
// overwritten — the rule was configured deliberately.
func Put(m pcommon.Map, attrs []Attr) {
	for _, a := range attrs {
		switch v := a.Val.(type) {
		case string:
			m.PutStr(a.Key, v)
		case bool:
			m.PutBool(a.Key, v)
		case float64:
			if v == float64(int64(v)) {
				m.PutInt(a.Key, int64(v))
			} else {
				m.PutDouble(a.Key, v)
			}
		case int64: // per the Attr contract; DecodeAny yields float64 today
			m.PutInt(a.Key, v)
		}
	}
}

// Key returns a stable identity string for a set of attributes, used to group
// records that share the same resource- or scope-level attributes. The order
// is the deterministic rule order, so equal attribute sets yield equal keys.
func Key(attrs []Attr) string {
	if len(attrs) == 0 {
		return ""
	}
	var b strings.Builder
	for _, a := range attrs {
		b.WriteString(a.Key)
		b.WriteByte('=')
		// Tag the type and quote strings so differently-typed values ("1" vs
		// 1) or values containing the separators cannot alias to the same key
		// and merge records into one mis-attributed resource/scope.
		switch v := a.Val.(type) {
		case string:
			b.WriteByte('s')
			b.WriteString(strconv.Quote(v))
		case bool:
			b.WriteByte('b')
			b.WriteString(strconv.FormatBool(v))
		case float64:
			b.WriteByte('f')
			b.WriteString(strconv.FormatFloat(v, 'g', -1, 64))
		}
		b.WriteByte('\x00')
	}
	return b.String()
}
