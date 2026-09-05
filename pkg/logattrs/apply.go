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
		case int64: // per the Attr contract: decodeScalar yields int64 for an integral token
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
			// A whole float64 keys as the int it will be STORED as (Put maps it
			// to PutInt): keying it 'f' while storing it int meant {"shard":2}
			// and {"shard":2.0} grouped as two ResourceLogs whose exported
			// resources were byte-identical — split, never merged, but a
			// duplicate resource per payload for an emitter mixing spellings.
			if v == float64(int64(v)) {
				b.WriteByte('i')
				b.WriteString(strconv.FormatInt(int64(v), 10))
				break
			}
			b.WriteByte('f')
			b.WriteString(strconv.FormatFloat(v, 'g', -1, 64))
		case int64: // per the Attr contract: decodeScalar yields int64 for an
			// integral token, Put honors it, and a value must not be silently
			// dropped from the grouping key (two sets differing only in an
			// int64 would merge).
			b.WriteByte('i')
			b.WriteString(strconv.FormatInt(v, 10))
		}
		b.WriteByte('\x00')
	}
	return b.String()
}
