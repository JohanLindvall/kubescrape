package otlpingest

// Redaction of pushed log bodies (ServerConfig.Scrub), whatever shape the body has:
// every string leaf of a structured body is scrubbed, keyed entries probed as
// "key=value" the way the patterns are written for lines.

import (
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// maxBodyScrubDepth bounds the walk over a structured body, and it is DERIVED
// from the wire guard rather than chosen: it is the deepest level at which a
// decodable body can hold a string leaf, so the walk reaches every leaf a
// pushed body can carry and a redaction can never be missed for depth.
//
// The arithmetic is depth.go's (maxNestingDepth counts WIRE levels): a log
// body's AnyValue sits at wire depth 4, an array level costs two more
// (AnyValue -> ArrayValue -> AnyValue; a map costs three), and a string leaf
// is one level below its AnyValue — so a leaf at scrub depth d sits at wire
// depth 5+2d, which the guard admits only while 5+2d <= maxNestingDepth, i.e.
// d <= 47 at the current bound. It was a flat 8, and a secret at map depth 9
// — well inside the guard — was forwarded unredacted and uncounted, against
// scrubBody's own contract.
//
// The walk is still bounded, and by the same thing that bounds the decode: the
// recursion depth by this constant, the node count by the body's bytes. An
// over-depth subtree can therefore only reach here IN-PROCESS, never from a
// sender.
const maxBodyScrubDepth = (maxNestingDepth - 5) / 2

// scrubBody redacts every string leaf of a log body, whatever shape it has.
//
// The OTel logging SDKs and the collector's json_parser/transform emit
// STRUCTURED bodies — a map or a slice — for exactly the records most likely to
// carry credentials as a field. Scrubbing only ValueTypeStr meant the same
// message redacted on the tailer path (where it is a raw line) and shipped in
// clear when an SDK sent it as a kvlist, with nothing counted and the choice
// invisible to the operator.
func (s *Server) scrubBody(v pcommon.Value, depth int) { s.scrubValue("", v, depth) }

// scrubValue redacts v, using key for context when v is a map entry.
//
// The key matters: the patterns are written for LINES, where a secret appears
// as `password=hunter2`. Split across a map entry the value alone is an opaque
// string no pattern can judge, so a keyed entry is probed as "key=value" and
// only the value replaced. Self-contained secrets (bearer tokens, AWS keys, PEM
// blocks) still match the value on its own, which is tried first.
func (s *Server) scrubValue(key string, v pcommon.Value, depth int) {
	if depth > maxBodyScrubDepth {
		return
	}
	switch v.Type() {
	case pcommon.ValueTypeStr:
		if scrubbed := s.cfg.Scrub.Scrub(v.Str()); scrubbed != v.Str() {
			v.SetStr(scrubbed)
			return
		}
		if key == "" {
			return
		}
		probe := key + "=" + v.Str()
		scrubbed := s.cfg.Scrub.Scrub(probe)
		if scrubbed == probe {
			return
		}
		// Take the tail after the key we prefixed — NOT after the first '='
		// anywhere, which for a key like "auth=token" yielded a value of
		// "token=[REDACTED]". And when the pattern consumed the key too (a
		// user rule whose replacement carries no '=', which the default
		// [REDACTED] does not), fall back to redacting the whole value: the
		// old code left it UNTOUCHED while Scrub had already counted a
		// redaction, so the metric reported a redaction that never happened
		// and the secret shipped in clear.
		if tail, ok := strings.CutPrefix(scrubbed, key+"="); ok {
			v.SetStr(tail)
			return
		}
		v.SetStr(scrubbed)
	case pcommon.ValueTypeMap:
		m := v.Map()
		m.Range(func(k string, mv pcommon.Value) bool {
			s.scrubValue(k, mv, depth+1)
			return true
		})
	case pcommon.ValueTypeSlice:
		sl := v.Slice()
		for i := 0; i < sl.Len(); i++ {
			// A slice element has no key of its own; it inherits the key of the
			// entry holding the slice ("args": ["api_key=sk-1"]).
			s.scrubValue(key, sl.At(i), depth+1)
		}
	}
}
