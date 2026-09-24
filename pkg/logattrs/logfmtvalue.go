package logattrs

import (
	"bytes"
	"unsafe"

	"github.com/JohanLindvall/logfmt"
)

// DecodeLogfmtValue returns the string form of a raw value yielded by
// logfmt.Iterate over line.
//
// Escapes are decoded ONLY for a value the writer QUOTED — which is what
// go-logfmt does, and what the escapes mean. `msg="a \"b\""` is `a "b"`, but
// `path=C:\logs\app.log` contains no escape sequence at all: go-kit, logrus
// and slog quote a value only when it holds a space, a quote or an equals
// sign, so unquoted backslashes are a shape real emitters produce. Unescaping
// those dropped the backslashes (`C:logsapp.log`, `\d+\s+` → `d+s+`) and, for
// the recognised letters, turned `arg=a\nb` into an attribute — and, through
// internal/logline, a Prometheus/OTLP label value — carrying a real newline.
//
// Quoted-ness is not in the callback's signature, so it is recovered from
// val's position in line: Iterate slices values out of line with two-index
// slicing, so cap carries the offset, and the bytes are compared to confirm
// it. Anything that does not check out is treated as unquoted and returned
// verbatim — the conservative direction, since leaving an escape undecoded
// preserves the writer's bytes while decoding a non-escape destroys them.
func DecodeLogfmtValue(line, val []byte) string {
	if !needsDecode(line, val) {
		return string(val)
	}
	return string(logfmt.AppendUnescape(nil, val))
}

// LogfmtValueView is DecodeLogfmtValue without the copy where there is nothing
// to decode: a value DecodeLogfmtValue would return verbatim comes back as a
// read-only VIEW into line, and only a quoted value carrying an escape is
// decoded into new memory. The result may therefore alias line, so line must
// stay unchanged for as long as the result is used.
//
// It is the one decision both line-field scanners in this module make —
// Extractor.Extract here and internal/logline's KeyIndex.Parse, each of which
// iterates an unsafe view of an immutable Go string, so aliasing it is safe —
// and it exists because the two had spelled it separately and drifted (one
// aliased, one copied every lifted value). DecodeLogfmtValue keeps its
// always-copy contract: an external caller that reuses its buffer must never
// be handed an alias into it.
func LogfmtValueView(line, val []byte) string {
	if !needsDecode(line, val) {
		return unsafe.String(unsafe.SliceData(val), len(val))
	}
	return string(logfmt.AppendUnescape(nil, val))
}

// needsDecode reports whether val holds an escape to decode: it must carry
// one and have been QUOTED. The escape test runs first — the common path has
// nothing to decode, so quoted-ness does not matter and the position work
// never runs.
func needsDecode(line, val []byte) bool {
	return logfmt.NeedsUnescape(val) && quotedValue(line, val)
}

// quotedValue reports whether val (a subslice of line handed out by
// logfmt.Iterate) was written in quotes.
func quotedValue(line, val []byte) bool {
	off := len(line) - cap(val)
	if off <= 0 || off+len(val) > len(line) || line[off-1] != '"' {
		return false
	}
	return bytes.Equal(line[off:off+len(val)], val)
}
