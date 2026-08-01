package logattrs

import (
	"bytes"

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
	// The common path: nothing to decode, so quoted-ness does not matter and
	// the position work never runs.
	if !logfmt.NeedsUnescape(val) || !quotedValue(line, val) {
		return string(val)
	}
	return string(logfmt.AppendUnescape(nil, val))
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
