// Package clip is the one implementation of "cut a string to a byte bound on a
// rune boundary" for log lines, error strings and diagnostic documents.
//
// Seven packages carried their own copy of the same eight-line loop, and the
// copies had already diverged in the direction that matters: two of them cut at
// the bare byte index, so a caller-supplied value whose cut landed inside a
// multi-byte rune reached a log line or a JSON document as a half-encoded code
// point — a replacement character in whatever reads the line, and an
// invalid-UTF-8 string in a document a strict decoder rejects whole. The bound
// and the marker stay each caller's own (a log attribute is cut at 96 bytes, a
// URL path segment at 253, a corrupt label string at 256); only the cut lives
// here.
//
// Out of scope, deliberately: internal/metrics' truncLabelCut, which keeps
// invalid bytes on purpose when a value has no rune boundary to back off to
// (returning "" would drop the label and merge two series — see there), and
// internal/agent/cumagg's Retain, whose copy is the point (a retained label must
// not pin the megabyte it was cut from). Both are hot paths with their own
// contracts; this package serves the cold ones.
package clip

import "unicode/utf8"

// Runes cuts s to at most n bytes, backing the cut off to the start of the rune
// straddling it, so the result is valid UTF-8 whenever s is. n <= 0 yields "".
// A string within the bound is returned as is — no copy, no allocation.
func Runes(s string, n int) string {
	if n <= 0 {
		return ""
	}
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// Marked is Runes with mark appended when — and only when — s was cut, so a
// reader can tell a clipped value from a short one. The mark is not counted
// against n.
func Marked(s string, n int, mark string) string {
	if len(s) <= n {
		return s
	}
	return Runes(s, n) + mark
}

// Ellipsis is Marked with the one-rune ellipsis every log-attribute clip in
// this repo uses.
func Ellipsis(s string, n int) string { return Marked(s, n, "…") }
