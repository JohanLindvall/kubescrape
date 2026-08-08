package logchain

import "unicode/utf8"

// Attribute keys marking a record whose body was cut at a size cap. Every
// producer that truncates stamps AttrTruncated; AttrOriginalLength (the
// pre-cut body length in bytes) is stamped only where the producer still
// holds the whole message when it cuts — journald reads the oversized
// message and measures it before TruncateRunes. The tailer deliberately
// stamps AttrTruncated ALONE: its multiline caps discard the overflow inside
// the library, whose Entry reports only a Truncated bool (never the dropped
// size), and the entry's file-offset span is a different quantity (raw
// on-disk bytes including CRI framing and newlines), so the original body
// length is genuinely unknowable at its truncation site.
const (
	AttrTruncated      = "log.truncated"
	AttrOriginalLength = "log.original_length"
)

// TruncateRunes cuts s to at most n bytes on a rune boundary. Shared because
// two producers needed the same cut and one had it wrong: journald truncates
// oversized journal messages (per-entry hot path — this is a reslice, no
// copy), and azurediag's auth error path truncated HTTP bodies with a bare
// s[:n], which could split a multibyte rune and put invalid UTF-8 into an
// error string.
func TruncateRunes(s string, n int) string {
	// n <= 0 means "at most zero bytes"; without this, s[n] below panics on a
	// negative n. Callers pass positive constants, but the cut must be total.
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
