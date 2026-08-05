package logchain

import "unicode/utf8"

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
