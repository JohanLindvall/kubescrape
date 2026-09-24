package logattrs

import (
	"testing"
	"unsafe"

	"github.com/JohanLindvall/logfmt"
)

// go-logfmt unescapes QUOTED values only. Applying it to everything mangled
// the shapes go-kit/logrus/slog emit unquoted — Windows paths, regexes — and
// for the recognised letters injected control characters into attributes.
func TestDecodeLogfmtValueOnlyUnescapesQuoted(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, line, want string
	}{
		{"unquoted windows path", `msg=hi path=C:\logs\app.log`, `C:\logs\app.log`},
		{"unquoted regex", `msg=hi path=\d+\s+ok`, `\d+\s+ok`},
		{"unquoted newline escape", `msg=hi path=a\nb`, `a\nb`},
		{"quoted escape decodes", `msg=hi path="a \"b\""`, `a "b"`},
		{"quoted backslash decodes", `msg=hi path="C:\\logs"`, `C:\logs`},
		{"no escapes", `msg=hi path=/var/log/app.log`, `/var/log/app.log`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := []byte(tc.line)
			var got string
			var seen bool
			_ = logfmt.Iterate(buf, func(key, val []byte) bool {
				if string(key) == "path" {
					got, seen = DecodeLogfmtValue(buf, val), true
				}
				return true
			})
			if !seen {
				t.Fatal("path key not yielded")
			}
			if got != tc.want {
				t.Errorf("got %q; want %q", got, tc.want)
			}
		})
	}
}

// LogfmtValueView is the decision both line-field scanners share: the SAME
// value DecodeLogfmtValue returns, served as a view into the line whenever
// there is nothing to decode — unquoted values included, backslashes and all —
// and decoded into new memory only for a quoted value carrying an escape.
func TestLogfmtValueViewDecodesLikeDecodeLogfmtValueAndAliasesTheRest(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, line string
		aliases    bool
	}{
		{"unquoted windows path", `msg=hi path=C:\logs\app.log`, true},
		{"unquoted newline escape", `msg=hi path=a\nb`, true},
		{"quoted, nothing to decode", `msg=hi path="a b"`, true},
		{"no escapes", `msg=hi path=/var/log/app.log`, true},
		{"quoted escape decodes", `msg=hi path="a \"b\""`, false},
		{"quoted backslash decodes", `msg=hi path="C:\\logs"`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := []byte(tc.line)
			var view, decoded string
			var seen bool
			_ = logfmt.Iterate(buf, func(key, val []byte) bool {
				if string(key) == "path" {
					view, decoded, seen = LogfmtValueView(buf, val), DecodeLogfmtValue(buf, val), true
				}
				return true
			})
			if !seen {
				t.Fatal("path key not yielded")
			}
			if view != decoded {
				t.Fatalf("LogfmtValueView = %q; DecodeLogfmtValue = %q", view, decoded)
			}
			start := uintptr(unsafe.Pointer(unsafe.SliceData(buf)))
			p := uintptr(unsafe.Pointer(unsafe.StringData(view)))
			if inLine := p >= start && p < start+uintptr(len(buf)); inLine != tc.aliases {
				t.Errorf("view aliases the line = %v; want %v", inLine, tc.aliases)
			}
		})
	}
}
