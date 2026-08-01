package logattrs

import (
	"testing"

	"github.com/JohanLindvall/logfmt"
)

// go-logfmt unescapes QUOTED values only. Applying it to everything mangled
// the shapes go-kit/logrus/slog emit unquoted — Windows paths, regexes — and
// for the recognised letters injected control characters into attributes.
func TestDecodeLogfmtValueOnlyUnescapesQuoted(t *testing.T) {
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
