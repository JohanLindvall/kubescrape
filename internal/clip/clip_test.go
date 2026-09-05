package clip

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestRunesCutsOnRuneBoundaries(t *testing.T) {
	for _, tt := range []struct {
		s    string
		n    int
		want string
	}{
		{"", 5, ""},
		{"abc", 5, "abc"},
		{"abc", 3, "abc"},
		{"abcdef", 3, "abc"},
		{"héllo", 2, "h"},  // é is two bytes; the cut backs off to before it
		{"héllo", 3, "hé"}, // the cut lands on a boundary
		{"日本語", 4, "日"},    // three-byte runes
		{"日本語", 6, "日本"},
		{"日本語", 7, "日本"},
		{"日本語", 9, "日本語"},
		{"abc", 0, ""},
		{"abc", -1, ""},
		{"\x80\x80\x80abc", 2, ""}, // no boundary to back off to: nothing survives
	} {
		if got := Runes(tt.s, tt.n); got != tt.want {
			t.Errorf("Runes(%q, %d) = %q, want %q", tt.s, tt.n, got, tt.want)
		}
	}
}

func TestMarkedAppendsOnlyWhenCut(t *testing.T) {
	if got := Marked("short", 10, "…"); got != "short" {
		t.Fatalf("an in-bound value grew a mark: %q", got)
	}
	if got := Marked("exactly", 7, "…"); got != "exactly" {
		t.Fatalf("a value at the bound grew a mark: %q", got)
	}
	if got := Marked("hé there", 2, "...(truncated)"); got != "h...(truncated)" {
		t.Fatalf("Marked = %q", got)
	}
	if got := Ellipsis("日本語", 4); got != "日…" {
		t.Fatalf("Ellipsis = %q", got)
	}
}

func TestRunesNeverInventsInvalidUTF8(t *testing.T) {
	s := strings.Repeat("aé日😀", 64)
	for n := -1; n <= len(s)+1; n++ {
		got := Runes(s, n)
		if !utf8.ValidString(got) {
			t.Fatalf("Runes(_, %d) = %q is not valid UTF-8", n, got)
		}
		if len(got) > max(n, 0) {
			t.Fatalf("Runes(_, %d) returned %d bytes", n, len(got))
		}
		if !strings.HasPrefix(s, got) {
			t.Fatalf("Runes(_, %d) = %q is not a prefix", n, got)
		}
	}
}

func TestRunesDoesNotAllocateWithinTheBound(t *testing.T) {
	s := "a value that fits"
	if n := testing.AllocsPerRun(100, func() { _ = Runes(s, len(s)) }); n != 0 {
		t.Fatalf("Runes on an in-bound value allocates %v times", n)
	}
}
