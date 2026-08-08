package logchain

import "testing"

// The truncation-marking keys are wire-visible (backends filter on them, and
// journald's + the tailer's records already ship them): a refactor renaming
// the constants must not change the strings.
func TestTruncationAttrKeysAreStable(t *testing.T) {
	if AttrTruncated != "log.truncated" {
		t.Errorf("AttrTruncated = %q, want %q", AttrTruncated, "log.truncated")
	}
	if AttrOriginalLength != "log.original_length" {
		t.Errorf("AttrOriginalLength = %q, want %q", AttrOriginalLength, "log.original_length")
	}
}

func TestTruncateRunes(t *testing.T) {
	tests := []struct {
		name string
		s    string
		n    int
		want string
	}{
		{"shorter than n", "abc", 10, "abc"},
		{"equal to n", "abc", 3, "abc"},
		{"cut on ascii boundary", "abcdef", 3, "abc"},
		{"cut steps back off a multibyte rune", "a£c", 2, "a"}, // £ is 2 bytes at [1,2]
		{"zero n", "abc", 0, ""},
		{"negative n does not panic", "abc", -1, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := TruncateRunes(tt.s, tt.n); got != tt.want {
				t.Fatalf("TruncateRunes(%q, %d) = %q, want %q", tt.s, tt.n, got, tt.want)
			}
		})
	}
}
