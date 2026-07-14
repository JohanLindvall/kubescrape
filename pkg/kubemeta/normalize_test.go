package kubemeta

import (
	"testing"
)

func TestNormalizeContainerID(t *testing.T) {
	cases := map[string]string{
		"containerd://abc123": "abc123",
		"docker://abc123":     "abc123",
		"cri-o://abc123":      "abc123",
		"containerd:/abc123":  "abc123", // collapsed by HTTP path cleaning
		"abc123":              "abc123",
		" containerd://x ":    "x",
		"":                    "",
	}
	for in, want := range cases {
		if got := NormalizeContainerID(in); got != want {
			t.Errorf("NormalizeContainerID(%q) = %q, want %q", in, got, want)
		}
	}
}
