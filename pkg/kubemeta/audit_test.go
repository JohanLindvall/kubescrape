package kubemeta

import "testing"

// TestAudit_NormalizeContainerID covers every runtime prefix plus garbage, and
// pins idempotence (the agent and the HTTP handler both normalize).
func TestAudit_NormalizeContainerID(t *testing.T) {
	cases := []struct{ in, want string }{
		{"containerd://abc123", "abc123"},
		{"docker://abc123", "abc123"},
		{"cri-o://abc123", "abc123"},
		{"rkt://abc123", "abc123"},
		{"abc123", "abc123"},
		{"containerd:/abc123", "abc123"}, // HTTP path-cleaned form
		{"containerd:abc123", "abc123"},
		{"  containerd://  abc123  ", "abc123"},
		{"", ""},
		{":", ""},
		{"://", ""},
		{"containerd://", ""},
		{"a:b:c", "c"}, // cut at the LAST colon
		{"///abc", "///abc"},
		{"\t\ndocker://abc\r\n", "abc"},
		{"docker://abc def", "abc def"},
		{"CONTAINERD://ABC", "ABC"},
	}
	for _, tc := range cases {
		got := NormalizeContainerID(tc.in)
		if got != tc.want {
			t.Errorf("NormalizeContainerID(%q) = %q, want %q", tc.in, got, tc.want)
		}
		if again := NormalizeContainerID(got); again != got {
			t.Errorf("NOT IDEMPOTENT: NormalizeContainerID(%q) = %q, then %q", tc.in, got, again)
		}
		// The result must never retain a colon — that is what makes it idempotent.
		for _, r := range got {
			if r == ':' {
				t.Errorf("NormalizeContainerID(%q) = %q still contains a colon", tc.in, got)
			}
		}
	}
}
