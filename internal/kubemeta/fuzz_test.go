package kubemeta

import "testing"

// FuzzNormalizeContainerID checks NormalizeContainerID never panics and is
// idempotent: normalizing an already-normalized ID must be a no-op, since the
// server normalizes IDs arriving both raw and already-stripped (agent paths,
// HTTP path cleaning).
func FuzzNormalizeContainerID(f *testing.F) {
	for _, s := range []string{
		"containerd://abcdef0123456789",
		"docker://abc",
		"cri-o://abc",
		"containerd:/abc", // collapsed by HTTP path cleaning
		"abc",
		"",
		"   containerd://abc   ",
		"://",
		":",
		"a:b",
		"scheme://a:b",
		"containerd:// abc",
		"containerd:///abc",
		"\x00:\xff//x",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, id string) {
		once := NormalizeContainerID(id)
		twice := NormalizeContainerID(once)
		if once != twice {
			t.Fatalf("NormalizeContainerID not idempotent: %q -> %q -> %q", id, once, twice)
		}
	})
}
