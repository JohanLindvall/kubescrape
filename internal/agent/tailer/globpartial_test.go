package tailer

import (
	"os"
	"path/filepath"
	"testing"
)

// A single unreadable subdirectory under a `**` include must not blind the
// tailer to every readable file under that include. The glob still reports the
// listing incomplete (ok=false, so pruning/gone-detection stay off), but the
// readable file is discovered. Regression for the FilepathGlob(WithFailOnIOErrors)
// abort-the-whole-walk-and-return-nothing behavior.
func TestGlobCollectsReadableFilesPastAnUnreadableDir(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "good"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "bad"), 0o755); err != nil {
		t.Fatal(err)
	}
	good := filepath.Join(root, "good", "a.log")
	if err := os.WriteFile(good, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "bad", "b.log"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Make the sibling dir unreadable so the `**` walk hits an IO error.
	if err := os.Chmod(filepath.Join(root, "bad"), 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(filepath.Join(root, "bad"), 0o755) })
	if os.Geteuid() == 0 {
		t.Skip("running as root: chmod 000 does not deny access")
	}

	s := &compiledSource{include: []string{filepath.Join(root, "**", "*.log")}}
	got, ok := s.glob()
	if ok {
		t.Errorf("glob() reported ok=true despite an unreadable subtree; pruning/gone-detection must stay off")
	}
	found := false
	for _, p := range got {
		if p == good {
			found = true
		}
	}
	if !found {
		t.Errorf("readable file %q was NOT discovered past the unreadable sibling dir; got %v", good, got)
	}
}
