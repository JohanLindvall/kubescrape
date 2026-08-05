package positions

import (
	"os"
	"path/filepath"
	"testing"
)

// The orphan sweep must key off the literal basename prefix, not a glob built
// from the path: a positions path containing glob metacharacters ([ * ?) must
// not make the sweep match — and remove — unrelated files.
func TestOrphanSweepIgnoresGlobMetacharacters(t *testing.T) {
	dir := t.TempDir()
	// Basename carries a character class "[a]".
	path := filepath.Join(dir, "pos[a].json")

	// A real orphan of THIS store (the "<base>.tmp-*" shape save leaves behind).
	ours := filepath.Join(dir, "pos[a].json.tmp-abc123")
	if err := os.WriteFile(ours, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A bystander a naive glob would remove once "[a]" is read as a class
	// matching the literal 'a': "posa.json.tmp-...".
	bystander := filepath.Join(dir, "posa.json.tmp-xyz")
	if err := os.WriteFile(bystander, []byte("keep me"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(path); err != nil {
		t.Fatalf("Open: %v", err)
	}

	if _, err := os.Stat(ours); !os.IsNotExist(err) {
		t.Errorf("our orphan temp was not swept (stat err = %v)", err)
	}
	if _, err := os.Stat(bystander); err != nil {
		t.Errorf("unrelated file removed by the sweep: %v", err)
	}
}
