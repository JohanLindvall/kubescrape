package spool

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// A damaged frame LENGTH in the tail segment must not silently discard every
// fsynced record behind it. The checksum covers the length precisely so the
// damage is detectable; whatever a truncate still costs must be counted.
func TestProbeDamagedLengthDoesNotSilentlyTruncate(t *testing.T) {
	dir := t.TempDir()
	sp, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	const n = 20
	for i := 0; i < n; i++ {
		if err := sp.Append([]byte(fmt.Sprintf("record-%02d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := sp.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrupt the LENGTH of record 5 downward, so it still lands in bounds —
	// the case that used to mis-frame the whole remainder.
	segs, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	if len(segs) != 1 {
		t.Fatalf("want one segment, got %v", segs)
	}
	raw, err := os.ReadFile(segs[0])
	if err != nil {
		t.Fatal(err)
	}
	off := int64(segHeaderLen)
	for i := 0; i < 5; i++ {
		n := int64(raw[off])<<24 | int64(raw[off+1])<<16 | int64(raw[off+2])<<8 | int64(raw[off+3])
		off += frameHeaderLen(formatVersion) + n
	}
	raw[off+3] ^= 0x02 // flip a low bit of record 5's length
	if err := os.WriteFile(segs[0], raw, 0o600); err != nil {
		t.Fatal(err)
	}

	sp2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sp2.Close() }()

	got, corrupt := 0, 0
	for {
		data, commit, ok, err := sp2.Pop()
		if err != nil {
			corrupt++
		}
		if !ok {
			break
		}
		_ = data
		got++
		commit()
	}
	t.Logf("delivered=%d corrupt=%d discardedBytes=%d (appended %d)", got, corrupt, sp2.Discarded(), n)
	// A damaged frame LENGTH cannot be re-framed without a per-frame sync
	// marker (a format change), so records behind it are still lost. What must
	// never happen is losing them INVISIBLY: the checksum walk detects the
	// damage and the truncate is counted, so an operator can see it.
	if got < n && corrupt == 0 && sp2.Discarded() == 0 {
		t.Fatalf("lost %d records with NO ErrCorrupt and NO discarded-byte count: the loss is invisible", n-got)
	}
}
