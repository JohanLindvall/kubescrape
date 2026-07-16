package spool

import (
	"encoding/binary"
	"errors"
	"os"
	"runtime"
	"testing"
)

// The write tail is also the read head in a single-segment spool. When its
// frame stream is corrupt (an overshooting length is corruption, never a torn
// append — Append fsyncs whole frames), skipLostHead must REPLACE the tail with
// a fresh segment rather than skip it in place: appends must keep working, the
// seq must stay monotonic, and nothing may panic on the segs slice.
func TestSkipLostHeadReplacesCorruptTail(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir, Options{})
	mustAppend(t, s, "doomed")

	// Corrupt the first frame's length field (offset segHeaderLen) to overshoot
	// the file size.
	segs := segFiles(t, dir)
	if len(segs) != 1 {
		t.Fatalf("segments = %d, want 1", len(segs))
	}
	f, err := os.OpenFile(segs[0], os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	var huge [4]byte
	binary.BigEndian.PutUint32(huge[:], 1<<30)
	if _, err := f.WriteAt(huge[:], segHeaderLen); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	// Pop surfaces the corruption and replaces the tail.
	_, _, _, err = s.Pop()
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Pop err = %v, want ErrCorrupt", err)
	}
	if _, err := os.Stat(segs[0]); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dead segment %s not removed", segs[0])
	}

	// The queue is empty and healthy: appends land in a fresh, higher-seq
	// segment and drain back intact.
	if data, _, ok, err := s.Pop(); err != nil || ok {
		t.Fatalf("post-replacement Pop = (%q, ok=%v, err=%v), want empty", data, ok, err)
	}
	mustAppend(t, s, "alive")
	if got := drain(t, s); len(got) != 1 || got[0] != "alive" {
		t.Fatalf("drained %v, want [alive]", got)
	}
	newSegs := segFiles(t, dir)
	if len(newSegs) == 0 || newSegs[0] == segs[0] {
		t.Fatalf("tail not replaced: %v (old %s)", newSegs, segs[0])
	}
}

// A single-segment spool whose tail file vanishes externally must likewise
// replace it and keep accepting appends.
func TestSkipLostHeadReplacesVanishedTail(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir, Options{})
	mustAppend(t, s, "gone")
	s.dropReadHandle() // ensure Pop reopens by path and sees the removal

	segs := segFiles(t, dir)
	if err := os.Remove(segs[0]); err != nil {
		t.Fatal(err)
	}
	// Pop reports the read error (surfaced, not silent) and skips the dead head.
	if _, _, ok, err := s.Pop(); err == nil && ok {
		t.Fatal("Pop delivered data from a vanished segment")
	}
	mustAppend(t, s, "alive")
	if got := drain(t, s); len(got) != 1 || got[0] != "alive" {
		t.Fatalf("drained %v, want [alive]", got)
	}
}

// When the replacement segment cannot be created, skipLostHead must keep the
// stale entry (reads keep erroring) rather than leaving segs empty and
// panicking on the next access.
func TestSkipLostHeadReplacementFailureNoPanic(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("relies on directory permissions")
	}
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dir := t.TempDir()
	s := mustOpen(t, dir, Options{})
	mustAppend(t, s, "doomed")

	segs := segFiles(t, dir)
	f, err := os.OpenFile(segs[0], os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	var huge [4]byte
	binary.BigEndian.PutUint32(huge[:], 1<<30)
	if _, err := f.WriteAt(huge[:], segHeaderLen); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	// Make the dir read-only so appendSegment fails inside skipLostHead.
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	if _, _, _, err := s.Pop(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Pop err = %v, want ErrCorrupt", err)
	}
	// The stale entry was kept: further Pops keep erroring but never panic.
	for i := 0; i < 3; i++ {
		if _, _, ok, _ := s.Pop(); ok {
			t.Fatal("Pop delivered data from a corrupt tail")
		}
	}
	// Once the dir is writable again the spool heals on the next Pop.
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	_, _, _, _ = s.Pop() // replacement succeeds this time
	mustAppend(t, s, "healed")
	if got := drain(t, s); len(got) != 1 || got[0] != "healed" {
		t.Fatalf("drained %v, want [healed]", got)
	}
}
