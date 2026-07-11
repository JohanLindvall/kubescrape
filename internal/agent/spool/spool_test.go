package spool

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func popString(t *testing.T, s *Spool) (string, func(), bool) {
	t.Helper()
	data, commit, ok := s.Pop()
	if !ok {
		return "", nil, false
	}
	return string(data), commit, true
}

func TestAppendPopOrder(t *testing.T) {
	s, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	for _, v := range []string{"a", "bb", "ccc"} {
		if err := s.Append([]byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []string{"a", "bb", "ccc"} {
		got, commit, ok := popString(t, s)
		if !ok || got != want {
			t.Fatalf("pop = %q ok=%v, want %q", got, ok, want)
		}
		commit()
	}
	if _, _, ok := s.Pop(); ok {
		t.Error("queue should be empty")
	}
}

func TestUncommittedRedeliveredAfterRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{"one", "two", "three"} {
		if err := s.Append([]byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	// Consume the first, commit it; peek the second but DON'T commit.
	got, commit, _ := popString(t, s)
	if got != "one" {
		t.Fatalf("first = %q", got)
	}
	commit()
	if got, _, _ := popString(t, s); got != "two" {
		t.Fatalf("second = %q", got)
	}
	_ = s.Close()

	// Reopen: "two" (uncommitted) and "three" must both remain, in order.
	s2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()
	for _, want := range []string{"two", "three"} {
		got, commit, ok := popString(t, s2)
		if !ok || got != want {
			t.Fatalf("after restart pop = %q ok=%v, want %q", got, ok, want)
		}
		commit()
	}
	if _, _, ok := s2.Pop(); ok {
		t.Error("queue should be drained after restart")
	}
}

func TestSizeCap(t *testing.T) {
	s, err := Open(t.TempDir(), Options{MaxBytes: 32})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	// Each frame is 4 + len bytes. 10-byte payloads → 14 bytes each; two fit
	// under 32, the third does not.
	payload := []byte("0123456789")
	if err := s.Append(payload); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(payload); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(payload); err != ErrFull {
		t.Fatalf("third append err = %v, want ErrFull", err)
	}
	// Draining one frees room again.
	_, commit, _ := s.Pop()
	commit()
	if err := s.Append(payload); err != nil {
		t.Fatalf("append after drain: %v", err)
	}
}

func TestSegmentRotationAndDeletion(t *testing.T) {
	dir := t.TempDir()
	// Tiny segments so each record lands in its own segment after the first.
	s, err := Open(dir, Options{SegmentBytes: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	for i := 0; i < 5; i++ {
		if err := s.Append([]byte(fmt.Sprintf("rec%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	// Consuming most records should let old segments be deleted.
	for i := 0; i < 4; i++ {
		got, commit, ok := popString(t, s)
		if !ok || got != fmt.Sprintf("rec%d", i) {
			t.Fatalf("rec %d = %q ok=%v", i, got, ok)
		}
		commit()
	}
	segs, _ := filepath.Glob(filepath.Join(dir, "*"+segSuffix))
	if len(segs) > 2 {
		t.Errorf("consumed segments not reclaimed: %d segment files remain", len(segs))
	}
}

func TestTornTailIgnored(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Append([]byte("intact")); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()

	// Simulate a crash mid-append: append a header claiming 100 bytes but only
	// a few trailing bytes to the segment file.
	seg, _ := filepath.Glob(filepath.Join(dir, "*"+segSuffix))
	f, err := os.OpenFile(seg[0], os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte{0, 0, 0, 100, 'x', 'y', 'z'})
	_ = f.Close()

	s2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()
	got, commit, ok := popString(t, s2)
	if !ok || got != "intact" {
		t.Fatalf("pop = %q ok=%v, want intact", got, ok)
	}
	commit()
	if _, _, ok := s2.Pop(); ok {
		t.Error("torn frame should not be delivered")
	}
	// The tail was truncated, so new appends land cleanly.
	if err := s2.Append([]byte("after")); err != nil {
		t.Fatal(err)
	}
	if got, _, _ := popString(t, s2); got != "after" {
		t.Fatalf("post-repair pop = %q", got)
	}
}

func TestBytesAndSignal(t *testing.T) {
	s, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	if s.Bytes() != 0 {
		t.Fatalf("empty spool backlog = %d", s.Bytes())
	}
	if err := s.Append([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	// Append signals waiters (non-blocking channel, one notification pending).
	select {
	case <-s.Signal():
	default:
		t.Fatal("Append did not signal")
	}
	if s.Bytes() <= 0 {
		t.Fatalf("backlog after append = %d", s.Bytes())
	}
	// Consuming and committing shrinks the backlog back to zero.
	data, commit, ok := popString(t, s)
	if !ok || data != "hello" {
		t.Fatalf("pop = %q, %v", data, ok)
	}
	commit()
	if s.Bytes() != 0 {
		t.Fatalf("backlog after commit = %d", s.Bytes())
	}
}

// Orphan bytes past the last whole frame (a partial append whose rollback did
// not complete, e.g. ENOSPC then crash) are truncated on reopen, and appends
// after an in-process rollback failure re-verify the tail.
func TestOrphanTailBytesRecovered(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Append([]byte("first")); err != nil {
		t.Fatal(err)
	}
	tailPath := s.segs[len(s.segs)-1].path

	// Simulate the failed-rollback state: partial frame bytes on disk that the
	// size accounting does not know about, and a closed write handle.
	_ = s.w.Close()
	s.w = nil
	f, err := os.OpenFile(tailPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0x00, 0x00, 0x10}); err != nil { // torn header
		t.Fatal(err)
	}
	_ = f.Close()

	// The next Append must reopen, truncate the orphan bytes, and land the
	// frame where the accounting expects it.
	if err := s.Append([]byte("second")); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"first", "second"} {
		got, commit, ok := popString(t, s)
		if !ok || got != want {
			t.Fatalf("Pop = %q,%v want %q", got, ok, want)
		}
		commit()
	}
	_ = s.Close()

	// Same orphan situation across a restart: reopen truncates and both the
	// backlog accounting and appends stay consistent.
	s2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()
	tail2 := s2.segs[len(s2.segs)-1]
	f2, err := os.OpenFile(tail2.path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f2.Write([]byte("garbage-no-header......")); err != nil {
		t.Fatal(err)
	}
	_ = f2.Close()
	_ = s2.Close()

	s3, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s3.Close() }()
	if got := s3.Bytes(); got != 0 {
		t.Fatalf("backlog after orphan truncation = %d, want 0", got)
	}
	if err := s3.Append([]byte("third")); err != nil {
		t.Fatal(err)
	}
	got, commit, ok := popString(t, s3)
	if !ok || got != "third" {
		t.Fatalf("Pop = %q,%v want third", got, ok)
	}
	commit()
}

// A legacy 16-byte cursor (pre-checksum) is honored; a torn/corrupt cursor
// falls back to redelivering from the oldest segment instead of seeking to a
// wrong position.
func TestCursorChecksum(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{"a", "b", "c"} {
		if err := s.Append([]byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	// Consume "a" so the persisted cursor is non-zero.
	_, commit, ok := s.Pop()
	if !ok {
		t.Fatal("pop failed")
	}
	commit()
	seq, off := s.segs[0].seq, s.readOff
	_ = s.Close()

	cursor := filepath.Join(dir, cursorName)

	// Legacy format: first 16 bytes of the current cursor.
	full, err := os.ReadFile(cursor)
	if err != nil {
		t.Fatal(err)
	}
	if len(full) != cursorLen {
		t.Fatalf("cursor length = %d, want %d", len(full), cursorLen)
	}
	if err := os.WriteFile(cursor, full[:16], 0o644); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if s2.segs[0].seq != seq || s2.readOff != off {
		t.Fatalf("legacy cursor: seq/off = %d/%d, want %d/%d", s2.segs[0].seq, s2.readOff, seq, off)
	}
	if got, _, _ := popString(t, s2); got != "b" {
		t.Fatalf("after legacy cursor Pop = %q, want b", got)
	}
	_ = s2.Close()

	// Torn cursor (checksum mismatch): redeliver from the start.
	bad := append([]byte(nil), full...)
	bad[20] ^= 0xff
	if err := os.WriteFile(cursor, bad, 0o644); err != nil {
		t.Fatal(err)
	}
	s3, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s3.Close() }()
	if s3.readOff != 0 {
		t.Fatalf("torn cursor readOff = %d, want 0 (redeliver)", s3.readOff)
	}
	if got, _, _ := popString(t, s3); got != "a" {
		t.Fatalf("after torn cursor Pop = %q, want a (redelivered)", got)
	}
}
