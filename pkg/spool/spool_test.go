package spool

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func popString(t *testing.T, s *Spool) (string, func(), bool) {
	t.Helper()
	data, commit, ok, _ := s.Pop()
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
	if _, _, ok, _ := s.Pop(); ok {
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
	if _, _, ok, _ := s2.Pop(); ok {
		t.Error("queue should be drained after restart")
	}
}

func TestSizeCap(t *testing.T) {
	// Backlog counts the segment header (8) plus each frame (12 + payload).
	// With 10-byte payloads a frame is 22 bytes: 8 + 22 + 22 = 52 fits, a
	// third frame (74) does not.
	s, err := Open(t.TempDir(), Options{MaxBytes: 52})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
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
	_, commit, _, _ := s.Pop()
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
	if _, _, ok, _ := s2.Pop(); ok {
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
	_, commit, ok, _ := s.Pop()
	if !ok {
		t.Fatal("pop failed")
	}
	commit()
	seq, off := s.segs[0].seq, s.readOff
	_ = s.Close()

	cursor := filepath.Join(dir, cursorName)

	full, err := os.ReadFile(cursor)
	if err != nil {
		t.Fatal(err)
	}
	if len(full) != cursorLen {
		t.Fatalf("cursor length = %d, want %d", len(full), cursorLen)
	}
	_ = seq
	_ = off

	// A short (e.g. pre-checksum) cursor carries no valid checksum, so it is
	// not trusted: redeliver from the oldest segment rather than seek to a
	// position that might skip undelivered frames.
	if err := os.WriteFile(cursor, full[:16], 0o644); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if got, _, _ := popString(t, s2); got != "a" {
		t.Fatalf("after short cursor Pop = %q, want a (redeliver from the start)", got)
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
	if s3.readOff != segHeaderLen {
		t.Fatalf("torn cursor readOff = %d, want %d (redeliver from the start)", s3.readOff, segHeaderLen)
	}
	if got, _, _ := popString(t, s3); got != "a" {
		t.Fatalf("after torn cursor Pop = %q, want a (redelivered)", got)
	}
}

func TestPopSkipsLostHeadSegment(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{SegmentBytes: 16}) // tiny: every record rotates
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if err := s.Append([]byte("first-record")); err != nil {
		t.Fatal(err)
	}
	if err := s.Append([]byte("second-record")); err != nil {
		t.Fatal(err)
	}
	if len(s.segs) < 2 {
		t.Fatalf("expected 2 segments, got %d", len(s.segs))
	}
	if err := os.Remove(s.segs[0].path); err != nil {
		t.Fatal(err)
	}
	_, _, ok, err := s.Pop()
	if ok || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected not-exist error, got ok=%v err=%v", ok, err)
	}
	data, commit, ok, err := s.Pop()
	if err != nil || !ok {
		t.Fatalf("expected next segment's record, got ok=%v err=%v", ok, err)
	}
	if string(data) != "second-record" {
		t.Fatalf("got %q", data)
	}
	commit()
}

func TestForeignFormatSegmentsDropped(t *testing.T) {
	dir := t.TempDir()
	// A segment from an unreadable format: no magic (an older agent), and one
	// naming a version this build does not know (a newer agent).
	legacy := filepath.Join(dir, fmt.Sprintf("%020d%s", 5, segSuffix))
	if err := os.WriteFile(legacy, []byte{0, 0, 0, 3, 'o', 'l', 'd'}, 0o644); err != nil {
		t.Fatal(err)
	}
	future := filepath.Join(dir, fmt.Sprintf("%020d%s", 6, segSuffix))
	fhdr := make([]byte, segHeaderLen)
	copy(fhdr, segMagic[:])
	binary.BigEndian.PutUint16(fhdr[len(segMagic):], 999)
	if err := os.WriteFile(future, append(fhdr, 'x'), 0o644); err != nil {
		t.Fatal(err)
	}

	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Open with foreign segments: %v", err)
	}
	defer func() { _ = s.Close() }()
	if _, err := os.Stat(legacy); !os.IsNotExist(err) {
		t.Error("segment without the magic was not discarded")
	}
	if _, err := os.Stat(future); !os.IsNotExist(err) {
		t.Error("segment with an unknown version was not discarded")
	}
	// The magic-less "legacy" segment is surfaced as one corrupt read (its
	// records are lost and, unlike a known format bump, a missing magic is
	// indistinguishable from bit rot — so it is counted, not silent). The
	// unknown-version "future" segment is a deliberate format change and stays
	// silent.
	if _, _, _, err := s.Pop(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("first Pop = %v, want ErrCorrupt for the dropped magic-less segment", err)
	}
	// The spool still works, writing the current format.
	if err := s.Append([]byte("fresh")); err != nil {
		t.Fatal(err)
	}
	if got, commit, ok := popString(t, s); !ok || got != "fresh" {
		t.Fatalf("Pop = %q (ok=%v), want fresh", got, ok)
	} else {
		commit()
	}
	version, ok, _, err := readSegHeader(s.segs[len(s.segs)-1].path)
	if err != nil || !ok {
		t.Fatalf("readSegHeader: ok=%v err=%v", ok, err)
	}
	if version != formatVersion {
		t.Fatalf("new segment version = %d, want %d", version, formatVersion)
	}
}

// TestCorruptPayloadDropped: a flipped payload byte fails the frame checksum,
// so the record is dropped and reported — never delivered mangled — and the
// following records still drain.
func TestCorruptPayloadDropped(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{"alpha", "bravo", "charlie"} {
		if err := s.Append([]byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	path := s.segs[0].path
	_ = s.Close()

	// Flip a byte inside the first frame's payload (past the segment and frame
	// headers).
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[segHeaderLen+frameHeaderV1] ^= 0xff
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()

	// The damaged record surfaces as ErrCorrupt, not as data.
	data, _, ok, err := s2.Pop()
	if ok || data != nil {
		t.Fatalf("corrupt frame was delivered: %q", data)
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
	// The rest of the segment still drains.
	for _, want := range []string{"bravo", "charlie"} {
		got, commit, gotOK := popString(t, s2)
		if !gotOK || got != want {
			t.Fatalf("Pop = %q (ok=%v), want %q", got, gotOK, want)
		}
		commit()
	}
}

// TestPopAfterCaughtUpRotation pins the retire-in-Pop fix: a consumer fully
// caught up (readOff == tail size) when an Append rotates to a new segment
// previously saw "empty" forever — no commit ever ran again to retire the
// consumed head.
func TestPopAfterCaughtUpRotation(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{SegmentBytes: 32})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	if err := s.Append([]byte("first-record-padding-x")); err != nil {
		t.Fatal(err)
	}
	data, commit, ok, err := s.Pop()
	if err != nil || !ok || string(data) != "first-record-padding-x" {
		t.Fatalf("pop 1: ok=%v err=%v data=%q", ok, err, data)
	}
	commit() // fully caught up: readOff == segs[0].size, single segment

	// This append exceeds SegmentBytes and rotates to a new segment.
	if err := s.Append([]byte("second-record-after-rotation")); err != nil {
		t.Fatal(err)
	}
	data, commit, ok, err = s.Pop()
	if err != nil || !ok {
		t.Fatalf("pop 2 wedged: ok=%v err=%v backlog=%d", ok, err, s.Bytes())
	}
	if string(data) != "second-record-after-rotation" {
		t.Fatalf("pop 2: %q", data)
	}
	commit()
	if got := s.Bytes(); got != 0 {
		t.Fatalf("backlog after full drain: %d", got)
	}
}

// TestCrashRecoveryDeliversUncommitted simulates kill -9: a spool abandoned
// without Close, with a torn frame left half-written at the tail (the crash
// interrupted an Append mid-frame). Reopening must truncate the torn frame,
// deliver every committed-but-unacked record, and keep accepting appends.
func TestCrashRecoveryDeliversUncommitted(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"one", "two", "three", "four"}
	for _, v := range want {
		if err := s.Append([]byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	// Consume the first record; the rest are still owed.
	got, commit, ok := popString(t, s)
	if !ok || got != "one" {
		t.Fatalf("pop = %q (ok=%v)", got, ok)
	}
	commit()
	tail := s.segs[len(s.segs)-1].path
	// Abandon without Close (kill -9), then leave a torn frame behind: a full
	// frame header promising bytes that were never written.
	f, err := os.OpenFile(tail, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	var hdr [frameHeaderV1]byte
	binary.BigEndian.PutUint32(hdr[:4], 999) // claims 999 bytes; none follow
	if _, err := f.Write(hdr[:]); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	s2, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Open after crash: %v", err)
	}
	defer func() { _ = s2.Close() }()
	for _, w := range want[1:] {
		got, commit, ok := popString(t, s2)
		if !ok || got != w {
			t.Fatalf("after crash Pop = %q (ok=%v), want %q", got, ok, w)
		}
		commit()
	}
	if _, _, ok, err := s2.Pop(); ok || err != nil {
		t.Fatalf("expected an empty queue after draining, got ok=%v err=%v", ok, err)
	}
	// The torn frame was truncated, so appends resume cleanly.
	if err := s2.Append([]byte("after-crash")); err != nil {
		t.Fatal(err)
	}
	got, commit, ok = popString(t, s2)
	if !ok || got != "after-crash" {
		t.Fatalf("post-crash append Pop = %q (ok=%v)", got, ok)
	}
	commit()
}
