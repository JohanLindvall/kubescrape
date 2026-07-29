package spool

import (
	"errors"
	"fmt"
	"testing"
)

func TestGroupCommitDurableAfterSync(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{"a", "bb", "ccc"} {
		if err := s.AppendNoSync([]byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()

	s, err = Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	for _, want := range []string{"a", "bb", "ccc"} {
		got, commit, ok := popString(t, s)
		if !ok || got != want {
			t.Fatalf("pop = %q ok=%v, want %q", got, ok, want)
		}
		commit()
	}
}

// Unsynced records are readable immediately: durability is the producer's
// concern (Sync), visibility is not deferred with it.
func TestAppendNoSyncVisibleBeforeSync(t *testing.T) {
	s, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if err := s.AppendNoSync([]byte("early")); err != nil {
		t.Fatal(err)
	}
	got, commit, ok := popString(t, s)
	if !ok || got != "early" {
		t.Fatalf("pop before Sync = %q ok=%v", got, ok)
	}
	commit()
}

// Rotation must flush the unsynced group on the old tail first — after
// appendSegment closes its handle those frames could never be fsynced.
func TestGroupCommitAcrossRotation(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{SegmentBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	for i := 0; i < 20; i++ {
		v := fmt.Sprintf("record-%02d-padding-padding-padding", i)
		want = append(want, v)
		if err := s.AppendNoSync([]byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	if s.Segments() < 2 {
		t.Fatalf("segments = %d, want a rotation mid-group", s.Segments())
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()

	s, err = Open(dir, Options{SegmentBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	for _, w := range want {
		got, commit, ok := popString(t, s)
		if !ok || got != w {
			t.Fatalf("pop = %q ok=%v, want %q", got, ok, w)
		}
		commit()
	}
}

// A synced Append on the same tail is a group barrier: its fsync covers the
// pending frames, so the following Sync has nothing left to report.
func TestSyncedAppendCoversPendingGroup(t *testing.T) {
	s, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if err := s.AppendNoSync([]byte("grouped")); err != nil {
		t.Fatal(err)
	}
	if err := s.Append([]byte("barrier")); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	pending := s.pendingSync
	s.mu.Unlock()
	if pending {
		t.Fatal("a synced Append must clear the pending group (same fsync)")
	}
	if err := s.Sync(); err != nil {
		t.Fatalf("Sync after the barrier: %v", err)
	}
}

// A failed write invalidates the whole unsynced group: Sync must surface an
// error — never falsely ack — and the producer re-appends. Duplicates of
// group frames that survived the failed rollback are legal (at-least-once).
func TestSyncReportsInvalidatedGroup(t *testing.T) {
	s, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if err := s.AppendNoSync([]byte("doomed")); err != nil {
		t.Fatal(err)
	}
	// Sabotage the write handle: the next append's write fails, which rolls
	// back (or, failing that, closes) the tail and invalidates the group.
	s.mu.Lock()
	_ = s.w.Close()
	s.mu.Unlock()
	if err := s.AppendNoSync([]byte("also-doomed")); err == nil {
		t.Fatal("append on a sabotaged handle must fail")
	}
	if err := s.Sync(); err == nil {
		t.Fatal("Sync must report the invalidated group, not ack it")
	}
	if err := s.Sync(); err != nil {
		t.Fatalf("the invalidation is reported once, then cleared: %v", err)
	}

	// The producer's re-append works and is delivered.
	if err := s.Append([]byte("retried")); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for {
		got, commit, ok, err := s.Pop()
		if err != nil {
			continue // rollback debris may surface as a counted corrupt frame
		}
		if !ok {
			break
		}
		seen[string(got)] = true
		commit()
	}
	if !seen["retried"] {
		t.Fatalf("re-appended record missing; saw %v", seen)
	}
}

// ErrFull mid-group leaves the accepted frames intact and syncable.
func TestErrFullMidGroup(t *testing.T) {
	s, err := Open(t.TempDir(), Options{MaxBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if err := s.AppendNoSync([]byte("fits")); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendNoSync(make([]byte, 128)); !errors.Is(err, ErrFull) {
		t.Fatalf("err = %v, want ErrFull", err)
	}
	if err := s.Sync(); err != nil {
		t.Fatalf("the accepted part of the group must still sync: %v", err)
	}
	got, commit, ok := popString(t, s)
	if !ok || got != "fits" {
		t.Fatalf("pop = %q ok=%v", got, ok)
	}
	commit()
}

// A clean Close flushes the pending group — records AppendNoSync accepted
// must not be lost to an orderly shutdown that forgot the final Sync.
func TestCloseFlushesPendingGroup(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AppendNoSync([]byte("last-words")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	got, commit, ok := popString(t, s)
	if !ok || got != "last-words" {
		t.Fatalf("pop after reopen = %q ok=%v", got, ok)
	}
	commit()
}
