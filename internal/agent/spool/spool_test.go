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
