package spool

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func mustOpen(t *testing.T, dir string, opts Options) *Spool {
	t.Helper()
	s, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func mustAppend(t *testing.T, s *Spool, data string) {
	t.Helper()
	if err := s.Append([]byte(data)); err != nil {
		t.Fatalf("Append(%q): %v", data, err)
	}
}

func drain(t *testing.T, s *Spool) []string {
	t.Helper()
	var out []string
	for {
		data, commit, ok, err := s.Pop()
		if err != nil {
			t.Logf("Pop err: %v", err)
			continue
		}
		if !ok {
			return out
		}
		out = append(out, string(data))
		commit()
	}
}

func segFiles(t *testing.T, dir string) []string {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(dir, "*"+segSuffix))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func diskBytes(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	for _, p := range segFiles(t, dir) {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		total += fi.Size()
	}
	return total
}

// ---------------------------------------------------------------------------
// A1. A record that alone exceeds MaxBytes can never be appended, even to an
// empty spool. The producer (tailer/Buffered) treats ErrFull as backpressure and
// rewinds, so this is a permanent stall on that record rather than a drop.
// ---------------------------------------------------------------------------

func TestAudit_OversizedRecordNeverFits(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir, Options{MaxBytes: 100})
	big := make([]byte, 200)
	for i := 0; i < 5; i++ {
		if err := s.Append(big); !errors.Is(err, ErrFull) {
			t.Fatalf("Append(oversized) = %v, want ErrFull", err)
		}
	}
	if got := s.Bytes(); got != 0 {
		t.Fatalf("backlog after rejected appends = %d, want 0", got)
	}
	// Even a fully-drained spool keeps rejecting it: there is no progress the
	// caller can make. Document the exact threshold.
	fit := make([]byte, 100-FrameOverhead)
	if err := s.Append(fit); err != nil {
		t.Fatalf("Append(exactly MaxBytes-FrameOverhead) = %v, want nil", err)
	}
	one := make([]byte, 100-FrameOverhead+1)
	s2 := mustOpen(t, t.TempDir(), Options{MaxBytes: 100})
	if err := s2.Append(one); !errors.Is(err, ErrFull) {
		t.Fatalf("Append(MaxBytes-FrameOverhead+1) = %v, want ErrFull", err)
	}
}

// ---------------------------------------------------------------------------
// A2. Concurrency contract. The doc says one producer + one consumer. Prove the
// mutex actually holds under a hostile mix (run with -race), and prove what
// happens when the contract is broken (two consumers) — is it enforced or just
// assumed?
// ---------------------------------------------------------------------------

func TestAudit_ConcurrentAppendPopRace(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir, Options{SegmentBytes: 256})
	var wg sync.WaitGroup
	const n = 300
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			for {
				err := s.Append([]byte(fmt.Sprintf("record-%04d", i)))
				if err == nil {
					break
				}
				if errors.Is(err, ErrFull) {
					continue
				}
				t.Errorf("Append: %v", err)
				return
			}
		}
	}()
	got := 0
	wg.Add(1)
	go func() {
		defer wg.Done()
		for got < n {
			data, commit, ok, err := s.Pop()
			if err != nil {
				t.Errorf("Pop: %v", err)
				return
			}
			if !ok {
				continue
			}
			want := fmt.Sprintf("record-%04d", got)
			if string(data) != want {
				t.Errorf("Pop = %q, want %q (FIFO order broken)", data, want)
				return
			}
			commit()
			got++
		}
	}()
	wg.Wait()
	if got != n {
		t.Fatalf("consumed %d, want %d", got, n)
	}
}

// TestAudit_TwoConsumersDuplicate documents that the single-consumer contract is
// NOT enforced: two goroutines popping concurrently each get the same record.
func TestAudit_TwoConsumersDuplicate(t *testing.T) {
	s := mustOpen(t, t.TempDir(), Options{})
	mustAppend(t, s, "only-one")

	// Two Pops without an intervening commit: the second sees the same readOff.
	a, ca, oka, err := s.Pop()
	if !oka || err != nil {
		t.Fatalf("Pop 1: ok=%v err=%v", oka, err)
	}
	b, cb, okb, err := s.Pop()
	if !okb || err != nil {
		t.Fatalf("Pop 2: ok=%v err=%v", okb, err)
	}
	if string(a) != "only-one" || string(b) != "only-one" {
		t.Fatalf("got %q / %q", a, b)
	}
	t.Logf("CONTRACT: Pop without commit re-delivers the same record (both %q) — single-consumer is assumed, not enforced", a)
	ca()
	cb()
	if _, _, ok, _ := s.Pop(); ok {
		t.Fatal("queue not empty after both commits")
	}
}

// ---------------------------------------------------------------------------
// A3. Crash-recovery of the newest segment: partially-written FRAME vs
// partially-written HEADER vs zero-length file.
// ---------------------------------------------------------------------------

func TestAudit_ReopenPartialFrame(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir, Options{})
	mustAppend(t, s, "good-1")
	mustAppend(t, s, "good-2")
	_ = s.Close()

	// Simulate a crash mid-frame: append a header claiming 50 bytes plus 10.
	p := segFiles(t, dir)[0]
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	var hdr [frameHeaderV1]byte
	binary.BigEndian.PutUint32(hdr[:4], 50)
	binary.BigEndian.PutUint64(hdr[4:], frameSum(hdr[:4], make([]byte, 50)))
	if _, err := f.Write(hdr[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(make([]byte, 10)); err != nil { // torn: 10 of 50
		t.Fatal(err)
	}
	_ = f.Close()

	s2 := mustOpen(t, dir, Options{})
	got := drain(t, s2)
	if len(got) != 2 || got[0] != "good-1" || got[1] != "good-2" {
		t.Fatalf("after torn-frame reopen got %v, want [good-1 good-2]", got)
	}
	mustAppend(t, s2, "after")
	if got := drain(t, s2); len(got) != 1 || got[0] != "after" {
		t.Fatalf("append after torn-tail repair got %v", got)
	}
}

func TestAudit_ReopenPartialSegmentHeader(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir, Options{SegmentBytes: 32})
	mustAppend(t, s, "aaaaaaaaaaaaaaaaaaaaaaaa") // fills seg 1
	mustAppend(t, s, "bbbb")                     // rotates into seg 2
	_ = s.Close()
	segs := segFiles(t, dir)
	if len(segs) < 2 {
		t.Fatalf("want >=2 segments, got %v", segs)
	}
	// Truncate the newest segment's header to 3 bytes: a crash between create
	// and header sync.
	last := segs[len(segs)-1]
	if err := os.Truncate(last, 3); err != nil {
		t.Fatal(err)
	}
	s2 := mustOpen(t, dir, Options{SegmentBytes: 32})
	got := drain(t, s2)
	t.Logf("partial-header newest segment: recovered %v (the segment and its records are dropped by design)", got)
	if len(got) != 1 || got[0] != "aaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("older segment's data lost too: got %v", got)
	}
	mustAppend(t, s2, "still-works")
	if got := drain(t, s2); len(got) != 1 {
		t.Fatalf("spool unusable after partial-header repair: %v", got)
	}
}

func TestAudit_ZeroLengthSegmentFile(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir, Options{SegmentBytes: 32})
	mustAppend(t, s, "aaaaaaaaaaaaaaaaaaaaaaaa")
	mustAppend(t, s, "bbbb")
	_ = s.Close()
	segs := segFiles(t, dir)
	// Zero the OLDEST (head) segment — the one holding undelivered data.
	if err := os.Truncate(segs[0], 0); err != nil {
		t.Fatal(err)
	}
	s2 := mustOpen(t, dir, Options{SegmentBytes: 32})
	got := drain(t, s2)
	t.Logf("zero-length head segment: Open silently removed it, drained %v", got)
	// The lost data is expected; what matters is that the loss is REPORTED.
	// Open returns no error and Pop returns no ErrCorrupt: the caller cannot
	// count it (obs.BufferReadErrors is fed from Pop only).
	if len(got) != 1 || got[0] != "bbbb" {
		t.Fatalf("got %v, want [bbbb]", got)
	}
}

// TestSegmentDropOnLoadIsCounted: a segment whose header is damaged (or a
// zero-length segment file) is removed by load() with no error and no signal to
// the caller. Every other data-loss path in the spool surfaces ErrCorrupt from
// Pop so the agent can count obs.BufferReadErrors{lost=true}; this one loses an
// entire segment (up to SegmentBytes, 8MiB by default) silently.
func TestSegmentDropOnLoadIsCounted(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir, Options{SegmentBytes: 64})
	for i := 0; i < 8; i++ {
		mustAppend(t, s, fmt.Sprintf("record-%02d-payload", i))
	}
	_ = s.Close()
	segs := segFiles(t, dir)
	if len(segs) < 3 {
		t.Fatalf("want >=3 segments, got %v", segs)
	}
	// Corrupt the magic of a middle segment (bit rot / partially-written file).
	if err := os.WriteFile(segs[1], []byte("XXXXXXXX"), 0o644); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir, Options{SegmentBytes: 64})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s2.Close() }()

	var corrupt int
	var got []string
	for {
		data, commit, ok, err := s2.Pop()
		if err != nil {
			if errors.Is(err, ErrCorrupt) {
				corrupt++
				continue
			}
			t.Fatalf("Pop: %v", err)
		}
		if !ok {
			break
		}
		got = append(got, string(data))
		commit()
	}
	if len(got) == 8 {
		t.Fatalf("no records lost — test setup did not corrupt anything")
	}
	if corrupt == 0 {
		t.Fatalf("BUG: %d of 8 records lost (drained %v) but Open returned nil and Pop never reported ErrCorrupt — "+
			"the whole segment is dropped silently, so the agent cannot count the loss", 8-len(got), got)
	}
}

// ---------------------------------------------------------------------------
// A4. Cursor edge cases.
// ---------------------------------------------------------------------------

func writeCursor(t *testing.T, dir string, seq, off int64, valid bool) {
	t.Helper()
	var buf [cursorLen]byte
	binary.BigEndian.PutUint64(buf[:8], uint64(seq))
	binary.BigEndian.PutUint64(buf[8:16], uint64(off))
	sum := cursorSum(buf[:16])
	if !valid {
		sum ^= 1
	}
	binary.BigEndian.PutUint64(buf[16:], sum)
	if err := os.WriteFile(filepath.Join(dir, cursorName), buf[:], 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestAudit_CursorPastSegmentEnd(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir, Options{})
	mustAppend(t, s, "one")
	mustAppend(t, s, "two")
	_ = s.Close()
	// A cursor whose offset overruns the segment (impossible in practice, but
	// the load path must not seek past the end and silently swallow the queue).
	writeCursor(t, dir, 1, 1<<40, true)
	s2 := mustOpen(t, dir, Options{})
	got := drain(t, s2)
	if len(got) != 2 {
		t.Fatalf("overrunning cursor: got %v, want both records redelivered", got)
	}
}

func TestAudit_TornCursorRedelivers(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir, Options{})
	mustAppend(t, s, "one")
	mustAppend(t, s, "two")
	_, commit, ok, err := s.Pop()
	if !ok || err != nil {
		t.Fatalf("Pop: %v %v", ok, err)
	}
	commit()
	_ = s.Close()
	writeCursor(t, dir, 1, 100, false) // torn: bad checksum
	s2 := mustOpen(t, dir, Options{})
	if got := drain(t, s2); len(got) != 2 {
		t.Fatalf("torn cursor: got %v, want redelivery of both", got)
	}
}

// TestCursorSeqAheadKeepsSegments: a cursor naming a segment sequence higher
// than any on disk makes load() delete every segment below it, including
// undelivered data. A torn cursor is caught by its checksum, but a cursor that
// is checksum-valid yet stale-high (e.g. restored from a snapshot, or a spool
// dir whose segments were rolled back) silently drops the backlog.
func TestCursorSeqAheadKeepsSegments(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir, Options{SegmentBytes: 40})
	for i := 0; i < 6; i++ {
		mustAppend(t, s, fmt.Sprintf("rec-%02d-data", i))
	}
	_ = s.Close()
	segs := segFiles(t, dir)
	if len(segs) < 3 {
		t.Fatalf("want >=3 segments, got %d", len(segs))
	}
	writeCursor(t, dir, 1<<30, segHeaderLen, true) // valid checksum, absurd seq

	s2 := mustOpen(t, dir, Options{SegmentBytes: 40})
	got := drain(t, s2)
	if len(got) != 6 {
		t.Fatalf("BUG: cursor seq ahead of every segment dropped %d of 6 records "+
			"(load() removes segs[0] while segs[0].seq < cursorSeq without bounding by the newest seq); drained %v",
			6-len(got), got)
	}
}

// TestSeqRestartAfterVersionDropKeepsBacklog reaches the same defect through
// the PUBLIC API only, with no hand-written cursor.
//
// The documented lifecycle of a format bump (or a downgrade to a build that does
// not know the on-disk version) drops every segment at open. load() then starts
// the new segment at seq 1 — but the cursor file has no version, survives, and
// still names the OLD (higher) sequence. It is not rewritten until the first
// commit. Any records appended and NOT yet committed when the process next
// restarts are silently deleted by load()'s "drop consumed segments" loop, which
// removes every segs[0] with seq < cursorSeq without bounding cursorSeq by the
// newest segment on disk.
func TestSeqRestartAfterVersionDropKeepsBacklog(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: a normal spool that reaches segment seq 4 and commits, so the
	// cursor on disk names seq 4.
	s := mustOpen(t, dir, Options{SegmentBytes: 40})
	for i := 0; i < 8; i++ {
		mustAppend(t, s, fmt.Sprintf("old-%02d", i))
	}
	for i := 0; i < 8; i++ {
		_, commit, ok, err := s.Pop()
		if !ok || err != nil {
			t.Fatalf("Pop: ok=%v err=%v", ok, err)
		}
		commit()
	}
	_ = s.Close()
	cur, err := os.ReadFile(filepath.Join(dir, cursorName))
	if err != nil {
		t.Fatal(err)
	}
	cursorSeq := binary.BigEndian.Uint64(cur[:8])
	if cursorSeq < 2 {
		t.Fatalf("setup: cursor seq %d too low to demonstrate", cursorSeq)
	}

	// Phase 2: the on-disk format changes (or the agent is rolled back), so the
	// remaining segments name a version this build cannot read. Documented
	// behavior: they are dropped at open.
	for _, p := range segFiles(t, dir) {
		f, err := os.OpenFile(p, os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		var v [2]byte
		binary.BigEndian.PutUint16(v[:], 99) // unknown version
		if _, err := f.WriteAt(v[:], int64(len(segMagic))); err != nil {
			t.Fatal(err)
		}
		_ = f.Close()
	}

	// Phase 3: fresh start. Segment numbering restarts at 1; the cursor still
	// says seq 4. The collector is down, so nothing is ever committed.
	s2 := mustOpen(t, dir, Options{SegmentBytes: 40})
	const n = 8
	for i := 0; i < n; i++ {
		mustAppend(t, s2, fmt.Sprintf("new-%02d", i))
	}
	_ = s2.Close()

	// Phase 4: the pod restarts. Every one of those records is durable on disk
	// (each Append fsynced) and none was ever delivered.
	s3 := mustOpen(t, dir, Options{SegmentBytes: 40})
	got := drain(t, s3)
	if len(got) != n {
		t.Fatalf("BUG: %d of %d fsynced, undelivered records silently deleted at Open "+
			"(cursor seq %d > every segment seq after the numbering restarted at 1); drained %v",
			n-len(got), n, cursorSeq, got)
	}
}

// ---------------------------------------------------------------------------
// A5. Retirement / accounting.
// ---------------------------------------------------------------------------

func TestAudit_BacklogAcrossSegmentBoundary(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir, Options{SegmentBytes: 40})
	for i := 0; i < 6; i++ {
		mustAppend(t, s, fmt.Sprintf("rec-%02d", i))
	}
	before := s.Bytes()
	if before <= 0 {
		t.Fatalf("Bytes() = %d before any Pop", before)
	}
	prev := before
	for i := 0; i < 6; i++ {
		data, commit, ok, err := s.Pop()
		if !ok || err != nil {
			t.Fatalf("Pop %d: ok=%v err=%v", i, ok, err)
		}
		commit()
		now := s.Bytes()
		if now > prev {
			t.Fatalf("backlog grew across a Pop/commit: %d -> %d (record %q)", prev, now, data)
		}
		if now < 0 {
			t.Fatalf("backlog went negative: %d", now)
		}
		prev = now
	}
	if s.Bytes() != 0 {
		t.Fatalf("drained backlog = %d, want 0 (frames+headers must fully account)", s.Bytes())
	}
	t.Logf("on-disk after full drain: %d bytes (Bytes()=0) — delivered-but-unreclaimed prefix", diskBytes(t, dir))
}

func TestAudit_RetirementWithCursorMidSegment(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir, Options{SegmentBytes: 48})
	for i := 0; i < 9; i++ {
		mustAppend(t, s, fmt.Sprintf("r%02d", i))
	}
	// Consume 4 (lands mid-segment), then restart.
	for i := 0; i < 4; i++ {
		_, commit, ok, err := s.Pop()
		if !ok || err != nil {
			t.Fatalf("Pop: %v %v", ok, err)
		}
		commit()
	}
	_ = s.Close()
	s2 := mustOpen(t, dir, Options{SegmentBytes: 48})
	got := drain(t, s2)
	want := []string{"r04", "r05", "r06", "r07", "r08"}
	if len(got) != len(want) {
		t.Fatalf("after restart mid-segment got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("after restart got %v, want %v", got, want)
		}
	}
}

// TestAudit_MaxBytesAfterRetirement: MaxBytes caps the UNDELIVERED backlog, so a
// consumer that keeps up must let the producer keep appending forever.
func TestAudit_MaxBytesAfterRetirement(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir, Options{SegmentBytes: 64, MaxBytes: 256})
	for i := 0; i < 500; i++ {
		if err := s.Append([]byte(fmt.Sprintf("payload-%03d", i))); err != nil {
			t.Fatalf("Append %d: %v (backlog=%d, disk=%d)", i, err, s.Bytes(), diskBytes(t, dir))
		}
		data, commit, ok, err := s.Pop()
		if !ok || err != nil {
			t.Fatalf("Pop %d: ok=%v err=%v", i, ok, err)
		}
		if want := fmt.Sprintf("payload-%03d", i); string(data) != want {
			t.Fatalf("Pop = %q, want %q", data, want)
		}
		commit()
	}
	if d := diskBytes(t, dir); d > 256+2*64 {
		t.Fatalf("on-disk %d far above MaxBytes+1seg after a keeping-up consumer", d)
	}
}

// ---------------------------------------------------------------------------
// A6. Write error mid-Append (the ENOSPC case the spool exists for).
// ---------------------------------------------------------------------------

// TestAudit_WriteErrorMidAppend closes the write handle underneath Append so the
// frame write fails, and checks the queue is left consistent and reopens clean.
func TestAudit_WriteErrorMidAppend(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir, Options{})
	mustAppend(t, s, "before")

	s.mu.Lock()
	sizeBefore := s.segs[len(s.segs)-1].size
	_ = s.w.Close() // every subsequent write/sync/truncate on this fd fails
	s.mu.Unlock()

	if err := s.Append([]byte("during-failure")); err == nil {
		t.Fatal("Append on a dead handle returned nil")
	}
	s.mu.Lock()
	if s.w != nil {
		t.Errorf("write handle not dropped after a failed rollback")
	}
	if got := s.segs[len(s.segs)-1].size; got != sizeBefore {
		t.Errorf("tail size advanced on a failed Append: %d -> %d", sizeBefore, got)
	}
	s.mu.Unlock()

	// Next Append must reopen and re-verify the tail.
	mustAppend(t, s, "after")
	got := drain(t, s)
	for _, r := range got {
		if r == "during-failure" {
			t.Fatalf("BUG: a record whose Append returned an error was delivered: %v", got)
		}
	}
	if len(got) != 2 || got[0] != "before" || got[1] != "after" {
		t.Fatalf("got %v, want [before after]", got)
	}
	_ = s.Close()

	// And it must survive a restart.
	s2 := mustOpen(t, dir, Options{})
	mustAppend(t, s2, "post-restart")
	if got := drain(t, s2); len(got) != 1 || got[0] != "post-restart" {
		t.Fatalf("after restart got %v", got)
	}
}

// ---------------------------------------------------------------------------
// A7. Use after Close.
// ---------------------------------------------------------------------------

// TestAudit_PopAfterClose: Append checks s.closed; Pop and commit do not.
func TestAudit_PopAfterClose(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir, Options{})
	mustAppend(t, s, "one")
	mustAppend(t, s, "two")
	_ = s.Close()

	if err := s.Append([]byte("x")); err == nil {
		t.Fatal("Append after Close returned nil")
	}
	data, commit, ok, err := s.Pop()
	if !ok {
		t.Logf("Pop after Close: ok=false err=%v (rejected)", err)
		return
	}
	t.Logf("CONTRACT: Pop after Close still returns %q (err=%v) — Close is not enforced on the read path", data, err)
	commit() // persistCursor is a no-op: cursorF is nil
	s2 := mustOpen(t, dir, Options{})
	got := drain(t, s2)
	if len(got) != 2 {
		t.Errorf("BUG: a commit after Close was partially applied — restart drained %v, want both records "+
			"(the commit must either persist or be rejected, not advance in-memory state only)", got)
	} else {
		t.Logf("commit after Close does not persist (both records redelivered) — at-least-once holds")
	}
}

// ---------------------------------------------------------------------------
// A8. Empty payloads and a valid-header/empty-body segment.
// ---------------------------------------------------------------------------

func TestAudit_EmptyRecord(t *testing.T) {
	s := mustOpen(t, t.TempDir(), Options{})
	if err := s.Append(nil); err != nil {
		t.Fatalf("Append(nil): %v", err)
	}
	mustAppend(t, s, "next")
	data, commit, ok, err := s.Pop()
	if !ok || err != nil {
		t.Fatalf("Pop: ok=%v err=%v", ok, err)
	}
	if len(data) != 0 {
		t.Fatalf("empty record came back as %q", data)
	}
	commit()
	if got := drain(t, s); len(got) != 1 || got[0] != "next" {
		t.Fatalf("after an empty record got %v", got)
	}
}

func TestAudit_HeaderOnlyMiddleSegment(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir, Options{SegmentBytes: 32})
	mustAppend(t, s, "aaaaaaaaaaaaaaaaaaaaaaaa")
	mustAppend(t, s, "bbbbbbbbbbbbbbbbbbbbbbbb")
	mustAppend(t, s, "cccc")
	_ = s.Close()
	segs := segFiles(t, dir)
	if len(segs) < 3 {
		t.Fatalf("want >=3 segments, got %d", len(segs))
	}
	// Truncate a MIDDLE segment to header-only: valid header, empty body.
	if err := os.Truncate(segs[1], segHeaderLen); err != nil {
		t.Fatal(err)
	}
	s2 := mustOpen(t, dir, Options{SegmentBytes: 32})
	var corrupt int
	var got []string
	for {
		data, commit, ok, err := s2.Pop()
		if err != nil {
			if errors.Is(err, ErrCorrupt) {
				corrupt++
				continue
			}
			t.Fatalf("Pop: %v", err)
		}
		if !ok {
			break
		}
		got = append(got, string(data))
		commit()
	}
	t.Logf("header-only middle segment: drained %v, ErrCorrupt count=%d", got, corrupt)
	if len(got) != 2 {
		t.Fatalf("an empty middle segment cost more than its own (empty) contents: got %v", got)
	}
	// The truncated middle segment's record ("bbb...") is lost — but that loss
	// must be SURFACED as one ErrCorrupt so the agent counts it, not retired
	// silently in retireConsumedLocked.
	if corrupt != 1 {
		t.Fatalf("header-only middle segment loss went uncounted: ErrCorrupt count=%d, want 1", corrupt)
	}
}
