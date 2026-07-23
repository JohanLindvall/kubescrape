// Package spool is a disk-backed FIFO byte queue. Records are appended durably
// to segment files and consumed in order, surviving process restarts. It backs
// the agent's log-export buffer: a collector outage spools records to disk
// (bounded by a size cap) instead of pinning the tailer to old file offsets, so
// the source files can rotate away while their data waits on the node.
//
// # On-disk format
//
// A directory of segment files (`<seq>.seg`, zero-padded, ascending). Each
// segment opens with an 8-byte header — the magic "KSPOOL" plus a big-endian
// uint16 format version — and continues with frames laid out per that version.
// Version 1 frames are: a 4-byte big-endian length, an 8-byte xxhash64 over the
// length bytes and the payload together, then the payload. Checksumming the
// length alongside the payload means a flipped length byte is caught instead of
// mis-framing the rest of the segment, and a damaged record is dropped and
// reported rather than handed to the collector as plausible-looking telemetry.
//
// A segment whose header is absent or names a version this build does not know
// is discarded at open: the spool is a transient buffer, so carrying a reader
// for every past or future format would not pay for itself. The version is per
// segment rather than per spool, so when a new format is added (bump
// formatVersion and extend the frame read/write paths), segments of both
// versions coexist in one directory, each read with the framing it was written
// in.
//
// Appends fsync the frame before returning, so a producer that has observed a
// successful Append may safely advance its own checkpoint. A separate `cursor`
// file records the read position ({segment seq, offset}) and is rewritten in
// place after each commit; a torn cursor fails its checksum on the next load
// and redelivers from the oldest segment (duplicates, within at-least-once). On
// restart the newest segment's torn tail — a frame the crash left incomplete —
// is truncated away, while a frame that is whole but fails its checksum is
// dropped by Pop when it is reached, so one damaged byte costs one record
// rather than every record behind it.
package spool

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/cespare/xxhash/v2"
)

// ErrFull is returned by Append when the queue is at its size cap.
var ErrFull = errors.New("spool: full")

// ErrCorrupt is returned by Pop when the head frame fails its integrity check:
// a checksum mismatch, a length overrunning the segment, or a segment tail too
// short to hold a frame header. The damaged data is dropped — the frame alone
// when the framing is still trustworthy, otherwise the whole segment — and the
// queue advances, so corruption degrades to reported loss rather than to
// corrupt telemetry or a wedged queue.
var ErrCorrupt = errors.New("spool: corrupt")

const (
	defaultSegmentBytes = 8 << 20
	segSuffix           = ".seg"
	cursorName          = "cursor"

	// segHeaderLen is the per-segment header: magic(6) + version(2).
	segHeaderLen = 8

	// formatVersion is the frame format new segments are written in. Bump it
	// (and extend frameHeaderLen, knownVersions, and the frame read/write
	// paths) to change the framing; existing segments keep their own version.
	formatVersion uint16 = 1

	// frameHeaderV1 is version 1's frame header: a uint32 big-endian length
	// plus the xxhash64 of the length bytes and the payload.
	frameHeaderV1 = 12
)

// FrameOverhead is the per-record framing cost in bytes for the current
// format, so callers can size backlog comparisons against record lengths.
const FrameOverhead = 12

// The exported overhead must track the current format's frame header.
var _ = [1]struct{}{}[FrameOverhead-frameHeaderV1]

// segMagic opens every segment, followed by the big-endian format version.
var segMagic = [6]byte{'K', 'S', 'P', 'O', 'O', 'L'}

// knownVersions are the frame formats this build can read. A segment naming
// anything else — an older format, or one written by a newer agent — is
// discarded at open.
var knownVersions = []uint16{1}

// frameHeaderLen is the frame header size of a segment's format version.
func frameHeaderLen(version uint16) int64 {
	switch version {
	case 1:
		return frameHeaderV1
	default:
		return frameHeaderV1 // unreachable: unknown versions never load
	}
}

// frameSum is version 1's integrity check over one frame: the length bytes and
// the payload together, so a corrupted length cannot masquerade as a valid one.
func frameSum(lenBytes, payload []byte) uint64 {
	var d xxhash.Digest // stack-held: no allocation per frame
	d.Reset()
	_, _ = d.Write(lenBytes)
	_, _ = d.Write(payload)
	return d.Sum64()
}

// Options configure a Spool.
type Options struct {
	// SegmentBytes caps one segment file (0 = 8MiB). A single record may exceed
	// it; segments rotate lazily once non-empty.
	SegmentBytes int64
	// MaxBytes caps the total on-disk size; Append returns ErrFull beyond it
	// (0 = unbounded).
	MaxBytes int64
}

type segment struct {
	seq  int64
	size int64
	path string
	// version is the segment's frame format, read from its header. It is per
	// segment, so a format bump does not invalidate the segments already on
	// disk — each is read with the framing it was written in.
	version uint16
}

// frameHdr is the frame header size for this segment's format.
func (sg segment) frameHdr() int64 { return frameHeaderLen(sg.version) }

// Spool is a durable FIFO byte queue. One producer (Append) and one consumer
// (Pop/commit) may run concurrently; all state is mutex-guarded.
type Spool struct {
	dir         string
	segmentSize int64
	maxBytes    int64

	mu      sync.Mutex
	segs    []segment // ascending by seq; segs[0] is the read head, last is the write tail
	w       *os.File  // append handle for the newest segment
	cursorF *os.File
	readF   *os.File // cached read handle for the head segment
	readSeq int64    // segment seq readF is open on
	readOff int64    // offset within segs[0] of the next unread frame
	signal  chan struct{}
	closed  bool
	// pendingCorrupt counts corrupt segments whose records were lost and must be
	// surfaced: a bad/damaged header dropped at load, or a header-only middle
	// segment (truncation damage) retired in retireConsumedLocked. Pop surfaces
	// one ErrCorrupt per count so the consumer's normal read-error counting sees
	// it — otherwise a whole segment of records would vanish with no signal.
	// Deliberately in-memory: a crash before the next Pop loses only the loss
	// COUNT (the data is already gone either way); persisting it is not worth a
	// disk format field.
	pendingCorrupt int
}

// Open opens or creates the spool rooted at dir.
func Open(dir string, opts Options) (*Spool, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	segSize := opts.SegmentBytes
	if segSize <= 0 {
		segSize = defaultSegmentBytes
	}
	s := &Spool{dir: dir, segmentSize: segSize, maxBytes: opts.MaxBytes, signal: make(chan struct{}, 1)}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Spool) segPath(seq int64) string {
	return filepath.Join(s.dir, fmt.Sprintf("%020d%s", seq, segSuffix))
}

// load discovers existing segments, repairs the write tail, and positions the
// read cursor. Segments in an unreadable format are removed.
func (s *Spool) load() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, segSuffix) {
			continue
		}
		seq, err := strconv.ParseInt(strings.TrimSuffix(name, segSuffix), 10, 64)
		if err != nil {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return err
		}
		path := s.segPath(seq)
		version, ok, corrupt, err := readSegHeader(path)
		if err != nil {
			return err
		}
		if !ok {
			// No readable header, or a format this build cannot read: the spool
			// is a transient buffer, so drop it rather than guess at its framing.
			// A DAMAGED header (bit rot, torn create) lost real records — Pop
			// surfaces one ErrCorrupt so the loss is counted; a merely unknown
			// version is a deliberate format change whose data the design accepts
			// dropping, so it stays silent.
			_ = os.Remove(path)
			if corrupt {
				s.pendingCorrupt++
			}
			continue
		}
		s.segs = append(s.segs, segment{seq: seq, size: info.Size(), path: path, version: version})
	}
	sort.Slice(s.segs, func(i, j int) bool { return s.segs[i].seq < s.segs[j].seq })

	cursorSeq, cursorOff, haveCursor, err := s.loadCursor()
	if err != nil {
		return err
	}

	if len(s.segs) == 0 {
		// Every segment was dropped (a format bump or wholesale corruption).
		// Resume numbering PAST the persisted cursor, never back at 1: segment
		// seqs must stay monotonic across the spool's life, or a fresh segment
		// could reuse the exact seq a stale cursor still names and be mistaken
		// for the consumed one — deleting live, never-delivered records.
		start := int64(1)
		if haveCursor {
			start = max(1, cursorSeq+1)
		}
		if err := s.appendSegment(start); err != nil {
			return err
		}
	} else if err := s.openTail(); err != nil {
		return err
	}

	// No valid cursor (fresh spool, or a torn rewrite): start from the oldest
	// segment. If the cursor names a segment newer than anything on disk it is
	// foreign or hand-corrupted — discard it and redeliver from the oldest,
	// which is safe under at-least-once, rather than letting the removal loop
	// below strip every segment down to the last.
	if !haveCursor || cursorSeq > s.segs[len(s.segs)-1].seq {
		cursorSeq, cursorOff = s.segs[0].seq, segHeaderLen
	}
	// Drop consumed segments (seq < cursorSeq) left behind by a crash between
	// deleting a segment and persisting the cursor.
	for len(s.segs) > 1 && s.segs[0].seq < cursorSeq {
		_ = os.Remove(s.segs[0].path)
		s.segs = s.segs[1:]
	}
	s.readOff = segHeaderLen
	if s.segs[0].seq == cursorSeq && cursorOff <= s.segs[0].size {
		s.readOff = max(cursorOff, segHeaderLen)
	}
	return nil
}

// readSegHeader returns the segment's format version. ok is false when the file
// carries no magic or names a version this build cannot read. corrupt is true
// only for a DAMAGED header (missing/wrong magic, short read) — genuine loss to
// count; a valid-magic-but-unknown-version segment (a deliberate format bump or
// a rollback, whose data the design accepts dropping) is ok=false, corrupt=false.
func readSegHeader(path string) (version uint16, ok, corrupt bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, false, false, nil
		}
		return 0, false, false, err
	}
	defer func() { _ = f.Close() }()
	var buf [segHeaderLen]byte
	n, err := f.ReadAt(buf[:], 0)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return 0, false, false, err
	}
	if n < segHeaderLen || !bytes.Equal(buf[:len(segMagic)], segMagic[:]) {
		return 0, false, true, nil // damaged header: the records are lost, count it
	}
	version = binary.BigEndian.Uint16(buf[len(segMagic):])
	return version, slices.Contains(knownVersions, version), false, nil
}

// openTail opens the newest segment for appending and truncates any torn tail
// (a frame whose write a crash left incomplete) or orphan bytes beyond the last
// whole frame (a partial append whose rollback did not complete). A tail in an
// older format is frozen — never appended to — and a fresh segment takes over,
// so one file never mixes two framings.
func (s *Spool) openTail() error {
	tail := &s.segs[len(s.segs)-1]
	good, err := lastCompleteOffset(*tail)
	if err != nil {
		return err
	}
	if info, err := os.Stat(tail.path); err != nil {
		return err
	} else if info.Size() != good {
		if err := os.Truncate(tail.path, good); err != nil {
			return err
		}
	}
	tail.size = good
	if tail.version != formatVersion {
		return s.appendSegment(tail.seq + 1)
	}
	f, err := os.OpenFile(tail.path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	s.w = f
	return nil
}

// appendSegment creates a new segment with the given seq, writes its header,
// and makes it the write tail.
func (s *Spool) appendSegment(seq int64) error {
	path := s.segPath(seq)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	var hdr [segHeaderLen]byte
	copy(hdr[:], segMagic[:])
	binary.BigEndian.PutUint16(hdr[len(segMagic):], formatVersion)
	if _, err := f.Write(hdr[:]); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if s.w != nil {
		_ = s.w.Close()
	}
	s.w = f
	s.segs = append(s.segs, segment{seq: seq, size: segHeaderLen, path: path, version: formatVersion})
	s.syncDir()
	return nil
}

// lastCompleteOffset walks a segment's frames by their lengths and returns the
// offset just past the last structurally complete one — where a torn tail is
// truncated and the next append lands.
//
// It deliberately does not verify checksums. A torn write only ever damages the
// END of the file, so a frame that is structurally whole but fails its checksum
// is far more likely to be a durable frame with a damaged byte than a torn one;
// truncating there would throw away every good frame that follows it. Instead
// such a frame stays put and Pop drops it individually (reporting ErrCorrupt),
// so the blast radius of one bad byte is one record. A torn write whose length
// field itself was mangled is still caught: its bogus length either overruns
// the file (truncated here) or desynchronizes the walk into garbage that fails
// the same structural check a frame or two later.
func lastCompleteOffset(sg segment) (int64, error) {
	f, err := os.Open(sg.path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	if sg.size < segHeaderLen {
		return sg.size, nil // shorter than its own header: nothing usable
	}
	fh := sg.frameHdr()
	off := int64(segHeaderLen)
	hdr := make([]byte, fh)
	for off+fh <= sg.size {
		if _, err := f.ReadAt(hdr, off); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break // file shorter than recorded: torn tail
			}
			// A real I/O error is not a truncation point: truncating here
			// would silently discard valid fsynced frames after it.
			return 0, err
		}
		end := off + fh + int64(binary.BigEndian.Uint32(hdr[:4]))
		if end > sg.size {
			break // torn frame
		}
		off = end
	}
	return off, nil
}

// backlog is the number of undelivered bytes: every segment's size minus the
// consumed prefix of the read head (caller holds the lock). Delivered records
// still occupy disk until their whole segment is retired, so physical usage can
// exceed this by up to one segment.
func (s *Spool) backlog() int64 {
	var total int64
	for _, seg := range s.segs {
		total += seg.size
	}
	return total - s.readOff
}

// Append durably enqueues one record. It returns ErrFull when the size cap
// would be exceeded, leaving the queue unchanged so the caller can apply
// backpressure.
func (s *Spool) Append(data []byte) error {
	frame := int64(frameHeaderV1 + len(data))
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("spool: closed")
	}
	if s.maxBytes > 0 && s.backlog()+frame > s.maxBytes {
		return ErrFull
	}
	if s.w == nil {
		// A previous failed append could not roll back and closed the handle;
		// reopen re-verifies the tail and truncates the orphan bytes.
		if err := s.openTail(); err != nil {
			return err
		}
	}
	tail := &s.segs[len(s.segs)-1]
	// Rotate only once the tail holds a frame, so an oversized record cannot
	// leave an empty segment behind.
	if tail.size > segHeaderLen && tail.size+frame > s.segmentSize {
		if err := s.appendSegment(tail.seq + 1); err != nil {
			return err
		}
		tail = &s.segs[len(s.segs)-1]
	}
	var hdr [frameHeaderV1]byte
	binary.BigEndian.PutUint32(hdr[:4], uint32(len(data)))
	binary.BigEndian.PutUint64(hdr[4:], frameSum(hdr[:4], data))
	if _, err := s.w.Write(hdr[:]); err != nil {
		s.rollbackTail(tail)
		return err
	}
	if _, err := s.w.Write(data); err != nil {
		s.rollbackTail(tail)
		return err
	}
	if err := s.w.Sync(); err != nil {
		s.rollbackTail(tail)
		return err
	}
	tail.size += frame
	s.notify()
	return nil
}

// rollbackTail restores the write tail to its last known-good size after a
// failed append (e.g. ENOSPC mid-frame — the condition a disk spool exists
// for). O_APPEND writes land at the physical end, so leaving partial bytes
// would desynchronize the frame stream from the size accounting and could be
// misparsed as frames after a restart. If the truncate itself fails the
// handle is closed and the next Append reopens and re-verifies the tail.
func (s *Spool) rollbackTail(tail *segment) {
	if err := s.w.Truncate(tail.size); err == nil {
		return
	}
	_ = s.w.Close()
	s.w = nil
}

// Pop returns the next record and a commit function that removes it. ok false
// with a nil error means the queue is empty; a non-nil error means the head
// frame could not be read — the caller should surface it and retry. ErrCorrupt
// means damaged data was dropped and fs.ErrNotExist that the head segment's
// file was gone; either way the queue has advanced past it, so the next Pop
// makes progress. The record is not removed until commit is called, so a crash
// before commit re-delivers it (at-least-once). commit is idempotent.
func (s *Spool) Pop() (data []byte, commit func(), ok bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Report each corrupt segment whose records were lost (dropped at load, or a
	// header-only middle segment retired below) as one corrupt read, so the
	// caller's read-error metric accounts for the lost records.
	if s.pendingCorrupt > 0 {
		s.pendingCorrupt--
		return nil, nil, false, fmt.Errorf("%w: unreadable segment dropped", ErrCorrupt)
	}
	// Retire fully-consumed head segments up front. commit's retire loop
	// rightly never removes the write tail — but when the consumer was fully
	// caught up (readOff == tail size, the healthy steady state) and an
	// Append then rotated to a new segment, no commit ever runs again against
	// the old head, so without this the queue would report empty forever
	// while the backlog grows. Also heals such wedged spools loaded from disk.
	s.retireConsumedLocked()
	head := &s.segs[0]
	fh := head.frameHdr()
	if s.readOff+fh > head.size {
		if len(s.segs) > 1 {
			// A middle segment never grows, so trailing bytes shorter than a
			// frame header are truncation damage; its remaining frames are
			// unreadable. Skip the segment (surfaced to the caller) rather
			// than reporting an empty queue forever while the backlog grows.
			seq := head.seq
			s.skipLostHead()
			return nil, nil, false, fmt.Errorf("%w: truncated segment %d", ErrCorrupt, seq)
		}
		return nil, nil, false, nil // write tail: wait for the next append
	}
	f, err := s.readHandle(head)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			s.skipLostHead()
		}
		return nil, nil, false, err
	}
	hdr := make([]byte, fh)
	if _, err := f.ReadAt(hdr, s.readOff); err != nil {
		s.dropReadHandle() // reopen fresh next time
		return nil, nil, false, err
	}
	n := int64(binary.BigEndian.Uint32(hdr[:4]))
	if s.readOff+fh+n > head.size {
		// An overshooting length is corruption on any segment: a middle
		// segment's size is final, and the write tail's size only ever covers
		// whole fsynced frames (Append advances it after Sync, under the lock;
		// openTail truncates torn tails at load) — so this is never a torn
		// append in progress. The frame boundaries from here on are lost:
		// waiting would wedge the queue forever, and on the tail a future
		// append could grow the segment past the bogus length and deliver
		// bytes spanning unrelated frames. Skip the segment; the tail is
		// replaced by a fresh one so appends keep working.
		seq := head.seq
		s.skipLostHead()
		return nil, nil, false, fmt.Errorf("%w: frame length in segment %d", ErrCorrupt, seq)
	}
	payload := make([]byte, n)
	if _, err := f.ReadAt(payload, s.readOff+fh); err != nil {
		s.dropReadHandle()
		return nil, nil, false, err
	}
	end := s.readOff + fh + n
	if frameSum(hdr[:4], payload) != binary.BigEndian.Uint64(hdr[4:]) {
		// The record is damaged. Its length still framed it within the
		// segment, so trust the framing exactly this far: drop the frame,
		// advance, and let the next Pop re-check. If the length itself was
		// what got corrupted, the next read lands mid-stream and fails its own
		// checks, which skips the segment.
		seq := head.seq
		s.readOff = end
		s.persistCursor()
		return nil, nil, false, fmt.Errorf("%w: frame checksum in segment %d", ErrCorrupt, seq)
	}

	var done bool
	headSeq := head.seq
	commit = func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if done {
			return
		}
		done = true
		// The head may no longer be the segment this frame was popped from:
		// a later Pop can hit a vanished/corrupt head and skip to the next
		// segment. Applying the stale end offset there would silently retire
		// the NEW head's never-delivered records, so a commit whose segment
		// is gone is a no-op — the frame's segment was already skipped and
		// its loss accounted.
		if len(s.segs) == 0 || s.segs[0].seq != headSeq {
			return
		}
		s.readOff = end
		s.retireConsumedLocked()
		s.persistCursor()
	}
	return payload, commit, true, nil
}

// retireConsumedLocked removes fully-consumed head segments (never the write
// tail); caller holds the lock and persists the cursor afterwards if it
// matters for durability (Pop's opportunistic call relies on the next commit
// for that).
func (s *Spool) retireConsumedLocked() {
	for len(s.segs) > 1 && s.readOff >= s.segs[0].size {
		// A non-tail segment sized down to (or below) its bare header carries no
		// frames yet is not the legitimately-empty write tail: rotation only
		// freezes a tail once it exceeds the header (see Append), so this is
		// truncation damage. Retiring it drops its records with readOff already
		// at the header, so no Pop ever reads — and reports — the loss. Count it
		// as one corrupt read, matching every other corruption path.
		if s.segs[0].size <= segHeaderLen {
			s.pendingCorrupt++
		}
		if s.readSeq == s.segs[0].seq {
			s.dropReadHandle()
		}
		_ = os.Remove(s.segs[0].path)
		s.segs = s.segs[1:]
		s.readOff = segHeaderLen
	}
}

// readHandle returns a cached open handle for the head segment, reopening when
// the head changed (Pop used to open the file per call).
func (s *Spool) readHandle(head *segment) (*os.File, error) {
	if s.readF != nil && s.readSeq == head.seq {
		return s.readF, nil
	}
	s.dropReadHandle()
	f, err := os.Open(head.path)
	if err != nil {
		return nil, err
	}
	s.readF, s.readSeq = f, head.seq
	return f, nil
}

func (s *Spool) dropReadHandle() {
	if s.readF != nil {
		_ = s.readF.Close()
		s.readF = nil
	}
}

// skipLostHead advances past a head segment whose frames are unrecoverable
// (file vanished, or a corrupt/truncated frame stream): waiting on them would
// wedge the queue forever. Any remaining file is removed so a restart does
// not rediscover the dead segment. The write tail is never skipped in place —
// it is replaced by a fresh segment instead, so appends keep working (caller
// holds the lock).
func (s *Spool) skipLostHead() {
	s.dropReadHandle()
	if len(s.segs) > 1 {
		_ = os.Remove(s.segs[0].path) // no-op when the file already vanished
		s.segs = s.segs[1:]
		s.readOff = segHeaderLen
		s.persistCursor()
		return
	}
	// The single segment (also the write tail) is unusable; start a fresh
	// one. The old entry is dropped only once the replacement exists —
	// leaving segs empty would panic every segs[0]/segs[len-1] access (Pop,
	// Append's openTail, stale commits).
	seq := s.segs[0].seq + 1
	if s.w != nil {
		_ = s.w.Close()
		s.w = nil
	}
	oldPath := s.segs[0].path
	old := s.segs
	// NB: appendSegment appends into the shared backing array, overwriting
	// old[0] — only oldPath (copied above) identifies the dead file afterwards.
	s.segs = s.segs[:0]
	if err := s.appendSegment(seq); err != nil {
		s.segs = old // keep the stale entry; reads keep erroring, but no panic
		return
	}
	_ = os.Remove(oldPath) // no-op when the file already vanished
	s.readOff = segHeaderLen
	s.persistCursor()
}

// Signal fires (non-blocking) after each Append so a consumer can wait for work
// without polling.
func (s *Spool) Signal() <-chan struct{} { return s.signal }

func (s *Spool) notify() {
	select {
	case s.signal <- struct{}{}:
	default:
	}
}

// Bytes is the current undelivered backlog in bytes (what MaxBytes caps).
func (s *Spool) Bytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.backlog()
}

// Close releases the file handles. It does not delete queued data.
func (s *Spool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.dropReadHandle()
	if s.w != nil {
		_ = s.w.Close()
		s.w = nil
	}
	if s.cursorF != nil {
		_ = s.cursorF.Close()
		s.cursorF = nil
	}
	return nil
}

// loadCursor reads the persisted {seq, offset}; a missing, short or torn cursor
// (found is false) starts at the oldest segment. The record carries a checksum
// because it is rewritten in place: a partial write on power loss must fall back
// to redelivering (duplicates, within at-least-once) rather than seek to a mixed
// old/new position that silently skips undelivered frames. It must not reference
// s.segs — load() calls it before the segment set is established.
func (s *Spool) loadCursor() (seq, off int64, found bool, err error) {
	f, err := os.OpenFile(filepath.Join(s.dir, cursorName), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return 0, 0, false, err
	}
	s.cursorF = f
	var buf [cursorLen]byte
	n, _ := f.ReadAt(buf[:], 0)
	if n >= cursorLen && binary.BigEndian.Uint64(buf[16:]) == cursorSum(buf[:16]) {
		return int64(binary.BigEndian.Uint64(buf[:8])), int64(binary.BigEndian.Uint64(buf[8:16])), true, nil
	}
	return 0, 0, false, nil
}

const cursorLen = 24 // seq(8) + offset(8) + fnv64a checksum(8)

func cursorSum(b []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(b)
	return h.Sum64()
}

// persistCursor rewrites the checksummed cursor in place (caller holds the
// lock). A torn rewrite fails its checksum on the next load and redelivers.
func (s *Spool) persistCursor() {
	if s.cursorF == nil {
		return
	}
	var buf [cursorLen]byte
	binary.BigEndian.PutUint64(buf[:8], uint64(s.segs[0].seq))
	binary.BigEndian.PutUint64(buf[8:16], uint64(s.readOff))
	binary.BigEndian.PutUint64(buf[16:], cursorSum(buf[:16]))
	if _, err := s.cursorF.WriteAt(buf[:], 0); err == nil {
		_ = s.cursorF.Sync()
	}
}

// syncDir fsyncs the spool directory so a newly created/removed segment survives
// a crash (best-effort).
func (s *Spool) syncDir() {
	if d, err := os.Open(s.dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}
