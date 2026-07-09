// Package spool is a disk-backed FIFO byte queue. Records are appended durably
// to segment files and consumed in order, surviving process restarts. It backs
// the agent's log-export buffer: a collector outage spools records to disk
// (bounded by a size cap) instead of pinning the tailer to old file offsets, so
// the source files can rotate away while their data waits on the node.
//
// Layout: a directory of segment files (`<seq>.seg`, zero-padded, ascending),
// each a sequence of length-prefixed frames (4-byte big-endian length + bytes).
// Appends fsync the frame before returning, so a producer that has observed a
// successful Append may safely advance its own checkpoint. A separate 16-byte
// `cursor` file records the read position ({segment seq, offset}); it is
// rewritten in place after each commit. On restart the newest segment's torn
// tail (a frame interrupted by a crash) is truncated away.
package spool

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// ErrFull is returned by Append when the queue is at its size cap.
var ErrFull = errors.New("spool: full")

const (
	defaultSegmentBytes = 8 << 20
	segSuffix           = ".seg"
	cursorName          = "cursor"
	frameHeader         = 4 // uint32 big-endian length prefix
)

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
}

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
	readOff int64 // offset within segs[0] of the next unread frame
	signal  chan struct{}
	closed  bool
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
// read cursor.
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
		s.segs = append(s.segs, segment{seq: seq, size: info.Size(), path: s.segPath(seq)})
	}
	sort.Slice(s.segs, func(i, j int) bool { return s.segs[i].seq < s.segs[j].seq })

	if len(s.segs) == 0 {
		if err := s.appendSegment(1); err != nil {
			return err
		}
	} else if err := s.openTail(); err != nil {
		return err
	}

	cursorSeq, cursorOff, err := s.loadCursor()
	if err != nil {
		return err
	}
	// Drop consumed segments (seq < cursorSeq) left behind by a crash between
	// deleting a segment and persisting the cursor.
	for len(s.segs) > 1 && s.segs[0].seq < cursorSeq {
		_ = os.Remove(s.segs[0].path)
		s.segs = s.segs[1:]
	}
	if s.segs[0].seq == cursorSeq && cursorOff <= s.segs[0].size {
		s.readOff = cursorOff
	}
	return nil
}

// openTail opens the newest segment for appending and truncates any torn tail
// (a frame whose write was interrupted by a crash).
func (s *Spool) openTail() error {
	tail := &s.segs[len(s.segs)-1]
	good, err := lastCompleteOffset(tail.path, tail.size)
	if err != nil {
		return err
	}
	if good < tail.size {
		if err := os.Truncate(tail.path, good); err != nil {
			return err
		}
		tail.size = good
	}
	f, err := os.OpenFile(tail.path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	s.w = f
	return nil
}

// appendSegment creates a new empty segment with the given seq and makes it the
// write tail.
func (s *Spool) appendSegment(seq int64) error {
	path := s.segPath(seq)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	if s.w != nil {
		_ = s.w.Close()
	}
	s.w = f
	s.segs = append(s.segs, segment{seq: seq, size: 0, path: path})
	s.syncDir()
	return nil
}

// lastCompleteOffset walks the length-prefixed frames of a segment and returns
// the offset just past the last whole frame (the truncation point for a torn
// tail).
func lastCompleteOffset(path string, size int64) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	var off int64
	hdr := make([]byte, frameHeader)
	for off+frameHeader <= size {
		if _, err := f.ReadAt(hdr, off); err != nil {
			break
		}
		end := off + frameHeader + int64(binary.BigEndian.Uint32(hdr))
		if end > size {
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
	frame := int64(frameHeader + len(data))
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("spool: closed")
	}
	if s.maxBytes > 0 && s.backlog()+frame > s.maxBytes {
		return ErrFull
	}
	tail := &s.segs[len(s.segs)-1]
	if tail.size > 0 && tail.size+frame > s.segmentSize {
		if err := s.appendSegment(tail.seq + 1); err != nil {
			return err
		}
		tail = &s.segs[len(s.segs)-1]
	}
	var hdr [frameHeader]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(data)))
	if _, err := s.w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := s.w.Write(data); err != nil {
		return err
	}
	if err := s.w.Sync(); err != nil {
		return err
	}
	tail.size += frame
	s.notify()
	return nil
}

// Pop returns the next record and a commit function that removes it, or ok
// false when the queue is empty. The record is not removed until commit is
// called, so a crash before commit re-delivers it (at-least-once). commit is
// idempotent.
func (s *Spool) Pop() (data []byte, commit func(), ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	head := &s.segs[0]
	if s.readOff+frameHeader > head.size {
		return nil, nil, false
	}
	f, err := os.Open(head.path)
	if err != nil {
		return nil, nil, false
	}
	defer func() { _ = f.Close() }()
	hdr := make([]byte, frameHeader)
	if _, err := f.ReadAt(hdr, s.readOff); err != nil {
		return nil, nil, false
	}
	n := int64(binary.BigEndian.Uint32(hdr))
	if s.readOff+frameHeader+n > head.size {
		return nil, nil, false // torn (only possible in the write tail)
	}
	payload := make([]byte, n)
	if _, err := f.ReadAt(payload, s.readOff+frameHeader); err != nil {
		return nil, nil, false
	}
	end := s.readOff + frameHeader + n

	var done bool
	commit = func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if done {
			return
		}
		done = true
		s.readOff = end
		// Retire fully-consumed segments (never the write tail).
		for len(s.segs) > 1 && s.readOff >= s.segs[0].size {
			_ = os.Remove(s.segs[0].path)
			s.segs = s.segs[1:]
			s.readOff = 0
		}
		s.persistCursor()
	}
	return payload, commit, true
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

// loadCursor reads the persisted {seq, offset}; a missing cursor starts at the
// oldest segment.
func (s *Spool) loadCursor() (seq, off int64, err error) {
	f, err := os.OpenFile(filepath.Join(s.dir, cursorName), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return 0, 0, err
	}
	s.cursorF = f
	var buf [16]byte
	if _, err := f.ReadAt(buf[:], 0); err != nil {
		return s.segs[0].seq, 0, nil // fresh/short cursor: start at the oldest segment
	}
	return int64(binary.BigEndian.Uint64(buf[:8])), int64(binary.BigEndian.Uint64(buf[8:])), nil
}

// persistCursor rewrites the 16-byte cursor in place (caller holds the lock).
func (s *Spool) persistCursor() {
	if s.cursorF == nil {
		return
	}
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[:8], uint64(s.segs[0].seq))
	binary.BigEndian.PutUint64(buf[8:], uint64(s.readOff))
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
