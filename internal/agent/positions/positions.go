// Package positions is the agent's unified, on-disk position store. A single
// file holds both the log tailer's per-file read offsets and the journald
// reader's cursor, so a restart resumes every input from one mounted file.
//
// The two producers run on different goroutines and update independent
// sections; the Store keeps both sections in memory and rewrites the whole
// document atomically under a mutex, so neither clobbers the other's data.
package positions

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/JohanLindvall/haste/xxh3"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// LogPos is one log file's committed position and identity fingerprint.
type LogPos struct {
	Offset          int64  `json:"offset"`
	Inode           uint64 `json:"inode"`
	FingerprintLen  int64  `json:"fpLen,omitempty"`
	FingerprintHash uint64 `json:"fpHash,omitempty"`
	// Pending names the rotated-away files whose tails are still part of a
	// multi-line group buffered across one or more rotations, oldest first. On
	// restart they are re-read in order before the current file so the group
	// reconstructs without loss even across several rotations.
	Pending []Prefix `json:"pending,omitempty"`
}

// Prefix identifies the unexported tail of a rotated-away log file.
type Prefix struct {
	Inode           uint64 `json:"inode"`
	FingerprintLen  int64  `json:"fpLen,omitempty"`
	FingerprintHash uint64 `json:"fpHash,omitempty"`
	From            int64  `json:"from"`
	To              int64  `json:"to"`
}

// doc is the on-disk shape.
type doc struct {
	Logs          map[string]LogPos `json:"logs,omitempty"`
	JournalCursor string            `json:"journalCursor,omitempty"`
}

// Store persists positions to a single file.
type Store struct {
	mu   sync.Mutex
	path string
	doc  doc
	// corrupt records that the file existed but failed to decode — a syntax
	// error keeps nothing, a type error keeps the well-formed entries — so a
	// missing entry must not be mistaken for a first run (see Corrupt).
	corrupt bool
	// written identifies the document THIS process last renamed into place, so
	// save can skip a write whose bytes are already durable. It is a 128-bit
	// xxh3 of the marshalled document rather than the bytes themselves: this is
	// identity hashing (a collision would skip a write that was needed), which
	// is what the repo's 128-bit half is for, and retaining the bytes would pin
	// a second copy of a document that reaches ~1.8 MB at 3000 tracked files.
	// writtenValid is false until a write of ours has SUCCEEDED, so the first
	// save of a process always writes: the file on disk may have been left by
	// an older build, or hand-edited, and its bytes are not ours to assume.
	written      [16]byte
	writtenValid bool
}

// Open loads the store at path, tolerating a missing or corrupt file (it
// then starts empty; a subsequent Save rewrites it). Any other read error is
// returned: starting empty on a transient EACCES/EIO would skip every
// existing log to its end and then overwrite the good file on the next Save,
// silently losing the entire unshipped window the file exists to protect.
func Open(path string) (*Store, error) {
	s := &Store{path: path}
	// Sweep temp files left by saves that died between CreateTemp and Rename
	// (the exact "<base>.tmp-*" shape save creates; nothing ever renames or
	// reuses one, so they would accumulate forever). Removing a temp that
	// belongs to a still-live concurrent writer — a terminating pod's agent
	// overlapping its replacement — is safe: that writer's rename simply
	// fails, a save the design already treats as losable, and its next save
	// starts from a fresh temp.
	//
	// List the directory and match the basename prefix in code rather than
	// embedding the path in a filepath.Glob pattern: a positions path holding
	// a glob metacharacter ([ * ?) would otherwise make the pattern match — and
	// remove — files that are not this store's temps.
	if entries, err := os.ReadDir(filepath.Dir(path)); err != nil {
		// Not fatal — a missing or unreadable parent is diagnosed properly
		// below, on the path where it decides whether saves can work at all —
		// but worth saying, because it is also why no sweep happened.
		slog.Debug("could not list the positions directory to sweep stale temporary files",
			"dir", filepath.Dir(path), "error", err)
	} else {
		prefix := filepath.Base(path) + ".tmp-"
		swept, failed := 0, 0
		for _, e := range entries {
			if !strings.HasPrefix(e.Name(), prefix) {
				continue
			}
			if err := os.Remove(filepath.Join(filepath.Dir(path), e.Name())); err != nil {
				failed++
				continue
			}
			swept++
		}
		if failed > 0 {
			// Each leftover is a save that died between CreateTemp and Rename,
			// and one that cannot be removed says the directory is not writable
			// the way this store needs it — which is the same condition that
			// will make every save fail (counted
			// kubescrape_positions_save_errors_total, but only once a save is
			// attempted, which is one flush interval too late to be the first
			// thing an operator sees).
			slog.Warn("could not remove stale positions temporary files; the positions directory may not be writable",
				"dir", filepath.Dir(path), "entries", failed)
		} else if swept > 0 {
			slog.Info("swept stale positions temporary files left by a previous process", "dir", filepath.Dir(path), "entries", swept)
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
		// An absent FILE is a first run; an absent (or non-directory) PARENT
		// means every save must fail — CreateTemp needs the directory. Callers
		// only Warn on failed saves, so a mistyped path or an unmounted volume
		// would run with no persistence at all: refuse it here, at startup,
		// where the error is fatal and diagnosable.
		dir := filepath.Dir(path)
		fi, statErr := os.Stat(dir)
		if statErr != nil {
			return nil, fmt.Errorf("positions file %s: parent directory %s does not exist (mistyped path, or its volume is not mounted?): %w", path, dir, statErr)
		}
		if !fi.IsDir() {
			return nil, fmt.Errorf("positions file %s: parent %s is not a directory", path, dir)
		}
		// FIRST RUN, and it is worth one line, because of what follows from it:
		// with no stored positions -logs-unknown-files=auto resolves to "end",
		// so every log file that already exists on the node is skipped to its
		// end as history and its current contents are never exported. That is
		// the intended behaviour and it is also the single most common "the
		// agent is not collecting anything" report on a first live run — an
		// operator who sees this line knows to look at the flag rather than at
		// the pipeline.
		slog.Info("no positions file yet; this is a first run for this volume, so existing log files are treated per -logs-unknown-files and the journal starts at its tail",
			"path", path)
		return s, nil
	}
	// A corrupt file must not wedge startup, but it must not look like a FIRST
	// RUN either. What a failed decode leaves depends on the error class: a
	// SYNTAX error decodes nothing (the store comes up empty), while a TYPE
	// error keeps decoding past the offending value, so the well-formed
	// entries ARE kept and only the mistyped ones are missing. Either way an
	// input with no entry must not read as "no store, treat existing files as
	// history" — that skips it to its end and its whole unshipped window is
	// lost, silently and indistinguishably from a clean first start. Record
	// that a store EXISTED so `auto` can choose re-reading (duplicates, which
	// at-least-once already tolerates) over losing data.
	if err := json.Unmarshal(data, &s.doc); err != nil {
		obs.PositionsCorrupt.Inc()
		s.corrupt = true
		slog.Warn("positions file corrupt; whatever decoded is kept, and inputs without an entry are re-read from the start rather than skipped as history",
			"path", path, "error", err)
	}
	// Lifecycle, once: WHERE the agent resumes from. Without it a restart that
	// silently resumed at zero (a wiped volume, a mistyped -positions-file
	// pointing somewhere new) looks exactly like a healthy one until the
	// duplicate records arrive downstream. Counts only — a cursor is opaque and
	// a path list is unbounded, and neither belongs on a startup line.
	slog.Info("positions loaded", "path", path, "entries", len(s.doc.Logs),
		"bytes", len(data), "journalCursor", s.doc.JournalCursor != "", "corrupt", s.corrupt)
	return s, nil
}

// Corrupt reports that the positions file existed but failed to decode (in
// full or in part), so a missing entry is missing for a reason that is NOT
// "first run". Callers deciding where an unknown file starts must prefer
// re-reading over skipping to the end.
func (s *Store) Corrupt() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.corrupt
}

// Logs returns a copy of the stored log positions, deep through Pending: the
// caller uses the result on its own goroutine, and a shared slice header
// would put its appends outside this store's mutex.
func (s *Store) Logs() map[string]LogPos {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]LogPos, len(s.doc.Logs))
	for k, v := range s.doc.Logs {
		v.Pending = clonePrefixes(v.Pending)
		out[k] = v
	}
	return out
}

// SetLogs replaces the log section and persists. The map and its Pending
// slices are copied, never retained: the caller keeps mutating both after
// the call, on a goroutine this store's mutex does not cover.
func (s *Store) SetLogs(m map[string]LogPos) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	logs := make(map[string]LogPos, len(m))
	for k, v := range m {
		v.Pending = clonePrefixes(v.Pending)
		logs[k] = v
	}
	s.doc.Logs = logs
	return s.save()
}

// SetLogsOwned replaces the log section with a map the store TAKES OVER, and
// persists. The caller must never read or write m (nor any Pending slice it
// holds) after the call — the store keeps it, and Logs hands copies out from
// it on other goroutines.
//
// It exists for the tailer's saveCheckpoints, which builds a fresh map on every
// save and drops it on return: SetLogs' defensive copy then duplicated ~3000
// entries and every Pending slice for nothing, on the single sweep goroutine
// that serves every log file on the node. Callers that keep their map must use
// SetLogs.
func (s *Store) SetLogsOwned(m map[string]LogPos) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.doc.Logs = m
	return s.save()
}

// clonePrefixes copies a Pending slice (Prefix holds no reference types, so a
// slice copy is a deep one); len 0 maps to nil, keeping omitempty encoding.
func clonePrefixes(p []Prefix) []Prefix {
	if len(p) == 0 {
		return nil
	}
	return append([]Prefix(nil), p...)
}

// JournalCursor returns the stored journald cursor ("" if none).
func (s *Store) JournalCursor() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.doc.JournalCursor
}

// SetJournalCursor replaces the journald cursor and persists.
func (s *Store) SetJournalCursor(cursor string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.doc.JournalCursor = cursor
	return s.save()
}

// save writes the whole document atomically and durably (write + fsync +
// rename + best-effort directory sync), unless the document is byte-identical
// to the one this process last wrote (see below). The caller holds the mutex.
// Without
// the fsync a power loss shortly after the rename can leave a zero-length
// file — and an empty positions file means the tailer skips every existing
// log to its end and journald seeks to the tail, silently losing the entire
// unshipped window, precisely when durability matters most.
//
// A failure is counted here (kubescrape_positions_save_errors_total), not at
// the call sites: callers only Warn and carry on, so offsets silently not
// persisting — a bad path, a read-only mount, a full disk — would otherwise
// leave every metric flat while a restart re-reads or skips a stale window.
//
// A save whose document is byte-identical to the one this process last wrote
// is SKIPPED. That is not an optimisation with a semantic cost: writing bytes
// that are already durable at that path changes nothing on disk, and it is
// what the two fsyncs are spent on. Measured on ext4/nvme, one save is ~11 ms
// at 100 tracked files and ~15 ms at 3000, of which ~11 ms is the two fsyncs
// and the rest is marshalling — so the skip turns the tailer's 10-second
// cadence on a quiet node, and every repeat save inside one sweep, from a
// whole-node stall into a marshal.
//
// The one behaviour it gives up is re-asserting our document over a
// concurrent writer's: a terminating pod's agent and its replacement overlap
// for a few seconds, and where the old code would rename our (identical)
// document back over the old process's final save, we now leave that one in
// place while ours is unchanged. Both are complete documents and "last rename
// wins" already permitted exactly that ordering (see write); as soon as this
// process makes any progress its next save writes.
func (s *Store) save() error {
	data, err := json.Marshal(&s.doc)
	if err != nil {
		obs.PositionsSaveErrors.Inc()
		return err
	}
	sum := xxh3.Sum128(data).Bytes()
	if s.writtenValid && sum == s.written {
		return nil
	}
	if err := s.write(data); err != nil {
		// The identity is cleared, not left stale: a failed write may have
		// renamed nothing, or (a full disk mid-fsync) left the path holding
		// something else entirely, so the next save must write unconditionally.
		s.writtenValid = false
		obs.PositionsSaveErrors.Inc()
		return err
	}
	s.written, s.writtenValid = sum, true
	return nil
}

// write is save's I/O half: unique temp file, fsync, rename, best-effort
// directory sync.
func (s *Store) write(data []byte) error {
	// Unique temp name (not a fixed ".tmp"): a terminating pod's agent and
	// its replacement can briefly run concurrently, and with a shared name
	// one writer could rename the other's half-written file into place.
	// Each writer renames only its own fully-synced file; last rename wins
	// with a complete document either way.
	tmp, err := os.CreateTemp(filepath.Dir(s.path), filepath.Base(s.path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if err := writeAndSync(tmp, data); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, s.path); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if d, err := os.Open(filepath.Dir(s.path)); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// writeAndSync writes data and fsyncs before closing, so the rename that
// follows cannot surface a zero-length file after a power loss.
func writeAndSync(f *os.File, data []byte) error {
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
