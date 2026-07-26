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
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

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
	// corrupt records that the file existed but decoded to nothing, so an
	// empty store must not be mistaken for a first run (see Corrupt).
	corrupt bool
}

// Open loads the store at path, tolerating a missing or corrupt file (it
// then starts empty; a subsequent Save rewrites it). Any other read error is
// returned: starting empty on a transient EACCES/EIO would skip every
// existing log to its end and then overwrite the good file on the next Save,
// silently losing the entire unshipped window the file exists to protect.
func Open(path string) (*Store, error) {
	s := &Store{path: path}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return s, nil
		}
		return nil, err
	}
	// A corrupt file must not wedge startup, but it must not look like a FIRST
	// RUN either. json.Unmarshal decodes NOTHING on a syntax error (the old
	// comment here claimed a decodable prefix survives — it does not), so the
	// store comes up empty, which -logs-unknown-files=auto reads as "no store,
	// treat existing files as history" and skips every file to its end: the
	// whole unshipped window is lost, silently and indistinguishably from a
	// clean first start. Record that a store EXISTED so `auto` can choose
	// re-reading (duplicates, which at-least-once already tolerates) over
	// losing data.
	if err := json.Unmarshal(data, &s.doc); err != nil {
		obs.PositionsCorrupt.Inc()
		s.corrupt = true
		slog.Warn("positions file corrupt; it decoded to nothing, so tracked inputs are re-read from the start rather than skipped as history",
			"path", path, "error", err)
	}
	return s, nil
}

// Corrupt reports that the positions file existed but failed to decode, so the
// store is empty for a reason that is NOT "first run". Callers deciding where an
// unknown file starts must prefer re-reading over skipping to the end.
func (s *Store) Corrupt() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.corrupt
}

// Logs returns a copy of the stored log positions.
func (s *Store) Logs() map[string]LogPos {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]LogPos, len(s.doc.Logs))
	for k, v := range s.doc.Logs {
		out[k] = v
	}
	return out
}

// SetLogs replaces the log section and persists.
func (s *Store) SetLogs(m map[string]LogPos) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.doc.Logs = m
	return s.save()
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
// rename + best-effort directory sync). The caller holds the mutex. Without
// the fsync a power loss shortly after the rename can leave a zero-length
// file — and an empty positions file means the tailer skips every existing
// log to its end and journald seeks to the tail, silently losing the entire
// unshipped window, precisely when durability matters most.
func (s *Store) save() error {
	data, err := json.Marshal(s.doc)
	if err != nil {
		return err
	}
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
