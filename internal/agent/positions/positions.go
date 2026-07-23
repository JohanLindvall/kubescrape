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
	// A corrupt file must not wedge startup: whatever fields decoded before
	// the error stay (harmless — re-read is the worst case) and the next
	// save overwrites the whole doc atomically. It must not be SILENT
	// either — recurring corruption across restarts is a failing disk, and
	// without the count it looks like ordinary restarts with odd re-reads.
	if err := json.Unmarshal(data, &s.doc); err != nil {
		obs.PositionsCorrupt.Inc()
		slog.Warn("positions file corrupt; keeping decodable prefix, affected inputs re-read",
			"path", path, "error", err)
	}
	return s, nil
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
	tmp := s.path + ".tmp"
	if err := writeFileSync(tmp, data); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return err
	}
	if d, err := os.Open(filepath.Dir(s.path)); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// writeFileSync is os.WriteFile plus an fsync before close, so a rename that
// follows cannot surface a zero-length file after a power loss.
func writeFileSync(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
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
