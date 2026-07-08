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
	"os"
	"sync"
)

// LogPos is one log file's committed position and identity fingerprint.
type LogPos struct {
	Offset          int64  `json:"offset"`
	Inode           uint64 `json:"inode"`
	FingerprintLen  int64  `json:"fpLen,omitempty"`
	FingerprintHash uint64 `json:"fpHash,omitempty"`
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
// then starts empty). A subsequent Save rewrites it.
func Open(path string) *Store {
	s := &Store{path: path}
	if data, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(data, &s.doc) // corrupt file → start empty, overwritten on next Save
	}
	return s
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

// save writes the whole document atomically (write + rename). The caller
// holds the mutex.
func (s *Store) save() error {
	data, err := json.Marshal(s.doc)
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
