package main

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTokenFileRefreshesAndSurvivesTransientErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("  first\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tf := newTokenFile(path, slog.New(slog.DiscardHandler))
	if got, err := tf.read(); err != nil || got != "first" {
		t.Fatalf("read = %q, %v; want first (whitespace trimmed)", got, err)
	}
	if got := tf.get(); got != "first" {
		t.Fatalf("get = %q, want the cached value", got)
	}

	// A rotated Secret must be picked up without a restart.
	if err := os.WriteFile(path, []byte("second"), 0o600); err != nil {
		t.Fatal(err)
	}
	tf.mu.Lock()
	tf.fetched = time.Now().Add(-2 * tokenReadInterval)
	tf.mu.Unlock()
	if got := tf.get(); got != "second" {
		t.Fatalf("get after rotation = %q, want second", got)
	}

	// A read failure mid-swap keeps the last good token rather than sending
	// an empty Authorization header on every subsequent request.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	tf.mu.Lock()
	tf.fetched = time.Now().Add(-2 * tokenReadInterval)
	tf.mu.Unlock()
	if got := tf.get(); got != "second" {
		t.Fatalf("get after a failed re-read = %q, want the last good token", got)
	}

	// An empty file is an error, not a silently empty token.
	if err := os.WriteFile(path, []byte("\n\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := tf.read(); err == nil {
		t.Fatal("an empty token file must be an error")
	}
}
