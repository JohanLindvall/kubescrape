package main

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
)

// tokenReadInterval bounds how often a token file is re-read. Kubernetes
// projects rotated Secret contents into the mounted file, so the value must be
// re-read rather than captured once — but not on every request.
const tokenReadInterval = time.Minute

// tokenFile serves a bearer token from a mounted file, re-reading it at most
// once per interval. A failed re-read keeps serving the last good value: a
// transient read error during a ConfigMap/Secret swap must not turn every
// subsequent request into an authentication failure.
type tokenFile struct {
	path string
	log  *slog.Logger

	mu      sync.Mutex
	token   string
	fetched time.Time
}

func newTokenFile(path string, log *slog.Logger) *tokenFile {
	return &tokenFile{path: path, log: log}
}

func (t *tokenFile) read() (string, error) {
	data, err := os.ReadFile(t.path)
	if err != nil {
		return "", err
	}
	tok := strings.TrimSpace(string(data))
	if tok == "" {
		return "", fmt.Errorf("token file %s is empty", t.path)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.token = tok
	t.fetched = time.Now()
	return tok, nil
}

// get returns the current token, refreshing it when the cached value is stale.
func (t *tokenFile) get() string {
	t.mu.Lock()
	tok, fresh := t.token, time.Since(t.fetched) < tokenReadInterval
	t.mu.Unlock()
	if fresh {
		return tok
	}
	next, err := t.read()
	if err != nil {
		// Keep the last good token; the caller's request may still succeed.
		t.log.Warn("re-reading token file", "path", t.path, "error", err)
		return tok
	}
	return next
}
