package metaclient

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// lockProbe records whether c.mu was held while a log record was handled.
//
// TryRLock is the probe: a plain slog handler cannot observe the caller's
// locks, but an RWMutex can be asked. In a single-goroutine test the only
// thing that can be holding the exclusive lock when a record arrives is the
// emitting call itself.
type lockProbe struct {
	inner slog.Handler
	c     *Client
	held  atomic.Bool
}

func (p *lockProbe) Enabled(ctx context.Context, l slog.Level) bool { return p.inner.Enabled(ctx, l) }

func (p *lockProbe) Handle(ctx context.Context, r slog.Record) error {
	if p.c.mu.TryRLock() {
		p.c.mu.RUnlock()
	} else {
		p.held.Store(true)
	}
	return p.inner.Handle(ctx, r)
}

func (p *lockProbe) WithAttrs(a []slog.Attr) slog.Handler { return p.inner.WithAttrs(a) }
func (p *lockProbe) WithGroup(n string) slog.Handler      { return p.inner.WithGroup(n) }

// The cache-at-cap warning must be emitted with c.mu RELEASED.
//
// c.mu is the exclusive lock every concurrent metadata lookup on the node
// serialises through, and an slog write is a handler call plus a write to
// stderr that blocks when nothing drains it. Emitted inside the critical
// section — which is where it was written — a stalled log collector parks
// every ingest and cadvisor lookup on the node behind one log line, and it
// does so exactly when the cache is thrashing.
func TestCacheEvictionWarnIsNotEmittedUnderTheLock(t *testing.T) {
	s := newSrv(t)
	s.maxAge = "60"

	buf := &syncBuf{}
	probe := &lockProbe{inner: slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo})}
	c := New(Config{Base: s.URL, Timeout: 5 * time.Second, Log: slog.New(probe)})
	probe.c = c

	now := time.Now()
	c.now = func() time.Time { return now }

	// Fill past the hard cap with entries too fresh for the idle sweep, so the
	// insert below takes the ARBITRARY trim — the branch that warns.
	c.mu.Lock()
	for i := 0; i <= maxCacheEntries; i++ {
		c.cache[fmt.Sprintf("u%d", i)] = cacheEntry{used: now}
	}
	c.mu.Unlock()

	if _, err := c.PodByName(context.Background(), "ns", "p"); err != nil {
		t.Fatal(err)
	}

	if out := buf.String(); !strings.Contains(out, "hard cap") {
		t.Fatalf("the at-cap warning did not fire, so nothing was probed:\n%s", out)
	}
	if probe.held.Load() {
		t.Error("the cache-at-cap warning was emitted while c.mu was held: every concurrent lookup on the node blocks behind that log write")
	}
}
