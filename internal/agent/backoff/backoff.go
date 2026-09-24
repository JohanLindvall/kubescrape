// Package backoff is the restart/retry backoff the agent's long-running
// consumers (journald, the events watch, the azurediag Kafka loop) share:
// sleep the current delay, double it to a 30s cap, and reset it when a run
// stays healthy. It was spelled three times, inline, and the copies could
// only drift — this is the same class of consolidation as logchain's
// resolver, for the loop AROUND the producers rather than the records inside
// them. The bounded in-call retry (otlpexport.Retry) waits through a B too,
// and the disk-buffer drain doubles through Double, so every doubling delay in
// the agent stops at the same Cap.
//
// Deliberately NOT absorbed: selfmeta.Poll's backoff, whose contract differs
// (it backs off from 5s toward a REFRESH interval, keeps last-good state on
// failure, and never uses the healthy-run rule).
package backoff

import (
	"context"
	"time"
)

// Cap bounds the delay. It is also the healthy-run threshold ResetIfHealthy
// applies: the two being one value is deliberate — a run must outlive the
// worst-case wait before it counts as evidence the trouble has passed.
const Cap = 30 * time.Second

// B is a doubling backoff. The zero value is unusable; New sets the initial
// delay (each producer defaults its own config field before calling).
type B struct {
	initial time.Duration
	next    time.Duration
}

// New returns a backoff starting at initial, which is clamped to Cap: the cap
// bounds EVERY wait, the first included. An unclamped initial above it waited
// that long once and then "doubled" DOWN to Cap (an -otlp-retry-backoff of 1m
// waited 1m, 30s, 30s), which is neither a cap nor a doubling.
func New(initial time.Duration) *B {
	initial = min(initial, Cap)
	return &B{initial: initial, next: initial}
}

// Delay is what the NEXT Wait (or Sleep) will wait — read it for the log line
// that announces the wait, BEFORE calling Wait (which doubles it).
func (b *B) Delay() time.Duration { return b.next }

// Wait waits out the current delay, then doubles it up to Cap (Double), and
// reports whether the wait ran to completion: false means ctx ended first. It
// doubles even on a cancelled ctx — the caller's loop exits on ctx anyway, and
// checking would complicate the contract for nothing.
//
// NewTimer+Stop rather than time.After is a style choice, not a leak fix: since
// Go 1.23 a timer nothing references any more is garbage-collected whether or
// not it fired or was stopped, so an abandoned time.After costs nothing past the
// select. Stop merely releases it at once.
func (b *B) Wait(ctx context.Context) bool {
	t := time.NewTimer(b.next)
	defer t.Stop()
	b.next = Double(b.next)
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// Sleep is Wait for a caller whose loop checks ctx itself.
func (b *B) Sleep(ctx context.Context) { b.Wait(ctx) }

// Double is the one doubling rule: twice d, never past Cap. A holder whose
// delay is not a B (the disk-buffer drain, whose delay persists across queue
// cycles and resets to zero) doubles through this so the ceiling is one value.
func Double(d time.Duration) time.Duration { return min(2*d, Cap) }

// Reset returns the delay to its initial value (a delivered export, a
// committed cursor — whatever the producer treats as proof of health).
func (b *B) Reset() { b.next = b.initial }

// ResetIfHealthy resets when the run that began at started lasted at least
// Cap. Without this rule a few hiccups spread over the agent's lifetime
// would pin every future restart at the 30s worst case; with a shorter
// threshold, a consumer that dies quickly but not instantly would keep
// resetting and hammer its upstream at the initial delay forever.
func (b *B) ResetIfHealthy(started time.Time) {
	if time.Since(started) >= Cap {
		b.Reset()
	}
}
