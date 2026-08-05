// Package backoff is the restart/retry backoff the agent's long-running
// consumers (journald, the events watch, the azurediag Kafka loop) share:
// sleep the current delay, double it to a 30s cap, and reset it when a run
// stays healthy. It was spelled three times, inline, and the copies could
// only drift — this is the same class of consolidation as logchain's
// resolver, for the loop AROUND the producers rather than the records inside
// them.
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

// New returns a backoff starting at initial.
func New(initial time.Duration) *B { return &B{initial: initial, next: initial} }

// Delay is what the NEXT Sleep will wait — read it for the log line that
// announces the wait, BEFORE calling Sleep (which doubles it).
func (b *B) Delay() time.Duration { return b.next }

// Sleep waits out the current delay (returning early when ctx is done), then
// doubles it up to Cap. It doubles even on a cancelled ctx — the caller's
// loop exits on ctx anyway, and checking would complicate the contract for
// nothing.
func (b *B) Sleep(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-time.After(b.next):
	}
	if b.next *= 2; b.next > Cap {
		b.next = Cap
	}
}

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
