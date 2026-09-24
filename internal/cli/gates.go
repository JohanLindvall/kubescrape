package cli

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
)

// GateWatch describes one process's wait for its readiness gates: what is
// pending, how often to look, and what to say while it waits. The READY action
// is not here — each binary does its own (the service closes the channel
// /readyz latches on, the agent logs which gates it waited for).
type GateWatch struct {
	// Pending returns the gates not yet satisfied, in a stable order. It is
	// called once per Poll, so it must be cheap.
	Pending func() []string
	// Poll is the steady check cadence — never a backoff: readiness gates a
	// rollout or a Service's endpoints, so it must be announced when it
	// happens rather than up to a backoff later. Only the warning is throttled.
	Poll time.Duration
	// Grace is how long the gates may stay pending before the first warning,
	// ReWarn how often it is repeated after that.
	Grace, ReWarn time.Duration
	// NotReady is the warning's message and Note, when set, its remediation
	// hint; ShuttingDown is the message when the process ends with gates still
	// pending (a pod killed before it was ever ready is a different story from
	// one that served and was rolled).
	NotReady, Note, ShuttingDown string
}

// WatchGates polls w.Pending until it is empty — returning true and how long
// that took — or until ctx ends, returning false. While gates stay pending
// past w.Grace it warns, naming them under `gates` (the value the
// kubescrape_readiness_gate metric's `gate` label carries) with `waited`, at
// most once per w.ReWarn.
//
// Both binaries' readiness loops were this body twice, each throttling with a
// hand-rolled timestamp and naming the pending list under its own key.
func WatchGates(ctx context.Context, log *slog.Logger, w GateWatch) (ready bool, waited time.Duration) {
	start := time.Now()
	var warn logdedupe.Throttle
	t := time.NewTimer(w.Poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			if pending := w.Pending(); len(pending) > 0 {
				log.Warn(w.ShuttingDown,
					"gates", strings.Join(pending, ","), "elapsed", time.Since(start).Round(time.Second))
			}
			return false, time.Since(start)
		case <-t.C:
		}
		pending := w.Pending()
		if len(pending) == 0 {
			return true, time.Since(start)
		}
		// The grace is checked first so it claims no throttle slot: the
		// throttle's zero value fires at once, which makes the first warning
		// land at the grace and not a ReWarn after it.
		if elapsed := time.Since(start); elapsed >= w.Grace && warn.Allow(w.ReWarn) {
			args := []any{"gates", strings.Join(pending, ","), "elapsed", elapsed.Round(time.Second)}
			if w.Note != "" {
				args = append(args, "note", w.Note)
			}
			log.Warn(w.NotReady, args...)
		}
		t.Reset(w.Poll)
	}
}
