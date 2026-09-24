package main

// The shutdown sequence's budgets and joins: the HTTP drain, and the bounded
// waits on the goroutines run() started.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/cli"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
)

// shutdownTotal bounds the WHOLE shutdown sequence run() controls and
// shutdownStep any single step of it.
//
// The total is deliberately well under the 30s the kubelet gives a pod that
// names no terminationGracePeriodSeconds — which is what this Deployment is:
// the two deferred listener stoppers (obs.ServeMetrics/ServePprof, 5s each —
// obs.ListenerShutdownTimeout) run AFTER this sequence, so 15 + 5 + 5 = 25s
// leaves the kubelet's own overhead and the exporter close inside the grace. shutdownStep is
// metrics.FinalExportTimeout, so nothing changes on the path that has the whole
// budget to itself.
const (
	shutdownTotal = 15 * time.Second
	shutdownStep  = metrics.FinalExportTimeout
)

// shutdownHTTP releases the parked container lookups and then drains the HTTP
// server within budget. It returns an error only for a shutdown failure that is
// not the deadline; a missed deadline is reported as a WARN.
//
// The ORDER is the fix, and the mechanism is worth stating exactly, because the
// obvious reading of it is wrong. srv.Shutdown stops the listeners, closes the
// IDLE connections and then waits for the active handlers; at its deadline it
// merely RETURNS context.DeadlineExceeded — it does not touch an active
// connection (only srv.Close does), and a handler that finishes afterwards
// still writes its response to a client that is still attached. What actually
// cut the observed request was the PROCESS EXITING a few steps later: run
// returns, the sockets die with it, and the client sees "Empty reply from
// server" — no status, no body, nothing an agent can classify — while the
// process exited 0 with four INFO lines and no hint anything had been dropped.
// Draining FIRST turns every parked lookup into a 503 + Retry-After that
// finishes well inside the step, and the deadline — which used to be swallowed
// on purpose — now WARNS with what is still in flight and about to be cut by
// the exit, because that silence is what made this invisible in the first place.
func shutdownHTTP(ctx context.Context, srv *http.Server, drain func() int, inFlight func() int64, budget time.Duration, log *slog.Logger) error {
	if n := drain(); n > 0 {
		// Worth a line of its own: these clients got a refusal rather than the
		// metadata they asked for, and it is the one loss a graceful shutdown
		// causes. The count spans BOTH parking spots (the store's per-ID waiters
		// and the requests waiting on the initial sync);
		// kubescrape_container_lookups_drained_total carries the store's share
		// into the final export.
		log.Info("released blocked container lookups so they can be answered", "lookups", n)
	}
	sctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	err := srv.Shutdown(sctx)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.DeadlineExceeded):
		// Shutdown gave up WAITING; it did not close anything. These handlers
		// keep running and their clients stay attached until the process exits
		// moments later, which is what cuts them without a response.
		log.Warn("http shutdown budget exceeded; requests still in flight will be cut without a response when the process exits",
			"budget", budget, "requestsInFlight", inFlight())
		return nil
	default:
		return fmt.Errorf("http shutdown: %w", err)
	}
}

// joinOnEarlyReturn is run()'s deferred BACKSTOP join: on an early `return err`
// it is the only thing that stops and drains the started goroutines before the
// deferred exporter.Close runs under them, so it must run there.
//
// On the normal path it must do NOTHING — not even ask for a budget. The inline
// join has already run, and asking stepBudget for one is not free: once the
// shared deadline is spent (a hanging collector makes the inline steps use it
// exactly) that call logs the one-time "shutdown deadline exceeded ... final
// exports are lost" WARN — AFTER "shutdown complete ... deadlineExceeded=false"
// has been logged, contradicting it, from a join with nothing left to join.
func joinOnEarlyReturn(joined bool, stop func(), wg *sync.WaitGroup, budget func() time.Duration) {
	if joined {
		return
	}
	stop()
	_ = cli.WaitFor(wg, budget())
}
