package cli

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// ShutdownContext returns a process lifetime for a main: ctx is cancelled by
// the first SIGINT/SIGTERM or by stop, and releaseSignals unregisters the
// handler. A main defers releaseSignals FIRST, so it runs LAST — after the
// budgeted shutdown it exists to protect.
//
// Two functions because they are two decisions. stop used to BE
// signal.NotifyContext's stop, and that one also calls signal.Stop: every path
// that cancelled the process context before its shutdown was done — the
// agent's fatal pipeline failure, the metadata service's normal path, which
// cancels ctx to join its exporting goroutines before the final export — put
// SIGTERM back on Go's default action, so a SIGTERM landing during that
// shutdown (a liveness kill, a rollout) killed the process on the spot: final
// exports and drains aborted, exit status 143 instead of the process's own.
// Cancelling is now a plain context cancel; the signal stays handled (a second
// one is absorbed) until the main returns, and the kubelet's SIGKILL at the end
// of the grace period remains the hard bound.
func ShutdownContext() (ctx context.Context, stop, releaseSignals context.CancelFunc) {
	sigCtx, sigStop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	ctx, stop = context.WithCancel(sigCtx)
	return ctx, stop, sigStop
}

// WaitFor waits for wg with a deadline, reporting whether it finished in time.
// Both mains join their goroutine groups through it on the way out.
//
// TERMINAL PATHS ONLY. On the timeout arm it ABANDONS the goroutine blocked in
// wg.Wait — there is no way to cancel a WaitGroup — so a caller that reaches
// this while something is genuinely stuck leaks one goroutine until the process
// exits. Every caller is on a main's way out, where that is milliseconds;
// anything else wants a context-scoped wait instead of this.
//
// The TIMER is stopped on return. That is tidiness, not a leak fix: since Go
// 1.23 an unreferenced timer is garbage-collected, so time.After would be
// equally correct here — unlike the goroutine, which nothing can reclaim.
//
// A NON-POSITIVE budget is answered, never assumed (WaitForProbe): a shutdown
// that has spent its shared deadline asks with a budget of 0, and a zero timer
// is ready before the goroutine observing the WaitGroup has been scheduled, so
// an ALREADY-DRAINED group reported a timeout that had not happened — on
// exactly the shutdown whose log is read to find out what went wrong, where a
// "did not stop" warning then accused goroutines that had stopped.
func WaitFor(wg *sync.WaitGroup, budget time.Duration) bool {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	t := time.NewTimer(max(budget, WaitForProbe))
	defer t.Stop()
	select {
	case <-done:
		return true
	case <-t.C:
		return false
	}
}

// WaitForProbe is the floor under WaitFor's budget. It is a SCHEDULING grace,
// not a wait: it exists only so that a spent budget still gets an honest answer,
// and it cannot delay a shutdown that has budget left.
const WaitForProbe = 10 * time.Millisecond
