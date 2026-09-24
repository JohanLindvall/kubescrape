package main

// The agent's /readyz: the startup gates each pipeline registers and satisfies,
// and the watchdog that names a gate that will not clear.

import (
	"context"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/cli"
)

// gateMetadata is satisfied by the first successful node-metadata fetch.
const gateMetadata = "metadata-service"

// readiness tracks the startup gates /readyz reports on.
//
// A DaemonSet rolling update advances only when the new pod reports ready, so
// this endpoint decides whether a bad rollout stops at the first node or
// marches across the fleet. It previously returned the same static "ok" as
// /healthz — the agent was "ready" the instant the mux was built, even if it
// could not reach the metadata service and could therefore attribute nothing.
//
// Gates are registered at startup and satisfied as each becomes true; /readyz
// is 200 only when none are pending, and reports the pending ones so the
// failure is diagnosable from the probe alone.
type readiness struct {
	mu    sync.Mutex
	gates map[string]bool
}

func newReadiness() *readiness { return &readiness{gates: map[string]bool{}} }

// require registers a gate that must be satisfied before the agent is ready.
func (r *readiness) require(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.gates[name]; !ok {
		r.gates[name] = false
	}
}

// done marks a gate satisfied. Safe to call repeatedly and from any goroutine.
func (r *readiness) done(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gates[name] = true
}

// gate registers a gate and returns the func that satisfies it, so a
// require/done pair cannot name two different gates. The returned func has
// done's semantics: idempotent, safe from any goroutine.
func (r *readiness) gate(name string) func() {
	r.require(name)
	return func() { r.done(name) }
}

// pending returns the unsatisfied gates, sorted for a stable probe body.
func (r *readiness) pending() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for name, ok := range r.gates {
		if !ok {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}

// states is every registered gate and whether it is satisfied, for
// obs.RegisterReadiness — the metric half of the probe body, so a fleet stuck
// unready is visible without a shell on one of its pods.
func (r *readiness) states() map[string]bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return maps.Clone(r.gates)
}

// How long a gate may stay pending before it is worth a line, and how often to
// repeat it. A rolling update that stops at the first node is the worst first-run
// experience there is, and the process itself is otherwise silent about it: the
// pipelines all logged "started", the kubelet's probe is failing, and nothing in
// the log says which subsystem is holding it.
//
// The grace is generous on purpose — reaching the metadata service takes a few
// seconds on a cold cluster, and a warning during normal startup teaches
// operators to ignore this one.
// Vars, not consts, so a test can drive the warn without sleeping through the
// grace — the same reason the store's clock is injectable.
var (
	readinessGrace  = 30 * time.Second
	readinessReWarn = 2 * time.Minute
)

// watch reports readiness once, and keeps reporting a gate that will not clear.
// It returns as soon as everything is satisfied (the gates are STARTUP gates:
// they never go back), or when the process is shutting down.
func (r *readiness) watch(ctx context.Context, log *slog.Logger) {
	ready, waited := cli.WatchGates(ctx, log, cli.GateWatch{
		Pending: r.pending,
		// The check is one mutex-guarded map read.
		Poll:         min(time.Second, readinessGrace),
		Grace:        readinessGrace,
		ReWarn:       readinessReWarn,
		NotReady:     "not ready: /readyz is 503, so a rolling update will not advance past this pod",
		ShuttingDown: "shutting down before becoming ready",
	})
	if ready {
		log.Info("ready", "elapsed", waited.Round(time.Second), "gates", strings.Join(r.names(), ","))
	}
}

// names is every registered gate, sorted — what the ready line reports, so the
// one Info line says what was actually waited for.
func (r *readiness) names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Sorted(maps.Keys(r.gates))
}
