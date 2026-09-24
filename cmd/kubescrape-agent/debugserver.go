package main

// The -listen server: health, readiness and the /debug surfaces (debugMux),
// gated where they carry data (debugauth.go).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/promscrape"
	"github.com/JohanLindvall/kubescrape/internal/agent/tailer"
)

// debugMux is the routing table itself, split out from the server so a test
// can drive the REAL one. The gate is only as good as its registration: a test
// that wrapped a handler itself would keep passing after the registration lost
// the wrapper, which is precisely the regression this split makes impossible.
func (p *pipelines) debugMux(guard *debugGuard, tl *tailer.Tailer, sc *promscrape.Scraper) *http.ServeMux {
	mux := http.NewServeMux()
	// The homepage's link list is appended beside each registration below, so
	// it can only ever advertise what this process actually serves.
	links := []debugLink{
		{"/healthz", "/healthz", "liveness (static ok)"},
		{"/readyz", "/readyz", "readiness; pending gates in the body"},
	}
	ok := func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}
	mux.HandleFunc("GET /healthz", ok)
	// Readiness is NOT liveness: a rolling update advances on this, so it
	// reports whether the agent can actually do its job. The pending gates are
	// in the body, so a stuck rollout is diagnosable from the probe alone.
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if pending := p.ready.pending(); len(pending) > 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprintf(w, "not ready: %s\n", strings.Join(pending, ", "))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	if tl != nil {
		// Per-file tail positions and lag (refreshed ~10s), largest lag first.
		mux.HandleFunc("GET /debug/tailer", guard.protect(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			enc := json.NewEncoder(w)
			enc.SetIndent("", "  ")
			_ = enc.Encode(tl.Status())
		}))
		links = append(links, debugLink{"/debug/tailer", "/debug/tailer",
			"per-file tail positions and lag (largest first), rate-limit state, malformed pod annotations"})
	}
	if sc != nil {
		// The last scrape cycle's per-target outcomes, failures first: which
		// targets were discovered, which are down and why.
		mux.HandleFunc("GET /debug/targets", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			enc := json.NewEncoder(w)
			enc.SetIndent("", "  ")
			_ = enc.Encode(sc.Status())
		})
		links = append(links, debugLink{"/debug/targets", "/debug/targets",
			"per-target last scrape outcomes, failures first, pending targets included"})
	}
	// Live OTLP debug stream: what THIS agent is exporting, as JSON lines,
	// filtered/sampled per request (see internal/agent/debugtap).
	mux.HandleFunc("GET /debug/otlp", guard.protect(p.debugTap.ServeHTTP))
	mux.HandleFunc("GET /debug/otlp/ui", guard.protect(p.debugTap.ServeUI))
	links = append(links,
		debugLink{"/debug/otlp/ui", "/debug/otlp/ui",
			"live OTLP debug stream (UI): what this agent is exporting, filtered and sampled"},
		debugLink{"/debug/otlp?sample=100", "/debug/otlp",
			"the raw stream (curl -N): signal=logs|metrics|traces, attr=key=value globs, sample=pct"})
	if p.transforms != nil {
		// The active transform program's content hash: which nodes have
		// converged after a reload.
		mux.HandleFunc("GET /debug/transforms", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"hash": p.transforms.Active().Hash})
		})
		links = append(links, debugLink{"/debug/transforms", "/debug/transforms",
			"active transform program hash (per-node convergence after a reload)"})
	}
	var notes []string
	if *metricsListen != "" {
		notes = append(notes, "Prometheus metrics are on their own port: "+*metricsListen+" /metrics.")
	}
	if *pprofListen != "" {
		notes = append(notes, "pprof profiles are on their own port: "+*pprofListen+" /debug/pprof/.")
	}
	// The homepage links surfaces this reader may well be refused, so it says
	// which key opens them: without this the refusal is a 403 in a browser tab
	// with no way back to the flag that governs it.
	if guard.authenticated() {
		notes = append(notes, "/debug/otlp, /debug/otlp/ui and /debug/tailer stream this node's exported "+
			"telemetry: they are served to a local connection (kubectl port-forward) or to a request carrying "+
			"the -debug-token-file bearer token.")
	} else {
		notes = append(notes, "/debug/otlp, /debug/otlp/ui and /debug/tailer stream this node's exported "+
			"telemetry: they are served ONLY to a local connection (kubectl port-forward, or a container in "+
			"this pod). Set -debug-token-file to read them from elsewhere with a bearer token.")
	}
	home := debugHome(links, notes)
	mux.HandleFunc("GET /debug", home)
	mux.HandleFunc("GET /debug/{$}", home)
	// A bare port-forward lands on the homepage rather than a 404.
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/debug", http.StatusTemporaryRedirect)
	})
	return mux
}

// startDebugServer serves /healthz, /readyz and the /debug endpoints on
// -listen, shutting down on ctx cancel.
//
// The data-bearing surfaces (/debug/otlp, its UI, /debug/tailer) go through
// debugGuard.protect — this port is reachable from every pod in the cluster and
// that stream is this node's whole telemetry feed. See debugauth.go.
//
// guard is built by run() BEFORE any pipeline starts (newDebugGuard's token
// read is fatal, and a fatal return after the receivers are acking is data
// loss); it is nil exactly when -listen is empty.
func (p *pipelines) startDebugServer(ctx context.Context, guard *debugGuard, tl *tailer.Tailer, sc *promscrape.Scraper) {
	if *listen == "" {
		// Legal, and quietly expensive — configWarnings says so, for
		// -check-config and every start alike.
		return
	}
	mux := p.debugMux(guard, tl, sc)
	// Every handler here answers from an in-memory snapshot in
	// milliseconds, so tight timeouts are safe: ReadHeaderTimeout kills
	// Slowloris header trickling, Read/WriteTimeout bound trickled bodies
	// and stuck response writes, IdleTimeout reaps parked keep-alives.
	srv := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			// FATAL, like the ingest listener. This port carries /readyz, which
			// a rolling update advances on: an agent that cannot bind it is one
			// the kubelet will never call ready, so leaving the process running
			// buys nothing and hides the cause behind a probe timeout. Exiting
			// non-zero puts the bind error in the pod's own restart loop, where
			// somebody is already looking.
			p.fatal("health/debug server", err)
		}
	}()
	go func() {
		<-ctx.Done()
		// WithoutCancel(ctx), never a bare Background: the repo-wide rule for a
		// detached shutdown step (a bare Background silently strips whatever the
		// caller put on the context).
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			// Not fatal — the process is already leaving — but not silent
			// either: a debug handler still running here is one whose client is
			// about to be cut without a response when the process exits.
			p.log.Warn("health/debug server did not shut down cleanly", "error", err, "addr", *listen)
		}
	}()
	// The ACCESS MODE is on the line, not just the address: "who can read this
	// node's telemetry feed" is a property an operator must be able to read off
	// a running agent without diffing its flags.
	access := "local-only"
	if guard.authenticated() {
		access = "token"
	}
	p.log.Info("health/debug server started", "addr", *listen, "debugAccess", access)
}
