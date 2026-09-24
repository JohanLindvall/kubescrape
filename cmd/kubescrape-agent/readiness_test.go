package main

import (
	"context"
	"log/slog"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// /readyz gates a DaemonSet rolling update, so it must report NOT ready until
// the agent can do its job. It used to be the same static "ok" handler as
// /healthz, which let a broken rollout march across every node.
func TestReadinessGates(t *testing.T) {
	r := newReadiness()
	if got := r.pending(); len(got) != 0 {
		t.Fatalf("a gateless agent must be ready immediately, pending=%v", got)
	}

	r.require(gateMetadata)
	if got := r.pending(); len(got) != 1 || got[0] != gateMetadata {
		t.Fatalf("pending = %v, want [%s]", got, gateMetadata)
	}

	// require is idempotent: it must not clear a satisfied gate.
	r.done(gateMetadata)
	r.require(gateMetadata)
	if got := r.pending(); len(got) != 0 {
		t.Fatalf("pending = %v after the gate was satisfied, want none", got)
	}

	// Pending gates are sorted, so the probe body is stable.
	r.require("b")
	r.require("a")
	if got := strings.Join(r.pending(), ","); got != "a,b" {
		t.Fatalf("pending = %q, want a stable sorted list %q", got, "a,b")
	}
}

// The ingest listeners are what applications on the node push into, so they
// gate readiness exactly like the trace tier's do: an update that advanced
// before they bound rolled across the fleet while every node's receiver was a
// void, and /readyz said 200 throughout.
func TestIngestGatesReadinessOnItsListeners(t *testing.T) {
	defer func(on bool, g, h string) {
		*ingestOn, *ingestGRPC, *ingestHTTP = on, g, h
	}(*ingestOn, *ingestGRPC, *ingestHTTP)

	builders, err := buildAttrs(nil)
	if err != nil {
		t.Fatal(err)
	}
	start := func(t *testing.T, grpcAddr, httpAddr string) (*pipelines, func()) {
		t.Helper()
		*ingestOn, *ingestGRPC, *ingestHTTP = true, grpcAddr, httpAddr
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		p := &pipelines{
			wg: &wg, stop: cancel, log: slog.New(slog.DiscardHandler),
			ready: newReadiness(), attrBuilders: builders,
			fatalErr: &atomic.Pointer[error]{},
		}
		if err := p.startIngest(ctx); err != nil {
			cancel()
			t.Fatalf("startIngest: %v", err)
		}
		return p, func() { cancel(); wg.Wait() }
	}

	// An address that cannot be bound: the gate must hold /readyz down rather
	// than let the rollout proceed.
	//
	// A REAL EADDRINUSE (a port this test holds), and the assertion only once
	// the bind has demonstrably FAILED — p.fatal recorded the listener's error.
	// Asserting straight after startIngest returned read the gate before the
	// spawned goroutine had tried to bind at all, so it held on every run
	// however Ready was ordered against the bind: a listener that fired Ready
	// BEFORE binding passed this test, and nothing else in the repo pins "a
	// failed bind never marks the agent ready".
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()
	p, stop := start(t, held.Addr().String(), "")
	for deadline := time.Now().Add(5 * time.Second); p.fatalErr.Load() == nil; {
		if time.Now().After(deadline) {
			stop()
			t.Fatalf("binding the held address %s never failed; the refusal half of this test cannot be exercised", held.Addr())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !slices.Contains(p.ready.pending(), gateIngest) {
		stop()
		t.Fatalf("pending = %v after the ingest listener failed to bind (%v); want %q: a rolling update would advance past a node whose receiver is a void",
			p.ready.pending(), *p.fatalErr.Load(), gateIngest)
	}
	stop()

	// And it clears once the listener is bound.
	p, stop = start(t, freeAddr(t), "")
	defer stop()
	for deadline := time.Now().Add(5 * time.Second); slices.Contains(p.ready.pending(), gateIngest); {
		if time.Now().After(deadline) {
			t.Fatalf("gate %q still pending after the listener bound", gateIngest)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
