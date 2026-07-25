package main

import (
	"strings"
	"testing"
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
