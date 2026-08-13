package main

import (
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/agent/servicegraph"
)

// -check-config must refuse a shard set that CrashLoops the tier.
//
// The dry run validated the serviceGraphShards SECTION and stopped, so every
// rule that only exists once the per-shard destination is derived was outside
// it: CI exited 0, printed "service-graph forwarding shards=2 ..." as if the
// ring were fine, and the same ConfigMap then aborted every pod of the
// StatefulSet at startup (podManagementPolicy: Parallel — a fresh install
// CrashLoops the whole tier, a rolling update halts on the first shard and takes
// its slice of the cluster's trace pushes down with it).
//
// This is the wiring half; servicegraph.TestValidateAgainstMatchesNewResharder
// pins the equivalence with what a start builds.
func TestValidateConfigChecksTheShardClients(t *testing.T) {
	defer restoreServiceGraphFlags(t)()
	*serviceGraphOn = true
	*serviceGraphToken = "/etc/kubescrape/service-graph/token" // shape only here
	*serviceGraphEndpoint = ""
	*serviceGraphShards = 0

	yes := true
	shards := servicegraph.ReshardConfig{
		StatefulSet: "kubescrape-servicegraph", Replicas: 2, Namespace: "monitoring",
		// TLS material beside a plaintext hop: a security control that silently
		// does nothing, which otlpexport.New refuses at the real start.
		Insecure: &yes, CAFile: "/etc/kubescrape/ca.pem",
	}
	err := validateConfig(agentConfig{ServiceGraphShards: &shards}, "")
	if err == nil {
		t.Fatal("-check-config accepted a shard set that aborts every trace-tier pod at startup")
	}
	if !strings.Contains(err.Error(), "service-graph shard ") || !strings.Contains(err.Error(), "plaintext") {
		t.Fatalf("error %q does not name the shard and the mistake", err)
	}

	// The same section with the hop's TLS intent spelled consistently is a
	// config a real start accepts, and the dry run must not invent a refusal of
	// its own (a check that is stricter than a start CrashLoops just as hard).
	shards.Insecure = nil
	if err := validateConfig(agentConfig{ServiceGraphShards: &shards}, ""); err != nil {
		t.Fatalf("rejected a shard set a real start accepts: %v", err)
	}
}
