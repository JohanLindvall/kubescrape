package main

import (
	"strings"
	"testing"
)

// The tier binds up to four listeners from four independent flags, and the
// chart renders three of them from values — so `serviceGraph.port: 4317` (prose
// in values.yaml warns against it; nothing enforced it) puts the INTERNAL
// receiver on the application gRPC port. -check-config exited 0 on that: the
// ring cross-check compares the ring with its matching listener and never the
// listeners with each other. At the real start the two servers bind
// concurrently and whichever loses says `address already in use` and kills the
// process.
func TestValidateConfigRefusesCollidingTierListeners(t *testing.T) {
	defer restoreServiceGraphFlags(t)()
	*serviceGraphOn = true
	*serviceGraphToken = "/etc/kubescrape/service-graph/token"
	*serviceGraphEndpoint, *serviceGraphShards = "", 0
	*serviceGraphIngest = true
	*serviceGraphIngestGRPC, *serviceGraphIngestHTTP = ":4317", ":4318"

	// The chart's one-value mistake.
	*serviceGraphListen, *serviceGraphHTTPListen = ":4317", ""
	err := validateConfig(agentConfig{}, "")
	if err == nil {
		t.Fatal("-check-config accepted the internal receiver on an application port: the tier CrashLoops at bind")
	}
	if !strings.Contains(err.Error(), "-service-graph-listen") || !strings.Contains(err.Error(), "-service-graph-ingest-grpc") {
		t.Fatalf("error %q does not name both flags", err)
	}

	// Distinct ports are the working configuration and must stay accepted.
	*serviceGraphListen = ":4319"
	if err := validateConfig(agentConfig{}, ""); err != nil {
		t.Fatalf("rejected the default port layout: %v", err)
	}

	// A listener the start never binds cannot collide: with the application
	// ports off, the flags still hold their addresses and refusing them would
	// make the dry run stricter than the start.
	*serviceGraphIngest = false
	*serviceGraphListen = ":4317"
	if err := validateConfig(agentConfig{}, ""); err != nil {
		t.Fatalf("refused a collision with a listener -service-graph-ingest=false never binds: %v", err)
	}

	// The two internal listeners are each other's neighbours too.
	*serviceGraphIngest = true
	*serviceGraphListen, *serviceGraphHTTPListen = ":4319", ":4319"
	if err := validateConfig(agentConfig{}, ""); err == nil {
		t.Fatal("accepted the internal gRPC and HTTP receivers on one address")
	}

	// And off the tier none of this is read: the same ConfigMap and the same
	// flag defaults are shared with the DaemonSet, which binds neither.
	*serviceGraphOn = false
	if err := validateConfig(agentConfig{}, ""); err != nil {
		t.Fatalf("a colliding pair of tier listeners failed a workload that binds neither: %v", err)
	}
}

// The address comparison decides whether a refusal fires, so its two edges are
// pinned: a wildcard host contends with every address on its port, and two
// DIFFERENT hosts on one port do not contend at all (refusing those would make
// the dry run stricter than a start that binds them happily).
func TestSameListenAddr(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		want bool
	}{
		{":4317", ":4317", true},
		{":4317", "0.0.0.0:4317", true},     // "" and 0.0.0.0 are one wildcard
		{"0.0.0.0:4317", "[::]:4317", true}, // both wildcards
		{"127.0.0.1:4317", ":4317", true},   // the wildcard covers loopback
		{":04317", ":4317", true},           // one port, two spellings
		{"127.0.0.1:4317", "10.0.0.1:4317", false},
		{":4317", ":4318", false},
		{"", ":4317", false},     // empty disables the listener
		{"4317", ":4317", false}, // not host:port: left to fail at bind
	} {
		if got := sameListenAddr(tc.a, tc.b); got != tc.want {
			t.Errorf("sameListenAddr(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
		if got := sameListenAddr(tc.b, tc.a); got != tc.want {
			t.Errorf("sameListenAddr(%q, %q) = %v, want %v (it must be symmetric)", tc.b, tc.a, got, tc.want)
		}
	}
}

// The collision is not a TIER property, and checking only the tier's four
// listeners (and only under -service-graph) left two shapes an operator can
// type and -check-config signed off on:
//
//   - -ingest beside -service-graph. The node agent's logs-and-metrics receiver
//     and the tier's trace receiver DEFAULT to the same :4317/:4318, so one
//     process asked for both binds each twice; the loser gets `address already
//     in use`, and which server that is varies per restart.
//   - -pprof-listen typed onto -metrics-listen's :9090, which needs no feature
//     flag at all.
func TestValidateConfigRefusesAnyTwoListenersOnOneAddress(t *testing.T) {
	defer restoreServiceGraphFlags(t)()
	ingest, igrpc, ihttp := *ingestOn, *ingestGRPC, *ingestHTTP
	lst, mtr, pprof := *listen, *metricsListen, *pprofListen
	defer func() {
		*ingestOn, *ingestGRPC, *ingestHTTP = ingest, igrpc, ihttp
		*listen, *metricsListen, *pprofListen = lst, mtr, pprof
	}()

	// The shipped defaults on a plain DaemonSet agent must stay accepted.
	*ingestOn, *serviceGraphOn = false, false
	*listen, *metricsListen, *pprofListen = ":8081", ":9090", ""
	if err := validateConfig(agentConfig{}, ""); err != nil {
		t.Fatalf("the default listener layout was refused: %v", err)
	}

	// -ingest with its default ports, on a plain agent: still fine.
	*ingestOn, *ingestGRPC, *ingestHTTP = true, ":4317", ":4318"
	if err := validateConfig(agentConfig{}, ""); err != nil {
		t.Fatalf("-ingest on its own default ports was refused: %v", err)
	}

	// ...and the same flags with the tier turned on, which is where they
	// collide.
	*serviceGraphOn = true
	*serviceGraphToken = "/etc/kubescrape/service-graph/token"
	*serviceGraphEndpoint, *serviceGraphShards = "", 0
	*serviceGraphListen, *serviceGraphHTTPListen = ":4319", ""
	*serviceGraphIngest = true
	*serviceGraphIngestGRPC, *serviceGraphIngestHTTP = ":4317", ":4318"
	err := validateConfig(agentConfig{}, "")
	if err == nil {
		t.Fatal("-check-config accepted -ingest beside -service-graph on one set of ports: both bind :4317 and the loser CrashLoops")
	}
	if !strings.Contains(err.Error(), "-ingest-grpc-endpoint") || !strings.Contains(err.Error(), "-service-graph-ingest-grpc") {
		t.Fatalf("error %q does not name both flags", err)
	}

	// -pprof-listen onto -metrics-listen, with no feature flag involved.
	*ingestOn, *serviceGraphOn = false, false
	*pprofListen = ":9090"
	err = validateConfig(agentConfig{}, "")
	if err == nil {
		t.Fatal("-check-config accepted -pprof-listen on -metrics-listen's address")
	}
	if !strings.Contains(err.Error(), "-pprof-listen") || !strings.Contains(err.Error(), "-metrics-listen") {
		t.Fatalf("error %q does not name both flags", err)
	}

	// An empty listener is disabled and collides with nothing, however many of
	// them there are.
	*listen, *metricsListen, *pprofListen = "", "", ""
	if err := validateConfig(agentConfig{}, ""); err != nil {
		t.Fatalf("three disabled listeners were read as a collision: %v", err)
	}
}
