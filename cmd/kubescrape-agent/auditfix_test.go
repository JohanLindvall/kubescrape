package main

import (
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/agent/servicegraph"
)

// The ring's TRANSPORT lives in the config section and the receivers in the
// flags, and a template ring is symmetric: it addresses THIS pod on exactly the
// protocol and port it addresses every sibling on. `protocol: http` with no
// -service-graph-http-listen therefore means every sibling POSTs /v1/traces at a
// gRPC-only listener — and because the readiness gates watch BINDING, the
// StatefulSet rolls out green and then refuses every application push for its
// lifetime. -check-config is the place that knows both surfaces, so it is the
// place that has to refuse.
func TestShardRingMustReachThisShardsOwnListener(t *testing.T) {
	defer restoreServiceGraphFlags(t)()
	tierOn(t)
	*serviceGraphEndpoint = "kubescrape-servicegraph.monitoring.svc:4319"
	*serviceGraphShards = 2

	// The chart's own shape: gRPC on exactly the port the ring dials.
	*serviceGraphListen, *serviceGraphHTTPListen = ":4319", ""
	if err := validateConfig(agentConfig{}, ""); err != nil {
		t.Fatalf("the chart's own flag-only shard config was rejected: %v", err)
	}

	// `protocol: http` with only the gRPC receiver bound.
	httpRing := func() agentConfig {
		return agentConfig{ServiceGraphShards: &servicegraph.ReshardConfig{Protocol: "http"}}
	}
	err := validateConfig(httpRing(), "")
	if err == nil {
		t.Fatal("serviceGraphShards.protocol: http was accepted with no -service-graph-http-listen: every sibling would forward at a gRPC-only listener")
	}
	if !strings.Contains(err.Error(), "-service-graph-http-listen") {
		t.Fatalf("error %q does not name the missing listener flag", err)
	}

	// Bound, but on a port nothing in the ring dials.
	*serviceGraphListen, *serviceGraphHTTPListen = "", ":4320"
	err = validateConfig(httpRing(), "")
	if err == nil {
		t.Fatal("an HTTP receiver on a port the ring never dials was accepted")
	}
	if !strings.Contains(err.Error(), "4319") || !strings.Contains(err.Error(), ":4320") {
		t.Fatalf("error %q names neither the port the ring dials nor the one bound", err)
	}

	// Bound on the ring's port: accepted.
	*serviceGraphHTTPListen = ":4319"
	if err := validateConfig(httpRing(), ""); err != nil {
		t.Fatalf("an HTTP receiver on the ring's own port was rejected: %v", err)
	}

	// The same check on the default transport: a gRPC ring dialling 4319 while
	// the receiver binds something else.
	*serviceGraphListen, *serviceGraphHTTPListen = ":4321", ""
	if err := validateConfig(agentConfig{}, ""); err == nil {
		t.Fatal("a gRPC receiver on a port the ring never dials was accepted")
	}
}

// Two shapes the cross-check must NOT refuse, both "this pod need not be in the
// ring it dials": explicit endpoints name the shard set outright (NewResharder
// only warns when self is not among them), and a single-shard template has no
// internal hop at all.
func TestShardListenerCrossCheckExemptions(t *testing.T) {
	defer restoreServiceGraphFlags(t)()
	tierOn(t)
	*serviceGraphListen, *serviceGraphHTTPListen = ":4319", ""

	*serviceGraphEndpoint, *serviceGraphShards = "", 0
	explicit := agentConfig{ServiceGraphShards: &servicegraph.ReshardConfig{
		Protocol:  "http",
		Endpoints: []string{"http://sg-0:4319", "http://sg-1:4319"},
	}}
	if err := validateConfig(explicit, ""); err != nil {
		t.Fatalf("explicit endpoints over http were refused for this pod's own listener: %v", err)
	}

	*serviceGraphEndpoint, *serviceGraphShards = "kubescrape-servicegraph.monitoring.svc:4319", 1
	single := agentConfig{ServiceGraphShards: &servicegraph.ReshardConfig{Protocol: "http"}}
	if err := validateConfig(single, ""); err != nil {
		t.Fatalf("a single-shard tier — no internal hop at all — was refused: %v", err)
	}
}

// The merge documents "the config section wins, field by field", and Service is
// the field that decides the per-pod DNS the ring dials. Overwriting an explicit
// one with the endpoint's first label addressed a governing Service nobody
// publishes records under, silently: the merge reports no error and
// -check-config stays green.
func TestShardSectionServiceSurvivesTheFlagTemplate(t *testing.T) {
	defer restoreServiceGraphFlags(t)()
	*serviceGraphEndpoint = "kubescrape-servicegraph.monitoring.svc:4319"
	*serviceGraphShards = 2

	cfg, err := serviceGraphShardConfig(&servicegraph.ReshardConfig{Service: "kubescrape-sg-headless"})
	if err != nil {
		t.Fatalf("merging a section that names only the Service: %v", err)
	}
	if cfg.Service != "kubescrape-sg-headless" {
		t.Fatalf("service = %q, want the section's kubescrape-sg-headless", cfg.Service)
	}
	// What the section left unset still comes from the flag template.
	if cfg.StatefulSet != "kubescrape-servicegraph" || cfg.Namespace != "monitoring" || cfg.Port != 4319 {
		t.Fatalf("statefulSet/namespace/port = %q/%q/%d, want the flag's", cfg.StatefulSet, cfg.Namespace, cfg.Port)
	}
	// And with the section silent, the endpoint's label still fills it in — the
	// convention (the governing Service carries the StatefulSet's name).
	if cfg, err = serviceGraphShardConfig(nil); err != nil || cfg.Service != "kubescrape-servicegraph" {
		t.Fatalf("service = %q, err = %v; want the endpoint's label as the DEFAULT", cfg.Service, err)
	}
}

// The no-persistence warning describes journald as much as the tailer —
// journald has no cursor file of its own, so it resumes at the journal TAIL —
// and it used to live inside startLogs, behind -logs, where a journald-only
// agent could never be told.
func TestPositionsWarningCoversJournald(t *testing.T) {
	logs, journald, pos := *logsOn, *journaldOn, *positionsFile
	t.Cleanup(func() { *logsOn, *journaldOn, *positionsFile = logs, journald, pos })

	*logsOn, *journaldOn, *positionsFile = false, true, ""
	got := strings.Join(configWarnings(agentConfig{}), "\n")
	if !strings.Contains(got, "-positions-file") || !strings.Contains(got, "journal") {
		t.Fatalf("a journald-only agent with no positions file was not told it starts at the tail: %q", got)
	}

	// The tailer's half still warns...
	*logsOn, *journaldOn = true, false
	if got := strings.Join(configWarnings(agentConfig{}), "\n"); !strings.Contains(got, "-positions-file") {
		t.Fatalf("a tailing agent with no positions file was not warned: %q", got)
	}

	// ...and neither pipeline has offsets to lose, or the file is configured.
	*logsOn, *journaldOn = false, false
	if got := strings.Join(configWarnings(agentConfig{}), "\n"); strings.Contains(got, "-positions-file") {
		t.Fatalf("warned about persistence with nothing that persists offsets: %q", got)
	}
	*logsOn, *journaldOn, *positionsFile = true, true, "/var/lib/kubescrape/positions.json"
	if got := strings.Join(configWarnings(agentConfig{}), "\n"); strings.Contains(got, "-positions-file") {
		t.Fatalf("warned about a positions file that IS configured: %q", got)
	}
}
