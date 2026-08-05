package main

import (
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/agent/logscrub"
	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/agent/route"
	"github.com/JohanLindvall/kubescrape/internal/agent/tailbuffer"
	"github.com/JohanLindvall/kubescrape/internal/agent/tailsample"
	"github.com/JohanLindvall/kubescrape/internal/agent/tracesample"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
)

// -check-config is only useful if it rejects everything a real start rejects.
// Both go through this one function, so they cannot drift.
func TestValidateConfigRejectsBadSections(t *testing.T) {
	cases := []struct {
		name string
		cfg  agentConfig
		want string
	}{
		{
			"unknown scrub pattern",
			agentConfig{LogScrubbing: &logscrub.Config{Builtin: []string{"no-such"}}},
			"logScrubbing",
		},
		{
			"malformed routing glob",
			agentConfig{Routing: &route.Config{Routes: []route.Route{
				{Name: "team-a", Namespaces: []string{"team-[a"}},
			}}},
			"routing",
		},
		{
			"route without namespaces",
			agentConfig{Routing: &route.Config{Routes: []route.Route{{Name: "x"}}}},
			"namespaces are required",
		},
		{
			"malformed trace-sampling duration",
			agentConfig{TraceSampling: &tracesample.Config{Probability: 0.5, KeepSlowerThan: "2quarters"}},
			"traceSampling",
		},
		{
			// The policy list is COMPILED by the dry run, so a bad regex, an
			// impossible rate allocation or (here) an unknown policy type fails
			// -check-config rather than the first trace.
			"unknown tail-sampling policy type",
			agentConfig{TailSampling: &tailbuffer.Config{Config: tailsample.Config{
				Policies: []tailsample.PolicyConfig{{Name: "x", Type: "sometimes"}}}}},
			"tailSampling",
		},
		{
			"malformed tail-sampling duration",
			agentConfig{TailSampling: &tailbuffer.Config{
				Config:       tailsample.Config{Policies: []tailsample.PolicyConfig{{Name: "all", Type: tailsample.TypeAlwaysSample}}},
				DecisionWait: "five seconds"}},
			"decisionWait",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateConfig(tc.cfg, "")
			if err == nil {
				t.Fatalf("validateConfig accepted an invalid config")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not name the offending section (%q)", err, tc.want)
			}
		})
	}
}

func TestValidateConfigAcceptsEmpty(t *testing.T) {
	if err := validateConfig(agentConfig{}, ""); err != nil {
		t.Fatalf("an empty config must be valid: %v", err)
	}
}

// -check-config is only trustworthy if it accepts EXACTLY what a real start
// accepts, which means compiling with the SAME options (the name prefix
// included). A nameless rule is rejected whatever the prefix — the prefix used
// to mask the empty-name check, compiling a rule with no name into a metric
// literally named the prefix, with two such rules silently sharing one series
// — and a named rule must pass with the prefix applied through both paths.
func TestValidateConfigUsesTheSameOptionsAsAStart(t *testing.T) {
	nameless := agentConfig{LogMetrics: &metrics.DynamicConfig{Metrics: []metrics.Dynamic{{
		Name: "", Type: metrics.CounterType, Value: "1",
		MatchRegexp: []string{"__line__=ERROR"},
	}}}}
	named := agentConfig{LogMetrics: &metrics.DynamicConfig{Metrics: []metrics.Dynamic{{
		Name: "errors_total", Type: metrics.CounterType, Value: "1",
		MatchRegexp: []string{"__line__=ERROR"},
	}}}}

	old := *logsMetricsPrefix
	defer func() { *logsMetricsPrefix = old }()

	for _, prefix := range []string{"app_", ""} {
		*logsMetricsPrefix = prefix
		if err := validateConfig(nameless, ""); err == nil {
			t.Fatalf("prefix %q: accepted a nameless metric rule", prefix)
		}
		if err := validateConfig(named, ""); err != nil {
			t.Fatalf("prefix %q: rejected a config a real start accepts: %v", prefix, err)
		}
	}
}

// With all three signals overridden, BuildExporter never constructs the
// default chain — the collectorless case, where -otlp-endpoint still holds
// the stock collector address nothing dials. Validating that base anyway made
// -check-config (and, since run() calls it too, every real start) reject a
// config the exporter accepts, CrashLooping the DaemonSet fleet-wide.
func TestValidateConfigSkipsAnUnbuiltBase(t *testing.T) {
	old := *otlpEndpoint
	defer func() { *otlpEndpoint = old }()
	*otlpEndpoint = "" // no default destination at all

	full := &otlpexport.ExportConfig{
		Logs:    &otlpexport.ExportOverride{Endpoint: "https://loki:443", Protocol: "http"},
		Metrics: &otlpexport.ExportOverride{Endpoint: "https://mimir:443", Protocol: "http"},
		Traces:  &otlpexport.ExportOverride{Endpoint: "https://tempo:443", Protocol: "http"},
	}
	if err := validateConfig(agentConfig{Export: full}, ""); err != nil {
		t.Fatalf("rejected a fully-overridden export config a real start accepts: %v", err)
	}

	// One signal short: the default IS built, so its base must still validate.
	partial := &otlpexport.ExportConfig{Logs: full.Logs, Metrics: full.Metrics}
	if err := validateConfig(agentConfig{Export: partial}, ""); err == nil {
		t.Fatal("accepted an endpoint-less base that the default chain would be built from")
	}
}

// -check-config must reject exactly what a real start rejects. It used to
// check a route's name, namespaces and patterns and stop, so a scheme-less
// route endpoint passed the dry run and CrashLooped the agent on start — the
// one outcome the check exists to prevent.
func TestValidateConfigChecksRouteDestinations(t *testing.T) {
	bad := agentConfig{Routing: &route.Config{Routes: []route.Route{{
		Name: "tenant-a", Namespaces: []string{"a-*"},
		Endpoint: "collector.example.com:4318", // no scheme
	}}}}
	oldProto, oldEP := *otlpProtocol, *otlpEndpoint
	defer func() { *otlpProtocol, *otlpEndpoint = oldProto, oldEP }()
	// A valid base, so the ROUTE endpoint is the only thing under test.
	*otlpProtocol, *otlpEndpoint = "http", "https://collector.example.com:4318"

	if err := validateConfig(bad, ""); err == nil {
		t.Fatal("accepted a scheme-less route endpoint that otlpexport.New rejects at startup")
	}

	good := agentConfig{Routing: &route.Config{Routes: []route.Route{{
		Name: "tenant-a", Namespaces: []string{"a-*"},
		Endpoint: "https://collector.example.com:4318",
	}}}}
	if err := validateConfig(good, ""); err != nil {
		t.Fatalf("rejected a route a real start accepts: %v", err)
	}
}

// A route with no endpoint inherits the flag base — which is not a
// destination at all when every signal is overridden in export:, since
// BuildExporter never builds the default chain. Inheriting it there sent a
// tenant's telemetry to whatever the endpoint flag happened to default to.
func TestRouteWithoutEndpointRejectedWhenTheBaseIsUnused(t *testing.T) {
	full := &otlpexport.ExportConfig{
		Logs:    &otlpexport.ExportOverride{Endpoint: "https://loki:443", Protocol: "http"},
		Metrics: &otlpexport.ExportOverride{Endpoint: "https://mimir:443", Protocol: "http"},
		Traces:  &otlpexport.ExportOverride{Endpoint: "https://tempo:443", Protocol: "http"},
	}
	headerOnly := []route.Route{{Name: "tenant-a", Namespaces: []string{"a-*"}, Headers: map[string]string{"X-Scope-OrgID": "a"}}}

	cfg := agentConfig{Export: full, Routing: &route.Config{Routes: headerOnly}}
	if err := validateConfig(cfg, ""); err == nil {
		t.Fatal("accepted a header-only route inheriting a base the deployment never dials")
	}

	// With the default chain in play, inheriting the base is the point.
	partial := &otlpexport.ExportConfig{Logs: full.Logs}
	cfg = agentConfig{Export: partial, Routing: &route.Config{Routes: headerOnly}}
	if err := validateConfig(cfg, ""); err != nil {
		t.Fatalf("rejected a header-only route where the base IS the fallback destination: %v", err)
	}

	// All three signals overridden, but one override sets only HEADERS — it
	// inherits the base endpoint through merged(), so the base is still a
	// destination and an endpoint-less route is legitimate. Testing struct
	// presence instead of endpoints failed this config at startup.
	inherits := &otlpexport.ExportConfig{
		Logs:    full.Logs,
		Metrics: full.Metrics,
		Traces:  &otlpexport.ExportOverride{Headers: map[string]string{"X-Scope-OrgID": "traces"}},
	}
	cfg = agentConfig{Export: inherits, Routing: &route.Config{Routes: headerOnly}}
	if err := validateConfig(cfg, ""); err != nil {
		t.Fatalf("rejected a route inheriting a base that a header-only override still dials: %v", err)
	}
}
