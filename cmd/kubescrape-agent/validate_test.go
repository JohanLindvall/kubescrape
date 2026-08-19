package main

import (
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

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

// typedFlags makes flagWasSet report exactly these flags as typed. The refusals
// read the OPERATOR's command line, and a test cannot type one: flag.Set is the
// only way into flag.Visit's record and there is no way out, so a typed flag
// would leak into every later test in this package. The real command line is
// covered by TestCheckConfigExitStatus, which parses one in a child process.
func typedFlags(t *testing.T, names ...string) {
	t.Helper()
	old := flagWasSet
	t.Cleanup(func() { flagWasSet = old })
	flagWasSet = func(name string) bool { return slices.Contains(names, name) }
}

// restoreFlagValues restores the flag values a test overwrites in place.
func restoreFlagValues(t *testing.T) {
	t.Helper()
	timeout, limit, burst := *scrapeTimeout, *logsRateLimit, *logsRateBurst
	t.Cleanup(func() { *scrapeTimeout, *logsRateLimit, *logsRateBurst = timeout, limit, burst })
}

// A flag value that can only ever be a mistake is REFUSED, not normalised. The
// consumers still normalise (promscrape.New defaults the timeout, tailer.New
// floors the burst) — that is the library guarantee for a zeroed field — but an
// operator who typed the flag asked for something the process will not do, and
// learning that from a warning scrolling past a rolling update is learning it
// too late. Every message names the flag, the value and a usable one.
func TestValidateConfigRefusesTypedFlagNonsense(t *testing.T) {
	for _, tc := range []struct {
		name  string
		flag  string
		apply func()
		want  []string
	}{
		{
			"zero scrape timeout", "scrape-timeout",
			func() { *scrapeTimeout = 0 },
			[]string{"-scrape-timeout=0s", "positive duration"},
		},
		{
			"negative scrape timeout", "scrape-timeout",
			func() { *scrapeTimeout = -time.Second },
			[]string{"-scrape-timeout=-1s", "positive duration"},
		},
		{
			// A bucket typed as a bucket: it can never hold the one whole token
			// a line costs, so pause mode reads nothing, forever.
			"burst below one whole token", "logs-rate-burst",
			func() { *logsRateLimit, *logsRateBurst = 100, 0.5 },
			[]string{"-logs-rate-burst=0.5", "burst of 1 or more"},
		},
		{
			"negative burst", "logs-rate-burst",
			func() { *logsRateLimit, *logsRateBurst = 100, -1 },
			[]string{"-logs-rate-burst=-1", "burst of 1 or more"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			restoreFlagValues(t)
			typedFlags(t, tc.flag)
			tc.apply()
			err := validateConfig(agentConfig{}, "")
			if err == nil {
				t.Fatalf("accepted a typed -%s value the process cannot honour", tc.flag)
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error %q does not name %q", err, want)
				}
			}
		})
	}
}

// The refusal is about what an operator TYPED, which is why it reads
// flag.Visit's record rather than comparing against the flag's default: a
// non-positive bound arriving any other way — a default that changes later, a
// value set programmatically — belongs to the constructors that normalise it.
func TestValidateConfigRefusesOnlyTypedValues(t *testing.T) {
	restoreFlagValues(t)
	typedFlags(t) // nothing typed
	*scrapeTimeout, *logsRateLimit, *logsRateBurst = 0, 100, 0.5
	if err := validateConfig(agentConfig{}, ""); err != nil {
		t.Fatalf("refused a value no operator typed: %v", err)
	}
}

// The usable shapes must keep passing, or the refusal is worse than the trap it
// replaces.
func TestValidateConfigAcceptsUsableTypedFlags(t *testing.T) {
	restoreFlagValues(t)
	typedFlags(t, "scrape-timeout", "logs-rate-limit", "logs-rate-burst")

	*scrapeTimeout, *logsRateLimit, *logsRateBurst = 30*time.Second, 100, 200
	if err := validateConfig(agentConfig{}, ""); err != nil {
		t.Fatalf("refused usable values: %v", err)
	}

	// 0 is the documented sentinel for "2x -logs-rate-limit", and the chart
	// passes it verbatim on every deployment that enables rate limiting, so
	// reading it as a bucket size would refuse the stock config.
	*logsRateBurst = 0
	if err := validateConfig(agentConfig{}, ""); err != nil {
		t.Fatalf("refused the derive sentinel the chart passes: %v", err)
	}

	// A bucket DERIVED below one token is warned about (TestDerivedRateBurstWarns),
	// never refused: the operator typed the rate, not the bucket, and the rate is
	// delivered exactly.
	*logsRateLimit = 0.2
	if err := validateConfig(agentConfig{}, ""); err != nil {
		t.Fatalf("refused a rate whose derived bucket the tailer floors: %v", err)
	}
	if got := warnText(agentConfig{}); !strings.Contains(got, "bucket of 0.4") {
		t.Fatalf("the derived bucket normalises silently: %q", got)
	}
}

// checkConfigArgsEnv carries the child process' command line.
const checkConfigArgsEnv = "KUBESCRAPE_TEST_CHECK_CONFIG_ARGS"

// Refusing rather than warning is only worth anything if -check-config EXITS
// NON-ZERO on it: that exit status is what a CI job and a pre-rollout check
// read. Exercised in a child process, which is also the only place the real
// command line — flag.Parse, and so the real flagWasSet — is reachable: the
// parent's os.Args belong to `go test`.
func TestCheckConfigExitStatus(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		wantErr bool
		want    string
	}{
		{"typed zero scrape timeout", []string{"-scrape-timeout=0"}, true, "-scrape-timeout=0s"},
		{"typed sub-token burst", []string{"-logs-rate-limit=100", "-logs-rate-burst=0.5"}, true, "-logs-rate-burst=0.5"},
		// Warned, and the dry run still exits 0 — the operator typed the rate,
		// and the rate is what they get.
		{"derived sub-token bucket", []string{"-logs-rate-limit=0.2"}, false, "bucket of 0.4"},
		{"usable values", []string{"-scrape-timeout=30s", "-logs-rate-limit=100", "-logs-rate-burst=200"}, false, ""},
		{"nothing typed", nil, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			argv := append([]string{"-check-config", "-node-name=test-node"}, tc.args...)
			cmd := exec.Command(os.Args[0], "-test.run=TestCheckConfigChild")
			cmd.Env = append(os.Environ(), checkConfigArgsEnv+"="+strings.Join(argv, " "))
			out, err := cmd.CombinedOutput()
			if tc.wantErr && err == nil {
				t.Fatalf("-check-config exited 0 on %v:\n%s", tc.args, out)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("-check-config exited non-zero on %v: %v\n%s", tc.args, err, out)
			}
			if tc.want != "" && !strings.Contains(string(out), tc.want) {
				t.Fatalf("-check-config output does not name %q:\n%s", tc.want, out)
			}
		})
	}
}

// TestCheckConfigChild is the child half of TestCheckConfigExitStatus: it runs
// the real run(), so the exit status under test is the binary's own. Skipped
// unless the parent asked for it.
func TestCheckConfigChild(t *testing.T) {
	argv := os.Getenv(checkConfigArgsEnv)
	if argv == "" {
		t.Skip("child half of TestCheckConfigExitStatus")
	}
	os.Args = append([]string{"kubescrape-agent"}, strings.Fields(argv)...)
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
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

// routeExportConfig's own-endpoint branch now builds on
// otlpexport.Config.TransportOnly instead of hand-dropping each credential.
// The rework must be BIT-IDENTICAL to the old derivation — this test IS that
// old derivation, compared field-for-field against the new one for every
// shape the old code distinguished, with ONE deliberate divergence spelled
// here: an UNSET route `insecure` (nil) keeps the merged base's value instead
// of a bool's zero, because every route written before the field existed
// dialled plaintext through -otlp-insecure and must keep doing so across the
// upgrade. The base is constructed with EVERY destination-scoped flag and
// section field set, so a field the new derivation drops (or newly inherits)
// cannot hide behind a zero value.
func TestRouteExportConfigMatchesTheOldDerivation(t *testing.T) {
	oldDerivation := func(exp *otlpexport.ExportConfig, rt route.Route) otlpexport.Config {
		rcfg := exp.ApplyBase(baseExportConfig())
		if len(rt.Headers) > 0 {
			merged := make(map[string]string, len(rcfg.Headers)+len(rt.Headers))
			for k, v := range rcfg.Headers {
				merged[k] = v
			}
			for k, v := range rt.Headers {
				merged[k] = v
			}
			rcfg.Headers = merged
		}
		if rt.Endpoint != "" {
			rcfg.Endpoint = rt.Endpoint
			rcfg.BearerTokenFile = rt.BearerTokenFile
			rcfg.ClientCertFile = rt.ClientCertFile
			rcfg.ClientKeyFile = rt.ClientKeyFile
			rcfg.CAFile = rt.CAFile
			if rt.Insecure != nil {
				rcfg.Insecure = *rt.Insecure
			}
		}
		return rcfg
	}

	// A base with every destination-scoped field populated: bearer, CA,
	// skip-verify from the flags; headers and the mTLS client pair from the
	// export section.
	oldEP, oldBearer, oldCA, oldSkip := *otlpEndpoint, *otlpBearer, *otlpCAFile, *otlpSkipTLS
	defer func() { *otlpEndpoint, *otlpBearer, *otlpCAFile, *otlpSkipTLS = oldEP, oldBearer, oldCA, oldSkip }()
	*otlpEndpoint = "base-collector:4317"
	*otlpBearer = "/base/bearer-token"
	*otlpCAFile = "/base/ca.pem"
	*otlpSkipTLS = true

	exp := &otlpexport.ExportConfig{
		Headers:        map[string]string{"X-Scope-OrgID": "base", "X-Base": "1"},
		ClientCertFile: "/base/client.pem",
		ClientKeyFile:  "/base/client-key.pem",
	}

	insecureTrue, insecureFalse := true, false
	for _, tc := range []struct {
		name string
		rt   route.Route
	}{
		{"base-only route", route.Route{
			Name: "t", Namespaces: []string{"t-*"},
		}},
		{"own-endpoint route with credentials", route.Route{
			Name: "t", Namespaces: []string{"t-*"},
			Endpoint:        "https://tenant.example.com:443",
			BearerTokenFile: "/route/bearer-token",
			ClientCertFile:  "/route/client.pem",
			ClientKeyFile:   "/route/client-key.pem",
			CAFile:          "/route/ca.pem",
			Insecure:        &insecureTrue,
		}},
		{"own-endpoint route with an explicit insecure:false", route.Route{
			Name: "t", Namespaces: []string{"t-*"},
			Endpoint: "tenant.example.com:4317",
			Insecure: &insecureFalse,
		}},
		{"own-endpoint route without credentials", route.Route{
			Name: "t", Namespaces: []string{"t-*"},
			Endpoint: "https://tenant.example.com:443",
		}},
		{"headers merge onto an own-endpoint route", route.Route{
			Name: "t", Namespaces: []string{"t-*"},
			Endpoint: "https://tenant.example.com:443",
			Headers:  map[string]string{"X-Scope-OrgID": "route", "X-Route": "1"},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := routeExportConfig(exp, tc.rt)
			if err != nil {
				t.Fatalf("routeExportConfig: %v", err)
			}
			if want := oldDerivation(exp, tc.rt); !reflect.DeepEqual(got, want) {
				t.Errorf("derivation drifted from the old one:\n got: %+v\nwant: %+v", got, want)
			}
		})
	}
}

// A route config written before the `insecure` field existed reached its
// plaintext in-cluster collector through the flag base's -otlp-insecure
// (default true). The field's introduction must not flip those routes to TLS
// on upgrade — the exports fail as endless transient Unavailable, which
// -check-config cannot see — so unset (nil) inherits the merged base and an
// explicit value wins in either direction.
func TestRouteInsecureUnsetInheritsTheFlagBase(t *testing.T) {
	rt := route.Route{Name: "t", Namespaces: []string{"t-*"}, Endpoint: "collector.team-a:4317"}

	got, err := routeExportConfig(nil, rt)
	if err != nil {
		t.Fatalf("routeExportConfig: %v", err)
	}
	if !got.Insecure {
		t.Fatal("an insecure-less own-endpoint route flipped to TLS: pre-field configs dial plaintext through the base")
	}

	// An explicit false is the operator choosing TLS, base notwithstanding.
	no := false
	rt.Insecure = &no
	if got, err = routeExportConfig(nil, rt); err != nil || got.Insecure {
		t.Fatalf("insecure:false must mean TLS whatever the base (insecure=%v, err=%v)", got.Insecure, err)
	}

	// And with a TLS base, unset still inherits while explicit true wins.
	oldInsecure := *otlpInsecure
	defer func() { *otlpInsecure = oldInsecure }()
	*otlpInsecure = false
	rt.Insecure = nil
	if got, err = routeExportConfig(nil, rt); err != nil || got.Insecure {
		t.Fatalf("unset insecure must inherit a TLS base (insecure=%v, err=%v)", got.Insecure, err)
	}
	yes := true
	rt.Insecure = &yes
	if got, err = routeExportConfig(nil, rt); err != nil || !got.Insecure {
		t.Fatalf("insecure:true must mean plaintext whatever the base (insecure=%v, err=%v)", got.Insecure, err)
	}
}

// The `type: script` tail-sampling cross-check is TIER-ONLY, like every other
// tier-only refusal in validateConfig. ONE ConfigMap is mounted into the
// DaemonSet, the events/Azure singleton and the tier, and the chart renders
// -transforms-file on none of them, so a check that fires wherever the section
// is merely PRESENT makes a config that is valid exactly where it is read exit
// every other workload 1 at startup — the singleton unrepairably, since its
// template exposes no extraVolumes to mount a transforms file with.
func TestScriptTailSamplingPolicyIsRefusedOnlyOnTheTier(t *testing.T) {
	cfg := agentConfig{TailSampling: &tailbuffer.Config{Config: tailsample.Config{
		Policies: []tailsample.PolicyConfig{{Name: "keep-interesting", Type: tailsample.TypeScript}},
	}}}
	// The DaemonSet / events singleton: the section is ignored here
	// (startServiceGraph warns that it is), so nothing about it may be fatal.
	if err := validateConfig(cfg, ""); err != nil {
		t.Fatalf("off the trace tier a script tail-sampling policy must not be fatal — the workload never reads the section: %v", err)
	}

	// On the tier it IS read, and a policy naming a decide(trace) the transforms
	// file does not define can never be satisfied, so the refusal stays.
	withServiceGraph(t)
	token, listen := *serviceGraphToken, *serviceGraphListen
	defer func() { *serviceGraphToken, *serviceGraphListen = token, listen }()
	*serviceGraphToken, *serviceGraphListen = "/etc/kubescrape/sg/token", ":4319"
	err := validateConfig(cfg, "")
	if err == nil || !strings.Contains(err.Error(), "sample:") {
		t.Fatalf("on the tier the cross-check must still refuse the unsatisfiable policy, got %v", err)
	}
}
