package main

import (
	"fmt"
	"os"
	"os/exec"
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
			// route("name") resolves to the FIRST route of a name, and every
			// per-route series and failure line is keyed by it, so a second
			// route of the same name is unreachable by script and
			// indistinguishable everywhere an operator looks.
			"duplicate route name",
			agentConfig{Routing: &route.Config{Routes: []route.Route{
				{Name: "team", Namespaces: []string{"team-a-*"}, Headers: map[string]string{"X-Scope-OrgID": "a"}},
				{Name: "team", Namespaces: []string{"team-b-*"}, Headers: map[string]string{"X-Scope-OrgID": "b"}},
			}}},
			`name "team" is already used by route 0`,
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

// A real start CONSUMES what compileConfig compiled instead of compiling each
// section again behind error branches validation had already made unreachable.
// So every section it compiles has to come back populated when present — a
// field left nil would switch that section off at a real start while
// -check-config went on calling it valid — and nil when absent, which is what
// run() and the start functions test for. The kubelet endpoint comes back
// NORMALISED, because startScraper no longer parses the flag at all.
func TestCompileConfigHandsTheStartEverythingItCompiled(t *testing.T) {
	restoreKubeletFlags(t)
	dir := t.TempDir()
	cfg, err := loadAgentConfig(writeFile(t, dir, "config.yaml", `
resourceAttributes:
  static:
    k8s.cluster.name: prod
logAttributes:
  rules:
    - key: level
      attribute: log.level
logScrubbing:
  builtin: [defaults]
logMetrics:
  metrics:
    - name: errors_total
      value: "1"
      match: ["level=error"]
logs:
  sources:
    - name: app
      include: ["/var/log/app/*.log"]
  rules:
    - action: drop
      match: ["__severity__=debug"]
metrics:
  pipelines:
    all:
      - action: drop
        metrics: "go_.*"
routing:
  routes:
    - name: team-a
      namespaces: ["team-a-*"]
      headers: {X-Scope-OrgID: a}
`))
	if err != nil {
		t.Fatal(err)
	}
	transforms := writeFile(t, dir, "transforms.yaml", "logs: |\n  def transform(batch):\n      pass\n")
	*kubeletEndpoint = "https://fd00:10::5:10250"

	cc, err := compileConfig(*cfg, transforms)
	if err != nil {
		t.Fatalf("compileConfig: %v", err)
	}
	for name, missing := range map[string]bool{
		"resourceAttributes": cc.attrs == nil,
		"logAttributes":      cc.logAttrs == nil,
		"logScrubbing":       cc.scrub == nil,
		"logMetrics":         cc.logMetrics == nil,
		"logs.rules":         cc.logRules == nil,
		"logs.sources":       len(cc.logSources) != 1,
		"metrics.pipelines":  cc.metricFilters == nil,
		"routing":            len(cc.routes) != len(cfg.Routing.Routes),
		"-transforms-file":   cc.transforms == nil,
	} {
		if missing {
			t.Errorf("%s was compiled and validated but not handed to the start", name)
		}
	}
	if want := "https://[fd00:10::5]:10250"; cc.kubeletBase != want {
		t.Errorf("kubeletBase = %q, want the normalised %q", cc.kubeletBase, want)
	}

	*kubeletEndpoint = ""
	empty, err := compileConfig(agentConfig{}, "")
	if err != nil {
		t.Fatalf("compileConfig(empty): %v", err)
	}
	if empty.scrub != nil || empty.logMetrics != nil || empty.logRules != nil || empty.logSources != nil ||
		empty.metricFilters != nil || empty.routes != nil || empty.transforms != nil || empty.kubeletBase != "" {
		t.Errorf("an absent section must come back nil (run() and the start functions test for it): %+v", *empty)
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
	inFlight, recv := *ingestMaxInFlight, *ingestGRPCMaxRecv
	scrapeIv, logsMetricsIv := *scrapeInterval, *logsMetricsEvery
	t.Cleanup(func() {
		*scrapeTimeout, *logsRateLimit, *logsRateBurst = timeout, limit, burst
		*ingestMaxInFlight, *ingestGRPCMaxRecv = inFlight, recv
		*scrapeInterval, *logsMetricsEvery = scrapeIv, logsMetricsIv
	})
}

// A flag value outside its closed set, or -ingest with nothing to bind, is
// refused by validateConfig — the function -check-config and a real start
// share — rather than only by run()'s prologue, where these used to live and
// where no test could reach them. Typed or not: no default spells any of them.
func TestValidateConfigRefusesUnknownFlagChoices(t *testing.T) {
	mode, unknown, on, grpcAddr, httpAddr := *ingestMetrics, *logsUnknownFiles, *ingestOn, *ingestGRPC, *ingestHTTP
	t.Cleanup(func() {
		*ingestMetrics, *logsUnknownFiles, *ingestOn, *ingestGRPC, *ingestHTTP = mode, unknown, on, grpcAddr, httpAddr
	})
	for _, tc := range []struct {
		name  string
		apply func()
		want  string
	}{
		{"unknown ingest metrics mode", func() { *ingestMetrics = "per-point" }, `invalid -ingest-metrics-mode "per-point"`},
		{"unknown unknown-files mode", func() { *logsUnknownFiles = "middle" }, `invalid -logs-unknown-files "middle"`},
		{"ingest with no listener", func() { *ingestOn, *ingestGRPC, *ingestHTTP = true, "", "" }, "both -ingest-grpc-endpoint and -ingest-http-endpoint are empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			*ingestMetrics, *logsUnknownFiles, *ingestOn, *ingestGRPC, *ingestHTTP = mode, unknown, on, grpcAddr, httpAddr
			if err := validateConfig(agentConfig{}, ""); err != nil {
				t.Fatalf("the stock flags must validate: %v", err)
			}
			tc.apply()
			err := validateConfig(agentConfig{}, "")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want an error containing %q", err, tc.want)
			}
		})
	}
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
		{
			// The neighbouring -otlp-max-send-bytes documents "negative
			// disables", so a negative here reads as "no bound" and is in fact
			// normalised to the built-in 32, which the effective-limits line
			// then reports where the operator meant "unbounded".
			"negative ingest in-flight bound", "ingest-max-in-flight",
			func() { *ingestMaxInFlight = -1 },
			[]string{"-ingest-max-in-flight=-1", "0 for the default", "-otlp-max-send-bytes"},
		},
		{
			// Same trap, the other flag: a negative silently runs at the 4 MiB
			// default and every larger push is refused with ResourceExhausted
			// while the cap looks disabled.
			"negative ingest recv cap", "ingest-grpc-max-recv-bytes",
			func() { *ingestGRPCMaxRecv = -1 },
			[]string{"-ingest-grpc-max-recv-bytes=-1", "0 for the default"},
		},
		{
			// One typed 0, three meanings: the scrape loop never runs, the
			// cgroup sampler exports on a 30s window of its own, and the
			// effective-limits line prints 0. None of them is "off".
			"zero scrape interval", "scrape-interval",
			func() { *scrapeInterval = 0 },
			[]string{"-scrape-interval=0s", "positive duration", "-metrics=false"},
		},
		{
			"negative scrape interval", "scrape-interval",
			func() { *scrapeInterval = -time.Second },
			[]string{"-scrape-interval=-1s", "positive duration"},
		},
		{
			// Observed at full per-line cost, never exported until shutdown.
			"zero log-metrics interval", "logs-metrics-interval",
			func() { *logsMetricsEvery = 0 },
			[]string{"-logs-metrics-interval=0s", "positive duration", "logMetrics section"},
		},
		{
			"negative log-metrics interval", "logs-metrics-interval",
			func() { *logsMetricsEvery = -time.Second },
			[]string{"-logs-metrics-interval=-1s", "positive duration"},
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
	*scrapeInterval, *logsMetricsEvery = 0, 0
	if err := validateConfig(agentConfig{}, ""); err != nil {
		t.Fatalf("refused a value no operator typed: %v", err)
	}
}

// The usable shapes must keep passing, or the refusal is worse than the trap it
// replaces.
func TestValidateConfigAcceptsUsableTypedFlags(t *testing.T) {
	restoreFlagValues(t)
	typedFlags(t, "scrape-timeout", "logs-rate-limit", "logs-rate-burst", "scrape-interval", "logs-metrics-interval")

	*scrapeTimeout, *logsRateLimit, *logsRateBurst = 30*time.Second, 100, 200
	*scrapeInterval, *logsMetricsEvery = 15*time.Second, time.Minute
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

// The two ingest bounds' documented spelling of "use the built-in default" is 0,
// and it must keep passing — the chart and the manifests render it — as must a
// positive bound. Only the negative is refused.
func TestValidateConfigAcceptsTheIngestBoundDefaults(t *testing.T) {
	restoreFlagValues(t)
	typedFlags(t, "ingest-max-in-flight", "ingest-grpc-max-recv-bytes")

	for _, tc := range []struct{ inFlight, recv int }{
		{0, 0}, {64, 8 << 20}, {1, 1},
	} {
		*ingestMaxInFlight, *ingestGRPCMaxRecv = tc.inFlight, tc.recv
		if err := validateConfig(agentConfig{}, ""); err != nil {
			t.Fatalf("refused -ingest-max-in-flight=%d -ingest-grpc-max-recv-bytes=%d: %v", tc.inFlight, tc.recv, err)
		}
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
	// (the config summary's tierOnlySections line says so, at Info), so
	// nothing about it may be fatal.
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
