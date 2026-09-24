package main

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JohanLindvall/logfmt"

	"github.com/JohanLindvall/kubescrape/internal/agent/servicegraph"
	"github.com/JohanLindvall/kubescrape/internal/cli"
)

// summaryLines renders the effective-configuration dump through the PRODUCTION
// handler and returns one map of key/value pairs per line, keyed by its msg.
// Through the real handler because the summary's whole job is to be greppable:
// a line that only reads well in a Go string is not the deliverable.
func summaryLines(t *testing.T, cfg agentConfig) map[string]map[string]string {
	t.Helper()
	var buf bytes.Buffer
	printConfigSummary(cfg, slog.New(cli.NewLogfmtHandler(&buf, slog.LevelInfo)))
	out := map[string]map[string]string{}
	for line := range bytes.SplitSeq(bytes.TrimSuffix(buf.Bytes(), []byte("\n")), []byte("\n")) {
		if err := logfmt.Validate(line); err != nil {
			t.Fatalf("summary line is not logfmt: %v\n%s", err, line)
		}
		pairs := map[string]string{}
		var msg string
		_ = logfmt.Iterate(line, func(k, v []byte) bool {
			if string(k) == "msg" {
				msg = string(v)
				return true
			}
			pairs[string(k)] = string(v)
			return true
		})
		if msg == "" {
			t.Fatalf("summary line has no msg: %s", line)
		}
		out[msg] = pairs
	}
	return out
}

func restoreSummaryFlags(t *testing.T) {
	t.Helper()
	logs, metrics, ingest, sg, events, azure, journald, node, summary, cgroup :=
		*logsOn, *metricsOn, *ingestOn, *serviceGraphOn, *eventsOn, *azureOn, *journaldOn, *nodeOn, *summaryOn, *cgroupStatsOn
	cadvisor := *cadvisorOn
	nn, ep, kubelet, meta := *nodeName, *otlpEndpoint, *kubeletEndpoint, *metadataURL
	t.Cleanup(func() {
		*logsOn, *metricsOn, *ingestOn, *serviceGraphOn, *eventsOn = logs, metrics, ingest, sg, events
		*azureOn, *journaldOn, *nodeOn, *summaryOn, *cgroupStatsOn = azure, journald, node, summary, cgroup
		*cadvisorOn = cadvisor
		*nodeName, *otlpEndpoint, *kubeletEndpoint, *metadataURL = nn, ep, kubelet, meta
	})
}

// The dump is what an operator has on a first live run, before any pipeline has
// produced a byte: every destination it will talk to, every socket it will
// bind, the identity it will stamp on its own series, and the cadences most
// likely to be wrong. A missing line reads as "not configured".
func TestEffectiveConfigDumpNamesDestinationsListenersIdentityAndLimits(t *testing.T) {
	restoreSummaryFlags(t)
	*nodeName = "node-1"
	*otlpEndpoint = "otel-collector.monitoring:4317"
	*metadataURL = "http://kubescrape.monitoring"
	*kubeletEndpoint = "https://10.0.0.1:10250"
	*ingestOn = true

	lines := summaryLines(t, agentConfig{})
	want := map[string][]string{
		"effective configuration": {"role", "sections", "optionalPipelines", "pipelines", "positionsFile", "transformsFile", "enrich", "selfAttributes", "logLevel"},
		"effective destinations":  {"metadataEndpoint", "otlpEndpoint", "otlpProtocol", "kubeletEndpoint", "bufferDir", "bufferMaxBytes", "otlpBearerTokenFile"},
		"effective listeners":     {"listen", "metricsListen", "pprofListen", "ingestGRPC", "ingestHTTP"},
		"effective identity":      {"node", "namespace", "serviceName", "instance", "selfMetricsInterval"},
		"effective limits":        {"scrapeInterval", "scrapeTimeout", "metadataWait", "logsExcludeNamespaces", "logsUnknownFiles", "ingestMaxInFlight"},
	}
	for msg, keys := range want {
		pairs, ok := lines[msg]
		if !ok {
			t.Errorf("the summary has no %q line", msg)
			continue
		}
		for _, k := range keys {
			if _, ok := pairs[k]; !ok {
				t.Errorf("%q does not report %q: %v", msg, k, pairs)
			}
		}
	}
	if got := lines["effective destinations"]["metadataEndpoint"]; got != *metadataURL {
		t.Errorf("metadataEndpoint = %q, want the flag's effective value", got)
	}
	if got := lines["effective identity"]["node"]; got != "node-1" {
		t.Errorf("node = %q, want node-1", got)
	}
}

// The identity a cluster-scoped role stamps is its POD, not its node: two
// workloads colliding on one (job, instance) interleave counters and render
// perfectly while doing it. The summary is where that is caught before it ships.
func TestEffectiveIdentityFollowsTheDeploymentRole(t *testing.T) {
	restoreSummaryFlags(t)
	*nodeName = "node-1"

	// A node agent: every per-node pipeline on, instance = the node.
	*logsOn, *metricsOn = true, true
	*serviceGraphOn, *eventsOn = false, false
	lines := summaryLines(t, agentConfig{})
	if role, inst := lines["effective configuration"]["role"], lines["effective identity"]["instance"]; role != "node-agent" || inst != "node-1" {
		t.Errorf("node agent: role/instance = %q/%q, want node-agent/node-1", role, inst)
	}

	// The trace tier: every per-node pipeline off, instance = the pod.
	*logsOn, *metricsOn, *cadvisorOn, *nodeOn, *summaryOn = false, false, false, false, false
	*journaldOn, *ingestOn, *cgroupStatsOn = false, false, false
	*serviceGraphOn = true
	old := selfInstanceName
	selfInstanceName = func() string { return "kubescrape-traces-0" }
	t.Cleanup(func() { selfInstanceName = old })
	lines = summaryLines(t, agentConfig{})
	if got := lines["effective configuration"]["role"]; got != "trace-tier-shard" {
		t.Errorf("role = %q, want trace-tier-shard", got)
	}
	if got := lines["effective identity"]["instance"]; got != "kubescrape-traces-0" {
		t.Errorf("instance = %q, want the pod name — a shard sharing a node with the DaemonSet would otherwise collide with it", got)
	}
}

// The identity line is the resource the self-metrics carry, READ BACK — not a
// second derivation of the role rule and of attrs.Identity's node fallback kept
// beside agentSelfResource, which is what it was: the two agreed only because
// nothing had changed one without the other yet. Pinned per role against
// agentSelfResource itself, including the hostNetwork singleton with no
// $POD_NAME, whose resource leaves the instance to Identity's node fallback.
func TestEffectiveIdentityIsTheStampedResource(t *testing.T) {
	restoreSummaryFlags(t)
	old := selfInstanceName
	t.Cleanup(func() { selfInstanceName = old })
	*nodeName = "node-1"
	perNodeOff := func() {
		*logsOn, *metricsOn, *cadvisorOn, *nodeOn, *summaryOn = false, false, false, false, false
		*journaldOn, *ingestOn, *cgroupStatsOn = false, false, false
	}
	for _, tc := range []struct {
		name     string
		set      func()
		pod      string
		wantInst string
	}{
		{"node agent", func() { *logsOn = true; *eventsOn, *azureOn, *serviceGraphOn = false, false, false }, "kubescrape-agent-x7k2p", "node-1"},
		{"events singleton", func() { perNodeOff(); *eventsOn, *azureOn, *serviceGraphOn = true, false, false }, "kubescrape-events-5d9f", "kubescrape-events-5d9f"},
		{"trace tier shard", func() { perNodeOff(); *eventsOn, *azureOn, *serviceGraphOn = false, false, true }, "kubescrape-traces-0", "kubescrape-traces-0"},
		{"singleton with no pod name", func() { perNodeOff(); *eventsOn, *azureOn, *serviceGraphOn = true, false, false }, "", "node-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.set()
			selfInstanceName = func() string { return tc.pod }
			res := agentSelfResource(*nodeName).Attributes()
			got := summaryLines(t, agentConfig{})["effective identity"]
			for key, attr := range map[string]string{
				"serviceName": "service.name",
				"instance":    "service.instance.id",
				"namespace":   "k8s.namespace.name",
			} {
				want := ""
				if v, ok := res.Get(attr); ok {
					want = v.AsString()
				}
				if got[key] != want {
					t.Errorf("summary %s = %q, but the stamped resource's %s is %q", key, got[key], attr, want)
				}
			}
			if got["instance"] != tc.wantInst || got["serviceName"] != agentServiceName {
				t.Errorf("serviceName/instance = %q/%q, want %s/%s", got["serviceName"], got["instance"], agentServiceName, tc.wantInst)
			}
		})
	}
}

// The summary reports credential FILES, never credentials: it is emitted at
// Info on every start, and a token in a log aggregator is there forever.
func TestEffectiveConfigDumpCarriesPathsNotCredentials(t *testing.T) {
	restoreSummaryFlags(t)
	tokenFile, ca := *otlpBearer, *otlpCAFile
	t.Cleanup(func() { *otlpBearer, *otlpCAFile = tokenFile, ca })
	*otlpBearer = "/var/run/secrets/otlp/token"
	*otlpCAFile = "/etc/ssl/collector-ca.pem"

	dest := summaryLines(t, agentConfig{})["effective destinations"]
	if dest["otlpBearerTokenFile"] != "/var/run/secrets/otlp/token" {
		t.Errorf("otlpBearerTokenFile = %q, want the path", dest["otlpBearerTokenFile"])
	}
	// Every key that can carry credential-adjacent material must name a file.
	for k, v := range dest {
		if strings.Contains(strings.ToLower(k), "token") && !strings.HasSuffix(k, "File") {
			t.Errorf("destination key %q=%q looks like a credential rather than a path to one", k, v)
		}
	}
}

// The ingest admission bounds are printed RESOLVED — the value in force, not
// the value typed. Both flags' stock value is 0 ("the built-in default"), so
// the raw flag put ingestMaxInFlight=0 on every default deployment beside a
// shed running at 32, reading as "unbounded"; and the per-message gRPC cap was
// not printed at all. They govern the trace tier's application ports too, so
// the tier reports them — it used to print neither, being -ingest=false.
func TestEffectiveLimitsReportTheIngestBoundsInForce(t *testing.T) {
	restoreSummaryFlags(t)
	inflight, recv := *ingestMaxInFlight, *ingestGRPCMaxRecv
	sgIngest, sgGRPC, sgHTTP := *serviceGraphIngest, *serviceGraphIngestGRPC, *serviceGraphIngestHTTP
	t.Cleanup(func() {
		*ingestMaxInFlight, *ingestGRPCMaxRecv = inflight, recv
		*serviceGraphIngest, *serviceGraphIngestGRPC, *serviceGraphIngestHTTP = sgIngest, sgGRPC, sgHTTP
	})
	check := func(what, wantInFlight, wantRecv string) {
		t.Helper()
		limits := summaryLines(t, agentConfig{})["effective limits"]
		if got := limits["ingestMaxInFlight"]; got != wantInFlight {
			t.Errorf("%s: ingestMaxInFlight = %q, want %q (the bound in force)", what, got, wantInFlight)
		}
		if got := limits["ingestGRPCMaxRecvBytes"]; got != wantRecv {
			t.Errorf("%s: ingestGRPCMaxRecvBytes = %q, want %q (the cap in force)", what, got, wantRecv)
		}
	}

	// The DaemonSet receiver at the stock values: the built-in defaults.
	*ingestOn, *serviceGraphOn = true, false
	*ingestMaxInFlight, *ingestGRPCMaxRecv = 0, 0
	check("default -ingest", "32", "4194304")

	// An explicit value is what is in force, so it is what is printed.
	*ingestMaxInFlight, *ingestGRPCMaxRecv = 8, 16<<20
	check("explicit -ingest", "8", "16777216")

	// The trace tier: -ingest off, application ports on — the same knobs
	// govern them (startServiceGraphIngest), so the same line reports them.
	*ingestOn, *serviceGraphOn = false, true
	*ingestMaxInFlight, *ingestGRPCMaxRecv = 0, 0
	*serviceGraphIngest, *serviceGraphIngestGRPC, *serviceGraphIngestHTTP = true, ":4317", ":4318"
	check("trace tier", "32", "4194304")

	// A tier serving no application port, and no -ingest: nothing these bounds
	// govern is running, so they are not reported.
	*serviceGraphIngest = false
	if limits := summaryLines(t, agentConfig{})["effective limits"]; limits["ingestMaxInFlight"] != "" || limits["ingestGRPCMaxRecvBytes"] != "" {
		t.Errorf("no ingest listener runs, yet the ingest bounds were reported: %v", limits)
	}
}

// The forwarding line names the ring THIS process re-shards onto, so it is
// printed on the tier only. Off it, a serviceGraphShards section in the shared
// ConfigMap is inert — nothing but the tier forwards traces, applications push
// to it — and the same dry run's tier-only-sections line says so; printing
// "service-graph forwarding shards=3" beside that contradicted it.
func TestServiceGraphForwardingIsReportedOnlyOnTheTier(t *testing.T) {
	restoreSummaryFlags(t)
	cfg := agentConfig{ServiceGraphShards: &servicegraph.ReshardConfig{
		StatefulSet: "kubescrape-traces", Namespace: "monitoring", Replicas: 3,
	}}

	*serviceGraphOn = false
	lines := summaryLines(t, cfg)
	if fwd, ok := lines["service-graph forwarding"]; ok {
		t.Errorf("a node agent reported forwarding traces it never receives: %v", fwd)
	}
	var inert string
	for msg, pairs := range lines {
		if strings.HasPrefix(msg, "tier-only config sections present") {
			inert = pairs["sections"]
		}
	}
	if !strings.Contains(inert, "serviceGraphShards") {
		t.Errorf("off the tier the section must be reported inert, got sections=%q", inert)
	}

	*serviceGraphOn = true
	if got := summaryLines(t, cfg)["service-graph forwarding"]["shards"]; got != "3" {
		t.Errorf("on the tier the forwarding line must name the ring: shards=%q, want 3", got)
	}
}

// syncBuffer is a bytes.Buffer a watchdog goroutine writes while the test reads
// it. The handler writes from another goroutine, so the buffer needs the lock —
// the race detector is right about that, and a sleep-then-read test would only
// hide it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// A gate that never clears is the worst first-run experience there is: the
// rollout stops at the first node and the process says nothing. The watchdog is
// what breaks that silence, and it must name the gate.
func TestReadinessWatchWarnsAboutAGateThatWillNotClear(t *testing.T) {
	grace, rewarn := readinessGrace, readinessReWarn
	readinessGrace, readinessReWarn = 10*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { readinessGrace, readinessReWarn = grace, rewarn })

	r := newReadiness()
	r.require(gateMetadata)
	var buf syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.watch(ctx, slog.New(cli.NewLogfmtHandler(&buf, slog.LevelInfo)))
	}()
	deadline := time.After(2 * time.Second)
	for !strings.Contains(buf.String(), gateMetadata) {
		select {
		case <-deadline:
			t.Fatalf("no warning naming the pending gate after the grace:\n%s", buf.String())
		case <-time.After(time.Millisecond):
		}
	}
	if !strings.Contains(buf.String(), "level=WARN") {
		t.Errorf("the pending gate was reported below WARN:\n%s", buf.String())
	}
	cancel()
	<-done
	if !strings.Contains(buf.String(), "shutting down before becoming ready") {
		t.Errorf("a pod killed before it ever became ready did not say so:\n%s", buf.String())
	}
}

// The other half: it says so ONCE when the agent becomes ready, and then stops
// — Info has to stay quiet in steady state.
func TestReadinessWatchReportsReadyOnceAndReturns(t *testing.T) {
	grace, rewarn := readinessGrace, readinessReWarn
	readinessGrace, readinessReWarn = 10*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { readinessGrace, readinessReWarn = grace, rewarn })

	r := newReadiness()
	done := r.gate(gateMetadata)
	done()
	var buf syncBuffer
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		r.watch(context.Background(), slog.New(cli.NewLogfmtHandler(&buf, slog.LevelInfo)))
	}()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("watch did not return once every gate was satisfied")
	}
	if n := strings.Count(buf.String(), "msg=ready"); n != 1 {
		t.Errorf("logged %d ready lines, want exactly 1:\n%s", n, buf.String())
	}
	if strings.Contains(buf.String(), "level=WARN") {
		t.Errorf("an agent that became ready immediately warned about it:\n%s", buf.String())
	}
}

// states() is what obs.RegisterReadiness publishes, so an unready fleet is
// visible to an alert and not only to whoever can curl a pod.
func TestReadinessStatesReportEveryGate(t *testing.T) {
	r := newReadiness()
	r.require("a")
	satisfy := r.gate("b")
	if got := r.states(); len(got) != 2 || got["a"] || got["b"] {
		t.Fatalf("states = %v, want both gates present and pending", got)
	}
	satisfy()
	if got := r.states(); !got["b"] || got["a"] {
		t.Errorf("states = %v, want b satisfied and a pending", got)
	}
}

// -listen empty takes /readyz with it, so a rolling update has nothing to gate
// on and marches across the fleet whatever each agent's state. Legal, but it
// must not be silent — this is the flag that turns the readiness work off. And
// it is a configWarnings entry, not a start-path Warn, so -check-config says it
// too: the start-only line meant a dry run described a different agent.
func TestEmptyListenIsWarnedAboutRatherThanSilent(t *testing.T) {
	addr := *listen
	t.Cleanup(func() { *listen = addr })
	*listen = ""

	if got := warnText(agentConfig{}); !strings.Contains(got, "-listen is empty") || !strings.Contains(got, "/readyz") {
		t.Errorf("configWarnings did not say that readiness is unprobeable with no -listen:\n%s", got)
	}
	*listen = ":8080"
	if got := warnText(agentConfig{}); strings.Contains(got, "-listen is empty") {
		t.Errorf("a set -listen was reported empty:\n%s", got)
	}
}

// Same for the trace tier with no application port: a start used to warn and
// -check-config did not.
func TestTierWithoutApplicationPortsIsWarnedByTheDryRun(t *testing.T) {
	withServiceGraph(t)
	on, g, h := *serviceGraphIngest, *serviceGraphIngestGRPC, *serviceGraphIngestHTTP
	t.Cleanup(func() { *serviceGraphIngest, *serviceGraphIngestGRPC, *serviceGraphIngestHTTP = on, g, h })

	const want = "the trace tier accepts no application pushes"
	*serviceGraphIngest, *serviceGraphIngestGRPC, *serviceGraphIngestHTTP = true, ":4317", ""
	if got := warnText(agentConfig{}); strings.Contains(got, want) {
		t.Fatalf("a tier serving gRPC was reported as accepting nothing:\n%s", got)
	}
	*serviceGraphIngestGRPC = ""
	if got := warnText(agentConfig{}); !strings.Contains(got, want) {
		t.Errorf("both application listeners empty, and -check-config said nothing:\n%s", got)
	}
	*serviceGraphIngest, *serviceGraphIngestGRPC = false, ":4317"
	if got := warnText(agentConfig{}); !strings.Contains(got, want) {
		t.Errorf("-service-graph-ingest=false, and -check-config said nothing:\n%s", got)
	}
}
