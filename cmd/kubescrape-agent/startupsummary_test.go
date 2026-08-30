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
	for _, line := range bytes.Split(bytes.TrimSuffix(buf.Bytes(), []byte("\n")), []byte("\n")) {
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
	nn, ep, kubelet, meta := *nodeName, *otlpEndpoint, *kubeletEndpoint, *metadataURL
	t.Cleanup(func() {
		*logsOn, *metricsOn, *ingestOn, *serviceGraphOn, *eventsOn = logs, metrics, ingest, sg, events
		*azureOn, *journaldOn, *nodeOn, *summaryOn, *cgroupStatsOn = azure, journald, node, summary, cgroup
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
// must not be silent — this is the flag that turns the readiness work off.
func TestEmptyListenIsWarnedAboutRatherThanSilent(t *testing.T) {
	addr := *listen
	t.Cleanup(func() { *listen = addr })
	*listen = ""

	var buf bytes.Buffer
	p := &pipelines{log: slog.New(cli.NewLogfmtHandler(&buf, slog.LevelInfo)), ready: newReadiness()}
	_ = p.startDebugServer(context.Background(), nil, nil)
	out := buf.String()
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "/readyz") {
		t.Errorf("an agent with no -listen did not say that readiness is unprobeable:\n%s", out)
	}
}
