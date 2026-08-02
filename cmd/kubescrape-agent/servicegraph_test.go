package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/agent/servicegraph"
)

// The service graph costs a workload, a network hop per edge-forming span and
// a new metric family. None of it may start because the binary did, and the
// defaults are also the contract the chart and docs quote (the receiver port in
// particular: 4317 is the agents' OWN ingest port, and a shard listening there
// would loop spans back through the fan-out).
func TestServiceGraphIsOffByDefault(t *testing.T) {
	if *serviceGraphOn {
		t.Error("-service-graph defaults to on")
	}
	if *serviceGraphShards != 0 {
		t.Errorf("-service-graph-shards defaults to %d, want 0", *serviceGraphShards)
	}
	if *serviceGraphEndpoint != "" || *serviceGraphToken != "" || *serviceGraphHTTPListen != "" {
		t.Errorf("a service-graph address/credential defaults to non-empty: endpoint=%q token=%q http=%q",
			*serviceGraphEndpoint, *serviceGraphToken, *serviceGraphHTTPListen)
	}
	if *serviceGraphListen != ":4319" {
		t.Errorf("-service-graph-listen defaults to %q, want :4319 (:4317 is the agent's own ingest port)", *serviceGraphListen)
	}
	if *serviceGraphIv != time.Minute {
		t.Errorf("-service-graph-interval defaults to %v, want 1m", *serviceGraphIv)
	}
	// The two sides' defaults must be the SAME port. They are configured
	// independently — the shard from -service-graph-listen, the agents from the
	// serviceGraphShards section's `port` — so a config that names neither has
	// the agents dialling wherever servicegraph.DefaultShardPort points. It
	// pointed at 4317, the agents' OWN ingest port, which the shard pods do not
	// serve (-ingest=false): every forward failed into a counter and the graph
	// stayed empty.
	if _, port, err := net.SplitHostPort(*serviceGraphListen); err != nil {
		t.Errorf("-service-graph-listen %q is not host:port: %v", *serviceGraphListen, err)
	} else if port != strconv.Itoa(servicegraph.DefaultShardPort) {
		t.Errorf("the shard listens on %s by default but the agents dial %d: a config naming neither port talks to the wrong one",
			port, servicegraph.DefaultShardPort)
	}

	p := testPipelines(t)
	if err := p.startServiceGraph(); err != nil {
		t.Fatalf("startServiceGraph with the feature off: %v", err)
	}
	if p.serviceGraphProc != nil || p.serviceGraphReg != nil {
		t.Error("the shard role started with -service-graph off")
	}
	if pending := p.ready.pending(); len(pending) != 0 {
		t.Errorf("registered readiness gates with the feature off: %v", pending)
	}
	fwd, err := serviceGraphForwarder(nil, p.log)
	if err != nil {
		t.Fatalf("building a forwarder with nothing configured: %v", err)
	}
	if fwd != nil {
		t.Error("built a shard forwarder with no shards configured")
	}
}

func TestValidateConfigAcceptsServiceGraphSections(t *testing.T) {
	cfg := agentConfig{
		ServiceGraph: &servicegraph.Config{
			Wait: "5s", MaxItems: 1000, MaxCardinality: 5000,
			StaleAfter:       "10m",
			HistogramBuckets: []float64{0.1, 0.25, 1, 5},
			Dimensions:       []string{"http.route"},
		},
		ServiceGraphShards: &servicegraph.ForwardConfig{
			StatefulSet: "kubescrape-servicegraph", Replicas: 3,
			Namespace: "monitoring", Port: 4319,
			Dimensions: []string{"http.route"}, // must match the shard's
		},
	}
	if err := validateConfig(cfg, ""); err != nil {
		t.Fatalf("rejected a config a real start accepts: %v", err)
	}
}

// The documented YAML must decode through the REAL path — loadAgentConfig's
// sigs.k8s.io/yaml UnmarshalStrict — not merely through a Go struct literal.
// That decode is YAML -> JSON -> encoding/json, which accepts only a raw
// nanosecond integer for a time.Duration, and because the whole file is strict
// a single undecodable field rejects EVERY section: the DaemonSet and the shard
// StatefulSet both fail to start on a config the docs, the chart's values
// comments and README all show verbatim.
func TestServiceGraphDocumentedYAMLDecodes(t *testing.T) {
	// Copied from docs/CONFIGURATION.md and charts/kubescrape/values.yaml.
	const doc = `
serviceGraph:
  wait: 10s
  maxItems: 10000
  maxCardinality: 20000
  staleAfter: 15m
  histogramBuckets: [0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 6.4, 12.8]
  exemplars: true
  dimensions: [http.route]
  virtualNodePeerAttributes: [peer.service, db.name, db.system]
serviceGraphShards:
  statefulSet: kubescrape-servicegraph
  replicas: 2
  namespace: monitoring
  port: 4319
  dimensions: [http.route]
  peerAttributes: [peer.service, db.name, db.system]
`
	path := filepath.Join(t.TempDir(), "agent.yaml")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadAgentConfig(path)
	if err != nil {
		t.Fatalf("the documented service-graph YAML does not load: %v", err)
	}
	if cfg.ServiceGraph == nil || cfg.ServiceGraph.Wait != "10s" || cfg.ServiceGraph.StaleAfter != "15m" {
		t.Fatalf("serviceGraph decoded as %+v", cfg.ServiceGraph)
	}
	if err := validateConfig(*cfg, ""); err != nil {
		t.Fatalf("validateConfig rejected the documented config: %v", err)
	}
	// And the values reach the objects a real start builds.
	if got := servicegraph.NewProcessor(*cfg.ServiceGraph, slog.New(slog.DiscardHandler)).Wait(); got != 10*time.Second {
		t.Errorf("pairing window = %v, want 10s", got)
	}
}

// -check-config is only useful if it rejects everything a real start rejects,
// and both go through validateConfig. A half-configured shard set is the case
// worth naming: it reads as configured, forwards nothing, and is otherwise
// indistinguishable from the feature being off.
func TestValidateConfigRejectsBadServiceGraph(t *testing.T) {
	cases := []struct {
		name string
		cfg  agentConfig
		want string
	}{
		{
			"buckets out of order",
			agentConfig{ServiceGraph: &servicegraph.Config{HistogramBuckets: []float64{1, 0.5}}},
			"serviceGraph",
		},
		{
			"zero bucket bound",
			agentConfig{ServiceGraph: &servicegraph.Config{HistogramBuckets: []float64{0, 1}}},
			"serviceGraph",
		},
		{
			"negative wait",
			agentConfig{ServiceGraph: &servicegraph.Config{Wait: "-1s"}},
			"wait",
		},
		{
			"unparseable wait",
			agentConfig{ServiceGraph: &servicegraph.Config{Wait: "ten seconds"}},
			`serviceGraph.wait "ten seconds"`,
		},
		{
			"unparseable staleAfter",
			agentConfig{ServiceGraph: &servicegraph.Config{StaleAfter: "quarter of an hour"}},
			`serviceGraph.staleAfter "quarter of an hour"`,
		},
		{
			"negative cardinality cap",
			agentConfig{ServiceGraph: &servicegraph.Config{MaxCardinality: -1}},
			"serviceGraph",
		},
		{
			"a shard template naming no shards",
			agentConfig{ServiceGraphShards: &servicegraph.ForwardConfig{StatefulSet: "sg"}},
			"replicas",
		},
		{
			"a shard count addressing nothing",
			agentConfig{ServiceGraphShards: &servicegraph.ForwardConfig{Replicas: 3}},
			"statefulSet",
		},
		{
			"an empty explicit endpoint",
			agentConfig{ServiceGraphShards: &servicegraph.ForwardConfig{Endpoints: []string{"sg-0:4319", "  "}}},
			"endpoints[1]",
		},
		{
			"unknown shard protocol",
			agentConfig{ServiceGraphShards: &servicegraph.ForwardConfig{
				StatefulSet: "sg", Replicas: 1, Protocol: "quic",
			}},
			"protocol",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateConfig(tc.cfg, "")
			if err == nil {
				t.Fatal("validateConfig accepted an invalid service-graph config")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not name the offending field (%q)", err, tc.want)
			}
		})
	}
}

// The shard's receiver takes spans from every pod in the cluster, so it must
// never be reachable unauthenticated. The chart deliberately renders
// -service-graph WITHOUT the token flag when no Secret is configured, so this
// refusal is the guard rail its values comments promise — and it has to fire in
// validateConfig, which -check-config and every real start both run.
func TestServiceGraphShardRequiresATokenFile(t *testing.T) {
	defer restoreServiceGraphFlags(t)()
	*serviceGraphOn = true
	*serviceGraphToken = ""

	err := validateConfig(agentConfig{}, "")
	if err == nil {
		t.Fatal("-service-graph without -service-graph-token-file was accepted: the span receiver would be open to the whole cluster")
	}
	if !strings.Contains(err.Error(), "-service-graph-token-file") {
		t.Fatalf("error %q does not name the missing flag", err)
	}

	// Shape only here: whether the file exists is the real start's business.
	*serviceGraphToken = "/etc/kubescrape/service-graph/token"
	if err := validateConfig(agentConfig{}, ""); err != nil {
		t.Fatalf("rejected a shard config a real start accepts: %v", err)
	}

	// A shard with NO listener receives nothing, pairs nothing, and reports
	// ready forever — the gate clears when the receiver binds, and there is
	// nothing to bind. sgReceiver.Run refuses it, but only at the real start, so
	// -check-config has to refuse it too or it passes a config that CrashLoops
	// the StatefulSet.
	*serviceGraphListen, *serviceGraphHTTPListen = "", ""
	err = validateConfig(agentConfig{}, "")
	if err == nil {
		t.Fatal("-service-graph with neither listen address was accepted: the shard would receive nothing")
	}
	if !strings.Contains(err.Error(), "-service-graph-listen") {
		t.Fatalf("error %q does not name the missing flag", err)
	}
	// Either listener alone is enough.
	*serviceGraphHTTPListen = ":4320"
	if err := validateConfig(agentConfig{}, ""); err != nil {
		t.Fatalf("the HTTP receiver alone was rejected: %v", err)
	}
	*serviceGraphListen, *serviceGraphHTTPListen = ":4319", ""
	if err := validateConfig(agentConfig{}, ""); err != nil {
		t.Fatalf("the gRPC receiver alone was rejected: %v", err)
	}

	// And the start is where an unreadable or empty file is fatal — the dry run
	// cannot see the filesystem, so the listener must refuse to open rather
	// than come up with an empty accept set.
	*serviceGraphToken = filepath.Join(t.TempDir(), "not-there")
	p := testPipelines(t)
	if err := p.startServiceGraph(); err == nil {
		t.Fatal("the shard started with an unreadable token file")
	}
	if p.serviceGraphReg != nil {
		t.Error("the shard wired its registry before the token check")
	}
}

// The chart configures the agent half through flags alone, so the exact string
// it renders — the GOVERNING HEADLESS Service — must derive the template the
// ring expands to per-pod addresses.
func TestServiceGraphEndpointFlagDerivesTheShardTemplate(t *testing.T) {
	defer restoreServiceGraphFlags(t)()

	*serviceGraphEndpoint = "kubescrape-servicegraph.monitoring.svc:4319"
	*serviceGraphShards = 2
	cfg, err := serviceGraphShardConfig(nil)
	if err != nil {
		t.Fatalf("the chart's own endpoint was rejected: %v", err)
	}
	// One label feeds both: the StatefulSet's pods are named after it, and its
	// governing Service carries the same name by convention (and in the chart).
	if cfg.StatefulSet != "kubescrape-servicegraph" || cfg.Service != "kubescrape-servicegraph" {
		t.Errorf("statefulSet/service = %q/%q, want kubescrape-servicegraph", cfg.StatefulSet, cfg.Service)
	}
	if cfg.Namespace != "monitoring" || cfg.Port != 4319 || cfg.Replicas != 2 {
		t.Errorf("namespace/port/replicas = %q/%d/%d, want monitoring/4319/2", cfg.Namespace, cfg.Port, cfg.Replicas)
	}
	if !cfg.Enabled() {
		t.Error("the flag-only form is not Enabled(): the chart's deployment would forward nothing")
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("the flag-derived config does not validate: %v", err)
	}

	// A fully-qualified name keeps only the first two labels; shardTargets
	// re-renders the .svc suffix itself.
	*serviceGraphEndpoint = "sg.mon.svc.cluster.local:4319"
	if cfg, err = serviceGraphShardConfig(nil); err != nil {
		t.Fatalf("a fully-qualified endpoint was rejected: %v", err)
	}
	if cfg.StatefulSet != "sg" || cfg.Namespace != "mon" {
		t.Errorf("statefulSet/namespace = %q/%q, want sg/mon", cfg.StatefulSet, cfg.Namespace)
	}

	// No namespace label: resolved at start from the agent's own.
	*serviceGraphEndpoint = "sg:4319"
	if cfg, err = serviceGraphShardConfig(nil); err != nil || cfg.Namespace != "" {
		t.Fatalf("bare host: cfg.Namespace = %q, err = %v; want empty (the agent's own namespace resolves at start)", cfg.Namespace, err)
	}
}

// Half a flag pair addresses either nothing or a shard set of unknown size,
// and both forward silently into a graph that never fills.
func TestServiceGraphFlagPairsMustBeComplete(t *testing.T) {
	defer restoreServiceGraphFlags(t)()

	*serviceGraphShards = 2
	*serviceGraphEndpoint = ""
	if _, err := serviceGraphShardConfig(nil); err == nil || !strings.Contains(err.Error(), "-service-graph-endpoint") {
		t.Fatalf("a shard count with nothing to address: err = %v, want one naming -service-graph-endpoint", err)
	}

	*serviceGraphShards = 0
	*serviceGraphEndpoint = "sg.mon.svc:4319"
	if _, err := serviceGraphShardConfig(nil); err == nil || !strings.Contains(err.Error(), "-service-graph-shards") {
		t.Fatalf("an endpoint with no shard count: err = %v, want one naming -service-graph-shards", err)
	}

	// A URL would end up inside a DNS name and fail far from its cause.
	*serviceGraphShards = 2
	*serviceGraphEndpoint = "http://sg.mon.svc:4319"
	if _, err := serviceGraphShardConfig(nil); err == nil {
		t.Fatal("a URL was accepted as the shard tier's Service address")
	}
}

// The section is the richer form (a tier outside Kubernetes, TLS, tokens per
// shard); the flags are what the chart renders. Where both are set the section
// wins, or reaching for it would be impossible in exactly the deployment that
// renders the flags.
func TestServiceGraphSectionWinsOverTheFlags(t *testing.T) {
	defer restoreServiceGraphFlags(t)()
	*serviceGraphEndpoint = "kubescrape-servicegraph.monitoring.svc:4319"
	*serviceGraphShards = 2
	*serviceGraphToken = "/flag/token"

	sec := &servicegraph.ForwardConfig{
		Endpoints:       []string{"sg-a:4319", "sg-b:4319"},
		BearerTokenFile: "/section/token",
	}
	cfg, err := serviceGraphShardConfig(sec)
	if err != nil {
		t.Fatalf("merging the section with the flags: %v", err)
	}
	if len(cfg.Endpoints) != 2 || cfg.StatefulSet != "" {
		t.Errorf("explicit endpoints were overlaid with the flag template: %+v", cfg)
	}
	if cfg.BearerTokenFile != "/section/token" {
		t.Errorf("bearerTokenFile = %q, want the section's", cfg.BearerTokenFile)
	}
	// What the section leaves unset still comes from the flags.
	if cfg.Replicas != 2 {
		t.Errorf("replicas = %d, want the flag's 2", cfg.Replicas)
	}

	// And with no section at all the token flag serves both roles: the shard's
	// listener credential and the agent's.
	sec = nil
	if cfg, err = serviceGraphShardConfig(sec); err != nil || cfg.BearerTokenFile != "/flag/token" {
		t.Fatalf("bearerTokenFile = %q, err = %v; want the flag's on both sides", cfg.BearerTokenFile, err)
	}
}

// The whole hop, both transports: an agent's own exporter against the shard's
// receiver. It proves the two halves agree on the wire (gzip, the OTLP trace
// service, the `authorization: Bearer` header) and that an unauthenticated push
// reaches neither the pairing store nor an ack.
func TestServiceGraphReceiverAuthenticatesAndPairs(t *testing.T) {
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenPath, []byte("s3cr3t\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	wrongPath := filepath.Join(dir, "wrong")
	if err := os.WriteFile(wrongPath, []byte("not-it"), 0o600); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.DiscardHandler)
	tok, err := newRotatingToken(tokenPath, log)
	if err != nil {
		t.Fatal(err)
	}

	var spans atomic.Int64
	ready := make(chan struct{})
	grpcAddr, httpAddr := freeAddr(t), freeAddr(t)
	rcv := &sgReceiver{
		grpcAddr: grpcAddr,
		httpAddr: httpAddr,
		tokens:   tok.tokens,
		consume:  func(td ptrace.Traces) { spans.Add(int64(td.SpanCount())) },
		ready:    sync.OnceFunc(func() { close(ready) }),
		log:      log,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- rcv.Run(ctx) }()
	select {
	case <-ready:
	case err := <-errc:
		t.Fatalf("receiver stopped before it was ready: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("the receiver never became ready")
	}

	send := func(t *testing.T, endpoint, protocol, token string) error {
		t.Helper()
		c, err := otlpexport.New(otlpexport.Config{
			Endpoint: endpoint, Protocol: protocol, Insecure: true,
			BearerTokenFile: token, Compression: "gzip", Timeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()
		return c.ExportTraces(ctx, oneClientSpan())
	}

	if err := send(t, grpcAddr, "grpc", tokenPath); err != nil {
		t.Fatalf("an authenticated gRPC forward failed: %v", err)
	}
	if err := send(t, "http://"+httpAddr, "http", tokenPath); err != nil {
		t.Fatalf("an authenticated HTTP forward failed: %v", err)
	}
	if got := spans.Load(); got != 2 {
		t.Fatalf("pairing store saw %d spans, want 2 (one per transport)", got)
	}

	if err := send(t, grpcAddr, "grpc", wrongPath); err == nil {
		t.Error("a gRPC push with the wrong token was accepted")
	}
	if err := send(t, "http://"+httpAddr, "http", wrongPath); err == nil {
		t.Error("an HTTP push with the wrong token was accepted")
	}
	if err := send(t, grpcAddr, "grpc", ""); err == nil {
		t.Error("a gRPC push with no token at all was accepted")
	}
	if got := spans.Load(); got != 2 {
		t.Fatalf("a rejected push reached the pairing store (%d spans)", got)
	}

	cancel()
	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("Run returned %v on a cancelled shutdown, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the context was cancelled")
	}
}

// --- helpers ---

// testPipelines is the minimum a start function needs: lifecycle, logger,
// readiness and the fatal slot. Nothing here acquires anything.
func testPipelines(t *testing.T) *pipelines {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var fatal atomic.Pointer[error]
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
	return &pipelines{
		ctx: ctx, wg: &wg, stop: cancel, ready: newReadiness(),
		log: slog.New(slog.DiscardHandler), fatalErr: &fatal,
	}
}

// restoreServiceGraphFlags snapshots the package-level flags this feature
// reads and returns the restore func (they are process-global, and the other
// tests in this package read them too).
func restoreServiceGraphFlags(t *testing.T) func() {
	t.Helper()
	on, listen, httpListen := *serviceGraphOn, *serviceGraphListen, *serviceGraphHTTPListen
	token, shards, endpoint := *serviceGraphToken, *serviceGraphShards, *serviceGraphEndpoint
	return func() {
		*serviceGraphOn, *serviceGraphListen, *serviceGraphHTTPListen = on, listen, httpListen
		*serviceGraphToken, *serviceGraphShards, *serviceGraphEndpoint = token, shards, endpoint
	}
}

// freeAddr reserves a loopback port and releases it. The listeners under test
// bind their own addresses (that is what the readiness gate reports on), so a
// pre-bound listener cannot be injected.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

// oneClientSpan is the smallest payload the forwarder would ever send: one
// CLIENT span, which is half of an edge.
func oneClientSpan() ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "checkout")
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetTraceID(pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	span.SetSpanID(pcommon.SpanID{1, 2, 3, 4, 5, 6, 7, 8})
	span.SetKind(ptrace.SpanKindClient)
	span.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Unix(1, 0)))
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Unix(1, int64(50*time.Millisecond))))
	return td
}
