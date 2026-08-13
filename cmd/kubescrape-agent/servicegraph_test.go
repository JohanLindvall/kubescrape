package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"math"
	mathrand "math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/gzip"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/agent/servicegraph"
	"github.com/JohanLindvall/kubescrape/internal/bearer"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/metaclient"
	"github.com/JohanLindvall/kubescrape/pkg/otlpsplit"
)

// The trace tier costs a workload, an internal hop per span and a new metric
// family. None of it may start because the binary did, and the defaults are also
// the contract the chart and docs quote — the INTERNAL receiver port in
// particular, which must never coincide with the tier's own application ports.
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
		t.Errorf("-service-graph-listen defaults to %q, want :4319", *serviceGraphListen)
	}
	if *serviceGraphIv != time.Minute {
		t.Errorf("-service-graph-interval defaults to %v, want 1m", *serviceGraphIv)
	}
	// The two sides' defaults must be the SAME port. They are configured
	// independently — the receiver from -service-graph-listen, the sending shard
	// from the serviceGraphShards section's `port` — so a config naming neither
	// has the hop dialling wherever servicegraph.DefaultShardPort points.
	if _, port, err := net.SplitHostPort(*serviceGraphListen); err != nil {
		t.Errorf("-service-graph-listen %q is not host:port: %v", *serviceGraphListen, err)
	} else if port != strconv.Itoa(servicegraph.DefaultShardPort) {
		t.Errorf("the internal receiver listens on %s by default but a hop dials %d: a config naming neither port talks to the wrong one",
			port, servicegraph.DefaultShardPort)
	}
	// And the internal port must NOT be one of the application ports. A hop
	// addressed to those would be re-enriched (against a peer that is a shard)
	// and re-sharded on every pass — the amplification loop the forwarded marker
	// exists to refuse. The marker turns it into a counter; distinct defaults
	// keep it from happening at all.
	for _, app := range []string{*serviceGraphIngestGRPC, *serviceGraphIngestHTTP} {
		if app == *serviceGraphListen {
			t.Errorf("the internal receiver and an application listener share %q: an internal hop would re-enter the entry path", app)
		}
	}

	ctx, p := testPipelines(t)
	if err := p.startServiceGraph(ctx); err != nil {
		t.Fatalf("startServiceGraph with the feature off: %v", err)
	}
	if p.serviceGraphProc != nil || p.serviceGraphReg != nil {
		t.Error("the tier started with -service-graph off")
	}
	if pending := p.ready.pending(); len(pending) != 0 {
		t.Errorf("registered readiness gates with the feature off: %v", pending)
	}
	r, err := serviceGraphResharder(nil, p.log)
	if err != nil {
		t.Fatalf("building a resharder with nothing configured: %v", err)
	}
	if r != nil {
		t.Error("built a resharder with no shards configured")
	}
}

// A single-shard tier has nothing to re-shard, and must not be forced to name a
// ring to say so: -service-graph-shards=1 is a complete, valid configuration.
func TestSingleShardTierNeedsNoRing(t *testing.T) {
	defer restoreServiceGraphFlags(t)()
	*serviceGraphShards = 1
	*serviceGraphEndpoint = ""
	cfg, err := serviceGraphShardConfig(nil)
	if err != nil {
		t.Fatalf("a single-shard tier was rejected: %v", err)
	}
	if cfg.Enabled() {
		t.Error("a single-shard tier claims a shard set to address")
	}
	r, err := serviceGraphResharder(nil, slog.New(slog.DiscardHandler))
	if err != nil || r != nil {
		t.Errorf("serviceGraphResharder = %v, %v; want nil, nil", r, err)
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
		ServiceGraphShards: &servicegraph.ReshardConfig{
			StatefulSet: "kubescrape-servicegraph", Replicas: 3,
			Namespace: "monitoring", Port: 4319, Self: "kubescrape-servicegraph-1",
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
			agentConfig{ServiceGraphShards: &servicegraph.ReshardConfig{StatefulSet: "sg"}},
			"replicas",
		},
		{
			"a shard count addressing nothing",
			agentConfig{ServiceGraphShards: &servicegraph.ReshardConfig{Replicas: 3}},
			"statefulSet",
		},
		{
			"an empty explicit endpoint",
			agentConfig{ServiceGraphShards: &servicegraph.ReshardConfig{Endpoints: []string{"sg-0:4319", "  "}}},
			"endpoints[1]",
		},
		{
			"unknown shard protocol",
			agentConfig{ServiceGraphShards: &servicegraph.ReshardConfig{
				StatefulSet: "sg", Replicas: 1, Protocol: "quic",
			}},
			"protocol",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The shard ring is read ONLY by the trace tier, so that is where
			// its section is validated: off the tier the flags are empty and a
			// half-filled template would otherwise be fatal on a workload that
			// never reads it (one ConfigMap serves the DaemonSet, the
			// singleton and the tier alike). See TestShardSectionIsIgnoredOffTheTier.
			if tc.cfg.ServiceGraphShards != nil {
				tierOn(t)
			}
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
	ctx, p := testPipelines(t)
	if err := p.startServiceGraph(ctx); err == nil {
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

	sec := &servicegraph.ReshardConfig{
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

// The whole internal hop, both transports: a shard's own exporter against a
// sibling's receiver. It proves the two halves agree on the wire (gzip, the OTLP
// trace service, the `authorization: Bearer` header) and that an unauthenticated
// push reaches neither the owner chain nor an ack.
//
// The authentication is not decoration. What this port accepts is treated as
// FINAL — never enriched, never re-sharded — so an unauthenticated one would let
// anything that can reach the pod put unattributed spans straight into the
// collector under whatever resource it chose.
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
	tok, err := bearer.NewRotating(tokenPath, log)
	if err != nil {
		t.Fatal(err)
	}

	var spans atomic.Int64
	ready := make(chan struct{})
	grpcAddr, httpAddr := freeAddr(t), freeAddr(t)
	rcv := &sgReceiver{
		grpcAddr: grpcAddr,
		httpAddr: httpAddr,
		tokens:   tok.Tokens,
		consume: func(_ context.Context, td ptrace.Traces) error {
			spans.Add(int64(td.SpanCount()))
			return nil
		},
		ready: sync.OnceFunc(func() { close(ready) }),
		log:   log,
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

// --- the receive path's two roles ---

// captureTraces records what the owner chain was handed.
type captureTraces struct {
	mu  sync.Mutex
	got []ptrace.Traces
	err error
}

func (c *captureTraces) ExportTraces(_ context.Context, td ptrace.Traces) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	c.got = append(c.got, td)
	return nil
}

func (c *captureTraces) spans() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, td := range c.got {
		n += td.SpanCount()
	}
	return n
}

// A payload carrying the forwarded marker on the APPLICATION port means an
// internal hop was addressed to the wrong port. Accepting it would re-enrich it
// (against a peer that is now a kubescrape shard) and re-shard it again, on
// every pass, until the cluster's network is the incident.
//
// It must be refused PERMANENTLY: a retryable refusal would have the sending
// shard re-push the same payload forever, which is the same loop at a slower
// rate. And it must be counted, because a bounded failure nobody can see is
// still a silent one.
func TestApplicationPortRefusesAForwardedPayload(t *testing.T) {
	owner := &captureTraces{}
	resharder := servicegraph.NewResharderWithClients("sg-0", nil, 0, slog.New(slog.DiscardHandler))
	entry := &sgEntry{resharder: resharder, owner: owner}

	td := oneClientSpan()
	td.ResourceSpans().At(0).Resource().Attributes().PutBool(servicegraph.ForwardedMarker, true)

	err := entry.ExportTraces(context.Background(), td)
	if err == nil {
		t.Fatal("the application port accepted an already-re-sharded payload: an internal hop pointed here would amplify without bound")
	}
	if !otlpexport.IsPermanent(err) {
		t.Errorf("the refusal is retryable (%v); the sending shard would re-push it forever", err)
	}
	if !strings.Contains(err.Error(), servicegraph.ForwardedMarker) {
		t.Errorf("the error %q does not name the marker that caused it", err)
	}
	if owner.spans() != 0 {
		t.Error("a refused payload still reached the owner chain")
	}
	if st := resharder.Stats(); st.LoopsBlocked != 1 {
		t.Errorf("LoopsBlocked = %d, want 1 (the refused span)", st.LoopsBlocked)
	}
}

// The ordinary application push: no marker, so it is re-sharded (here a
// single-shard tier owns everything) and handed to the owner chain exactly once.
func TestApplicationPortRunsTheOwnerChainOnce(t *testing.T) {
	owner := &captureTraces{}
	entry := &sgEntry{resharder: nil, owner: owner} // single-shard tier

	if err := entry.ExportTraces(context.Background(), oneClientSpan()); err != nil {
		t.Fatalf("ExportTraces: %v", err)
	}
	if got := owner.spans(); got != 1 {
		t.Errorf("the owner chain saw %d spans, want 1", got)
	}
}

// The INTERNAL receiver is terminal: it strips the transport marker and runs the
// owner chain, and it never re-shards. The marker must not survive to the
// collector — it is kubescrape's plumbing, and it would land as a resource
// attribute on every application span in the cluster (and as a target_info
// dimension downstream).
func TestInternalReceiverStripsTheMarkerAndIsTerminal(t *testing.T) {
	owner := &captureTraces{}
	consume := ownerReceive(owner)

	td := oneClientSpan()
	td.ResourceSpans().At(0).Resource().Attributes().PutBool(servicegraph.ForwardedMarker, true)
	if err := consume(context.Background(), td); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if owner.spans() != 1 {
		t.Fatalf("the owner chain saw %d spans, want 1", owner.spans())
	}
	if servicegraph.IsForwarded(owner.got[0]) {
		t.Error("the internal marker reached the owner chain, and from there the collector")
	}
}

// A failing owner chain must reach the sender. The receiving shard holds the
// only copy of those spans once it acks, so an ack it cannot honour is a silent
// deletion — and the sender's retry is the entire recovery mechanism.
func TestOwnerChainFailurePropagates(t *testing.T) {
	owner := &captureTraces{err: errors.New("collector unreachable")}
	if err := ownerReceive(owner)(context.Background(), oneClientSpan()); err == nil {
		t.Fatal("the internal receiver acked a payload the owner chain refused")
	}
	entry := &sgEntry{owner: owner}
	if err := entry.ExportTraces(context.Background(), oneClientSpan()); err == nil {
		t.Fatal("the application port acked a payload the owner chain refused")
	}
}

// sgPairTap must feed the pairing store only after a SUCCESSFUL export: a failed
// one is retried by the application with the identical batch, and an edge
// counted before the export would be counted again on every retry.
func TestPairTapCountsOnlyAfterASuccessfulExport(t *testing.T) {
	proc := servicegraph.NewProcessor(servicegraph.Config{}, slog.New(slog.DiscardHandler))
	inner := &captureTraces{err: errors.New("nope")}
	tap := &sgPairTap{proc: proc, inner: inner}

	if err := tap.ExportTraces(context.Background(), oneClientSpan()); err == nil {
		t.Fatal("the tap swallowed an export failure")
	}
	if st := proc.Stats(); st.Items != 0 {
		t.Errorf("the pairing store took %d half-edges from a failed export; a retry would double-count them", st.Items)
	}
	inner.err = nil
	if err := tap.ExportTraces(context.Background(), oneClientSpan()); err != nil {
		t.Fatalf("ExportTraces: %v", err)
	}
	if st := proc.Stats(); st.Items != 1 {
		t.Errorf("the pairing store holds %d half-edges after one CLIENT span, want 1", st.Items)
	}
}

// --- peer-IP attribution ---

// The correctness trap of the whole topology: a peer address that resolves to
// one of the tier's own pods was rewritten in flight, and attributing an
// application's traces to a kubescrape shard would be confident, plausible and
// wrong on every span in the cluster.
func TestPeerIsOurOwnWorkload(t *testing.T) {
	self := &kubemeta.Pod{
		Name: "kubescrape-servicegraph-0", Namespace: "monitoring", UID: "uid-self",
		Owners: []kubemeta.Owner{{Kind: "StatefulSet", Name: "kubescrape-servicegraph", UID: "sts-uid"}},
	}
	sibling := &kubemeta.Pod{
		Name: "kubescrape-servicegraph-3", Namespace: "monitoring", UID: "uid-sibling",
		Owners: []kubemeta.Owner{{Kind: "StatefulSet", Name: "kubescrape-servicegraph", UID: "sts-uid"}},
	}
	app := &kubemeta.Pod{
		Name: "checkout-7d9f8c6b5d-x2k9p", Namespace: "shop", UID: "uid-app",
		Owners: []kubemeta.Owner{{Kind: "ReplicaSet", Name: "checkout-7d9f", UID: "rs-uid"},
			{Kind: "Deployment", Name: "checkout", UID: "deploy-uid"}},
	}
	// A pod in OUR namespace that is not our workload: the metadata service and
	// the DaemonSet live there too, and refusing them would be over-broad.
	neighbour := &kubemeta.Pod{
		Name: "kubescrape-agent-abcde", Namespace: "monitoring", UID: "uid-ds",
		Owners: []kubemeta.Owner{{Kind: "DaemonSet", Name: "kubescrape-agent", UID: "ds-uid"}},
	}

	p := &pipelines{selfPod: func() *kubemeta.Pod { return self }}
	for _, tc := range []struct {
		name string
		pod  *kubemeta.Pod
		want bool
	}{
		{"ourselves (SNAT back to this pod)", self, true},
		{"a sibling shard (an internal hop, or a proxy on the tier)", sibling, true},
		{"an application", app, false},
		{"another workload in our namespace", neighbour, false},
		{"nothing resolved", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := p.peerIsOurOwnWorkload(tc.pod); got != tc.want {
				t.Errorf("peerIsOurOwnWorkload = %v, want %v", got, tc.want)
			}
		})
	}

	// Before the self lookup lands — and with -self-attributes off — nothing is
	// refused. The check exists to prevent a confident lie; inventing one from a
	// lookup that has not happened would be the same mistake reversed.
	unknown := &pipelines{selfPod: func() *kubemeta.Pod { return nil }}
	if unknown.peerIsOurOwnWorkload(sibling) {
		t.Error("refused an attribution before this process knew its own pod")
	}
	if (&pipelines{}).peerIsOurOwnWorkload(sibling) {
		t.Error("refused an attribution with -self-attributes off")
	}
}

// --- helpers ---

// testPipelines is the minimum a start function needs: lifecycle, logger,
// readiness and the fatal slot. Nothing here acquires anything.
func testPipelines(t *testing.T) (context.Context, *pipelines) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var fatal atomic.Pointer[error]
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
	return ctx, &pipelines{
		wg: &wg, stop: cancel, ready: newReadiness(),
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
	// The application listeners too: they are half of what the listener
	// cross-check reads, and a test that moved one without restoring it would
	// hand every later test in this package a tier bound to the wrong ports.
	ingest, igrpc, ihttp := *serviceGraphIngest, *serviceGraphIngestGRPC, *serviceGraphIngestHTTP
	return func() {
		*serviceGraphOn, *serviceGraphListen, *serviceGraphHTTPListen = on, listen, httpListen
		*serviceGraphToken, *serviceGraphShards, *serviceGraphEndpoint = token, shards, endpoint
		*serviceGraphIngest, *serviceGraphIngestGRPC, *serviceGraphIngestHTTP = ingest, igrpc, ihttp
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

// --- exactly once, across the internal hop ---

// The guarantee the whole two-hop shape has to hold: every pushed span reaches
// exactly ONE owner chain. Counting a span on the entry shard as well as on the
// owner would inflate the graph and the RED metrics by one copy per hop, and
// counting it on neither would lose it outright.
func TestEverySpanReachesExactlyOneOwnerChain(t *testing.T) {
	// The sibling shard: its "client" is really the sibling's own owner chain,
	// reached over the wire in production and directly here.
	siblingOwner := &captureTraces{}
	sibling := &siblingShard{owner: siblingOwner}
	resharder := servicegraph.NewResharderWithClients("shard-0",
		map[string]servicegraph.TracesExporter{"shard-1": sibling}, 0, slog.New(slog.DiscardHandler))

	localOwner := &captureTraces{}
	entry := &sgEntry{resharder: resharder, owner: localOwner}

	// Enough distinct traces that the ring splits them across both shards.
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "checkout")
	ss := rs.ScopeSpans().AppendEmpty()
	const traces = 200
	for i := 0; i < traces; i++ {
		sp := ss.Spans().AppendEmpty()
		sp.SetTraceID(pcommon.TraceID{byte(i), byte(i >> 8), 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 1})
		sp.SetSpanID(pcommon.SpanID{byte(i), 2, 3, 4, 5, 6, 7, 8})
		sp.SetKind(ptrace.SpanKindClient)
	}
	if err := entry.ExportTraces(context.Background(), td); err != nil {
		t.Fatalf("ExportTraces: %v", err)
	}

	if localOwner.spans() == 0 || siblingOwner.spans() == 0 {
		t.Fatalf("the ring did not split: local=%d sibling=%d", localOwner.spans(), siblingOwner.spans())
	}
	if got := localOwner.spans() + siblingOwner.spans(); got != traces {
		t.Errorf("owner chains saw %d spans in total, want exactly %d", got, traces)
	}
	// And the sibling's owner chain saw the marker stripped, exactly as the
	// internal receiver does it.
	for _, batch := range siblingOwner.got {
		if servicegraph.IsForwarded(batch) {
			t.Fatal("the sibling's owner chain saw kubescrape's internal marker")
		}
	}
	// The internal hop is a real hop: its failure must fail the push, or the
	// entry shard would ack spans it is the only copy of.
	sibling.err = errors.New("shard down")
	if err := entry.ExportTraces(context.Background(), td); err == nil {
		t.Fatal("the entry shard acked a push whose internal hop failed; those spans have no other copy")
	}
}

// siblingShard stands in for the network plus the sibling's internal receiver:
// it does what sgReceiver does with what it is handed.
type siblingShard struct {
	owner *captureTraces
	err   error
}

func (s *siblingShard) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	if s.err != nil {
		return s.err
	}
	if !servicegraph.IsForwarded(td) {
		return errors.New("an internal hop arrived without the forwarded marker")
	}
	return ownerReceive(s.owner)(ctx, td)
}

// THE DRIFT the shared body reader resolves: this receiver had its own copy of
// the OTLP/HTTP body reader, and the fix that makes an over-cap GZIP report 413
// landed only in otlpingest's. The copy wrapped the compressed body in an
// io.LimitReader — the exact shape that fix replaced — so truncating an
// oversized gzip at the cap produced `unexpected EOF` from the decompressor and
// answered 400 "malformed OTLP traces payload" for a payload that was merely
// too big.
//
// 400 is not a cosmetic difference on THIS hop. The sender is another
// kubescrape: otlpexport.IsPermanent reads 400 as a definitive rejection, so
// the sending shard drops the batch (and the application's spans with it)
// instead of surfacing something its own retry or split could act on. 413 is
// the honest answer, and it is what both receivers give now.
func TestServiceGraphHTTPOversizedGzipIs413(t *testing.T) {
	rcv := &sgReceiver{
		httpAddr: freeAddr(t),
		tokens:   func() []string { return []string{"s3cr3t"} },
		consume:  func(context.Context, ptrace.Traces) error { return nil },
		log:      slog.New(slog.DiscardHandler),
	}
	ready := make(chan struct{})
	rcv.ready = sync.OnceFunc(func() { close(ready) })
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

	post := func(body []byte) int {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, "http://"+rcv.httpAddr+"/v1/traces", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set("Content-Encoding", "gzip")
		req.Header.Set("Authorization", "Bearer s3cr3t")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode
	}

	// Incompressible data: the COMPRESSED body itself exceeds the cap, so the
	// reader's own truncation is what the decompressor trips over.
	raw := make([]byte, sgMaxRecvBytes()+(1<<20))
	rnd := mathrand.New(mathrand.NewSource(1))
	_, _ = rnd.Read(raw)
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write(raw)
	_ = zw.Close()
	if buf.Len() <= sgMaxRecvBytes() {
		t.Fatalf("test payload compressed to %d bytes, below the cap", buf.Len())
	}
	if code := post(buf.Bytes()); code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized compressed body status = %d, want 413 (400 tells a kubescrape sender to drop the batch)", code)
	}

	// Zip bomb: tiny compressed, decompresses beyond the cap. This one was
	// already 413 and must stay so.
	var bomb bytes.Buffer
	zw = gzip.NewWriter(&bomb)
	_, _ = zw.Write(make([]byte, sgMaxRecvBytes()+2))
	_ = zw.Close()
	if code := post(bomb.Bytes()); code != http.StatusRequestEntityTooLarge {
		t.Errorf("zip-bomb status = %d, want 413", code)
	}

	// An unsupported media type stays 415, and a genuinely malformed payload
	// stays 400: the fix must not turn every read failure into 413.
	req, _ := http.NewRequest(http.MethodPost, "http://"+rcv.httpAddr+"/v1/traces", bytes.NewReader([]byte("x")))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer s3cr3t")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Errorf("wrong Content-Type status = %d, want 415", resp.StatusCode)
	}

	cancel()
	<-errc
}

// One ConfigMap is shared by the DaemonSet, the events/Azure singleton and the
// trace tier. A `serviceGraphShards:` section is read ONLY by the tier, so a
// section that is valid where it is read must not be fatal where it is not:
// off the tier the shard FLAGS are empty, and a section supplying one half of
// the template (replicas without statefulSet, or the reverse) tripped the
// half-filled-template refusal on every workload that never reads it —
// CrashLoopBackOff for logs, scraped metrics and cadvisor across the fleet.
func TestShardSectionIsIgnoredOffTheTier(t *testing.T) {
	old := *serviceGraphOn
	*serviceGraphOn = false
	t.Cleanup(func() { *serviceGraphOn = old })

	for _, tc := range []struct {
		name  string
		shard *servicegraph.ReshardConfig
	}{
		{"template naming no shards", &servicegraph.ReshardConfig{StatefulSet: "sg"}},
		{"count addressing nothing", &servicegraph.ReshardConfig{Replicas: 3}},
		{"unknown protocol", &servicegraph.ReshardConfig{StatefulSet: "sg", Replicas: 1, Protocol: "quic"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateConfig(agentConfig{ServiceGraphShards: tc.shard}, ""); err != nil {
				t.Fatalf("a tier-only section was fatal on a workload that never reads it: %v", err)
			}
		})
	}

	// ...and it is still refused ON the tier, where it IS read.
	tierOn(t)
	if err := validateConfig(agentConfig{ServiceGraphShards: &servicegraph.ReshardConfig{StatefulSet: "sg"}}, ""); err == nil {
		t.Error("the tier must still refuse a half-filled shard template")
	}
}

// tierOn puts the process in trace-tier shape for the duration of the test:
// the flag plus the two things the tier refuses to start without.
func tierOn(t *testing.T) {
	t.Helper()
	on, tok := *serviceGraphOn, *serviceGraphToken
	*serviceGraphOn, *serviceGraphToken = true, "/flag/token"
	t.Cleanup(func() { *serviceGraphOn, *serviceGraphToken = on, tok })
}

// The internal hop's receive floor is derived with the sender's default split
// cap as a lower bound (sgMaxRecvFloor), so raising otlpsplit.DefaultMaxBytes
// can never silently out-size what a sibling shard will accept. Today the
// historical 4 MiB dominates; this pin makes any future change of the FLOOR's
// value a deliberate test update rather than a side effect.
func TestSGMaxRecvFloorValue(t *testing.T) {
	if sgMaxRecvFloor != 4<<20 {
		t.Fatalf("sgMaxRecvFloor = %d, want %d (4 MiB): if otlpsplit.DefaultMaxBytes grew past 4 MiB this is expected — update the pin deliberately", sgMaxRecvFloor, 4<<20)
	}
	if sgMaxRecvFloor < otlpsplit.DefaultMaxBytes {
		t.Fatalf("sgMaxRecvFloor (%d) is below the sender's default split cap (%d): the internal hop would reject default-sized parts", sgMaxRecvFloor, otlpsplit.DefaultMaxBytes)
	}
}

// A negative -otlp-max-send-bytes disables splitting, so the sending shard
// forwards whole enriched payloads with no cap of its own; the internal hop's
// receive cap is disabled with it. Reading the negative form as "use the
// floor" turned every over-floor share into a deterministic ring rejection
// that the application's SDK then retried until its budget dropped the spans.
func TestSGMaxRecvBytesNegativeDisablesTheCapWithTheSplitting(t *testing.T) {
	old := *otlpMaxSendBytes
	defer func() { *otlpMaxSendBytes = old }()

	*otlpMaxSendBytes = -1
	if got := sgMaxRecvBytes(); got != math.MaxInt32 {
		t.Fatalf("sgMaxRecvBytes() with splitting disabled = %d, want unlimited (MaxInt32): a finite cap under an uncapped sender re-creates the rejection", got)
	}
	*otlpMaxSendBytes = 0
	if got := sgMaxRecvBytes(); got != sgMaxRecvFloor {
		t.Fatalf("sgMaxRecvBytes() at the default = %d, want the floor %d", got, sgMaxRecvFloor)
	}
	*otlpMaxSendBytes = 8 << 20
	if got := sgMaxRecvBytes(); got != 8<<20 {
		t.Fatalf("sgMaxRecvBytes() = %d, want the raised flag value 8 MiB", got)
	}
}

// --- the loop guard's position in the receive path ---

// The marker's refusal has to be reached BEFORE enrichment, and this is the
// assertion that says so: a marked payload must cost the metadata service
// nothing.
//
// Enrichment is one lookup per resource and one
// kubescrape_ingest_resources_total tick per resource, spent on traffic that is
// then thrown away — and on this port what it would deduce is wrong twice over,
// since the connection's peer is the sibling SHARD that misaddressed the hop,
// not an application. The refusal must also stay PERMANENT: a retryable one has
// the sending shard re-push it forever, which is the amplification loop at a
// slower rate.
//
// It runs through startServiceGraphIngest — the production wiring — because
// what regresses is the hook being SET, not the guard's verdict.
func TestApplicationPortRefusesAForwardedPayloadAboveEnrichment(t *testing.T) {
	var lookups atomic.Int64
	metaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lookups.Add(1)
		http.NotFound(w, r) // resolving is irrelevant; being ASKED is the signal
	}))
	defer metaSrv.Close()

	defer func(on bool, g, h string, shards int, ep string) {
		*serviceGraphIngest, *serviceGraphIngestGRPC, *serviceGraphIngestHTTP = on, g, h
		*serviceGraphShards, *serviceGraphEndpoint = shards, ep
	}(*serviceGraphIngest, *serviceGraphIngestGRPC, *serviceGraphIngestHTTP, *serviceGraphShards, *serviceGraphEndpoint)
	*serviceGraphIngest = true
	*serviceGraphIngestGRPC, *serviceGraphIngestHTTP = freeAddr(t), freeAddr(t)
	*serviceGraphShards, *serviceGraphEndpoint = 0, "" // a single-shard tier: nothing to re-shard

	builders, err := buildAttrs(nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, p := testPipelines(t)
	p.meta = metaclient.New(metaclient.Config{Base: metaSrv.URL, Timeout: 5 * time.Second})
	p.attrBuilders = builders

	owner := &captureTraces{}
	if err := p.startServiceGraphIngest(ctx, owner); err != nil {
		t.Fatalf("startServiceGraphIngest: %v", err)
	}

	conn, err := grpc.NewClient(*serviceGraphIngestGRPC, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	grpcClient := ptraceotlp.NewGRPCClient(conn)

	sendGRPC := func(t *testing.T, td ptrace.Traces) error {
		t.Helper()
		var lastErr error
		for i := 0; i < 100; i++ {
			_, lastErr = grpcClient.Export(context.Background(), ptraceotlp.NewExportRequestFromTraces(td))
			if status.Code(lastErr) != codes.Unavailable {
				return lastErr // bound: this is the server's own answer
			}
			time.Sleep(20 * time.Millisecond)
		}
		return lastErr
	}
	postHTTP := func(t *testing.T, td ptrace.Traces) int {
		t.Helper()
		body, err := ptraceotlp.NewExportRequestFromTraces(td).MarshalProto()
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 100; i++ {
			resp, err := http.Post("http://"+*serviceGraphIngestHTTP+"/v1/traces", "application/x-protobuf", bytes.NewReader(body))
			if err != nil {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			defer func() { _ = resp.Body.Close() }()
			return resp.StatusCode
		}
		t.Fatal("the application HTTP listener never answered")
		return 0
	}

	// A container id the enricher WOULD look up, so a lookup count of zero is
	// evidence of the ordering rather than of an idless payload.
	marked := func() ptrace.Traces {
		td := oneClientSpan()
		a := td.ResourceSpans().At(0).Resource().Attributes()
		a.PutStr("container.id", "cafe01")
		a.PutBool(servicegraph.ForwardedMarker, true)
		return td
	}

	if err := sendGRPC(t, marked()); status.Code(err) != codes.InvalidArgument {
		t.Errorf("gRPC refusal = %v (%v), want InvalidArgument: anything else has the sending shard re-push it forever", status.Code(err), err)
	} else if !strings.Contains(err.Error(), servicegraph.ForwardedMarker) {
		t.Errorf("the refusal %q does not name the marker that caused it", err)
	}
	if code := postHTTP(t, marked()); code != http.StatusBadRequest {
		t.Errorf("HTTP refusal status = %d, want 400 (otlpexport.IsPermanent reads it as do-not-retry)", code)
	}
	if n := lookups.Load(); n != 0 {
		t.Errorf("a refused payload cost %d metadata lookups; the guard runs above enrichment precisely so it costs none", n)
	}
	if owner.spans() != 0 {
		t.Errorf("a refused payload reached the owner chain (%d spans)", owner.spans())
	}

	// The same payload unmarked: accepted, and the enricher IS consulted — which
	// is what makes the zero above mean an ordering and not a dead enricher.
	unmarked := func() ptrace.Traces {
		td := oneClientSpan()
		td.ResourceSpans().At(0).Resource().Attributes().PutStr("container.id", "cafe01")
		return td
	}
	if err := sendGRPC(t, unmarked()); err != nil {
		t.Fatalf("an ordinary application push over gRPC failed: %v", err)
	}
	if code := postHTTP(t, unmarked()); code != http.StatusOK {
		t.Fatalf("an ordinary application push over HTTP answered %d", code)
	}
	if lookups.Load() == 0 {
		t.Error("an accepted push consulted no metadata service; the refusal assertion above proves nothing")
	}
	if owner.spans() != 2 {
		t.Errorf("the owner chain saw %d spans, want 2 (one per transport)", owner.spans())
	}
}

// The internal port is the OTHER half of the marker's contract: what a sibling
// shard forwards arrives marked BY DESIGN, so this port strips it and accepts.
// Refusing here would break re-sharding outright, and letting the marker survive
// would put a kubescrape resource attribute on every application span in the
// cluster.
func TestInternalPortStripsTheMarkerAndAccepts(t *testing.T) {
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte("s3cr3t\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.DiscardHandler)
	tok, err := bearer.NewRotating(tokenPath, log)
	if err != nil {
		t.Fatal(err)
	}

	owner := &captureTraces{}
	ready := make(chan struct{})
	rcv := &sgReceiver{
		grpcAddr: freeAddr(t),
		httpAddr: freeAddr(t),
		tokens:   tok.Tokens,
		consume:  ownerReceive(owner), // the production callback
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

	send := func(t *testing.T, endpoint, protocol string) {
		t.Helper()
		c, err := otlpexport.New(otlpexport.Config{
			Endpoint: endpoint, Protocol: protocol, Insecure: true,
			BearerTokenFile: tokenPath, Compression: "gzip", Timeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()
		td := oneClientSpan()
		td.ResourceSpans().At(0).Resource().Attributes().PutBool(servicegraph.ForwardedMarker, true)
		if err := c.ExportTraces(ctx, td); err != nil {
			t.Fatalf("a marked %s forward to the internal port was refused: %v", protocol, err)
		}
	}
	send(t, rcv.grpcAddr, "grpc")
	send(t, "http://"+rcv.httpAddr, "http")

	if owner.spans() != 2 {
		t.Fatalf("the owner chain saw %d spans, want 2 (one per transport)", owner.spans())
	}
	for _, batch := range owner.got {
		if servicegraph.IsForwarded(batch) {
			t.Error("the internal marker reached the owner chain, and from there the collector")
		}
	}

	cancel()
	<-errc
}
