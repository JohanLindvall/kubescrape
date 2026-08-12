package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/cgroupstats"
	"github.com/JohanLindvall/kubescrape/internal/agent/promscrape"
)

// restoreCgroupFlags restores the flag values a test overwrites in place.
func restoreCgroupFlags(t *testing.T) {
	t.Helper()
	on, iv, root, scrape, div := *cgroupStatsOn, *cgroupStatsIv, *cgroupRoot, *scrapeInterval, *cgroupDiscoverIv
	t.Cleanup(func() {
		*cgroupStatsOn, *cgroupStatsIv, *cgroupRoot, *scrapeInterval = on, iv, root, scrape
		*cgroupDiscoverIv = div
	})
}

// The sampler's ticker period. A non-positive value panics time.NewTicker, and
// the panic would land where the pipeline STARTS rather than where the flag was
// typed — so -check-config has to catch it.
func TestCgroupStatsIntervalMustBePositive(t *testing.T) {
	for _, v := range []time.Duration{0, -time.Second} {
		restoreCgroupFlags(t)
		typedFlags(t, "cgroup-stats-interval")
		*cgroupStatsIv = v
		err := validateConfig(agentConfig{}, "")
		if err == nil {
			t.Fatalf("accepted -cgroup-stats-interval=%s", v)
		}
		for _, want := range []string{"-cgroup-stats-interval", "positive duration"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not name %q", err, want)
			}
		}
	}
}

// The floor, which exists only in the direction that burns the NODE: one sweep
// is three cgroup reads per container, so a period ten times below the floor is
// a busy loop on the process that tails every log file here — bought for burst
// resolution no export window can carry.
func TestCgroupStatsIntervalHasAFloor(t *testing.T) {
	for _, v := range []time.Duration{time.Millisecond, cgroupstats.MinInterval - time.Nanosecond} {
		restoreCgroupFlags(t)
		typedFlags(t, "cgroup-stats-interval")
		*cgroupStatsIv = v
		err := validateConfig(agentConfig{}, "")
		if err == nil {
			t.Fatalf("accepted -cgroup-stats-interval=%s, below the %s floor", v, cgroupstats.MinInterval)
		}
		if !strings.Contains(err.Error(), cgroupstats.MinInterval.String()) {
			t.Errorf("error %q does not name the floor", err)
		}
	}
	// The floor itself, and the default, are legal.
	for _, v := range []time.Duration{cgroupstats.MinInterval, cgroupstats.DefaultInterval} {
		restoreCgroupFlags(t)
		typedFlags(t, "cgroup-stats-interval")
		*cgroupStatsIv = v
		if err := validateConfig(agentConfig{}, ""); err != nil {
			t.Fatalf("refused -cgroup-stats-interval=%s: %v", v, err)
		}
	}
}

// Above the floor the value is legal, but three reads per container per period
// is a cost an operator should have chosen deliberately rather than discovered
// on a node's CPU graph.
func TestCgroupStatsCostlyIntervalWarns(t *testing.T) {
	restoreCgroupFlags(t)
	*cgroupStatsOn = true
	*cgroupStatsIv, *scrapeInterval = 200*time.Millisecond, 30*time.Second
	got := warnText(agentConfig{})
	if !strings.Contains(got, "-cgroup-stats-interval=200ms") {
		t.Fatalf("the warning must name the value: %q", got)
	}
	if !strings.Contains(got, "reads a second") {
		t.Fatalf("the warning must say what it costs: %q", got)
	}

	// The default is silent, or the warning becomes noise.
	*cgroupStatsIv = cgroupstats.DefaultInterval
	if got := warnText(agentConfig{}); strings.Contains(got, "reads a second") {
		t.Fatalf("the default sampling period warned about its cost: %q", got)
	}
	// And nothing is said when the pipeline is off.
	*cgroupStatsOn, *cgroupStatsIv = false, 150*time.Millisecond
	if got := warnText(agentConfig{}); strings.Contains(got, "cgroup-stats-interval") {
		t.Fatalf("warned about a pipeline that is off: %q", got)
	}
}

// A relative root resolves against the container's working directory, which in
// a distroless image is "/" — so it would find nothing while reporting no
// error, which is precisely the outcome this pipeline is built never to produce
// quietly.
func TestCgroupStatsRootMustBeAbsolute(t *testing.T) {
	restoreCgroupFlags(t)
	typedFlags(t, "cgroup-stats-root")
	*cgroupRoot = "sys/fs/cgroup"
	err := validateConfig(agentConfig{}, "")
	if err == nil {
		t.Fatal("accepted a relative -cgroup-stats-root")
	}
	if !strings.Contains(err.Error(), "absolute") {
		t.Errorf("error %q does not say what is wrong", err)
	}

	// The empty default means autodetect and must stay legal even when typed.
	*cgroupRoot = ""
	if err := validateConfig(agentConfig{}, ""); err != nil {
		t.Fatalf("refused the autodetect sentinel: %v", err)
	}
	*cgroupRoot = "/host/sys/fs/cgroup"
	if err := validateConfig(agentConfig{}, ""); err != nil {
		t.Fatalf("refused an absolute root: %v", err)
	}
}

// The refusals read what an OPERATOR typed, like every other one in
// checkFlagValues: a value arriving programmatically is the constructor's to
// normalise (cgroupstats.New defaults a non-positive interval).
func TestCgroupStatsRefusalsAreTypedOnly(t *testing.T) {
	restoreCgroupFlags(t)
	typedFlags(t) // nothing typed
	*cgroupStatsIv, *cgroupRoot = 0, "relative/path"
	if err := validateConfig(agentConfig{}, ""); err != nil {
		t.Fatalf("refused values no operator typed: %v", err)
	}
}

// A sampling period at or near the export window degenerates the distribution
// into the average the cadvisor scrape already publishes — legal, but it
// silently undoes the reason the pipeline was enabled, so it is named.
func TestCgroupStatsIntervalNearTheWindowWarns(t *testing.T) {
	restoreCgroupFlags(t)
	*cgroupStatsOn = true
	*cgroupStatsIv, *scrapeInterval = 15*time.Second, 30*time.Second
	got := warnText(agentConfig{})
	if !strings.Contains(got, "-cgroup-stats-interval=15s") {
		t.Fatalf("the warning must name the value: %q", got)
	}
	if !strings.Contains(got, "TWO readings") {
		t.Fatalf("the warning must say why it matters: %q", got)
	}

	// The stock arrangement (1s inside 30s) is silent, or the warning becomes
	// noise an operator learns to skip past.
	*cgroupStatsIv = time.Second
	if got := warnText(agentConfig{}); strings.Contains(got, "cgroup-stats-interval") {
		t.Fatalf("the default sampling rate warned: %q", got)
	}

	// And nothing is said when the pipeline is off — one ConfigMap and one flag
	// set are shared across workloads that do not run it.
	*cgroupStatsOn, *cgroupStatsIv = false, time.Minute
	if got := warnText(agentConfig{}); strings.Contains(got, "cgroup-stats-interval") {
		t.Fatalf("warned about a pipeline that is off: %q", got)
	}
}

// The discovery ticker's period. time.NewTicker PANICS on a non-positive one,
// and this flag has one extra trap the sampling period does not: discovery is
// the ONLY way a container enters the sampled set, so "0 = never re-scan" would
// be a plausible reading that silently freezes the container set at whatever
// the first pass found.
func TestCgroupStatsDiscoverIntervalMustBePositive(t *testing.T) {
	for _, v := range []time.Duration{0, -time.Second} {
		restoreCgroupFlags(t)
		typedFlags(t, "cgroup-stats-discover-interval")
		*cgroupDiscoverIv = v
		err := validateConfig(agentConfig{}, "")
		if err == nil {
			t.Fatalf("accepted -cgroup-stats-discover-interval=%s", v)
		}
		for _, want := range []string{"-cgroup-stats-discover-interval", "positive duration"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not name %q", err, want)
			}
		}
	}
}

// The floor, which is about the METADATA SERVICE rather than this node: every
// pass re-offers each unresolved cgroup, and on a fresh node that is one per pod
// (the sandbox, which never resolves) for the first few minutes.
func TestCgroupStatsDiscoverIntervalHasAFloor(t *testing.T) {
	for _, v := range []time.Duration{time.Millisecond, cgroupstats.MinDiscoverInterval - time.Nanosecond} {
		restoreCgroupFlags(t)
		typedFlags(t, "cgroup-stats-discover-interval")
		*cgroupDiscoverIv = v
		err := validateConfig(agentConfig{}, "")
		if err == nil {
			t.Fatalf("accepted -cgroup-stats-discover-interval=%s, below the %s floor", v, cgroupstats.MinDiscoverInterval)
		}
		if !strings.Contains(err.Error(), cgroupstats.MinDiscoverInterval.String()) {
			t.Errorf("error %q does not name the floor", err)
		}
	}
	for _, v := range []time.Duration{cgroupstats.MinDiscoverInterval, cgroupstats.DefaultDiscoverInterval, time.Minute} {
		restoreCgroupFlags(t)
		typedFlags(t, "cgroup-stats-discover-interval")
		*cgroupDiscoverIv = v
		if err := validateConfig(agentConfig{}, ""); err != nil {
			t.Fatalf("refused -cgroup-stats-discover-interval=%s: %v", v, err)
		}
	}
}

// Legal above the floor, but the cost lands on the metadata service and is
// multiplied by every node in the fleet, so it is named.
func TestCgroupStatsFastDiscoveryWarns(t *testing.T) {
	restoreCgroupFlags(t)
	*cgroupStatsOn = true
	*cgroupDiscoverIv = 2 * time.Second
	got := warnText(agentConfig{})
	if !strings.Contains(got, "-cgroup-stats-discover-interval=2s") {
		t.Fatalf("the warning must name the value: %q", got)
	}
	if !strings.Contains(got, "lookups a second") {
		t.Fatalf("the warning must say what it costs and where: %q", got)
	}

	// The default is silent, or the warning becomes noise.
	*cgroupDiscoverIv = cgroupstats.DefaultDiscoverInterval
	if got := warnText(agentConfig{}); strings.Contains(got, "discover-interval") {
		t.Fatalf("the default discovery cadence warned about its cost: %q", got)
	}
	// And nothing is said when the pipeline is off — one flag set is shared
	// across workloads that do not run it.
	*cgroupStatsOn, *cgroupDiscoverIv = false, time.Second
	if got := warnText(agentConfig{}); strings.Contains(got, "discover-interval") {
		t.Fatalf("warned about a pipeline that is off: %q", got)
	}
}

// The flag has to reach the sampler, and the startup line has to report the
// EFFECTIVE period rather than what was typed: a value the constructor clamps
// otherwise leaves the line that says what the pipeline is doing saying
// something else.
func TestCgroupStatsDiscoverIntervalReachesTheSampler(t *testing.T) {
	restoreCgroupFlags(t)
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "cgroup.controllers"), []byte("cpu memory\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	*cgroupStatsOn, *cgroupRoot = true, root
	*cgroupDiscoverIv, *cgroupStatsIv = 7*time.Second, time.Second

	var buf bytes.Buffer
	var wg sync.WaitGroup
	p := &pipelines{log: slog.New(slog.NewTextHandler(&buf, nil)), wg: &wg}
	ctx, cancel := context.WithCancel(context.Background())
	if err := p.startCgroupStats(ctx, nil); err != nil {
		t.Fatalf("startCgroupStats: %v", err)
	}
	cancel()
	wg.Wait()
	if p.cgroupSampler == nil {
		t.Fatal("no sampler was published")
	}
	if got := p.cgroupSampler.DiscoverInterval(); got != 7*time.Second {
		t.Errorf("the sampler discovers every %s; -cgroup-stats-discover-interval=7s never reached it", got)
	}
	if !strings.Contains(buf.String(), "discoverInterval=7s") {
		t.Errorf("the startup line does not report the effective discovery interval:\n%s", buf.String())
	}
}

// The sampler must resolve container identity through the SAME code the
// cadvisor scrape does, so its ten gauges join the series they explain. The
// wiring that guarantees it is this interface satisfaction — a compile-time
// assertion, because the alternative (a second resolver) would be a runtime
// divergence nobody notices until the join silently returns nothing.
func TestScraperSatisfiesTheCgroupResolver(t *testing.T) {
	var _ cgroupstats.Resolver = (*promscrape.Scraper)(nil)
}

// An explicitly configured root that is not a cgroup v2 hierarchy is an
// operator error — identical on every node, fixable by editing one value — and
// stays FATAL. (Its sibling, a node that simply runs cgroup v1, degrades to
// "this pipeline off, everything else running"; the two classifications are
// decided in cgroupstats.New and pinned by
// TestUnsupportedNodeIsDistinguishableFromAnOperatorError.)
func TestCgroupStatsFatalOnAnExplicitlyWrongRoot(t *testing.T) {
	restoreCgroupFlags(t)
	root := t.TempDir()
	for _, c := range []string{"cpu", "cpuacct", "memory"} { // the v1 shape
		if err := os.MkdirAll(filepath.Join(root, c), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	*cgroupStatsOn, *cgroupRoot = true, root
	p := &pipelines{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	err := p.startCgroupStats(context.Background(), nil)
	if err == nil {
		t.Fatal("a -cgroup-stats-root that is not a cgroup v2 hierarchy started anyway")
	}
	if errors.Is(err, cgroupstats.ErrUnsupportedNode) {
		t.Errorf("classified as a node property: an operator's own value must not be quietly dropped, it must be refused: %v", err)
	}
	if p.cgroupSampler != nil {
		t.Error("a failed sampler was published for the shutdown path to export")
	}
}

// The -check-config summary must not claim this pipeline is ON.
//
// It is the one pipeline a NODE can refuse: cgroupstats.New reports
// ErrUnsupportedNode on a cgroup v1 host (or one with no /sys/fs/cgroup mounted
// into the pod), and startCgroupStats then disables it alone and keeps every
// other pipeline running. The summary reads flags and probes nothing —
// deliberately, and the node's cgroup version is not knowable from a dry run
// anyway — so "cgroupStats=on" was this line reporting a pipeline as running
// where it may never start, on the very node where somebody would run
// -check-config to find out why the ten gauges are missing.
func TestConfigSummaryReportsCgroupStatsAsARequestNotAsRunning(t *testing.T) {
	restoreCgroupFlags(t)
	summary := func() string {
		var buf bytes.Buffer
		printConfigSummary(agentConfig{}, slog.New(slog.NewTextHandler(&buf, nil)))
		return buf.String()
	}

	*cgroupStatsOn = true
	got := summary()
	if strings.Contains(got, "cgroupStats=on") {
		t.Errorf("the summary reports cgroupStats=on, which a cgroup v1 node makes false — and it is exactly the node an operator runs -check-config on:\n%s", got)
	}
	if !strings.Contains(got, "cgroupStats=requested") {
		t.Errorf("the summary does not report the request either:\n%s", got)
	}

	// Off is off: there is nothing conditional about a flag nobody set.
	*cgroupStatsOn = false
	if got := summary(); !strings.Contains(got, "cgroupStats=off") {
		t.Errorf("a disabled pipeline is not reported as off:\n%s", got)
	}
}

// -cgroup-stats is a PER-NODE pipeline. If it were missing from
// perNodePipelinesOff, an agent running only the sampler would be mistaken for
// the cluster-singleton role and would take its instance identity from a pod
// name that changes on every restart — resetting the node's whole cumulative
// history, which is the exact trap that helper's comment records.
func TestCgroupStatsCountsAsAPerNodePipeline(t *testing.T) {
	restoreCgroupFlags(t)
	offs := []*bool{logsOn, metricsOn, cadvisorOn, nodeOn, journaldOn, ingestOn}
	for _, p := range offs {
		old := *p
		*p = false
		t.Cleanup(func() { *p = old })
	}
	*cgroupStatsOn = false
	if !perNodePipelinesOff() {
		t.Fatal("every per-node pipeline is off but perNodePipelinesOff() says otherwise")
	}
	*cgroupStatsOn = true
	if perNodePipelinesOff() {
		t.Fatal("-cgroup-stats is a per-node pipeline; with it on this agent is not a cluster singleton")
	}
}
