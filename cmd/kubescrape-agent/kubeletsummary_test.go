package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/promscrape"
)

// restoreKubeletFlags gives a test the three kubelet toggles and the endpoint
// to itself. Process-global flags, so nothing here may run in parallel — the
// reason cmd/kubescrape-agent is deliberately not t.Parallel().
func restoreKubeletFlags(t *testing.T) {
	t.Helper()
	endpoint, token, cadvisor, node, summary := *kubeletEndpoint, *kubeletToken, *cadvisorOn, *nodeOn, *summaryOn
	health, interval := *healthMetrics, *scrapeInterval
	metrics, logs := *metricsOn, *logsOn
	t.Cleanup(func() {
		*kubeletEndpoint, *kubeletToken, *cadvisorOn, *nodeOn, *summaryOn = endpoint, token, cadvisor, node, summary
		*healthMetrics, *scrapeInterval = health, interval
		*metricsOn, *logsOn = metrics, logs
	})
}

// -kubelet-summary is a PER-NODE pipeline. Missing from perNodePipelinesOff, an
// agent running only this scrape would be mistaken for the cluster-singleton
// role and would take its instance identity from a pod name that changes on
// every restart — resetting the node's whole cumulative history, the trap that
// helper's comment records.
func TestKubeletSummaryCountsAsAPerNodePipeline(t *testing.T) {
	restoreKubeletFlags(t)
	for _, p := range []*bool{logsOn, metricsOn, cadvisorOn, nodeOn, journaldOn, ingestOn, cgroupStatsOn} {
		old := *p
		*p = false
		t.Cleanup(func() { *p = old })
	}
	*summaryOn = false
	if !perNodePipelinesOff() {
		t.Fatal("every per-node pipeline is off but perNodePipelinesOff() says otherwise")
	}
	*summaryOn = true
	if perNodePipelinesOff() {
		t.Fatal("-kubelet-summary is a per-node pipeline; with it on this agent is not a cluster singleton")
	}
}

// The whole flag-to-wire path in one assertion: -kubelet-summary must reach
// KubeletConfig.Summary, count towards startScraper's kubeletScrapes predicate
// (without it the Scraper is never built at all, so an agent asked for exactly
// this pipeline runs nothing), and be scheduled on its own key.
//
// The kubelet answers 403 — the first-contact failure this pipeline actually
// has — so the request itself is the evidence and nothing downstream of the
// fetch is exercised here.
func TestKubeletSummaryIsScheduledFromItsFlag(t *testing.T) {
	restoreKubeletFlags(t)

	paths := make(chan string, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case paths <- r.URL.Path:
		default:
		}
		w.WriteHeader(http.StatusForbidden)
	}))
	t.Cleanup(srv.Close)

	// kubeletGet reads the bearer token before it dials, so a scrape with no
	// readable token file never reaches the server at all.
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("token"), 0o600); err != nil {
		t.Fatal(err)
	}

	*kubeletEndpoint, *kubeletToken = srv.URL, tokenFile
	*cadvisorOn, *nodeOn, *summaryOn = false, false, true
	*metricsOn, *logsOn = false, false
	// exportHealth needs an exporter and an attribute builder this bundle has
	// no reason to carry; the health gauges have their own coverage.
	*healthMetrics = false
	*scrapeInterval = time.Hour // one cycle, then park

	var wg sync.WaitGroup
	p := &pipelines{wg: &wg, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, cancel := context.WithCancel(context.Background())
	sc := p.startScraper(ctx)
	if sc == nil {
		t.Fatal("no Scraper was built for an agent whose only pipeline is -kubelet-summary: kubeletScrapes does not count it")
	}

	var got string
	select {
	case got = <-paths:
	case <-time.After(10 * time.Second):
		cancel()
		wg.Wait()
		t.Fatal("the kubelet was never asked for anything; -kubelet-summary reached no schedule")
	}
	cancel()
	wg.Wait()

	if got != "/stats/summary" {
		t.Fatalf("the kubelet was asked for %q, want /stats/summary", got)
	}
	// The three scrapes are independent toggles, so the two that are off must
	// not have been fetched on the same cycle.
	for {
		select {
		case extra := <-paths:
			t.Errorf("fetched %q with -cadvisor=false -node-metrics=false", extra)
			continue
		default:
		}
		break
	}
}

// A kubelet scrape enabled with no -kubelet-endpoint is the quietest failure a
// pipeline has: it is never scheduled, so nothing is attempted, nothing fails
// and no counter moves. -cadvisor and -node-metrics default to ON, so the
// commonest way in is typing nothing at all.
func TestKubeletScrapeWithoutEndpointWarns(t *testing.T) {
	restoreKubeletFlags(t)
	*kubeletEndpoint = ""

	// Every flag that asked for a kubelet scrape must be NAMED: "some kubelet
	// scrape will not run" leaves the operator to work out which, and the
	// answer differs per deployment.
	*cadvisorOn, *nodeOn, *summaryOn = true, true, true
	got := strings.Join(configWarnings(agentConfig{}), "\n")
	for _, want := range []string{"-cadvisor", "-node-metrics", "-kubelet-summary", "-kubelet-endpoint"} {
		if !strings.Contains(got, want) {
			t.Errorf("the warning does not name %s:\n%s", want, got)
		}
	}

	// Only the ones actually asked for.
	*cadvisorOn, *nodeOn, *summaryOn = false, false, true
	got = strings.Join(configWarnings(agentConfig{}), "\n")
	if !strings.Contains(got, "-kubelet-summary") {
		t.Errorf("a summary-only agent with no endpoint was not warned:\n%s", got)
	}
	if strings.Contains(got, "-cadvisor") || strings.Contains(got, "-node-metrics") {
		t.Errorf("the warning names scrapes that were not asked for:\n%s", got)
	}

	// Nothing asked for: an agent that only tails logs is a legitimate
	// deployment and must hear nothing about the kubelet.
	*cadvisorOn, *nodeOn, *summaryOn = false, false, false
	if got := strings.Join(configWarnings(agentConfig{}), "\n"); strings.Contains(got, "-kubelet-endpoint") {
		t.Errorf("warned about an endpoint no pipeline needs:\n%s", got)
	}

	// And with the endpoint set, the scrapes run — silence is the only correct
	// answer for the shipped manifests' own shape.
	*cadvisorOn, *nodeOn, *summaryOn = true, true, true
	*kubeletEndpoint = "https://10.0.0.1:10250"
	if got := strings.Join(configWarnings(agentConfig{}), "\n"); strings.Contains(got, "-kubelet-endpoint") {
		t.Errorf("warned about a -kubelet-endpoint that IS configured:\n%s", got)
	}
}

// The pipeline brought two config surfaces with it — metrics.pipelines.summary
// (the cardinality lever: two thirds of this payload is volumes) and
// resourceAttributes.pipelines.summary (the node resource only). Neither needed
// a line in validateConfig, because both compile through calls that were
// already there; this pins that "for free" rather than leaving it as a claim,
// since a surface -check-config cannot reach is one whose typo is discovered by
// a rollout.
func TestSummaryConfigSurfacesReachCheckConfig(t *testing.T) {
	drop := []promscrape.FilterRule{{Action: "drop", Metrics: `k8s\.volume\..+`}}
	prefix := "summary"

	ok := agentConfig{
		Metrics:            &promscrape.MetricsConfig{Pipelines: map[string][]promscrape.FilterRule{"summary": drop}},
		ResourceAttributes: &attrs.Config{Pipelines: map[string]*attrs.Config{"summary": {InstancePrefix: &prefix}}},
	}
	if err := validateConfig(ok, ""); err != nil {
		t.Fatalf("a valid summary pipeline was refused: %v", err)
	}

	// A typo is the whole point: an unknown pipeline name is silently no rules
	// at all, which reads as "my drop rule does not work".
	bad := agentConfig{Metrics: &promscrape.MetricsConfig{Pipelines: map[string][]promscrape.FilterRule{"summry": drop}}}
	if err := validateConfig(bad, ""); err == nil || !strings.Contains(err.Error(), "metrics.pipelines") {
		t.Errorf("a misspelled metrics pipeline was accepted: %v", err)
	}
	bad = agentConfig{ResourceAttributes: &attrs.Config{Pipelines: map[string]*attrs.Config{"summry": {InstancePrefix: &prefix}}}}
	if err := validateConfig(bad, ""); err == nil || !strings.Contains(err.Error(), "resourceAttributes") {
		t.Errorf("a misspelled resourceAttributes pipeline was accepted: %v", err)
	}
}

// -check-config's summary answers "is this what I meant?", so a pipeline
// missing from it reads as "not configured" — the one wrong answer it can give.
func TestKubeletSummaryIsInTheConfigSummary(t *testing.T) {
	restoreKubeletFlags(t)
	summary := func() string {
		var buf strings.Builder
		printConfigSummary(agentConfig{}, slog.New(slog.NewTextHandler(&buf, nil)))
		return buf.String()
	}

	*summaryOn = true
	if got := summary(); !strings.Contains(got, "summary=on") {
		t.Errorf("the config summary does not report the kubelet summary scrape:\n%s", got)
	}
	*summaryOn = false
	if got := summary(); !strings.Contains(got, "summary=off") {
		t.Errorf("a disabled pipeline is not reported as off:\n%s", got)
	}
}
