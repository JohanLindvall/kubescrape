package promscrape

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// MetricExporter sends one OTLP metrics payload.
type MetricExporter interface {
	ExportMetrics(ctx context.Context, md pmetric.Metrics) error
}

// TargetSource lists the scrape targets for a node; implemented by
// metaclient.Client.
type TargetSource interface {
	NodeTargets(ctx context.Context, node string) ([]kubemeta.ScrapeTarget, error)
}

// AuthSource resolves scrape-auth secret references ("ns/name/key").
type AuthSource interface {
	ScrapeAuth(ctx context.Context, ref string) (string, error)
}

// Config configures the scraper.
type Config struct {
	Node        string
	Interval    time.Duration
	Timeout     time.Duration // per-target scrape timeout
	Concurrency int           // concurrent target scrapes
	BatchPoints int           // flush to the exporter after this many data points
	// BatchBytes flushes a chunk once its estimated OTLP size reaches this
	// many bytes, whichever limit BatchPoints or BatchBytes hits first (0 =
	// the default; negative disables the byte bound). A collector's default
	// gRPC receive limit is 4 MiB on the DECOMPRESSED message, and BatchPoints
	// alone does not bound bytes: 10k points of a label-rich family marshal to
	// over 5 MiB, which the collector rejects wholesale — every export of that
	// target fails and all of its metrics are lost.
	BatchBytes   int
	MaxLineBytes int // skip exposition lines longer than this
	MaxSamples   int // abort a single scrape beyond this many samples (0 = unlimited)
	// Exemplars negotiates the OpenMetrics format and attaches exemplars to
	// counter and histogram data points.
	Exemplars bool
	// DisableTargets turns off scraping of annotation-discovered pod and
	// service targets (the kubelet scrapes are configured separately).
	DisableTargets bool
	// Kubelet configures scraping of the kubelet's cadvisor and node
	// metrics endpoints.
	Kubelet KubeletConfig
	// Attrs holds the per-pipeline resource attribute builders (nil =
	// defaults).
	Attrs *attrs.Builders
	// NodeInfo supplies the agent node's metadata for attribute templates
	// (nil = name only, from Node).
	NodeInfo func() *attrs.NodeInfo
	// Filters drops/keeps scraped series per pipeline (nil = keep all).
	Filters *MetricFilters
	// Splitters re-attribute series of matching targets (kube-state-metrics
	// style) into per-object resources; they resolve metadata through
	// Kubelet.Meta.
	Splitters []*Splitter
	// HealthMetrics exports synthetic up / scrape_duration_seconds /
	// scrape_samples_scraped gauges per target after every cycle.
	HealthMetrics bool
	Logger        *slog.Logger
	Targets       TargetSource
	// Auth resolves monitor endpoints' bearerTokenSecret refs (metaclient;
	// nil = targets carrying AuthSecret fail their scrape with an error).
	Auth      AuthSource
	Exporter  MetricExporter
	StartTime time.Time // cumulative-sum start timestamp (agent start)
}

// Scraper periodically scrapes all targets of one node and exports the
// samples as OTLP metrics.
//
// Efficiency: the exposition body is stream-parsed (constant memory per
// target) and converted into pmetric batches flushed once BatchPoints data
// points OR BatchBytes estimated bytes accumulate, which are exported and
// released before parsing continues — a 100k-series target never resides fully
// in memory. The byte bound is what keeps a chunk under the collector's 4 MiB
// default receive limit (a point count does not bound bytes); it is checked
// between the points of a flushing histogram/summary family too, so one
// enormous family cannot overshoot it.
type Scraper struct {
	cfg  Config
	http *http.Client
	log  *slog.Logger

	kubeletHTTP *http.Client
	// podCache backs the metadata lookups of the cadvisor batcher and the
	// splitters; splitters run on concurrent scrape goroutines.
	cacheMu  sync.Mutex
	podCache map[string]podCacheEntry

	// status is the last completed cycle's per-target outcomes, served on the
	// agent's GET /debug/targets (see status.go).
	status atomic.Pointer[CycleStatus]

	// insecureHTTP serves monitor endpoints with tlsConfig.insecureSkipVerify.
	insecureHTTP *http.Client
	// authCache holds monitor bearer tokens by "ns/name/key" ref (1-minute
	// TTL); scrapes run on concurrent goroutines.
	authMu    sync.Mutex
	authCache map[string]authCacheEntry
	// relabels caches monitor endpoints' compiled metricRelabelings.
	relabels relabelCache
}

type authCacheEntry struct {
	token   string
	fetched time.Time
}

// New creates a Scraper.
func New(cfg Config) *Scraper {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if cfg.BatchPoints <= 0 {
		cfg.BatchPoints = 10_000
	}
	if cfg.BatchBytes == 0 {
		cfg.BatchBytes = defaultBatchBytes
	}
	if cfg.MaxLineBytes <= 0 {
		cfg.MaxLineBytes = 1 << 20
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Scraper{
		cfg: cfg,
		http: &http.Client{
			Timeout: cfg.Timeout,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 2,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		// For monitor endpoints declaring tlsConfig.insecureSkipVerify:
		// scoped to those targets only, never the default.
		insecureHTTP: &http.Client{
			Timeout: cfg.Timeout,
			Transport: &http.Transport{
				TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
				MaxIdleConnsPerHost: 2,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		log:         log,
		kubeletHTTP: newKubeletHTTPClient(cfg.Kubelet, cfg.Timeout),
		podCache:    make(map[string]podCacheEntry),
		authCache:   make(map[string]authCacheEntry),
	}
}

// authToken resolves a monitor endpoint's bearer token via the metadata
// service, cached for a minute (tokens rotate; per-cycle lookups must not
// hammer the service).
func (s *Scraper) authToken(ctx context.Context, ref string) (string, error) {
	s.authMu.Lock()
	if e, ok := s.authCache[ref]; ok && time.Since(e.fetched) < time.Minute {
		s.authMu.Unlock()
		return e.token, nil
	}
	s.authMu.Unlock()
	if s.cfg.Auth == nil {
		return "", fmt.Errorf("no auth source configured")
	}
	token, err := s.cfg.Auth.ScrapeAuth(ctx, ref)
	if err != nil {
		return "", err
	}
	s.authMu.Lock()
	s.authCache[ref] = authCacheEntry{token: token, fetched: time.Now()}
	s.authMu.Unlock()
	return token, nil
}

// Run scrapes every Interval until ctx is done. The first cycle starts
// immediately.
func (s *Scraper) Run(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()
	for {
		s.cycle(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Scraper) cycle(ctx context.Context) {
	var targets []kubemeta.ScrapeTarget
	if !s.cfg.DisableTargets {
		var err error
		targets, err = s.cfg.Targets.NodeTargets(ctx, s.cfg.Node)
		if err != nil {
			s.log.Error("fetching scrape targets", "node", s.cfg.Node, "error", err)
			// The kubelet scrapes below do not depend on the target list.
		}
	}

	sem := make(chan struct{}, s.cfg.Concurrency)
	var (
		wg       sync.WaitGroup
		healthMu sync.Mutex
		outcomes []scrapeOutcome
	)
	record := func(o scrapeOutcome) {
		result := "ok"
		if !o.ok {
			result = "error"
		}
		obs.Scrapes.WithLabelValues(o.pipeline, result).Inc()
		obs.ScrapeDuration.WithLabelValues(o.pipeline).Observe(o.duration.Seconds())
		obs.ScrapeSamples.WithLabelValues(o.pipeline).Add(float64(o.samples))
		// Collected unconditionally: the /debug/targets snapshot wants every
		// outcome even when health metrics are off.
		healthMu.Lock()
		outcomes = append(outcomes, o)
		healthMu.Unlock()
	}
	spawn := func(pipeline, url string, target *kubemeta.ScrapeTarget, scrape func(context.Context) (int, error)) bool {
		select {
		case <-ctx.Done():
			return false
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			start := time.Now()
			samples, err := scrape(ctx)
			errStr := ""
			if err != nil {
				errStr = err.Error()
			}
			record(scrapeOutcome{
				pipeline: pipeline, url: url, target: target,
				ok: err == nil, err: errStr, duration: time.Since(start), samples: samples,
			})
			if err != nil && ctx.Err() == nil {
				s.log.Warn("scrape failed", "pipeline", pipeline, "url", url, "error", err)
			}
		}()
		return true
	}
	if s.cfg.Kubelet.Endpoint != "" {
		base := strings.TrimRight(s.cfg.Kubelet.Endpoint, "/")
		if s.cfg.Kubelet.Cadvisor {
			spawn(pipelineCadvisor, base+"/metrics/cadvisor", nil, s.scrapeCadvisor)
		}
		if s.cfg.Kubelet.NodeMetrics {
			spawn(pipelineNode, base+"/metrics", nil, s.scrapeNodeMetrics)
		}
	}

	for i := range targets {
		t := targets[i]
		if !spawn(pipelineTargets, t.URL, &t, func(ctx context.Context) (int, error) {
			return s.scrapeTarget(ctx, t)
		}) {
			break // ctx done; join what already started
		}
	}
	wg.Wait()

	s.publishStatus(outcomes, time.Now())
	if s.cfg.HealthMetrics && len(outcomes) > 0 && ctx.Err() == nil {
		s.exportHealth(ctx, outcomes)
	}
}

// scrapeOutcome is the health record of one scrape.
type scrapeOutcome struct {
	pipeline string
	url      string
	target   *kubemeta.ScrapeTarget // nil for the kubelet scrapes
	ok       bool
	err      string
	duration time.Duration
	samples  int
}

// exportHealth emits the Prometheus-style synthetic series (up,
// scrape_duration_seconds, scrape_samples_scraped) for every scrape of the
// cycle, on the target's resource.
func (s *Scraper) exportHealth(ctx context.Context, outcomes []scrapeOutcome) {
	md := pmetric.NewMetrics()
	ts := pcommon.NewTimestampFromTime(time.Now())
	for _, o := range outcomes {
		rm := md.ResourceMetrics().AppendEmpty()
		res := rm.Resource()
		if o.target != nil {
			s.fillTargetResource(res, o.url, &o.target.Pod, o.target.Service)
		} else {
			res.Attributes().PutStr("url.full", o.url)
			res.Attributes().PutStr("service.name", "kubelet")
			s.attrsFor(o.pipeline).Build(res, attrs.Context{Node: s.nodeInfo()})
		}
		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName(scopeName)
		gauge := func(name string, v float64) {
			m := sm.Metrics().AppendEmpty()
			m.SetName(name)
			dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
			dp.SetDoubleValue(v)
			dp.SetTimestamp(ts)
		}
		up := 0.0
		if o.ok {
			up = 1
		}
		gauge("up", up)
		gauge("scrape_duration_seconds", o.duration.Seconds())
		gauge("scrape_samples_scraped", float64(o.samples))
	}
	if err := s.cfg.Exporter.ExportMetrics(ctx, md); err != nil && ctx.Err() == nil {
		s.log.Warn("exporting scrape health metrics", "error", err)
	}
}

// fillTargetResource stamps url.full and builds a target's own resource
// attributes (the pipelineTargets set with the pod/service/node context) — the
// convention shared by the scrape, health, and split-self resources.
func (s *Scraper) fillTargetResource(res pcommon.Resource, url string, pod *kubemeta.Pod, svc *kubemeta.Service) {
	res.Attributes().PutStr("url.full", url)
	s.attrsFor(pipelineTargets).Build(res, attrs.Context{Pod: pod, Service: svc, Node: s.nodeInfo()})
}

// resolveContext resolves a described object's pod/container through the metadata
// service — an exact container incarnation by container id, else the pod by
// namespace+name (cross-checked against uid) with a named container matched
// within it; a container-name miss stamps k8s.container.name on res. It returns
// the built attrs.Context (Node NOT set — the caller adds it) and whether
// anything resolved; on no resolution the caller writes its own identity
// fallback. Shared by the cadvisor and split batchers.
func (s *Scraper) resolveContext(ctx context.Context, containerID, namespace, pod, uid, container string, res pcommon.Resource) (attrs.Context, bool) {
	var actx attrs.Context
	if containerID != "" {
		if md := s.containerMeta(ctx, containerID); md != nil {
			actx.Pod, actx.Container = &md.Pod, &md.Container
			return actx, true
		}
	}
	if pod != "" {
		if meta := s.podMeta(ctx, namespace, pod); meta != nil && (uid == "" || meta.UID == uid) {
			actx.Pod = meta
			if container != "" {
				for i := range meta.Containers {
					if meta.Containers[i].Name == container {
						actx.Container = &meta.Containers[i]
						break
					}
				}
				if actx.Container == nil {
					res.Attributes().PutStr("k8s.container.name", container)
				}
			}
			return actx, true
		}
	}
	return actx, false
}

func (s *Scraper) scrapeTarget(ctx context.Context, t kubemeta.ScrapeTarget) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.URL, nil)
	if err != nil {
		return 0, err
	}
	if s.cfg.Exemplars {
		req.Header.Set("Accept", "application/openmetrics-text;version=1.0.0;q=1,text/plain;version=0.0.4;q=0.5")
	} else {
		req.Header.Set("Accept", "text/plain;version=0.0.4")
	}
	if t.AuthSecret != "" {
		token, err := s.authToken(ctx, t.AuthSecret)
		if err != nil {
			return 0, fmt.Errorf("scrape auth %s: %w", t.AuthSecret, err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}
	client := s.http
	if t.InsecureSkipVerify {
		client = s.insecureHTTP
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer drainClose(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status %d", resp.StatusCode)
	}

	// The target decides the format; some exporters serve OpenMetrics
	// regardless of Accept, so detect from the response.
	openMetrics := strings.Contains(resp.Header.Get("Content-Type"), "openmetrics")

	var cb chunker
	if sp := s.splitterFor(t.Pod); sp != nil {
		cb = newSplitBatcher(s, ctx, t, sp, time.Now())
	} else {
		cb = newBatcher(func(res pcommon.Resource) {
			s.fillTargetResource(res, t.URL, &t.Pod, t.Service)
		}, s.cfg.BatchPoints, s.cfg.StartTime, time.Now())
	}
	relabel, err := s.relabels.session(t.MetricRelabelings)
	if err != nil {
		return 0, err // exporting what the user asked to drop is worse than failing visibly
	}
	return s.parseAndExportFiltered(ctx, resp.Body, openMetrics, s.cfg.Exemplars, cb, pipelineTargets, t.URL, relabel)
}

// batcher accumulates samples of one source into a pmetric.Metrics payload
// with a single resource, grouping data points by metric name.
type batcher struct {
	fillResource func(pcommon.Resource)
	limit        int
	startTS      pcommon.Timestamp
	scrapeTS     pcommon.Timestamp
	md           pmetric.Metrics
	sm           pmetric.ScopeMetrics
	byName       map[string]pmetric.Metric
	// lastName/lastMetric short-circuit the byName probe: consecutive samples
	// almost always belong to the same family, and names are interned so the
	// comparison is usually pointer-equal.
	lastName   string
	lastMetric pmetric.Metric
	lastOK     bool
	points     int
	bytes      int
}

func newBatcher(fillResource func(pcommon.Resource), limit int, start, scrape time.Time) *batcher {
	b := &batcher{
		fillResource: fillResource,
		limit:        limit,
		startTS:      pcommon.NewTimestampFromTime(start),
		scrapeTS:     pcommon.NewTimestampFromTime(scrape),
	}
	b.reset()
	return b
}

func (b *batcher) reset() {
	b.md = pmetric.NewMetrics()
	rm := b.md.ResourceMetrics().AppendEmpty()
	b.fillResource(rm.Resource())
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName(scopeName)
	b.sm = sm
	if b.byName == nil {
		b.byName = make(map[string]pmetric.Metric)
	} else {
		clear(b.byName)
	}
	b.lastOK = false
	b.points = 0
	b.bytes = resourceBytes(rm.Resource(), scopeName) // this chunk's single resource
}

// take returns the accumulated payload and starts a fresh batch.
func (b *batcher) take() pmetric.Metrics {
	md := b.md
	b.reset()
	return md
}

func (b *batcher) count() int { return b.points }
func (b *batcher) size() int  { return b.bytes }

// Pipeline identifiers for attribute-builder selection.
const (
	pipelineTargets  = "targets"
	pipelineCadvisor = "cadvisor"
	pipelineNode     = "node"
)

// attrsFor picks the attribute builder for a pipeline; nil is valid (built-in
// defaults).
func (s *Scraper) attrsFor(pipeline string) *attrs.Builder {
	if s.cfg.Attrs == nil {
		return nil
	}
	switch pipeline {
	case pipelineCadvisor:
		return s.cfg.Attrs.Cadvisor
	case pipelineNode:
		return s.cfg.Attrs.Node
	default:
		return s.cfg.Attrs.Targets
	}
}

// nodeInfo returns the agent node's metadata for templates.
func (s *Scraper) nodeInfo() *attrs.NodeInfo {
	if s.cfg.NodeInfo != nil {
		if n := s.cfg.NodeInfo(); n != nil {
			return n
		}
	}
	return &attrs.NodeInfo{Name: s.cfg.Node}
}
