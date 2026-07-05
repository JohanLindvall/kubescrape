package promscrape

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/kubemeta"
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

// Config configures the scraper.
type Config struct {
	Node         string
	Interval     time.Duration
	Timeout      time.Duration // per-target scrape timeout
	Concurrency  int           // concurrent target scrapes
	BatchPoints  int           // flush to the exporter after this many data points
	MaxLineBytes int           // skip exposition lines longer than this
	MaxSamples   int           // abort a single scrape beyond this many samples (0 = unlimited)
	Logger       *slog.Logger
	Targets      TargetSource
	Exporter     MetricExporter
	StartTime    time.Time // cumulative-sum start timestamp (agent start)
}

// Scraper periodically scrapes all targets of one node and exports the
// samples as OTLP metrics.
//
// Efficiency: the exposition body is stream-parsed (constant memory per
// target) and converted into pmetric batches of at most BatchPoints data
// points, which are exported and released before parsing continues — a
// 100k-series target never resides fully in memory.
type Scraper struct {
	cfg  Config
	http *http.Client
	log  *slog.Logger
}

// New creates a Scraper.
func New(cfg Config) *Scraper {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if cfg.BatchPoints <= 0 {
		cfg.BatchPoints = 10_000
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
		log: log,
	}
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
	targets, err := s.cfg.Targets.NodeTargets(ctx, s.cfg.Node)
	if err != nil {
		s.log.Error("fetching scrape targets", "node", s.cfg.Node, "error", err)
		return
	}

	sem := make(chan struct{}, s.cfg.Concurrency)
	var wg sync.WaitGroup
	for _, t := range targets {
		select {
		case <-ctx.Done():
			return
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func(t kubemeta.ScrapeTarget) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := s.scrapeTarget(ctx, t); err != nil && ctx.Err() == nil {
				s.log.Warn("scrape failed", "url", t.URL, "pod", t.Pod.Namespace+"/"+t.Pod.Name, "error", err)
			}
		}(t)
	}
	wg.Wait()
}

func (s *Scraper) scrapeTarget(ctx context.Context, t kubemeta.ScrapeTarget) error {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.URL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/plain;version=0.0.4")
	resp, err := s.http.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}

	b := newBatcher(t, s.cfg.BatchPoints, s.cfg.StartTime, time.Now())
	parser := NewParser(s.cfg.MaxLineBytes)
	samples := 0
	malformed, err := parser.Parse(resp.Body, func(sample Sample) error {
		samples++
		if s.cfg.MaxSamples > 0 && samples > s.cfg.MaxSamples {
			return ErrTooManySamples
		}
		b.add(sample)
		if b.points >= s.cfg.BatchPoints {
			return s.cfg.Exporter.ExportMetrics(ctx, b.take())
		}
		return nil
	})
	if err != nil {
		return err
	}
	if b.points > 0 {
		if err := s.cfg.Exporter.ExportMetrics(ctx, b.take()); err != nil {
			return err
		}
	}
	if malformed > 0 {
		s.log.Warn("scrape had malformed lines", "url", t.URL, "malformed", malformed, "samples", samples)
	}
	return nil
}

// batcher accumulates samples of one target into a pmetric.Metrics payload,
// grouping data points by metric name.
type batcher struct {
	target   kubemeta.ScrapeTarget
	limit    int
	startTS  pcommon.Timestamp
	scrapeTS pcommon.Timestamp
	md       pmetric.Metrics
	sm       pmetric.ScopeMetrics
	byName   map[string]pmetric.Metric
	points   int
}

func newBatcher(t kubemeta.ScrapeTarget, limit int, start, scrape time.Time) *batcher {
	b := &batcher{
		target:   t,
		limit:    limit,
		startTS:  pcommon.NewTimestampFromTime(start),
		scrapeTS: pcommon.NewTimestampFromTime(scrape),
	}
	b.reset()
	return b
}

func (b *batcher) reset() {
	b.md = pmetric.NewMetrics()
	rm := b.md.ResourceMetrics().AppendEmpty()
	attrs.Pod(rm.Resource(), b.target.Pod)
	attrs.Service(rm.Resource(), b.target.Service)
	rm.Resource().Attributes().PutStr("url.full", b.target.URL)
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("github.com/JohanLindvall/kubescrape/agent/promscrape")
	b.sm = sm
	b.byName = make(map[string]pmetric.Metric)
	b.points = 0
}

// take returns the accumulated payload and starts a fresh batch.
func (b *batcher) take() pmetric.Metrics {
	md := b.md
	b.reset()
	return md
}

func (b *batcher) add(s Sample) {
	m, ok := b.byName[s.Name]
	if !ok {
		m = b.sm.Metrics().AppendEmpty()
		m.SetName(s.Name)
		switch s.Kind {
		case KindSum:
			sum := m.SetEmptySum()
			sum.SetIsMonotonic(true)
			sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		default:
			m.SetEmptyGauge()
		}
		b.byName[s.Name] = m
	}

	var dp pmetric.NumberDataPoint
	switch m.Type() {
	case pmetric.MetricTypeSum:
		dp = m.Sum().DataPoints().AppendEmpty()
		dp.SetStartTimestamp(b.startTS)
	default:
		dp = m.Gauge().DataPoints().AppendEmpty()
	}
	dp.SetDoubleValue(s.Value)
	if s.TimestampMs != 0 {
		dp.SetTimestamp(pcommon.Timestamp(s.TimestampMs * int64(time.Millisecond)))
	} else {
		dp.SetTimestamp(b.scrapeTS)
	}
	labels := dp.Attributes()
	labels.EnsureCapacity(len(s.Labels))
	for _, l := range s.Labels {
		labels.PutStr(l.Name, l.Value)
	}
	b.points++
}
