package promscrape

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
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
	Auth AuthSource
	// NativeHistograms offers the protobuf exposition format to annotation/
	// monitor targets — splitter-backed ones included — the only format
	// carrying native histograms, which convert to OTLP exponential
	// histograms.
	NativeHistograms bool
	Exporter         MetricExporter
	StartTime        time.Time // cumulative-sum start timestamp (agent start)
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

	// Per-target scheduling: due holds each target's next scrape time and
	// targetIntervals its resolved period (both keyed by URL, rebuilt every
	// cycle so vanished targets drop out). warned dedupes per-target
	// complaints. Written only by the cycle goroutine, but Run reads the
	// intervals to size its ticker, so they are guarded.
	// tlsClients caches per-target transports keyed by their resolved TLS
	// material (see clientFor); targets sharing a CA share a connection pool.
	tlsMu      sync.Mutex
	tlsClients map[string]*http.Client

	dueMu           sync.Mutex
	due             map[string]time.Time
	targetIntervals map[string]time.Duration
	warned          map[string]bool

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
			// No client Timeout: the per-request context carries the effective
			// (possibly per-target) budget, and a fixed client timeout silently
			// capped every target that asked for longer.
			CheckRedirect: noRedirect,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 2,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		// For monitor endpoints declaring tlsConfig.insecureSkipVerify:
		// scoped to those targets only, never the default.
		insecureHTTP: &http.Client{
			// No client Timeout: the per-request context carries the effective
			// (possibly per-target) budget, and a fixed client timeout silently
			// capped every target that asked for longer.
			CheckRedirect: noRedirect,
			Transport: &http.Transport{
				TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
				MaxIdleConnsPerHost: 2,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		tlsClients:  map[string]*http.Client{},
		log:         log,
		kubeletHTTP: newKubeletHTTPClient(cfg.Kubelet, cfg.Timeout),
		podCache:    make(map[string]podCacheEntry),
		authCache:   make(map[string]authCacheEntry),
	}
}

// scrapeProto runs the protobuf exposition path with the same export/full
// closures the text path uses.
func (s *Scraper) scrapeProto(ctx context.Context, body io.Reader, cb chunker, relabel *relabelFilter, what string) (int, error) {
	export := func() error {
		return s.cfg.Exporter.ExportMetrics(ctx, cb.take())
	}
	full := func() bool {
		return cb.count() >= s.cfg.BatchPoints ||
			(s.cfg.BatchBytes > 0 && cb.size() >= s.cfg.BatchBytes)
	}
	samples, malformed, err := s.parseProtoAndExport(ctx, body, cb, pipelineTargets, relabel, export, full)
	if malformed > 0 {
		obs.ScrapeMalformed.WithLabelValues(pipelineTargets).Add(float64(malformed))
		s.log.Warn("scrape had malformed proto families", "target", what, "malformed", malformed, "samples", samples)
	}
	if err != nil {
		return samples, err
	}
	if cb.count() > 0 {
		return samples, export()
	}
	return samples, nil
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

// Run scrapes until ctx is done; the first cycle starts immediately.
//
// The loop ticks at the FINEST cadence any target asks for (never longer than
// -scrape-interval): a monitor endpoint may set its own `interval`, and each
// target is scraped only when its own period has elapsed (see cycle). With no
// per-target intervals this is exactly a -scrape-interval ticker.
func (s *Scraper) Run(ctx context.Context) {
	tick := s.cfg.Interval
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		s.cycle(ctx)
		if want := s.tickInterval(); want != tick {
			tick = want
			ticker.Reset(tick)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// tickInterval is the loop period: the smallest interval any target requested,
// floored at 1s so a nonsense CR cannot spin the scraper.
func (s *Scraper) tickInterval() time.Duration {
	s.dueMu.Lock()
	defer s.dueMu.Unlock()
	out := s.cfg.Interval
	for _, iv := range s.targetIntervals {
		if iv > 0 && iv < out {
			out = iv
		}
	}
	return max(out, time.Second)
}

// targetInterval resolves a target's effective scrape period: its own
// `interval` when the monitor set a valid one, else the agent's default. An
// unparseable value is reported once and falls back rather than failing the
// target — the CR is the user's, and dropping their metrics over a typo in an
// optional field is worse than scraping it at the default cadence.
func (s *Scraper) targetInterval(t kubemeta.ScrapeTarget) time.Duration {
	if t.Interval == "" {
		return s.cfg.Interval
	}
	d, err := parsePromDuration(t.Interval)
	if err != nil || d <= 0 {
		s.warnOnce("interval:"+t.URL, "ignoring invalid scrape interval on target",
			"url", t.URL, "monitor", t.Monitor, "interval", t.Interval)
		return s.cfg.Interval
	}
	return d
}

// targetTimeout resolves a target's per-scrape timeout, clamped to its own
// interval: a scrape outliving its period would overlap the next one.
func (s *Scraper) targetTimeout(t kubemeta.ScrapeTarget, interval time.Duration) time.Duration {
	out := s.cfg.Timeout
	asked := time.Duration(0)
	if t.ScrapeTimeout != "" {
		d, err := parsePromDuration(t.ScrapeTimeout)
		if err != nil || d <= 0 {
			s.warnOnce("timeout:"+t.URL, "ignoring invalid scrape timeout on target",
				"url", t.URL, "monitor", t.Monitor, "scrapeTimeout", t.ScrapeTimeout)
		} else {
			out, asked = d, d
		}
	}
	// Clamped to the target's interval AND the agent's own: cycle() waits for
	// every scrape it started, and Run only ticks after cycle returns, so one
	// target's long timeout stalls the whole node's scrape loop.
	//
	// The agent's own interval is the constraint a user cannot see from their
	// CR: a monitor at `interval: 5m, scrapeTimeout: 2m` is entirely
	// self-consistent, yet the timeout still collapses to -scrape-interval
	// (30s by default) and a slow exporter is cut off. That is a real
	// limitation of the synchronous cycle, not a misconfiguration — so say so
	// once instead of quietly handing back a fifth of what was asked for.
	got := min(out, interval, s.cfg.Interval)
	if asked > 0 && got < asked {
		s.warnOnce("timeoutclamp:"+t.URL, "scrape timeout clamped below the monitor's scrapeTimeout; raise -scrape-interval to allow a longer one",
			"url", t.URL, "monitor", t.Monitor,
			"scrapeTimeout", asked, "effective", got, "scrapeInterval", s.cfg.Interval)
	}
	return got
}

// parsePromDuration accepts the duration syntax prometheus-operator's CRD
// validates (`y`, `w`, `d`, `h`, `m`, `s`, `ms`, largest unit first, e.g.
// "1d12h"), falling back to Go's parser for anything it does not recognise so
// plain "30s"/"1m30s" keep working.
//
// Go's time.ParseDuration rejects y/w/d outright, so an `interval: 1d` — which
// the API server ACCEPTS, because the CRD's own pattern allows it — used to
// parse-fail and silently drop the target back to the default cadence.
func parsePromDuration(s string) (time.Duration, error) {
	if s == "0" {
		return 0, nil
	}
	m := promDurationRE.FindStringSubmatch(s)
	if m == nil {
		// Not the Prometheus shape; let Go try (it also accepts "1h30m",
		// "500ms" and the fractional forms Prometheus does not).
		return time.ParseDuration(s)
	}
	units := []time.Duration{
		365 * 24 * time.Hour, // y, as prometheus/common/model defines it
		7 * 24 * time.Hour,   // w
		24 * time.Hour,       // d
		time.Hour,            // h
		time.Minute,          // m
		time.Second,          // s
		time.Millisecond,     // ms
	}
	var out time.Duration
	for i, u := range units {
		g := m[i+1]
		if g == "" {
			continue
		}
		n, err := strconv.ParseInt(g, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("duration %q: %w", s, err)
		}
		out += time.Duration(n) * u
	}
	return out, nil
}

// promDurationRE mirrors prometheus-operator's Duration validation pattern, so
// exactly the values the API server admits are the values we accept.
var promDurationRE = regexp.MustCompile(`^(?:([0-9]+)y)?(?:([0-9]+)w)?(?:([0-9]+)d)?(?:([0-9]+)h)?(?:([0-9]+)m)?(?:([0-9]+)s)?(?:([0-9]+)ms)?$`)

// warnOnce logs a per-key message at most once per process, for per-target
// complaints that would otherwise repeat every cycle forever.
func (s *Scraper) warnOnce(key, msg string, args ...any) {
	s.dueMu.Lock()
	if s.warned == nil {
		s.warned = map[string]bool{}
	}
	seen := s.warned[key]
	s.warned[key] = true
	s.dueMu.Unlock()
	if !seen {
		s.log.Warn(msg, args...)
	}
}

func (s *Scraper) cycle(ctx context.Context) {
	var targets []kubemeta.ScrapeTarget
	targetsOK := s.cfg.DisableTargets // nothing to schedule when targets are off
	if !s.cfg.DisableTargets {
		var err error
		targets, err = s.cfg.Targets.NodeTargets(ctx, s.cfg.Node)
		targetsOK = err == nil
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
	now := time.Now()
	due := make(map[string]time.Time, len(targets)+2)
	intervals := make(map[string]time.Duration, len(targets))
	// dueNow reports whether a scheduled key is due, and records its next time.
	// The kubelet scrapes go through it too: they are the most expensive on the
	// node, and Run's tick is set by whatever target asks for the finest
	// cadence, so leaving them unscheduled made one 10s monitor re-clock
	// /metrics/cadvisor for the whole fleet.
	dueNow := func(key string, iv time.Duration) bool {
		next, scheduled := s.dueAt(key)
		if scheduled && now.Add(iv/10).Before(next) {
			due[key] = next
			return false
		}
		if scheduled {
			// Advance from the DUE time, not from now: the tick may land
			// slightly early (iv/10), and folding that slack in every round
			// drifted long-interval targets ~10% faster, permanently.
			if n := next.Add(iv); n.After(now) {
				due[key] = n
			} else {
				due[key] = now.Add(iv) // fell far behind: resynchronise
			}
		} else {
			due[key] = now.Add(iv)
		}
		return true
	}

	if s.cfg.Kubelet.Endpoint != "" {
		base := strings.TrimRight(s.cfg.Kubelet.Endpoint, "/")
		if s.cfg.Kubelet.Cadvisor && dueNow(kubeletDueKeys[0], s.cfg.Interval) {
			spawn(pipelineCadvisor, base+"/metrics/cadvisor", nil, s.scrapeCadvisor)
		}
		if s.cfg.Kubelet.NodeMetrics && dueNow(kubeletDueKeys[1], s.cfg.Interval) {
			spawn(pipelineNode, base+"/metrics", nil, s.scrapeNodeMetrics)
		}
	}

	// Per-target cadence. EVERY target is scheduled, defaulting to the agent's
	// interval; only a target whose monitor set an explicit `interval` may
	// speed the loop's tick up. Clock jitter between the ticker and a due time
	// is absorbed by the slack in dueNow rather than by leaving targets
	// unscheduled. The maps are rebuilt from the current target list each
	// cycle, so a vanished target takes its schedule with it.
	for i := range targets {
		t := targets[i]
		// EVERY target is scheduled, defaulting to the agent's interval. Only
		// scheduling the ones with an explicit interval was a mistake: Run ticks
		// at the finest cadence any target asks for, so a single monitor with
		// `interval: 10s` made every unscheduled target — and both kubelet
		// scrapes, the most expensive on the node — run at 10s instead of
		// -scrape-interval. kube-prometheus-stack ships exactly such monitors,
		// so this tripled a default fleet's scrape rate silently.
		iv := s.targetInterval(t)
		if t.Interval != "" {
			// Only an EXPLICIT interval may speed the loop's tick up.
			intervals[t.URL] = iv
		}
		if !dueNow(t.URL, iv) {
			continue
		}
		timeout := s.targetTimeout(t, iv)
		if !spawn(pipelineTargets, t.URL, &t, func(ctx context.Context) (int, error) {
			return s.scrapeTarget(ctx, t, timeout)
		}) {
			break // ctx done; join what already started
		}
	}
	if targetsOK {
		// A failed target fetch leaves `targets` empty; committing that as the
		// schedule would discard every due time and re-scrape everything next
		// cycle, ignoring the intervals entirely.
		s.setSchedule(due, intervals)
	} else {
		// The KUBELET due times are not derived from the target list and must
		// be committed regardless — they ride in the same map only for
		// convenience. Discarding them left both kubelet scrapes permanently
		// past due while targetIntervals stayed frozen at whatever fine cadence
		// a monitor had asked for, so a metadata-service rollout re-clocked
		// /metrics/cadvisor and /metrics to (say) 10s on every node in the
		// cluster — a load spike on every kubelet exactly while the control
		// plane is already degraded.
		s.setKubeletSchedule(due)
	}
	wg.Wait()

	s.publishStatus(outcomes, time.Now())
	if s.cfg.HealthMetrics && len(outcomes) > 0 && ctx.Err() == nil {
		s.exportHealth(ctx, outcomes)
	}
}

// dueAt returns a target's next scheduled scrape time.
func (s *Scraper) dueAt(url string) (time.Time, bool) {
	s.dueMu.Lock()
	defer s.dueMu.Unlock()
	t, ok := s.due[url]
	return t, ok
}

// setSchedule replaces the per-target schedule with this cycle's.
func (s *Scraper) setSchedule(due map[string]time.Time, intervals map[string]time.Duration) {
	s.dueMu.Lock()
	defer s.dueMu.Unlock()
	s.due, s.targetIntervals = due, intervals
}

// setKubeletSchedule commits only the kubelet entries of a cycle whose target
// list could not be fetched, leaving the target schedule (and the intervals
// derived from it) untouched for the next successful cycle.
func (s *Scraper) setKubeletSchedule(due map[string]time.Time) {
	s.dueMu.Lock()
	defer s.dueMu.Unlock()
	if s.due == nil {
		// The very first cycle can be a failing one — an agent normally starts
		// before the metadata service is reachable — so the schedule map may
		// not exist yet.
		s.due = make(map[string]time.Time, len(kubeletDueKeys))
	}
	for _, k := range kubeletDueKeys {
		if when, ok := due[k]; ok {
			s.due[k] = when
		}
	}
}

// kubeletDueKeys are the schedule keys of the two kubelet scrapes. They are
// NUL-prefixed so they cannot collide with a target URL.
var kubeletDueKeys = []string{"\x00cadvisor", "\x00node"}

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

func (s *Scraper) scrapeTarget(ctx context.Context, t kubemeta.ScrapeTarget, timeout time.Duration) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.URL, nil)
	if err != nil {
		return 0, err
	}
	useProto := s.cfg.NativeHistograms
	switch {
	case useProto:
		req.Header.Set("Accept", acceptProto)
	case s.cfg.Exemplars:
		req.Header.Set("Accept", "application/openmetrics-text;version=1.0.0;q=1,text/plain;version=0.0.4;q=0.5")
	default:
		req.Header.Set("Accept", "text/plain;version=0.0.4")
	}
	if err := s.applyAuth(ctx, req, t); err != nil {
		return 0, err
	}
	client, err := s.clientFor(ctx, t, timeout)
	if err != nil {
		return 0, err
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
		}, s.cfg.StartTime, time.Now())
	}
	relabel, err := s.relabels.session(t.MetricRelabelings)
	if err != nil {
		return 0, err // exporting what the user asked to drop is worse than failing visibly
	}
	if strings.Contains(resp.Header.Get("Content-Type"), "application/vnd.google.protobuf") {
		return s.scrapeProto(ctx, resp.Body, cb, relabel, t.URL)
	}
	return s.parseAndExportFiltered(ctx, resp.Body, openMetrics, s.cfg.Exemplars, cb, pipelineTargets, t.URL, relabel)
}

// batcher accumulates samples of one source into a pmetric.Metrics payload
// with a single resource, grouping data points by metric name.
type batcher struct {
	fillResource func(pcommon.Resource)
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

func newBatcher(fillResource func(pcommon.Resource), start, scrape time.Time) *batcher {
	b := &batcher{
		fillResource: fillResource,
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
