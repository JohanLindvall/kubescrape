package promscrape

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net/http"
	neturl "net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/transform"
	"github.com/JohanLindvall/kubescrape/internal/bearer"
	"github.com/JohanLindvall/kubescrape/internal/clip"
	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/promdur"
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
	Node     string
	Interval time.Duration
	// Timeout is the per-target scrape budget (0 or negative = the default,
	// defaultScrapeTimeout). It is NOT read as "no timeout": targetTimeout
	// takes the MINIMUM of it and the intervals, so a non-positive value is an
	// already-expired context — every target and both kubelet scrapes then fail
	// with "context deadline exceeded" on every cycle, i.e. total metric loss
	// from a flag an operator can plausibly set to 0 meaning "unlimited".
	Timeout     time.Duration
	Concurrency int // concurrent target scrapes
	BatchPoints int // flush to the exporter after this many data points
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
	// TargetHook, when set, runs over each fetched target list before
	// scheduling (the transforms file's targets: hook — drop or rewrite
	// targets with script logic the declarative config cannot express). It
	// runs once per fetch, not per sample: N targets per 30s cycle.
	TargetHook func([]kubemeta.ScrapeTarget) []kubemeta.ScrapeTarget
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
	// kubeletToken is the mounted ServiceAccount token presented to the kubelet
	// (nil when Kubelet.TokenFile is unset).
	kubeletToken *bearer.File
	// podCache backs the metadata lookups of the cadvisor batcher and the
	// splitters; splitters run on concurrent scrape goroutines. cacheSwept is
	// when the expiry sweep last ran (see evictCacheLocked).
	cacheMu    sync.Mutex
	podCache   map[string]podCacheEntry
	cacheSwept time.Time

	// status is the last completed cycle's per-target outcomes, served on the
	// agent's GET /debug/targets (see status.go).
	status atomic.Pointer[CycleStatus]

	// Per-target scheduling: due holds each target's next scrape time and
	// targetIntervals its resolved period (both keyed by scheduleKey, rebuilt
	// every cycle so vanished targets drop out). warned dedupes per-target
	// complaints — keyed by CONFIGURATION origin, never by URL, and bounded
	// (see warnOnce/warnTarget). Written only by the cycle goroutine, but Run
	// reads the intervals to size its ticker, so they are guarded.
	// tlsClients caches per-target transports keyed by their resolved TLS
	// material (see clientFor); targets sharing a CA share a connection pool.
	tlsMu      sync.Mutex
	tlsClients map[string]*http.Client

	dueMu           sync.Mutex
	due             map[string]time.Time
	targetIntervals map[string]time.Duration
	// warned dedupes per-target complaints. The table (and its
	// suppress-never-clear saturation policy) is internal/logdedupe, shared
	// with the metadata service's scrape-auth throttle — the two were written
	// separately and disagreed on exactly that policy.
	warned *logdedupe.Table
	// metaBudgetWarn throttles the exhausted-metadata-allowance warning
	// (metabudget.go). Keyless and re-arming, unlike warned: the condition is
	// an OUTAGE rather than a configuration mistake, so it is worth saying
	// again the next time it happens.
	metaBudgetWarn logdedupe.Throttle
	// failWarned throttles the per-target "scrape failed" line (failures.go).
	// A SECOND table rather than `warned`, because the two windows differ on
	// purpose: a configuration complaint is once per process (nothing changes
	// until a CR is edited), while a failing scrape re-warns, an operator
	// having fixed it out of band being the expected outcome.
	failMu     sync.Mutex
	failWarned *logdedupe.Table
	// emptyTargets latches the "this node has no scrape targets" state so the
	// transition is reported once and the recovery is reported at all — the
	// apiserver-reachability shape, for the condition that is the most common
	// first-run failure and moves no counter of its own.
	emptyTargets    bool
	emptyTargetWarn logdedupe.Throttle
	// lastTargets is the previous cycle's target count, for the Debug line that
	// reports the set changing size. Written only by the cycle goroutine.
	lastTargets    int
	lastTargetsSet bool
	// tlsEvictWarn and relabelEvictWarn throttle the two bounded per-target
	// caches' eviction notices. Both evict SILENTLY by design — the scrape
	// still works, it just rebuilds a transport or recompiles a chain — so
	// nothing at all reported a fleet whose credentials rotate faster than the
	// cache holds them, which is a handshake per target per cycle.
	//
	// ONE GATE PER CACHE, not one shared gate: they are independent conditions
	// with different remedies (rotate less / stop templating regexes), and a
	// shared keyless throttle would let whichever fired first suppress the
	// other for the whole window — the report that never arrives is the one
	// nobody knows to look for.
	tlsEvictWarn     logdedupe.Throttle
	relabelEvictWarn logdedupe.Throttle

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

// defaultScrapeTimeout is the per-scrape budget for a Config that supplies no
// usable one. It matches the agent's own -scrape-timeout default, so a config
// that zeroed the field behaves exactly like one that never set it.
const defaultScrapeTimeout = 15 * time.Second

// New creates a Scraper.
func New(cfg Config) *Scraper {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if cfg.Timeout <= 0 {
		// Defaulted like every other bound rather than passed through: see the
		// field's comment for why a non-positive value cannot mean "no timeout"
		// here. Run's `Interval <= 0` guard is the same philosophy — a nonsense
		// duration gets a defined, non-destructive meaning instead of a
		// fleet-wide failure mode with no metric naming it.
		cfg.Timeout = defaultScrapeTimeout
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
	sc := &Scraper{
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
	if cfg.Kubelet.TokenFile != "" {
		// Not read here: a projection that is not yet mounted must not stop the
		// agent, and kubeletGet surfaces the error on the scrape it belongs to.
		sc.kubeletToken = bearer.NewFile(cfg.Kubelet.TokenFile, log)
	}
	return sc
}

// scrapeProto runs the protobuf exposition path on the same scrapeSession
// harness the text path uses (proto only ever runs on the targets pipeline).
func (s *Scraper) scrapeProto(ctx context.Context, body io.Reader, cb chunker, relabel *relabelFilter, what, warnKey string) (int, error) {
	ss := s.newScrapeSession(ctx, cb, pipelineTargets, what, warnKey, relabel, true)
	defer ss.reportDropped()
	malformed, err := s.parseProtoAndExport(ss, body)
	ss.reportMalformed("scrape had malformed proto families", malformed, nil)
	if err != nil {
		return ss.samples, err
	}
	if ss.cb.count() > 0 {
		return ss.samples, ss.export()
	}
	return ss.samples, nil
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
		return "", errors.New("no auth source configured")
	}
	token, err := s.cfg.Auth.ScrapeAuth(ctx, ref)
	if err != nil {
		return "", err
	}
	s.authMu.Lock()
	now := time.Now()
	// Drop what has aged out. These entries hold bearer tokens, CA bundles and
	// client private keys; keeping them resident for the process lifetime past
	// their 1-minute usefulness is secret material sitting in heap for nothing
	// (the service side evicts, and the sibling TLS-client cache in this
	// package is bounded).
	for k, e := range s.authCache {
		if now.Sub(e.fetched) >= time.Minute {
			delete(s.authCache, k)
		}
	}
	s.authCache[ref] = authCacheEntry{token: token, fetched: now}
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
	if tick <= 0 {
		// Same hazard as the log-metrics loop: time.NewTicker panics on a
		// non-positive duration and this comes straight from -scrape-interval.
		// A zero scrape interval means "never", not "crash".
		s.log.Warn("scrape interval is not positive; the scrape loop will not run", "interval", tick)
		<-ctx.Done()
		return
	}
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

// parseTargetDuration parses one monitor-supplied duration field (kind names
// it in the warn key, attr in the log line). ok is false for an unparseable or
// non-positive value, which is reported once and left to the caller's fallback
// rather than failing the target — the CR is the user's, and dropping their
// metrics over a typo in an optional field is worse than scraping at the
// default cadence.
//
// The offending VALUE rides on the line but is NOT part of the key: see
// warnOnce. It used to be, so that re-introducing a different typo warned
// again — a real but small diagnostic gain, paid for by letting anyone who can
// edit one ServiceMonitor mint an unbounded number of never-expiring keys (one
// per edit) and permanently saturate the table every other complaint in this
// package shares. One line per (field, target) is what the operator needs; the
// second typo on the same field of the same monitor is now silent until a
// restart, which is the cheap half of that trade.
func (s *Scraper) parseTargetDuration(t kubemeta.ScrapeTarget, kind, attr, value string) (time.Duration, bool) {
	// promdur is the shared prometheus-operator duration parser (the metadata
	// service's monitor merge reads the same values through it); the
	// non-positive gate below is THIS caller's rule, not the parser's.
	d, err := promdur.Parse(value)
	if err != nil || d <= 0 {
		s.warnOnce(kind+":"+warnTarget(t), "ignoring invalid scrape "+kind+" on target",
			"url", t.URL, "monitor", t.Monitor, attr, clipForLog(value))
		return 0, false
	}
	return d, true
}

// targetInterval resolves a target's effective scrape period: its own
// `interval` when the monitor set a valid one, else the agent's default.
func (s *Scraper) targetInterval(t kubemeta.ScrapeTarget) time.Duration {
	if t.Interval == "" {
		return s.cfg.Interval
	}
	if d, ok := s.parseTargetDuration(t, "interval", "interval", t.Interval); ok {
		return d
	}
	return s.cfg.Interval
}

// targetTimeout resolves a target's per-scrape timeout, clamped to its own
// interval: a scrape outliving its period would overlap the next one.
func (s *Scraper) targetTimeout(t kubemeta.ScrapeTarget, interval time.Duration) time.Duration {
	out := s.cfg.Timeout
	asked := time.Duration(0)
	if t.ScrapeTimeout != "" {
		if d, ok := s.parseTargetDuration(t, "timeout", "scrapeTimeout", t.ScrapeTimeout); ok {
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
		// The key is the target's identity alone; the value it asked for is a
		// PARSED duration by now, and rides as an attribute.
		s.warnOnce("timeoutclamp:"+warnTarget(t), "scrape timeout clamped below the monitor's scrapeTimeout; raise -scrape-interval to allow a longer one",
			"url", t.URL, "monitor", t.Monitor,
			"scrapeTimeout", asked, "effective", got, "scrapeInterval", s.cfg.Interval)
	}
	return got
}

// maxWarnKeys bounds the dedupe table. The keys are configuration-derived, so a
// healthy cluster holds a handful of them; the cap only catches a pathological
// generator (thousands of monitors, each with its own typo).
//
// Reaching it SUPPRESSES further keys and says so once — see internal/logdedupe
// for why clearing instead is worse than the unbounded map it replaced.
const maxWarnKeys = 1024

// warnOnce logs a per-key message at most once per process, for per-target
// complaints that would otherwise repeat every cycle forever. A zero re-warn
// window is what makes it "once": the operator has to edit a CR, and until they
// do there is nothing new to say.
//
// THE KEY RULE, and it is a security boundary rather than a style preference:
// **a key is built from IDENTITY, never from content a target supplies.** The
// window is zero, so nothing in this table ever expires; the cap therefore
// SUPPRESSES (internal/logdedupe's rule, and the right one — clearing is worse)
// and the suppression is permanent for the process. A key carrying bytes off a
// scraped body, or a free-form field a workload can rewrite, hands whoever
// controls those bytes a way to mint maxWarnKeys distinct keys and shut every
// FUTURE warning in this package — the kubelet's RBAC refusal, a monitor's
// uncompilable regex — for the life of the agent. Identity (warnTarget, the
// kubelet endpoint, a pipeline name) is bounded by the cluster's own objects;
// content is not. Content still RIDES on the line, through clipForLog.
func (s *Scraper) warnOnce(key, msg string, args ...any) {
	s.dueMu.Lock()
	if s.warned == nil {
		s.warned = logdedupe.New(maxWarnKeys, 0)
	}
	tab := s.warned
	s.dueMu.Unlock()

	allow, saturated := tab.Allow(key)
	if saturated {
		s.log.Warn("scrape warning dedupe table is full; further distinct warnings are suppressed",
			"keys", maxWarnKeys)
	}
	if allow {
		s.log.Warn(msg, args...)
	}
}

// maxLoggedValueBytes bounds one target-supplied value on a log line. A metric
// family name or a monitor field arrives from outside this process and is
// bounded only by the body/CR size, and a megabyte of it in a log record is a
// second flood in the shape of one line.
const maxLoggedValueBytes = 96

// clipForLog renders a target-supplied value for a log attribute: bounded, and
// cut on a rune boundary (internal/clip) so a clipped UTF-8 sequence does not
// become a replacement character in whatever reads the line. It is the
// counterpart of the key rule above — the value cannot be part of the KEY, so
// this is how it still reaches the operator.
func clipForLog(v string) string { return clip.Ellipsis(v, maxLoggedValueBytes) }

// warnTarget identifies a target for warnOnce by the CONFIGURATION that
// produced it, never by its URL.
//
// The URL embeds the pod IP, so keying on it was wrong twice over: the table
// grew one entry per pod incarnation for the process' whole life, and the
// warning re-fired on every pod restart — defeating the "once" it exists for,
// with the noisiest clusters (frequent restarts) getting the most noise. What
// the operator actually has to fix is a field on a monitor CR, a Service
// annotation or a workload's pod annotation, and all three outlive the pods
// they produce targets for.
func warnTarget(t kubemeta.ScrapeTarget) string {
	if t.Monitor != "" {
		return t.Source + ":" + t.Monitor // "ns/name" of the ServiceMonitor/PodMonitor
	}
	if t.Service != nil {
		return t.Source + ":" + t.Service.Namespace + "/" + t.Service.Name
	}
	// Pod annotations. The chain is direct-owner-first, so the LAST owner is
	// the workload root: a Deployment rather than its per-revision ReplicaSet,
	// so a rollout does not mint a new key. A bare pod falls back to its name.
	if n := len(t.Pod.Owners); n > 0 {
		o := t.Pod.Owners[n-1]
		return t.Source + ":" + t.Pod.Namespace + "/" + o.Kind + "/" + o.Name
	}
	return t.Source + ":" + t.Pod.Namespace + "/" + t.Pod.Name
}

func (s *Scraper) cycle(ctx context.Context) {
	var targets []kubemeta.ScrapeTarget
	targetsOK := s.cfg.DisableTargets // nothing to schedule when targets are off
	if !s.cfg.DisableTargets {
		var err error
		targets, err = s.cfg.Targets.NodeTargets(ctx, s.cfg.Node)
		targetsOK = err == nil
		if targetsOK && s.cfg.TargetHook != nil {
			// The hook can drop targets, and a dropped target is
			// indistinguishable from one the metadata service never returned:
			// same empty list, same silence. Report the difference so a
			// transforms-file mistake is not diagnosed as a discovery problem.
			before := len(targets)
			targets = s.cfg.TargetHook(targets)
			if n := before - len(targets); n != 0 {
				s.log.Debug("the transforms file's targets: hook changed the target list",
					"node", s.cfg.Node, "targets", len(targets), "dropped", n)
			}
		}
		if err != nil {
			s.log.Error("fetching scrape targets", "node", s.cfg.Node, "error", err)
			// The kubelet scrapes below do not depend on the target list.
		} else {
			s.reportTargetSet(targets)
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
			if err != nil {
				// Counted (and warned) whatever the context says: a scrape
				// cancelled by shutdown classifies as `canceled` and reports
				// only the counter, so the rate stays honest without a rolling
				// update logging one accusation per target.
				key := url
				if target != nil {
					key = warnTarget(*target)
				}
				s.reportScrapeFailure(pipeline, url, key, err, ctx.Err() != nil)
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
			// The carried time was computed from the interval in force when it
			// was set, so a SHORTENED one must pull it in: an endpoint edited
			// from `interval: 2h` to 30s — or a deleted monitor whose target
			// falls back to -scrape-interval — keeps the same schedule key, and
			// nothing else clamps the stored time, so the target went unscraped
			// for the residual 2h while every cycle re-committed it. Lengthening
			// needs no counterpart: it applies at the next due time.
			if n := now.Add(iv); next.After(n) {
				next = n
			}
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
		if s.cfg.Kubelet.Cadvisor && dueNow(dueKeyCadvisor, s.cfg.Interval) {
			spawn(pipelineCadvisor, base+"/metrics/cadvisor", nil, s.scrapeCadvisor)
		}
		if s.cfg.Kubelet.NodeMetrics && dueNow(dueKeyNode, s.cfg.Interval) {
			spawn(pipelineNode, base+"/metrics", nil, s.scrapeNodeMetrics)
		}
		if s.cfg.Kubelet.Summary && dueNow(dueKeySummary, s.cfg.Interval) {
			spawn(pipelineSummary, base+summaryPath, nil, s.scrapeSummary)
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
		key := scheduleKey(t)
		if t.Interval != "" {
			// Only an EXPLICIT interval may speed the loop's tick up.
			intervals[key] = iv
		}
		if !dueNow(key, iv) {
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

	s.publishStatus(outcomes, targets, targetsOK, time.Now())
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

// The schedule keys of the three kubelet scrapes, NUL-prefixed so they cannot
// collide with a target URL. cycle() names them individually and
// kubeletDueKeys is what setKubeletSchedule commits: an index into the slice
// would let a fourth scrape (or a removed one) shift the entries under a call
// site the compiler cannot check — which is a panic on a path that runs once
// per cycle on every node.
const (
	dueKeyCadvisor = "\x00cadvisor"
	dueKeyNode     = "\x00node"
	dueKeySummary  = "\x00summary"
)

var kubeletDueKeys = []string{dueKeyCadvisor, dueKeyNode, dueKeySummary}

// scheduleKey identifies one target's schedule slot. The URL alone does NOT:
// the metadata service dedupes same-URL targets only WITHIN a pod, so two
// hostNetwork pods sharing the node's IP with the same annotated port yield
// identical URLs with different pod documents (the case its own sort comment
// names). Sharing a slot let the cycle's last-processed duplicate overwrite the
// other's due time and interval — a 10s endpoint on one collapsed the other
// onto its cadence, or was itself coarsened to the default.
//
// The key always carries all three separators, so it cannot collide with the
// NUL-prefixed kubelet keys even for a target with an empty URL.
func scheduleKey(t kubemeta.ScrapeTarget) string {
	return t.URL + "\x00" + t.Pod.UID + "\x00" + t.Source + "\x00" + t.Monitor
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
		sm.Scope().SetVersion(obs.ScopeVersion)
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
	// Handoff: md is fresh per cycle and a failure is only warned about, never
	// re-sent — the next cycle rebuilds it — so the transform seam may run in
	// place.
	if err := s.cfg.Exporter.ExportMetrics(transform.Handoff(ctx), md); err != nil && ctx.Err() == nil {
		s.log.Warn("exporting scrape health metrics", "error", err)
	}
}

// fillTargetResource stamps url.full and builds a target's own resource
// attributes (the pipelineTargets set with the pod/service/node context) — the
// convention shared by the scrape, health, and split-self resources.
func (s *Scraper) fillTargetResource(res pcommon.Resource, url string, pod *kubemeta.Pod, svc *kubemeta.Service) {
	res.Attributes().PutStr("url.full", url)
	// The TARGET's address is its instance, exactly as in Prometheus.
	//
	// attrs.Identity otherwise derives service.instance.id from the pod UID,
	// which does not distinguish two targets on ONE pod — a pod annotated with
	// `prometheus.io/port: "8080,9100"`, or two ServiceMonitor endpoints. Both
	// then rendered the same (job, instance), so `up`, scrape_duration_seconds
	// and scrape_samples_scraped arrived twice with the same identity and the
	// same timestamp and disagreeing values: a duplicate series in one payload,
	// which a backend reads as a conflict rather than as two targets. url.full
	// is on the resource but the OTLP→Prometheus translation makes no label of
	// it, so it could not disambiguate anything.
	//
	// Identity never overwrites an instance a caller already set, so setting it
	// here wins. WIRE-VISIBLE: series scraped from annotated pods and monitors
	// change `instance` from the pod UID to host:port at the upgrade boundary —
	// which is the value Prometheus itself would have used, and the reason the
	// two targets were indistinguishable before.
	if inst := targetInstance(url); inst != "" {
		res.Attributes().PutStr("service.instance.id", inst)
	}
	s.attrsFor(pipelineTargets).Build(res, attrs.Context{Pod: pod, Service: svc, Node: s.nodeInfo()})
}

// targetInstance is the host:port of a scrape URL — Prometheus' `instance`.
// Empty when the URL does not parse, in which case the caller leaves the
// derivation to attrs.Identity as before.
func targetInstance(rawURL string) string {
	u, err := neturl.Parse(rawURL)
	if err != nil || u.Host == "" {
		return ""
	}
	return u.Host
}

// resolveContext resolves a described object's pod/container through the metadata
// service — an exact container incarnation by container id, else the pod by
// namespace+name (accepted only when podAnswersFor says the object the API
// server holds is the one the kubelet described under uid) with a named
// container matched within it; a container-name miss stamps
// k8s.container.name on res. It returns
// the built attrs.Context (Node NOT set — the caller adds it) and whether the
// row's IDENTITY resolved; on false the caller writes its own identity fallback.
// The two questions are not the same one: a row naming a container id the store
// does not know yet gets the POD context (Build still enriches from it) together
// with false, because its container identity can only come from the caller's own
// labels. Shared by the cadvisor and split batchers.
//
// The THIRD value classifies a failure for the one caller whose retry policy
// turns on it (FillContainerResource, for internal/agent/cgroupstats): true
// means the metadata service ANSWERED and its answer placed nothing, false
// means it could not answer at all. With no id to look up at all it is true —
// nothing was asked, so there is no outage to wait out and no later attempt
// that could go differently.
func (s *Scraper) resolveContext(ctx context.Context, containerID, namespace, pod, uid, container string, res pcommon.Resource) (attrs.Context, bool, bool) {
	var actx attrs.Context
	if containerID != "" {
		md, answered := s.containerMeta(ctx, containerID)
		if md != nil {
			actx.Pod, actx.Container = &md.Pod, &md.Container
			return actx, true, true
		}
		// The store does not know THIS incarnation — typically because the kubelet
		// has not posted a just-started container's id to the API server yet, and
		// the miss is negative-cached for a minute. Falling through to the pod
		// branch below would match the pod's CURRENT container by NAME and stamp
		// that incarnation's container.id, image and restart_count — hence
		// service.instance.id — onto this row: a dead incarnation's identity on
		// the live one's cumulative counters, and a resource byte-identical to the
		// one the other incarnation is keyed under (both keys carry the container
		// id), i.e. two ResourceMetrics with the same identity in one payload.
		// Take the pod for its owner/label enrichment, but report UNRESOLVED so
		// the caller's identity fallback keeps the row's OWN container id, name
		// and image.
		//
		// The classification stays the CONTAINER lookup's: the service has said
		// this container id is unknown, and whether the pod lookup beside it
		// also reached the service says nothing about that verdict.
		if pod != "" {
			if meta, _ := s.podMeta(ctx, namespace, pod); meta != nil && podAnswersFor(meta, uid) {
				actx.Pod = meta
			}
		}
		return actx, false, answered
	}
	if pod != "" {
		meta, answered := s.podMeta(ctx, namespace, pod)
		if meta != nil && podAnswersFor(meta, uid) {
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
			return actx, true, true
		}
		// A pod that resolved under a DIFFERENT uid is an answer too: this name
		// belongs to another pod now, and asking again in a second changes
		// nothing.
		return actx, false, answered
	}
	return actx, false, true
}

func (s *Scraper) scrapeTarget(ctx context.Context, t kubemeta.ScrapeTarget, timeout time.Duration) (int, error) {
	// The allowance covers a SPLITTER's per-object enrichment, which resolves
	// through the same podMeta/containerMeta seam the kubelet batchers use and
	// on the same scrape context — a KSM target naming thousands of objects is
	// the shape with the most lookups per scrape in the whole agent.
	ctx, cancel := s.scrapeContext(ctx, timeout, pipelineTargets)
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
		// A credential this agent could not RESOLVE, which is a different
		// remedy from one the target refused (see failures.go): the metadata
		// service has to be running -scrape-auth-secrets and this agent has to
		// hold a matching token.
		return 0, classify(reasonAuth, err)
	}
	client, err := s.clientFor(ctx, t, timeout)
	if err != nil {
		return 0, classify(reasonTLS, err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer drainClose(resp.Body)
	if resp.StatusCode != http.StatusOK {
		// The typed form, not fmt.Errorf: it is what lets 401/403 be counted
		// apart from every other non-200 without matching the message text. The
		// rendered string is byte-identical to what it replaced.
		return 0, &statusError{code: resp.StatusCode}
	}

	// The target decides the format; some exporters serve OpenMetrics
	// regardless of Accept, so detect from the response.
	contentType := resp.Header.Get("Content-Type")
	openMetrics := strings.Contains(contentType, "openmetrics")
	s.reportNegotiation(t, useProto, contentType, openMetrics)

	// warnTarget, never the URL: the per-scrape complaints are deduped by the
	// CONFIGURATION that produced the target, so a pod restart neither re-fires
	// them nor grows the shared dedupe table.
	warnKey := warnTarget(t)

	var cb chunker
	if sp := s.splitterFor(t.Pod); sp != nil {
		cb = newSplitBatcher(ctx, s, t, sp, time.Now())
	} else {
		cb = newBatcher(func(res pcommon.Resource) {
			s.fillTargetResource(res, t.URL, &t.Pod, t.Service)
		}, s.cfg.StartTime, time.Now())
	}
	relabel, relabelEvicted, err := s.relabels.session(t.MetricRelabelings)
	if relabelEvicted {
		// Reported here rather than inside the cache: session holds the cache's
		// mutex while it evicts, and every scrape goroutine on the node contends
		// for it. Nothing is lost — the chain recompiles — so this is a Warn
		// about COST, and the realistic cause is a controller templating a
		// distinct regex per monitor, which no counter in this package sees.
		s.warnCacheEviction(&s.relabelEvictWarn, "compiled metricRelabelings chains", maxRelabelChains,
			"more distinct rule chains are in use than the cache holds: a controller is probably minting a templated or hashed regex per monitor, so every scrape recompiles its chain")
	}
	if err != nil {
		// Exporting what the user asked to drop is worse than failing visibly —
		// but the failure has to name the monitor whose regex is broken, or the
		// only evidence is one target permanently down for a reason that is not
		// on the target.
		s.warnOnce("relabel:"+warnTarget(t), "a monitor's metricRelabelings would not compile; the scrape fails rather than export series the rule asked to drop",
			"url", t.URL, "monitor", t.Monitor, "error", err)
		return 0, classify(reasonRelabel, err)
	}
	if strings.Contains(contentType, protoContentType) {
		// Only decode protobuf when the operator OPTED IN (NativeHistograms).
		// The proto path materialises the whole MetricFamily via proto.Unmarshal
		// (no streaming bound like the text front's), and the transport
		// transparently gunzips, so a tiny gzip body inflates to a multi-hundred-MB
		// heap spike that OOMKills the DaemonSet — and the target, not the
		// operator, chooses the response Content-Type. Without the opt-in we
		// sent Accept: text/plain, so a protobuf response is a misbehaving
		// target: fail the scrape visibly (up=0) rather than parse it.
		if !s.cfg.NativeHistograms {
			// Named once per target as well as counted: the scrape fails with
			// up=0 and the operator's next question is whose fault that is —
			// the answer being that the target ignored our Accept header, and
			// that one flag makes it work.
			// The Content-Type is the TARGET's bytes — a header it chose, of
			// whatever length it chose — so it rides through clipForLog like
			// every other value from outside this process.
			s.warnOnce("protorefused:"+warnKey,
				"target served the protobuf exposition although this agent asked for text; refusing to decode it",
				"url", t.URL, "monitor", t.Monitor, "contentType", clipForLog(contentType), "flag", "-scrape-native-histograms")
			return 0, classify(reasonProtoRefused,
				errors.New("target served protobuf but native histograms are not enabled"))
		}
		return s.scrapeProto(ctx, resp.Body, cb, relabel, t.URL, warnKey)
	}
	return s.parseAndExportFiltered(ctx, resp.Body, openMetrics, s.cfg.Exemplars, cb, pipelineTargets, t.URL, warnKey, relabel)
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
	sm.Scope().SetVersion(obs.ScopeVersion)
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
	pipelineSummary  = "summary"
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
	case pipelineSummary:
		// Only the /stats/summary NODE resource and this pipeline's health
		// gauges: the pod, container and volume resources go through
		// fillIdentityResource, which is the cadvisor builder's, so they stay
		// byte-identical to the cadvisor series they join. Without the case,
		// exportHealth's kubelet branch would fall through to the TARGETS builder
		// and emit up{job="kubelet",instance="<node>"} a second time with a
		// different value — one series, two conflicting points, one payload.
		return s.cfg.Attrs.Summary
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
