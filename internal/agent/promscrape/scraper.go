package promscrape

import (
	"context"
	"crypto/tls"
	"errors"
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
	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
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
	Node     string
	Interval time.Duration
	// Timeout is the per-target scrape budget (0 or negative = the default,
	// defaultScrapeTimeout). It is NOT read as "no timeout": targetTimeout
	// takes the MINIMUM of it and the intervals, so a non-positive value is an
	// already-expired context — every target and every kubelet scrape then fail
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
	// Auth resolves every secret ref a monitor endpoint carries — bearerTokenSecret,
	// basicAuth, authorization credentials and the tlsConfig CA/cert/key
	// (metaclient; nil = a target carrying any such ref fails its scrape with
	// an error).
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
	// kubeletURLs are the kubelet scrape URLs, derived once from
	// Kubelet.Endpoint (see kubeletURLs).
	kubeletURLs kubeletURLs
	// kubeletToken is the mounted ServiceAccount token presented to the kubelet
	// (nil when Kubelet.TokenFile is unset).
	kubeletToken *bearer.File
	// podCache backs every metadata lookup that goes through resolveContext —
	// the cadvisor and /stats/summary batchers, the splitters and
	// FillContainerResource (internal/agent/cgroupstats) — which run on
	// concurrent scrape goroutines and the sampler's own. cacheSwept is when
	// the expiry sweep last ran (see evictCacheLocked).
	cacheMu    sync.Mutex
	podCache   map[string]podCacheEntry
	cacheSwept time.Time

	// status is the last completed cycle's per-target outcomes, served on the
	// agent's GET /debug/targets (see status.go).
	status atomic.Pointer[CycleStatus]

	// tlsClients caches per-target transports keyed by their resolved TLS
	// material (see clientFor); targets sharing a CA share a connection pool.
	tlsMu      sync.Mutex
	tlsClients map[string]tlsClientEntry

	// Per-target scheduling (schedule.go): due holds each target's next scrape
	// time and targetIntervals its resolved period (both keyed by scheduleKey,
	// rebuilt every cycle so vanished targets drop out). Written only by the
	// cycle goroutine, but Run reads the intervals to size its ticker, so they
	// are guarded.
	dueMu           sync.Mutex
	due             map[string]time.Time
	targetIntervals map[string]time.Duration
	// warned dedupes per-target complaints — keyed by CONFIGURATION origin,
	// never by URL, and bounded (see warnOnce/warnTarget). The table (and its
	// suppress-never-clear saturation policy) is internal/logdedupe, shared
	// with the metadata service's scrape-auth throttle — the two were written
	// separately and disagreed on exactly that policy.
	warned *logdedupe.Table
	// metaBudgetWarn throttles the exhausted-metadata-allowance warning
	// (metabudget.go). Keyless and re-arming, unlike warned: the condition is
	// an OUTAGE rather than a configuration mistake, so it is worth saying
	// again the next time it happens. ONE GATE PER PIPELINE (indexed by
	// metaBudgetSlot), for the reason tlsEvictWarn and relabelEvictWarn are
	// two: a metadata-service blackhole exhausts the cadvisor and summary
	// allowances in the same cycle, and one shared gate let whichever scrape
	// ended first take the slot every window — the other pipeline's
	// `unattributed=` count, which is what the line is for, never appeared.
	metaBudgetWarn [len(metaBudgetPipelines) + 1]logdedupe.Throttle
	// failWarned throttles the per-target "scrape failed" line (failures.go).
	// A SECOND table rather than `warned`, because the two windows differ on
	// purpose: a configuration complaint is once per process (nothing changes
	// until a CR is edited), while a failing scrape re-warns, an operator
	// having fixed it out of band being the expected outcome.
	//
	// Both tables are built in New and never reassigned: they are internally
	// synchronised, so every scrape goroutine's complaint goes straight to the
	// table instead of first contending for a mutex (the schedule's, for
	// warned) that existed only to create it lazily.
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
	// lastGoodTargets is the RAW list (before the targets: hook) the last
	// SUCCESSFUL fetch returned, and lastGoodAt when; a failed fetch reuses it
	// for up to maxStaleTargetList (see fetchTargets). The slice is metaclient's
	// cached value under its treat-as-immutable contract, which the hook honours
	// by returning a fresh slice. Written only by the cycle goroutine.
	lastGoodTargets []kubemeta.ScrapeTarget
	lastGoodAt      time.Time
	// fetchFail narrates a failing target-list fetch (fetchTargets): a
	// transition Error, a throttled repeat, a recovery Info. Cycle goroutine
	// only, apart from the throttle's own atomic.
	fetchFail targetFetchState
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
	// healthExportWarn throttles exportHealth's failure line: the export runs
	// once per cycle on every node, and a collector outage — which otlpexport
	// already narrates with a transition, a re-warn and a recovery — made it a
	// Warn per node per cycle for as long as the outage lasted.
	healthExportWarn logdedupe.Throttle

	// insecureHTTP serves monitor endpoints with tlsConfig.insecureSkipVerify.
	insecureHTTP *http.Client
	// authCache holds EVERY scrape-auth secret ref this node has resolved, by
	// "ns/name/key": bearer, basic-auth and authorization credentials, and the
	// tlsConfig CA bundles, client certificates and client PRIVATE KEYS. Fresh
	// for authCacheTTL, retained while still asked for up to authStaleGrace (see
	// authToken), and swept on every lookup AND every cycle
	// (sweepAuthCacheLocked), so the material does not outlive its use. Scrapes
	// run on concurrent goroutines, hence authMu.
	authMu    sync.Mutex
	authCache map[string]authCacheEntry
	// authStaleWarn throttles the "serving a retained credential" warning. One
	// keyless gate: the condition is a metadata-service or API-server OUTAGE,
	// and every ref on the node takes the path at once.
	authStaleWarn logdedupe.Throttle
	// relabels caches monitor endpoints' compiled metricRelabelings.
	relabels relabelCache
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
		cfg:  cfg,
		http: newScrapeClient(nil, scrapeMaxIdlePerHost),
		// For monitor endpoints declaring tlsConfig.insecureSkipVerify:
		// scoped to those targets only, never the default.
		insecureHTTP: newScrapeClient(&tls.Config{InsecureSkipVerify: true}, scrapeMaxIdlePerHost),
		tlsClients:   map[string]tlsClientEntry{},
		log:          log,
		kubeletHTTP:  newKubeletHTTPClient(cfg.Kubelet, cfg.Timeout),
		kubeletURLs:  newKubeletURLs(cfg.Kubelet.Endpoint),
		podCache:     make(map[string]podCacheEntry),
		authCache:    make(map[string]authCacheEntry),
		warned:       logdedupe.New(maxWarnKeys, 0),
		failWarned:   logdedupe.New(maxScrapeFailKeys, scrapeFailWarnEvery),
	}
	if cfg.Kubelet.TokenFile != "" {
		// Not read here: a projection that is not yet mounted must not stop the
		// agent, and kubeletGet surfaces the error on the scrape it belongs to.
		sc.kubeletToken = bearer.NewFile(cfg.Kubelet.TokenFile, log)
	}
	return sc
}

// maxStaleTargetList bounds how long a failed target-list fetch is answered
// with the list the last SUCCESSFUL fetch returned.
//
// Reuse exists because the metadata service is a singleton (one replica, a PDB
// allowing it to be evicted), so a node drain or a rollout is enough to fail
// every agent's fetch for as long as the replacement takes to become ready —
// and scraping NOTHING for that long is a hole in every discovered target's
// data while the targets themselves are perfectly healthy. The bound exists
// because a list is a snapshot of pod IPs: a pod deleted during the outage
// leaves its address to be recycled, and pod CIDRs are per NODE, so reuse on
// this very node is the likely case. For up to this long, then, a recycled
// address can be scraped under the dead pod's identity (its labels, its
// service.name, its `job`). Five minutes is past an ordinary rollout while
// keeping that window short; past it the agent stops scraping discovered
// targets until the service answers, as it always did.
const maxStaleTargetList = 5 * time.Minute

// targetFetchWarnEvery re-states a target fetch that keeps failing.
const targetFetchWarnEvery = time.Minute

// targetFetchState narrates a failing target-list fetch the way the tailer
// narrates a failing export: an Error on the transition, a repeat at most once
// per targetFetchWarnEvery carrying the failure count and the outage length,
// an Error when the stale list stops being reused, and an Info on recovery.
// Unthrottled, the failure was one Error per cycle per node — at a tick that
// can be as short as 1s, since the intervals a monitor asked for stay in force
// while the fetch fails.
type targetFetchState struct {
	failing  bool
	failures int
	since    time.Time
	warn     logdedupe.Throttle
	// reusing records whether the previous failed cycle reused the stale list,
	// so its EXPIRY is reported once as a transition of its own.
	reusing bool
}

// fetchTargets returns this cycle's discovered targets (after the targets:
// hook) and whether they are to be SCHEDULED — committed as the per-target
// schedule and treated by publishStatus as the current list. That is true for
// a fresh list and for a reused one; false only when the fetch failed and there
// is no usable last-known list, in which case the caller leaves the target
// schedule untouched for the next successful cycle.
//
// A reused list does NOT go through reportTargetSet: kubescrape_scrape_targets
// keeps its "last successful fetch" meaning, and the empty-list narration must
// not read an outage as discovery.
func (s *Scraper) fetchTargets(ctx context.Context) ([]kubemeta.ScrapeTarget, bool) {
	targets, err := s.cfg.Targets.NodeTargets(ctx, s.cfg.Node)
	if err == nil {
		s.noteTargetFetchRecovered(len(targets))
		s.lastGoodTargets, s.lastGoodAt = targets, time.Now()
		targets = s.applyTargetHook(targets)
		s.reportTargetSet(targets)
		return targets, true
	}
	if ctx.Err() != nil {
		// Shutdown cut the fetch short: not an outage, and nothing is scraped
		// from here on anyway (spawn refuses a done context).
		return nil, false
	}
	now := time.Now()
	reuse := !s.lastGoodAt.IsZero() && now.Sub(s.lastGoodAt) < maxStaleTargetList
	s.noteTargetFetchFailed(err, reuse, now)
	if !reuse {
		return nil, false
	}
	// The hook runs again rather than its last output being kept: the
	// transforms file may have hot-reloaded since.
	return s.applyTargetHook(s.lastGoodTargets), true
}

// applyTargetHook runs the transforms file's targets: hook, if any.
func (s *Scraper) applyTargetHook(targets []kubemeta.ScrapeTarget) []kubemeta.ScrapeTarget {
	if s.cfg.TargetHook == nil {
		return targets
	}
	// The hook can drop targets, and a dropped target is indistinguishable from
	// one the metadata service never returned: same empty list, same silence.
	// Report the difference so a transforms-file mistake is not diagnosed as a
	// discovery problem.
	before := len(targets)
	targets = s.cfg.TargetHook(targets)
	if n := before - len(targets); n != 0 {
		s.log.Debug("the transforms file's targets: hook changed the target list",
			"node", s.cfg.Node, "targets", len(targets), "dropped", n)
	}
	return targets
}

// noteTargetFetchFailed narrates one failed fetch; see targetFetchState.
// kubescrape_metadata_requests_total{outcome="error"} is the ongoing signal.
func (s *Scraper) noteTargetFetchFailed(err error, reusing bool, now time.Time) {
	f := &s.fetchFail
	f.failures++
	wasReusing := f.reusing
	f.reusing = reusing
	reuseArgs := func(args []any) []any {
		if reusing {
			return append(args, "targets", len(s.lastGoodTargets), "grace", maxStaleTargetList,
				"note", "scraping the last known target list meanwhile")
		}
		return append(args, "note", "no discovered target is scraped until the metadata service answers; the kubelet scrapes continue")
	}
	if !f.failing {
		f.failing, f.since = true, now
		// The transition CLAIMS the throttle's window, so the next cycle of the
		// same outage does not open it with a second line saying the same thing.
		f.warn = logdedupe.Throttle{}
		f.warn.Allow(targetFetchWarnEvery)
		s.log.Error("fetching scrape targets failed", reuseArgs([]any{"node", s.cfg.Node, "error", err})...)
		return
	}
	outage := now.Sub(f.since).Round(time.Second)
	if wasReusing && !reusing {
		// Its own transition, not a throttled repeat: this is the moment the
		// outage starts costing discovered targets their data.
		s.log.Error("the last known scrape target list is too old to reuse; discovered targets are no longer scraped",
			"node", s.cfg.Node, "error", err, "failures", f.failures, "outage", outage, "grace", maxStaleTargetList)
		return
	}
	if f.warn.Allow(targetFetchWarnEvery) {
		s.log.Error("fetching scrape targets is still failing",
			reuseArgs([]any{"node", s.cfg.Node, "error", err, "failures", f.failures, "outage", outage})...)
	}
}

// noteTargetFetchRecovered is the recovery half: without it an outage ends in
// a silence indistinguishable from the scraper having stopped.
func (s *Scraper) noteTargetFetchRecovered(targets int) {
	f := &s.fetchFail
	if !f.failing {
		return
	}
	s.log.Info("fetching scrape targets recovered", "node", s.cfg.Node, "targets", targets,
		"failures", f.failures, "outage", time.Since(f.since).Round(time.Second))
	f.failing, f.failures, f.since, f.reusing = false, 0, time.Time{}, false
}

func (s *Scraper) cycle(ctx context.Context) {
	// Release aged-out secret material on a cadence that does not depend on a
	// target still asking for it (see sweepAuthCacheLocked), and superseded
	// per-target TLS clients likewise (see sweepTLSClients).
	s.sweepAuthCache()
	s.sweepTLSClients()

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
		wg.Go(func() {
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
		})
		return true
	}
	// This cycle's schedule. It starts with the kubelet keys and grows as the
	// targets are added below; the size hint is len(kubeletDueKeys), derived
	// from the authoritative list (a literal was a second copy of it and went
	// stale when /stats/summary added a third key).
	due := make(map[string]time.Time, len(kubeletDueKeys))
	// Every scrape of the cycle is gated by dueNow, the kubelet scrapes
	// included: they are the most expensive on the node, and Run's tick is set
	// by whatever target asks for the finest cadence, so leaving them
	// unscheduled made one 10s monitor re-clock /metrics/cadvisor for the whole
	// fleet.
	//
	// The kubelet scrapes are spawned BEFORE the target list is fetched,
	// because they do not depend on it. The fetch is bounded only by the
	// metadata client's own timeout (15s at the defaults), so spawning after it
	// made a blackholed metadata service delay every kubelet pipeline by that
	// much each cycle — /metrics included, which resolves nothing — and made
	// the cycle cost fetch PLUS scrapes instead of the longer of the two.
	if s.cfg.Kubelet.Endpoint != "" {
		now := time.Now()
		if s.cfg.Kubelet.Cadvisor && s.dueNow(due, now, dueKeyCadvisor, s.cfg.Interval) {
			spawn(pipelineCadvisor, s.kubeletURLs.cadvisor, nil, s.scrapeCadvisor)
		}
		if s.cfg.Kubelet.NodeMetrics && s.dueNow(due, now, dueKeyNode, s.cfg.Interval) {
			spawn(pipelineNode, s.kubeletURLs.node, nil, s.scrapeNodeMetrics)
		}
		if s.cfg.Kubelet.Summary && s.dueNow(due, now, dueKeySummary, s.cfg.Interval) {
			spawn(pipelineSummary, s.kubeletURLs.summary, nil, s.scrapeSummary)
		}
	}

	var targets []kubemeta.ScrapeTarget
	targetsOK := s.cfg.DisableTargets // nothing to schedule when targets are off
	if !s.cfg.DisableTargets {
		targets, targetsOK = s.fetchTargets(ctx)
	}
	intervals := make(map[string]time.Duration, len(targets))
	// The targets are judged against a clock read AFTER the fetch: one read
	// before it would be stale by however long the fetch took, so a target
	// that fell due meanwhile would be judged not due, and one that was due
	// would have its next due time stamped early by the same amount.
	now := time.Now()

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
		if !s.dueNow(due, now, key, iv) {
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
		// A fresh list, or a reused last-known one (fetchTargets), is committed
		// alike. A failed fetch with NO reusable list leaves `targets` empty;
		// committing that as the schedule would discard every due time and
		// re-scrape everything next cycle, ignoring the intervals entirely.
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
			s.fillKubeletResource(res, o.pipeline, o.url)
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
	// Bounded like every other export in the cycle. This one runs AFTER
	// wg.Wait, on Run's own un-deadlined context, so it was the one export no
	// scrape budget reached: on an unbuffered chain a destination that HANGS
	// (a blackholed collector, one tenant's route) held the node's whole scrape
	// loop for the exporter's retry budget — ~48s at the defaults, against a
	// 30s interval — every cycle, since Run cannot tick while cycle() runs.
	// kubeletTimeout is the cycle's own clamp (min of -scrape-timeout and
	// -scrape-interval), so a cycle now costs at most two budgets. Cutting the
	// retries short loses nothing: the payload is rebuilt every cycle and never
	// re-sent.
	hctx, cancel := context.WithTimeout(ctx, s.kubeletTimeout())
	defer cancel()
	// Handoff: md is fresh per cycle and a failure is only warned about, never
	// re-sent — the next cycle rebuilds it — so the transform seam may run in
	// place (Consumed: the next cycle's points are new ones).
	//
	// The warn reads the PARENT context: a shutdown stays silent, while the
	// health export's own deadline expiring is exactly the hang worth a line.
	if err := s.cfg.Exporter.ExportMetrics(transform.Consumed(hctx), md); err != nil && ctx.Err() == nil &&
		s.healthExportWarn.Allow(scrapeFailWarnEvery) {
		s.log.Warn("exporting scrape health metrics", "error", err,
			"note", "throttled while it persists; kubescrape_export_requests_total is the ongoing signal")
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
		req.Header.Set("Accept", acceptOpenMetrics)
	default:
		req.Header.Set("Accept", acceptExposition)
	}
	if err := s.applyAuth(ctx, req, t); err != nil {
		// A credential this agent could not RESOLVE, which is a different
		// remedy from one the target refused (see failures.go): the metadata
		// service has to be running -scrape-auth-secrets and this agent has to
		// hold a matching token.
		return 0, classify(reasonAuth, err)
	}
	client, err := s.clientFor(ctx, t)
	if err != nil {
		return 0, classify(reasonTLS, err)
	}
	// The relabel chain is compiled BEFORE the request is sent: it depends on
	// nothing but the target's rules, so a chain that will not compile fails
	// this scrape whatever the target answers. Compiled after client.Do, a
	// broken monitor still cost a full HTTP scrape of every matched target
	// every cycle — plus a fresh TCP+TLS handshake whenever the body was over
	// drainClose's bound — only to throw the response away.
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
