package promscrape

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/transform"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/promparse"
)

// MetaSource resolves pod and container metadata; implemented by
// metaclient.Client.
type MetaSource interface {
	PodByName(ctx context.Context, namespace, name string) (*kubemeta.Pod, error)
	Container(ctx context.Context, id string, wait time.Duration) (*kubemeta.ContainerMetadata, error)
}

// KubeletConfig configures scraping of the kubelet's metrics endpoints.
type KubeletConfig struct {
	// Endpoint is the kubelet base URL, e.g. "https://10.0.0.5:10250".
	// Empty disables both kubelet scrapes.
	Endpoint string
	// Cadvisor scrapes <Endpoint>/metrics/cadvisor: per-container cgroup
	// metrics, split into one OTLP resource per pod/container.
	Cadvisor bool
	// NodeMetrics scrapes <Endpoint>/metrics: the kubelet's own metrics,
	// exported under a node-level resource.
	NodeMetrics bool
	// TokenFile supplies the bearer token (the mounted ServiceAccount token;
	// it rotates). Empty sends no Authorization. Re-read at most once per
	// minute through internal/bearer, with the last good value kept across a
	// failed re-read — this used to be an os.ReadFile on EVERY kubelet
	// request, so the swap window kubelet opens when it rotates the projection
	// failed the scrape outright.
	TokenFile string
	// InsecureTLS skips certificate verification; kubelet serving
	// certificates are typically self-signed.
	InsecureTLS bool
	// DisableRollups drops the hierarchical cgroup aggregates: series for
	// cgroups above pod level (id "/", "/kubepods", QoS and system slices)
	// and pod-level rows of container-scoped families (the pod cgroup rolls
	// its containers up). Genuinely pod-scoped families
	// (container_network_*, which have no per-container breakdown),
	// container-level series and machine_* are kept.
	DisableRollups bool
	// Meta resolves the pod and container metadata referenced by cadvisor
	// series labels.
	Meta MetaSource
}

// chunker is a sink that also manages batch lifecycles.
type chunker interface {
	sink
	take() pmetric.Metrics
	count() int
	size() int // estimated encoded size of the accumulated batch
}

// defaultBatchBytes bounds one exported chunk well below the 4 MiB default
// gRPC receive limit of a collector (which applies to the decompressed
// message), leaving room for the size estimate's error margin.
const defaultBatchBytes = 3 << 20

// scrapeSession owns the per-scrape state the text and protobuf parse fronts
// share (they drifted twice before it existed — see reportMalformed and
// salvage): the filter session and relabel chain with their drop counters, the
// MaxSamples abort, the exportFailed latch, the batch-full predicate, the
// salvage-on-abort policy and the malformed accounting. State lives in struct
// fields and the per-sample entry points (accept, keep) are METHODS, never
// closures: the keep/emit path runs per sample and must stay 0-alloc
// (TestFilterSessionAllocationBudget is the package's guard on the filter
// half).
type scrapeSession struct {
	s        *Scraper
	ctx      context.Context
	cb       chunker
	conv     *converter
	pipeline string
	what     string
	// warnKey identifies what an operator would have to EDIT to stop a
	// per-scrape complaint, for the warnOnce dedupe table: warnTarget(t) for a
	// discovered target (never the URL — see warnTarget), the flag-derived
	// endpoint for the kubelet scrapes.
	warnKey string
	filter  *filterSession
	relabel *relabelFilter
	proto   bool // selects each front's historical log wording

	samples        int
	droppedFilter  int
	droppedRelabel int
	exportFailed   bool
}

func (s *Scraper) newScrapeSession(ctx context.Context, cb chunker, pipeline, what, warnKey string, relabel *relabelFilter, proto bool) *scrapeSession {
	// Handoff to the transform seam, for every chunk this session exports: a
	// chunk is take()n out of its batcher and never re-sent — a failure
	// latches exportFailed so even salvage skips it, and the next scrape cycle
	// rebuilds from a fresh scrape — so the transform wrapper may run its
	// script in place instead of deep-copying the 3 MiB / 10k-point payload.
	ss := &scrapeSession{s: s, ctx: transform.Handoff(ctx), cb: cb, pipeline: pipeline, what: what, warnKey: warnKey, relabel: relabel, proto: proto}
	ss.filter = s.cfg.Filters.filterFor(pipeline).session()
	ss.conv = newConverter(cb, ss.exportIfFull)
	return ss
}

// full reports whether the accumulated chunk hit either batch bound
// (BatchPoints data points or BatchBytes estimated bytes, whichever first).
func (ss *scrapeSession) full() bool {
	return ss.cb.count() >= ss.s.cfg.BatchPoints ||
		(ss.s.cfg.BatchBytes > 0 && ss.cb.size() >= ss.s.cfg.BatchBytes)
}

// export ships the accumulated chunk; a failure latches exportFailed so
// salvage knows re-sending is pointless.
func (ss *scrapeSession) export() error {
	if err := ss.s.cfg.Exporter.ExportMetrics(ss.ctx, ss.cb.take()); err != nil {
		ss.exportFailed = true
		return err
	}
	return nil
}

// exportIfFull is the converter's emit hook: flush between points once a chunk
// fills (a single family can hold thousands of label sets).
func (ss *scrapeSession) exportIfFull() error {
	if ss.full() {
		return ss.export()
	}
	return nil
}

// flushIfFull additionally finishes the converter first — the protobuf front's
// mid-family flush: a delimited exposition emits one MetricFamily per metric
// holding ALL its series, so checking only at the family boundary would build
// the whole batch in memory and blow BatchBytes for a high-cardinality native
// family (classic samples already flush mid-family via exportIfFull).
func (ss *scrapeSession) flushIfFull() error {
	if !ss.full() {
		return nil
	}
	if err := ss.conv.finish(); err != nil {
		return err
	}
	// finish() emits through the converter's own exportIfFull hook, so the chunk
	// it just drained can have shipped everything and left this one empty. The
	// three sibling export sites all guard; an empty payload costs an OTLP RPC
	// and, with -buffer-dir, a durable queue record.
	if ss.cb.count() == 0 {
		return nil
	}
	return ss.export()
}

// keep applies the per-pipeline filter, then the endpoint's relabel chain.
// Drops are counted in fields and reported once per scrape (reportDropped): a
// WithLabelValues probe here would be on the hot path the whole package's
// allocation discipline is about.
func (ss *scrapeSession) keep(name string, labels []Label) bool {
	if !ss.filter.Keep(name, labels) {
		ss.droppedFilter++
		return false
	}
	if ss.relabel != nil && !ss.relabel.Keep(name, labels) {
		ss.droppedRelabel++
		return false
	}
	return true
}

// accept consumes one classic sample: the text parser's callback and the
// protobuf front's emit.
func (ss *scrapeSession) accept(sample Sample) error {
	ss.samples++
	if ss.s.cfg.MaxSamples > 0 && ss.samples > ss.s.cfg.MaxSamples {
		return ErrTooManySamples
	}
	if !ss.keep(sample.Name, sample.Labels) {
		return nil
	}
	return ss.conv.add(sample)
}

// countNative charges one native-histogram point against MaxSamples — native
// points bypass accept (they go straight to the batcher, not the converter)
// and the cap must bound them too.
func (ss *scrapeSession) countNative() error {
	ss.samples++
	if ss.s.cfg.MaxSamples > 0 && ss.samples > ss.s.cfg.MaxSamples {
		return ErrTooManySamples
	}
	return nil
}

// reportDropped tallies the drop counters into obs.ScrapeSamplesDropped, once
// per scrape from a defer — the abort paths must report too: a scrape that
// tripped the sample limit still filtered everything it parsed.
func (ss *scrapeSession) reportDropped() {
	if ss.droppedFilter > 0 {
		obs.ScrapeSamplesDropped.WithLabelValues(ss.pipeline, "filter").Add(float64(ss.droppedFilter))
	}
	if ss.droppedRelabel > 0 {
		obs.ScrapeSamplesDropped.WithLabelValues(ss.pipeline, "relabel").Add(float64(ss.droppedRelabel))
	}
}

// salvage exports the partially converted scrape after an abort (sample limit,
// truncated body, read timeout mid-body, over-cap proto message). Every metric
// kind here is cumulative, so a partial scrape costs only the missing series —
// whereas discarding the conversion threw away everything parsed before the
// abort (the protobuf path did exactly that until it learned the text path's
// policy; the harness exists so the two cannot disagree again). Pointless when
// the failure WAS the export (the collector just rejected a chunk) or when the
// context is gone (the send cannot succeed either).
func (ss *scrapeSession) salvage() {
	if ss.exportFailed || ss.ctx.Err() != nil {
		return
	}
	if ferr := ss.conv.finish(); ferr == nil && ss.cb.count() > 0 {
		if eerr := ss.export(); eerr != nil {
			if ss.proto {
				ss.s.log.Warn("exporting partial proto scrape", "pipeline", ss.pipeline, "error", eerr)
			} else {
				ss.s.log.Warn("exporting partial scrape", "target", ss.what, "error", eerr)
			}
		}
	}
}

// reportMalformed counts and logs a scrape's rejected lines/families (msg is
// the front's wording; abortErr rides on the text path's abort report). It
// runs on the abort paths too: returning without it made a scrape that tripped
// the sample limit, was truncated, or timed out mid-body report zero malformed
// lines even when the body was garbage — so the one metric that identifies the
// cause never moved, and the protobuf path (which did report it) disagreed
// with the text path on identical input.
//
// The LOG line is deduped per (complaint, target) — nothing about a target's
// broken exposition changes between cycles, and the metric is the ongoing
// signal. Unbounded, it was one Warn per scrape per target forever.
func (ss *scrapeSession) reportMalformed(msg string, malformed int, abortErr error) {
	if malformed == 0 {
		return
	}
	obs.ScrapeMalformed.WithLabelValues(ss.pipeline).Add(float64(malformed))
	if abortErr != nil {
		ss.s.warnOnce(msg+":"+ss.warnKey, msg, "target", ss.what, "malformed", malformed, "samples", ss.samples, "error", abortErr)
	} else {
		ss.s.warnOnce(msg+":"+ss.warnKey, msg, "target", ss.what, "malformed", malformed, "samples", ss.samples)
	}
}

// reportBadExemplars counts a scrape's unparseable exemplar suffixes, apart
// from reportMalformed: the samples carrying them were exported, and
// kubescrape_scrape_malformed_total means data was DROPPED — a target whose
// exemplars alone are broken must not move an operator's data-loss signal, and
// must still be visible as something to fix.
func (ss *scrapeSession) reportBadExemplars(n int) {
	if n == 0 {
		return
	}
	obs.ScrapeExemplarsMalformed.WithLabelValues(ss.pipeline).Add(float64(n))
	ss.s.warnOnce("exemplars:"+ss.warnKey, "scrape had malformed exemplars (samples exported without them)",
		"target", ss.what, "exemplars", n, "samples", ss.samples)
}

// parseAndExport streams one scrape body through the series filter and the
// converter into cb, exporting a chunk whenever BatchPoints data points or
// BatchBytes estimated bytes accumulate. It returns the number of samples
// parsed.
//
// An aborted parse (sample limit, a truncated body, a read timeout mid-body)
// still exports what was converted before the abort: a partial scrape is worth
// far more than nothing, and every kind here is cumulative, so a missing series
// simply does not appear for that cycle.
//
// The kubelet scrapes (and tests) come through here: their `what` is derived
// from a flag rather than from a pod IP, so it is a stable warnOnce key.
func (s *Scraper) parseAndExport(ctx context.Context, body io.Reader, openMetrics, withExemplars bool, cb chunker, pipeline, what string) (int, error) {
	return s.parseAndExportFiltered(ctx, body, openMetrics, withExemplars, cb, pipeline, what, what, nil)
}

// parseAndExportFiltered additionally applies a per-target relabel session
// (monitor endpoints' metricRelabelings; nil = none) and takes the target's
// warnOnce key. The text-format front: the per-scrape policy lives on
// scrapeSession, shared with the protobuf front.
func (s *Scraper) parseAndExportFiltered(ctx context.Context, body io.Reader, openMetrics, withExemplars bool, cb chunker, pipeline, what, warnKey string, relabel *relabelFilter) (int, error) {
	ss := s.newScrapeSession(ctx, cb, pipeline, what, warnKey, relabel, false)
	defer ss.reportDropped()
	parser := promparse.Get(promparse.Options{MaxLineBytes: s.cfg.MaxLineBytes, OpenMetrics: openMetrics, Exemplars: withExemplars})
	defer promparse.Put(parser)
	malformed, err := parser.Parse(body, ss.accept)
	// Read while the parser is still ours (Put hands it to the next scrape),
	// and on the abort path too: a body that tripped the sample limit still
	// carried whatever exemplars were parsed before it.
	ss.reportBadExemplars(parser.MalformedExemplars())
	if err != nil {
		ss.salvage()
		ss.reportMalformed("aborted scrape had malformed lines", malformed+ss.conv.malformed, err)
		return ss.samples, err
	}
	// Reported from a defer (like reportDropped, and like the protobuf front's
	// own accounting) so a failing finish or export cannot swallow it: a target
	// serving partly-garbage exposition to a collector that rejects the chunk
	// moved kubescrape_scrapes_total{outcome="error"} while the one metric
	// naming the CAUSE stayed flat — and the two fronts reported differently for
	// identical input, which is the drift scrapeSession exists to prevent. The
	// converter's own count is read here, after finish has added to it.
	defer func() { ss.reportMalformed("scrape had malformed lines", malformed+ss.conv.malformed, nil) }()
	if err := ss.conv.finish(); err != nil {
		return ss.samples, err
	}
	if ss.cb.count() > 0 {
		if err := ss.export(); err != nil {
			return ss.samples, err
		}
	}
	return ss.samples, nil
}

// kubeletGet fetches a kubelet URL with bearer-token authentication. The
// caller must close the response body.
func (s *Scraper) kubeletGet(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/plain;version=0.0.4")
	if s.kubeletToken != nil {
		// A read error here fails only a scrape whose credential has NEVER been
		// readable; a transient failure mid-rotation serves the last good token
		// (internal/bearer).
		token, err := s.kubeletToken.Token()
		if err != nil {
			return nil, fmt.Errorf("reading token: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := s.kubeletHTTP.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		drainClose(resp.Body)
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	return resp, nil
}

// drainClose reads a bounded remainder of an HTTP body before closing so the
// keep-alive connection can be reused, then closes it.
func drainClose(rc io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(rc, 1<<20))
	_ = rc.Close()
}

// scrapeCadvisor scrapes <kubelet>/metrics/cadvisor. cadvisor series carry
// the pod identity as labels (namespace/pod/container); they are routed into
// one OTLP resource per pod and container, with full metadata resolved
// through the metadata service.
func (s *Scraper) scrapeCadvisor(ctx context.Context) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	url := strings.TrimRight(s.cfg.Kubelet.Endpoint, "/") + "/metrics/cadvisor"
	resp, err := s.kubeletGet(ctx, url)
	if err != nil {
		return 0, err
	}
	defer drainClose(resp.Body)

	cb := newCadvisorBatcher(s, time.Now(), ctx)
	return s.parseAndExport(ctx, resp.Body, false, false, cb, pipelineCadvisor, url)
}

// scrapeNodeMetrics scrapes <kubelet>/metrics under a node-level resource.
func (s *Scraper) scrapeNodeMetrics(ctx context.Context) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	url := strings.TrimRight(s.cfg.Kubelet.Endpoint, "/") + "/metrics"
	resp, err := s.kubeletGet(ctx, url)
	if err != nil {
		return 0, err
	}
	defer drainClose(resp.Body)

	b := newBatcher(func(res pcommon.Resource) {
		a := res.Attributes()
		a.PutStr("service.name", "kubelet")
		a.PutStr("url.full", url)
		s.attrsFor(pipelineNode).Build(res, attrs.Context{Node: s.nodeInfo()})
	}, s.cfg.StartTime, time.Now())
	return s.parseAndExport(ctx, resp.Body, false, false, b, pipelineNode, url)
}

// metaSource returns the metadata source shared by the kubelet scrapes and
// the splitters. Splitters require Kubelet.Meta to be set even when the
// kubelet scrapes are disabled.
func (s *Scraper) metaSource() MetaSource {
	return s.cfg.Kubelet.Meta
}

// newKubeletHTTPClient builds the TLS client for the kubelet.
func newKubeletHTTPClient(cfg KubeletConfig, timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		// The rationale is written out at targetauth.go's noRedirect: Go does
		// not strip Authorization across a same-host https->http redirect. This
		// is the ONE scrape client that was not updated — and the one that
		// attaches the node's ServiceAccount token.
		CheckRedirect: noRedirect,
		Transport: &http.Transport{
			TLSClientConfig:     &tls.Config{InsecureSkipVerify: cfg.InsecureTLS},
			MaxIdleConnsPerHost: 1,
			IdleConnTimeout:     90 * time.Second,
		},
	}
}
