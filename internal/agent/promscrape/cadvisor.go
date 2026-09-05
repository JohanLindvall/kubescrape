package promscrape

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"strconv"
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
	// Summary scrapes <Endpoint>/stats/summary: the kubelet's JSON stats
	// document, converted into the per-pod, per-container, per-volume and
	// per-node filesystem/ephemeral-storage/process gauges cadvisor does not
	// report (summary.go).
	//
	// It authorizes against a DIFFERENT subresource from the other two — the
	// kubelet checks /stats/* against nodes/stats, not nodes/metrics — which is
	// why the flag behind it is off by default: with the binary rolled ahead of
	// the ClusterRole, an on-by-default scrape would 403 on every node in the
	// fleet every interval.
	Summary bool
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

// batch is the chunk-lifecycle half: what a batcher has to offer for the shared
// BatchPoints/BatchBytes bound (Scraper.chunkFull) to apply to it. It is split
// out of chunker because the /stats/summary batcher builds its points from JSON
// and never receives an exposition Sample — implementing sink there would mean
// three methods that can never be called, while the bound is the half that MUST
// be shared: it is what keeps a chunk under the collector's receive limit, and
// summary.go's one-resource-per-object shape is exactly where an unbounded
// batch goes past it.
type batch interface {
	take() pmetric.Metrics
	count() int
	size() int // estimated encoded size of the accumulated batch
}

// chunker is a sink that also manages batch lifecycles.
type chunker interface {
	sink
	batch
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
	// detail is the text parser's per-cause breakdown of this scrape's
	// malformed count, read off the parser before it is returned to the pool
	// (the protobuf front leaves it zero: its families fail whole, so there is
	// nothing per-line to attribute).
	detail promparse.MalformedDetail
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

// chunkFull reports whether an accumulated chunk hit either batch bound
// (BatchPoints data points or BatchBytes estimated bytes, whichever first). On
// the Scraper rather than on scrapeSession because the /stats/summary scrape
// needs the same bound without the exposition-parse machinery around it.
func (s *Scraper) chunkFull(cb batch) bool {
	return cb.count() >= s.cfg.BatchPoints ||
		(s.cfg.BatchBytes > 0 && cb.size() >= s.cfg.BatchBytes)
}

// full reports whether the accumulated chunk hit either batch bound.
func (ss *scrapeSession) full() bool { return ss.s.chunkFull(ss.cb) }

// export ships the accumulated chunk; a failure latches exportFailed so
// salvage knows re-sending is pointless.
func (ss *scrapeSession) export() error {
	if err := ss.s.cfg.Exporter.ExportMetrics(ss.ctx, ss.cb.take()); err != nil {
		ss.exportFailed = true
		// Classified here, at the one place every chunk of every pipeline
		// ships from: a scrape that parsed perfectly and lost its payload at
		// the COLLECTOR is the failure most often misdiagnosed as a broken
		// target, and it is the only reason on kubescrape_scrape_failures_total
		// where the target is innocent.
		return classify(reasonExport, err)
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
	// The third reason is not a config decision like the other two: the
	// converter refused to hold more of one histogram/summary family
	// (maxFamilyAccBytes). Nobody asked for that drop, so unlike filter/relabel
	// it also warns — deduped per target like every other per-scrape complaint,
	// since a target's exposition does not change between cycles and the counter
	// is the ongoing signal.
	if ss.conv.dropped > 0 {
		obs.ScrapeSamplesDropped.WithLabelValues(ss.pipeline, "accumulator").Add(float64(ss.conv.dropped))
		ss.s.warnOnce("accbudget:"+ss.warnKey,
			"scrape family exceeded the converter's per-family memory budget; the samples past it were dropped",
			"target", ss.what, "dropped", ss.conv.dropped, "samples", ss.samples)
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
	args := []any{"target", ss.what, "malformed", malformed, "samples", ss.samples}
	// The DETAIL is what turns the number into an action: a line over the
	// bound, a body cut mid-stream and an exporter repeating a label name read
	// identically in the count and take three different remedies (see
	// promparse.MalformedDetail). Only the nonzero ones ride, so an ordinary
	// unparseable line still logs exactly what it used to.
	d := ss.detail
	if d.OverLongLines != 0 {
		args = append(args, "overLong", d.OverLongLines, "flag", "-scrape-max-line-bytes")
	}
	if d.TruncatedLines != 0 {
		args = append(args, "truncated", d.TruncatedLines)
	}
	if d.DuplicateLabels != 0 {
		args = append(args, "duplicateLabels", d.DuplicateLabels)
	}
	if d.TooManyLabels != 0 {
		args = append(args, "tooManyLabels", d.TooManyLabels)
	}
	if abortErr != nil {
		args = append(args, "error", abortErr)
	}
	ss.s.warnOnce(msg+":"+ss.warnKey, msg, args...)
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
	ss.detail = parser.MalformedDetail()
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

// The Accept headers the kubelet endpoints are fetched with. Two of the three
// serve Prometheus exposition; /stats/summary serves JSON and is asked for as
// such (the kubelet ignores Accept there, but a request that claims to want
// exposition and parses JSON is a lie a future reader has to untangle).
const (
	acceptExposition = "text/plain;version=0.0.4"
	acceptJSON       = "application/json"
)

// statusError is a non-200 from a scrape — a kubelet endpoint or a discovered
// target — carrying the code so a caller can say something more useful about it
// than the number.
//
// The bare number is not diagnosable for the one status an operator will
// actually meet: the kubelet authorizes each of its endpoints against its own
// subresource, so a 403 on /stats/summary while /metrics/cadvisor succeeds
// means a missing RBAC RULE, not a broken credential — and the text says so
// where the summary scrape catches it. The message is unchanged from the
// fmt.Errorf it replaced, so nothing reading the log line moves.
//
// It is the TARGET path's status error too (it began as the kubelet's alone),
// so failureReason can split 401/403 out of every pipeline's non-200s without
// matching on the message text.
type statusError struct{ code int }

func (e *statusError) Error() string { return "status " + strconv.Itoa(e.code) }

// kubeletGet fetches a kubelet URL with bearer-token authentication, offering
// accept as the request's Accept header. The caller must close the response
// body.
func (s *Scraper) kubeletGet(ctx context.Context, url, accept string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", accept)
	if s.kubeletToken != nil {
		// A read error here fails only a scrape whose credential has NEVER been
		// readable; a transient failure mid-rotation serves the last good token
		// (internal/bearer).
		token, err := s.kubeletToken.Token()
		if err != nil {
			// `auth` rather than `unauthorized`: nothing was sent, so the
			// kubelet refused nothing — the projection at -kubelet-token-file
			// has never been readable by this process (internal/bearer keeps
			// the last good value across a transient failure, so reaching here
			// means there has never been one). The path, never the token.
			s.warnOnce("kubelettoken:"+s.cfg.Kubelet.TokenFile,
				"the kubelet bearer token could not be read; every kubelet scrape on this node will fail",
				"tokenFile", s.cfg.Kubelet.TokenFile, "error", err)
			return nil, classify(reasonAuth, fmt.Errorf("reading token: %w", err))
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := s.kubeletHTTP.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		drainClose(resp.Body)
		return nil, &statusError{code: resp.StatusCode}
	}
	return resp, nil
}

// drainClose reads a bounded remainder of an HTTP body before closing so the
// keep-alive connection can be reused, then closes it.
func drainClose(rc io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(rc, 1<<20))
	_ = rc.Close()
}

// kubeletTimeout is the per-scrape budget for the two kubelet endpoints,
// clamped to the scrape interval exactly as targetTimeout clamps every
// discovered target: cycle() waits for every scrape it started and Run only
// ticks after cycle returns, so a -scrape-timeout longer than -scrape-interval
// stretched the WHOLE NODE's cadence whenever a kubelet hung — the kubelet
// scrapes were the one pair the clamp missed. The clamp lives in the request
// CONTEXT, which wins over the kubelet client's baked-in Timeout (the shorter
// of the two applies; the client's is only the backstop for a request without
// a deadline).
func (s *Scraper) kubeletTimeout() time.Duration {
	if s.cfg.Interval > 0 {
		return min(s.cfg.Timeout, s.cfg.Interval)
	}
	return s.cfg.Timeout
}

// scrapeCadvisor scrapes <kubelet>/metrics/cadvisor. cadvisor series carry
// the pod identity as labels (namespace/pod/container); they are routed into
// one OTLP resource per pod and container, with full metadata resolved
// through the metadata service.
func (s *Scraper) scrapeCadvisor(ctx context.Context) (int, error) {
	ctx, cancel := s.scrapeContext(ctx, s.kubeletTimeout(), pipelineCadvisor)
	defer cancel()

	url := strings.TrimRight(s.cfg.Kubelet.Endpoint, "/") + "/metrics/cadvisor"
	resp, err := s.kubeletGet(ctx, url, acceptExposition)
	if err != nil {
		s.reportKubeletRefusal(pipelineCadvisor, url, err)
		return 0, err
	}
	defer drainClose(resp.Body)

	cb := newCadvisorBatcher(ctx, s, time.Now())
	return s.parseAndExport(ctx, resp.Body, false, false, cb, pipelineCadvisor, url)
}

// scrapeNodeMetrics scrapes <kubelet>/metrics under a node-level resource.
func (s *Scraper) scrapeNodeMetrics(ctx context.Context) (int, error) {
	// No metadata allowance is carved out here and none is needed: this is the
	// one kubelet pipeline that resolves nothing, which is exactly why it kept
	// succeeding throughout the outage metabudget.go quotes.
	ctx, cancel := context.WithTimeout(ctx, s.kubeletTimeout())
	defer cancel()

	url := strings.TrimRight(s.cfg.Kubelet.Endpoint, "/") + "/metrics"
	resp, err := s.kubeletGet(ctx, url, acceptExposition)
	if err != nil {
		s.reportKubeletRefusal(pipelineNode, url, err)
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
		// A backstop only: the effective per-scrape budget is the request
		// context's kubeletTimeout() deadline, which may be SHORTER (clamped to
		// -scrape-interval) and always wins. This raw -scrape-timeout value
		// merely bounds a request that somehow arrives without a deadline.
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
