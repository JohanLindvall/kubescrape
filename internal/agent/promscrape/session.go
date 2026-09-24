package promscrape

// The scrape SESSION: the per-scrape harness every exposition pipeline runs on
// — the Accept offers a scrape is sent with, the series filter and relabel chain
// with their drop accounting, the MaxSamples abort, the chunk bound and the ONE
// site every chunk ships from (exportChunk, which /stats/summary's batcher uses
// too), the salvage-on-abort policy and the malformed accounting — and the two
// parse fronts that drive it: the text front (parseAndExport) and the protobuf
// one (scrapeProto; the decoding is protoparse.go's). The fronts drifted twice
// before the harness existed, which is what it is for.

import (
	"context"
	"io"

	"github.com/JohanLindvall/kubescrape/internal/agent/transform"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/promparse"
)

// The Accept headers every scrape is sent with — one block, so an offer and
// the fallback it names cannot drift apart.
//
// A discovered target is offered what the agent can parse (scrapeTarget):
// classic text, OpenMetrics when -scrape-exemplars wants exemplars, and the
// protobuf exposition first when -scrape-native-histograms opted in (it is the
// only format carrying native histograms). The target decides the format; the
// response's Content-Type, never the offer, selects the parser.
//
// Two of the kubelet's three endpoints serve Prometheus exposition and are asked
// for classic text; /stats/summary serves JSON and is asked for as such (the
// kubelet ignores Accept there, but a request that claims to want exposition
// and parses JSON is a lie a future reader has to untangle).
const (
	acceptExposition  = "text/plain;version=0.0.4"
	acceptOpenMetrics = "application/openmetrics-text;version=1.0.0;q=1," + acceptExposition + ";q=0.5"
	acceptProto       = protoContentType + ";proto=io.prometheus.client.MetricFamily;encoding=delimited;q=1," +
		"application/openmetrics-text;version=1.0.0;q=0.8," + acceptExposition + ";q=0.5"
	acceptJSON = "application/json"
)

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
	// badExemplars is the PROTOBUF front's count of exemplars refused for
	// their label block (the text front keeps its own on the parser and hands
	// it over through MalformedExemplars). Both land on the same counter
	// through reportBadExemplars: the sample was exported either way, so this
	// is never malformed.
	badExemplars int
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
	// Consumed, not just handed off: no retry brings these points back to the
	// script, so a failed chunk's script drops are counted then or never.
	ss := &scrapeSession{s: s, ctx: transform.Consumed(ctx), cb: cb, pipeline: pipeline, what: what, warnKey: warnKey, relabel: relabel, proto: proto}
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

// exportChunk ships one accumulated chunk: the one place every chunk of every
// pipeline ships from — scrapeSession.export for the exposition fronts,
// summaryBatcher.flush for /stats/summary.
//
// Classified here, so no chunk site can leave it out: a scrape that parsed
// perfectly and lost its payload at the COLLECTOR is the failure most often
// misdiagnosed as a broken target, and it is the only reason on
// kubescrape_scrape_failures_total where the target is innocent. Left to
// failureReason's inference, a collector error reads as `other` on gRPC or
// `connect`/`timeout` on OTLP/HTTP — against the scrape's URL, i.e. blaming a
// target (or a kubelet) that answered fine; the summary batcher once shipped
// through a copy of this call that did exactly that.
func (s *Scraper) exportChunk(ctx context.Context, cb batch) error {
	if err := s.cfg.Exporter.ExportMetrics(ctx, cb.take()); err != nil {
		return classify(reasonExport, err)
	}
	return nil
}

// export ships the accumulated chunk; a failure latches exportFailed so
// salvage knows re-sending is pointless.
func (ss *scrapeSession) export() error {
	if err := ss.s.exportChunk(ss.ctx, ss.cb); err != nil {
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
// truncated body, over-cap proto message). Every metric kind here is
// cumulative, so a partial scrape costs only the missing series — whereas
// discarding the conversion threw away everything parsed before the abort (the
// protobuf path did exactly that until it learned the text path's policy; the
// harness exists so the two cannot disagree again). Pointless when the failure
// WAS the export (the collector just rejected a chunk) or when the context is
// gone (the send cannot succeed either).
//
// That second case is the READ TIMEOUT mid-body, which is therefore NOT
// salvaged: no scrape client carries a Timeout of its own that could fire
// before the scrape context's deadline, so a body stalling past the budget
// fails on an expired context and exports only the chunks already flushed —
// the same outcome as Prometheus dropping a timed-out scrape. Giving the
// export a context of its own was rejected (metabudget.go: it would stretch
// the node's cadence), and reserving export time with an earlier read deadline
// would shorten every target's effective timeout; either is a design change,
// not a fix to make here.
func (ss *scrapeSession) salvage() {
	if ss.exportFailed || ss.ctx.Err() != nil {
		return
	}
	if ferr := ss.conv.finish(); ferr == nil && ss.cb.count() > 0 {
		if eerr := ss.export(); eerr != nil {
			// Throttled per target like the scrape-failure line beside it: an
			// aborting target in front of a failing collector is a persisting
			// condition, and unthrottled this was one Warn per such target per
			// cycle on every node. Deliberately NOT keyed by the error — the
			// collector's own outage narration (otlpexport) is where a changing
			// export failure is reported.
			if !ss.s.allowRepeatingWarn("salvage\x00" + ss.pipeline + "\x00" + ss.warnKey) {
				return
			}
			if ss.proto {
				ss.s.log.Warn("exporting partial proto scrape", "pipeline", ss.pipeline, "target", ss.what, "error", eerr)
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
		// No flag raises this bound (the note used to name one no binary
		// registers, which answers `flag provided but not defined` + exit 2
		// to the operator who follows it): the remedy is the exporter.
		args = append(args, "overLong", d.OverLongLines, "maxLineBytes", ss.s.cfg.MaxLineBytes,
			"note", "a line exceeded the per-line bound, which no flag raises: the target is emitting one enormous line (a runaway label value, or no newline)")
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
// An aborted parse (sample limit, a truncated body) still exports what was
// converted before the abort: a partial scrape is worth far more than nothing,
// and every kind here is cumulative, so a missing series simply does not
// appear for that cycle. A read that outlives the scrape budget is the
// exception — its context is gone, so only the chunks already flushed ship
// (see salvage).
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

// scrapeProto runs the protobuf exposition path on the same scrapeSession
// harness the text path uses (proto only ever runs on the targets pipeline).
func (s *Scraper) scrapeProto(ctx context.Context, body io.Reader, cb chunker, relabel *relabelFilter, what, warnKey string) (int, error) {
	ss := s.newScrapeSession(ctx, cb, pipelineTargets, what, warnKey, relabel, true)
	defer ss.reportDropped()
	malformed, err := ss.parseProtoAndExport(body)
	// Before the abort check, like the text front's: a body that tripped the
	// sample limit still carried whatever exemplars were converted before it.
	ss.reportBadExemplars(ss.badExemplars)
	ss.reportMalformed("scrape had malformed proto families", malformed, nil)
	if err != nil {
		return ss.samples, err
	}
	if ss.cb.count() > 0 {
		return ss.samples, ss.export()
	}
	return ss.samples, nil
}
