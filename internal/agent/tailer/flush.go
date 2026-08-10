package tailer

// Batch building and export: grouping records into OTLP payloads,
// enrichment/metrics/rules per record, and the per-segment commit
// bookkeeping handed to commitBatch/failBatch.

import (
	"context"
	"maps"
	"path/filepath"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/logchain"
	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

// logGrouper places records into ResourceLogs/ScopeLogs keyed by (file,
// resource attributes, scope attributes), so line-derived resource/scope
// attributes split records into the right resources. Without line attributes
// there is one scope per file (the plain map — the common case, avoiding the
// per-record key formatting), matching the previous behavior.
type logGrouper struct {
	ld     plog.Logs
	plain  map[*file]group
	scopes map[scopeKey]group
}

// group is one destination: the records slice a kept record is appended to,
// and the RESOURCE that slice hangs under. The resource is carried because a
// log-derived metric must land on the same resource as the records it counts —
// file.resource alone omits the line-lifted resource attributes this group was
// split on.
type group struct {
	sl  plog.ScopeLogs
	res pcommon.Map
}

// bindKey identifies one bound RESOURCE for the log-metrics cache: the file
// plus the line-lifted resource attributes, which are exactly what distinguish
// two groups' resources. Scope attrs are deliberately absent — they land on the
// scope, never the resource, so they cannot split a metric's identity.
type bindKey struct {
	f   *file
	res string
}

// scopeKey identifies one (file, line-derived resource attrs, scope attrs)
// group without the fmt.Sprintf allocation the old string key paid per
// attribute-carrying record.
type scopeKey struct {
	f          *file
	res, scope string
}

func (g *logGrouper) scope(f *file, resAttrs, scopeAttrs []logattrs.Attr) group {
	if len(resAttrs) == 0 && len(scopeAttrs) == 0 {
		if gr, ok := g.plain[f]; ok {
			return gr
		}
		gr := g.newScope(f, nil, nil)
		g.plain[f] = gr
		return gr
	}
	key := scopeKey{f: f, res: logattrs.Key(resAttrs), scope: logattrs.Key(scopeAttrs)}
	if gr, ok := g.scopes[key]; ok {
		return gr
	}
	gr := g.newScope(f, resAttrs, scopeAttrs)
	g.scopes[key] = gr
	return gr
}

// ScopeName is the instrumentation scope stamped on every record the tailer
// exports. Exported so the -test-config harness models the scope production
// ships instead of re-typing the literal (wire-visible: changing it splits
// every log stream at the upgrade boundary in backends that group on it).
const ScopeName = "github.com/JohanLindvall/kubescrape/agent/tailer"

func (g *logGrouper) newScope(f *file, resAttrs, scopeAttrs []logattrs.Attr) group {
	rl := g.ld.ResourceLogs().AppendEmpty()
	f.resource.CopyTo(rl.Resource())
	logattrs.Put(rl.Resource().Attributes(), resAttrs)
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName(ScopeName)
	sl.Scope().SetVersion(obs.ScopeVersion)
	logattrs.Put(sl.Scope().Attributes(), scopeAttrs)
	return group{sl: sl, res: rl.Resource().Attributes()}
}

// recordBuilder bundles the per-flush state for turning batch entries into
// grouped OTLP records: the grouper, and the shared per-record chain
// (internal/agent/logchain) that every log producer in this repo runs — line
// attributes, enrichment, log-metrics, keep/drop rules, in that order.
//
// It is also the chain's Producer: Dest and Stamp are the two things that stay
// with the tailer (where a kept record lands, and what the tailer knows about
// it). They are METHODS on this struct with the current entry in a field —
// deliberately, not closures: the flush path is allocation-pinned and a closure
// per record would be two allocations.
type recordBuilder struct {
	t     *Tailer
	g     *logGrouper
	now   pcommon.Timestamp
	chain *logchain.Chain[bindKey]
	kept  int

	// e and ext are the entry currently being emitted, re-pointed before each
	// Emit the way logchain.Resolver.Set re-points the resolver.
	e   entry
	ext logattrs.Result
	// parsedSev is the parse hook's severity for the CURRENT entry (reset per
	// record in buildRecord; applied in Stamp so the chain's enrichment and
	// rules see it).
	parsedSev string
	// materialised records that a group was created before the rules ran, so
	// the flush knows it may need pruning.
	materialised bool
}

func (t *Tailer) newRecordBuilder(ld plog.Logs) *recordBuilder {
	// anyPodRules is O(files), so evaluate it once (short-circuited when a
	// global rule set already forces the scratch path).
	return &recordBuilder{
		t:   t,
		g:   &logGrouper{ld: ld, plain: map[*file]group{}, scopes: map[scopeKey]group{}},
		now: pcommon.NewTimestampFromTime(time.Now()),
		chain: logchain.NewChain[bindKey](logchain.Config{
			Scrub:      t.cfg.Scrub,
			LogAttrs:   t.cfg.LogAttrs,
			Enrich:     t.cfg.Enrich,
			LogMetrics: t.cfg.LogMetrics,
			Rules:      t.cfg.Rules,
		}, t.cfg.Rules == nil && t.anyPodRules()),
	}
}

// Dest is the chain's lazy landing site: the ResourceLogs/ScopeLogs group for
// this record's file plus its line-derived resource/scope attributes. Called
// only for a KEPT record, so a rules drop never materialises a group.
func (b *recordBuilder) Dest() plog.LogRecordSlice {
	return b.g.scope(b.e.file, b.ext.Resource, b.ext.Scope).sl.LogRecords()
}

// Stamp fills in what the tailer knows about a record.
func (b *recordBuilder) Stamp(lr plog.LogRecord) {
	e := b.e
	if !e.time.IsZero() {
		// A zero time means the line carried none (a non-CRI line reaching the
		// containerd path). Stamping it produces an absurd absolute timestamp
		// rather than "unknown", which a backend either misfiles or rejects;
		// leaving it unset is exactly what OTLP defines for the case, and
		// ObservedTimestamp still carries when we read it.
		lr.SetTimestamp(pcommon.NewTimestampFromTime(e.time))
	}
	lr.SetObservedTimestamp(b.now)
	lr.Body().SetStr(e.body)
	if b.parsedSev != "" {
		lr.SetSeverityText(b.parsedSev)
	}
	if e.stream != "" {
		lr.Attributes().PutStr("log.iostream", e.stream)
	}
	if e.truncated {
		lr.Attributes().PutBool(logchain.AttrTruncated, true)
	}
	if e.match != "" {
		lr.Attributes().PutStr("log.multiline.match", e.match)
	}
	if b.t.cfg.FileAttributes {
		lr.Attributes().PutStr("log.file.name", filepath.Base(e.file.path))
		lr.Attributes().PutInt("log.file.position", e.start.off)
	}
}

// anyPodRules reports whether any tracked file carries annotation rules —
// the flush then needs the scratch slice and resolver even with no global
// rules configured. O(files) once per flush, only reached when the global
// pieces would otherwise be skipped.
func (t *Tailer) anyPodRules() bool {
	for _, f := range t.files {
		if f.podRules != nil {
			return true
		}
	}
	return false
}

// buildRecord renders one batch entry as an OTLP record, through the shared
// chain every log producer in this repo runs (internal/agent/logchain): scrub,
// lift line attributes, stamp, enrich, log-metrics, keep/drop rules.
//
// What is the tailer's alone stays here: the resource (the file's, built once
// at resolve time), the grouping, and the offset bookkeeping the caller does
// around this.
func (t *Tailer) buildRecord(b *recordBuilder, e entry) {
	b.parsedSev = ""
	// The parse hook (plain sources flagged parseScript) runs FIRST: it
	// produces the body the rest of the chain — scrub, extraction, metrics,
	// rules — then reads. Enrichment still runs after and may refine what the
	// script set.
	if t.cfg.ParseLine != nil && e.file.source.parseScript {
		if p, ok := t.cfg.ParseLine(e.body); ok {
			if p.HasBody {
				e.body = p.Body
			}
			if p.TimeUnixNano > 0 {
				e.time = time.Unix(0, p.TimeUnixNano)
			}
			b.parsedSev = p.SeverityText
		}
	}
	// Scrub + extract must precede GROUPING: the extraction's resource/scope
	// halves decide which ResourceLogs/ScopeLogs the record lands in.
	body, ext := b.chain.Line(e.body)
	e.body = body
	b.e, b.ext = e, ext
	// Metric labels and rule keys resolve against the record's attributes
	// first, then this line's lifted resource attributes, then the FILE's
	// resource — which is also the metric's OTLP resource and the bind key.
	// The metric's OTLP resource must be the GROUP's, not the file's: a line
	// that lifts a resource attribute lands under a resource carrying it, and
	// the metric counting that line has to agree — every other producer in this
	// repo passes the group's resource here. The bind key has to distinguish
	// those groups for the same reason, or two lifted variants of one file
	// would share one bound resource.
	//
	// Only the lifted-RESOURCE case pays for it. With none (the overwhelmingly
	// common line, and the one BenchmarkIngestLine pins at zero allocations)
	// the group's resource IS file.resource and the key is the bare file.
	res, key := e.file.resource.Attributes(), bindKey{f: e.file}
	if len(ext.Resource) > 0 {
		gr := b.g.scope(e.file, ext.Resource, ext.Scope)
		res, key = gr.res, bindKey{f: e.file, res: logattrs.Key(ext.Resource)}
		b.materialised = true
	}
	if b.chain.Emit(b, logchain.Input[bindKey]{
		Body:     body,
		Lifted:   ext,
		Resource: res,
		BoundKey: key,
		// Pod-annotation rules run before the global chain: a pod drop is
		// final, a pod keep still passes the global rules.
		PodRules: e.file.podRules,
	}) {
		b.kept++
	}
}

// proposeCandidates folds one entry's end position into the per-file,
// per-segment commit candidates. Segments the entry TRAVERSED on the way
// (start.seg up to but excluding end.seg) are fully covered through their
// recorded end, so their completion is proposed too. A dead segment id
// resolves to nothing at commit.
//
// "Fully covered" holds only while segmentsFed does. A replay stopped by the
// per-sweep byte budget leaves part of a segment UNREAD, and a multi-line
// group can still join across that interruption into the tail — so the entry
// spans a segment whose remainder was never fed. Proposing its recorded `to`
// then committed and RETIRED it (fd closed, checkpoint entry gone) over lines
// nobody had read: silent loss, in the recovery path that exists because
// nothing else can recover those bytes.
//
// The test is PER-SEGMENT (segment.fed), not the file-level segmentsFed: that
// one only turns true after the whole replay pass, so every mid-pass flush saw
// it false — and since this runs once per entry at flush time, gating on it
// DROPPED the claim rather than deferring it, leaving the segment unretirable
// for the process lifetime.
func proposeCandidates(cands map[*file]map[int]int64, e entry) {
	c := cands[e.file]
	if c == nil {
		c = make(map[int]int64)
		cands[e.file] = c
	}
	if e.end.off > c[e.end.seg] {
		c[e.end.seg] = e.end.off
	}
	if e.start.seg != e.end.seg {
		for _, sg := range e.file.segments {
			if sg.id >= e.start.seg && sg.id < e.end.seg && sg.fed && sg.to > c[sg.id] {
				c[sg.id] = sg.to
			}
		}
	}
}

// maybeFlush ships the batch once it has reached Config.BatchSize, and reports
// whether that flush FAILED and rewound f.
//
// Every caller sits inside a read/feed loop over f, and every one of them must
// stop on true: the rewind seeks the fd back to the committed offset and
// discards the pipeline, so reading on re-feeds the same bytes into a batch
// whose export just failed — a hot loop burning export attempts on the single
// sweep goroutine that serves every file on the node.
func (t *Tailer) maybeFlush(ctx context.Context, f *file) bool {
	if len(t.batch) < t.cfg.BatchSize {
		return false
	}
	gen := f.rewindGen
	t.flush(ctx)
	return f.rewindGen != gen
}

func (t *Tailer) flush(ctx context.Context) {
	if len(t.batch) == 0 {
		t.lastFlush = time.Now()
		return
	}
	ld := plog.NewLogs()
	b := t.newRecordBuilder(ld)
	// Per-file, per-segment commit candidates: the max exported end position
	// per segment, plus full-range proposals for segments an entry spans.
	cands := make(map[*file]map[int]int64)
	for _, e := range t.batch {
		t.buildRecord(b, e)
		proposeCandidates(cands, e)
	}
	if b.materialised {
		// buildRecord materialises a group UP FRONT for a line that lifts
		// resource attributes — the metric bound to it needs the group's
		// resource before the rules have decided whether the record survives.
		// If every such record was then dropped, that group is empty. The other
		// producers prune for the same reason; the tailer only has to when it
		// took this path, so the common flush is untouched.
		logchain.Prune(ld)
	}
	kept := b.kept

	// Clamp the candidates to the watermark (the lowest position still
	// buffered in the pipeline stages): a candidate in a segment NEWER than
	// the watermark's commits nothing yet, one in the SAME segment clamps to
	// the watermark offset, and OLDER segments are unconstrained — their
	// bytes precede everything still buffered.
	highs := make(map[*file]map[int]int64, len(cands))
	for f, c := range cands {
		// Re-offer every earlier batch's exported-but-withheld high position:
		// their bytes are already delivered, only the commit was clamped.
		for seg, off := range f.exportedHighs {
			if off > c[seg] {
				c[seg] = off
			}
		}
		// Snapshot the UNCLAMPED candidates per segment, before the watermark
		// clamp below rewrites c in place. One max per file is not enough:
		// each segment is withheld independently.
		highs[f] = maps.Clone(c)
		if wm, buffered := f.watermark(); buffered {
			for seg, off := range c {
				switch {
				case wm.seg < seg:
					delete(c, seg)
				case wm.seg == seg && wm.off < off:
					c[seg] = wm.off
				}
			}
		}
	}

	inf := &batchInfo{
		kept:  kept,
		cands: cands, highs: highs,
	}
	clear(t.batch) // unpin the exported bodies (a burst otherwise stays reachable)
	t.batch = t.batch[:0]
	t.lastFlush = time.Now()
	// An all-dropped batch has nothing to send but its offsets still commit.
	var err error
	if kept > 0 {
		// The transform runs ONCE per batch, in place, before the retry loop:
		// this flush just built ld and nothing else holds it. (Sending through
		// the transform-wrapped exporter instead re-copied the batch and re-ran
		// the script on every retry attempt.) A batch transformed to nothing
		// commits like an all-dropped one; a script error takes the failed-
		// export path below — not permanent, so the files rewind and the
		// re-read next sweep re-runs the possibly hot-reloaded program on a
		// batch rebuilt from source.
		if t.cfg.Transform != nil {
			err = t.cfg.Transform(ld)
		}
		if err == nil {
			// kept becomes the DELIVERED count: a transform may have dropped
			// records (each already counted into transform_dropped_total), and
			// kubescrape_log_entries_total means "entries exported" — counting
			// the pre-transform build size credited records nothing sent. The
			// offsets still commit for all of them.
			inf.kept = ld.LogRecordCount()
			if inf.kept > 0 {
				err = t.exportWithRetry(ctx, ld)
			}
		}
	}
	switch {
	case err == nil:
		t.commitBatch(inf)
	case otlpexport.IsPermanent(err):
		// A definitive rejection (bad payload, unimplemented, over a receiver's
		// body limit) is not survivable by retrying: rebuilding the identical
		// batch next sweep wedges the file at its committed offset FOREVER, and
		// with it every other file on the node — there is one sweep goroutine.
		// Lag then grows unbounded, the fd is pinned against logrotate, and the
		// backlog is lost outright when the file finally rotates away. Every
		// other producer (journald, events, azurediag, ingest, the disk buffer)
		// classifies; the tailer was the one that did not. Drop it and advance,
		// the way journald.flush does, so the pipeline survives the poison
		// record — counted and logged, never silent.
		t.log.Error("dropping a permanently rejected log batch and advancing",
			"records", inf.kept, "error", err)
		obs.LogPermanentDropped.Add(float64(inf.kept))
		// advanceBatch, not commitBatch: these records were NEVER delivered,
		// so they must not land in kubescrape_log_entries_total ("entries
		// exported"), and nothing was rewound here, so
		// kubescrape_log_export_failures_total ("files rewound") must not move
		// either — journald's permanent arm, the stated model, counts only its
		// drop. LogPermanentDropped plus the Error log carry the loss exactly.
		t.advanceBatch(inf)
	default:
		t.failBatch(inf, err)
	}
}

// exportWithRetry sends one batch through the shared bounded-retry shape
// (otlpexport.Retry: sleep-before-retry, doubling backoff, permanent
// rejections returned immediately for the caller to drop-and-advance). This
// used to be a hand-rolled copy that paid its doubled backoff once more AFTER
// the final failure — dead seconds on the single sweep goroutine, and of the
// shutdown budget everything after the tailer shares. The closure costs one
// allocation per FLUSH (amortized over up to BatchSize lines), not per line.
func (t *Tailer) exportWithRetry(ctx context.Context, ld plog.Logs) error {
	return otlpexport.Retry(ctx, 3, t.retryBackoff, func() error {
		return t.cfg.Exporter.ExportLogs(ctx, ld)
	})
}

// batchInfo carries a flushed batch's commit information from build to apply:
// per-file, per-segment committed-offset candidates (already clamped to the
// build-time watermark) and the unclamped high position behind them.
type batchInfo struct {
	kept int
	// cands maps each touched file to its per-segment commit candidates. A
	// segment id that no longer resolves (a truncated-away incarnation, or a
	// segment that completed earlier) commits nothing — the segment-qualified
	// position IS the staleness check.
	cands map[*file]map[int]int64
	// highs is the per-file, PER-SEGMENT unclamped end position: what could
	// commit once nothing is buffered. Recorded as file.exportedHighs on successful
	// commit where the watermark clamp withheld it.
	highs map[*file]map[int]int64
}

// commitBatch advances the committed offsets of a successfully exported
// batch: the tail candidate advances the file checkpoint, older segments'
// candidates advance their own records, and a segment whose whole range is
// now committed retires (fd closed, checkpoint entry gone).
func (t *Tailer) commitBatch(inf *batchInfo) {
	obs.LogEntries.Add(float64(inf.kept))
	t.advanceBatch(inf)
}

// advanceBatch moves the offsets a batch's delivery earned, WITHOUT counting
// its records as exported. Split out for the permanent-rejection path, which
// advances past records that were dropped rather than delivered.
func (t *Tailer) advanceBatch(inf *batchInfo) {
	for f, c := range inf.cands {
		for seg, off := range c {
			if seg == f.tail {
				if off > f.committed {
					f.committed = off
				}
				continue
			}
			if s := f.segmentByID(seg); s != nil && off > s.committed {
				s.committed = off
				// `to` < 0 means OPEN-ENDED: a rotation that happened while
				// the agent was down, whose end replaySegment pins only after
				// it has read to EOF. Every positive offset satisfies
				// `>= -1`, so retiring on this comparison alone closed the
				// segment on its FIRST commit — dropping its fd, its
				// checkpoint entry and its remaining owed range, silently and
				// uncounted. Nothing could commit mid-replay until the replay
				// loop learned to flush, which is what exposed this.
				if s.to >= 0 && s.committed >= s.to {
					f.retire(s)
				}
			}
		}
		// Entries past the committed positions were DELIVERED but their
		// commit was withheld by the build-time watermark clamp; remember the
		// high so a later flush can re-offer it once nothing is buffered.
		f.exportedHighs = nil
		for seg, off := range inf.highs[f] {
			// Keep only what is still AHEAD of that segment's commit frontier,
			// and only ids that still resolve — a retired or truncated-away
			// segment must not be re-offered forever.
			var committed int64
			if seg == f.tail {
				committed = f.committed
			} else if sg := f.segmentByID(seg); sg != nil {
				committed = sg.committed
			} else {
				continue // dead id: nothing to commit against
			}
			if off > committed {
				if f.exportedHighs == nil {
					f.exportedHighs = make(map[int]int64, 1)
				}
				f.exportedHighs[seg] = off
			}
		}
		// This delivery may have retired the last fed line standing between the
		// commit frontier and a run of skipped bytes behind it (a rate-DROPPED
		// or blank tail): those bytes are committable exactly now. Without it
		// `committed` freezes below readPos for the file's life — see
		// file.skipEnd.
		f.absorbSkipped()
	}
}

// failBatch rewinds a failed batch's files to their committed offsets; their
// bytes are re-read after the rewind. (t.batch is always empty here: flush
// clears it before the synchronous export, and nothing appends during it.)
func (t *Tailer) failBatch(inf *batchInfo, err error) {
	t.log.Error("exporting logs failed, rewinding", "records", inf.kept, "error", err)
	obs.LogExportFailures.Inc()
	for f := range inf.cands {
		t.rewind(f)
	}
}
