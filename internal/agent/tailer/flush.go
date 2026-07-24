package tailer

// Batch building and export: grouping records into OTLP payloads,
// enrichment/metrics/rules per record, and the per-segment commit
// bookkeeping handed to commitBatch/failBatch.

import (
	"context"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/logenrich"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
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
	plain  map[*file]plog.ScopeLogs
	scopes map[scopeKey]plog.ScopeLogs
}

// scopeKey identifies one (file, line-derived resource attrs, scope attrs)
// group without the fmt.Sprintf allocation the old string key paid per
// attribute-carrying record.
type scopeKey struct {
	f          *file
	res, scope string
}

func (g *logGrouper) scope(f *file, resAttrs, scopeAttrs []logattrs.Attr) plog.ScopeLogs {
	if len(resAttrs) == 0 && len(scopeAttrs) == 0 {
		if sl, ok := g.plain[f]; ok {
			return sl
		}
		sl := g.newScope(f, nil, nil)
		g.plain[f] = sl
		return sl
	}
	key := scopeKey{f: f, res: logattrs.Key(resAttrs), scope: logattrs.Key(scopeAttrs)}
	if sl, ok := g.scopes[key]; ok {
		return sl
	}
	sl := g.newScope(f, resAttrs, scopeAttrs)
	g.scopes[key] = sl
	return sl
}

func (g *logGrouper) newScope(f *file, resAttrs, scopeAttrs []logattrs.Attr) plog.ScopeLogs {
	rl := g.ld.ResourceLogs().AppendEmpty()
	f.resource.CopyTo(rl.Resource())
	logattrs.Put(rl.Resource().Attributes(), resAttrs)
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("github.com/JohanLindvall/kubescrape/agent/tailer")
	logattrs.Put(sl.Scope().Attributes(), scopeAttrs)
	return sl
}

// metricResolver resolves metric label/value keys for one record: the record's
// attributes (line-derived + enriched) first, then the file's resource
// attributes (k8s metadata). The two closures are bound once per flush; per
// record only the rec/res fields change, so record evaluation allocates no
// closures.
type metricResolver struct {
	rec, res pcommon.Map
	sev      string // lowercased severity text, for __severity__
	labelFn  func(string) string
	valueFn  func(string) (float64, bool)
	ruleFn   func(string) string
}

func newMetricResolver() *metricResolver {
	r := &metricResolver{}
	r.labelFn = r.label
	r.valueFn = r.value
	r.ruleFn = r.ruleLookup
	return r
}

// ruleLookup is the label resolver for log rules: the synthetic __severity__
// key (the enriched severity text, lowercased) plus the usual record/resource
// attribute chain.
func (r *metricResolver) ruleLookup(k string) string {
	if k == "__severity__" {
		return r.sev
	}
	return r.label(k)
}

func (r *metricResolver) lookup(k string) (pcommon.Value, bool) {
	if v, ok := r.rec.Get(k); ok {
		return v, true
	}
	return r.res.Get(k)
}

func (r *metricResolver) label(k string) string {
	if v, ok := r.lookup(k); ok {
		return v.AsString()
	}
	return ""
}

func (r *metricResolver) value(k string) (float64, bool) {
	v, ok := r.lookup(k)
	if !ok {
		return 0, false
	}
	switch v.Type() {
	case pcommon.ValueTypeDouble:
		return v.Double(), true
	case pcommon.ValueTypeInt:
		return float64(v.Int()), true
	default:
		f, err := strconv.ParseFloat(v.AsString(), 64)
		return f, err == nil
	}
}

// flush exports the batch. On success offsets are committed; on failure the
// files are rewound to the committed offsets so the data is re-read.
// recordBuilder bundles the per-flush state for turning batch entries into
// grouped OTLP records: the grouper, the shared key resolver, the per-file
// bound metric handles, and the one-record scratch slice used when rules may
// drop a record before it materializes a resource/scope.
type recordBuilder struct {
	g        *logGrouper
	now      pcommon.Timestamp
	bound    map[*file]metrics.BoundResource
	resolver *metricResolver
	scratch  plog.LogRecordSlice
	kept     int
}

func (t *Tailer) newRecordBuilder(ld plog.Logs) *recordBuilder {
	b := &recordBuilder{
		g:   &logGrouper{ld: ld, plain: map[*file]plog.ScopeLogs{}, scopes: map[scopeKey]plog.ScopeLogs{}},
		now: pcommon.NewTimestampFromTime(time.Now()),
	}
	// Per-file bound metric state (resource hash computed once per file) and
	// one reusable key resolver for the whole flush.
	if t.cfg.LogMetrics != nil || t.cfg.Rules != nil || t.anyPodRules() {
		b.resolver = newMetricResolver()
	}
	if t.cfg.LogMetrics != nil {
		b.bound = make(map[*file]metrics.BoundResource) // read only on the LogMetrics path
	}
	// With rules configured, records are built in a one-record scratch slice
	// and only MOVED into the batch when kept, so drops never materialize a
	// resource/scope. Without rules they are built in place, as before.
	if t.cfg.Rules != nil || t.anyPodRules() {
		b.scratch = plog.NewLogs().ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	}
	return b
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

// buildRecord renders one batch entry as an OTLP record (attribute stamping,
// enrichment, log-metrics, rules) into the right resource/scope group.
func (t *Tailer) buildRecord(b *recordBuilder, e entry) {
	// Scrub FIRST: everything downstream copies from the body — logattrs
	// lifts fields into attributes, enrich slices exception.stacktrace out of
	// it, log-metrics extract label values — and a secret must not survive
	// into any of them.
	if t.cfg.Scrub != nil {
		e.body = t.cfg.Scrub.Scrub(e.body)
	}
	// Extract configured line attributes; resource/scope ones drive the
	// grouping so records land under the right ResourceLogs/ScopeLogs.
	var extracted logattrs.Result
	if t.cfg.LogAttrs != nil {
		extracted = t.cfg.LogAttrs.Extract(e.body)
	}
	var lr plog.LogRecord
	if t.cfg.Rules != nil || e.file.podRules != nil {
		lr = b.scratch.AppendEmpty()
	} else {
		lr = b.g.scope(e.file, extracted.Resource, extracted.Scope).LogRecords().AppendEmpty()
		b.kept++
	}
	lr.SetTimestamp(pcommon.NewTimestampFromTime(e.time))
	lr.SetObservedTimestamp(b.now)
	lr.Body().SetStr(e.body)
	if e.stream != "" {
		lr.Attributes().PutStr("log.iostream", e.stream)
	}
	if e.truncated {
		lr.Attributes().PutBool("log.truncated", true)
	}
	if e.match != "" {
		lr.Attributes().PutStr("log.multiline.match", e.match)
	}
	if t.cfg.FileAttributes {
		lr.Attributes().PutStr("log.file.name", filepath.Base(e.file.path))
		lr.Attributes().PutInt("log.file.position", e.start.off)
	}
	logattrs.Put(lr.Attributes(), extracted.Log)
	if t.cfg.Enrich {
		logenrich.Apply(lr, e.body)
	}
	if t.cfg.LogMetrics != nil {
		// Metric label/value keys resolve against the record's attributes
		// (line-derived + enriched) first, then the file's resource
		// attributes (k8s metadata); the file's resource attributes become
		// the metric's OTLP resource (hashed once per file via Bind).
		bm, ok := b.bound[e.file]
		if !ok {
			bm = t.cfg.LogMetrics.Bind(e.file.resource.Attributes())
			b.bound[e.file] = bm
		}
		b.resolver.rec, b.resolver.res = lr.Attributes(), e.file.resource.Attributes()
		bm.Add(b.resolver.valueFn, b.resolver.labelFn, e.body)
	}
	if t.cfg.Rules != nil || e.file.podRules != nil {
		b.resolver.rec, b.resolver.res = lr.Attributes(), e.file.resource.Attributes()
		b.resolver.sev = lowerSeverity(lr.SeverityText())
		// Pod-annotation rules first, then the global chain: each is
		// first-match-wins on its own, a pod drop is final, a pod keep still
		// passes through the global rules.
		keep := e.file.podRules == nil || e.file.podRules.Keep(b.resolver.ruleFn, e.body)
		if keep && t.cfg.Rules != nil {
			keep = t.cfg.Rules.Keep(b.resolver.ruleFn, e.body)
		}
		if keep {
			b.scratch.MoveAndAppendTo(b.g.scope(e.file, extracted.Resource, extracted.Scope).LogRecords())
			b.kept++
		} else {
			b.scratch.RemoveIf(func(plog.LogRecord) bool { return true })
			obs.LogRulesDropped.Inc()
		}
	}
}

// proposeCandidates folds one entry's end position into the per-file,
// per-segment commit candidates. Segments the entry TRAVERSED on the way
// (start.seg up to but excluding end.seg) are fully covered through their
// recorded end, so their completion is proposed too. A dead segment id
// resolves to nothing at commit.
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
			if sg.id >= e.start.seg && sg.id < e.end.seg && sg.to > c[sg.id] {
				c[sg.id] = sg.to
			}
		}
	}
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
	kept := b.kept

	// Clamp the candidates to the watermark (the lowest position still
	// buffered in the pipeline stages): a candidate in a segment NEWER than
	// the watermark's commits nothing yet, one in the SAME segment clamps to
	// the watermark offset, and OLDER segments are unconstrained — their
	// bytes precede everything still buffered.
	highs := make(map[*file]pos, len(cands))
	for f, c := range cands {
		// Re-offer an earlier batch's exported-but-withheld high position:
		// its bytes are already delivered, only the commit was clamped.
		if hp := f.exportedHigh; hp.off > 0 && hp.off > c[hp.seg] {
			c[hp.seg] = hp.off
		}
		var high pos
		for seg, off := range c {
			if p := (pos{seg, off}); high.less(p) {
				high = p
			}
		}
		highs[f] = high
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
		err = t.exportWithRetry(ctx, ld)
	}
	if err != nil {
		t.failBatch(inf, err)
	} else {
		t.commitBatch(inf)
	}
}

func (t *Tailer) exportWithRetry(ctx context.Context, ld plog.Logs) error {
	var err error
	backoff := t.retryBackoff
	for attempt := 0; attempt < 3; attempt++ {
		if err = t.cfg.Exporter.ExportLogs(ctx, ld); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return err
}

// lowerSeverity lowercases a severity string without allocating for the
// values enrichment actually produces (ToLower allocates whenever any byte
// is uppercase — i.e. for nearly every record on the rules path).
func lowerSeverity(s string) string {
	switch s {
	case "":
		return ""
	case "TRACE", "trace":
		return "trace"
	case "DEBUG", "debug":
		return "debug"
	case "INFO", "info":
		return "info"
	case "WARN", "warn":
		return "warn"
	case "WARNING", "warning":
		return "warning"
	case "ERROR", "error":
		return "error"
	case "FATAL", "fatal":
		return "fatal"
	}
	return strings.ToLower(s)
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
	// highs is the per-file UNCLAMPED max end position: what could commit
	// once nothing is buffered. Recorded as file.exportedHigh on successful
	// commit where the watermark clamp withheld it.
	highs map[*file]pos
}

// commitBatch advances the committed offsets of a successfully exported
// batch: the tail candidate advances the file checkpoint, older segments'
// candidates advance their own records, and a segment whose whole range is
// now committed retires (fd closed, checkpoint entry gone).
func (t *Tailer) commitBatch(inf *batchInfo) {
	obs.LogEntries.Add(float64(inf.kept))
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
				if s.committed >= s.to {
					f.retire(s)
				}
			}
		}
		// Entries past the committed positions were DELIVERED but their
		// commit was withheld by the build-time watermark clamp; remember the
		// high so a later flush can re-offer it once nothing is buffered.
		if hi := inf.highs[f]; f.committedPos().less(hi) {
			f.exportedHigh = hi
		} else if !f.committedPos().less(f.exportedHigh) {
			// The commit frontier reached the remembered high: the re-offer
			// is spent; clear it so later flushes stop proposing it.
			f.exportedHigh = pos{}
		}
	}
}

// committedPos is the file's overall commit frontier: the oldest incomplete
// segment's progress, or the tail's committed offset when none remain.
func (f *file) committedPos() pos {
	if len(f.segments) > 0 {
		s := f.segments[0]
		return pos{s.id, s.committed}
	}
	return pos{f.tail, f.committed}
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
