// Package otlpsplit splits OTLP payloads (logs, metrics, traces) into parts
// whose encoded protobuf size stays within a byte cap, preserving
// resource/scope grouping. A collector's default gRPC receive limit applies
// to the DECOMPRESSED message, so producers that batch by record count need
// exactly this guarantee against wholesale rejection of oversized payloads.
//
// Invariant: a non-empty input never yields zero parts — an over-cap
// record-less resource is sent whole (rejected and counted at the collector,
// never silently reported delivered).
//
// Second invariant, and the one an ATTACKER tests: the parts are never more
// than minChunkRoomDiv times the input. Every chunk RE-COPIES the framing it
// carries (the resource, the scope it is filling, the metric shell of a
// data-point split), so a payload whose framing sits just under the cap would
// otherwise split into one part per record, each carrying that framing again —
// measured at 100,000 parts and 366 GiB of marshal+gzip+send from one 4.5 MiB
// unauthenticated push. Past the threshold the split is ABANDONED and the
// remainder ships as a single over-cap part, which is the answer this package
// already gives a leaf it cannot shrink. See splitPaysOff.
package otlpsplit

import (
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// A collector's default gRPC receive limit is 4 MiB and applies to the
// DECOMPRESSED message. Producers that do not chunk (journald, the tailer)
// batch by record count, so a burst of large records can marshal past that
// limit and be rejected wholesale — every retry re-sends the same oversized
// payload and wedges the signal. Sizing here at the exporter, on the exact
// proto size (attributes and framing included, not just bodies), keeps every
// producer safe in one place; producers that already chunk under the cap
// (promscrape, the ingest batcher) never trip the split.

// DefaultMaxBytes is a safe per-payload cap: comfortably under the OTLP
// collector's 4 MiB default gRPC receive limit, with margin for framing.
const DefaultMaxBytes = 4<<20 - 256<<10 // 3.75 MiB

// elemOverhead absorbs the per-element proto framing (field tag + length
// prefix) that the fine-grained *Size helpers exclude, so the summed budget
// never undercounts the real message size — the split stays on the safe side.
const elemOverhead = 8

// minChunkRoomDiv is the floor on a chunk's usable room: a chunk whose
// re-copied framing leaves less than maxBytes/minChunkRoomDiv for CONTENT is
// not worth emitting. The arithmetic is the whole argument — a framing of B
// bytes against a cap of M carries at most M-B bytes of content per part while
// costing M on the wire, so the amplification is M/(M-B) and is UNBOUNDED as B
// approaches M. Holding M-B >= M/d bounds it at d: at most d bytes marshaled,
// gzipped and sent per byte of input, whatever the input's shape.
//
// d = 4 rather than 2 because the guard must not fire on honest producers: a
// resource built from Kubernetes metadata cannot approach 2.8 MiB of framing
// (the API server caps an object's annotations at 256 KiB, and scraped HELP
// text rides its own budget), while the attacker's shape — one sender-chosen
// attribute a few bytes under the cap — is orders of magnitude past it.
const minChunkRoomDiv = 4

// splitPaysOff reports whether a chunk whose fixed re-copied framing is base
// bytes leaves enough room to be worth filling. When it does not, the caller
// abandons the split and lets the remainder ship as one over-cap part: the
// collector rejects that part and the loss is counted, where splitting on
// would have delivered nothing either (every part carries the same over-cap
// framing) at thousands of times the CPU and wire cost.
func splitPaysOff(base, maxBytes int) bool {
	return maxBytes-base >= maxBytes/minChunkRoomDiv
}

// Report accounts for what a split could NOT do. Both counts are zero for the
// ordinary case — a payload that fits, or one that splits cleanly. Either one
// non-zero means parts went out that the collector is expected to reject
// wholesale, so the caller must surface them; this package cannot count them
// itself (pkg/ never imports internal/, obs included).
type Report struct {
	// Oversize is the number of parts emitted knowingly over maxBytes because
	// a single leaf — one log record, span or data point, or a resource
	// carrying no scopes at all — is itself over the cap and nothing here can
	// shrink it.
	Oversize int
	// Abandoned is the number of resources (or over-large metrics) whose split
	// was given up under splitPaysOff. Their remainder ships as ONE over-cap
	// part; it is not also counted in Oversize, because the remedy differs —
	// Oversize says one item is too big, Abandoned says the FRAMING is.
	Abandoned int
}

var (
	logMarshaler    plog.ProtoMarshaler
	metricMarshaler pmetric.ProtoMarshaler
	traceMarshaler  ptrace.ProtoMarshaler
)

// Logs partitions ld so each part's encoded size is <= maxBytes,
// preserving resource/scope grouping. A single record larger than maxBytes is
// emitted alone (nothing here can shrink it; it will be rejected and counted).
// maxBytes <= 0, or a payload already within the cap, returns ld unchanged.
func Logs(ld plog.Logs, maxBytes int) []plog.Logs {
	parts, _ := LogsWithReport(ld, maxBytes)
	return parts
}

// LogsWithReport is Logs plus the accounting of what the split could NOT do.
// A caller that can count and warn — the exporter — takes this one, so the
// parts the collector is expected to reject stop being invisible.
func LogsWithReport(ld plog.Logs, maxBytes int) ([]plog.Logs, Report) {
	var rep Report
	if maxBytes <= 0 || logMarshaler.LogsSize(ld) <= maxBytes {
		return []plog.Logs{ld}, rep
	}
	var out []plog.Logs
	cur := plog.NewLogs()
	curBytes := 0
	flush := func() {
		if cur.ResourceLogs().Len() > 0 {
			out = append(out, cur)
			cur = plog.NewLogs()
			curBytes = 0
		}
	}
	src := ld.ResourceLogs()
	for i := 0; i < src.Len(); i++ {
		rl := src.At(i)
		rlBytes := logMarshaler.ResourceLogsSize(rl) + elemOverhead
		if rlBytes <= maxBytes {
			if curBytes > 0 && curBytes+rlBytes > maxBytes {
				flush()
			}
			rl.CopyTo(cur.ResourceLogs().AppendEmpty())
			curBytes += rlBytes
			continue
		}
		// This resource alone exceeds the cap: split its records.
		flush()
		if rl.ScopeLogs().Len() == 0 {
			// No scopes to split by: send it whole as its own part — rejected
			// and counted at the collector, never silently dropped (the
			// len(out)==0 guard below only covers the single-resource case).
			part := plog.NewLogs()
			rl.CopyTo(part.ResourceLogs().AppendEmpty())
			out = append(out, part)
			rep.Oversize++
			continue
		}
		splitBigResourceLogs(rl, maxBytes, &out, &rep)
	}
	flush()
	// Backstop for the never-zero-parts invariant (a zero-part return would
	// report the export "delivered" while sending nothing). Logically dead since
	// the per-resource zero-scope emit above — kept as a cheap final guard.
	if len(out) == 0 && ld.ResourceLogs().Len() > 0 {
		rep.Oversize++
		return []plog.Logs{ld}, rep
	}
	return out, rep
}

// splitBigResourceLogs packs one over-large ResourceLogs' records into whole-
// Logs chunks, each carrying a copy of the resource and of the scopes it holds
// records for.
//
// A chunk CARRIES ACROSS the scope loop: a scope boundary is not a part
// boundary. Starting a fresh chunk per scope made the part count
// max(ceil(bytes/cap), scopes), so an OTel-SDK payload — one ScopeLogs per
// instrumentation library, which -ingest and the trace tier forward verbatim —
// split into one part per library however small they were (64 scopes over a
// 17 MB push: 64 parts against an ideal of 5). Each part is its own timeout,
// auth build, gzip pass and round trip, and on the trace tier the entry
// shard's forward is synchronous and holds an in-flight slot for the whole
// sequence.
func splitBigResourceLogs(rl plog.ResourceLogs, maxBytes int, out *[]plog.Logs, rep *Report) {
	// The per-chunk fixed cost is the resource plus the CURRENT scope only,
	// which is what a chunk actually holds. Measuring it from a copy carrying
	// EVERY scope's framing charged each chunk (S-1) scopes it does not
	// contain; the over-count is safe for the cap but real for the budget, and
	// once it exceeded maxBytes the always-take-one guard turned every single
	// record into its own part.
	resBase := logMarshaler.ResourceLogsSize(emptyScopesRL(rl)) + elemOverhead
	var (
		ld       plog.Logs
		nrl      plog.ResourceLogs
		recs     plog.LogRecordSlice
		open     bool // a chunk is being filled
		curBytes int
		held     int // records in the current chunk, across its scopes
		// giveUp latches when the framing every chunk re-copies leaves too
		// little room (splitPaysOff): from there on the cap checks are skipped
		// and everything remaining accumulates into ONE final over-cap part,
		// which the collector rejects. That is the same delivery outcome as
		// splitting on — every part would carry the same over-cap framing —
		// for one round trip instead of hundreds of thousands.
		giveUp bool
	)
	abandon := func() {
		if !giveUp {
			giveUp = true
			rep.Abandoned++
		}
	}
	emit := func() {
		if open {
			// A chunk over the cap that was NOT abandoned holds a single leaf
			// nothing here can shrink; the abandoned remainder is accounted
			// once, at the decision, so it is not counted twice.
			if curBytes > maxBytes && !giveUp {
				rep.Oversize++
			}
			*out = append(*out, ld)
			open = false
		}
	}
	if !splitPaysOff(resBase, maxBytes) {
		abandon()
	}
	openChunk := func() {
		ld = plog.NewLogs()
		nrl = ld.ResourceLogs().AppendEmpty()
		rl.Resource().CopyTo(nrl.Resource())
		nrl.SetSchemaUrl(rl.SchemaUrl())
		curBytes, held, open = resBase, 0, true
	}
	addScope := func(sl plog.ScopeLogs, scopeBytes int) {
		nsl := nrl.ScopeLogs().AppendEmpty()
		sl.Scope().CopyTo(nsl.Scope())
		nsl.SetSchemaUrl(sl.SchemaUrl())
		recs = nsl.LogRecords()
		curBytes += scopeBytes
	}
	sls := rl.ScopeLogs()
	for i := 0; i < sls.Len(); i++ {
		sl := sls.At(i)
		// An empty scope carries identity (name, attributes, schema URL) that
		// the under-cap path preserves, so it rides along in the current chunk
		// rather than costing a part of its own.
		scopeBytes := logMarshaler.ScopeLogsSize(emptyRecordsSL(sl)) + elemOverhead
		// The scope identity is re-copied per chunk too, so it belongs in the
		// framing the productivity check weighs: a resource with a modest
		// attribute set and one scope whose NAME sits just under the cap
		// degenerates exactly like an over-large resource does.
		if !splitPaysOff(resBase+scopeBytes, maxBytes) {
			abandon()
		}
		if open && !giveUp && curBytes+scopeBytes > maxBytes {
			emit()
		}
		if !open {
			openChunk()
		}
		addScope(sl, scopeBytes)
		lrs := sl.LogRecords()
		for j := 0; j < lrs.Len(); j++ {
			lr := lrs.At(j)
			recBytes := logMarshaler.LogRecordSize(lr) + elemOverhead
			// Emit-and-reopen when the record would overflow a chunk holding
			// anything beyond this scope's own identity: `held > 0` alone let
			// a chunk that had accumulated record-less scopes' identity bytes
			// (curBytes > resBase+scopeBytes with held == 0) take the record
			// unchecked and emit an over-cap part. Only a record over the cap
			// in a chunk of its own — after the reopen leaves exactly
			// resBase+scopeBytes — goes alone.
			if !giveUp && (held > 0 || curBytes > resBase+scopeBytes) && curBytes+recBytes > maxBytes {
				emit()
				openChunk()
				addScope(sl, scopeBytes)
			}
			lr.CopyTo(recs.AppendEmpty())
			curBytes += recBytes
			held++
		}
	}
	emit()
}

// emptyScopesRL returns a copy of rl carrying its resource and schema URL but
// no scopes, to measure the per-chunk cost the resource alone contributes.
func emptyScopesRL(rl plog.ResourceLogs) plog.ResourceLogs {
	tmp := plog.NewLogs()
	nrl := tmp.ResourceLogs().AppendEmpty()
	rl.Resource().CopyTo(nrl.Resource())
	nrl.SetSchemaUrl(rl.SchemaUrl())
	return nrl
}

// emptyRecordsSL returns a copy of sl carrying its scope and schema URL but no
// records, to measure what adding that scope to a chunk costs.
func emptyRecordsSL(sl plog.ScopeLogs) plog.ScopeLogs {
	nsl := plog.NewScopeLogs()
	sl.Scope().CopyTo(nsl.Scope())
	nsl.SetSchemaUrl(sl.SchemaUrl())
	return nsl
}

// Metrics partitions md so each part's encoded size is <= maxBytes, splitting
// an over-large resource by metric and an over-large metric by DATA POINT (a
// single data point over the cap goes alone). Producers that pre-chunk never
// reach the metric split.
func Metrics(md pmetric.Metrics, maxBytes int) []pmetric.Metrics {
	parts, _ := MetricsWithReport(md, maxBytes)
	return parts
}

// MetricsWithReport is Metrics plus the accounting of what the split could NOT
// do (see LogsWithReport).
func MetricsWithReport(md pmetric.Metrics, maxBytes int) ([]pmetric.Metrics, Report) {
	var rep Report
	if maxBytes <= 0 || metricMarshaler.MetricsSize(md) <= maxBytes {
		return []pmetric.Metrics{md}, rep
	}
	var out []pmetric.Metrics
	cur := pmetric.NewMetrics()
	curBytes := 0
	flush := func() {
		if cur.ResourceMetrics().Len() > 0 {
			out = append(out, cur)
			cur = pmetric.NewMetrics()
			curBytes = 0
		}
	}
	src := md.ResourceMetrics()
	for i := 0; i < src.Len(); i++ {
		rm := src.At(i)
		rmBytes := metricMarshaler.ResourceMetricsSize(rm) + elemOverhead
		if rmBytes <= maxBytes {
			if curBytes > 0 && curBytes+rmBytes > maxBytes {
				flush()
			}
			rm.CopyTo(cur.ResourceMetrics().AppendEmpty())
			curBytes += rmBytes
			continue
		}
		flush()
		if rm.ScopeMetrics().Len() == 0 {
			// See Logs: a scope-less over-cap resource must still ship.
			part := pmetric.NewMetrics()
			rm.CopyTo(part.ResourceMetrics().AppendEmpty())
			out = append(out, part)
			rep.Oversize++
			continue
		}
		splitBigResourceMetrics(rm, maxBytes, &out, &rep)
	}
	flush()
	// A non-empty input must never yield zero parts (see Logs).
	if len(out) == 0 && md.ResourceMetrics().Len() > 0 {
		rep.Oversize++
		return []pmetric.Metrics{md}, rep
	}
	return out, rep
}

// splitBigResourceMetrics packs one over-large ResourceMetrics' metrics into
// whole-Metrics chunks. Chunks carry across the scope loop and the per-chunk
// base counts the CURRENT scope only — see splitBigResourceLogs for both.
func splitBigResourceMetrics(rm pmetric.ResourceMetrics, maxBytes int, out *[]pmetric.Metrics, rep *Report) {
	resBase := metricMarshaler.ResourceMetricsSize(emptyScopesRM(rm)) + elemOverhead
	var (
		md       pmetric.Metrics
		nrm      pmetric.ResourceMetrics
		ms       pmetric.MetricSlice
		open     bool
		curBytes int
		held     int
		scopes   int
		giveUp   bool // see splitBigResourceLogs
	)
	abandon := func() {
		if !giveUp {
			giveUp = true
			rep.Abandoned++
		}
	}
	emit := func() {
		if open {
			if curBytes > maxBytes && !giveUp {
				rep.Oversize++
			}
			*out = append(*out, md)
			open = false
		}
	}
	if !splitPaysOff(resBase, maxBytes) {
		abandon()
	}
	openChunk := func() {
		md = pmetric.NewMetrics()
		nrm = md.ResourceMetrics().AppendEmpty()
		rm.Resource().CopyTo(nrm.Resource())
		nrm.SetSchemaUrl(rm.SchemaUrl())
		curBytes, held, scopes, open = resBase, 0, 0, true
	}
	addScope := func(sm pmetric.ScopeMetrics, scopeBytes int) {
		nsm := nrm.ScopeMetrics().AppendEmpty()
		sm.Scope().CopyTo(nsm.Scope())
		nsm.SetSchemaUrl(sm.SchemaUrl())
		ms = nsm.Metrics()
		curBytes += scopeBytes
		scopes++
	}
	sms := rm.ScopeMetrics()
	for i := 0; i < sms.Len(); i++ {
		sm := sms.At(i)
		scopeBytes := metricMarshaler.ScopeMetricsSize(emptyMetricsSM(sm)) + elemOverhead
		base := resBase + scopeBytes // a chunk holding this scope alone
		if !splitPaysOff(base, maxBytes) {
			abandon()
		}
		if open && !giveUp && curBytes+scopeBytes > maxBytes {
			emit()
		}
		if !open {
			openChunk()
		}
		addScope(sm, scopeBytes)
		metrics := sm.Metrics()
		for j := 0; j < metrics.Len(); j++ {
			if !open { // the data-point split below closed the chunk
				openChunk()
				addScope(sm, scopeBytes)
			}
			m := metrics.At(j)
			mBytes := metricMarshaler.MetricSize(m) + elemOverhead
			// A metric that cannot fit in a chunk of its own is split by DATA
			// POINT. Stopping at the family would emit a part the collector
			// rejects wholesale — the exact loss this package exists to
			// prevent — and a single family (a KSM-style split, a fat
			// histogram) can be the whole payload.
			if !giveUp && base+mBytes > maxBytes && dataPointCount(m) > 1 {
				// Emit what is carried, unless the chunk holds nothing but this
				// scope's own identity — the data-point chunks below carry that
				// same scope, so emitting it would be a part with no content.
				if held > 0 || scopes > 1 {
					emit()
				} else {
					open = false
				}
				splitBigMetric(m, base, maxBytes, rep, func() (pmetric.Metrics, pmetric.MetricSlice) {
					nmd := pmetric.NewMetrics()
					x := nmd.ResourceMetrics().AppendEmpty()
					rm.Resource().CopyTo(x.Resource())
					x.SetSchemaUrl(rm.SchemaUrl())
					nsm := x.ScopeMetrics().AppendEmpty()
					sm.Scope().CopyTo(nsm.Scope())
					nsm.SetSchemaUrl(sm.SchemaUrl())
					return nmd, nsm.Metrics()
				}, out)
				continue
			}
			// held > 0 alone missed a chunk carrying earlier record-less
			// scopes' identity bytes — see the Logs splitter.
			if !giveUp && (held > 0 || curBytes > base) && curBytes+mBytes > maxBytes {
				emit()
				openChunk()
				addScope(sm, scopeBytes)
			}
			m.CopyTo(ms.AppendEmpty())
			curBytes += mBytes
			held++
		}
	}
	emit()
}

// splitBigMetric packs one over-large metric's data points into whole-Metrics
// chunks, each carrying a copy of the resource, scope and the metric shell
// (name/description/unit/metadata plus the type-level temporality and
// monotonicity). Everything else — data-point attributes, timestamps,
// exemplars, exponential-histogram scale/zero-count/offsets, summary quantiles
// — rides on the data point and is preserved by its copy. A single data point
// over the cap is emitted alone (nothing here can shrink it).
//
// base is the fixed per-chunk cost of the resource/scope framing; newChunk
// yields an empty chunk carrying it.
func splitBigMetric(m pmetric.Metric, base, maxBytes int, rep *Report, newChunk func() (pmetric.Metrics, pmetric.MetricSlice), out *[]pmetric.Metrics) {
	shell := pmetric.NewMetric()
	copyMetricShell(m, shell)
	// Per-chunk fixed cost: resource + scope framing plus the point-less metric.
	metricBase := base + metricMarshaler.MetricSize(shell) + elemOverhead
	// The shell is re-copied per chunk like the resource and the scope, and its
	// description is the SENDER's string on a pushed payload — so the same
	// degeneration is reachable through a metric nobody's resource is large:
	// see splitBigResourceLogs.
	giveUp := !splitPaysOff(metricBase, maxBytes)
	if giveUp {
		rep.Abandoned++
	}
	newMetricChunk := func() (pmetric.Metrics, pmetric.Metric) {
		md, ms := newChunk()
		nm := ms.AppendEmpty()
		shell.CopyTo(nm)
		return md, nm
	}
	n, sizeOf, appendTo := pointAccessors(m)
	md, nm := newMetricChunk()
	curBytes, held := metricBase, 0
	for i := 0; i < n; i++ {
		dpBytes := sizeOf(i) + elemOverhead
		if !giveUp && held > 0 && curBytes+dpBytes > maxBytes {
			if curBytes > maxBytes {
				rep.Oversize++
			}
			*out = append(*out, md)
			md, nm = newMetricChunk()
			curBytes, held = metricBase, 0
		}
		appendTo(i, nm)
		curBytes += dpBytes
		held++
	}
	if held > 0 {
		if curBytes > maxBytes && !giveUp {
			rep.Oversize++
		}
		*out = append(*out, md)
	}
}

// copyMetricShell copies m's identity and type-level fields into dst, leaving
// dst without data points.
func copyMetricShell(m, dst pmetric.Metric) {
	dst.SetName(m.Name())
	dst.SetDescription(m.Description())
	dst.SetUnit(m.Unit())
	m.Metadata().CopyTo(dst.Metadata())
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dst.SetEmptyGauge()
	case pmetric.MetricTypeSum:
		s := dst.SetEmptySum()
		s.SetIsMonotonic(m.Sum().IsMonotonic())
		s.SetAggregationTemporality(m.Sum().AggregationTemporality())
	case pmetric.MetricTypeHistogram:
		dst.SetEmptyHistogram().SetAggregationTemporality(m.Histogram().AggregationTemporality())
	case pmetric.MetricTypeExponentialHistogram:
		dst.SetEmptyExponentialHistogram().SetAggregationTemporality(m.ExponentialHistogram().AggregationTemporality())
	case pmetric.MetricTypeSummary:
		dst.SetEmptySummary()
	}
}

// dataPointCount is m's data-point count across all five metric types (0 for a
// type-less metric, which is therefore never data-point split).
func dataPointCount(m pmetric.Metric) int {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		return m.Gauge().DataPoints().Len()
	case pmetric.MetricTypeSum:
		return m.Sum().DataPoints().Len()
	case pmetric.MetricTypeHistogram:
		return m.Histogram().DataPoints().Len()
	case pmetric.MetricTypeExponentialHistogram:
		return m.ExponentialHistogram().DataPoints().Len()
	case pmetric.MetricTypeSummary:
		return m.Summary().DataPoints().Len()
	}
	return 0
}

// pointAccessors returns m's data-point count plus per-index size and
// copy-into-destination helpers, uniform across the five metric types. Cold
// path (only a metric that alone exceeds the cap gets here), so the per-call
// closures cost nothing in the common case.
func pointAccessors(m pmetric.Metric) (n int, sizeOf func(int) int, appendTo func(int, pmetric.Metric)) {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps := m.Gauge().DataPoints()
		return dps.Len(),
			func(i int) int { return metricMarshaler.NumberDataPointSize(dps.At(i)) },
			func(i int, dst pmetric.Metric) { dps.At(i).CopyTo(dst.Gauge().DataPoints().AppendEmpty()) }
	case pmetric.MetricTypeSum:
		dps := m.Sum().DataPoints()
		return dps.Len(),
			func(i int) int { return metricMarshaler.NumberDataPointSize(dps.At(i)) },
			func(i int, dst pmetric.Metric) { dps.At(i).CopyTo(dst.Sum().DataPoints().AppendEmpty()) }
	case pmetric.MetricTypeHistogram:
		dps := m.Histogram().DataPoints()
		return dps.Len(),
			func(i int) int { return metricMarshaler.HistogramDataPointSize(dps.At(i)) },
			func(i int, dst pmetric.Metric) { dps.At(i).CopyTo(dst.Histogram().DataPoints().AppendEmpty()) }
	case pmetric.MetricTypeExponentialHistogram:
		dps := m.ExponentialHistogram().DataPoints()
		return dps.Len(),
			func(i int) int { return metricMarshaler.ExponentialHistogramDataPointSize(dps.At(i)) },
			func(i int, dst pmetric.Metric) {
				dps.At(i).CopyTo(dst.ExponentialHistogram().DataPoints().AppendEmpty())
			}
	case pmetric.MetricTypeSummary:
		dps := m.Summary().DataPoints()
		return dps.Len(),
			func(i int) int { return metricMarshaler.SummaryDataPointSize(dps.At(i)) },
			func(i int, dst pmetric.Metric) { dps.At(i).CopyTo(dst.Summary().DataPoints().AppendEmpty()) }
	}
	return 0, func(int) int { return 0 }, func(int, pmetric.Metric) {}
}

func emptyScopesRM(rm pmetric.ResourceMetrics) pmetric.ResourceMetrics {
	tmp := pmetric.NewMetrics()
	nrm := tmp.ResourceMetrics().AppendEmpty()
	rm.Resource().CopyTo(nrm.Resource())
	nrm.SetSchemaUrl(rm.SchemaUrl())
	return nrm
}

func emptyMetricsSM(sm pmetric.ScopeMetrics) pmetric.ScopeMetrics {
	nsm := pmetric.NewScopeMetrics()
	sm.Scope().CopyTo(nsm.Scope())
	nsm.SetSchemaUrl(sm.SchemaUrl())
	return nsm
}

// Traces partitions td so each part's encoded size is <= maxBytes,
// splitting an over-large resource by span (a single span over the cap goes
// alone).
func Traces(td ptrace.Traces, maxBytes int) []ptrace.Traces {
	parts, _ := TracesWithReport(td, maxBytes)
	return parts
}

// TracesWithReport is Traces plus the accounting of what the split could NOT
// do (see LogsWithReport).
func TracesWithReport(td ptrace.Traces, maxBytes int) ([]ptrace.Traces, Report) {
	var rep Report
	if maxBytes <= 0 || traceMarshaler.TracesSize(td) <= maxBytes {
		return []ptrace.Traces{td}, rep
	}
	var out []ptrace.Traces
	cur := ptrace.NewTraces()
	curBytes := 0
	flush := func() {
		if cur.ResourceSpans().Len() > 0 {
			out = append(out, cur)
			cur = ptrace.NewTraces()
			curBytes = 0
		}
	}
	src := td.ResourceSpans()
	for i := 0; i < src.Len(); i++ {
		rs := src.At(i)
		rsBytes := traceMarshaler.ResourceSpansSize(rs) + elemOverhead
		if rsBytes <= maxBytes {
			if curBytes > 0 && curBytes+rsBytes > maxBytes {
				flush()
			}
			rs.CopyTo(cur.ResourceSpans().AppendEmpty())
			curBytes += rsBytes
			continue
		}
		flush()
		if rs.ScopeSpans().Len() == 0 {
			// See Logs: a scope-less over-cap resource must still ship.
			part := ptrace.NewTraces()
			rs.CopyTo(part.ResourceSpans().AppendEmpty())
			out = append(out, part)
			rep.Oversize++
			continue
		}
		splitBigResourceSpans(rs, maxBytes, &out, &rep)
	}
	flush()
	// A non-empty input must never yield zero parts (see Logs).
	if len(out) == 0 && td.ResourceSpans().Len() > 0 {
		rep.Oversize++
		return []ptrace.Traces{td}, rep
	}
	return out, rep
}

// splitBigResourceSpans packs one over-large ResourceSpans' spans into
// whole-Traces chunks. Chunks carry across the scope loop and the per-chunk
// base counts the CURRENT scope only — see splitBigResourceLogs for both.
func splitBigResourceSpans(rs ptrace.ResourceSpans, maxBytes int, out *[]ptrace.Traces, rep *Report) {
	resBase := traceMarshaler.ResourceSpansSize(emptyScopesRS(rs)) + elemOverhead
	var (
		td       ptrace.Traces
		nrs      ptrace.ResourceSpans
		spans    ptrace.SpanSlice
		open     bool
		curBytes int
		held     int
		giveUp   bool // see splitBigResourceLogs
	)
	abandon := func() {
		if !giveUp {
			giveUp = true
			rep.Abandoned++
		}
	}
	emit := func() {
		if open {
			if curBytes > maxBytes && !giveUp {
				rep.Oversize++
			}
			*out = append(*out, td)
			open = false
		}
	}
	if !splitPaysOff(resBase, maxBytes) {
		abandon()
	}
	openChunk := func() {
		td = ptrace.NewTraces()
		nrs = td.ResourceSpans().AppendEmpty()
		rs.Resource().CopyTo(nrs.Resource())
		nrs.SetSchemaUrl(rs.SchemaUrl())
		curBytes, held, open = resBase, 0, true
	}
	addScope := func(ss ptrace.ScopeSpans, scopeBytes int) {
		nss := nrs.ScopeSpans().AppendEmpty()
		ss.Scope().CopyTo(nss.Scope())
		nss.SetSchemaUrl(ss.SchemaUrl())
		spans = nss.Spans()
		curBytes += scopeBytes
	}
	sss := rs.ScopeSpans()
	for i := 0; i < sss.Len(); i++ {
		ss := sss.At(i)
		scopeBytes := traceMarshaler.ScopeSpansSize(emptySpansSS(ss)) + elemOverhead
		if !splitPaysOff(resBase+scopeBytes, maxBytes) {
			abandon()
		}
		if open && !giveUp && curBytes+scopeBytes > maxBytes {
			emit()
		}
		if !open {
			openChunk()
		}
		addScope(ss, scopeBytes)
		src := ss.Spans()
		for j := 0; j < src.Len(); j++ {
			sp := src.At(j)
			spBytes := traceMarshaler.SpanSize(sp) + elemOverhead
			// held > 0 alone missed a chunk carrying earlier span-less
			// scopes' identity bytes — see the Logs splitter.
			if !giveUp && (held > 0 || curBytes > resBase+scopeBytes) && curBytes+spBytes > maxBytes {
				emit()
				openChunk()
				addScope(ss, scopeBytes)
			}
			sp.CopyTo(spans.AppendEmpty())
			curBytes += spBytes
			held++
		}
	}
	emit()
}

func emptyScopesRS(rs ptrace.ResourceSpans) ptrace.ResourceSpans {
	tmp := ptrace.NewTraces()
	nrs := tmp.ResourceSpans().AppendEmpty()
	rs.Resource().CopyTo(nrs.Resource())
	nrs.SetSchemaUrl(rs.SchemaUrl())
	return nrs
}

func emptySpansSS(ss ptrace.ScopeSpans) ptrace.ScopeSpans {
	nss := ptrace.NewScopeSpans()
	ss.Scope().CopyTo(nss.Scope())
	nss.SetSchemaUrl(ss.SchemaUrl())
	return nss
}
