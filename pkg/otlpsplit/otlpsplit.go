// Package otlpsplit splits OTLP payloads (logs, metrics, traces) into parts
// whose encoded protobuf size stays within a byte cap, preserving
// resource/scope grouping. A collector's default gRPC receive limit applies
// to the DECOMPRESSED message, so producers that batch by record count need
// exactly this guarantee against wholesale rejection of oversized payloads.
//
// Invariant: a non-empty input never yields zero parts — an over-cap
// record-less resource is sent whole (rejected and counted at the collector,
// never silently reported delivered).
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
	if maxBytes <= 0 || logMarshaler.LogsSize(ld) <= maxBytes {
		return []plog.Logs{ld}
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
			continue
		}
		splitBigResourceLogs(rl, maxBytes, &out)
	}
	flush()
	// Backstop for the never-zero-parts invariant (a zero-part return would
	// report the export "delivered" while sending nothing). Logically dead since
	// the per-resource zero-scope emit above — kept as a cheap final guard.
	if len(out) == 0 && ld.ResourceLogs().Len() > 0 {
		return []plog.Logs{ld}
	}
	return out
}

// splitBigResourceLogs packs one over-large ResourceLogs' records into whole-
// Logs chunks, each carrying a copy of the resource and scope.
func splitBigResourceLogs(rl plog.ResourceLogs, maxBytes int, out *[]plog.Logs) {
	base := logMarshaler.ResourceLogsSize(emptyRecordsRL(rl)) + elemOverhead
	newChunk := func() (plog.Logs, plog.LogRecordSlice, plog.ScopeLogs) {
		ld := plog.NewLogs()
		nrl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().CopyTo(nrl.Resource())
		nrl.SetSchemaUrl(rl.SchemaUrl())
		nsl := nrl.ScopeLogs().AppendEmpty()
		return ld, nsl.LogRecords(), nsl
	}
	sls := rl.ScopeLogs()
	for i := 0; i < sls.Len(); i++ {
		sl := sls.At(i)
		emptyScope := sl.LogRecords().Len() == 0
		ld, recs, nsl := newChunk()
		sl.Scope().CopyTo(nsl.Scope())
		nsl.SetSchemaUrl(sl.SchemaUrl())
		curBytes := base
		lrs := sl.LogRecords()
		for j := 0; j < lrs.Len(); j++ {
			lr := lrs.At(j)
			recBytes := logMarshaler.LogRecordSize(lr) + elemOverhead
			if recs.Len() > 0 && curBytes+recBytes > maxBytes {
				*out = append(*out, ld)
				ld, recs, nsl = newChunk()
				sl.Scope().CopyTo(nsl.Scope())
				nsl.SetSchemaUrl(sl.SchemaUrl())
				curBytes = base
			}
			lr.CopyTo(recs.AppendEmpty())
			curBytes += recBytes
		}
		// Emit the last chunk if it holds records, or the scope was empty — an
		// empty scope carries identity (attributes, schema URL) that the
		// under-cap path preserves, so the split must too.
		if recs.Len() > 0 || emptyScope {
			*out = append(*out, ld)
		}
	}
}

// emptyRecordsRL returns a copy of rl carrying its resource and scope framing
// but no records, to measure the fixed per-chunk base cost.
func emptyRecordsRL(rl plog.ResourceLogs) plog.ResourceLogs {
	tmp := plog.NewLogs()
	nrl := tmp.ResourceLogs().AppendEmpty()
	rl.Resource().CopyTo(nrl.Resource())
	nrl.SetSchemaUrl(rl.SchemaUrl())
	for i := 0; i < rl.ScopeLogs().Len(); i++ {
		nsl := nrl.ScopeLogs().AppendEmpty()
		rl.ScopeLogs().At(i).Scope().CopyTo(nsl.Scope())
		nsl.SetSchemaUrl(rl.ScopeLogs().At(i).SchemaUrl())
	}
	return nrl
}

// Metrics partitions md so each part's encoded size is <= maxBytes,
// splitting an over-large resource by metric (a single metric over the cap goes
// alone). Producers that pre-chunk never reach the metric split.
func Metrics(md pmetric.Metrics, maxBytes int) []pmetric.Metrics {
	if maxBytes <= 0 || metricMarshaler.MetricsSize(md) <= maxBytes {
		return []pmetric.Metrics{md}
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
			// See splitLogs: a scope-less over-cap resource must still ship.
			part := pmetric.NewMetrics()
			rm.CopyTo(part.ResourceMetrics().AppendEmpty())
			out = append(out, part)
			continue
		}
		splitBigResourceMetrics(rm, maxBytes, &out)
	}
	flush()
	// A non-empty input must never yield zero parts (see splitLogs).
	if len(out) == 0 && md.ResourceMetrics().Len() > 0 {
		return []pmetric.Metrics{md}
	}
	return out
}

func splitBigResourceMetrics(rm pmetric.ResourceMetrics, maxBytes int, out *[]pmetric.Metrics) {
	base := metricMarshaler.ResourceMetricsSize(emptyMetricsRM(rm)) + elemOverhead
	newChunk := func() (pmetric.Metrics, pmetric.MetricSlice, pmetric.ScopeMetrics) {
		md := pmetric.NewMetrics()
		nrm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().CopyTo(nrm.Resource())
		nrm.SetSchemaUrl(rm.SchemaUrl())
		nsm := nrm.ScopeMetrics().AppendEmpty()
		return md, nsm.Metrics(), nsm
	}
	sms := rm.ScopeMetrics()
	for i := 0; i < sms.Len(); i++ {
		sm := sms.At(i)
		emptyScope := sm.Metrics().Len() == 0
		md, ms, nsm := newChunk()
		sm.Scope().CopyTo(nsm.Scope())
		nsm.SetSchemaUrl(sm.SchemaUrl())
		curBytes := base
		metrics := sm.Metrics()
		for j := 0; j < metrics.Len(); j++ {
			m := metrics.At(j)
			mBytes := metricMarshaler.MetricSize(m) + elemOverhead
			if ms.Len() > 0 && curBytes+mBytes > maxBytes {
				*out = append(*out, md)
				md, ms, nsm = newChunk()
				sm.Scope().CopyTo(nsm.Scope())
				nsm.SetSchemaUrl(sm.SchemaUrl())
				curBytes = base
			}
			m.CopyTo(ms.AppendEmpty())
			curBytes += mBytes
		}
		if ms.Len() > 0 || emptyScope {
			*out = append(*out, md)
		}
	}
}

func emptyMetricsRM(rm pmetric.ResourceMetrics) pmetric.ResourceMetrics {
	tmp := pmetric.NewMetrics()
	nrm := tmp.ResourceMetrics().AppendEmpty()
	rm.Resource().CopyTo(nrm.Resource())
	nrm.SetSchemaUrl(rm.SchemaUrl())
	for i := 0; i < rm.ScopeMetrics().Len(); i++ {
		nsm := nrm.ScopeMetrics().AppendEmpty()
		rm.ScopeMetrics().At(i).Scope().CopyTo(nsm.Scope())
		nsm.SetSchemaUrl(rm.ScopeMetrics().At(i).SchemaUrl())
	}
	return nrm
}

// Traces partitions td so each part's encoded size is <= maxBytes,
// splitting an over-large resource by span (a single span over the cap goes
// alone).
func Traces(td ptrace.Traces, maxBytes int) []ptrace.Traces {
	if maxBytes <= 0 || traceMarshaler.TracesSize(td) <= maxBytes {
		return []ptrace.Traces{td}
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
			// See splitLogs: a scope-less over-cap resource must still ship.
			part := ptrace.NewTraces()
			rs.CopyTo(part.ResourceSpans().AppendEmpty())
			out = append(out, part)
			continue
		}
		splitBigResourceSpans(rs, maxBytes, &out)
	}
	flush()
	// A non-empty input must never yield zero parts (see splitLogs).
	if len(out) == 0 && td.ResourceSpans().Len() > 0 {
		return []ptrace.Traces{td}
	}
	return out
}

func splitBigResourceSpans(rs ptrace.ResourceSpans, maxBytes int, out *[]ptrace.Traces) {
	base := traceMarshaler.ResourceSpansSize(emptySpansRS(rs)) + elemOverhead
	newChunk := func() (ptrace.Traces, ptrace.SpanSlice, ptrace.ScopeSpans) {
		td := ptrace.NewTraces()
		nrs := td.ResourceSpans().AppendEmpty()
		rs.Resource().CopyTo(nrs.Resource())
		nrs.SetSchemaUrl(rs.SchemaUrl())
		nss := nrs.ScopeSpans().AppendEmpty()
		return td, nss.Spans(), nss
	}
	sss := rs.ScopeSpans()
	for i := 0; i < sss.Len(); i++ {
		ss := sss.At(i)
		emptyScope := ss.Spans().Len() == 0
		td, spans, nss := newChunk()
		ss.Scope().CopyTo(nss.Scope())
		nss.SetSchemaUrl(ss.SchemaUrl())
		curBytes := base
		src := ss.Spans()
		for j := 0; j < src.Len(); j++ {
			sp := src.At(j)
			spBytes := traceMarshaler.SpanSize(sp) + elemOverhead
			if spans.Len() > 0 && curBytes+spBytes > maxBytes {
				*out = append(*out, td)
				td, spans, nss = newChunk()
				ss.Scope().CopyTo(nss.Scope())
				nss.SetSchemaUrl(ss.SchemaUrl())
				curBytes = base
			}
			sp.CopyTo(spans.AppendEmpty())
			curBytes += spBytes
		}
		if spans.Len() > 0 || emptyScope {
			*out = append(*out, td)
		}
	}
}

func emptySpansRS(rs ptrace.ResourceSpans) ptrace.ResourceSpans {
	tmp := ptrace.NewTraces()
	nrs := tmp.ResourceSpans().AppendEmpty()
	rs.Resource().CopyTo(nrs.Resource())
	nrs.SetSchemaUrl(rs.SchemaUrl())
	for i := 0; i < rs.ScopeSpans().Len(); i++ {
		nss := nrs.ScopeSpans().AppendEmpty()
		rs.ScopeSpans().At(i).Scope().CopyTo(nss.Scope())
		nss.SetSchemaUrl(rs.ScopeSpans().At(i).SchemaUrl())
	}
	return nrs
}
