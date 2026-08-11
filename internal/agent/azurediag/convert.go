package azurediag

// Record → OTLP conversion. The ARM resource the diagnostics are ABOUT
// becomes the OTLP resource (cloud.resource_id, cloud.account.id, azure.*,
// with service.name/namespace/instance.id derived so Mimir job/instance
// identity works out of the box) — one ResourceLogs/ResourceMetrics per
// distinct resource, exactly as the events pipeline groups by involved
// object.
//
// Logs keep the record's verbatim JSON as the body and run the same chain as
// every other log pipeline, in the same order: scrub → logAttributes →
// enrich → logMetrics (which see every record) → rules. Metrics are the
// pre-aggregated window statistics Azure emits — count/total/min/max/average
// — exported as five gauges per metric ("real" OTLP data points, not logs):
// gauges because each value describes one closed timeGrain window, which is
// also how the widely-deployed Prometheus Azure exporters shape them;
// sum_over_time/avg_over_time recover longer windows downstream.

import (
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/logchain"
	"github.com/JohanLindvall/kubescrape/internal/agent/logscrub"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// ScopeName is the OTLP instrumentation-scope name on every record and
// metric this package converts (wire-visible: changing it splits every
// stream at the upgrade boundary).
const ScopeName = "github.com/JohanLindvall/kubescrape/agent/azurediag"

// severityOf maps Azure's level strings onto OTLP severities. Numeric levels
// are NOT interpreted — Azure's own conventions disagree on their direction
// (Application Insights counts up with severity, others down), so a number
// falls through to the default and -enrich may still find an explicit level
// in the body.
//
// Lowercase, like every other producer in this repo: it is what the enrich
// package writes, and logenrich.Apply OVERWRITES the severity here whenever it
// parses a level out of the record — so any other casing was contradicted one
// line later, on whichever subset of records happened to parse.
func severityOf(level string) (plog.SeverityNumber, string) {
	switch {
	case strings.EqualFold(level, "Verbose") || strings.EqualFold(level, "Debug"):
		return plog.SeverityNumberDebug, "debug"
	case strings.EqualFold(level, "Warning") || strings.EqualFold(level, "Warn"):
		return plog.SeverityNumberWarn, "warn"
	case strings.EqualFold(level, "Error"):
		return plog.SeverityNumberError, "error"
	case strings.EqualFold(level, "Critical") || strings.EqualFold(level, "Fatal"):
		return plog.SeverityNumberFatal, "fatal"
	}
	return plog.SeverityNumberInfo, "info"
}

// resKey groups records by the ARM resource they describe.
func resKey(rec *record) string { return strings.ToLower(rec.resourceID) }

// resource builds the ARM resource's OTLP resource.
func (r *Reader) resource(rec *record) pcommon.Resource {
	res := pcommon.NewResource()
	a := res.Attributes()
	a.PutStr("cloud.provider", "azure")
	if rec.resourceID != "" {
		arm := parseResourceID(rec.resourceID)
		a.PutStr("cloud.resource_id", rec.resourceID)
		if arm.Subscription != "" {
			a.PutStr("cloud.account.id", arm.Subscription)
		}
		if arm.ResourceGroup != "" {
			a.PutStr("azure.resource_group.name", arm.ResourceGroup)
			// Mimir job = service.namespace/service.name: the resource group
			// is the natural namespace for an Azure resource.
			a.PutStr("service.namespace", arm.ResourceGroup)
		}
		if arm.Type != "" {
			a.PutStr("azure.resource.type", arm.Type)
		}
		if arm.Name != "" {
			a.PutStr("azure.resource.name", arm.Name)
			a.PutStr("service.name", arm.Name)
		}
		// Two same-named resources in different groups/subscriptions must
		// not merge into one series identity.
		a.PutStr("service.instance.id", resKey(rec))
	}
	if _, ok := a.Get("service.name"); !ok {
		a.PutStr("service.name", "azure-diagnostics")
	}
	if rec.location != "" {
		a.PutStr("cloud.region", strings.ToLower(rec.location))
	}
	// The reader's own node deliberately never appears (attrs.Context{}):
	// the resource an Azure record describes has no relation to wherever
	// this singleton happens to be scheduled.
	r.cfg.Attrs.Build(res, attrs.Context{})
	return res
}

// convertLogs turns the batch's log records into one plog.Logs, grouped per
// ARM resource. The per-record half — scrubbing, line attributes, enrichment,
// log-metrics (which see EVERY record) and the keep/drop rules — is the shared
// chain every log producer in this repo runs, in the same order
// (internal/agent/logchain); what stays here is the ARM RESOURCE and the
// grouping.
func (r *Reader) convertLogs(recs []record) plog.Logs {
	ld := plog.NewLogs()
	groups := logchain.NewGroups(ld, ScopeName, 8)
	observed := pcommon.NewTimestampFromTime(time.Now())
	sink := &recordSink{r: r, observed: observed, scrub: r.cfg.Scrub}
	chain := logchain.NewChain[string](logchain.Config{
		Scrub:      r.cfg.Scrub,
		LogAttrs:   r.cfg.LogAttrs,
		Enrich:     r.cfg.Enrich,
		LogMetrics: r.cfg.LogMetrics,
		Rules:      r.cfg.Rules,
	}, false)

	for i := range recs {
		rec := &recs[i]
		if rec.metric {
			continue
		}
		// Scrub runs first, before anything copies from the body.
		body, extracted := chain.Line(string(rec.raw))

		key := chain.GroupKey(resKey(rec), extracted)
		// The group is built BEFORE the record because metric and rule
		// resolution reads the group's own resource; a group the rules empty is
		// pruned below.
		sink.rec, sink.body = rec, body
		ent := groups.Get(key, extracted, sink)
		sink.sl = ent.SL
		chain.Emit(sink, logchain.Input[string]{
			Body: body, Lifted: extracted, Resource: ent.Res, BoundKey: key,
		})
	}
	// AFTER the chain, deliberately: log-metrics observe every record and the
	// keep/drop rules run before this, so a rule or a metric label keyed on
	// cloud.resource_id still sees it. Only the EXPORTED record loses the copy.
	elideRedundantIdentity(ld)
	// An all-dropped group leaves an empty ResourceLogs behind.
	logchain.Prune(ld)
	return ld
}

// redundantOnAzureResource reports whether an enrich-derived record attribute
// merely repeats what r.resource() already put on the OTLP resource.
//
// The converter is authoritative for identity on this path: it parses the ARM
// id out of the envelope's own resourceId field, while enrich re-derives one
// from the body TEXT. Keeping both is not just redundant, it is contradictory
// — the resource carries the id verbatim and enrich's copy is lowercased, so
// one payload ships two values under `cloud.resource_id` and a backend that
// flattens resource and record attributes picks a winner this repo does not
// control. It is also pure egress: measured against a live Event Hub, 286
// bytes on every record, 1 MB per 3,500-record payload.
//
// `azure.resource_group.id` goes for a second reason: enrich's ResourceGroupID
// is the group's full ARM ID (`/subscriptions/<sub>/resourcegroups/<name>`),
// while the resource carries the bare name as `azure.resource_group.name` —
// two shapes of the same fact, one per key.
//
// The `.id` suffix is load-bearing, not cosmetic. This attribute was spelled
// `azure.resource_group` until semconv v1.44.0 was checked against it: the
// registry defines `azure.resource_group.name` and nothing else in that
// namespace, so the bare key sat one dot from a real attribute while holding a
// different shape of its value — the collision semconv's naming guidance warns
// about, where a future OTel definition of that exact key would conflict
// silently rather than merely coexist. `.id` cannot collide that way: if OTel
// ever mints it, it means what we mean.
//
// Scoped to THIS path on purpose. The tailer, journald and ingest have no ARM
// resource, so for them the record attribute is the only carrier and
// logenrich must keep emitting it.
func redundantOnAzureResource(key string) bool {
	return key == "cloud.resource_id" || key == "azure.resource_group.id"
}

// elideRedundantIdentity drops the duplicated identity attributes from every
// record whose RESOURCE actually carries the ARM identity.
//
// The gate is what makes this safe: a record whose envelope had no resourceId
// (or one parseResourceID could not read) leaves the resource without
// cloud.resource_id, and there enrich's body-derived attributes are the ONLY
// carrier — dropping them would lose the identity rather than de-duplicate it.
func elideRedundantIdentity(ld plog.Logs) {
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		if _, ok := rl.Resource().Attributes().Get("cloud.resource_id"); !ok {
			continue
		}
		sls := rl.ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			recs := sls.At(j).LogRecords()
			for k := 0; k < recs.Len(); k++ {
				recs.At(k).Attributes().RemoveIf(func(key string, _ pcommon.Value) bool {
					return redundantOnAzureResource(key)
				})
			}
		}
	}
}

// recordSink is the chain's Producer for diagnostic records: the group a kept
// record lands in, and what the Azure reader knows about the record.
type recordSink struct {
	r        *Reader
	sl       plog.ScopeLogs
	rec      *record
	body     string
	observed pcommon.Timestamp
	scrub    *logscrub.Scrubber
}

func (s *recordSink) Dest() plog.LogRecordSlice { return s.sl.LogRecords() }

// FillResource builds a fresh group's resource: the ARM resource the record
// describes.
func (s *recordSink) FillResource(res pcommon.Resource) { s.r.resource(s.rec).CopyTo(res) }

func (s *recordSink) Stamp(lr plog.LogRecord) {
	ts := s.rec.ts
	if ts.IsZero() {
		ts = s.observed.AsTime()
	}
	lr.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	lr.SetObservedTimestamp(s.observed)
	sev, sevText := severityOf(s.rec.level)
	lr.SetSeverityNumber(sev)
	lr.SetSeverityText(sevText)
	lr.Body().SetStr(s.body)
	putLogAttrs(lr.Attributes(), s.rec, s.scrub)
}

// putLogAttrs stamps the record-level attributes describing the diagnostic
// itself; the body keeps the full record.
func putLogAttrs(dst pcommon.Map, rec *record, scrub *logscrub.Scrubber) {
	if rec.category != "" {
		dst.PutStr("azure.category", rec.category)
	}
	if rec.opName != "" {
		dst.PutStr("azure.operation.name", rec.opName)
	}
	if rec.resultType != "" {
		dst.PutStr("azure.result.type", rec.resultType)
	}
	if rec.resultDesc != "" {
		desc := rec.resultDesc
		if scrub != nil {
			// The description is free text copied out of the record — the
			// one log attribute that can carry what the body scrub caught.
			desc = scrub.Scrub(desc)
		}
		dst.PutStr("azure.result.description", desc)
	}
	if rec.correlationID != "" {
		dst.PutStr("azure.correlation.id", rec.correlationID)
	}
	if rec.tenantID != "" {
		dst.PutStr("azure.tenant.id", rec.tenantID)
	}
}

// convertMetrics turns the batch's metric records into one pmetric.Metrics:
// per ARM resource, per Azure metric, one gauge per present aggregation,
// named <prefix><metricname>.<aggregation>.
func (r *Reader) convertMetrics(recs []record) pmetric.Metrics {
	md := pmetric.NewMetrics()
	type group struct {
		sm     pmetric.ScopeMetrics
		byName map[string]pmetric.NumberDataPointSlice
	}
	groups := make(map[string]*group, 8)
	observed := pcommon.NewTimestampFromTime(time.Now())

	for i := range recs {
		rec := &recs[i]
		if !rec.metric {
			continue
		}
		key := resKey(rec)
		g, ok := groups[key]
		if !ok {
			rm := md.ResourceMetrics().AppendEmpty()
			r.resource(rec).CopyTo(rm.Resource())
			sm := rm.ScopeMetrics().AppendEmpty()
			sm.Scope().SetName(ScopeName)
			sm.Scope().SetVersion(obs.ScopeVersion)
			g = &group{sm: sm, byName: make(map[string]pmetric.NumberDataPointSlice, 8)}
			groups[key] = g
		}
		ts := pcommon.NewTimestampFromTime(rec.ts)
		if rec.ts.IsZero() {
			ts = observed
		}
		base := r.cfg.MetricPrefix + strings.ToLower(rec.metricName) + "."
		for agg := 0; agg < nAggs; agg++ {
			if !rec.has[agg] {
				continue
			}
			name := base + aggNames[agg]
			dps, ok := g.byName[name]
			if !ok {
				m := g.sm.Metrics().AppendEmpty()
				m.SetName(name)
				dps = m.SetEmptyGauge().DataPoints()
				g.byName[name] = dps
			}
			dp := dps.AppendEmpty()
			dp.SetTimestamp(ts)
			dp.SetDoubleValue(rec.aggs[agg])
			if rec.timeGrain != "" {
				dp.Attributes().PutStr("azure.metric.timegrain", rec.timeGrain)
			}
		}
	}
	return md
}
