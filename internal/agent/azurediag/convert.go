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
	"github.com/JohanLindvall/kubescrape/internal/agent/logenrich"
	"github.com/JohanLindvall/kubescrape/internal/agent/logscrub"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
)

const scopeName = "github.com/JohanLindvall/kubescrape/agent/azurediag"

// severityOf maps Azure's level strings onto OTLP severities. Numeric levels
// are NOT interpreted — Azure's own conventions disagree on their direction
// (Application Insights counts up with severity, others down), so a number
// falls through to the default and -enrich may still find an explicit level
// in the body.
func severityOf(level string) (plog.SeverityNumber, string) {
	switch {
	case strings.EqualFold(level, "Verbose") || strings.EqualFold(level, "Debug"):
		return plog.SeverityNumberDebug, "DEBUG"
	case strings.EqualFold(level, "Warning") || strings.EqualFold(level, "Warn"):
		return plog.SeverityNumberWarn, "WARN"
	case strings.EqualFold(level, "Error"):
		return plog.SeverityNumberError, "ERROR"
	case strings.EqualFold(level, "Critical") || strings.EqualFold(level, "Fatal"):
		return plog.SeverityNumberFatal, "FATAL"
	}
	return plog.SeverityNumberInfo, "INFO"
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
// ARM resource, applying the shared chain in the tailer's order.
func (r *Reader) convertLogs(recs []record) plog.Logs {
	ld := plog.NewLogs()
	scopes := make(map[string]plog.ScopeLogs, 8)
	resAttrs := make(map[string]pcommon.Map, 8)
	observed := pcommon.NewTimestampFromTime(time.Now())

	var (
		scratch  plog.LogRecordSlice
		resolver *entryResolver
		bound    map[string]metrics.BoundResource
	)
	if r.cfg.Rules != nil || r.cfg.LogMetrics != nil {
		resolver = newEntryResolver()
	}
	if r.cfg.Rules != nil {
		scratch = plog.NewLogRecordSlice()
	}
	if r.cfg.LogMetrics != nil {
		bound = make(map[string]metrics.BoundResource, 8)
	}

	for i := range recs {
		rec := &recs[i]
		if rec.metric {
			continue
		}
		body := string(rec.raw)
		if r.cfg.Scrub != nil {
			// Scrub before anything copies from the body, as everywhere else.
			body = r.cfg.Scrub.Scrub(body)
		}

		var extracted logattrs.Result
		key := resKey(rec)
		if r.cfg.LogAttrs != nil {
			extracted = r.cfg.LogAttrs.Extract(body)
			key = key + "\x01" + logattrs.Key(extracted.Resource) + "\x01" + logattrs.Key(extracted.Scope)
		}
		sl, ok := scopes[key]
		if !ok {
			rl := ld.ResourceLogs().AppendEmpty()
			r.resource(rec).CopyTo(rl.Resource())
			logattrs.Put(rl.Resource().Attributes(), extracted.Resource)
			sl = rl.ScopeLogs().AppendEmpty()
			sl.Scope().SetName(scopeName)
			logattrs.Put(sl.Scope().Attributes(), extracted.Scope)
			scopes[key] = sl
			resAttrs[key] = rl.Resource().Attributes()
		}

		var lr plog.LogRecord
		scratched := r.cfg.Rules != nil
		if scratched {
			scratch.RemoveIf(func(plog.LogRecord) bool { return true })
			lr = scratch.AppendEmpty()
		} else {
			lr = sl.LogRecords().AppendEmpty()
		}
		ts := rec.ts
		if ts.IsZero() {
			ts = observed.AsTime()
		}
		lr.SetTimestamp(pcommon.NewTimestampFromTime(ts))
		lr.SetObservedTimestamp(observed)
		sev, sevText := severityOf(rec.level)
		lr.SetSeverityNumber(sev)
		lr.SetSeverityText(sevText)
		lr.Body().SetStr(body)
		putLogAttrs(lr.Attributes(), rec, r.cfg.Scrub)
		logattrs.Put(lr.Attributes(), extracted.Log)
		if r.cfg.Enrich {
			logenrich.Apply(lr, body)
		}
		if r.cfg.LogMetrics != nil {
			bm, ok := bound[key]
			if !ok {
				bm = r.cfg.LogMetrics.Bind(resAttrs[key])
				bound[key] = bm
			}
			resolver.rec, resolver.res = lr.Attributes(), resAttrs[key]
			bm.Add(resolver.valueFn, resolver.labelFn, body)
		}
		if scratched {
			resolver.rec, resolver.res = lr.Attributes(), resAttrs[key]
			resolver.sev = strings.ToLower(lr.SeverityText())
			if r.cfg.Rules.Keep(resolver.ruleFn, body) {
				scratch.MoveAndAppendTo(sl.LogRecords())
			} else {
				scratch.RemoveIf(func(plog.LogRecord) bool { return true })
				obs.LogRulesDropped.Inc()
			}
		}
	}
	// An all-dropped group leaves an empty ResourceLogs behind.
	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		rl.ScopeLogs().RemoveIf(func(sl plog.ScopeLogs) bool { return sl.LogRecords().Len() == 0 })
		return rl.ScopeLogs().Len() == 0
	})
	return ld
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
			sm.Scope().SetName(scopeName)
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
