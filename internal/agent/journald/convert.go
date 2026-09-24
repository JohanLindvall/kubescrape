package journald

// Journal entries -> OTLP log records. What is journald's own is the unit's
// RESOURCE, its name (groupUnit) and the grouping; the per-record half is the
// chain every log producer runs (internal/agent/logchain).

import (
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/logchain"
)

// ScopeName is the OTLP instrumentation-scope name on every journal record
// (wire-visible: Loki/Elastic see it as a label, so changing it splits every
// journal stream at the upgrade boundary).
const ScopeName = "github.com/JohanLindvall/kubescrape/agent/journald"

// identGroupTag prefixes the group key of an entry named by its syslog
// identifier rather than a unit (see groupUnit).
const identGroupTag = "i\x02"

// groupUnit is the name an entry's resource carries and the key convert groups
// it under — the ONE naming rule, which convert, the resource it builds and
// debugBatch's report all read.
//
// A real systemd unit names itself. An entry without one (the kernel, a
// syslog-only process) falls back to its SYSLOG_IDENTIFIER, and one carrying
// neither to "journald". The fallback's key is TAGGED: a syslog identifier that
// happens to equal another entry's full unit name must not share its group —
// the group's resource carries systemd.unit only for real units, so sharing
// mis-attributes whichever entry arrives second.
func (e *entry) groupUnit() (name, key string) {
	if e.unit != "" {
		return e.unit, e.unit
	}
	name = e.ident
	if name == "" {
		name = "journald"
	}
	return name, identGroupTag + e.ident
}

// convert groups the batch into one resource per unit.
//
// The per-record half — line attributes, enrichment, log-metrics (which see
// EVERY entry) and the keep/drop rules (which run after enrichment, so
// __severity__ selects on the ENRICHED severity) — is the chain every log
// producer in this repo runs (internal/agent/logchain). What stays here is the
// unit's RESOURCE and the grouping.
//
// Bodies are already scrubbed: journald redacts where it builds the batch
// entry, before the record exists, so the chain's Scrub is nil.
func (r *Reader) convert() plog.Logs {
	ld := plog.NewLogs()
	// Every other log producer names its scope (tailer, events); the
	// journal's records once shipped with an empty otel_scope_name.
	groups := logchain.NewGroups(ld, ScopeName, 4)
	sink := &recordSink{r: r, observed: pcommon.NewTimestampFromTime(time.Now())}
	cc := r.cfg.Chain
	cc.Scrub = nil // scrubbed where the batch entry was built (stream)
	chain := logchain.NewChain[string](cc, false)
	for _, e := range r.batch {
		// false: journald retries a failed batch in place and never rebuilds
		// one, so no record reaches the chain twice (logchain.Input.Observed).
		body, extracted := chain.Line(e.body, false)
		unit, groupKey := e.groupUnit()
		key := chain.GroupKey(groupKey, extracted)
		// The group is built BEFORE the record because metric and rule
		// resolution reads the group's own resource; a group the rules empty is
		// pruned below rather than never created (the tailer, whose resolution
		// uses the FILE's resource, can be lazy instead — same payload).
		sink.e, sink.unit = e, unit
		sink.e.body = body // identical while Scrub is nil; not a fact to rely on
		ent := groups.Get(key, extracted, sink)
		sink.sl = ent.SL
		chain.Emit(sink, logchain.Input[string]{
			Body: body, Lifted: extracted, Resource: ent.Res, BoundKey: key,
		})
	}
	// An all-dropped unit leaves an empty group behind; prune so the payload
	// carries no record-less ResourceLogs.
	logchain.Prune(ld)
	return ld
}

// recordSink is the chain's Producer for journal entries: the group a kept
// record lands in, and what the journal knows about the record.
type recordSink struct {
	r        *Reader
	sl       plog.ScopeLogs
	e        entry
	unit     string // the group's name (entry.groupUnit)
	observed pcommon.Timestamp
}

func (s *recordSink) Dest() plog.LogRecordSlice { return s.sl.LogRecords() }

// FillResource builds a fresh unit group's resource. Identity attributes go
// in before Build so templates and the filter see them.
func (s *recordSink) FillResource(res pcommon.Resource) {
	res.Attributes().PutStr("service.name", strings.TrimSuffix(s.unit, ".service"))
	if s.e.unit != "" {
		res.Attributes().PutStr("systemd.unit", s.e.unit)
	}
	actx := attrs.Context{}
	if s.r.cfg.NodeInfo != nil {
		actx.Node = s.r.cfg.NodeInfo()
	}
	s.r.cfg.Attrs.Build(res, actx)
}

func (s *recordSink) Stamp(lr plog.LogRecord) {
	e := s.e
	lr.SetTimestamp(pcommon.NewTimestampFromTime(e.ts))
	lr.SetObservedTimestamp(s.observed)
	lr.SetSeverityNumber(e.severity)
	lr.SetSeverityText(e.sevText)
	lr.Body().SetStr(e.body)
	if e.origLen > 0 {
		lr.Attributes().PutBool(logchain.AttrTruncated, true)
		lr.Attributes().PutInt(logchain.AttrOriginalLength, int64(e.origLen))
	}
	if e.ident != "" {
		lr.Attributes().PutStr("syslog.identifier", e.ident)
	}
	if e.pid != 0 {
		lr.Attributes().PutInt("process.pid", e.pid)
	}
	if e.transport != "" {
		lr.Attributes().PutStr("systemd.transport", e.transport)
	}
}

// severity maps a syslog priority (0-7) to OTLP severity, following the
// mapping in the OpenTelemetry logs data model.
//
// The top three syslog severities are FATAL, not ERROR: emergency is FATAL3
// (23), alert FATAL2 (22), critical FATAL (21). This used to map them onto
// FATAL/ERROR3/ERROR2 — one grade too low across the board, and contradicted
// one line away by the enrich package's own syslogSeverity table, which
// logenrich.Apply then OVERWROTE the number with whenever it managed to parse
// the message. The same journal entry therefore reported a different severity
// depending on whether its body happened to look like a log line to a parser,
// which is the one thing a severity must not depend on.
//
// The text stays the source's own syslog word (the data model asks SeverityText
// to carry the original), lowercase — which is now the casing every producer in
// the repo uses, and the casing enrich writes when it overwrites. The grading
// the six canonical level names flatten away survives in the NUMBER: emerg,
// alert and crit are all "fatal" to enrich but 23/22/21 here, and notice is
// INFO2 rather than INFO.
func severity(priority string) (plog.SeverityNumber, string) {
	switch priority {
	case "0":
		return plog.SeverityNumberFatal3, "emerg"
	case "1":
		return plog.SeverityNumberFatal2, "alert"
	case "2":
		return plog.SeverityNumberFatal, "crit"
	case "3":
		return plog.SeverityNumberError, "err"
	case "4":
		return plog.SeverityNumberWarn, "warning"
	case "5":
		return plog.SeverityNumberInfo2, "notice"
	case "6":
		return plog.SeverityNumberInfo, "info"
	case "7":
		return plog.SeverityNumberDebug, "debug"
	}
	return plog.SeverityNumberUnspecified, ""
}
