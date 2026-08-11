// Package logenrich applies github.com/JohanLindvall/enrich to exported log
// records: metadata parsed from the line itself (JSON, logfmt, or common
// plain-text formats) is promoted into the OTLP first-class fields and
// record attributes. The body is never modified.
package logenrich

import (
	"encoding/hex"
	"strings"

	"github.com/JohanLindvall/enrich"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// Apply enriches one log record from an explicit line (the tailer/journald
// path). A parsed timestamp replaces the record timestamp (the CRI/journal
// ingest time belongs in ObservedTimestamp) and an explicit level replaces
// the severity; enrich's severity numbers are the OTLP severity numbers.
func Apply(lr plog.LogRecord, line string) {
	apply(lr, line, true)
}

// ApplyBodyText enriches one log record from a pre-rendered text view of its
// body (the OTLP ingest path). Unlike Apply it never overwrites a timestamp,
// severity, trace/span ID or attribute the sender already set — the sender is
// authoritative. The CALLER renders and passes the view rather
// than this package reading lr.Body() itself, for two reasons the adversarial
// review measured: a STRUCTURED body's Str() is "" (so reading it here meant
// the same logical `{"level":"error"}` line was severity-parsed when tailed
// and passed through untouched when pushed), and its AsString rendering costs
// 14-18x the wire bytes in transient heap — so it must be rendered ONCE,
// bounded, and shared by enrichment, log-metrics and the rules (otlpingest's
// chainBody), never re-rendered per consumer.
func ApplyBodyText(lr plog.LogRecord, body string) {
	apply(lr, body, false)
}

// The enrichment-outcome counters, resolved once per format: the label values
// are a closed set, so the per-line WithLabelValues map probe (1.5% of flush
// CPU) is pure overhead.
var (
	enrichedJSON    = obs.LogEnriched.WithLabelValues(enrich.FormatJSON)
	enrichedLogfmt  = obs.LogEnriched.WithLabelValues(enrich.FormatLogfmt)
	enrichedPattern = obs.LogEnriched.WithLabelValues(enrich.FormatPattern)
	enrichedNone    = obs.LogEnriched.WithLabelValues("none")
)

func enrichedCounter(format string) *metrics.RegCounter {
	switch format {
	case enrich.FormatJSON:
		return enrichedJSON
	case enrich.FormatLogfmt:
		return enrichedLogfmt
	case enrich.FormatPattern:
		return enrichedPattern
	default:
		return enrichedNone
	}
}

// apply parses line and promotes its metadata onto lr. When overwrite is
// false, only fields the record leaves unset are filled.
func apply(lr plog.LogRecord, line string, overwrite bool) {
	var res enrich.Result // stack-held; ParseInto avoids the per-line heap Result
	enrich.ParseInto(line, &res)
	e := &res
	enrichedCounter(e.Format).Inc()

	if !e.Time.IsZero() && (overwrite || lr.Timestamp() == 0) {
		if mayReplaceTimestamp(e.TimeHasZone, lr.Timestamp()) {
			lr.SetTimestamp(pcommon.NewTimestampFromTime(e.Time))
		} else {
			obs.LogEnrichTimeRejected.Inc()
		}
	}
	if e.SeverityNumber > 0 && (overwrite || (lr.SeverityNumber() == plog.SeverityNumberUnspecified && lr.SeverityText() == "")) {
		// Non-overwrite (ApplyBody): a sender-set SeverityText counts as "the
		// sender expressed severity" even with the number unset — clobbering the
		// text would discard sender intent.
		lr.SetSeverityNumber(plog.SeverityNumber(e.SeverityNumber))
		lr.SetSeverityText(e.Severity)
	}
	if (overwrite || lr.TraceID().IsEmpty()) && e.TraceID != "" {
		if id, ok := parseHexID(e.TraceID, 16); ok {
			lr.SetTraceID(pcommon.TraceID(id))
		}
	}
	if (overwrite || lr.SpanID().IsEmpty()) && e.SpanID != "" {
		if id, ok := parseHexID(e.SpanID, 8); ok {
			lr.SetSpanID(pcommon.SpanID([8]byte(id[:8])))
		}
	}

	attrs := lr.Attributes()
	putStr := func(key, val string) {
		if val == "" {
			return
		}
		if !overwrite {
			if _, exists := attrs.Get(key); exists {
				return
			}
		}
		attrs.PutStr(key, val)
	}
	putStr("log.template", e.Template)
	putStr("log.template_hash", e.TemplateHash)
	putStr("log.source_context", e.SourceContext)
	putStr("log.service", e.Service)
	putStr("log.service_version", e.Version)
	putStr("log.product", e.Product)
	putStr("cloud.resource_id", e.ResourceID)
	putStr("azure.resource_group.id", e.ResourceGroupID)
	putStr("azure.event_category", e.EventCategory)
	putStr("exception.type", e.ExceptionType)
	putStr("exception.message", e.ExceptionMessage)
	// Pattern-parsed stack traces are verbatim slices of the body (which can
	// be a megabyte-scale multiline entry) — exporting them again as an
	// attribute doubles the record. JSON-carried traces stay: there the body
	// is the raw JSON, not the readable trace.
	if e.Format != enrich.FormatPattern {
		putStr("exception.stacktrace", e.ExceptionStackTrace)
	}
}

// mayReplaceTimestamp reports whether a timestamp parsed from the line may be
// stamped on the record, given the producer's own (0 = the producer supplied
// none). A parsed timestamp REPLACES the producer's on the overwrite path,
// EXCEPT a zone-less one, which may only fill a producer timestamp that is
// absent.
//
// Replacing is usually right: the producer's stamp is when the line was read,
// the line's is when the event happened. The exception is a timestamp with no
// zone (enrich.Result.TimeHasZone false — klog's "MMDD hh:mm:ss", syslog
// RFC3164's, "2006-01-02 15:04:05", the slash-date forms). enrich must read
// those as UTC because nothing in the line says otherwise, so a container
// running with TZ set to anything else hands every record a time displaced by
// exactly that zone's offset — silently, for that workload's whole history.
// Against an ambiguous wall clock the producer's kernel-accurate CRI/journal
// stamp is the better datum: for a UTC container the two differ only by the
// write-to-read gap, and for any other container keeping the producer's is
// simply correct.
//
// The ambiguity is knowable AT THE PARSE, so it is read off the parse rather
// than guessed at afterwards from the displacement's size or shape. (The
// quarter-hour-grid heuristic this replaced could see neither a
// minute-precision stamp nor a sub-quarter-hour offset, and refused a
// legitimately displaced timestamp that happened to land near a boundary.)
//
// With NO producer timestamp — a plain (non-CRI) tailer source, an ingest
// record whose sender set none — there is nothing better to keep, so a
// zone-less time is taken whole however old it is. That is what keeps an
// archived plain file reading correctly.
//
// What the rule costs: an application that buffers and writes an OLD zone-less
// timestamp loses it in favour of the ingest time. That timestamp was the more
// accurate one — but only if its writer's clock was UTC, which is exactly what
// a zone-less stamp cannot tell us; the structured formats an emitter that
// cares about back-dating tends to use (JSON, logfmt, RFC3339) all carry a
// zone and are unaffected; ObservedTimestamp carries the ingest time either
// way; and the refusal is counted (obs.LogEnrichTimeRejected).
func mayReplaceTimestamp(hasZone bool, producer pcommon.Timestamp) bool {
	return hasZone || producer == 0
}

// parseHexID decodes an ID of want bytes from hex, tolerating dashes
// (GUID-style trace IDs). An all-zero ID is invalid in OTLP and rejected.
func parseHexID(s string, want int) ([16]byte, bool) {
	var out [16]byte
	if s == "" {
		return out, false
	}
	s = strings.ReplaceAll(s, "-", "")
	if len(s) != want*2 {
		return out, false
	}
	// Decode straight into out (no throwaway buffer); want is 8 or 16, both ≤ 16.
	if _, err := hex.Decode(out[:want], []byte(s)); err != nil {
		return out, false
	}
	nonzero := false
	for _, b := range out[:want] {
		if b != 0 {
			nonzero = true
			break
		}
	}
	if !nonzero {
		return [16]byte{}, false // reject an all-zero ID
	}
	return out, true
}
