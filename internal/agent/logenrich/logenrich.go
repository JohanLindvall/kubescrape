// Package logenrich applies github.com/JohanLindvall/enrich to exported log
// records: metadata parsed from the line itself (JSON, logfmt, or common
// plain-text formats) is promoted into the OTLP first-class fields and
// record attributes. The body is never modified.
package logenrich

import (
	"encoding/hex"
	"strings"
	"time"

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

// ApplyBody enriches one log record from its own body (the OTLP ingest path).
// Unlike Apply it never overwrites a timestamp, severity, trace/span ID or
// attribute the sender already set — the sender is authoritative.
func ApplyBody(lr plog.LogRecord) {
	apply(lr, lr.Body().Str(), false)
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
		if plausibleTime(e.Time, lr.Timestamp()) {
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
	putStr("azure.resource_group", e.ResourceGroup)
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

// A parsed line timestamp REPLACES the producer's on the overwrite path, and
// that is right: the producer's is when the line was read, the line's is when
// the event happened. The one case where it is reliably wrong is a ZONE-LESS
// timestamp, which enrichment must read as UTC (there is nothing else it could
// do) — so a container running with TZ set to anything else hands every record
// a time displaced by exactly that zone's offset, and the kernel-accurate
// CRI/journal time is discarded in its favour, silently, for that workload's
// whole history.
//
// The refusal keys on the SHAPE of that displacement rather than on its size,
// because size alone cannot tell a misread zone from a log that is legitimately
// old (an archived file, a backfilled batch, a message whose own timestamp
// predates its emission by design). A zone offset is a whole multiple of 15
// minutes — every offset in use is, from Nepal's +05:45 to Kiribati's +14:00 —
// between one quarter-hour and fourteen hours, and the gap between an
// application writing a line and the runtime reading it is milliseconds. A
// displacement that lands on that grid is a zone; anything else is data.
//
// What this deliberately does NOT catch, and does not try to: a zone-less log
// written with MINUTE precision (truncation moves it off the grid by up to a
// minute), an offset that is not a quarter-hour, and any displacement under 15
// minutes. What it costs when it fires wrongly — a line whose real emit-to-read
// delay happens to land within tolerance of a quarter-hour multiple — is that
// the record carries the ingest time, which ObservedTimestamp carries anyway,
// and the refusal is counted.
const (
	zoneQuantum   = 15 * time.Minute
	maxZoneOffset = 14 * time.Hour
	// zoneTolerance absorbs the write-to-read gap and a second-precision
	// timestamp's truncation. Widening it widens the window in which an
	// ordinary delay is mistaken for a zone, proportionally.
	zoneTolerance = 5 * time.Second
)

// plausibleTime reports whether a parsed line timestamp may replace the
// producer's own. A zero stamped time means the producer supplied none (a plain
// non-CRI file stamps none), so there is nothing to disagree with and the
// parsed time is taken whole however old it is.
func plausibleTime(parsed time.Time, stamped pcommon.Timestamp) bool {
	if stamped == 0 {
		return true
	}
	d := parsed.Sub(stamped.AsTime())
	if d < 0 {
		d = -d
	}
	if d < zoneQuantum-zoneTolerance || d > maxZoneOffset+zoneTolerance {
		return true
	}
	// On the grid, from either side of a quarter-hour boundary.
	off := d % zoneQuantum
	return off > zoneTolerance && off < zoneQuantum-zoneTolerance
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
