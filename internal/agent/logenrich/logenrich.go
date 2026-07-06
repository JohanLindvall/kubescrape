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
)

// Apply enriches one log record from its line. Fields already set on the
// record are overridden only when the line carries the corresponding
// metadata: a parsed timestamp replaces the record timestamp (the ingest
// time belongs in ObservedTimestamp), and an explicit level replaces the
// severity. enrich's severity numbers are the OTLP severity numbers.
func Apply(lr plog.LogRecord, line string) {
	e := enrich.Parse(line)
	if e == nil {
		return
	}

	if !e.Time.IsZero() {
		lr.SetTimestamp(pcommon.NewTimestampFromTime(e.Time))
	}
	if e.SeverityNumber > 0 {
		lr.SetSeverityNumber(plog.SeverityNumber(e.SeverityNumber))
		lr.SetSeverityText(e.Severity)
	}
	if id, ok := parseHexID(e.TraceID, 16); ok {
		lr.SetTraceID(pcommon.TraceID(id))
	}
	if id, ok := parseHexID(e.SpanID, 8); ok {
		lr.SetSpanID(pcommon.SpanID([8]byte(id[:8])))
	}

	attrs := lr.Attributes()
	putStr := func(key, val string) {
		if val != "" {
			attrs.PutStr(key, val)
		}
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
	putStr("exception.stacktrace", e.ExceptionStackTrace)
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
	buf := make([]byte, want)
	if _, err := hex.Decode(buf, []byte(s)); err != nil {
		return out, false
	}
	nonzero := false
	for _, b := range buf {
		if b != 0 {
			nonzero = true
			break
		}
	}
	if !nonzero {
		return out, false
	}
	copy(out[:], buf)
	return out, true
}
