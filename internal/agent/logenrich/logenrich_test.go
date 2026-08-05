package logenrich

import (
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

func newRecord() plog.LogRecord {
	return plog.NewLogs().ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
}

func TestApplyJSON(t *testing.T) {
	lr := newRecord()
	line := `{"@t":"2026-01-02T03:04:05Z","@l":"Warning","@mt":"Handled {Count} items","@i":"abc123","SourceContext":"My.App.Worker","traceid":"0af7651916cd43dd8448eb211c80319c","spanid":"b7ad6b7169203331","msg":"Handled 3 items"}`
	lr.Body().SetStr(line)
	Apply(lr, line)

	if got := lr.Timestamp().AsTime(); !got.Equal(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)) {
		t.Errorf("timestamp = %v", got)
	}
	if lr.SeverityNumber() != plog.SeverityNumberWarn || lr.SeverityText() != "warn" {
		t.Errorf("severity = %v %q", lr.SeverityNumber(), lr.SeverityText())
	}
	if lr.TraceID() != pcommon.TraceID([16]byte{0x0a, 0xf7, 0x65, 0x19, 0x16, 0xcd, 0x43, 0xdd, 0x84, 0x48, 0xeb, 0x21, 0x1c, 0x80, 0x31, 0x9c}) {
		t.Errorf("trace id = %v", lr.TraceID())
	}
	if lr.SpanID() != pcommon.SpanID([8]byte{0xb7, 0xad, 0x6b, 0x71, 0x69, 0x20, 0x33, 0x31}) {
		t.Errorf("span id = %v", lr.SpanID())
	}
	if v, _ := lr.Attributes().Get("log.template"); v.Str() != "Handled {Count} items" {
		t.Errorf("log.template = %q", v.Str())
	}
	if v, _ := lr.Attributes().Get("log.source_context"); v.Str() != "My.App.Worker" {
		t.Errorf("log.source_context = %q", v.Str())
	}
	if lr.Body().Str() != line {
		t.Errorf("body modified: %q", lr.Body().Str())
	}
}

func TestApplyKeepsDefaultsWhenAbsent(t *testing.T) {
	lr := newRecord()
	orig := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	lr.SetTimestamp(pcommon.NewTimestampFromTime(orig))
	lr.SetSeverityNumber(plog.SeverityNumberInfo)
	lr.SetSeverityText("info")
	Apply(lr, "a plain line with no metadata whatsoever")

	if got := lr.Timestamp().AsTime(); !got.Equal(orig) {
		t.Errorf("timestamp overridden: %v", got)
	}
	if lr.SeverityNumber() != plog.SeverityNumberInfo {
		t.Errorf("severity overridden: %v", lr.SeverityNumber())
	}
	if lr.Attributes().Len() != 0 {
		t.Errorf("unexpected attributes: %v", lr.Attributes().AsRaw())
	}
}

func TestApplyLogfmtSeverityOverrides(t *testing.T) {
	lr := newRecord()
	lr.SetSeverityNumber(plog.SeverityNumberInfo)
	lr.SetSeverityText("info")
	Apply(lr, `ts=2026-01-02T03:04:05Z level=error msg="boom"`)

	if lr.SeverityNumber() != plog.SeverityNumberError || lr.SeverityText() != "error" {
		t.Errorf("severity = %v %q", lr.SeverityNumber(), lr.SeverityText())
	}
}

func TestApplyGUIDTraceID(t *testing.T) {
	lr := newRecord()
	Apply(lr, `{"request_id":"0af76519-16cd-43dd-8448-eb211c80319c","msg":"x"}`)
	if lr.TraceID().IsEmpty() {
		t.Error("dashed GUID trace id not parsed")
	}
}

func TestApplyStacktraceDeduped(t *testing.T) {
	// Pattern-parsed exceptions: the trace is a verbatim slice of the body
	// and must not be duplicated as an attribute.
	lr := newRecord()
	line := "Unhandled exception. System.InvalidOperationException: boom\n   at Acme.Worker.Run() in /src/Worker.cs:line 42"
	Apply(lr, line)
	if v, ok := lr.Attributes().Get("exception.stacktrace"); ok {
		t.Errorf("duplicated stacktrace attribute: %q", v.Str())
	}
	if v, _ := lr.Attributes().Get("exception.type"); v.Str() != "System.InvalidOperationException" {
		t.Errorf("exception.type = %q", v.Str())
	}

	// JSON-carried exceptions: the body is the raw JSON, the unescaped trace
	// is new information and stays.
	lr = newRecord()
	Apply(lr, `{"@l":"Error","@m":"boom","@x":"System.InvalidOperationException: boom\r\n   at Acme.Worker.Run()"}`)
	if v, ok := lr.Attributes().Get("exception.stacktrace"); !ok || !strings.Contains(v.Str(), "at Acme.Worker.Run()") {
		t.Errorf("JSON stacktrace attribute missing or wrong: %q", v.Str())
	}
}

func TestParseHexID(t *testing.T) {
	if _, ok := parseHexID("", 16); ok {
		t.Error("empty accepted")
	}
	if _, ok := parseHexID("zzf7651916cd43dd8448eb211c80319c", 16); ok {
		t.Error("non-hex accepted")
	}
	if _, ok := parseHexID("0af7", 16); ok {
		t.Error("short accepted")
	}
	if _, ok := parseHexID("00000000000000000000000000000000", 16); ok {
		t.Error("all-zero accepted")
	}
	if _, ok := parseHexID("b7ad6b7169203331", 8); !ok {
		t.Error("valid span id rejected")
	}
}

// ApplyBody (the ingest path) fills only what the sender left unset.
func TestApplyBodyNeverOverwrites(t *testing.T) {
	lr := plog.NewLogRecord()
	lr.Body().SetStr(`{"level":"error","message":"boom","ts":"2026-07-11T10:00:00Z"}`)

	// Sender set nothing: severity and timestamp come from the body.
	ApplyBody(lr)
	if lr.SeverityNumber() != plog.SeverityNumberError {
		t.Fatalf("severity = %v", lr.SeverityNumber())
	}
	if lr.Timestamp() == 0 {
		t.Fatal("timestamp not filled from body")
	}

	// Sender-set fields are authoritative.
	lr2 := plog.NewLogRecord()
	lr2.Body().SetStr(`{"level":"error","message":"boom"}`)
	lr2.SetSeverityNumber(plog.SeverityNumberInfo)
	lr2.SetSeverityText("INFO")
	ApplyBody(lr2)
	if lr2.SeverityNumber() != plog.SeverityNumberInfo {
		t.Fatalf("sender severity overwritten: %v", lr2.SeverityNumber())
	}
}

// Enrichment reads a ZONE-LESS timestamp as UTC, so a container running with TZ
// set to anything else hands every record a time off by that zone's offset. On
// the overwrite path that silently discarded the kernel-accurate CRI/journal
// time in favour of a value hours away, for that workload's whole history.
func TestImplausibleParsedTimeKeepsTheProducerTimestamp(t *testing.T) {
	ingest := time.Date(2026, 1, 2, 8, 4, 5, 0, time.UTC)
	// America/New_York wall clock for the same instant, with no zone on it.
	lr := newRecord()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(ingest))
	before := obs.LogEnrichTimeRejected.Value()
	Apply(lr, "2026-01-02 03:04:05 INFO handled request")
	if got := lr.Timestamp().AsTime(); !got.Equal(ingest) {
		t.Errorf("timestamp = %v, want the producer's %v", got, ingest)
	}
	if got := obs.LogEnrichTimeRejected.Value() - before; got != 1 {
		t.Errorf("kubescrape_log_enrich_time_rejected_total moved by %v, want 1", got)
	}
	// The severity from the same line is still applied: only the timestamp is
	// in doubt.
	if lr.SeverityNumber() != plog.SeverityNumberInfo {
		t.Errorf("severity = %v, want info", lr.SeverityNumber())
	}

	// Everything OFF the zone grid is data, not a misread zone, and the parsed
	// time wins: the moment the application recorded beats the moment the line
	// was read. A displacement of hours is exactly how an archived file or a
	// backfilled batch legitimately reads.
	for _, line := range []string{
		"2026-01-02 08:04:03 INFO handled request", // seconds apart
		"2026-01-02 01:22:37 INFO handled request", // hours, off the quarter-hour grid
		"2019-03-04 05:06:07 INFO handled request", // years
		"2026-01-02 23:04:05 INFO handled request", // 15h — past the largest zone offset
	} {
		lr := newRecord()
		lr.SetTimestamp(pcommon.NewTimestampFromTime(ingest))
		before := obs.LogEnrichTimeRejected.Value()
		Apply(lr, line)
		if got := lr.Timestamp().AsTime(); got.Equal(ingest) {
			t.Errorf("line %q: kept the producer timestamp; the parsed one must win", line)
		}
		if got := obs.LogEnrichTimeRejected.Value() - before; got != 0 {
			t.Errorf("line %q: counted as rejected (%v)", line, got)
		}
	}

	// Every zone offset in use is on the grid, including the quarter-hour ones.
	for _, line := range []string{
		"2026-01-02 02:19:05 INFO handled request", // +05:45 (Nepal)
		"2026-01-02 13:34:05 INFO handled request", // -05:30
		"2026-01-01 18:04:05 INFO handled request", // +14:00 (Kiribati), the largest
	} {
		lr := newRecord()
		lr.SetTimestamp(pcommon.NewTimestampFromTime(ingest))
		Apply(lr, line)
		if got := lr.Timestamp().AsTime(); !got.Equal(ingest) {
			t.Errorf("line %q: timestamp = %v, want the producer's %v", line, got, ingest)
		}
	}

	// With NO producer timestamp there is nothing to disagree with, and the
	// parsed time is taken whole — which is what keeps a plain (non-CRI) source,
	// whose records carry none, reading an archive correctly.
	none := newRecord()
	Apply(none, "2019-03-04 05:06:07 INFO handled request")
	if got := none.Timestamp().AsTime(); !got.Equal(time.Date(2019, 3, 4, 5, 6, 7, 0, time.UTC)) {
		t.Errorf("timestamp = %v, want the parsed 2019-03-04", got)
	}
}
