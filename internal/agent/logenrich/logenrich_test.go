package logenrich

import (
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
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
