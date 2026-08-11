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

// ApplyBodyText (the ingest path) fills only what the sender left unset.
func TestApplyBodyNeverOverwrites(t *testing.T) {
	lr := plog.NewLogRecord()
	lr.Body().SetStr(`{"level":"error","message":"boom","ts":"2026-07-11T10:00:00Z"}`)

	// Sender set nothing: severity and timestamp come from the body.
	ApplyBodyText(lr, lr.Body().Str())
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
	ApplyBodyText(lr2, lr2.Body().Str())
	if lr2.SeverityNumber() != plog.SeverityNumberInfo {
		t.Fatalf("sender severity overwritten: %v", lr2.SeverityNumber())
	}
}

// Enrichment reads a ZONE-LESS timestamp as UTC, so a container running with TZ
// set to anything else hands every record a time off by that zone's offset. On
// the overwrite path that silently discarded the kernel-accurate CRI/journal
// time in favour of a value hours away, for that workload's whole history.
// enrich reports that ambiguity (Result.TimeHasZone), so the rule is exact: a
// wall clock never displaces a producer timestamp, and an instant always may.
func TestZonelessParsedTimeKeepsTheProducerTimestamp(t *testing.T) {
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

	// Every zone-less shape is refused the same way whatever the displacement
	// happens to be — including the cases the quarter-hour-grid heuristic this
	// replaced could not see: a stamp seconds away, one a few minutes away, and
	// a minute-precision one whose truncation puts it off any grid.
	for _, line := range []string{
		"2026-01-02 08:04:03 INFO handled request",              // seconds apart
		"2026-01-02 08:01:05 INFO handled request",              // three minutes
		"2026-01-02 01:22:37 INFO handled request",              // hours, off any grid
		"2019-03-04 05:06:07 INFO handled request",              // years
		"2026-01-02 02:19:05 INFO handled request",              // +05:45 (Nepal)
		"2026-01-01 18:04:05 INFO handled request",              // +14:00 (Kiribati), the largest
		"2026/01/02 03:04:05 [info] handled request",            // slash date
		"I0102 03:04:05.123456       1 controller.go:42] hello", // klog
	} {
		lr := newRecord()
		lr.SetTimestamp(pcommon.NewTimestampFromTime(ingest))
		before := obs.LogEnrichTimeRejected.Value()
		Apply(lr, line)
		if got := lr.Timestamp().AsTime(); !got.Equal(ingest) {
			t.Errorf("line %q: timestamp = %v, want the producer's %v", line, got, ingest)
		}
		if got := obs.LogEnrichTimeRejected.Value() - before; got != 1 {
			t.Errorf("line %q: rejection counted %v times, want 1", line, got)
		}
	}

	// A timestamp that states its own offset is an instant, not a guess, and
	// wins however far it is from the ingest time: an archived file or a
	// backfilled batch legitimately reads hours or years old, and nothing about
	// it is ambiguous.
	for _, line := range []string{
		"2019-03-04T05:06:07Z INFO handled request",           // years old, UTC
		"2019-03-04 05:06:07.000 +02:00 [main] INFO: handled", // offset after a space
		`{"@t":"2019-03-04T05:06:07Z","@l":"Info","@m":"x"}`,  // JSON
		`ts=2019-03-04T05:06:07Z level=info msg=x`,            // logfmt
	} {
		lr := newRecord()
		lr.SetTimestamp(pcommon.NewTimestampFromTime(ingest))
		before := obs.LogEnrichTimeRejected.Value()
		Apply(lr, line)
		if got := lr.Timestamp().AsTime(); got.Equal(ingest) {
			t.Errorf("line %q: kept the producer timestamp; a zoned parsed time must win", line)
		}
		if got := obs.LogEnrichTimeRejected.Value() - before; got != 0 {
			t.Errorf("line %q: counted as rejected (%v)", line, got)
		}
	}

	// The zoned spelling of the very first line resolves to exactly the instant
	// the producer stamped — which is the whole point: the zone was the missing
	// information, not the value.
	zoned := newRecord()
	zoned.SetTimestamp(pcommon.NewTimestampFromTime(ingest))
	Apply(zoned, "2026-01-02T03:04:05-05:00 INFO handled request")
	if got := zoned.Timestamp().AsTime(); !got.Equal(ingest) {
		t.Errorf("timestamp = %v, want %v", got, ingest)
	}

	// With NO producer timestamp there is nothing better to keep, and the parsed
	// time is taken whole however old and however ambiguous — which is what keeps
	// a plain (non-CRI) source, whose records carry none, reading an archive
	// correctly.
	none := newRecord()
	before = obs.LogEnrichTimeRejected.Value()
	Apply(none, "2019-03-04 05:06:07 INFO handled request")
	if got := none.Timestamp().AsTime(); !got.Equal(time.Date(2019, 3, 4, 5, 6, 7, 0, time.UTC)) {
		t.Errorf("timestamp = %v, want the parsed 2019-03-04", got)
	}
	if got := obs.LogEnrichTimeRejected.Value() - before; got != 0 {
		t.Errorf("counted as rejected (%v) with no producer timestamp to keep", got)
	}

	// Same on the ingest path (ApplyBodyText), where the sender set no timestamp: a
	// zone-less body stamp still fills it.
	body := plog.NewLogRecord()
	body.Body().SetStr("2019-03-04 05:06:07 INFO handled request")
	ApplyBodyText(body, body.Body().Str())
	if got := body.Timestamp().AsTime(); !got.Equal(time.Date(2019, 3, 4, 5, 6, 7, 0, time.UTC)) {
		t.Errorf("ApplyBodyText timestamp = %v, want the parsed 2019-03-04", got)
	}
}

// TestAzureAttributeKeys pins the record attributes an Azure-shaped line
// stamps, and the SHAPE of each value — the pair is what makes the key names
// load-bearing rather than cosmetic.
//
// `azure.resource_group.id` carries the resource group's own full ARM resource
// ID, never its bare name. It was spelled `azure.resource_group` until a
// semconv v1.44.0 audit: the registry defines `azure.resource_group.name` (the
// bare name) and nothing else in that namespace, so the bare key sat one dot
// from a real attribute while holding a different shape of its value — the
// silent-conflict case semconv's naming guidance warns about. Renaming to
// `.id` was wire-visible, so this test exists to make a revert deliberate.
func TestAzureAttributeKeys(t *testing.T) {
	lr := newRecord()
	line := `{"resourceId":"/SUBSCRIPTIONS/11111111-1111-1111-1111-111111111111/RESOURCEGROUPS/SHOP/PROVIDERS/MICROSOFT.WEB/SITES/ORDERS","eventCategory":"Administrative"}`
	lr.Body().SetStr(line)
	Apply(lr, line)

	want := map[string]string{
		"cloud.resource_id":       "/subscriptions/11111111-1111-1111-1111-111111111111/resourcegroups/shop/providers/microsoft.web/sites/orders",
		"azure.resource_group.id": "/subscriptions/11111111-1111-1111-1111-111111111111/resourcegroups/shop",
		"azure.event_category":    "Administrative",
	}
	for key, exp := range want {
		v, ok := lr.Attributes().Get(key)
		if !ok {
			t.Errorf("missing %s", key)
			continue
		}
		if v.Str() != exp {
			t.Errorf("%s = %q, want %q", key, v.Str(), exp)
		}
	}

	// The pre-audit spelling must not come back alongside the new one.
	if _, ok := lr.Attributes().Get("azure.resource_group"); ok {
		t.Error("azure.resource_group is the pre-semconv-audit key; only azure.resource_group.id may be emitted")
	}
	// This path never mints the registry's own key: it holds the BARE name,
	// which enrich does not extract. Only the azurediag converter, which parses
	// the ARM id from the envelope, is authoritative enough to set it.
	if _, ok := lr.Attributes().Get("azure.resource_group.name"); ok {
		t.Error("azure.resource_group.name is the converter's to set, from the envelope's own resourceId")
	}
}
