package azurediag

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
)

// The enricher re-derives an ARM identity from the record BODY, while the
// converter parses it from the envelope's own resourceId field and puts it on
// the OTLP resource. Where both fire, one payload used to ship two values under
// `cloud.resource_id` — the resource's verbatim, enrich's lowercased — plus
// `azure.resource_group` (the group's full ARM id) beside the resource's
// `azure.resource_group.name` (the bare name). Measured against a live Event
// Hub: 286 bytes on every record, 1 MB per 3,500-record payload, and a value a
// flattening backend picks between on its own.

// TestIdentityIsNotDuplicatedOntoEveryRecord drives the real convert path:
// identity stays on the resource, and the per-record Azure attributes that
// genuinely vary survive the elision.
func TestIdentityIsNotDuplicatedOntoEveryRecord(t *testing.T) {
	exp := &captureExporter{}
	src := newFakeSource([][]byte{[]byte(logEnvelope)})
	r := newTestReader(Config{Exporter: exp, Enrich: true}, src)
	runUntilCommit(t, r, src)

	exp.mu.Lock()
	if len(exp.logs) == 0 {
		exp.mu.Unlock()
		t.Fatal("no payload exported")
	}
	rls := exp.logs[0].ResourceLogs()
	if rls.Len() == 0 {
		exp.mu.Unlock()
		t.Fatal("no resource logs exported")
	}
	res := rls.At(0).Resource().Attributes()
	gotID, hasID := res.Get("cloud.resource_id")
	_, hasGroup := res.Get("azure.resource_group.name")
	exp.mu.Unlock()

	if !hasID {
		t.Fatal("resource lost cloud.resource_id")
	}
	if gotID.Str() != armID {
		t.Fatalf("resource cloud.resource_id = %q, want the verbatim %q", gotID.Str(), armID)
	}
	if !hasGroup {
		t.Fatal("resource lost azure.resource_group.name")
	}

	recs := exp.records()
	if len(recs) == 0 {
		t.Fatal("no records exported")
	}
	for _, rec := range recs {
		for _, key := range []string{"cloud.resource_id", "azure.resource_group"} {
			if v, ok := rec.Attributes().Get(key); ok {
				t.Errorf("record still carries %s = %q; the resource already states it", key, v.Str())
			}
		}
		// Eliding identity must not become eliding the record's own metadata.
		if _, ok := rec.Attributes().Get("azure.category"); !ok {
			t.Error("record lost azure.category, which varies per record and is not on the resource")
		}
	}
}

// TestIdentityIsKeptWhenTheResourceHasNone pins the gate, which is what makes
// the elision safe: a resource the converter could not identify (no resourceId
// on the envelope, or one parseResourceID could not read) leaves enrich's
// body-derived attributes as the ONLY carrier, and dropping them there would
// lose the identity rather than de-duplicate it.
//
// Driven directly rather than through convertLogs: enrich reads the same
// `resourceId` JSON key the envelope decoder does, so the two cannot be made to
// disagree from the wire.
func TestIdentityIsKeptWhenTheResourceHasNone(t *testing.T) {
	ld := plog.NewLogs()

	identified := ld.ResourceLogs().AppendEmpty()
	identified.Resource().Attributes().PutStr("cloud.resource_id", armID)
	addRecordWithIdentity(identified.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty())

	anonymous := ld.ResourceLogs().AppendEmpty()
	anonymous.Resource().Attributes().PutStr("cloud.provider", "azure")
	addRecordWithIdentity(anonymous.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty())

	elideRedundantIdentity(ld)

	kept := identified.ScopeLogs().At(0).LogRecords().At(0).Attributes()
	for _, key := range []string{"cloud.resource_id", "azure.resource_group"} {
		if _, ok := kept.Get(key); ok {
			t.Errorf("identified resource: record kept %s, want it elided", key)
		}
	}
	if _, ok := kept.Get("azure.category"); !ok {
		t.Error("identified resource: record lost azure.category")
	}

	orphan := anonymous.ScopeLogs().At(0).LogRecords().At(0).Attributes()
	for _, key := range []string{"cloud.resource_id", "azure.resource_group"} {
		if _, ok := orphan.Get(key); !ok {
			t.Errorf("unidentified resource: record lost %s, so the identity is now nowhere", key)
		}
	}
}

func addRecordWithIdentity(lr plog.LogRecord) {
	a := lr.Attributes()
	a.PutStr("cloud.resource_id", "/subscriptions/s/resourcegroups/rg/providers/x/y/z")
	a.PutStr("azure.resource_group", "/subscriptions/s/resourcegroups/rg")
	a.PutStr("azure.category", "AppLogs")
}
