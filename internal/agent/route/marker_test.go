package route

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// A Router with NO destinations is what both agent chains terminate in when no
// routing: section is configured (and what the self-metrics chain always gets,
// since it forks below the real router). It exists for exactly one job: strip
// route.ScriptMarker. Left on, the reserved attribute a transform script's
// route() stamps ships to the collector as a resource attribute and changes the
// stream identity — target_info gains a label — of every resource the script
// touched.
func TestDestinationlessRouterStripsTheScriptMarker(t *testing.T) {
	def := &capDest{}
	r := New(def, nil)

	ld := nsLogs("team-b")
	ld.ResourceLogs().At(0).Resource().Attributes().PutStr(ScriptMarker, "b")

	if err := r.ExportLogs(context.Background(), ld); err != nil {
		t.Fatalf("ExportLogs: %v", err)
	}
	if len(def.logs) != 1 {
		t.Fatalf("default chain got %d payloads, want 1", len(def.logs))
	}
	got := def.logs[0].ResourceLogs().At(0).Resource().Attributes()
	if v, ok := got.Get(ScriptMarker); ok {
		t.Errorf("%s survived to the destination as %q; a destination-less Router "+
			"is the only thing that strips it", ScriptMarker, v.Str())
	}
	if bs := bodies(def.logs); len(bs) != 1 || bs[0] != "from team-b" {
		t.Errorf("payload = %v, want the record delivered to the default chain", bs)
	}
}

// The caller's payload is retried as-is by its producer, so the strip may only
// touch the split's COPY — mutating the input would corrupt a retry.
func TestMarkerStripDoesNotMutateTheCallersPayload(t *testing.T) {
	r := New(&capDest{}, nil)

	ld := nsLogs("team-b")
	ld.ResourceLogs().At(0).Resource().Attributes().PutStr(ScriptMarker, "b")

	if err := r.ExportLogs(context.Background(), ld); err != nil {
		t.Fatalf("ExportLogs: %v", err)
	}
	if _, ok := ld.ResourceLogs().At(0).Resource().Attributes().Get(ScriptMarker); !ok {
		t.Error("the marker was removed from the CALLER's payload; the producer re-sends " +
			"that same object on retry, so the strip must happen on the copy")
	}
}

// Metrics take the same path — this is the signal the marker actually reached a
// backend on, since a stray resource attribute re-identifies every series.
func TestDestinationlessRouterStripsTheMarkerOnMetrics(t *testing.T) {
	def := &capMetrics{}
	r := New(def, nil)

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("k8s.namespace.name", "team-b")
	rm.Resource().Attributes().PutStr(ScriptMarker, "b")
	rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetName("m")

	if err := r.ExportMetrics(context.Background(), md); err != nil {
		t.Fatalf("ExportMetrics: %v", err)
	}
	if len(def.got) != 1 {
		t.Fatalf("default chain got %d payloads, want 1", len(def.got))
	}
	if _, ok := def.got[0].ResourceMetrics().At(0).Resource().Attributes().Get(ScriptMarker); ok {
		t.Errorf("%s survived onto an exported ResourceMetrics", ScriptMarker)
	}
}

// An UNMARKED payload must still take the uncopied fast path: the strip is not
// allowed to cost the 4,000-resource KSM/cadvisor shape a copy per export.
func TestUnmarkedPayloadStillTakesTheFastPath(t *testing.T) {
	r := New(&capDest{}, nil)
	if groups := r.split(3, func(int) pcommon.Resource { return pcommon.NewResource() }); groups != nil {
		t.Errorf("split returned %v for an unmarked, destination-less payload; want nil "+
			"(the fast path forwards the original without copying)", groups)
	}
}

type capMetrics struct{ got []pmetric.Metrics }

func (c *capMetrics) ExportLogs(context.Context, plog.Logs) error { return nil }
func (c *capMetrics) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	c.got = append(c.got, md)
	return nil
}
