package otlpingest

// Regression tests for the SEAMS between the per-request budgets: the lookup
// allowance the auto-mode decision spends on probes is also the allowance the
// sender's own attribution needs, and the decision's two halves cost wildly
// different amounts.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

// A push whose data points name thousands of unresolvable ids must still
// attribute the SENDER. The auto-mode decision probes every distinct point id
// (an unresolvable one is deliberately not evidence of anything, so the walk
// does not stop), and probes used to charge the same allowance the resource's
// own attribution is paid from — so a resource carrying a perfectly resolvable
// container.id came out unenriched, counted unresolved, indistinguishable from
// an id the cluster never had. Any pod could do it to itself with one label.
func TestAttributionSurvivesABudgetExhaustingProbeWalk(t *testing.T) {
	meta := &recordingMeta{fakeMeta: newMeta()}
	e := NewEnricher(Config{Meta: meta, MetricsMode: MetricsAuto})

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.id", "cafe01") // resolvable
	dps := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().
		SetEmptyGauge().DataPoints()
	points := maxLookupsPerRequest + 10
	for i := 0; i < points; i++ {
		dp := dps.AppendEmpty()
		dp.SetIntValue(1)
		dp.Attributes().PutStr("k8s.pod.uid", fmt.Sprintf("bogus-%d", i))
	}

	out := e.EnrichMetrics(context.Background(), md)

	if got := out.DataPointCount(); got != points {
		t.Fatalf("forwarded %d points, want all %d", got, points)
	}
	a := out.ResourceMetrics().At(0).Resource().Attributes()
	if v, ok := a.Get("k8s.pod.name"); !ok || v.Str() != "web-1" {
		t.Errorf("k8s.pod.name = %q (present=%v), want web-1: the probe walk spent the sender's attribution budget", v.Str(), ok)
	}
	if meta.calls > maxLookupsPerRequest {
		t.Errorf("lookups = %d, want <= %d: the reserved share must not raise the total", meta.calls, maxLookupsPerRequest)
	}
}

// The decision's cheap half must run to completion before its expensive half
// starts: one resource with NO id at all settles the whole push, so no other
// resource's data points are worth walking — and that walk is what spends the
// lookup budget, on probes whose answer cannot change the outcome.
func TestResourceIDPassPrecedesThePointWalk(t *testing.T) {
	meta := &recordingMeta{fakeMeta: newMeta()}
	e := NewEnricher(Config{Meta: meta, MetricsMode: MetricsAuto, Wait: 3 * time.Second})

	md := pmetric.NewMetrics()
	rm0 := md.ResourceMetrics().AppendEmpty()
	rm0.Resource().Attributes().PutStr("container.id", "cafe01")
	dps := rm0.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().
		SetEmptyGauge().DataPoints()
	for i := 0; i < 5000; i++ {
		dp := dps.AppendEmpty()
		dp.SetIntValue(1)
		dp.Attributes().PutStr("container.id", fmt.Sprintf("bogus-%d", i))
	}
	// The decisive resource: no container id, no pod uid, so resource mode
	// cannot suffice whatever rm0's points say.
	rm1 := md.ResourceMetrics().AppendEmpty()
	rm1.Resource().Attributes().PutStr("service.name", "no-id-here")
	dp := rm1.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().
		SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetIntValue(1)

	e.EnrichMetrics(context.Background(), md)

	// A wait-free container lookup is a resolvability PROBE (attribution carries
	// Config.Wait), and no probe may be issued for a decision already made.
	for id, waits := range meta.waits {
		for _, w := range waits {
			if w == 0 {
				t.Fatalf("probed %s while a later resource already decided the answer", id)
			}
		}
	}
}
