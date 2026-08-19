package cumagg_test

import (
	"context"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/agent/cumagg"
	"github.com/JohanLindvall/kubescrape/internal/agent/servicegraph"
	"github.com/JohanLindvall/kubescrape/internal/agent/spanmetrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// The two aggregators' staleAfter fields mean the same thing, and for a while
// they did not answer the same way: serviceGraph.staleAfter refused a negative
// duration, traceMetrics.staleAfter CLAMPED it to zero — which is that field's
// "disable eviction" spelling. So `traceMetrics: {staleAfter: "-15m"}` passed
// -check-config, passed New, and silently turned the cardinality cap into the
// one-way latch that eviction exists to prevent, on the one configuration where
// an operator plainly meant to set a window.
//
// Both surfaces now parse through cumagg.ParseStaleAfter. This test is the
// cross-package assertion that they still agree: it is deliberately outside
// both packages, because agreement between them is the property, and a test
// inside either one can only see its own half.
func TestNegativeStaleAfterRejectedByBothConfigSurfaces(t *testing.T) {
	for _, value := range []string{"-15m", "-1s", "-1ns"} {
		t.Run(value, func(t *testing.T) {
			sgErr := (&servicegraph.Config{StaleAfter: value}).Validate()
			smErr := spanmetrics.Config{StaleAfter: value}.Validate()
			if sgErr == nil {
				t.Errorf("serviceGraph.staleAfter accepted %q", value)
			}
			if smErr == nil {
				t.Errorf("traceMetrics.staleAfter accepted %q (the clamp is back: a negative "+
					"value disables eviction and the cardinality cap becomes a one-way latch)", value)
			}
			if sgErr == nil || smErr == nil {
				return
			}
			// Same shape of message, each naming its OWN config path: the rule
			// is shared, the field name an operator has to fix is not.
			for _, c := range []struct {
				name, field string
				err         error
			}{
				{"serviceGraph", "serviceGraph.staleAfter", sgErr},
				{"traceMetrics", "traceMetrics.staleAfter", smErr},
			} {
				for _, want := range []string{c.field, value, "negative", `"0"`} {
					if !strings.Contains(c.err.Error(), want) {
						t.Errorf("%s error %q does not name %q", c.name, c.err, want)
					}
				}
			}
		})
	}
}

// The other half of the same drift: a valid value, and the "0 disables it"
// escape hatch, must also read identically on both surfaces.
func TestStaleAfterSpellingsAgree(t *testing.T) {
	for _, value := range []string{"", "0", "0s", "5m", "15m"} {
		if err := (&servicegraph.Config{StaleAfter: value}).Validate(); err != nil {
			t.Errorf("serviceGraph.staleAfter %q rejected: %v", value, err)
		}
		if err := (spanmetrics.Config{StaleAfter: value}).Validate(); err != nil {
			t.Errorf("traceMetrics.staleAfter %q rejected: %v", value, err)
		}
	}
	for _, value := range []string{"fifteen minutes", "15", "m15"} {
		if err := (&servicegraph.Config{StaleAfter: value}).Validate(); err == nil {
			t.Errorf("serviceGraph.staleAfter accepted the unparseable %q", value)
		}
		if err := (spanmetrics.Config{StaleAfter: value}).Validate(); err == nil {
			t.Errorf("traceMetrics.staleAfter accepted the unparseable %q", value)
		}
	}
}

// captureExporter keeps the last payload a Store.Export sent.
type captureExporter struct{ md pmetric.Metrics }

func (c *captureExporter) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	c.md = md
	return nil
}

// Both trace-tier families render through the ONE ScopeMetrics-creating path in
// cumagg.Store.Render, and that path stamped a scope NAME and no VERSION — so
// traces.span.metrics.* and traces_service_graph_* were the only kubescrape
// payloads shipping otel_scope_version="", while METRICS.md promised the
// version universally. A query grouping the trace tier's RED or edge metrics by
// otel_scope_version, or inventorying producers with otel_scope_version!="",
// matched nothing for them on every release.
//
// Asserted from outside both packages for the same reason the staleAfter drift
// is: the property is that the two agree, and each package can only see its own
// half of a shared render path.
func TestBothTraceTierFamiliesCarryAScopeVersion(t *testing.T) {
	sg := servicegraph.NewRegistry(servicegraph.Config{})
	sg.Record(servicegraph.Edge{
		ClientService: "frontend", ServerService: "checkout",
		ClientSeconds: 0.15, ServerSeconds: 0.05,
		HaveClient: true, HaveServer: true,
	})

	sm := spanmetrics.New(spanmetrics.Config{})
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "frontend")
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("GET /x")
	span.SetKind(ptrace.SpanKindServer)
	sm.Consume(td)

	for _, c := range []struct {
		what   string
		export func(context.Context, cumagg.Exporter, pcommon.Resource) error
	}{
		{"servicegraph", sg.Export},
		{"spanmetrics", sm.Export},
	} {
		exp := &captureExporter{}
		if err := c.export(context.Background(), exp, pcommon.NewResource()); err != nil {
			t.Fatalf("%s export: %v", c.what, err)
		}
		if exp.md.ResourceMetrics().Len() == 0 {
			t.Fatalf("%s exported nothing; the test cannot see the scope", c.what)
		}
		scope := exp.md.ResourceMetrics().At(0).ScopeMetrics().At(0).Scope()
		if scope.Name() == "" {
			t.Errorf("%s scope has no name", c.what)
		}
		if scope.Version() != obs.ScopeVersion {
			t.Errorf("%s scope version = %q, want the build version %q — a versionless scope "+
				"drops this family out of every otel_scope_version query",
				c.what, scope.Version(), obs.ScopeVersion)
		}
	}
}
