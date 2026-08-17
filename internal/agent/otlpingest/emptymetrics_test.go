package otlpingest

import (
	"bytes"
	"context"
	"net/http"
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/pdatacheck"
)

// emptyOfEveryKind builds a payload holding one legitimate metric beside an
// empty one of every metric type the wire can carry, INCLUDING an untyped one
// (a name and nothing else, which is what a sender sends when it creates a
// descriptor and never records into it).
func emptyOfEveryKind() pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "sender")
	sm := rm.ScopeMetrics().AppendEmpty()

	good := sm.Metrics().AppendEmpty()
	good.SetName("good")
	good.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(1)

	sm.Metrics().AppendEmpty().SetName("empty_untyped")
	m := sm.Metrics().AppendEmpty()
	m.SetName("empty_gauge")
	m.SetEmptyGauge()
	m = sm.Metrics().AppendEmpty()
	m.SetName("empty_sum")
	m.SetEmptySum()
	m = sm.Metrics().AppendEmpty()
	m.SetName("empty_histogram")
	m.SetEmptyHistogram()
	m = sm.Metrics().AppendEmpty()
	m.SetName("empty_exp_histogram")
	m.SetEmptyExponentialHistogram()
	m = sm.Metrics().AppendEmpty()
	m.SetName("empty_summary")
	m.SetEmptySummary()
	return md
}

// A metric with no data points is legal OTLP and nothing downstream rejects
// one, so it would ride every hop to the collector and deliver no
// measurement. It must not leave the receipt seam — on EITHER transport, for
// every metric type, while the payload's real metric is forwarded untouched.
func TestPushedEmptyMetricsAreDroppedAtReceipt(t *testing.T) {
	for _, tc := range []struct {
		name string
		push func(t *testing.T, exp *captureExporter, md pmetric.Metrics)
	}{
		{"grpc", func(t *testing.T, exp *captureExporter, md pmetric.Metrics) {
			s := NewServer(ServerConfig{Enricher: newEnricher(newMeta(), MetricsAuto), Exporter: exp})
			g := &metricsGRPC{s: s}
			if _, err := g.Export(context.Background(), pmetricotlp.NewExportRequestFromMetrics(md)); err != nil {
				t.Fatal(err)
			}
		}},
		{"http", func(t *testing.T, exp *captureExporter, md pmetric.Metrics) {
			srv := httpTestServer(t, exp)
			body, err := pmetricotlp.NewExportRequestFromMetrics(md).MarshalProto()
			if err != nil {
				t.Fatal(err)
			}
			resp, err := http.Post(srv.URL+"/v1/metrics", "application/x-protobuf", bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exp := &captureExporter{}
			tc.push(t, exp, emptyOfEveryKind())

			if len(exp.metrics) != 1 {
				t.Fatalf("exports = %d, want 1", len(exp.metrics))
			}
			out := exp.metrics[0]
			if bad := pdatacheck.EmptyMetrics(out); len(bad) > 0 {
				t.Errorf("empty metrics reached the collector: %v", bad)
			}
			// The real metric must survive the prune untouched.
			got := map[string]bool{}
			rms := out.ResourceMetrics()
			for i := 0; i < rms.Len(); i++ {
				sms := rms.At(i).ScopeMetrics()
				for j := 0; j < sms.Len(); j++ {
					ms := sms.At(j).Metrics()
					for k := 0; k < ms.Len(); k++ {
						got[ms.At(k).Name()] = true
					}
				}
			}
			if len(got) != 1 || !got["good"] {
				t.Errorf("forwarded metrics = %v, want exactly {good}", got)
			}
		})
	}
}

// A push holding nothing but empty metrics is acked without a send: forwarding
// an envelope with no measurement in it is the waste the prune exists to
// prevent, and refusing it would make a sender retry a payload that will never
// become deliverable.
func TestPushOfOnlyEmptyMetricsIsAckedWithoutASend(t *testing.T) {
	exp := &captureExporter{}
	s := NewServer(ServerConfig{Enricher: newEnricher(newMeta(), MetricsAuto), Exporter: exp})

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "sender")
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("empty")
	m.SetEmptyGauge()

	g := &metricsGRPC{s: s}
	if _, err := g.Export(context.Background(), pmetricotlp.NewExportRequestFromMetrics(md)); err != nil {
		t.Fatalf("the push must be ACKED, not refused: %v", err)
	}
	if len(exp.metrics) != 0 {
		t.Fatalf("exports = %d, want 0 (nothing to send)", len(exp.metrics))
	}
}

// Emptied scopes and resources go with the metrics that were under them, and
// so do ones that arrived that way: an envelope carrying no metric is the same
// zero-information framing.
func TestEmptiedAndEmptyEnvelopesArePruned(t *testing.T) {
	md := pmetric.NewMetrics()
	// A resource whose only scope holds only an empty metric.
	rm := md.ResourceMetrics().AppendEmpty()
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("empty")
	m.SetEmptyGauge()
	// A resource with a scope that carries no metrics at all.
	md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	// A resource with no scopes at all.
	md.ResourceMetrics().AppendEmpty()
	// One that must survive whole.
	keep := md.ResourceMetrics().AppendEmpty()
	good := keep.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	good.SetName("good")
	good.SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(3)

	if n := dropEmptyMetrics(md); n != 1 {
		t.Errorf("dropped = %d, want 1 (only the metric counts; envelopes are a consequence)", n)
	}
	if got := md.ResourceMetrics().Len(); got != 1 {
		t.Fatalf("resources left = %d, want 1", got)
	}
	if got := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Name(); got != "good" {
		t.Errorf("survivor = %q, want \"good\"", got)
	}
}

// The prune is COUNTED. An empty metric is invisible by nature — nothing
// downstream rejects it and no other counter moves — so this series is the
// only report a sender's instrumentation bug ever gets.
func TestEmptyMetricPruneIsCounted(t *testing.T) {
	before := obs.IngestEmptyMetricsDropped.Value()

	exp := &captureExporter{}
	s := NewServer(ServerConfig{Enricher: newEnricher(newMeta(), MetricsAuto), Exporter: exp})
	g := &metricsGRPC{s: s}
	if _, err := g.Export(context.Background(), pmetricotlp.NewExportRequestFromMetrics(emptyOfEveryKind())); err != nil {
		t.Fatal(err)
	}

	// six empties: untyped, gauge, sum, histogram, exponential histogram, summary
	if got := obs.IngestEmptyMetricsDropped.Value() - before; got != 6 {
		t.Errorf("counted %v empty metrics, want 6", got)
	}
}

// The prune runs BEFORE enrichment, so the datapoint-mode splitter never sees
// an empty metric. That matters because the splitter deliberately PRESERVES
// one (metricGrouper.route keeps a point-less descriptor under the
// resource-level id rather than dropping it), which is the right call for a
// regrouper and the wrong outcome for the wire.
func TestSplitterNeverSeesAnEmptyMetric(t *testing.T) {
	exp := &captureExporter{}
	s := NewServer(ServerConfig{
		Enricher: newEnricher(newMeta(), MetricsDatapoint),
		Exporter: exp,
	})

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "sender")
	sm := rm.ScopeMetrics().AppendEmpty()
	good := sm.Metrics().AppendEmpty()
	good.SetName("good")
	dp := good.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(1)
	dp.Attributes().PutStr("container.id", "cafe01")
	m := sm.Metrics().AppendEmpty()
	m.SetName("empty")
	m.SetEmptyGauge()

	g := &metricsGRPC{s: s}
	if _, err := g.Export(context.Background(), pmetricotlp.NewExportRequestFromMetrics(md)); err != nil {
		t.Fatal(err)
	}
	if len(exp.metrics) != 1 {
		t.Fatalf("exports = %d, want 1", len(exp.metrics))
	}
	if bad := pdatacheck.EmptyMetrics(exp.metrics[0]); len(bad) > 0 {
		t.Errorf("the splitter carried empty metrics through: %v", bad)
	}
}
