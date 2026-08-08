package promscrape

import (
	"context"
	"maps"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// textConvert is protoConvert's text-front twin: the same session, the same
// batcher, the other parser — so a shape can be fed to both fronts and their
// verdicts compared.
func textConvert(t *testing.T, body string) map[string]pmetric.Metric {
	t.Helper()
	exp := &captureExporter{}
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets:  staticTargets{},
		Exporter: exp, StartTime: time.Now(),
	})
	cb := newBatcher(func(pcommon.Resource) {}, time.Now(), time.Now())
	if _, err := s.parseAndExport(context.Background(), strings.NewReader(body), false, false, cb, pipelineTargets, "t"); err != nil {
		t.Fatal(err)
	}
	out := map[string]pmetric.Metric{}
	for _, md := range exp.batches {
		rms := md.ResourceMetrics()
		for i := range rms.Len() {
			ms := rms.At(i).ScopeMetrics().At(0).Metrics()
			for j := range ms.Len() {
				out[ms.At(j).Name()] = ms.At(j)
			}
		}
	}
	return out
}

// A duplicate label name is the one malformed shape the protobuf wire format
// admits and the text grammar cannot express, so the two fronts must still
// agree on it: the text parser fails the line, and the protobuf front rejected
// only EMPTY names, which let the pair through. Both readings of such a metric
// disagree with what ships — labelValue takes the FIRST pair, pcommon.Map
// upserts so the LAST one is exported — so a drop rule matches a value the
// export does not carry, and a bucket row's own `le` shadows the synthesized
// one until every bucket collapses into a single accumulator with malformed=0.
func TestProtoRejectsDuplicateLabelNames(t *testing.T) {
	dup := &dto.MetricFamily{
		Name: ptr("dup_gauge"),
		Type: dto.MetricType_GAUGE.Enum(),
		Metric: []*dto.Metric{{
			Label: []*dto.LabelPair{
				{Name: ptr("a"), Value: ptr("keep")},
				{Name: ptr("a"), Value: ptr("drop_me")},
			},
			Gauge: &dto.Gauge{Value: ptr(1.0)},
		}},
	}
	good := &dto.MetricFamily{
		Name:   ptr("good_gauge"),
		Type:   dto.MetricType_GAUGE.Enum(),
		Metric: []*dto.Metric{{Gauge: &dto.Gauge{Value: ptr(3.0)}}},
	}
	got, malformed := protoConvert(t, dup, good)
	if _, ok := got["dup_gauge"]; ok {
		t.Error("a metric with a repeated label name was exported")
	}
	if _, ok := got["good_gauge"]; !ok {
		t.Errorf("the well-formed family was lost; got %v", slices.Sorted(maps.Keys(got)))
	}
	if malformed != 1 {
		t.Errorf("malformed = %d, want 1", malformed)
	}
	// The text front's verdict on the identical shape, so the two cannot drift.
	if text := textConvert(t, "dup_gauge{a=\"keep\",a=\"drop_me\"} 1\ngood_gauge 3\n"); len(text) != 1 {
		t.Errorf("text front exported %v, want only good_gauge", slices.Sorted(maps.Keys(text)))
	}
}

// The chunk-size estimate gates every flush, and the split and cadvisor
// batchers create one ScopeMetrics per described object — so a scope field the
// estimate does not charge for is thousands of uncounted bytes per scrape. The
// version is a 40-char VCS revision in a shipped build and 7 characters under
// `go test`, which is why the absolute chunk-limit guards cannot see it: the
// assertion has to be a delta.
func TestScopeVersionChargedToSizeEstimate(t *testing.T) {
	// The shipped stamp: obs.BuildVersion() is the raw 40-hex vcs.revision
	// whenever no -X supplies a Version, which is every build of this image.
	defer func(old string) { obs.ScopeVersion = old }(obs.ScopeVersion)
	obs.ScopeVersion = strings.Repeat("a", 40)
	bt := newBatcher(func(r pcommon.Resource) {
		r.Attributes().PutStr("k8s.node.name", "node-1")
	}, time.Unix(1, 0), time.Unix(2, 0))
	// A point-less batch: what is left is exactly the per-resource charge, so
	// the comparison isolates it from every other term of the estimate.
	est := bt.size()
	var m pmetric.ProtoMarshaler
	if enc := m.MetricsSize(bt.take()); est < enc {
		t.Fatalf("per-resource estimate %d < encoded %d: the split and cadvisor batchers charge this once per described object, so an under-charge is thousands of uncounted bytes per scrape", est, enc)
	}
}

// The `le`/`quantile` component labels are SYNTHESIZED after protoLabels has
// run, so a target carrying its own collides where the duplicate scan can no
// longer see it — and a histogram whose every bucket row reads the target's
// single `le` collapses into one bound with the wrong counts, silently.
func TestProtoRejectsSynthesizedLabelCollision(t *testing.T) {
	hist := &dto.MetricFamily{
		Name: ptr("rpc_seconds"),
		Type: dto.MetricType_HISTOGRAM.Enum(),
		Metric: []*dto.Metric{{
			Label: []*dto.LabelPair{{Name: ptr("le"), Value: ptr("0.1")}},
			Histogram: &dto.Histogram{
				SampleCount: ptr(uint64(11)), SampleSum: ptr(1.5),
				Bucket: []*dto.Bucket{
					{UpperBound: ptr(1.0), CumulativeCount: ptr(uint64(5))},
					{UpperBound: ptr(2.0), CumulativeCount: ptr(uint64(9))},
					{UpperBound: ptr(math.Inf(1)), CumulativeCount: ptr(uint64(11))},
				},
			},
		}},
	}
	summ := &dto.MetricFamily{
		Name: ptr("rpc_summary"),
		Type: dto.MetricType_SUMMARY.Enum(),
		Metric: []*dto.Metric{{
			Label: []*dto.LabelPair{{Name: ptr("quantile"), Value: ptr("0.5")}},
			Summary: &dto.Summary{
				SampleCount: ptr(uint64(2)), SampleSum: ptr(1.0),
				Quantile: []*dto.Quantile{{Quantile: ptr(0.99), Value: ptr(0.4)}},
			},
		}},
	}
	got, malformed := protoConvert(t, hist, summ)
	if len(got) != 0 {
		t.Errorf("exported %v, want nothing", slices.Sorted(maps.Keys(got)))
	}
	if malformed != 2 {
		t.Errorf("malformed = %d, want 2 (one per rejected metric)", malformed)
	}
}
