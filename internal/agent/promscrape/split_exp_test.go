// The splitter's exponential-histogram path: native points route through the
// same groupBy/dropLabels machinery as every other kind, so splitter-backed
// targets can accept the protobuf exposition.
package promscrape

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestSplitBatcherExponential(t *testing.T) {
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: time.Second,
		Targets: staticTargets{}, Exporter: &captureExporter{},
		Kubelet: KubeletConfig{Meta: &fakeMetaSource{}}, StartTime: time.Unix(1, 0),
	})
	sp, err := NewSplitters([]SplitterConfig{{
		Match: SplitterMatch{PodName: "ksm-.+"},
		Rules: []SplitRule{{Metrics: `kube_pod_.+`, GroupBy: map[string]string{
			"namespace": "k8s.namespace.name", "pod": "k8s.pod.name",
		}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	target := testTarget("http://ksm:8080/metrics")
	target.Pod.Name = "ksm-abc"
	cb := newSplitBatcher(context.Background(), s, target, sp[0], time.Unix(2, 0))

	cb.addExponential("kube_pod_latency", expPoint{
		labels: []Label{{Name: "namespace", Value: "ns1"}, {Name: "pod", Value: "p1"}, {Name: "code", Value: "200"}},
		schema: 2, count: 7, sum: 1.5, hasSum: true,
		pos: []uint64{1, 2, 4}, posOffset: -1, ts: 0,
	})
	if cb.count() != 1 || cb.size() == 0 {
		t.Fatalf("points/bytes accounting: %d/%d", cb.count(), cb.size())
	}
	md := cb.take()

	var found pmetric.ExponentialHistogramDataPoint
	var res pmetric.ResourceMetrics
	ok := false
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		ms := rm.ScopeMetrics().At(0).Metrics()
		for j := 0; j < ms.Len(); j++ {
			if m := ms.At(j); m.Name() == "kube_pod_latency" {
				if m.Type() != pmetric.MetricTypeExponentialHistogram {
					t.Fatalf("type = %v", m.Type())
				}
				found, res, ok = m.ExponentialHistogram().DataPoints().At(0), rm, true
			}
		}
	}
	if !ok {
		t.Fatal("exponential metric not emitted")
	}
	// groupBy labels moved onto the split resource; the rest stay on the point.
	if v, k := res.Resource().Attributes().Get("k8s.pod.name"); !k || v.Str() != "p1" {
		t.Fatalf("split resource lacks k8s.pod.name=p1")
	}
	if _, k := found.Attributes().Get("namespace"); k {
		t.Fatal("grouped label leaked onto the data point")
	}
	if v, k := found.Attributes().Get("code"); !k || v.Str() != "200" {
		t.Fatal("non-grouped label missing from the data point")
	}
	if found.Scale() != 2 || found.Count() != 7 || found.Sum() != 1.5 ||
		found.Positive().Offset() != -1 || found.Positive().BucketCounts().Len() != 3 {
		t.Fatalf("point = scale %d count %d sum %v", found.Scale(), found.Count(), found.Sum())
	}
}

// The protobuf path reaches the batcher through an expSink type assertion
// (addNativeHistogram), so the interface must actually be satisfied — a
// direct-call test would still pass if the assertion silently failed and
// counted every native family malformed.
var _ expSink = (*splitBatcher)(nil)
var _ expSink = (*batcher)(nil)
