package otlpingest

// Mid-push byte-budget exhaustion for an id that ALREADY has a group: the
// budget refuses the new (scope, metric) shell a later metric's point needs,
// so that point folds into the stripped overflow resource while the group's
// existing shells keep taking points. admit() used to leave that refusal
// uncounted, behind a comment claiming the points "still land on their own
// enriched resource" — the degradation was real and invisible.

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

func TestMidPushByteBudgetOnGroupedIDCountsAndFoldsToOverflow(t *testing.T) {
	capped := obs.Ingested.WithLabelValues("split_capped").Value()
	meta := &fakeMeta{pods: map[string]*kubemeta.Pod{
		"ksm-uid": {Name: "ksm-0", Namespace: "monitoring", UID: "ksm-uid", NodeName: "node1"},
		"uid-1":   {Name: "app-1", Namespace: "apps", UID: "uid-1", NodeName: "node1"},
	}}
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	a := rm.Resource().Attributes()
	a.PutStr("k8s.pod.uid", "ksm-uid")
	a.PutStr("service.name", "kube-state-metrics")
	// One resource copy exceeds the whole byte budget on its own, so the budget
	// binds immediately after the FIRST group is admitted and charged. (pcommon
	// CopyTo shares string bytes, so the big value costs headers, not copies.)
	a.PutStr("pad", strings.Repeat("x", maxSplitCopyBytes+1))
	sm := rm.ScopeMetrics().AppendEmpty()
	first := sm.Metrics().AppendEmpty()
	first.SetName("kube_pod_info")
	dps := first.SetEmptyGauge().DataPoints()
	for i := 0; i < 2; i++ {
		dp := dps.AppendEmpty()
		dp.SetIntValue(1)
		dp.Attributes().PutStr("k8s.pod.uid", "uid-1")
	}
	// A SECOND metric for the SAME described object, routed after the budget
	// has bound: its point needs a new metric shell in uid-1's group, which the
	// byte budget refuses.
	second := sm.Metrics().AppendEmpty()
	second.SetName("kube_pod_status_ready")
	dp := second.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetIntValue(1)
	dp.Attributes().PutStr("k8s.pod.uid", "uid-1")

	out := NewEnricher(Config{Meta: meta, MetricsMode: MetricsDatapoint}).
		EnrichMetrics(context.Background(), md)

	if got := out.DataPointCount(); got != 3 {
		t.Fatalf("forwarded %d points, want all 3 (a refused shell folds, it must not drop)", got)
	}

	var groupPoints, overflowPoints int
	rms := out.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		orm := rms.At(i)
		ra := orm.Resource().Attributes()
		enriched := false
		if v, ok := ra.Get("k8s.pod.name"); ok && v.Str() == "app-1" {
			enriched = true
		}
		sms := orm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				n := m.Gauge().DataPoints().Len()
				switch m.Name() {
				case "kube_pod_info":
					// The group's EXISTING shell keeps taking points past the
					// bind: both land on the enriched resource.
					if !enriched {
						t.Errorf("kube_pod_info points landed on an unenriched resource; the existing group must keep taking them")
					}
					groupPoints += n
				case "kube_pod_status_ready":
					// The refused shell's point folds into the stripped
					// overflow fallback — unenriched, sender identity gone, its
					// own id kept for downstream re-resolution.
					if enriched {
						t.Errorf("kube_pod_status_ready landed on the described object's group; the byte budget should have refused its shell")
					}
					if v, ok := ra.Get("service.name"); ok {
						t.Errorf("the overflow resource carries the sender's service.name=%q; it must be stripped", v.Str())
					}
					if _, ok := m.Gauge().DataPoints().At(0).Attributes().Get("k8s.pod.uid"); !ok {
						t.Error("the folded point lost its own id attribute")
					}
					overflowPoints += n
				}
			}
		}
	}
	if groupPoints != 2 || overflowPoints != 1 {
		t.Fatalf("placement: %d points on the enriched group (want 2), %d on the overflow fallback (want 1)", groupPoints, overflowPoints)
	}
	// The degradation is COUNTED: uid-1 tallies split_capped exactly once even
	// though it has its own group — its series forked onto the overflow
	// resource, which is what the counter's outcome reports.
	if got := obs.Ingested.WithLabelValues("split_capped").Value() - capped; got != 1 {
		t.Fatalf("split_capped delta = %v, want 1 (the grouped id's refused shell must be counted once)", got)
	}
}
