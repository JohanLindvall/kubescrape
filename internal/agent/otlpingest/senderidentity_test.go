package otlpingest

import (
	"context"
	"slices"
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// senderIdentityAttrs must never be NARROWER than what the attrs builder can
// emit. stripSenderIdentity runs before overwriteAttrs, whose keys come from
// the builder — so the strip matters exactly for keys the described object
// LACKS, and any builder-emittable key missing from the list survives from the
// EXPORTER onto the object it describes. The list is derived from
// attrs.IdentityKeys() precisely so it cannot drift again (a hand-copied list
// already had: attrs.Service's k8s.service.name/uid were absent, so a sender's
// service identity leaked onto every split-described object); this asserts the
// derivation stays a superset through the exported surface.
func TestSenderIdentityAttrsCoverEveryBuilderIdentityKey(t *testing.T) {
	for _, k := range attrs.IdentityKeys() {
		if !slices.Contains(senderIdentityAttrs, k) {
			t.Errorf("attrs.IdentityKeys lists %q but senderIdentityAttrs lacks it: a sender's %q would survive stripSenderIdentity onto every split-described object", k, k)
		}
	}
}

// The leak the drift caused, as behavior: a sender's k8s.service.name/uid must
// not survive onto a split-described object's resource, while non-identity
// sender attributes (a cluster name, custom keys) must.
func TestSplitStripsSenderServiceIdentity(t *testing.T) {
	meta := &fakeMeta{pods: map[string]*kubemeta.Pod{
		"ksm-uid": {Name: "ksm-0", Namespace: "monitoring", UID: "ksm-uid", NodeName: "node1"},
		"uid-1":   {Name: "app-1", Namespace: "apps", UID: "uid-1", NodeName: "node1"},
	}}
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	a := rm.Resource().Attributes()
	a.PutStr("k8s.pod.uid", "ksm-uid")
	a.PutStr("service.name", "kube-state-metrics")
	// The sender was discovered through a Service: attrs.Service stamps these
	// on scrape resources, and a collector-relayed payload can carry them.
	a.PutStr("k8s.service.name", "ksm-service")
	a.PutStr("k8s.service.uid", "ksm-service-uid")
	a.PutStr("cluster.name", "prod-eu") // non-identity: the sender's to keep
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("kube_pod_info")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetIntValue(1)
	dp.Attributes().PutStr("k8s.pod.uid", "uid-1")

	out := NewEnricher(Config{Meta: meta, MetricsMode: MetricsDatapoint}).
		EnrichMetrics(context.Background(), md)

	found := false
	rms := out.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		ra := rms.At(i).Resource().Attributes()
		v, ok := ra.Get("k8s.pod.name")
		if !ok || v.Str() != "app-1" {
			continue
		}
		found = true
		for _, k := range []string{"k8s.service.name", "k8s.service.uid"} {
			if got, ok := ra.Get(k); ok {
				t.Errorf("the described object's resource carries the SENDER's %s=%q; it must be stripped (the described pod has no such key for the overwrite to replace)", k, got.Str())
			}
		}
		if got, ok := ra.Get("cluster.name"); !ok || got.Str() != "prod-eu" {
			t.Errorf("the sender's non-identity cluster.name did not survive onto the split group (got %q, present=%v)", got.Str(), ok)
		}
	}
	if !found {
		t.Fatal("no split group for the described pod app-1 was produced")
	}
}
