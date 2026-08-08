package otlpingest

// Regression tests for what happens to a described object the split budgets
// REFUSE. maxSplitCopyBytes/maxSplitGroups is 8 KiB of resource estimate, so an
// attribute-rich exporter reaches the byte budget long before the group cap —
// the overflow path is the ordinary case for exactly the KSM-shaped senders
// split mode exists for, not a corner.

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// ksmPush builds a describing-exporter payload: one fat sender resource naming
// ITSELF, and pods data points naming distinct OTHER pods, all resolvable.
func ksmPush(t *testing.T, pods, padAttrs int) (pmetric.Metrics, *fakeMeta) {
	t.Helper()
	meta := &fakeMeta{pods: map[string]*kubemeta.Pod{
		"ksm-uid": {Name: "ksm-0", Namespace: "monitoring", UID: "ksm-uid", NodeName: "node1"},
	}}
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	a := rm.Resource().Attributes()
	a.PutStr("k8s.pod.uid", "ksm-uid")
	a.PutStr("k8s.pod.name", "ksm-0")
	a.PutStr("service.name", "kube-state-metrics")
	pad := strings.Repeat("x", 2<<10)
	for i := 0; i < padAttrs; i++ { // the sender's own resource, over the 8 KiB seam
		a.PutStr(fmt.Sprintf("sender.attr.%02d", i), pad)
	}
	g := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	g.SetName("kube_pod_info")
	dps := g.SetEmptyGauge().DataPoints()
	for i := 0; i < pods; i++ {
		uid := fmt.Sprintf("uid-%d", i)
		meta.pods[uid] = &kubemeta.Pod{Name: "app-" + uid, Namespace: "apps", UID: uid, NodeName: "node1"}
		dp := dps.AppendEmpty()
		dp.SetIntValue(1)
		dp.Attributes().PutStr("k8s.pod.uid", uid)
	}
	return md, meta
}

// A byte-refused object's points must not export under the SENDER's identity.
// The refusal used to fold them into the id-less "" chain, which skips the
// strip-and-overwrite the admitted groups get — so the exporter's
// k8s.pod.name/service.name labelled hundreds of other pods' series, which in
// Prometheus terms is those pods' series under the exporter's job and instance.
func TestByteRefusedGroupDoesNotCarrySenderIdentity(t *testing.T) {
	capped := obs.Ingested.WithLabelValues("split_capped").Value()
	const pods = 400
	md, meta := ksmPush(t, pods, 42) // ~84 KiB of sender resource attributes

	out := NewEnricher(Config{Meta: meta, MetricsMode: MetricsDatapoint}).
		EnrichMetrics(context.Background(), md)

	if got := out.DataPointCount(); got != pods {
		t.Fatalf("forwarded %d points, want all %d", got, pods)
	}
	if obs.Ingested.WithLabelValues("split_capped").Value() == capped {
		t.Fatal("nothing was refused: the payload no longer exercises the byte budget")
	}
	mislabelled := 0
	rms := out.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		v, ok := rm.Resource().Attributes().Get("service.name")
		if !ok || v.Str() != "kube-state-metrics" {
			continue
		}
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				mislabelled += ms.At(k).Gauge().DataPoints().Len()
			}
		}
	}
	if mislabelled != 0 {
		t.Errorf("%d described pods' points carry the exporter's service.name; the sender's identity must be stripped from a refused group", mislabelled)
	}
	// The id survives on the points, so a downstream consumer can still resolve
	// what this push could not afford to.
	last := rms.At(rms.Len() - 1)
	dp := last.ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0)
	if _, ok := dp.Attributes().Get("k8s.pod.uid"); !ok {
		t.Error("a refused point lost its own id attribute")
	}
}

// The other half of the same rule: a refused id that is the SENDER's own keeps
// the sender's resource. Those points describe the exporter, so its identity is
// theirs, and folding them into the stripped overflow group would lose what the
// push got right.
func TestByteRefusedSenderIDKeepsItsOwnIdentity(t *testing.T) {
	md, meta := ksmPush(t, 400, 42)
	// A second metric, routed after the budget has bound, whose points name the
	// SENDER — the id its own resource carries.
	sm := md.ResourceMetrics().At(0).ScopeMetrics().At(0)
	own := sm.Metrics().AppendEmpty()
	own.SetName("kube_state_metrics_build_info")
	for i := 0; i < 4; i++ {
		dp := own.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetIntValue(1)
		dp.Attributes().PutStr("k8s.pod.uid", "ksm-uid")
	}

	out := NewEnricher(Config{Meta: meta, MetricsMode: MetricsDatapoint}).
		EnrichMetrics(context.Background(), md)

	found := false
	rms := out.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		ms := rm.ScopeMetrics().At(0).Metrics()
		for j := 0; j < ms.Len(); j++ {
			if ms.At(j).Name() != "kube_state_metrics_build_info" {
				continue
			}
			found = true
			if v, ok := rm.Resource().Attributes().Get("service.name"); !ok || v.Str() != "kube-state-metrics" {
				t.Errorf("the sender's own points lost its service.name (got %q, present=%v)", v.Str(), ok)
			}
		}
	}
	if !found {
		t.Fatal("the sender's own metric was not forwarded")
	}
}

// The peer-IP outcome accounting belongs to a push that named NOTHING. A
// budget-refused id folding into the id-less chain made a fully-attributed
// exporter tally unresolved once per input ResourceMetrics — noise on the one
// counter that says whether ingest attribution works at all.
func TestByteRefusedGroupDoesNotCountUnresolved(t *testing.T) {
	unresolved := obs.Ingested.WithLabelValues("unresolved").Value()
	md, meta := ksmPush(t, 400, 42)

	NewEnricher(Config{Meta: meta, MetricsMode: MetricsDatapoint}).
		EnrichMetrics(context.Background(), md)

	if got := obs.Ingested.WithLabelValues("unresolved").Value() - unresolved; got != 0 {
		t.Fatalf("unresolved delta = %v, want 0: every id in this push resolved", got)
	}
}

// Once the byte budget binds, admit also refuses further scope/shell copies
// for ids that ALREADY have their own enriched group — and those refusals ARE
// counted, once per object. The refused points do not stay on the group: they
// fold into the stripped overflow resource, so the object's series fork across
// its enriched resource and an unenriched one — a real degradation of an
// object the push named, which is what split_capped reports. (An earlier
// version left the grouped case uncounted on the theory that the object "was
// in fact enriched"; its points' actual placement made that claim false and
// the mid-push bind invisible.)
func TestByteRefusedShellOfAnEnrichedGroupIsCounted(t *testing.T) {
	capped := obs.Ingested.WithLabelValues("split_capped").Value()

	const objects = 8
	meta := &fakeMeta{pods: map[string]*kubemeta.Pod{}}
	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	for _, desc := range []string{"", strings.Repeat("d", 4<<20)} {
		// Metric 1 admits every object cheaply; metric 2's fat descriptor drives
		// splitCopied past the budget partway through the SAME ids.
		g := sm.Metrics().AppendEmpty()
		g.SetName(fmt.Sprintf("m%d", len(desc)))
		g.SetDescription(desc)
		dps := g.SetEmptyGauge().DataPoints()
		for i := 0; i < objects; i++ {
			uid := fmt.Sprintf("uid-%d", i)
			meta.pods[uid] = &kubemeta.Pod{Name: "app-" + uid, Namespace: "apps", UID: uid, NodeName: "node1"}
			dp := dps.AppendEmpty()
			dp.SetIntValue(1)
			dp.Attributes().PutStr("k8s.pod.uid", uid)
		}
	}

	out := NewEnricher(Config{Meta: meta, MetricsMode: MetricsDatapoint}).
		EnrichMetrics(context.Background(), md)

	if got := out.DataPointCount(); got != 2*objects {
		t.Fatalf("forwarded %d points, want all %d", got, 2*objects)
	}
	// Every object got its own group from the first metric; the fat metric's
	// points split between shells admitted before the budget bound (on their
	// enriched groups) and the stripped overflow fallback after it.
	enriched := 0
	forked := map[string]struct{}{} // uids whose fat-metric points landed on overflow
	rms := out.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		_, isEnriched := rm.Resource().Attributes().Get("k8s.pod.name")
		if isEnriched {
			enriched++
		}
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				if isEnriched || len(m.Description()) == 0 {
					continue
				}
				dps := m.Gauge().DataPoints()
				for l := 0; l < dps.Len(); l++ {
					if v, ok := dps.At(l).Attributes().Get("k8s.pod.uid"); ok {
						forked[v.Str()] = struct{}{}
					}
				}
			}
		}
	}
	if enriched != objects {
		t.Fatalf("enriched groups = %d, want %d: the payload no longer admits every object first", enriched, objects)
	}
	if len(forked) == 0 {
		t.Fatal("no fat-metric point landed on the overflow fallback: the payload no longer exercises the mid-push byte bind")
	}
	if got := obs.Ingested.WithLabelValues("split_capped").Value() - capped; got != float64(len(forked)) {
		t.Errorf("split_capped delta = %v, want %d: each grouped object whose refused shell folded to overflow must count exactly once", got, len(forked))
	}
}
