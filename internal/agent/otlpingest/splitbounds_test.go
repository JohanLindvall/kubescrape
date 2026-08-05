package otlpingest

// Regression tests for the split path's OUTPUT bounds. The ingest byte budget
// bounds what a push READS and maxSplitGroups bounds how many groups it may
// mint — but the groups are full copies of the sender's resource (and every
// routed shell a copy of its metric's descriptor), so the group cap had to be
// per PAYLOAD and the minted bytes needed a budget of their own
// (maxSplitCopyBytes). Both degrade into the "" fallback: forwarded,
// unenriched, counted split_capped — never refused, never an OOM.

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// The split-group cap bounds one PUSH, not one input ResourceMetrics: with the
// cap checked against a per-grouper map, 3 input resources of ~780 distinct
// pod uids each minted ~2350 group resources — the payload's own structure
// chose the bound, and a 16 MiB body fits enough minimal ResourceMetrics for
// ~390k groups.
func TestSplitGroupCapIsPerPayload(t *testing.T) {
	capped := obs.Ingested.WithLabelValues("split_capped").Value()

	const rmCount = 3
	perRM := maxSplitGroups/rmCount + 100 // over the cap in aggregate, under it per input resource
	md := pmetric.NewMetrics()
	for r := 0; r < rmCount; r++ {
		dps := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().
			Metrics().AppendEmpty().SetEmptyGauge().DataPoints()
		for i := 0; i < perRM; i++ {
			dp := dps.AppendEmpty()
			dp.SetIntValue(1)
			dp.Attributes().PutStr("k8s.pod.uid", fmt.Sprintf("u-%d-%d", r, i))
		}
	}

	out := NewEnricher(Config{Meta: &fakeMeta{}, MetricsMode: MetricsDatapoint}).
		EnrichMetrics(context.Background(), md)

	if got := out.DataPointCount(); got != rmCount*perRM {
		t.Fatalf("forwarded %d points, want all %d (capped remainder folded, none dropped)", got, rmCount*perRM)
	}
	// The documented bound: the cap plus one "" fallback per input resource.
	if got := out.ResourceMetrics().Len(); got > maxSplitGroups+rmCount {
		t.Fatalf("output resources = %d, want <= %d: the group cap is per payload", got, maxSplitGroups+rmCount)
	}
	if got, want := obs.Ingested.WithLabelValues("split_capped").Value()-capped, float64(rmCount*perRM-maxSplitGroups); got != want {
		t.Errorf("split_capped delta = %v, want %v (one per refused id)", got, want)
	}
}

// The splitter's output BYTES are bounded. Every admitted group is a full copy
// of the sender's resource, so a 282 KB push — 200 KiB of resource attributes
// plus 2048 resolvable pod uids — serialized to 421 MB (1493x): one ~421 MB
// marshal per push at the disk buffer's enqueue, ~112 otlpsplit parts without
// one, from the unauthenticated listener. Past maxSplitCopyBytes the remaining
// objects share the source resource unenriched, like the group cap's overflow.
func TestSplitOutputBytesAreBounded(t *testing.T) {
	capped := obs.Ingested.WithLabelValues("split_capped").Value()

	const uids = 2048
	meta := &fakeMeta{pods: map[string]*kubemeta.Pod{}}
	for i := 0; i < uids; i++ {
		uid := fmt.Sprintf("uid-%d", i)
		meta.pods[uid] = &kubemeta.Pod{Name: "pod-" + uid, Namespace: "apps", UID: uid, NodeName: "node1"}
	}

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	pad := strings.Repeat("x", 4<<10)
	for i := 0; i < 50; i++ { // ~200 KiB of sender resource attributes
		rm.Resource().Attributes().PutStr(fmt.Sprintf("bulk.attr.%02d", i), pad)
	}
	g := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	g.SetName("kube_pod_info")
	dps := g.SetEmptyGauge().DataPoints()
	for i := 0; i < uids; i++ {
		dp := dps.AppendEmpty()
		dp.SetIntValue(1)
		dp.Attributes().PutStr("k8s.pod.uid", fmt.Sprintf("uid-%d", i))
	}

	var m pmetric.ProtoMarshaler
	in := m.MetricsSize(md)

	out := NewEnricher(Config{Meta: meta, MetricsMode: MetricsDatapoint}).
		EnrichMetrics(context.Background(), md)

	if got := out.DataPointCount(); got != uids {
		t.Fatalf("forwarded %d points, want all %d", got, uids)
	}
	// Copy budget plus estimate slack (the estimate charges string payloads,
	// not exact proto framing, and the last admitted group may overshoot by one
	// resource copy).
	bound := maxSplitCopyBytes + maxSplitCopyBytes/2
	if got := m.MetricsSize(out); got > bound {
		t.Fatalf("output serializes to %d bytes for a %d-byte input, want <= %d: the split output is unbounded", got, in, bound)
	}
	if obs.Ingested.WithLabelValues("split_capped").Value() == capped {
		t.Fatal("split_capped did not move: the byte-budget degradation must be counted")
	}
	// Degraded, not disabled: groups admitted before the budget bound are
	// still split out and enriched.
	enriched := 0
	rms := out.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		if _, ok := rms.At(i).Resource().Attributes().Get("k8s.pod.name"); ok {
			enriched++
		}
	}
	if enriched == 0 {
		t.Fatal("no group was enriched: the budget must degrade, not disable")
	}
}

// Descriptor copies are charged too: every point routed to a new shell repeats
// its metric's description, so 2048 cheaply-admitted groups times a 256 KiB
// description was the resource amplification through a different copy — and a
// group admitted under the byte budget must stop minting shells once it binds.
func TestSplitDescriptorCopiesAreBounded(t *testing.T) {
	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	desc := strings.Repeat("d", 256<<10)
	const metrics = 4
	for mi := 0; mi < metrics; mi++ {
		g := sm.Metrics().AppendEmpty()
		g.SetName(fmt.Sprintf("m%d", mi))
		g.SetDescription(desc)
		dps := g.SetEmptyGauge().DataPoints()
		for i := 0; i < maxSplitGroups; i++ {
			dp := dps.AppendEmpty()
			dp.SetIntValue(1)
			dp.Attributes().PutStr("k8s.pod.uid", fmt.Sprintf("uid-%d", i))
		}
	}

	out := NewEnricher(Config{Meta: &fakeMeta{}, MetricsMode: MetricsDatapoint}).
		EnrichMetrics(context.Background(), md)

	if got, want := out.DataPointCount(), metrics*maxSplitGroups; got != want {
		t.Fatalf("forwarded %d points, want all %d", got, want)
	}
	var m pmetric.ProtoMarshaler
	bound := maxSplitCopyBytes + maxSplitCopyBytes/2
	if got := m.MetricsSize(out); got > bound {
		t.Fatalf("output serializes to %d bytes, want <= %d: descriptor copies are not budgeted", got, bound)
	}
}
