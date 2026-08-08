package servicegraph

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/cumagg"
	"github.com/JohanLindvall/kubescrape/internal/obs"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

// capExporter captures exported payloads (mutexed: the Run test reads it from
// the test goroutine while Run exports from its own).
type capExporter struct {
	mu  sync.Mutex
	md  []pmetric.Metrics
	err error // returned instead of accepting, when set
}

func (c *capExporter) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	cp := pmetric.NewMetrics()
	md.CopyTo(cp)
	c.md = append(c.md, cp)
	return nil
}

func (c *capExporter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.md)
}

// last returns the most recent payload's metrics by name.
func (c *capExporter) last(t *testing.T) map[string]pmetric.Metric {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.md) == 0 {
		t.Fatal("no payload exported")
	}
	out := map[string]pmetric.Metric{}
	md := c.md[len(c.md)-1]
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				if _, dup := out[m.Name()]; dup {
					t.Fatalf("metric %q appears twice in one payload", m.Name())
				}
				out[m.Name()] = m
			}
		}
	}
	return out
}

// attrsOf renders a data point's attributes as a plain map, so a test can
// compare the WHOLE label set rather than spot-checking the labels it expects.
func attrsOf(a pcommon.Map) map[string]string {
	out := map[string]string{}
	for k, v := range a.All() {
		out[k] = v.AsString()
	}
	return out
}

// export runs one export cycle and fails the test if it errors.
func export(t *testing.T, r *Registry, exp *capExporter) {
	t.Helper()
	if err := r.Export(context.Background(), exp, pcommon.NewResource()); err != nil {
		t.Fatalf("Export: %v", err)
	}
}

// fixedClock pins the registry's clock so staleness is a test decision, not a
// timing race (the store tests do the same for expiry).
func fixedClock(r *Registry, t *time.Time) { r.now = func() time.Time { return *t } }

func edge(client, server string) Edge {
	return Edge{
		ClientService: client, ServerService: server,
		ClientSeconds: 0.15, ServerSeconds: 0.05,
		HaveClient: true, HaveServer: true,
		// Every edge the processor produces carries these (they are what an
		// exemplar needs), so the default test edge does too.
		TraceID: traceID(1), ClientSpanID: spanID(1), ServerSpanID: spanID(2),
	}
}

// exemplarsOf renders a histogram point's exemplars as (value, trace, span)
// triples, in order.
func exemplarsOf(p pmetric.HistogramDataPoint) []cumagg.Exemplar {
	out := make([]cumagg.Exemplar, 0, p.Exemplars().Len())
	for i := 0; i < p.Exemplars().Len(); i++ {
		e := p.Exemplars().At(i)
		out = append(out, cumagg.Exemplar{Set: true, Value: e.DoubleValue(), TS: e.Timestamp(),
			TraceID: e.TraceID(), SpanID: e.SpanID()})
	}
	return out
}

// firstHist returns the single data point of the named duration histogram.
func firstHist(t *testing.T, got map[string]pmetric.Metric, name string) pmetric.HistogramDataPoint {
	t.Helper()
	m, ok := got[name]
	if !ok {
		t.Fatalf("no %s in the payload (have %v)", name, keysOf(got))
	}
	dps := m.Histogram().DataPoints()
	if dps.Len() != 1 {
		t.Fatalf("%s points = %d, want 1", name, dps.Len())
	}
	return dps.At(0)
}

// The four names and their label set are Grafana Tempo's, and Grafana's Service
// Graph view queries them as literal strings: this test asserts the strings
// themselves, because everything else in the package can be right while the
// feature renders in nothing.
//
// It also pins the OTLP shape the Prometheus translation depends on — monotonic
// cumulative Sums (so `_total` is recognised as already-present rather than a
// name that happens to end in it) and unit "s" on the histograms, whose
// `seconds` token the unit rule then finds and leaves alone.
func TestMetricNamesAndLabelsAreTempoVerbatim(t *testing.T) {
	r := NewRegistry(Config{})
	r.Record(edge("frontend", "checkout"))

	exp := &capExporter{}
	export(t, r, exp)
	got := exp.last(t)

	want := []string{
		"traces_service_graph_request_total",
		"traces_service_graph_request_failed_total",
		"traces_service_graph_request_server_seconds",
		"traces_service_graph_request_client_seconds",
	}
	for _, name := range want {
		if _, ok := got[name]; !ok {
			t.Errorf("metric %q missing; exported %v", name, keysOf(got))
		}
	}
	if len(got) != len(want) {
		t.Errorf("exported %v, want exactly %v", keysOf(got), want)
	}

	wantLabels := map[string]string{
		"client":          "frontend",
		"server":          "checkout",
		"connection_type": "",
	}
	for _, name := range want[:2] { // the two counters
		m := got[name]
		if m.Type() != pmetric.MetricTypeSum {
			t.Errorf("%s type = %v, want Sum (a non-Sum never gets the _total suffix rule applied)", name, m.Type())
		}
		if !m.Sum().IsMonotonic() {
			t.Errorf("%s is not monotonic; the Prometheus mapping only treats monotonic Sums as counters", name)
		}
		if temp := m.Sum().AggregationTemporality(); temp != pmetric.AggregationTemporalityCumulative {
			t.Errorf("%s temporality = %v, want Cumulative", name, temp)
		}
		if u := m.Unit(); u != "" {
			t.Errorf("%s unit = %q, want empty (a unit would append a suffix to a name that is already exact)", name, u)
		}
		dps := m.Sum().DataPoints()
		if dps.Len() != 1 {
			t.Fatalf("%s points = %d, want 1", name, dps.Len())
		}
		if l := attrsOf(dps.At(0).Attributes()); !maps.Equal(l, wantLabels) {
			t.Errorf("%s labels = %v, want %v", name, l, wantLabels)
		}
	}
	for _, name := range want[2:] { // the two histograms
		m := got[name]
		if m.Type() != pmetric.MetricTypeHistogram {
			t.Errorf("%s type = %v, want Histogram", name, m.Type())
		}
		if u := m.Unit(); u != "s" {
			t.Errorf("%s unit = %q, want \"s\" (the name already carries `seconds`; the unit is what an OTLP-native consumer reads)", name, u)
		}
		if temp := m.Histogram().AggregationTemporality(); temp != pmetric.AggregationTemporalityCumulative {
			t.Errorf("%s temporality = %v, want Cumulative", name, temp)
		}
		dps := m.Histogram().DataPoints()
		if dps.Len() != 1 {
			t.Fatalf("%s points = %d, want 1", name, dps.Len())
		}
		if l := attrsOf(dps.At(0).Attributes()); !maps.Equal(l, wantLabels) {
			t.Errorf("%s labels = %v, want %v", name, l, wantLabels)
		}
	}
}

// connection_type carries Tempo's spellings verbatim, INCLUDING the empty one:
// Grafana selects `connection_type="messaging_system"` to draw a queue between
// two nodes, and a renamed value silently draws nothing.
func TestConnectionTypeValuesAreVerbatim(t *testing.T) {
	for _, ct := range []ConnectionType{ConnectionUnknown, ConnectionMessagingSystem, ConnectionDatabase, ConnectionVirtualNode} {
		t.Run(fmt.Sprintf("%q", string(ct)), func(t *testing.T) {
			r := NewRegistry(Config{})
			e := edge("frontend", "checkout")
			e.Connection = ct
			r.Record(e)

			exp := &capExporter{}
			export(t, r, exp)
			dps := exp.last(t)["traces_service_graph_request_total"].Sum().DataPoints()
			if dps.Len() != 1 {
				t.Fatalf("points = %d, want 1", dps.Len())
			}
			got := attrsOf(dps.At(0).Attributes())["connection_type"]
			if got != string(ct) {
				t.Errorf("connection_type = %q, want %q", got, string(ct))
			}
		})
	}
	// The permitted set is Tempo's store.ConnectionType and nothing else.
	if want := []ConnectionType{"", "messaging_system", "database", "virtual_node"}; !equalCT(want,
		[]ConnectionType{ConnectionUnknown, ConnectionMessagingSystem, ConnectionDatabase, ConnectionVirtualNode}) {
		t.Errorf("connection type constants drifted from Tempo's values")
	}
}

// Extra dimensions arrive on the Edge already prefixed and are emitted as-is;
// the virtual-node marker rides as its own `virtual_node` label, and only when
// a side really was synthesized.
func TestDimensionAndVirtualNodeLabels(t *testing.T) {
	r := NewRegistry(Config{})
	e := edge("frontend", "orders-db")
	e.Connection = ConnectionDatabase
	e.HaveServer = false // an uninstrumented database has no server half
	e.VirtualNode = virtualNodeServer
	e.Dimensions = []EdgeDimension{
		{"client_http.method", "GET"},
		{"server_db.system", "postgresql"},
	}
	r.Record(e)
	// A second edge with no dimensions and no virtual node, to pin the negative:
	// an empty label value does not survive into Prometheus, so the common edge
	// must not carry `virtual_node` at all.
	r.Record(edge("frontend", "checkout"))

	exp := &capExporter{}
	export(t, r, exp)
	dps := exp.last(t)["traces_service_graph_request_total"].Sum().DataPoints()
	if dps.Len() != 2 {
		t.Fatalf("points = %d, want 2", dps.Len())
	}
	byServer := map[string]map[string]string{}
	for i := 0; i < dps.Len(); i++ {
		l := attrsOf(dps.At(i).Attributes())
		byServer[l["server"]] = l
	}
	wantVirtual := map[string]string{
		"client":             "frontend",
		"server":             "orders-db",
		"connection_type":    "database",
		"virtual_node":       "server",
		"client_http.method": "GET",
		"server_db.system":   "postgresql",
	}
	if l := byServer["orders-db"]; !maps.Equal(l, wantVirtual) {
		t.Errorf("virtual-node edge labels = %v, want %v", l, wantVirtual)
	}
	plain := byServer["checkout"]
	if _, ok := plain["virtual_node"]; ok {
		t.Errorf("plain edge carries virtual_node = %q, want the label absent", plain["virtual_node"])
	}
	// The virtual server never measured anything: no server-latency point for it.
	shist := exp.last(t)["traces_service_graph_request_server_seconds"].Histogram().DataPoints()
	if shist.Len() != 1 {
		t.Fatalf("server histogram points = %d, want 1 (the virtual node has no server half)", shist.Len())
	}
	if s := attrsOf(shist.At(0).Attributes())["server"]; s != "checkout" {
		t.Errorf("server histogram point is for %q, want checkout", s)
	}
}

// Two identical calls whose halves arrived in OPPOSITE orders are one series,
// end to end through the processor. The registry keys a series on the
// dimension SEQUENCE (there is no sort here any more), so this is what the
// store's canonical client-then-server ordering buys: an arrival-ordered
// sequence would key the second call differently and render a byte-identical
// label set twice in one payload.
func TestArrivalOrderDoesNotSplitSeries(t *testing.T) {
	r := NewRegistry(Config{})
	p := NewProcessor(Config{Dimensions: []string{"http.method", "http.route"}}, nil)
	p.SetSink(r)
	attrs := map[string]string{"http.method": "GET", "http.route": "/orders"}
	half := func(tid byte, kind ptrace.SpanKind) ptrace.Traces {
		s := sgSpan{kind: kind, dur: 0.1, traceID: traceID(tid), spanID: spanID(1), attrs: attrs}
		if kind == ptrace.SpanKindServer {
			s.spanID, s.parentID = spanID(2), spanID(1)
		}
		return sgTraces("checkout", s)
	}
	p.Consume(half(1, ptrace.SpanKindClient)) // request 1: client half first
	p.Consume(half(1, ptrace.SpanKindServer))
	p.Consume(half(2, ptrace.SpanKindServer)) // request 2: server half first
	p.Consume(half(2, ptrace.SpanKindClient))

	exp := &capExporter{}
	export(t, r, exp)
	dps := exp.last(t)["traces_service_graph_request_total"].Sum().DataPoints()
	if dps.Len() != 1 {
		var got []map[string]string
		for i := 0; i < dps.Len(); i++ {
			got = append(got, attrsOf(dps.At(i).Attributes()))
		}
		t.Fatalf("points = %d (%v), want the two arrival orders to share one series", dps.Len(), got)
	}
	if v := dps.At(0).IntValue(); v != 2 {
		t.Fatalf("requests = %d, want both calls on the one series", v)
	}
}

// A dimension whose name collides with a built-in label is dropped rather than
// rendered: the attributes share one map, so it would overwrite the real client
// or server and two distinct series would render one identical label set.
func TestCollidingDimensionIsDropped(t *testing.T) {
	r := NewRegistry(Config{})
	e := edge("frontend", "checkout")
	e.Dimensions = []EdgeDimension{{"client", "impostor"}, {"connection_type", "nonsense"}, {"keep", "yes"}}
	r.Record(e)

	exp := &capExporter{}
	export(t, r, exp)
	dps := exp.last(t)["traces_service_graph_request_total"].Sum().DataPoints()
	want := map[string]string{"client": "frontend", "server": "checkout", "connection_type": "", "keep": "yes"}
	if l := attrsOf(dps.At(0).Attributes()); !maps.Equal(l, want) {
		t.Errorf("labels = %v, want %v", l, want)
	}
}

// The default buckets are Tempo's ExponentialBuckets(0.1, 2, 8), in seconds,
// and each side's latency lands in its own histogram — the two are measured by
// different processes and are never mixed.
func TestHistogramBucketsAndPerSideValues(t *testing.T) {
	r := NewRegistry(Config{})
	e := edge("frontend", "checkout")
	e.ClientSeconds = 0.15 // bucket (0.1, 0.2]
	e.ServerSeconds = 0.05 // bucket (-inf, 0.1]
	r.Record(e)

	exp := &capExporter{}
	export(t, r, exp)
	got := exp.last(t)

	wantBounds := []float64{0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 6.4, 12.8}
	client := got["traces_service_graph_request_client_seconds"].Histogram().DataPoints().At(0)
	if b := client.ExplicitBounds().AsRaw(); !equalFloats(b, wantBounds) {
		t.Errorf("client bounds = %v, want %v", b, wantBounds)
	}
	if c, s := client.Count(), client.Sum(); c != 1 || s != 0.15 {
		t.Errorf("client count/sum = %d/%v, want 1/0.15", c, s)
	}
	if bc := client.BucketCounts().AsRaw(); bc[1] != 1 {
		t.Errorf("client bucket counts = %v, want the single observation in index 1 (0.1 < 0.15 <= 0.2)", bc)
	}
	server := got["traces_service_graph_request_server_seconds"].Histogram().DataPoints().At(0)
	if bc := server.BucketCounts().AsRaw(); bc[0] != 1 {
		t.Errorf("server bucket counts = %v, want the single observation in index 0 (0.05 <= 0.1)", bc)
	}
	if n := len(server.BucketCounts().AsRaw()); n != len(wantBounds)+1 {
		t.Errorf("server bucket count length = %d, want %d (bounds + overflow)", n, len(wantBounds)+1)
	}
}

// Each side's latency histogram carries an exemplar pointing at the span that
// MEASURED that side — the shared trace id (the two halves cannot pair without
// it) plus that half's own span id. A single id on both would explain only one
// half of what it was attached to.
func TestDurationHistogramExemplars(t *testing.T) {
	r := NewRegistry(Config{})
	now := t0
	fixedClock(r, &now)
	r.Record(edge("frontend", "checkout"))

	exp := &capExporter{}
	export(t, r, exp)
	got := exp.last(t)

	wantTS := pcommon.NewTimestampFromTime(t0)
	client := exemplarsOf(firstHist(t, got, "traces_service_graph_request_client_seconds"))
	wantClient := []cumagg.Exemplar{{Set: true, Value: 0.15, TS: wantTS, TraceID: traceID(1), SpanID: spanID(1)}}
	if !slices.Equal(client, wantClient) {
		t.Errorf("client exemplars = %+v, want %+v", client, wantClient)
	}
	server := exemplarsOf(firstHist(t, got, "traces_service_graph_request_server_seconds"))
	wantServer := []cumagg.Exemplar{{Set: true, Value: 0.05, TS: wantTS, TraceID: traceID(1), SpanID: spanID(2)}}
	if !slices.Equal(server, wantServer) {
		t.Errorf("server exemplars = %+v, want %+v", server, wantServer)
	}
}

// One per BUCKET, latest wins: a busy edge must not accumulate an exemplar per
// request, and the newest is the one an operator watching a live graph wants.
func TestExemplarsAreOnePerBucketLatestWins(t *testing.T) {
	r := NewRegistry(Config{})
	now := t0
	fixedClock(r, &now)
	for _, secs := range []float64{0.15, 0.19, 0.5} { // two in (0.1,0.2], one in (0.4,0.8]
		e := edge("frontend", "checkout")
		e.ClientSeconds, e.HaveServer = secs, false
		e.ClientSpanID = spanID(byte(secs * 100))
		r.Record(e)
	}

	exp := &capExporter{}
	export(t, r, exp)
	ex := exemplarsOf(firstHist(t, exp.last(t), "traces_service_graph_request_client_seconds"))
	if len(ex) != 2 {
		t.Fatalf("exemplars = %+v, want one per occupied bucket", ex)
	}
	if ex[0].Value != 0.19 || ex[0].SpanID != spanID(19) {
		t.Errorf("first bucket's exemplar = %+v, want the LATEST of the two samples in it", ex[0])
	}
	if ex[1].Value != 0.5 {
		t.Errorf("second exemplar = %+v, want the 0.5s sample", ex[1])
	}
}

// Exemplars are cleared by DELIVERY, never by rendering: a failed send keeps
// them for the retry rather than wiping evidence no collector ever saw.
func TestExemplarsResetOnlyAfterDelivery(t *testing.T) {
	r := NewRegistry(Config{})
	now := t0
	fixedClock(r, &now)
	r.Record(edge("frontend", "checkout"))

	failing := &capExporter{err: errors.New("collector down")}
	if err := r.Export(context.Background(), failing, pcommon.NewResource()); err == nil {
		t.Fatal("Export succeeded, want the injected failure")
	}
	exp := &capExporter{}
	export(t, r, exp) // the retry
	if ex := exemplarsOf(firstHist(t, exp.last(t), "traces_service_graph_request_client_seconds")); len(ex) != 1 {
		t.Fatalf("exemplars after a failed send = %+v, want them kept for the retry", ex)
	}
	// Delivered now: the next export carries none, since nothing new was seen.
	export(t, r, exp)
	if ex := exemplarsOf(firstHist(t, exp.last(t), "traces_service_graph_request_client_seconds")); len(ex) != 0 {
		t.Fatalf("exemplars after delivery = %+v, want them cleared", ex)
	}
}

// No trace id, no exemplar: a link that resolves to nothing in Tempo is worse
// than no link. (The processor refuses to key such spans at all, so this is the
// belt to that braces.)
func TestExemplarsSkippedWithoutTraceID(t *testing.T) {
	r := NewRegistry(Config{})
	e := edge("frontend", "checkout")
	e.TraceID = pcommon.TraceID{}
	r.Record(e)

	exp := &capExporter{}
	export(t, r, exp)
	if ex := exemplarsOf(firstHist(t, exp.last(t), "traces_service_graph_request_client_seconds")); len(ex) != 0 {
		t.Fatalf("exemplars = %+v, want none without a trace id", ex)
	}
}

// The toggle, and the negative it guards: with exemplars off the histograms are
// otherwise unchanged (counts and buckets still land).
func TestExemplarsCanBeDisabled(t *testing.T) {
	off := false
	r := NewRegistry(Config{Exemplars: &off})
	r.Record(edge("frontend", "checkout"))

	exp := &capExporter{}
	export(t, r, exp)
	p := firstHist(t, exp.last(t), "traces_service_graph_request_client_seconds")
	if ex := exemplarsOf(p); len(ex) != 0 {
		t.Fatalf("exemplars = %+v, want none with exemplars: false", ex)
	}
	if p.Count() != 1 {
		t.Fatalf("count = %d, want the histogram itself unaffected", p.Count())
	}
}

// The failed counter is present at ZERO for a healthy edge. Tempo creates its
// child series on the first failure, which leaves failed/total undefined for
// exactly the edges that are working; a present zero makes the ratio total.
func TestFailedCounterPresentAtZeroAndCountsFailures(t *testing.T) {
	r := NewRegistry(Config{})
	r.Record(edge("frontend", "checkout"))

	exp := &capExporter{}
	export(t, r, exp)
	failed := exp.last(t)["traces_service_graph_request_failed_total"].Sum().DataPoints()
	if failed.Len() != 1 || failed.At(0).IntValue() != 0 {
		t.Fatalf("failed points/value = %d/%d, want 1/0", failed.Len(), failed.At(0).IntValue())
	}

	e := edge("frontend", "checkout")
	e.Failed = true
	r.Record(e)
	export(t, r, exp)
	got := exp.last(t)
	if v := got["traces_service_graph_request_total"].Sum().DataPoints().At(0).IntValue(); v != 2 {
		t.Errorf("request_total = %d, want 2 (cumulative across exports)", v)
	}
	if v := got["traces_service_graph_request_failed_total"].Sum().DataPoints().At(0).IntValue(); v != 1 {
		t.Errorf("request_failed_total = %d, want 1", v)
	}
}

// The cardinality cap refuses NEW edges only: the series already admitted are
// cumulative counters, and freezing them would look downstream like the traffic
// stopped rather than like a bound binding.
func TestCardinalityCapDropsOnlyNewSeries(t *testing.T) {
	r := NewRegistry(Config{MaxCardinality: 2})
	r.Record(edge("a", "b"))
	r.Record(edge("c", "d"))

	before := obs.ServiceGraphDropped.Value()
	r.Record(edge("e", "f"))
	r.Record(edge("g", "h"))
	if got := obs.ServiceGraphDropped.Value() - before; got != 2 {
		t.Errorf("dropped delta = %v, want 2", got)
	}
	// An EXISTING series still counts while the cap is full.
	r.Record(edge("a", "b"))

	exp := &capExporter{}
	export(t, r, exp)
	dps := exp.last(t)["traces_service_graph_request_total"].Sum().DataPoints()
	if dps.Len() != 2 {
		t.Fatalf("series = %d, want 2 (the cap holds)", dps.Len())
	}
	for i := 0; i < dps.Len(); i++ {
		l := attrsOf(dps.At(i).Attributes())
		if l["client"] == "a" && dps.At(i).IntValue() != 2 {
			t.Errorf("a->b requests = %d, want 2 (an admitted series keeps counting under the cap)", dps.At(i).IntValue())
		}
		if l["client"] == "e" || l["client"] == "g" {
			t.Errorf("refused edge %v was aggregated anyway", l)
		}
	}
}

// Eviction may only ever drop values a DELIVERED export carried. An export
// interval longer than staleAfter, or an export that FAILED, must not destroy
// observations nobody has seen.
func TestStaleEvictionOnlyAfterDelivery(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	r := NewRegistry(Config{StaleAfter: "15m"})
	fixedClock(r, &now)
	r.Record(edge("frontend", "checkout"))

	// A failed export renders the series but delivers nothing.
	bad := &capExporter{err: errors.New("collector down")}
	if err := r.Export(context.Background(), bad, pcommon.NewResource()); err == nil {
		t.Fatal("Export succeeded against a failing exporter")
	}

	evictedBefore := obs.ServiceGraphEvicted.Value()
	now = now.Add(16 * time.Minute)
	exp := &capExporter{}
	export(t, r, exp)
	if exp.count() != 1 {
		t.Fatalf("payloads = %d, want 1: an undelivered series is stale but must still be exported", exp.count())
	}
	if got := obs.ServiceGraphEvicted.Value() - evictedBefore; got != 0 {
		t.Errorf("evicted delta = %v, want 0 (nothing had been delivered yet)", got)
	}
	if v := exp.last(t)["traces_service_graph_request_total"].Sum().DataPoints().At(0).IntValue(); v != 1 {
		t.Errorf("request_total = %d, want 1", v)
	}

	// That export WAS delivered, so the same series may now age out.
	now = now.Add(16 * time.Minute)
	export(t, r, exp)
	if exp.count() != 1 {
		t.Errorf("payloads = %d, want 1: an all-evicted cycle has nothing to send", exp.count())
	}
	if got := obs.ServiceGraphEvicted.Value() - evictedBefore; got != 1 {
		t.Errorf("evicted delta = %v, want 1", got)
	}
	if n := r.store.Len(); n != 0 {
		t.Errorf("series after eviction = %d, want 0", n)
	}
}

// A series re-created after an eviction restarts its counters from zero, so it
// must also carry a FRESH start timestamp — that is how OTLP spells a
// cumulative reset, and reusing the old one reads as a counter jumping
// backwards.
func TestReCreatedSeriesGetsFreshStartTimestamp(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	r := NewRegistry(Config{StaleAfter: "1m"})
	fixedClock(r, &now)

	r.Record(edge("frontend", "checkout"))
	r.Record(edge("frontend", "checkout"))
	exp := &capExporter{}
	export(t, r, exp)
	first := exp.last(t)["traces_service_graph_request_total"].Sum().DataPoints().At(0)
	firstStart := first.StartTimestamp()
	if first.IntValue() != 2 {
		t.Fatalf("request_total = %d, want 2", first.IntValue())
	}

	now = now.Add(2 * time.Minute) // delivered above, so this evicts
	export(t, r, exp)
	if n := r.store.Len(); n != 0 {
		t.Fatalf("series after eviction = %d, want 0", n)
	}

	now = now.Add(time.Minute)
	r.Record(edge("frontend", "checkout"))
	export(t, r, exp)
	again := exp.last(t)["traces_service_graph_request_total"].Sum().DataPoints().At(0)
	if again.IntValue() != 1 {
		t.Errorf("request_total after re-creation = %d, want 1 (the counter restarted)", again.IntValue())
	}
	if again.StartTimestamp() <= firstStart {
		t.Errorf("start timestamp %v did not advance past %v; a restarted counter under the old start reads as a decrease",
			again.StartTimestamp(), firstStart)
	}
	if want := pcommon.NewTimestampFromTime(now); again.StartTimestamp() != want {
		t.Errorf("start timestamp = %v, want the re-creation time %v", again.StartTimestamp(), want)
	}
}

// An empty registry sends nothing at all rather than an empty payload, and Run
// always makes a final export so the last window's edges are not lost at
// shutdown.
func TestExportEmptyAndRunFinalExport(t *testing.T) {
	r := NewRegistry(Config{})
	exp := &capExporter{}
	export(t, r, exp)
	if exp.count() != 0 {
		t.Fatalf("payloads = %d, want 0 for an empty registry", exp.count())
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx, exp, time.Hour, pcommon.NewResource(), nil) }()
	r.Record(edge("frontend", "checkout"))
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
	if exp.count() != 1 {
		t.Fatalf("payloads = %d, want 1 from the final export", exp.count())
	}
}

// Record runs on the ingest goroutines, once per completed request. The warm
// path must not allocate: the series key is built on a stack buffer and looked
// up with map[string(key)], and the dimension names are sorted in place in a
// second stack scratch.
func BenchmarkRecord(b *testing.B) {
	r := NewRegistry(Config{})
	e := edge("frontend", "checkout")
	e.Connection = ConnectionDatabase
	e.Dimensions = []EdgeDimension{{"client_http.method", "GET"}, {"server_db.system", "postgresql"}}
	r.Record(e) // admit the series; the benchmark measures the warm path
	b.ReportAllocs()
	for b.Loop() {
		r.Record(e)
	}
}

// The benchmark REPORTS the budget; this fails when it is broken. Record is on
// the per-request path of every shard, and the two stack scratches (series key,
// dimension names) only pay off while nothing forces them onto the heap — a
// closure, a fmt call or a sort.Strings in this function would silently cost an
// allocation per request.
func TestRecordIsAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	r := NewRegistry(Config{})
	e := edge("frontend", "checkout")
	e.Dimensions = []EdgeDimension{{"client_http.method", "GET"}, {"server_db.system", "postgresql"}}
	r.Record(e) // admit the series: only the warm path is budgeted
	if n := testing.AllocsPerRun(200, func() { r.Record(e) }); n != 0 {
		t.Errorf("Record allocates %v times per call, want 0", n)
	}
}

// Record is called from the pairing store on the ingest goroutines while the
// export loop renders; -race is the point of this test.
func TestRecordConcurrent(t *testing.T) {
	r := NewRegistry(Config{})
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				e := edge(fmt.Sprintf("client-%d", w), fmt.Sprintf("server-%d", i%4))
				e.Failed = i%3 == 0
				r.Record(e)
			}
		}(w)
	}
	exp := &capExporter{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			export(t, r, exp)
		}
	}()
	wg.Wait()

	export(t, r, exp)
	var total int64
	for _, dp := range all(exp.last(t)["traces_service_graph_request_total"].Sum().DataPoints()) {
		total += dp.IntValue()
	}
	if want := int64(8 * 200); total != want {
		t.Errorf("summed request_total = %d, want %d", total, want)
	}
}

// --- helpers ---

func all(dps pmetric.NumberDataPointSlice) []pmetric.NumberDataPoint {
	out := make([]pmetric.NumberDataPoint, 0, dps.Len())
	for i := 0; i < dps.Len(); i++ {
		out = append(out, dps.At(i))
	}
	return out
}

func keysOf(m map[string]pmetric.Metric) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func equalFloats(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalCT(a, b []ConnectionType) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// The same defect the sibling spanmetrics generator had: truncating a Go string
// is a RESLICE, so a series' 256-byte label kept the whole sender-controlled
// string alive — here for the series' life AND, one layer earlier, for a whole
// Wait in the pairing store. Both retention points must cut with a copy; the
// key Record builds may still reslice (the map copies it on insert).
func TestTruncatedLabelsDoNotRetainTheSenderStrings(t *testing.T) {
	const huge = 4 << 20
	name := strings.Repeat("x", huge)
	peer := strings.Repeat("y", huge)
	dim := strings.Repeat("z", huge)

	p := NewProcessor(Config{Dimensions: []string{"http.route"}}, nil)
	reg := NewRegistry(Config{})
	p.SetSink(reg)
	p.Consume(sgTraces(name, sgSpan{
		name: "GET /orders", kind: ptrace.SpanKindClient, dur: 0.1,
		traceID: traceID(1), spanID: spanID(1),
		attrs: map[string]string{"peer.service": peer, "http.route": dim},
	}))
	// Expire the unpaired client half so it is promoted to a virtual-node edge
	// and reaches the registry.
	p.now = func() time.Time { return time.Now().Add(time.Hour) }
	p.Sweep()

	if reg.store.Len() != 1 {
		t.Fatalf("series = %d, want 1", reg.store.Len())
	}
	reg.store.Range(func(_ string, s *edgeSeries) bool {
		for _, l := range s.labels {
			var src string
			switch l.name {
			case labelClient:
				src = name
			case labelServer:
				src = peer
			case "client_http.route":
				src = dim
			default:
				continue
			}
			if len(l.value) != maxDimensionValueBytes {
				t.Errorf("label %s is %d bytes, want %d", l.name, len(l.value), maxDimensionValueBytes)
			}
			if unsafe.StringData(l.value) == unsafe.StringData(src) {
				t.Errorf("label %s still points into its %d-byte source: the whole string stays alive for as long as the series does",
					l.name, huge)
			}
		}
		return true
	})
}

// Rendering must not stall the receive path. Record is called by the pairing
// store from INSIDE its own mutex (upsert -> emit -> Record), so a render that
// held the series lock across the payload build put the WHOLE build — tens of
// milliseconds at the cardinality cap — in front of every Consume on the shard,
// once per export interval. The lock is now held only for the value snapshot.
func TestRenderDoesNotStallRecord(t *testing.T) {
	const series = 20000
	r := NewRegistry(Config{MaxCardinality: series + 1})
	for i := 0; i < series; i++ {
		e := edge(fmt.Sprintf("client-%05d", i), "checkout")
		e.Dimensions = []EdgeDimension{{"client_http.route", "/api/v1/orders"}}
		r.Record(e)
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	var maxStall atomic.Int64
	var calls atomic.Int64
	go func() {
		defer close(done)
		e := edge("client-00000", "checkout") // an admitted series: the warm path
		e.Dimensions = []EdgeDimension{{"client_http.route", "/api/v1/orders"}}
		for {
			select {
			case <-stop:
				return
			default:
			}
			t0 := time.Now()
			r.Record(e)
			if d := int64(time.Since(t0)); d > maxStall.Load() {
				maxStall.Store(d)
			}
			calls.Add(1)
		}
	}()
	// Let the recorder get going before the render starts.
	for calls.Load() < 1000 {
		time.Sleep(time.Millisecond)
	}

	start := time.Now()
	md := r.render(pcommon.NewResource(), time.Now())
	render := time.Since(start)
	close(stop)
	<-done

	stall := time.Duration(maxStall.Load())
	t.Logf("render of %d series took %v; worst concurrent Record took %v", series, render, stall)
	if md.ResourceMetrics().Len() == 0 {
		t.Fatal("nothing rendered")
	}
	// Relative, so a slow machine moves both numbers together; the absolute
	// floor keeps an unlucky GC pause from failing a fast render.
	if stall > render/2 && stall > 2*time.Millisecond {
		t.Errorf("a concurrent Record blocked for %v during a %v render: the payload build is holding the series lock, and the pairing store holds ITS mutex across Record",
			stall, render)
	}
}

// The render scratch is REUSED across renders, and a later, smaller render
// reslices it. The tail past the new length keeps its bucket and exemplar
// capacity on purpose — that reuse is what the scratch is for — but it must not
// keep the LABELS of series that are gone: a burst up to the cardinality cap
// followed by mass stale eviction would otherwise pin peak-cardinality label
// sets (a slice each, plus their retained strings) for the process' life, which
// is the same pin snapshot already avoids for the pointer slice.
func TestRenderScratchDoesNotPinTheLabelsOfEvictedSeries(t *testing.T) {
	const peak = 64
	now := time.Unix(1_700_000_000, 0)
	r := NewRegistry(Config{MaxCardinality: peak + 8, StaleAfter: "1m"})
	fixedClock(r, &now)
	exp := &capExporter{}

	for i := 0; i < peak; i++ {
		r.Record(edge(fmt.Sprintf("client-%03d", i), "checkout"))
	}
	export(t, r, exp) // renders every series and marks them delivered

	// The whole burst goes stale; the next render covers one series.
	now = now.Add(10 * time.Minute)
	r.Record(edge("client-new", "checkout"))
	export(t, r, exp)

	if got := r.store.Len(); got != 1 {
		t.Fatalf("the store holds %d series after the eviction, want 1", got)
	}
	if cap(r.snap) < peak {
		t.Fatalf("the scratch shrank to %d: the test no longer exercises its tail", cap(r.snap))
	}
	whole := r.snap[:cap(r.snap)]
	for i := len(r.snap); i < len(whole); i++ {
		if whole[i].labels != nil {
			t.Fatalf("scratch slot %d (past the %d live series) still holds an evicted series' label set", i, len(r.snap))
		}
	}
}

// Clearing the tail's labels bounds the STRINGS but not the ARRAYS: every unused
// slot keeps two bucket slices and two exemplar slices, and a cumagg.Exemplar is
// 48 B — 24.3 MB still reachable after one burst to the 20000-series cap and a
// mass stale eviction that left ONE series, 71% of it exemplars. The reuse is
// worth having for the steady state, not for a peak the process saw once, so a
// scratch that stays far under its capacity is rebuilt at the live size.
func TestRenderScratchShrinksBackAfterACardinalityBurst(t *testing.T) {
	const peak = 2000
	now := time.Unix(1_700_000_000, 0)
	r := NewRegistry(Config{MaxCardinality: peak + 8, StaleAfter: "1m"})
	fixedClock(r, &now)
	exp := &capExporter{}

	for i := 0; i < peak; i++ {
		r.Record(edge(fmt.Sprintf("client-%04d", i), "checkout"))
	}
	export(t, r, exp) // renders every series and marks them delivered
	if cap(r.snap) < peak {
		t.Fatalf("the scratch holds %d slots after rendering %d series", cap(r.snap), peak)
	}
	burstArrays := snapArrayCap(r)

	// The whole burst goes stale and one series takes its place. Several export
	// cycles, because the shrink is deliberately hysteretic: an oscillating
	// workload must keep its capacity.
	now = now.Add(10 * time.Minute)
	for i := 0; i < snapShrinkRuns+1; i++ {
		r.Record(edge("client-new", "checkout"))
		export(t, r, exp)
		now = now.Add(time.Second)
	}

	if got := r.store.Len(); got != 1 {
		t.Fatalf("the store holds %d series after the eviction, want 1", got)
	}
	if got := cap(r.snap); got > peak/snapShrinkFactor {
		t.Errorf("the scratch still holds %d slots for 1 live series", got)
	}
	if got := snapArrayCap(r); got > burstArrays/10 {
		t.Errorf("the scratch retains %d bucket+exemplar slots (peak render: %d): the tail's arrays are still reachable", got, burstArrays)
	}
}

// snapArrayCap totals the bucket and exemplar slots the whole scratch holds —
// its capacity, not its current length, which is the memory that stays
// reachable between renders.
func snapArrayCap(r *Registry) int {
	whole := r.snap[:cap(r.snap)]
	n := 0
	for i := range whole {
		n += cap(whole[i].client.buckets) + cap(whole[i].server.buckets)
		n += cap(whole[i].client.ex) + cap(whole[i].server.ex)
	}
	return n
}
