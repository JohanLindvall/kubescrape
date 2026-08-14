package metrics

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// The undelivered-chunk eviction must drop the resource that has gone QUIETEST,
// never the one still producing. retryOrder is rebuilt every Export in chunk
// order — fresh resources first, retained-only ones appended last — so popping
// index 0 systematically destroyed a live resource's whole retained pile (its
// sealed aggregation windows and expiry-grace samples are held nowhere else)
// while a dead pile survived at the tail.
func TestRetainEvictsTheStalestResourceNotTheLiveOne(t *testing.T) {
	set := &DynamicMetricSet{}

	t1 := time.Now().Add(-time.Minute)
	t2 := t1.Add(30 * time.Second)
	gen := func(ts time.Time, n int) seriesSamples {
		return seriesSamples{samples: make([]sample, n), ts: ts}
	}

	// The shape one Export past the first failure produces: "live" carries the
	// retained generation AND a fresh one (mergeRetry prepends the old), "quiet"
	// only the retained one — and mergeRetry appended "quiet" to the order after
	// the resources that had fresh samples, so it is at the TAIL.
	byResource := map[string][]seriesSamples{
		"live":  {gen(t1, 15_000), gen(t2, 15_000)},
		"quiet": {gen(t1, 30_000)},
	}
	set.retain(byResource, []string{"live", "quiet"})

	if _, ok := set.retryBy["live"]; !ok {
		t.Error("the still-producing resource was evicted")
	}
	if _, ok := set.retryBy["quiet"]; ok {
		t.Error("the resource that had gone quiet survived the eviction")
	}
	if set.DroppedUndelivered() != 1 {
		t.Errorf("dropped-undelivered = %d, want 1", set.DroppedUndelivered())
	}
	total := 0
	for _, ss := range set.retryBy {
		for _, e := range ss {
			total += len(e.samples)
		}
	}
	if total != set.retainedSamples {
		t.Errorf("accounting drifted: counted %d, actual %d", set.retainedSamples, total)
	}
}

// dupKeyResource builds a resource whose attributes REPEAT a key. Legal on the
// OTLP wire (attributes are a repeated KeyValue) and impossible through pdata's
// public Map API, so it goes through the JSON unmarshaler like a hostile sender
// would.
func dupKeyResource(t *testing.T, key, first, second string) pcommon.Map {
	t.Helper()
	raw := []byte(`{"resourceMetrics":[{"resource":{"attributes":[
		{"key":"` + key + `","value":{"stringValue":"` + first + `"}},
		{"key":"` + key + `","value":{"stringValue":"` + second + `"}}
	]},"scopeMetrics":[{}]}]}`)
	var um pmetric.JSONUnmarshaler
	md, err := um.UnmarshalMetrics(raw)
	if err != nil {
		t.Fatal(err)
	}
	attrs := md.ResourceMetrics().At(0).Resource().Attributes()
	if attrs.Len() != 2 {
		t.Fatal("test premise broken: duplicate keys were deduped at decode")
	}
	return attrs
}

// EmitDirect is a door into the store for a resource that arrived on the wire
// (a transform script's emit_metric over an ingested metrics/traces payload,
// which the ingest log chain's dedupe does not cover). resourceAccum folds one
// term per map ENTRY while resourceString renders last-wins, so {k=v,k=v} used
// to mint a series distinct from {k=v} whose rendered identity is the same —
// two byte-identical data points in one exported Metric.
func TestEmitDirectHashesTheRenderedResourceIdentity(t *testing.T) {
	setTimeForTest(time.Unix(1_800_400_000, 0))
	defer testEpoch.Store(0)

	set, err := newTestSet([]Dynamic{{Name: "emit_total", Type: CounterType, Value: "1"}})
	if err != nil {
		t.Fatal(err)
	}
	dup := dupKeyResource(t, "service.name", "a", "a")
	if err := set.EmitDirect("emit_total", 1, nil, dup); err != nil {
		t.Fatal(err)
	}
	if err := set.EmitDirect("emit_total", 1, nil, res(map[string]string{"service.name": "a"})); err != nil {
		t.Fatal(err)
	}
	if n := len(set.rules[0].series.db); n != 1 {
		t.Fatalf("a resource repeating a key minted %d live samples where its rendered identity is ONE", n)
	}

	exp := &capExporter{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}
	var real []float64
	_, vals := numberPoints(exp.md, "emit_total")
	for _, v := range vals {
		if v != 0 { // skip the synthetic counter baseline zeros
			real = append(real, v)
		}
	}
	if len(real) != 1 || real[0] != 2 {
		t.Fatalf("want one merged point of value 2, got %v", real)
	}
}

// The permutation half of the same divergence: {k=p,k=q} and {k=q,k=p} are the
// same multiset, so any order-independent fold over one term per ENTRY hashes
// them identically while they render as different resources — two senders'
// observations merged under whichever string was frozen at admit. Resolving
// the repeat last-wins, as the render does, is what separates them.
func TestEmitDirectDistinguishesPermutedDuplicateKeys(t *testing.T) {
	setTimeForTest(time.Unix(1_800_500_000, 0))
	defer testEpoch.Store(0)

	set, err := newTestSet([]Dynamic{{Name: "perm_total", Type: CounterType, Value: "1"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := set.EmitDirect("perm_total", 1, nil, dupKeyResource(t, "service.name", "p", "q")); err != nil {
		t.Fatal(err)
	}
	if err := set.EmitDirect("perm_total", 1, nil, dupKeyResource(t, "service.name", "q", "p")); err != nil {
		t.Fatal(err)
	}
	if n := len(set.rules[0].series.db); n != 2 {
		t.Fatalf("two resources rendering as service.name=q and =p folded into %d live samples, want 2", n)
	}
}
