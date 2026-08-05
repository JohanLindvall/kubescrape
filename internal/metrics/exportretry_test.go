package metrics

// Regression tests for the export retention (retryBy) semantics: retained
// generations keep their own snapshot timestamps, permanent rejections are
// dropped counted instead of re-offered forever, the pile is bounded by
// SAMPLES and not only by resources, and the hashed resource identity matches
// the rendered (truncated) one. Each pins a defect found by audit.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

// numberPoints walks every Sum/Gauge point of the named metric across the
// captured payloads, returning (timestamp, value) pairs in render order.
func numberPoints(md []pmetric.Metrics, name string) (ts []uint64, vals []float64) {
	for _, m := range md {
		rms := m.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					mt := ms.At(k)
					if mt.Name() != name {
						continue
					}
					var dps pmetric.NumberDataPointSlice
					switch mt.Type() {
					case pmetric.MetricTypeSum:
						dps = mt.Sum().DataPoints()
					case pmetric.MetricTypeGauge:
						dps = mt.Gauge().DataPoints()
					default:
						continue
					}
					for d := 0; d < dps.Len(); d++ {
						ts = append(ts, uint64(dps.At(d).Timestamp()))
						vals = append(vals, dps.At(d).DoubleValue())
					}
				}
			}
		}
	}
	return ts, vals
}

// A failed export's retained samples must re-offer at their ORIGINAL snapshot
// time. Restamping them with the retrying export's clock rendered a still-live
// cumulative series — which every fresh snapshot re-reads — twice under ONE
// timestamp with two different values in one payload: a duplicate a
// Prometheus-lineage backend rejects or ingests order-dependently.
func TestRetryKeepsOriginalTimestamps(t *testing.T) {
	setTimeForTest(time.Unix(1_800_000_000, 0))
	defer testEpoch.Store(0)

	set, err := newTestSet([]Dynamic{{
		Name: "dup_total", Type: CounterType, Value: "1", Match: []string{"m=1"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	add := func() {
		set.Add(nil, labelsFrom(map[string]string{"m": "1"}), res(map[string]string{"k8s.pod.name": "p"}), "")
	}
	add()
	if err := set.Export(context.Background(), &failingExporter{}, 0); err == nil {
		t.Fatal("want failure")
	}
	add()
	add()
	exp := &capExporter{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}

	ts, vals := numberPoints(exp.md, "dup_total")
	if len(vals) == 0 {
		t.Fatal("no points rendered")
	}
	// No two points may share a timestamp (the counter's synthetic baseline
	// zeros included), and values must be non-decreasing in render order —
	// the retained generation is the OLDER point of the same series.
	seen := map[uint64]float64{}
	for i, tsv := range ts {
		if prev, dup := seen[tsv]; dup {
			t.Fatalf("duplicate timestamp %d: values %v and %v in one payload", tsv, prev, vals[i])
		}
		seen[tsv] = vals[i]
	}
	for i := 1; i < len(vals); i++ {
		if ts[i] < ts[i-1] {
			t.Fatalf("timestamps out of order: %v", ts)
		}
		if vals[i] < vals[i-1] {
			t.Fatalf("cumulative values regressed in render order: %v", vals)
		}
	}
	// Both generations arrived: the retained value 1 and the fresh value 3.
	got := map[float64]bool{}
	for _, v := range vals {
		got[v] = true
	}
	if !got[1] || !got[3] {
		t.Fatalf("want the retained (1) and fresh (3) generations, got values %v", vals)
	}
}

// A permanently rejected chunk must be dropped and counted, never retained:
// the payload cannot become acceptable, and re-offering it re-sent the same
// refused chunk every interval forever while the pile grew. Every other
// producer in the repo classifies; the set takes the classifier by injection
// (it cannot import otlpexport — obs sits between the packages).
func TestPermanentRejectionDropsInsteadOfRetaining(t *testing.T) {
	setTimeForTest(time.Unix(1_800_100_000, 0))
	defer testEpoch.Store(0)

	set, err := newTestSet([]Dynamic{{
		Name: "perm_total", Type: CounterType, Value: "1", Match: []string{"m=1"},
	}}, WithPermanentClassifier(func(error) bool { return true }))
	if err != nil {
		t.Fatal(err)
	}
	set.Add(nil, labelsFrom(map[string]string{"m": "1"}), res(map[string]string{"k8s.pod.name": "p"}), "")
	if err := set.Export(context.Background(), &failingExporter{}, 0); err == nil {
		t.Fatal("the export error must still be reported")
	}
	if len(set.retryBy) != 0 || len(set.retryOrder) != 0 || set.retainedSamples != 0 {
		t.Fatalf("permanent rejection was retained: %d resources, %d samples", len(set.retryBy), set.retainedSamples)
	}
	if set.DroppedUndelivered() == 0 {
		t.Fatal("the drop was not counted")
	}
}

// The retention is bounded by SAMPLES, not only by distinct resources: a node
// has a handful of log resources, so the resource cap never bound while every
// failed export appended one more generation of every live series.
func TestRetryPileIsBoundedBySamples(t *testing.T) {
	setTimeForTest(time.Unix(1_800_200_000, 0))
	defer testEpoch.Store(0)

	set, err := newTestSet([]Dynamic{{
		Name: "grow_total", Type: CounterType, Value: "1",
		Match: []string{"m=1"}, Labels: []string{"id"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	// 4000 label combinations over 4 resources; each failed export retains
	// one generation (~4000 samples), crossing maxRetainedSamples (50k)
	// within ~13 failures.
	for i := 0; i < 4000; i++ {
		set.Add(nil, labelsFrom(map[string]string{"m": "1", "id": fmt.Sprint(i)}),
			res(map[string]string{"k8s.pod.name": fmt.Sprintf("p%d", i%4)}), "")
	}
	for i := 0; i < 20; i++ {
		if err := set.Export(context.Background(), &failingExporter{}, 0); err == nil {
			t.Fatal("want failure")
		}
		total := 0
		for _, ss := range set.retryBy {
			for _, e := range ss {
				total += len(e.samples)
			}
		}
		if total != set.retainedSamples {
			t.Fatalf("accounting drifted after export %d: counted %d, actual %d", i+1, set.retainedSamples, total)
		}
		// The cap may overshoot by at most one resource's worth per retain
		// (eviction drops whole oldest resources after the append).
		if set.retainedSamples > maxRetainedSamples+4000 {
			t.Fatalf("retained samples unbounded: %d after export %d", set.retainedSamples, i+1)
		}
	}
	if set.DroppedUndelivered() == 0 {
		t.Fatal("crossing the sample bound must drop and count oldest resources")
	}
}

// The hashed resource identity must be the RENDERED one: values are truncated
// at maxLabelValueBytes for retention and rendering, so the hash folds the
// same truncated view. Hashing the full value made two resources differing
// only past the bound two live samples sharing one serialized identity —
// duplicate same-timestamp points every export (and, for histograms, one
// merged point whose buckets summed both variants). Under truncation they ARE
// one identity, so they must be ONE series.
func TestLongResourceAttrHashMatchesRenderedIdentity(t *testing.T) {
	setTimeForTest(time.Unix(1_800_300_000, 0))
	defer testEpoch.Store(0)

	set, err := newTestSet([]Dynamic{{
		Name: "long_total", Type: CounterType, Value: "1", Match: []string{"m=1"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	prefix := strings.Repeat("a", 300)
	set.Add(nil, labelsFrom(map[string]string{"m": "1"}), res(map[string]string{"attr": prefix + "-variant-1"}), "")
	set.Add(nil, labelsFrom(map[string]string{"m": "1"}), res(map[string]string{"attr": prefix + "-variant-2"}), "")

	s := set.rules[0].series
	if len(s.db) != 1 {
		t.Fatalf("resources identical up to the truncation bound must be one series, got %d", len(s.db))
	}
	exp := &capExporter{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}
	_, vals := numberPoints(exp.md, "long_total")
	var real []float64
	for _, v := range vals {
		if v != 0 { // skip the synthetic counter baseline zeros
			real = append(real, v)
		}
	}
	if len(real) != 1 || real[0] != 2 {
		t.Fatalf("want one merged point of value 2, got %v", real)
	}
}

// A name prefix must not mask the empty-name check: "" + prefix compiled into
// a metric literally named the prefix, and two such rules silently shared one
// series.
func TestNamePrefixDoesNotMaskEmptyName(t *testing.T) {
	_, err := NewDynamicMetricSet([]Dynamic{{
		Type: CounterType, Value: "1", Match: []string{"m=1"},
	}}, WithNamePrefix("log_"))
	if err == nil || !strings.Contains(err.Error(), "no name") {
		t.Fatalf("want a no-name refusal, got %v", err)
	}
}
