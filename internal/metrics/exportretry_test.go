package metrics

// Regression tests for the export retention (retryBy) semantics: retained
// generations keep their own snapshot timestamps, permanent rejections are
// dropped counted instead of re-offered forever, the pile is bounded by
// SAMPLES and not only by resources, and the hashed resource identity matches
// the rendered (truncated) one. Each pins a defect found by audit.

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
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
// time. Restamping them with the retrying export's clock rendered a retained
// generation and the fresh snapshot of the same series twice under ONE
// timestamp with two different values in one payload: a duplicate a
// Prometheus-lineage backend rejects or ingests order-dependently. Only values
// the store no longer holds are retained, so the fixture is an aggregating
// gauge, whose sealed window the next observation replaces.
func TestRetryKeepsOriginalTimestamps(t *testing.T) {
	setTimeForTest(time.Unix(1_800_000_000, 0))
	defer testEpoch.Store(0)

	set, err := newTestSet([]Dynamic{{
		Name: "dup_max", Type: GaugeType, Action: "max", Value: "v", Match: []string{"m=1"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	add := func(v string) {
		set.Add(valuesFrom(map[string]string{"v": v}), labelsFrom(map[string]string{"m": "1"}), res(map[string]string{"k8s.pod.name": "p"}), "")
	}
	add("1")
	if err := set.Export(context.Background(), &failingExporter{}, 0); err == nil {
		t.Fatal("want failure")
	}
	add("2")
	add("3")
	exp := &capExporter{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}

	ts, vals := numberPoints(exp.md, "dup_max")
	if len(vals) == 0 {
		t.Fatal("no points rendered")
	}
	// No two points may share a timestamp, and they render oldest first — the
	// retained generation is the OLDER point of the same series.
	seen := map[uint64]float64{}
	for i, tsv := range ts {
		if prev, dup := seen[tsv]; dup {
			t.Fatalf("duplicate timestamp %d: values %v and %v in one payload", tsv, prev, vals[i])
		}
		seen[tsv] = vals[i]
	}
	for i := 1; i < len(ts); i++ {
		if ts[i] < ts[i-1] {
			t.Fatalf("timestamps out of order: %v", ts)
		}
	}
	// Both windows arrived: the retained (1) and the fresh (3).
	if len(vals) != 2 || vals[0] != 1 || vals[1] != 3 {
		t.Fatalf("want the retained window (1) then the fresh one (3), got values %v", vals)
	}
}

// A retained generation and the fresh snapshot of one series must render into
// ONE Metric per ScopeMetrics: two same-named Metrics in one scope violate
// OTLP's one-metric-per-name rule, and a strict consumer may reject the chunk
// (classified permanent → the samples dropped) or dedupe order-dependently.
func TestRetryMergesIntoOneMetricPerScope(t *testing.T) {
	setTimeForTest(time.Unix(1_800_000_000, 0))
	defer testEpoch.Store(0)

	set, err := newTestSet([]Dynamic{{
		Name: "dup_max", Type: GaugeType, Action: "max", Value: "v", Match: []string{"m=1"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	add := func(v string) {
		set.Add(valuesFrom(map[string]string{"v": v}), labelsFrom(map[string]string{"m": "1"}), res(map[string]string{"k8s.pod.name": "p"}), "")
	}
	add("1")
	if err := set.Export(context.Background(), &failingExporter{}, 0); err == nil {
		t.Fatal("want failure")
	}
	add("2")
	exp := &capExporter{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}
	for _, md := range exp.md {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				names := map[string]int{}
				for k := 0; k < ms.Len(); k++ {
					names[ms.At(k).Name()]++
				}
				for name, n := range names {
					if n > 1 {
						t.Fatalf("metric %q rendered %d times in one ScopeMetrics; generations must merge into one Metric", name, n)
					}
				}
			}
		}
	}
	// Both generations still arrived, as points of the one metric.
	_, vals := numberPoints(exp.md, "dup_max")
	got := map[float64]bool{}
	for _, v := range vals {
		got[v] = true
	}
	if !got[1] || !got[2] {
		t.Fatalf("want the retained (1) and fresh (2) generations, got values %v", vals)
	}
}

// The Registry has no retention, so a FAILED export must give each counter's
// consumed zero-baseline flag back: without the re-arm, a collector outage at
// the first self-metrics interval permanently ate the synthetic zero points
// and increase()/rate() missed every counter's whole first ramp — the exact
// defect the baselines were added for.
func TestRegistryFailedExportKeepsCounterBaseline(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("test_baseline_total", "d")
	// Built an hour ago, so the start floor (streamStart) does not bind and
	// both regular baseline zeros are due.
	for _, s := range r.series {
		s.created -= 3600
	}
	c.Inc()
	ctx := context.Background()
	res := pcommon.NewResource()
	if err := r.Export(ctx, &failingExporter{}, res); err == nil {
		t.Fatal("want failure")
	}
	exp := &capExporter{}
	if err := r.Export(ctx, exp, res); err != nil {
		t.Fatal(err)
	}
	_, vals := numberPoints(exp.md, "test_baseline_total")
	zeros := 0
	for _, v := range vals {
		if v == 0 {
			zeros++
		}
	}
	if zeros != 2 {
		t.Fatalf("delivered payload carried %d baseline zeros (values %v), want 2 — the failed export consumed them", zeros, vals)
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
// failed export appended one more generation of every aggregation window. Past
// the bound the OLDEST generations go, and the pile never exceeds it.
func TestRetryPileIsBoundedBySamples(t *testing.T) {
	setTimeForTest(time.Unix(1_800_200_000, 0))
	defer testEpoch.Store(0)

	set, err := newTestSet([]Dynamic{{
		Name: "grow_max", Type: GaugeType, Action: "max", Value: "v",
		Match: []string{"m=1"}, Labels: []string{"id"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	// 4000 label combinations over 4 resources, each observed afresh every
	// cycle, so every failed export retains one generation of 4000 windows
	// and crosses maxRetainedSamples (50k) within ~13 failures.
	for i := range 20 {
		for j := range 4000 {
			set.Add(valuesFrom(map[string]string{"v": "1"}), labelsFrom(map[string]string{"m": "1", "id": strconv.Itoa(j)}),
				res(map[string]string{"k8s.pod.name": fmt.Sprintf("p%d", j%4)}), "")
		}
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
		if set.retainedSamples > maxRetainedSamples {
			t.Fatalf("retained samples unbounded: %d after export %d", set.retainedSamples, i+1)
		}
	}
	if set.DroppedUndelivered() == 0 {
		t.Fatal("crossing the sample bound must drop and count the oldest generations")
	}
}

// The case the retention used to get most wrong: a LIVE series is not a
// value the store has lost, so a failed export must not keep a copy of it —
// every such copy was one more generation of every live series per failed
// cycle, which filled the sample cap within about thirteen cycles of an outage
// and then evicted whole resources, counting as LOST counters that the next
// export delivers in full. The store re-reads them; the failed export only
// hands back what its snapshot consumed, so the recovery still carries the
// full total AND the baseline zeros the failed first export never delivered.
func TestLiveSeriesAreReReadNotRetained(t *testing.T) {
	setTimeForTest(time.Unix(1_700_250_000, 0)) // in the PAST: the render clock is the wall clock, and baseline zeros need room before it
	defer testEpoch.Store(0)

	set, err := newTestSet([]Dynamic{{
		Name: "live_total", Type: CounterType, Value: "1",
		Match: []string{"m=1"}, Labels: []string{"id"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	add := func() {
		for j := range 4000 {
			set.Add(nil, labelsFrom(map[string]string{"m": "1", "id": strconv.Itoa(j)}),
				res(map[string]string{"k8s.pod.name": fmt.Sprintf("p%d", j%4)}), "")
		}
	}
	for range 20 {
		add()
		if err := set.Export(context.Background(), &failingExporter{}, 0); err == nil {
			t.Fatal("want failure")
		}
	}
	if set.retainedSamples != 0 || len(set.retryBy) != 0 {
		t.Fatalf("a failed export retained %d samples of live series over %d resources — the store still holds every one of them",
			set.retainedSamples, len(set.retryBy))
	}
	if got := set.DroppedUndelivered(); got != 0 {
		t.Fatalf("dropped-undelivered = %d, want 0: nothing was lost, the next export delivers every total", got)
	}

	exp := &capExporter{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}
	_, vals := numberPoints(exp.md, "live_total")
	full, zeros := 0, 0
	for _, v := range vals {
		switch v {
		case 20:
			full++
		case 0:
			zeros++
		default:
			t.Fatalf("a delivered total of %v, want every series at 20 (or a baseline zero)", v)
		}
	}
	if full != 4000 {
		t.Fatalf("delivered %d series at their full total, want 4000", full)
	}
	if zeros == 0 {
		t.Fatal("no baseline zeros on the recovery export: the failed first export consumed them and nothing gave them back")
	}
}

// A series that goes QUIET during an outage is the one the store will stop
// emitting: past maxAge it is not exported again, past the grace it is deleted.
// A failed export must therefore hand back `exported` as well as `initial`
// (series.rearm), so the delete still emits the value — as a final sample the
// retention then keeps — rather than the failure having silently counted as
// its one export.
func TestQuietSeriesSurvivesAnOutageThroughTheStore(t *testing.T) {
	t0 := int64(1_800_260_000)
	setTimeForTest(time.Unix(t0, 0))
	defer testEpoch.Store(0)

	set, err := newTestSet([]Dynamic{{
		Name: "quiet_total", Type: CounterType, Value: "1", Match: []string{"m=1"}, MaxAge: "10s",
	}})
	if err != nil {
		t.Fatal(err)
	}
	set.Add(nil, labelsFrom(map[string]string{"m": "1"}), res(map[string]string{"k8s.pod.name": "quiet"}), "")
	if err := set.Export(context.Background(), &failingExporter{}, 0); err == nil {
		t.Fatal("want failure")
	}
	// Past maxAge + grace: the store deletes the series at this export.
	setTimeForTest(time.Unix(t0+600, 0))
	if err := set.Export(context.Background(), &failingExporter{}, 0); err == nil {
		t.Fatal("want failure")
	}
	if n := set.rules[0].series.count; n != 0 {
		t.Fatalf("the quiet series is still live (%d); the fixture must reach the grace delete", n)
	}
	exp := &capExporter{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}
	_, vals := numberPoints(exp.md, "quiet_total")
	delivered := false
	for _, v := range vals {
		if v == 1 {
			delivered = true
		}
	}
	if !delivered {
		t.Fatalf("the quiet series' only observation was never delivered (values %v)", vals)
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

// A retained histogram generation must be immune to observations that arrive
// between the failed export and the successful re-offer. The retained point is
// a FINAL one — here the grace delete's emit of a never-exported sample — and
// the same label set observed again in between is a fresh admit whose counts
// are its own: the retained (older) point must still render the pre-failure
// distribution under its older timestamp and count, not an internally
// inconsistent mix of the two.
func TestRetainedHistogramGenerationImmutable(t *testing.T) {
	t0 := int64(1_800_100_000)
	setTimeForTest(time.Unix(t0, 0))
	defer testEpoch.Store(0)

	set, err := newTestSet([]Dynamic{{
		Name: "h_seconds", Type: HistogramType, Value: "v", Buckets: []float64{1, 10}, MaxAge: "10s",
	}})
	if err != nil {
		t.Fatal(err)
	}
	add := func(v string) {
		set.Add(valuesFrom(map[string]string{"v": v}), labelsFrom(nil), noRes(), "")
	}

	add("0.5") // cumulative counts [1, 1], total 1
	setTimeForTest(time.Unix(t0+600, 0))
	if err := set.Export(context.Background(), &failingExporter{}, 0); err == nil {
		t.Fatal("want failure") // the grace delete emitted it, final: retained
	}
	add("5") // a fresh sample of the same label set: cumulative [0, 1], total 1
	exp := &capExporter{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}

	m, ok := exp.find("h_seconds")
	if !ok || m.Type() != pmetric.MetricTypeHistogram {
		t.Fatalf("histogram not rendered: %v", ok)
	}
	dps := m.Histogram().DataPoints()
	if dps.Len() != 2 {
		t.Fatalf("points = %d, want 2 (retained generation + fresh snapshot)", dps.Len())
	}
	// mergeRetry prepends: the retained generation renders first, and must
	// carry the pre-failure distribution exactly.
	old := dps.At(0)
	if old.Count() != 1 {
		t.Fatalf("retained count = %d, want 1", old.Count())
	}
	if got := old.BucketCounts().AsRaw(); got[0] != 1 || got[1] != 0 || got[2] != 0 {
		t.Fatalf("retained buckets = %v, want [1 0 0] — the later observation leaked into the retained generation", got)
	}
	fresh := dps.At(1)
	if fresh.Count() != 1 {
		t.Fatalf("fresh count = %d, want 1", fresh.Count())
	}
	if got := fresh.BucketCounts().AsRaw(); got[0] != 0 || got[1] != 1 || got[2] != 0 {
		t.Fatalf("fresh buckets = %v, want [0 1 0]", got)
	}
	if old.StartTimestamp() >= fresh.StartTimestamp() {
		t.Fatalf("retained start %d is not before the re-admitted stream's %d", old.StartTimestamp(), fresh.StartTimestamp())
	}
}

// A failed export hands its consumed flags back to the LIVE samples
// (series.rearm) while producers keep observing into them. The re-arm takes the
// series lock like every other writer; run with -race, this is the exercise.
// Nothing observed during the outage may be lost or double counted: the
// recovery export carries every series at its full total.
func TestFailedExportRearmIsSafeAgainstConcurrentObservation(t *testing.T) {
	setTimeForTest(time.Unix(1_700_270_000, 0))
	defer testEpoch.Store(0)

	set, err := newTestSet([]Dynamic{{
		Name: "race_total", Type: CounterType, Value: "1", Match: []string{"m=1"}, Labels: []string{"id"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	const producers, perProducer = 4, 2000
	var wg sync.WaitGroup
	for g := range producers {
		wg.Go(func() {
			for range perProducer {
				set.Add(nil, labelsFrom(map[string]string{"m": "1", "id": strconv.Itoa(g)}), noRes(), "")
			}
		})
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	// Fail exports until the producers are done, then once more: the last
	// failed export reads every observation, so the re-arm has all of them.
	fails := 0
	for finished := false; !finished; {
		select {
		case <-done:
			finished = true
		default:
		}
		if err := set.Export(context.Background(), &failingExporter{}, 0); err != nil {
			fails++
		}
	}
	if fails == 0 {
		t.Fatal("no export failed; the fixture never reached the re-arm")
	}

	exp := &capExporter{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}
	_, vals := numberPoints(exp.md, "race_total")
	full := 0
	for _, v := range vals {
		switch v {
		case perProducer:
			full++
		case 0: // baseline
		default:
			t.Errorf("a delivered total of %v, want %d (or a baseline zero)", v, perProducer)
		}
	}
	if full != producers {
		t.Fatalf("series delivered at their full total = %d, want %d", full, producers)
	}
}
