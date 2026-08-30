package logchain

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/enrich"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
)

// testProducer is a minimal Producer: one group, created lazily so the tests
// can see whether Dest was consulted for a dropped record.
type testProducer struct {
	ld    plog.Logs
	sl    plog.ScopeLogs
	built bool
	// severity is stamped onto the record, standing in for whatever the real
	// producers put there (journald's PRIORITY, the events reader's Type).
	severity string
	body     string
	// ts stands in for the producer's own timestamp (the CRI/journal ingest
	// time), which is what a zone-less timestamp parsed off the line loses to.
	ts pcommon.Timestamp
	// stamped counts Stamp calls, to pin that a record is stamped exactly once.
	stamped int
}

func newTestProducer() *testProducer {
	return &testProducer{ld: plog.NewLogs()}
}

func (p *testProducer) Dest() plog.LogRecordSlice {
	if !p.built {
		p.sl = p.ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
		p.built = true
	}
	return p.sl.LogRecords()
}

func (p *testProducer) Stamp(lr plog.LogRecord) {
	p.stamped++
	lr.Body().SetStr(p.body)
	if p.severity != "" {
		lr.SetSeverityText(p.severity)
	}
	if p.ts != 0 {
		lr.SetTimestamp(p.ts)
	}
}

func (p *testProducer) records() int {
	n := 0
	rls := p.ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		sls := rls.At(i).ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			n += sls.At(j).LogRecords().Len()
		}
	}
	return n
}

func emptyRes() pcommon.Map { return pcommon.NewMap() }

func dropFilter(t *testing.T, rules []logline.LineRule) *logline.LineFilter {
	t.Helper()
	f, err := logline.NewLineFilter(rules)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// A dropped record must not appear in the payload AND must never have caused a
// group to be created: the tailer's grouper builds a ResourceLogs on demand, so
// consulting Dest for a record the rules reject would emit a record-less
// resource for every all-dropped file.
func TestDroppedRecordNeverTouchesDest(t *testing.T) {
	c := NewChain[string](Config{
		Rules: dropFilter(t, []logline.LineRule{{Action: "drop", Match: []string{"__severity__=debug"}}}),
	}, false)
	p := newTestProducer()
	p.severity, p.body = "DEBUG", "a debug line"

	if kept := c.Emit(p, Input[string]{Body: p.body, Resource: emptyRes(), BoundKey: "k"}); kept {
		t.Fatal("Emit kept a record the rules drop")
	}
	if p.built {
		t.Error("a dropped record materialised a resource group")
	}
	if p.records() != 0 {
		t.Errorf("payload carries %d records after a drop", p.records())
	}
	if p.stamped != 1 {
		t.Errorf("Stamp called %d times, want exactly 1 (a dropped record is still built, in the scratch)", p.stamped)
	}

	// A kept record lands, and the scratch is clean for it — a scratch left
	// holding the dropped record would move BOTH across.
	p.severity, p.body = "ERROR", "an error line"
	if kept := c.Emit(p, Input[string]{Body: p.body, Resource: emptyRes(), BoundKey: "k"}); !kept {
		t.Fatal("Emit dropped a record the rules keep")
	}
	if p.records() != 1 {
		t.Fatalf("payload carries %d records, want exactly 1 (the drop must not ride along)", p.records())
	}
	if got := p.sl.LogRecords().At(0).Body().Str(); got != "an error line" {
		t.Errorf("record body = %q, want the kept line", got)
	}
}

// Log metrics observe EVERY record, including one the rules then drop: a metric
// counting errors must not fall to zero because someone stopped SHIPPING the
// error lines. All four producers had this ordering and it is now in one place.
func TestMetricsObserveDroppedRecords(t *testing.T) {
	set, err := metrics.NewDynamicMetricSet([]metrics.Dynamic{{
		Name: "lines_total", Type: metrics.CounterType, Value: "1",
	}})
	if err != nil {
		t.Fatal(err)
	}
	c := NewChain[string](Config{
		LogMetrics: set,
		Rules:      dropFilter(t, []logline.LineRule{{Action: "drop", Match: []string{"__severity__=debug"}}}),
	}, false)
	p := newTestProducer()
	p.severity, p.body = "DEBUG", "a debug line"
	if c.Emit(p, Input[string]{Body: p.body, Resource: emptyRes(), BoundKey: "k"}) {
		t.Fatal("Emit kept a dropped record")
	}
	var exp captureMetrics
	if err := set.Export(context.Background(), &exp, 0); err != nil {
		t.Fatal(err)
	}
	if exp.points == 0 {
		t.Fatal("a dropped record was not observed by log metrics: rules run AFTER metrics for exactly this reason")
	}
}

// Rules run AFTER enrichment, so __severity__ selects on the severity ENRICH
// derived from the line rather than whatever the producer stamped. The tailer,
// journald, the events reader and the Azure reader all relied on this order and
// each implemented it separately.
func TestRulesSeeTheEnrichedSeverity(t *testing.T) {
	c := NewChain[string](Config{
		Enrich: true,
		Rules:  dropFilter(t, []logline.LineRule{{Action: "drop", Match: []string{"__severity__=error"}}}),
	}, false)
	p := newTestProducer()
	// The producer stamps nothing; the LINE says it is an error.
	p.body = `{"level":"error","msg":"boom"}`
	if c.Emit(p, Input[string]{Body: p.body, Resource: emptyRes(), BoundKey: "k"}) {
		t.Fatal("a line whose ENRICHED severity is error was kept: the rules ran before enrichment")
	}
}

// Per-record rules (the tailer's pod annotations) run BEFORE the global chain:
// a pod drop is final, a pod keep still passes the global rules. This existed
// only in the tailer's copy of the loop.
func TestPodRulesRunBeforeTheGlobalChain(t *testing.T) {
	global := dropFilter(t, []logline.LineRule{{Action: "drop", MatchRegexp: []string{"__line__=noisy"}}})
	podDrop := dropFilter(t, []logline.LineRule{{Action: "drop", MatchRegexp: []string{"__line__=secret"}}})
	podKeep := dropFilter(t, []logline.LineRule{{Action: "keep", MatchRegexp: []string{"__line__=secret"}}})

	c := NewChain[string](Config{Rules: global}, false)
	p := newTestProducer()

	p.body = "a secret line"
	if c.Emit(p, Input[string]{Body: p.body, Resource: emptyRes(), BoundKey: "k", PodRules: podDrop}) {
		t.Error("a pod drop was not final")
	}
	// A pod KEEP still has to pass the global chain.
	p.body = "a secret noisy line"
	if c.Emit(p, Input[string]{Body: p.body, Resource: emptyRes(), BoundKey: "k", PodRules: podKeep}) {
		t.Error("a pod keep bypassed the global rules")
	}
	p.body = "a secret quiet line"
	if !c.Emit(p, Input[string]{Body: p.body, Resource: emptyRes(), BoundKey: "k", PodRules: podKeep}) {
		t.Error("a pod keep that also passes the global rules was dropped")
	}
}

// With NO rules configured anywhere, a record is built straight into the
// producer's destination — no scratch, no move.
func TestWithoutRulesRecordsAreBuiltInPlace(t *testing.T) {
	c := NewChain[string](Config{}, false)
	p := newTestProducer()
	p.body = "a line"
	if !c.Emit(p, Input[string]{Body: p.body, Resource: emptyRes(), BoundKey: "k"}) {
		t.Fatal("Emit dropped a record with no rules configured")
	}
	if p.records() != 1 {
		t.Fatalf("payload carries %d records, want 1", p.records())
	}
}

// A line-lifted RESOURCE attribute ranks between the record's attributes and
// the resource's for rule keys. The tailer resolves against the FILE's base
// resource while the lifted attributes are still a pending slice, so without
// this the same logAttributes + logs.rules config selected differently
// depending on which pipeline carried the line — the reason SetLifted exists,
// and the reason it must be in the shared chain rather than one producer's copy
// of the loop.
func TestLiftedResourceAttributesAreVisibleToRules(t *testing.T) {
	c := NewChain[string](Config{
		Rules: dropFilter(t, []logline.LineRule{{Action: "drop", Match: []string{"tenant=internal"}}}),
	}, false)
	p := newTestProducer()
	p.body = "a line"
	lifted := logattrs.Result{Resource: []logattrs.Attr{{Key: "tenant", Val: "internal"}}}
	if c.Emit(p, Input[string]{Body: p.body, Lifted: lifted, Resource: emptyRes(), BoundKey: "k"}) {
		t.Error("a rule keyed on a line-lifted RESOURCE attribute did not match it")
	}
	other := logattrs.Result{Resource: []logattrs.Attr{{Key: "tenant", Val: "customer"}}}
	if !c.Emit(p, Input[string]{Body: p.body, Lifted: other, Resource: emptyRes(), BoundKey: "k"}) {
		t.Error("a record whose lifted attribute does not match the drop rule was dropped")
	}
}

// Prune removes the groups an all-dropped batch leaves behind. Producers that
// must build their group before the record (their metric and rule resolution
// reads the group's own resource) rely on it; the payload must never carry a
// record-less ResourceLogs.
func TestPruneRemovesEmptyGroups(t *testing.T) {
	ld := plog.NewLogs()
	empty := ld.ResourceLogs().AppendEmpty()
	empty.ScopeLogs().AppendEmpty()
	full := ld.ResourceLogs().AppendEmpty()
	full.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("x")

	Prune(ld)
	if ld.ResourceLogs().Len() != 1 {
		t.Fatalf("ResourceLogs = %d, want 1", ld.ResourceLogs().Len())
	}
	if got := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str(); got != "x" {
		t.Errorf("the surviving record is %q", got)
	}
}

// The enrichment counters are per-RECORD outcomes, so Input.Observed has to
// reach them too — they live a package away in logenrich, below the gate that
// already covers LogMetrics.Add and LogRulesDropped, and the chain is the only
// place that knows a pass is a re-read.
//
// It matters because sum(kubescrape_log_enriched_total) is read as
// kubescrape_log_entries_total decomposed by parse format: the two agree to the
// record whenever nothing is failing, and a rewind counts passes in the
// numerator against deliveries in the denominator. Measured on a live cluster
// whose collector was scaled to zero for three minutes: 32,245 enrichments and
// 31,324 timestamp refusals for 277 delivered records.
//
// What must NOT change is the record: suppression is counting-only, so the
// re-read is enriched exactly as the first pass was.
func TestEnrichmentCountersSkipAnObservedRecord(t *testing.T) {
	c := NewChain[string](Config{Enrich: true}, false)
	p := newTestProducer()
	// A zone-less timestamp on the line, and a producer timestamp for it to
	// lose to — which is the refusal LogEnrichTimeRejected counts.
	ingest := time.Date(2026, 1, 2, 8, 4, 5, 0, time.UTC)
	p.body = "2026-01-02 03:04:05 INFO handled request"
	p.ts = pcommon.NewTimestampFromTime(ingest)

	enriched := obs.LogEnriched.WithLabelValues(enrich.FormatPattern).Value()
	rejected := obs.LogEnrichTimeRejected.Value()

	in := Input[string]{Body: p.body, Resource: emptyRes(), BoundKey: "k"}
	if !c.Emit(p, in) {
		t.Fatal("Emit dropped the first pass's record")
	}
	// The same bytes again, as a rewind hands them back.
	in.Observed = true
	if !c.Emit(p, in) {
		t.Fatal("Emit dropped the re-read record")
	}

	if got := obs.LogEnriched.WithLabelValues(enrich.FormatPattern).Value() - enriched; got != 1 {
		t.Errorf("kubescrape_log_enriched_total{format=pattern} moved by %v for one record read twice, want 1: "+
			"a re-read multiplies the format decomposition of kubescrape_log_entries_total by the number of "+
			"attempts an outage spans", got)
	}
	if got := obs.LogEnrichTimeRejected.Value() - rejected; got != 1 {
		t.Errorf("kubescrape_log_enrich_time_rejected_total moved by %v for one record read twice, want 1", got)
	}

	// Both records exist and both were enriched: the producer's timestamp
	// survived the zone-less parse, and the line's level reached the severity.
	if p.records() != 2 {
		t.Fatalf("payload carries %d records, want 2", p.records())
	}
	recs := p.sl.LogRecords()
	for i := 0; i < recs.Len(); i++ {
		lr := recs.At(i)
		if got := lr.Timestamp().AsTime(); !got.Equal(ingest) {
			t.Errorf("record %d: timestamp = %v, want the producer's %v", i, got, ingest)
		}
		if lr.SeverityNumber() != plog.SeverityNumberInfo {
			t.Errorf("record %d: severity = %v, want info — suppressing the COUNT must not suppress the enrichment",
				i, lr.SeverityNumber())
		}
	}
}

// captureMetrics counts the data points a DynamicMetricSet exports.
type captureMetrics struct{ points int }

func (c *captureMetrics) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	c.points += md.DataPointCount()
	return nil
}

// The perRecordRules argument is a PROMISE — "no record in this batch carries
// its own rules" — kept by an ordering argument a package away (the tailer's
// anyPodRules pass). Emit upgrades rather than panicking when it is broken,
// which is right, and until now was also silent: the chain would keep working,
// paying an allocation per flush, with nothing anywhere saying the pre-pass had
// drifted from what the producer hands over. A branch nobody can observe
// taking is a fix that regresses unnoticed.
func TestBrokenPerRecordRulesPromiseIsReported(t *testing.T) {
	var logged bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	promiseWarn = logdedupe.Throttle{}

	// perRecordRules=false, and no global rules — yet a record arrives with
	// its own. That is exactly the drift.
	c := NewChain[string](Config{}, false)
	p := newTestProducer()
	p.body = "a line"
	podKeep := dropFilter(t, []logline.LineRule{{Action: "keep", Match: []string{"__line__=~.*"}}})
	if !c.Emit(p, Input[string]{Body: p.body, Resource: emptyRes(), BoundKey: "k", PodRules: podKeep}) {
		t.Fatal("the upgraded chain dropped a record its pod rules kept")
	}
	if p.records() != 1 {
		t.Fatalf("payload carries %d records, want 1 — the upgrade must not lose the record", p.records())
	}
	if out := logged.String(); !strings.Contains(out, "no record would carry its own rules") {
		t.Fatalf("the broken promise was not reported:\n%s", out)
	}

	// Throttled: "once per flush" is every ten seconds on every node.
	before := strings.Count(logged.String(), "no record would carry its own rules")
	for range 5 {
		c2 := NewChain[string](Config{}, false)
		c2.Emit(newTestProducer(), Input[string]{Body: "x", Resource: emptyRes(), BoundKey: "k", PodRules: podKeep})
	}
	if got := strings.Count(logged.String(), "no record would carry its own rules"); got != before {
		t.Fatalf("the drift was reported %d times inside one window, want %d", got, before)
	}
}
