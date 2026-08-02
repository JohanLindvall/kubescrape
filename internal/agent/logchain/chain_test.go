package logchain

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
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

// captureMetrics counts the data points a DynamicMetricSet exports.
type captureMetrics struct{ points int }

func (c *captureMetrics) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	c.points += md.DataPointCount()
	return nil
}
