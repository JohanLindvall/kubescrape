// Tests for flushing and export (flush.go): record building, enrichment,
// grouping, log rules, log-metrics resolution and commit clamping.
package tailer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestEnrichedRecords(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	tl.cfg.Enrich = true
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir,
		`2026-07-05T10:00:00Z stdout F {"@t":"2026-01-02T03:04:05Z","level":"error","traceid":"0af7651916cd43dd8448eb211c80319c","msg":"boom"}`,
		"2026-07-05T10:00:01Z stdout F plain line",
	)
	waitFor(t, func() bool { return len(exp.get()) == 2 }, "2 log records")

	lr, ok := exp.record(0)
	if !ok {
		t.Fatal("record 0 missing")
	}
	if lr.SeverityNumber() != plog.SeverityNumberError || lr.SeverityText() != "error" {
		t.Errorf("severity = %v %q", lr.SeverityNumber(), lr.SeverityText())
	}
	if !lr.Timestamp().AsTime().Equal(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)) {
		t.Errorf("timestamp = %v; want the line's own", lr.Timestamp().AsTime())
	}
	if lr.TraceID().IsEmpty() {
		t.Error("trace id not set")
	}

	// The plain line keeps the CRI timestamp and default severity.
	lr, ok = exp.record(1)
	if !ok {
		t.Fatal("record 1 missing")
	}
	if !lr.Timestamp().AsTime().Equal(time.Date(2026, 7, 5, 10, 0, 1, 0, time.UTC)) {
		t.Errorf("plain-line timestamp = %v; want the CRI one", lr.Timestamp().AsTime())
	}
	if lr.SeverityNumber() != plog.SeverityNumberUnspecified {
		t.Errorf("plain-line severity = %v", lr.SeverityNumber())
	}
}

func TestFileAttributes(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	tl.cfg.FileAttributes = true
	stop := startTailer(t, tl)
	defer stop()

	line0 := `2026-07-05T10:00:00Z stdout F hello`
	writeLog(t, dir, line0, `2026-07-05T10:00:01Z stdout F world`)
	waitFor(t, func() bool { return len(exp.get()) == 2 }, "2 records")

	// log.file.position is the record's start: record 0 begins at 0, record 1
	// just after the first physical line (its bytes + newline).
	for i, want := range []int64{0, int64(len(line0) + 1)} {
		lr, ok := exp.record(i)
		if !ok {
			t.Fatalf("record %d missing", i)
		}
		if name, ok := lr.Attributes().Get("log.file.name"); !ok || name.Str() != logName {
			t.Errorf("record %d log.file.name = %v, want %s", i, name.AsRaw(), logName)
		}
		if pos, ok := lr.Attributes().Get("log.file.position"); !ok || pos.Int() != want {
			t.Errorf("record %d log.file.position = %v, want %d", i, pos.AsRaw(), want)
		}
	}
}

func TestLogAttrsGrouping(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	ex, err := logattrs.New(&logattrs.Config{Rules: []logattrs.Rule{
		{Key: "tenant", Attribute: "tenant.id", Target: logattrs.TargetResource},
		{Key: "req", Target: logattrs.TargetLog},
	}})
	if err != nil {
		t.Fatal(err)
	}
	tl.cfg.LogAttrs = ex
	stop := startTailer(t, tl)
	defer stop()

	// Two lines for tenant A, one for tenant B, one non-structured — the
	// tenant attribute is a resource attribute, so A and B must land in
	// separate ResourceLogs.
	writeLog(t, dir,
		`2026-07-05T10:00:00Z stdout F {"tenant":"a","req":"r1"}`,
		`2026-07-05T10:00:01Z stdout F {"tenant":"b","req":"r2"}`,
		`2026-07-05T10:00:02Z stdout F {"tenant":"a","req":"r3"}`,
		`2026-07-05T10:00:03Z stdout F plain line`,
	)
	waitFor(t, func() bool { return len(exp.get()) == 4 }, "4 records")

	exp.mu.Lock()
	tenantCounts := map[string]int{}
	for _, ld := range exp.full {
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			rl := ld.ResourceLogs().At(i)
			tenant := "<none>"
			if v, ok := rl.Resource().Attributes().Get("tenant.id"); ok {
				tenant = v.Str()
			}
			n := 0
			for j := 0; j < rl.ScopeLogs().Len(); j++ {
				n += rl.ScopeLogs().At(j).LogRecords().Len()
			}
			tenantCounts[tenant] += n
		}
	}
	exp.mu.Unlock() // record() below locks exp.mu itself
	if tenantCounts["a"] != 2 || tenantCounts["b"] != 1 || tenantCounts["<none>"] != 1 {
		t.Errorf("tenant record counts = %+v", tenantCounts)
	}
	// The log-target attribute lands on the record.
	lr, ok := exp.record(0)
	if !ok {
		t.Fatal("record 0 missing")
	}
	if v, ok := lr.Attributes().Get("req"); !ok || v.Str() != "r1" {
		t.Errorf("req attribute = %v", v.AsRaw())
	}
}

// The tailer's metricResolver must resolve metric values/labels and rule keys
// against RECORD attributes (line-derived, via logattrs) first and RESOURCE
// attributes (k8s metadata) second — the pooled resolver's per-record binding.
// The metrics package tests these semantics with fake closures; this pins the
// tailer's actual wiring.
func TestMetricResolverRecordAndResourceAttrs(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)

	// Lift the JSON key "dur" onto the log RECORD as attribute "req.ms".
	ex, err := logattrs.New(&logattrs.Config{Rules: []logattrs.Rule{
		{Key: "dur", Attribute: "req.ms"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	tl.cfg.LogAttrs = ex

	// Value from the RECORD attribute; label from a RESOURCE attribute
	// (k8s.pod.name comes from fakeMeta's metadata).
	set, err := metrics.NewDynamicMetricSet([]metrics.Dynamic{{
		Name: "req_ms_total", Type: metrics.CounterType, Value: "req.ms",
		Labels: []string{"pod=$k8s.pod.name"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	tl.cfg.LogMetrics = set

	// A drop rule keyed on the RECORD attribute (metricResolver.ruleLookup's
	// attribute arm): lines with req.ms=13 are dropped from export.
	tl.cfg.Rules = mustLineFilter(t, []logline.LineRule{
		{Action: "drop", Match: []string{"req.ms=13"}},
	})

	dropped := obs.LogRulesDropped.Value()
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir,
		timeNowCRI()+` stdout F {"dur": 40, "msg": "a"}`,
		timeNowCRI()+` stdout F {"dur": 13, "msg": "unlucky"}`,
		timeNowCRI()+` stdout F {"dur": 2, "msg": "b"}`,
	)
	waitFor(t, func() bool { return len(exp.get()) == 2 }, "2 kept records")
	waitFor(t, func() bool { return obs.LogRulesDropped.Value()-dropped == 1 }, "1 rule-dropped record")

	// Metrics saw all three lines (metrics run before rules): 40+13+2.
	waitFor(t, func() bool { return countMetric(t, set, "req_ms_total") == 55 }, "metric sum 55")

	// The label resolved from the file's RESOURCE attributes.
	expm := &capMetricsExporter{}
	if err := set.Export(t.Context(), expm, 0); err != nil {
		t.Fatal(err)
	}
	if !expm.hasLabel("req_ms_total", "pod", "pod1") {
		t.Fatal("label pod=pod1 (resource attribute) not resolved")
	}
}

// hasLabel reports whether any exported data point of the named metric carries
// the given label value.
func (c *capMetricsExporter) hasLabel(name, key, val string) bool {
	for _, md := range c.md {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					m := ms.At(k)
					if m.Name() != name {
						continue
					}
					dps := m.Sum().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						if v, ok := dps.At(l).Attributes().Get(key); ok && v.Str() == val {
							return true
						}
					}
				}
			}
		}
	}
	return false
}

func mustLineFilter(t *testing.T, rules []logline.LineRule) *logline.LineFilter {
	t.Helper()
	f, err := logline.NewLineFilter(rules)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// Rules drop matching records; offsets still advance so dropped lines are not
// re-read after a restart, and log metrics still see every line.
func TestRulesDrop(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	tl.statusEvery = 30 * time.Millisecond
	tl.cfg.Enrich = true
	tl.cfg.Rules = mustLineFilter(t, []logline.LineRule{
		{Action: "drop", Match: []string{"__severity__=debug"}},
	})
	set, err := metrics.NewDynamicMetricSet([]metrics.Dynamic{{
		Name: "lines_total", Type: metrics.CounterType, Value: "1",
		MatchRegexp: []string{"__line__=."},
	}})
	if err != nil {
		t.Fatal(err)
	}
	tl.cfg.LogMetrics = set
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir,
		timeNowCRI()+" stdout F level=debug noisy detail",
		timeNowCRI()+" stdout F level=info kept one",
		timeNowCRI()+" stdout F level=debug more noise",
		timeNowCRI()+" stdout F level=error kept two",
	)
	waitFor(t, func() bool { return len(exp.get()) == 2 }, "2 kept records")
	got := exp.get()
	if got[0] != "level=info kept one" || got[1] != "level=error kept two" {
		t.Fatalf("kept records = %q", got)
	}

	// Metrics saw all four lines, not just the kept ones.
	waitFor(t, func() bool { return countMetric(t, set, "lines_total") == 4 }, "metric count 4")

	// The dropped lines' offsets committed: the file shows no lag.
	tl2 := tl // status is published by the running tailer
	waitFor(t, func() bool {
		for _, fs := range tl2.Status() {
			if fs.Lag == 0 && fs.Committed > 0 {
				return true
			}
		}
		return false
	}, "offsets committed past dropped lines")
}

// A batch where every record is dropped exports nothing but still commits.
func TestRulesAllDropped(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	tl.statusEvery = 30 * time.Millisecond
	tl.cfg.Rules = mustLineFilter(t, []logline.LineRule{
		{Action: "drop", MatchRegexp: []string{"__line__=."}},
	})
	stop := startTailer(t, tl)
	defer stop()

	rateLines(t, dir, 0, 5)
	waitFor(t, func() bool {
		for _, fs := range tl.Status() {
			if fs.Committed > 0 && fs.Lag == 0 {
				return true
			}
		}
		return false
	}, "offsets committed with everything dropped")
	if n := len(exp.get()); n != 0 {
		t.Fatalf("exported %d records, want 0", n)
	}
}

// Sampling keeps a deterministic fraction of matching lines.
func TestRulesSample(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	tl.cfg.Rules = mustLineFilter(t, []logline.LineRule{
		{Action: "keep", MatchRegexp: []string{"__line__=chatty"}, Sample: 0.5},
	})
	stop := startTailer(t, tl)
	defer stop()

	lines := make([]string, 0, 21)
	for i := 0; i < 20; i++ {
		lines = append(lines, fmt.Sprintf("%s stdout F chatty %02d", timeNowCRI(), i))
	}
	lines = append(lines, timeNowCRI()+" stdout F normal line")
	writeLog(t, dir, lines...)

	waitFor(t, func() bool { return len(exp.get()) == 11 }, "10 sampled + 1 unmatched")
	got := exp.get()
	if got[len(got)-1] != "normal line" {
		t.Fatalf("unmatched line missing: %q", got)
	}
}

// countMetric renders the set and returns the total of a counter.
func countMetric(t *testing.T, set *metrics.DynamicMetricSet, name string) float64 {
	t.Helper()
	exp := &capMetricsExporter{}
	if err := set.Export(t.Context(), exp, 0); err != nil {
		t.Fatal(err)
	}
	return exp.total(name)
}

// capMetricsExporter captures exported metrics for countMetric. Payloads are
// deep-copied: Export reuses and clears its payload after each ExportMetrics
// call (the real client has marshaled it by then).
type capMetricsExporter struct{ md []pmetric.Metrics }

func (c *capMetricsExporter) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	cp := pmetric.NewMetrics()
	md.CopyTo(cp)
	c.md = append(c.md, cp)
	return nil
}

func (c *capMetricsExporter) total(name string) float64 {
	var sum float64
	for _, md := range c.md {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					if ms.At(k).Name() != name || ms.At(k).Type() != pmetric.MetricTypeSum {
						continue
					}
					dps := ms.At(k).Sum().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						sum += dps.At(d).DoubleValue()
					}
				}
			}
		}
	}
	return sum
}

// A flush whose records carry line-derived RESOURCE attributes (several
// ResourceLogs per file) and which FAILS: every group must rewind together and
// be re-shipped — the grouping must not change the offset accounting.
func TestLogAttrsGroupsRewindOnFailedFlush(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	ex, err := logattrs.New(&logattrs.Config{Rules: []logattrs.Rule{
		{Key: "tenant", Attribute: "tenant.id", Target: logattrs.TargetResource},
	}})
	if err != nil {
		t.Fatal(err)
	}
	tl.cfg.LogAttrs = ex

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir,
		`2026-07-05T10:00:00Z stdout F {"tenant":"a","msg":"ra1"}`,
		`2026-07-05T10:00:01Z stdout F {"tenant":"b","msg":"rb1"}`,
		`2026-07-05T10:00:02Z stdout F {"tenant":"a","msg":"ra2"}`,
	)
	tl.scanDir(nil, false)

	exp.fail = 3 // the first flush fails: all three groups must rewind
	tl.sweep(ctx, true)
	tl.flush(ctx)

	for i := 0; i < 3; i++ {
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}
	got := exp.get()
	for _, want := range []string{"ra1", "rb1", "ra2"} {
		found := false
		for _, g := range got {
			if strings.Contains(g, want) {
				found = true
			}
		}
		if !found {
			t.Fatalf("AT-LEAST-ONCE VIOLATED: %q lost after a failed flush of attribute-split groups; exported = %v", want, got)
		}
	}
}

// A candidate naming a DEAD segment id (a truncated-away incarnation) must
// resolve to nothing: neither the tail checkpoint nor any live segment may
// move. The segment-qualified position IS the staleness check that the old
// rotation-generation protocol provided.
func TestDeadSegmentCandidateCommitsNothing(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)

	f := &file{path: filepath.Join(dir, logName), committed: 7,
		source: &compiledSource{name: "containers", containerd: true}}
	f.readPos = 42
	tl.newPipeline(f) // issues tail id 1
	deadSeg := f.tail
	f.newTail() // the old incarnation's id is now dead

	inf := &batchInfo{
		cands: map[*file]map[int]int64{f: {deadSeg: 100}},
		highs: map[*file]pos{f: {seg: deadSeg, off: 100}},
	}
	tl.commitBatch(inf)
	if f.committed != 7 {
		t.Fatalf("dead-segment candidate applied to the tail: committed = %d, want 7", f.committed)
	}
	if len(f.segments) != 0 {
		t.Fatalf("dead-segment candidate materialized a segment: %v", f.segments)
	}

	// The old gen-checked pipelined model SKIPPED the rewind of a stale-gen
	// file at apply time (a rotation might have reset its offsets in between).
	// Synchronous flush removed that interleaving, so failBatch now rewinds
	// EVERY batched file unconditionally — including one whose only candidate
	// names a dead segment: rewind is idempotent (readPos back to committed)
	// and cannot corrupt the already-restarted offsets.
	f.readPos = 99 // pretend read-ahead past committed
	tl.failBatch(inf, errors.New("boom"))
	if f.readPos != f.committed {
		t.Fatalf("failBatch did not rewind the dead-segment file: readPos=%d committed=%d", f.readPos, f.committed)
	}
}

// A record exported while ANOTHER stream's multi-line group is still buffered
// has its commit withheld by the build-time watermark clamp. Once the group
// resolves, the withheld high offset must be re-offered (file.exportedHigh) —
// without it, committed freezes below readPos FOREVER: the high entry belongs
// to an earlier batch no later maxOffsets ever sees, so a restart re-reads
// the tail (duplicates), idle-close can never release the fd, and the lag
// gauges show phantom backlog.
func TestWithheldCommitReleasedOnceGroupResolves(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveMultilineTailer(dir, exp)

	tl.scanDir(tl.loadCheckpoints(), true)
	start, rest := panicLines()
	writeLog(t, dir, append(start, rest...)...)
	tl.scanDir(nil, false)

	deadline := time.Now().Add(5 * time.Second)
	path := filepath.Join(dir, logName)
	for time.Now().Before(deadline) {
		tl.sweep(ctx, true)
		tl.flush(ctx)
		f := tl.files[path]
		st, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if f != nil && f.committed == st.Size() {
			return // everything read is either exported or accounted; no freeze
		}
		time.Sleep(20 * time.Millisecond)
	}
	f := tl.files[path]
	t.Fatalf("checkpoint frozen below file size: committed=%d readPos=%d (withheld high never re-offered)",
		f.committed, f.readPos)
}
