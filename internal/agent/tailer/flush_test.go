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

	"github.com/JohanLindvall/kubescrape/internal/agent/logscrub"
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

// The tailer's logchain.Resolver must resolve metric values/labels and rule keys
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

	// A drop rule keyed on the RECORD attribute (logchain.Resolver.RuleFn's
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
		highs: map[*file]map[int]int64{f: {deadSeg: 100}},
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
// resolves, the withheld high offset must be re-offered (file.exportedHighs) —
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

// The scrubber runs before anything copies from the body: the exported
// record must carry the redacted form.
func TestScrubRedactsExportedBodies(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	scr, err := logscrub.New(logscrub.Config{Builtin: []string{"defaults"}})
	if err != nil {
		t.Fatal(err)
	}
	tl.cfg.Scrub = scr

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir,
		"2026-07-05T10:00:00Z stdout F calling with Authorization: Bearer secret.token.here done",
		"2026-07-05T10:00:01Z stdout F nothing sensitive")
	tl.scanDir(nil, false)
	driveUntil(t, ctx, tl, func() bool { return len(exp.get()) == 2 }, "records exported")

	got := exp.get()
	if !strings.Contains(got[0], "Bearer [REDACTED]") || strings.Contains(got[0], "secret.token.here") {
		t.Fatalf("body not scrubbed: %q", got[0])
	}
	if got[1] != "nothing sensitive" {
		t.Fatalf("innocuous body changed: %q", got[1])
	}
}

// A definitive rejection must not be retried forever. One sweep goroutine
// serves every file on the node, so a batch the collector will never accept
// (over a receiver's body limit, unimplemented, malformed) would otherwise pin
// the file at its committed offset and stop ALL log shipping on that node —
// until the file rotates away and the backlog is lost outright. The poisoned
// batch is dropped, counted, and everything after it still ships.
func TestPermanentRejectionDropsAndKeepsShipping(t *testing.T) {
	before := obs.LogPermanentDropped.Value()
	beforeEntries := obs.LogEntries.Value()
	beforeFailures := obs.LogExportFailures.Value()
	dir := t.TempDir()
	exp := &fakeExporter{permanentN: 1} // the first batch is rejected for good
	tl := newTestTailer(dir, "", exp)
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir, `2026-07-05T10:00:00Z stdout F poison`)
	waitFor(t, func() bool { return obs.LogPermanentDropped.Value() > before },
		"the permanently rejected batch to be dropped and counted")

	writeLines(t, filepath.Join(dir, logName), `2026-07-05T10:00:01Z stdout F after`)
	waitFor(t, func() bool {
		for _, r := range exp.get() {
			if r == "after" {
				return true
			}
		}
		return false
	}, "later lines to ship after the drop")

	// The dropped batch must not have been re-sent: its offsets advanced.
	for _, r := range exp.get() {
		if r == "poison" {
			t.Fatal("the permanently rejected record was re-exported; its offsets did not advance")
		}
	}

	// The drop is counted as a DROP, not as an export and not as a rewind:
	// "poison" never reached the collector and no file was rewound. Exactly
	// one record shipped after it ("after"), which is what LogEntries counts.
	if got := obs.LogEntries.Value() - beforeEntries; got != 1 {
		t.Errorf("kubescrape_log_entries_total moved by %v; want 1 — the dropped record was counted as exported", got)
	}
	if got := obs.LogExportFailures.Value() - beforeFailures; got != 0 {
		t.Errorf("kubescrape_log_export_failures_total moved by %v; want 0 — nothing was rewound", got)
	}
}

// A multi-line group can join across a replay that the per-sweep byte budget
// interrupted, so the entry spans a segment whose remainder was never read.
// Proposing that segment's recorded `to` committed and retired it — fd closed,
// checkpoint entry gone — over lines nobody had fed: silent loss in the one
// path that exists because nothing else can recover those bytes.
func TestTraversedSegmentsAreNotClaimedWhileUnfed(t *testing.T) {
	sg := &segment{id: 1, committed: 100, to: 5000}
	f := &file{ledger: ledger{segments: []*segment{sg}, tail: 2}}
	e := entry{file: f, start: pos{seg: 1, off: 4000}, end: pos{seg: 2, off: 80}}

	// Interrupted replay: segment 1 still owes [committed, to).
	cands := map[*file]map[int]int64{}
	proposeCandidates(cands, e)
	if off, ok := cands[f][1]; ok {
		t.Errorf("claimed segment 1 through %d while its range was still unfed; committing it retires the segment over unread lines", off)
	}
	if cands[f][2] != 80 {
		t.Errorf("the entry's own end must still commit, got %d", cands[f][2])
	}

	// Fed: every owed line is live, so the traversal genuinely covers the
	// segment and its completion is proposed.
	sg.fed = true
	cands = map[*file]map[int]int64{}
	proposeCandidates(cands, e)
	if cands[f][1] != 5000 {
		t.Errorf("a fed traversed segment must be proposed complete, got %d", cands[f][1])
	}

	// The gate must be PER-SEGMENT: the file-level segmentsFed is false for
	// the whole of a replay pass, and proposeCandidates is evaluated once at
	// flush time — so gating on it dropped this claim permanently, leaving a
	// segment nothing could ever retire.
	f.segmentsFed = false
	cands = map[*file]map[int]int64{}
	proposeCandidates(cands, e)
	if cands[f][1] != 5000 {
		t.Errorf("a fed segment was not claimed during a replay pass (segmentsFed false), got %d; nothing re-offers it, so the segment can never retire", cands[f][1])
	}
}

// Withholding is per SEGMENT: the watermark clamp lowers or deletes each
// segment's candidate independently. The memory of what was withheld must be
// per segment too.
//
// Collapsing it to one max per file discarded an OLDER segment's
// delivered-but-withheld high the moment a newer segment had one, because
// pos.less orders by segment id first. That segment's `committed` then stuck
// below its `to` for good, and retirement is the only thing that closes its fd
// and drops its checkpoint Prefix entry — so a rotated inode was pinned and
// re-persisted on every save, forever.
func TestWithheldCommitsAreRememberedPerSegment(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)

	f := &file{path: filepath.Join(dir, logName), committed: 0,
		source: &compiledSource{name: "containers", containerd: true}}
	tl.newPipeline(f)
	oldSeg := f.tail
	// An older segment with an owed range, then a fresh tail.
	f.segments = append(f.segments, &segment{id: oldSeg, committed: 10, to: 100, fed: true})
	f.newTail()
	newSeg := f.tail

	// Both segments have delivered-but-withheld highs in the same batch: the
	// clamp lowered the CANDIDATES (what commits now) below the unclamped
	// highs (what was actually delivered).
	inf := &batchInfo{
		cands: map[*file]map[int]int64{f: {oldSeg: 10, newSeg: 0}},
		highs: map[*file]map[int]int64{f: {oldSeg: 100, newSeg: 50}},
	}
	tl.advanceBatch(inf)

	if got := f.exportedHighs[oldSeg]; got != 100 {
		t.Errorf("the OLDER segment's withheld high was forgotten: exportedHighs[%d] = %d, want 100 "+
			"(it can never retire, so its fd and checkpoint entry leak)", oldSeg, got)
	}
	if got := f.exportedHighs[newSeg]; got != 50 {
		t.Errorf("the newer segment's withheld high was forgotten: exportedHighs[%d] = %d, want 50", newSeg, got)
	}
}

// A log-derived metric must land on the SAME OTLP resource as the records it
// counts. When a line lifts a RESOURCE attribute the records go under a
// resource carrying it, and the metric has to agree — every other producer in
// this repo passes the group's resource; the tailer passed the file's, which
// omits the lifted half.
//
// Two lines lifting DIFFERENT values must also produce two distinct bound
// resources, or the second silently inherits the first's.
func TestLogMetricResourceCarriesLiftedResourceAttrs(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)

	ex, err := logattrs.New(&logattrs.Config{Rules: []logattrs.Rule{
		{Key: "tenant", Attribute: "tenant.id", Target: "resource"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	tl.cfg.LogAttrs = ex

	set, err := metrics.NewDynamicMetricSet([]metrics.Dynamic{{
		Name: "lines_total", Type: metrics.CounterType, Value: "1",
	}})
	if err != nil {
		t.Fatal(err)
	}
	tl.cfg.LogMetrics = set

	stop := startTailer(t, tl)
	defer stop()
	writeLog(t, dir,
		timeNowCRI()+` stdout F {"tenant": "acme", "msg": "a"}`,
		timeNowCRI()+` stdout F {"tenant": "globex", "msg": "b"}`,
	)
	waitFor(t, func() bool { return len(exp.get()) == 2 }, "both records exported")
	waitFor(t, func() bool { return countMetric(t, set, "lines_total") == 2 }, "both lines counted")

	expm := &capMetricsExporter{}
	if err := set.Export(t.Context(), expm, 0); err != nil {
		t.Fatal(err)
	}
	// The metric's resources must carry the lifted attribute, and carry BOTH
	// values — one bound resource per group, not one per file.
	got := map[string]bool{}
	for _, md := range expm.md {
		rms := md.ResourceMetrics()
		for i := range rms.Len() {
			if v, ok := rms.At(i).Resource().Attributes().Get("tenant.id"); ok {
				got[v.Str()] = true
			}
		}
	}
	for _, want := range []string{"acme", "globex"} {
		if !got[want] {
			t.Errorf("no log-metric resource carries tenant.id=%q; the metric's resource omits the "+
				"line-lifted attributes its records were grouped by (got %v)", want, got)
		}
	}
}
