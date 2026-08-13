package azurediag

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/logscrub"
	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
)

// captureExporter records exported payloads; failN fails the first N sends
// of each signal with err.
type captureExporter struct {
	mu       sync.Mutex
	logs     []plog.Logs
	metrics  []pmetric.Metrics
	failLogs int
	failMet  int
	err      error
}

func (c *captureExporter) ExportLogs(_ context.Context, ld plog.Logs) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failLogs > 0 {
		c.failLogs--
		return c.err
	}
	out := plog.NewLogs()
	ld.CopyTo(out)
	c.logs = append(c.logs, out)
	return nil
}

func (c *captureExporter) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failMet > 0 {
		c.failMet--
		return c.err
	}
	out := pmetric.NewMetrics()
	md.CopyTo(out)
	c.metrics = append(c.metrics, out)
	return nil
}

func (c *captureExporter) records() []plog.LogRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []plog.LogRecord
	for _, ld := range c.logs {
		rls := ld.ResourceLogs()
		for i := 0; i < rls.Len(); i++ {
			sls := rls.At(i).ScopeLogs()
			for j := 0; j < sls.Len(); j++ {
				lrs := sls.At(j).LogRecords()
				for k := 0; k < lrs.Len(); k++ {
					out = append(out, lrs.At(k))
				}
			}
		}
	}
	return out
}

// fakeSource replays canned polls, blocks when exhausted, and signals each
// commit so tests are deterministic (no sleeps — the injectable-clock
// discipline, applied to sequencing).
type fakeSource struct {
	polls     [][][]byte
	polled    int
	commits   int
	committed chan struct{}
	closed    bool
}

func newFakeSource(polls ...[][]byte) *fakeSource {
	return &fakeSource{polls: polls, committed: make(chan struct{}, 16)}
}

func (f *fakeSource) poll(ctx context.Context) ([][]byte, bool, error) {
	if len(f.polls) == 0 {
		<-ctx.Done()
		return nil, false, ctx.Err()
	}
	p := f.polls[0]
	f.polls = f.polls[1:]
	f.polled++
	// A nil poll stands for an errors-only fetch: no records, no error, and
	// not healthy — see pollResult.
	return p, p != nil, nil
}

func (f *fakeSource) commit(context.Context) error {
	f.commits++
	f.committed <- struct{}{}
	return nil
}
func (f *fakeSource) close() { f.closed = true }

// runUntilCommit runs the reader until the source commits (or times out),
// then cancels and waits for Run to return.
func runUntilCommit(t *testing.T, r *Reader, src *fakeSource) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	select {
	case <-src.committed:
	case <-time.After(5 * time.Second):
		t.Error("timed out waiting for a commit")
	}
	cancel()
	<-done
}

func newTestReader(cfg Config, src source) *Reader {
	r := New(cfg)
	r.open = func() (source, error) { return src, nil }
	return r
}

const armID = "/SUBSCRIPTIONS/6B2B3E76-0000-0000-0000-000000000000/RESOURCEGROUPS/MYRG/PROVIDERS/MICROSOFT.SQL/SERVERS/MYSRV/DATABASES/MYDB"

const logEnvelope = `{"records":[
  {"time":"2026-07-28T10:00:00.0000000Z","resourceId":"` + armID + `","category":"SQLSecurityAuditEvents","level":"Warning","operationName":"AuditEvent","resultType":"Failed","properties":{"statement":"select 1","user":"app"}},
  {"time":"2026-07-28T10:00:01.0000000Z","resourceId":"` + armID + `","category":"SQLSecurityAuditEvents","level":"Informational","operationName":"AuditEvent","properties":{"statement":"select 2"}}
]}`

const metricEnvelope = `{"records":[
  {"count":4,"total":40,"minimum":1,"maximum":19,"average":10,"resourceId":"` + armID + `","time":"2026-07-28T10:01:00Z","metricName":"cpu_percent","timeGrain":"PT1M"},
  {"count":2,"total":6,"minimum":2,"maximum":4,"average":3,"resourceId":"` + armID + `","time":"2026-07-28T10:02:00Z","metricName":"cpu_percent","timeGrain":"PT1M"}
]}`

// collectEnvelope adapts the callback form for tests.
func collectEnvelope(msg []byte) ([][]byte, error) {
	var out [][]byte
	err := splitEnvelope(msg, func(raw []byte) error {
		out = append(out, raw)
		return nil
	})
	return out, err
}

func TestSplitEnvelope(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int
	}{
		{`{"records":[{"a":1},{"b":2}]}`, 2},
		{`[{"a":1},{"b":2},{"c":3}]`, 3},
		{`{"time":"x","category":"y"}`, 1}, // bare single record
		{`{"records":null}`, 1},            // no records ARRAY: the object is the record
		{`{"records":[]}`, 0},
		{`{"records":[{"s":"tricky \" ]} string","n":[1,[2,3]]},{"b":2}]}`, 2},
		{"  \n[ {\"a\":1} , {\"b\":2} ]", 2},
	} {
		got, err := collectEnvelope([]byte(tc.in))
		if err != nil {
			t.Fatalf("%s: %v", tc.in, err)
		}
		if len(got) != tc.want {
			t.Fatalf("%s: records = %d, want %d (%q)", tc.in, len(got), tc.want, got)
		}
	}
	// Strict since the move to lightning's ArrayEach: malformed syntax is an
	// error, never elements silently fused (the old splitter accepted a
	// missing comma and yielded two objects as one garbage element).
	for _, bad := range []string{``, `garbage`, `[{"a":1}`, `[{"a":1} {"b":2}]`, `{"records":[1,]}`} {
		if _, err := collectEnvelope([]byte(bad)); err == nil {
			t.Fatalf("%q: want an error", bad)
		}
	}
}

func TestDecodeClassifiesRecords(t *testing.T) {
	raws, err := collectEnvelope([]byte(metricEnvelope))
	if err != nil {
		t.Fatal(err)
	}
	rec, _, err := decodeRecord(raws[0], nil)
	if err != nil {
		t.Fatal(err)
	}
	if !rec.metric || rec.metricName != "cpu_percent" || rec.timeGrain != "PT1M" {
		t.Fatalf("metric record misdecoded: %+v", rec)
	}
	if !rec.has[aggAverage] || rec.aggs[aggAverage] != 10 || rec.aggs[aggTotal] != 40 {
		t.Fatalf("aggregations misdecoded: %+v", rec)
	}
	if rec.ts.UTC().Format(time.RFC3339) != "2026-07-28T10:01:00Z" {
		t.Fatalf("time misdecoded: %v", rec.ts)
	}

	raws, _ = collectEnvelope([]byte(logEnvelope))
	rec, _, err = decodeRecord(raws[0], nil)
	if err != nil {
		t.Fatal(err)
	}
	if rec.metric {
		t.Fatal("log record classified as metric")
	}
	if rec.category != "SQLSecurityAuditEvents" || rec.level != "Warning" || rec.resultType != "Failed" {
		t.Fatalf("log fields misdecoded: %+v", rec)
	}
	// A log with a "count" field but no metricName stays a log.
	rec, _, err = decodeRecord([]byte(`{"category":"c","count":3}`), nil)
	if err != nil || rec.metric {
		t.Fatalf("count-bearing log misclassified (err=%v metric=%v)", err, rec.metric)
	}
}

func TestParseResourceID(t *testing.T) {
	arm := parseResourceID(armID)
	if arm.Subscription != "6b2b3e76-0000-0000-0000-000000000000" {
		t.Fatalf("subscription = %q", arm.Subscription)
	}
	if arm.ResourceGroup != "myrg" {
		t.Fatalf("rg = %q", arm.ResourceGroup)
	}
	if arm.Type != "microsoft.sql/servers/databases" {
		t.Fatalf("type = %q", arm.Type)
	}
	if arm.Name != "mydb" {
		t.Fatalf("name = %q", arm.Name)
	}
	// Nested provider: the leaf provider owns the resource.
	nested := parseResourceID("/subscriptions/S/resourceGroups/RG/providers/Microsoft.Web/sites/s1/providers/Microsoft.Insights/components/c1")
	if nested.Type != "microsoft.insights/components" || nested.Name != "c1" {
		t.Fatalf("nested = %+v", nested)
	}
	// Malformed IDs degrade to empty fields, never panic.
	if got := parseResourceID(""); got != (armResource{}) {
		t.Fatalf("empty id = %+v", got)
	}
}

func TestLogConversion(t *testing.T) {
	exp := &captureExporter{}
	src := newFakeSource([][]byte{[]byte(logEnvelope)})
	r := newTestReader(Config{Exporter: exp, Enrich: true}, src)
	runUntilCommit(t, r, src)

	recs := exp.records()
	if len(recs) != 2 {
		t.Fatalf("records = %d, want 2", len(recs))
	}
	if recs[0].SeverityNumber() != plog.SeverityNumberWarn {
		t.Fatalf("severity = %v, want WARN", recs[0].SeverityNumber())
	}
	if !strings.Contains(recs[0].Body().Str(), `"statement":"select 1"`) {
		t.Fatalf("body is not the verbatim record: %q", recs[0].Body().Str())
	}
	for k, want := range map[string]string{
		"azure.category":       "SQLSecurityAuditEvents",
		"azure.operation.name": "AuditEvent",
		"azure.result.type":    "Failed",
	} {
		if v, ok := recs[0].Attributes().Get(k); !ok || v.Str() != want {
			t.Errorf("attr %s = %q, want %q", k, v.AsString(), want)
		}
	}
	// The ARM resource is the OTLP resource.
	res := exp.logs[0].ResourceLogs().At(0).Resource().Attributes()
	for k, want := range map[string]string{
		"cloud.provider":            "azure",
		"cloud.resource_id":         armID,
		"cloud.account.id":          "6b2b3e76-0000-0000-0000-000000000000",
		"azure.resource_group.name": "myrg",
		"azure.resource.type":       "microsoft.sql/servers/databases",
		"azure.resource.name":       "mydb",
		"service.name":              "mydb",
		"service.namespace":         "myrg",
	} {
		if v, ok := res.Get(k); !ok || v.Str() != want {
			t.Errorf("resource %s = %q (present=%v), want %q", k, v.AsString(), ok, want)
		}
	}
	if src.commits != 1 {
		t.Fatalf("commits = %d, want 1", src.commits)
	}
}

func TestMetricConversion(t *testing.T) {
	exp := &captureExporter{}
	src := newFakeSource([][]byte{[]byte(metricEnvelope)})
	r := newTestReader(Config{Exporter: exp}, src)
	runUntilCommit(t, r, src)

	if len(exp.metrics) != 1 {
		t.Fatalf("metric payloads = %d, want 1", len(exp.metrics))
	}
	md := exp.metrics[0]
	if md.ResourceMetrics().Len() != 1 {
		t.Fatalf("resources = %d, want 1 (same ARM resource)", md.ResourceMetrics().Len())
	}
	sm := md.ResourceMetrics().At(0).ScopeMetrics().At(0)
	byName := map[string]pmetric.Metric{}
	for i := 0; i < sm.Metrics().Len(); i++ {
		m := sm.Metrics().At(i)
		byName[m.Name()] = m
	}
	if len(byName) != 5 {
		t.Fatalf("metrics = %d (%v), want 5 aggregations", len(byName), byName)
	}
	avg, ok := byName["azure.cpu_percent.average"]
	if !ok {
		t.Fatalf("missing azure.cpu_percent.average; have %v", byName)
	}
	if avg.Type() != pmetric.MetricTypeGauge {
		t.Fatalf("type = %v, want gauge", avg.Type())
	}
	dps := avg.Gauge().DataPoints()
	if dps.Len() != 2 {
		t.Fatalf("datapoints = %d, want 2 (both windows)", dps.Len())
	}
	if dps.At(0).DoubleValue() != 10 || dps.At(1).DoubleValue() != 3 {
		t.Fatalf("values = %v, %v", dps.At(0).DoubleValue(), dps.At(1).DoubleValue())
	}
	if v, ok := dps.At(0).Attributes().Get("azure.metric.timegrain"); !ok || v.Str() != "PT1M" {
		t.Fatalf("timegrain attr missing/wrong: %v", v.AsString())
	}
	res := md.ResourceMetrics().At(0).Resource().Attributes()
	if v, ok := res.Get("service.instance.id"); !ok || v.Str() != strings.ToLower(armID) {
		t.Fatalf("service.instance.id = %q", v.AsString())
	}
}

// A transient export failure must NOT commit: the poll is retried in place
// and the offsets stay where the collector last acknowledged.
func TestCommitOnlyAfterAck(t *testing.T) {
	exp := &captureExporter{failLogs: 1, err: errors.New("collector down")}
	src := newFakeSource([][]byte{[]byte(logEnvelope)})
	r := newTestReader(Config{Exporter: exp, RetryBackoff: time.Millisecond}, src)
	runUntilCommit(t, r, src)

	if len(exp.logs) != 1 {
		t.Fatalf("logs delivered = %d, want 1 (retried past the failure)", len(exp.logs))
	}
	if src.commits != 1 {
		t.Fatalf("commits = %d, want exactly 1, after the ack", src.commits)
	}
}

// A permanent rejection drops the payload and commits past it: one poison
// batch must not wedge the partition forever.
func TestPermanentRejectionSkipsPast(t *testing.T) {
	exp := &captureExporter{failLogs: 99, err: &otlpexport.HTTPStatusError{Code: 400}}
	src := newFakeSource([][]byte{[]byte(logEnvelope)})
	r := newTestReader(Config{Exporter: exp, RetryBackoff: time.Millisecond}, src)
	runUntilCommit(t, r, src)

	if len(exp.logs) != 0 {
		t.Fatalf("logs delivered = %d, want 0 (rejected)", len(exp.logs))
	}
	if src.commits != 1 {
		t.Fatalf("commits = %d, want 1 (skip past the poison)", src.commits)
	}
}

// Undecodable messages are counted and committed past — poison data must not
// wedge the partition.
func TestUndecodableMessageCommitsPast(t *testing.T) {
	exp := &captureExporter{}
	src := newFakeSource([][]byte{[]byte("not json at all")})
	r := newTestReader(Config{Exporter: exp}, src)
	runUntilCommit(t, r, src)

	if len(exp.logs) != 0 || len(exp.metrics) != 0 {
		t.Fatal("nothing should have been exported")
	}
	if src.commits != 1 {
		t.Fatalf("commits = %d, want 1", src.commits)
	}
}

func TestRulesDropAzureLogs(t *testing.T) {
	rules, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "drop", Match: []string{"__severity__=info"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	exp := &captureExporter{}
	src := newFakeSource([][]byte{[]byte(logEnvelope)})
	r := newTestReader(Config{Exporter: exp, Rules: rules}, src)
	runUntilCommit(t, r, src)

	recs := exp.records()
	if len(recs) != 1 {
		t.Fatalf("records = %d, want only the Warning kept", len(recs))
	}
	if recs[0].SeverityNumber() != plog.SeverityNumberWarn {
		t.Fatalf("kept record severity = %v", recs[0].SeverityNumber())
	}
	// Dropped records still commit — the batch is settled.
	if src.commits != 1 {
		t.Fatalf("commits = %d, want 1", src.commits)
	}
}

func TestReadyAfterFirstPoll(t *testing.T) {
	ready := make(chan struct{})
	src := newFakeSource([][]byte{}) // one empty poll, then blocks
	r := newTestReader(Config{Exporter: &captureExporter{}, Ready: func() { close(ready) }}, src)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("ready was not signalled after the first poll")
	}
}

// The readiness half of the scoped-fetch-error classification: an errors-only
// fetch no longer fails the poll (a per-topic problem must not tear the group
// down), so the gate has to be held closed by the poll's HEALTHY flag instead.
// Otherwise an identity that can consume nothing reports ready and the
// azure-eventhub gate stops meaning anything.
func TestErrorOnlyPollDoesNotClearReadiness(t *testing.T) {
	// Two errors-only fetches, then a real one.
	src := newFakeSource(nil, nil, [][]byte{[]byte(logEnvelope)})
	readyAfter := -1
	r := newTestReader(Config{
		Exporter: &captureExporter{},
		Ready:    func() { readyAfter = src.polled },
	}, src)
	runUntilCommit(t, r, src)
	if readyAfter != 3 {
		t.Fatalf("ready fired after poll %d, want 3 — an errors-only fetch must leave the gate closed", readyAfter)
	}
}

func TestSeverityMapping(t *testing.T) {
	for level, want := range map[string]plog.SeverityNumber{
		"Informational": plog.SeverityNumberInfo,
		"Warning":       plog.SeverityNumberWarn,
		"Error":         plog.SeverityNumberError,
		"Critical":      plog.SeverityNumberFatal,
		"Verbose":       plog.SeverityNumberDebug,
		"4":             plog.SeverityNumberInfo, // numeric levels deliberately uninterpreted
		"":              plog.SeverityNumberInfo,
	} {
		if got, _ := severityOf(level); got != want {
			t.Errorf("severityOf(%q) = %v, want %v", level, got, want)
		}
	}
}

// Severity TEXT casing is a cross-producer contract, not a local style
// choice: convert runs logenrich.Apply with overwrite semantics over every
// record, and enrich writes its six level names in lowercase. Uppercase here
// meant one Azure record shipped "ERROR" and the next shipped "error" purely
// because the second body happened to parse — a difference a consumer sees as
// two distinct severity values on one stream. journald and the events reader
// carry the same assertion.
func TestSeverityTextIsLowercase(t *testing.T) {
	for _, level := range []string{"Informational", "Warning", "Error", "Critical", "Verbose", "Fatal", "Warn", "Debug", "", "4"} {
		_, text := severityOf(level)
		if text == "" || text != strings.ToLower(text) {
			t.Errorf("severityOf(%q) text = %q, want a lowercase level name", level, text)
		}
	}
}

// AzureRecords is dimensioned signal/plural like AzureExported and like every
// other producer's counter; it used to spell the same dimension "kind" with
// singular values, so the decoded and exported counts of one pipeline shared
// no label to join on.
func TestRecordsCounterUsesSignalLabel(t *testing.T) {
	beforeLogs := obs.AzureRecords.WithLabelValues("logs").Value()
	beforeMetrics := obs.AzureRecords.WithLabelValues("metrics").Value()

	r := newTestReader(Config{Exporter: &captureExporter{}}, newFakeSource(nil))
	recs := r.decode([][]byte{
		[]byte(`{"records":[{"time":"2026-01-01T00:00:00Z","resourceId":"/SUBSCRIPTIONS/S/RESOURCEGROUPS/RG/PROVIDERS/P/T/N","category":"c"}]}`),
		[]byte(`{"records":[{"time":"2026-01-01T00:00:00Z","resourceId":"/SUBSCRIPTIONS/S/RESOURCEGROUPS/RG/PROVIDERS/P/T/N","metricName":"m","total":1}]}`),
	})
	if len(recs) != 2 {
		t.Fatalf("decoded %d records, want 2", len(recs))
	}
	if got := obs.AzureRecords.WithLabelValues("logs").Value() - beforeLogs; got != 1 {
		t.Errorf("signal=\"logs\" = %v, want 1", got)
	}
	if got := obs.AzureRecords.WithLabelValues("metrics").Value() - beforeMetrics; got != 1 {
		t.Errorf("signal=\"metrics\" = %v, want 1", got)
	}
	// The old singular values must be gone, not merely joined by new ones.
	if got := obs.AzureRecords.WithLabelValues("log").Value(); got != 0 {
		t.Errorf("the singular kind value \"log\" is still being emitted: %v", got)
	}
	if got := obs.AzureRecords.WithLabelValues("metric").Value(); got != 0 {
		t.Errorf("the singular kind value \"metric\" is still being emitted: %v", got)
	}
}

// Several Readers now run in ONE process (one per credential — see
// ResolveSources), and they share the compiled chain: the scrubber, the
// logattrs extractor, the rules filter and — the one with mutable state — the
// log-metrics set. Nothing here was concurrent before this: a single azurediag
// Reader owned the chain within its own goroutine. Run under -race, this is
// what says the sharing is safe; without it the failure would be a corrupted
// series map on a customer's node, not a test.
func TestConcurrentReadersShareTheChain(t *testing.T) {
	scrub, err := logscrub.New(logscrub.Config{Builtin: []string{"defaults"}})
	if err != nil {
		t.Fatal(err)
	}
	extractor, err := logattrs.New(&logattrs.Config{Rules: []logattrs.Rule{
		{Key: "properties.user", Attribute: "enduser.id"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	rules, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "keep", Match: []string{"azure.category=SQLSecurityAuditEvents"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	set, err := metrics.NewDynamicMetricSet([]metrics.Dynamic{{
		Name: "azure_records_total", Type: metrics.CounterType, Value: "1",
		Labels: []string{"category=$azure.category"},
	}})
	if err != nil {
		t.Fatal(err)
	}

	const readers = 4
	exp := &captureExporter{} // shared: its own mutex is part of what is exercised
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	srcs := make([]*fakeSource, readers)
	for i := range readers {
		polls := make([][][]byte, 8)
		for p := range polls {
			polls[p] = [][]byte{[]byte(logEnvelope)}
		}
		src := newFakeSource(polls...)
		srcs[i] = src
		r := newTestReader(Config{
			Exporter: exp, Enrich: true, Scrub: scrub, LogAttrs: extractor,
			Rules: rules, LogMetrics: set,
		}, src)
		wg.Add(1)
		go func() { defer wg.Done(); r.Run(ctx) }()
	}

	// Wait for every reader to have committed all of its polls, then stop.
	for _, src := range srcs {
		for range 8 {
			select {
			case <-src.committed:
			case <-time.After(30 * time.Second):
				cancel()
				wg.Wait()
				t.Fatal("timed out waiting for the readers to drain")
			}
		}
	}
	cancel()
	wg.Wait()

	// Every envelope carries two SQLSecurityAuditEvents records, all kept.
	if got, want := len(exp.records()), readers*8*2; got != want {
		t.Fatalf("records = %d, want %d — a shared chain dropped or duplicated work", got, want)
	}
}

// A transient export failure is THIS pipeline's, on BOTH signals.
//
// It used to bump kubescrape_log_export_failures_total, whose help reads "Log
// batch exports that failed after retries (files rewound)" — and this reader
// owns no file at all: the shipped singleton runs `-azure-diagnostics
// -logs=false`, so an operator alerting on a rewinding tailer was paged by a
// pod that runs none. journald was given its own counter for exactly that
// reason and its two siblings were left behind. The METRICS signal was worse
// than mislabelled: it was counted NOWHERE per-pipeline (only the client
// layer's generic obs.Exports, which cannot say which producer retried), so a
// hub carrying platform metrics retried invisibly.
func TestTransientExportFailuresCountPerSignal(t *testing.T) {
	beforeLogs := obs.AzureExportFailures.WithLabelValues("logs").Value()
	beforeMetrics := obs.AzureExportFailures.WithLabelValues("metrics").Value()
	beforeRewinds := obs.LogExportFailures.Value()

	// One envelope of each signal, each failing twice before it lands.
	exp := &captureExporter{failLogs: 2, failMet: 2, err: errors.New("collector down")}
	src := newFakeSource([][]byte{[]byte(logEnvelope), []byte(metricEnvelope)})
	r := newTestReader(Config{Exporter: exp, RetryBackoff: time.Millisecond}, src)
	runUntilCommit(t, r, src)

	if len(exp.logs) != 1 || len(exp.metrics) != 1 {
		t.Fatalf("delivered logs=%d metrics=%d, want 1 each (retried past the failures)",
			len(exp.logs), len(exp.metrics))
	}
	if got := obs.AzureExportFailures.WithLabelValues("logs").Value() - beforeLogs; got != 2 {
		t.Errorf("kubescrape_azure_export_failures_total{signal=\"logs\"} delta = %v, want 2 "+
			"(one per failed attempt)", got)
	}
	if got := obs.AzureExportFailures.WithLabelValues("metrics").Value() - beforeMetrics; got != 2 {
		t.Errorf("kubescrape_azure_export_failures_total{signal=\"metrics\"} delta = %v, want 2: "+
			"the metrics signal's retries were counted by no per-pipeline series at all", got)
	}
	if got := obs.LogExportFailures.Value() - beforeRewinds; got != 0 {
		t.Errorf("kubescrape_log_export_failures_total delta = %v, want 0: azurediag must not bump "+
			"the tailer's files-rewound counter", got)
	}
}
