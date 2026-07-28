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

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/logline"
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
	commits   int
	committed chan struct{}
	closed    bool
}

func newFakeSource(polls ...[][]byte) *fakeSource {
	return &fakeSource{polls: polls, committed: make(chan struct{}, 16)}
}

func (f *fakeSource) poll(ctx context.Context) ([][]byte, error) {
	if len(f.polls) == 0 {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	p := f.polls[0]
	f.polls = f.polls[1:]
	return p, nil
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

func TestSplitEnvelope(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int
	}{
		{`{"records":[{"a":1},{"b":2}]}`, 2},
		{`[{"a":1},{"b":2},{"c":3}]`, 3},
		{`{"time":"x","category":"y"}`, 1}, // bare single record
		{`{"records":[]}`, 0},
		{`{"records":[{"s":"tricky \" ]} string","n":[1,[2,3]]},{"b":2}]}`, 2},
		{"  \n[ {\"a\":1} , {\"b\":2} ]", 2},
	} {
		got, err := splitEnvelope([]byte(tc.in))
		if err != nil {
			t.Fatalf("%s: %v", tc.in, err)
		}
		if len(got) != tc.want {
			t.Fatalf("%s: records = %d, want %d (%q)", tc.in, len(got), tc.want, got)
		}
	}
	for _, bad := range []string{``, `garbage`, `[{"a":1}`} {
		if _, err := splitEnvelope([]byte(bad)); err == nil {
			t.Fatalf("%q: want an error", bad)
		}
	}
}

func TestDecodeClassifiesRecords(t *testing.T) {
	raws, err := splitEnvelope([]byte(metricEnvelope))
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

	raws, _ = splitEnvelope([]byte(logEnvelope))
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

func TestNamespaceFromConnectionString(t *testing.T) {
	cs := "Endpoint=sb://myns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc="
	if got := namespaceFromConnectionString(cs); got != "myns.servicebus.windows.net" {
		t.Fatalf("namespace = %q", got)
	}
	if got := namespaceFromConnectionString("SharedAccessKey=abc"); got != "" {
		t.Fatalf("namespace = %q, want empty", got)
	}
}
