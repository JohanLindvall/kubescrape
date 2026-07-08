package journald

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/agent/logattrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/positions"
)

// captureExporter records exported batches; Fail(n) makes the next n
// exports error.
type captureExporter struct {
	mu       sync.Mutex
	batches  []plog.Logs
	failures int
}

func (c *captureExporter) ExportLogs(_ context.Context, ld plog.Logs) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failures > 0 {
		c.failures--
		return fmt.Errorf("injected export failure")
	}
	c.batches = append(c.batches, ld)
	return nil
}

func (c *captureExporter) records() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, ld := range c.batches {
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			rl := ld.ResourceLogs().At(i)
			for j := 0; j < rl.ScopeLogs().Len(); j++ {
				lrs := rl.ScopeLogs().At(j).LogRecords()
				for k := 0; k < lrs.Len(); k++ {
					out = append(out, lrs.At(k).Body().Str())
				}
			}
		}
	}
	return out
}

// stubJournalctl writes a fake journalctl: it emits the entries whose
// cursor sorts after --after-cursor (all of them without one), then sleeps.
// Entry i has cursor "c<i>", unit and message from the arrays.
func stubJournalctl(t *testing.T, units, messages []string) string {
	t.Helper()
	dir := t.TempDir()
	var body string
	body = "#!/bin/sh\nafter=\"\"\nfor a in \"$@\"; do case \"$a\" in --after-cursor=*) after=\"${a#--after-cursor=}\";; esac; done\n"
	for i := range messages {
		cursor := fmt.Sprintf("c%02d", i)
		line := fmt.Sprintf(`{"__CURSOR":"%s","MESSAGE":"%s","PRIORITY":"6","_SYSTEMD_UNIT":"%s","_PID":"42","SYSLOG_IDENTIFIER":"stub","__REALTIME_TIMESTAMP":"1700000000%06d"}`,
			cursor, messages[i], units[i], i)
		// Emit the entry unless its cursor is <= the resume cursor.
		body += fmt.Sprintf("if [ -z \"$after\" ] || [ \"%s\" \\> \"$after\" ]; then echo '%s'; fi\n", cursor, line)
	}
	// exec so the reader's kill reaches the sleep itself; an orphaned child
	// would hold the test binary's stderr open and stall `go test` for 60s.
	body += "exec sleep 60\n"
	path := filepath.Join(dir, "journalctl")
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func startReader(t *testing.T, cfg Config) (*captureExporter, context.CancelFunc) {
	t.Helper()
	exp := &captureExporter{}
	cfg.Exporter = exp
	cfg.FlushInterval = 20 * time.Millisecond
	cfg.RestartBackoff = 10 * time.Millisecond
	r := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	return exp, cancel
}

func TestJournalExport(t *testing.T) {
	path := stubJournalctl(t,
		[]string{"kubelet.service", "kubelet.service", "sshd.service"},
		[]string{"one", "two", "three"})
	exp, _ := startReader(t, Config{Path: path})

	waitFor(t, "3 records", func() bool { return len(exp.records()) == 3 })

	// Per-unit resources: kubelet entries share one, sshd gets its own.
	exp.mu.Lock()
	defer exp.mu.Unlock()
	units := map[string]int{}
	for _, ld := range exp.batches {
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			rl := ld.ResourceLogs().At(i)
			svc, _ := rl.Resource().Attributes().Get("service.name")
			unit, _ := rl.Resource().Attributes().Get("systemd.unit")
			n := 0
			for j := 0; j < rl.ScopeLogs().Len(); j++ {
				n += rl.ScopeLogs().At(j).LogRecords().Len()
			}
			units[svc.Str()+"/"+unit.Str()] += n
			lr := rl.ScopeLogs().At(0).LogRecords().At(0)
			if lr.SeverityNumber() != plog.SeverityNumberInfo || lr.SeverityText() != "info" {
				t.Errorf("severity = %v %q", lr.SeverityNumber(), lr.SeverityText())
			}
			if pid, ok := lr.Attributes().Get("process.pid"); !ok || pid.Int() != 42 {
				t.Errorf("process.pid = %v", pid)
			}
		}
	}
	if units["kubelet/kubelet.service"] != 2 || units["sshd/sshd.service"] != 1 {
		t.Errorf("per-unit record counts = %v", units)
	}
}

func TestJournalCursorResume(t *testing.T) {
	path := stubJournalctl(t,
		[]string{"a.service", "a.service", "a.service"},
		[]string{"one", "two", "three"})
	posPath := filepath.Join(t.TempDir(), "positions.json")
	pos := positions.Open(posPath)

	exp, cancel := startReader(t, Config{Path: path, Positions: pos})
	waitFor(t, "first run's 3 records", func() bool { return len(exp.records()) == 3 })
	waitFor(t, "cursor committed", func() bool { return pos.JournalCursor() == "c02" })
	cancel()

	// A fresh reader resumes past the committed cursor: nothing re-emitted.
	exp2, _ := startReader(t, Config{Path: path, Positions: positions.Open(posPath)})
	time.Sleep(150 * time.Millisecond)
	if got := exp2.records(); len(got) != 0 {
		t.Fatalf("resumed run re-emitted %v", got)
	}
}

func TestJournalExportFailureRereads(t *testing.T) {
	path := stubJournalctl(t,
		[]string{"a.service", "a.service"},
		[]string{"one", "two"})
	exp := &captureExporter{failures: 1}
	r := New(Config{
		Path:           path,
		Positions:      positions.Open(filepath.Join(t.TempDir(), "positions.json")),
		Exporter:       exp,
		FlushInterval:  20 * time.Millisecond,
		RestartBackoff: 10 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	// The failed batch is re-read after the subprocess restart; no cursor
	// was committed, so both entries arrive.
	waitFor(t, "re-read records", func() bool { return len(exp.records()) == 2 })
	if got := exp.records(); got[0] != "one" || got[1] != "two" {
		t.Fatalf("records = %v", got)
	}
}

func TestJournalRestartAfterExit(t *testing.T) {
	// A stub that exits immediately after emitting one entry: the reader
	// must restart it and (thanks to the committed cursor) not duplicate.
	dir := t.TempDir()
	path := filepath.Join(dir, "journalctl")
	script := "#!/bin/sh\nafter=\"\"\nfor a in \"$@\"; do case \"$a\" in --after-cursor=*) after=\"${a#--after-cursor=}\";; esac; done\n" +
		"if [ -z \"$after\" ]; then echo '{\"__CURSOR\":\"c0\",\"MESSAGE\":\"only\",\"PRIORITY\":\"4\",\"_SYSTEMD_UNIT\":\"a.service\",\"__REALTIME_TIMESTAMP\":\"1700000000000000\"}'; fi\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	exp, _ := startReader(t, Config{Path: path, Positions: positions.Open(filepath.Join(dir, "positions.json"))})

	waitFor(t, "one record", func() bool { return len(exp.records()) == 1 })
	// Give it a few restart cycles; the record must not repeat.
	time.Sleep(200 * time.Millisecond)
	if got := exp.records(); len(got) != 1 {
		t.Fatalf("records after restarts = %v", got)
	}
}

func TestJournalEnrich(t *testing.T) {
	// The JSON message carries its own level, which must win over the
	// journal's PRIORITY (6 = info) when enrichment is on.
	dir := t.TempDir()
	path := filepath.Join(dir, "journalctl")
	msg := `{\"@t\":\"2026-01-02T03:04:05Z\",\"level\":\"error\",\"msg\":\"boom\"}`
	script := "#!/bin/sh\n" +
		`echo '{"__CURSOR":"c0","MESSAGE":"` + msg + `","PRIORITY":"6","_SYSTEMD_UNIT":"a.service","__REALTIME_TIMESTAMP":"1700000000000000"}'` + "\n" +
		"exec sleep 60\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	exp, _ := startReader(t, Config{Path: path, Enrich: true})
	waitFor(t, "one record", func() bool { return len(exp.records()) == 1 })

	exp.mu.Lock()
	defer exp.mu.Unlock()
	lr := exp.batches[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	if lr.SeverityNumber() != plog.SeverityNumberError || lr.SeverityText() != "error" {
		t.Errorf("severity = %v %q; want the line's own level over PRIORITY", lr.SeverityNumber(), lr.SeverityText())
	}
	if !lr.Timestamp().AsTime().Equal(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)) {
		t.Errorf("timestamp = %v; want the line's own", lr.Timestamp().AsTime())
	}
}

func TestJournalLogAttrs(t *testing.T) {
	// Structured JSON messages carry a tenant; as a resource attribute it
	// splits records into separate resources even within one unit.
	path := stubJournalctl(t,
		[]string{"a.service", "a.service"},
		[]string{`{\"tenant\":\"x\",\"req\":\"r1\"}`, `{\"tenant\":\"y\",\"req\":\"r2\"}`})
	ex, err := logattrs.New(&logattrs.Config{Rules: []logattrs.Rule{
		{Key: "tenant", Attribute: "tenant.id", Target: logattrs.TargetResource},
		{Key: "req", Target: logattrs.TargetLog},
	}})
	if err != nil {
		t.Fatal(err)
	}
	exp, _ := startReader(t, Config{Path: path, LogAttrs: ex})
	waitFor(t, "2 records", func() bool { return len(exp.records()) == 2 })

	exp.mu.Lock()
	defer exp.mu.Unlock()
	tenants := map[string]int{}
	for _, ld := range exp.batches {
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			rl := ld.ResourceLogs().At(i)
			v, _ := rl.Resource().Attributes().Get("tenant.id")
			n := 0
			for j := 0; j < rl.ScopeLogs().Len(); j++ {
				n += rl.ScopeLogs().At(j).LogRecords().Len()
			}
			tenants[v.Str()] += n
		}
	}
	if tenants["x"] != 1 || tenants["y"] != 1 {
		t.Errorf("tenant record counts = %+v (want one resource each)", tenants)
	}
}

func TestFieldString(t *testing.T) {
	fields := map[string]any{
		"S":     "plain",
		"BYTES": []any{float64('h'), float64('i')},
		"MULTI": []any{"first", "second"},
	}
	if got := fieldString(fields, "S"); got != "plain" {
		t.Errorf("string = %q", got)
	}
	if got := fieldString(fields, "BYTES"); got != "hi" {
		t.Errorf("bytes = %q", got)
	}
	if got := fieldString(fields, "MULTI"); got != "first" {
		t.Errorf("multi = %q", got)
	}
	if got := fieldString(fields, "MISSING"); got != "" {
		t.Errorf("missing = %q", got)
	}
}
