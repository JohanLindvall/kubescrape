// Package journald tails the systemd journal by running journalctl as a
// subprocess (`-f -o json`) and exporting the entries as OTLP log records.
// Delivery is at-least-once: the cursor of the newest exported entry is
// persisted only after a successful export, and on any failure the
// subprocess is restarted from the persisted cursor, re-reading whatever was
// in flight.
//
// The distroless kubescrape image does not contain journalctl; using this
// input requires an image that provides it (see the chart values).
package journald

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/logattrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/logenrich"
	"github.com/JohanLindvall/kubescrape/internal/agent/positions"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// LogExporter sends one OTLP logs payload.
type LogExporter interface {
	ExportLogs(ctx context.Context, ld plog.Logs) error
}

// Config configures the journal reader.
type Config struct {
	// Path is the journalctl binary (default "journalctl", resolved via
	// $PATH).
	Path string
	// Dir reads a specific journal directory (journalctl -D); "" uses the
	// system default.
	Dir string
	// Units restricts to these systemd units (journalctl -u, repeated);
	// empty reads everything.
	Units []string
	// Positions persists the last exported cursor across restarts (nil = no
	// persistence; every start then begins at the tail).
	Positions *positions.Store

	BatchSize     int           // flush after this many entries
	FlushInterval time.Duration // flush at least this often
	MaxEntryBytes int           // cap on one journal message

	// Enrich parses metadata out of each message (timestamp, severity,
	// trace/span IDs, exception details, ...) into the record's OTLP fields
	// and attributes; an explicit level in the message wins over the journal
	// priority.
	Enrich bool
	// LogAttrs lifts configured keys out of structured messages onto the
	// record as resource/scope/log attributes (nil = none).
	LogAttrs *logattrs.Extractor

	// Attrs builds the exported resource attributes (nil = defaults).
	Attrs *attrs.Builder
	// NodeInfo supplies the agent node's metadata for attribute templates.
	NodeInfo func() *attrs.NodeInfo

	Exporter LogExporter
	Logger   *slog.Logger

	// RestartBackoff is the initial delay before restarting a failed
	// subprocess or retrying a failed export, doubled up to 30s (default
	// 1s; tests shorten it).
	RestartBackoff time.Duration
}

// Reader runs journalctl and exports its output. All fields are owned by
// the single Run goroutine.
type Reader struct {
	cfg Config
	log *slog.Logger

	batch       []entry
	lastFlush   time.Time
	cursor      string // last successfully exported cursor
	batchCursor string // cursor of the newest buffered entry
}

type entry struct {
	unit     string // resource grouping key
	body     string
	ts       time.Time
	severity plog.SeverityNumber
	sevText  string
	pid      int64
	ident    string // SYSLOG_IDENTIFIER
}

// New creates a Reader.
func New(cfg Config) *Reader {
	if cfg.Path == "" {
		cfg.Path = "journalctl"
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1024
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 2 * time.Second
	}
	if cfg.MaxEntryBytes <= 0 {
		cfg.MaxEntryBytes = 1 << 20
	}
	if cfg.RestartBackoff <= 0 {
		cfg.RestartBackoff = time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Reader{cfg: cfg, log: cfg.Logger}
}

// Run reads until ctx is done, restarting the subprocess on any failure.
func (r *Reader) Run(ctx context.Context) {
	r.cursor = r.loadCursor()
	backoff := r.cfg.RestartBackoff
	for ctx.Err() == nil {
		err := r.follow(ctx)
		if ctx.Err() != nil {
			break
		}
		obs.JournalRestarts.Inc()
		r.log.Warn("journalctl stopped; restarting", "error", err, "backoff", backoff)
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}
	// Final flush of whatever is buffered; the subprocess is already gone.
	r.flush(context.Background())
}

// follow runs one journalctl incarnation, streaming until it exits or an
// export fails. On export failure the buffered entries are dropped and the
// subprocess killed; the caller restarts from the committed cursor.
func (r *Reader) follow(ctx context.Context) error {
	args := []string{"-f", "-o", "json", "--no-pager"}
	if r.cursor != "" {
		args = append(args, "--after-cursor="+r.cursor)
	} else {
		// No committed position: start at the tail rather than replaying
		// the whole journal.
		args = append(args, "-n", "0")
	}
	if r.cfg.Dir != "" {
		args = append(args, "-D", r.cfg.Dir)
	}
	for _, u := range r.cfg.Units {
		args = append(args, "-u", u)
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := exec.CommandContext(cctx, r.cfg.Path, args...)
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting %s: %w", r.cfg.Path, err)
	}
	defer func() {
		cancel()
		_ = cmd.Wait()
	}()
	r.log.Info("journalctl started", "path", r.cfg.Path, "args", strings.Join(args, " "))

	r.batch = r.batch[:0]
	r.batchCursor = ""
	r.lastFlush = time.Now()

	// The flush interval must fire even while no entries arrive, so lines
	// are handed over from a reader goroutine bound to this incarnation.
	lines := make(chan []byte)
	readErr := make(chan error, 1)
	go func() {
		defer close(lines)
		sc := bufio.NewScanner(stdout)
		sc.Buffer(make([]byte, 64*1024), r.cfg.MaxEntryBytes+64*1024)
		for sc.Scan() {
			line := make([]byte, len(sc.Bytes()))
			copy(line, sc.Bytes())
			select {
			case lines <- line:
			case <-cctx.Done():
				return
			}
		}
		readErr <- sc.Err()
	}()

	ticker := time.NewTicker(r.cfg.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if len(r.batch) > 0 && time.Since(r.lastFlush) >= r.cfg.FlushInterval {
				if err := r.flush(ctx); err != nil {
					return err
				}
			}
		case line, ok := <-lines:
			if !ok {
				// Subprocess output ended: flush what we have, then report.
				if err := r.flush(ctx); err != nil {
					return err
				}
				select {
				case err := <-readErr:
					if err != nil {
						return fmt.Errorf("reading journalctl output: %w", err)
					}
				default:
				}
				return fmt.Errorf("journalctl exited")
			}
			r.ingest(line)
			if len(r.batch) >= r.cfg.BatchSize {
				if err := r.flush(ctx); err != nil {
					return err
				}
			}
		}
	}
}

// ingest parses one journalctl JSON line into the batch.
func (r *Reader) ingest(line []byte) {
	var fields map[string]any
	if err := json.Unmarshal(line, &fields); err != nil {
		r.log.Debug("unparsable journal line", "error", err)
		return
	}
	msg := fieldString(fields, "MESSAGE")
	if len(msg) > r.cfg.MaxEntryBytes {
		msg = msg[:r.cfg.MaxEntryBytes]
	}
	e := entry{
		unit:  fieldString(fields, "_SYSTEMD_UNIT"),
		ident: fieldString(fields, "SYSLOG_IDENTIFIER"),
		body:  msg,
	}
	if usec, err := strconv.ParseInt(fieldString(fields, "__REALTIME_TIMESTAMP"), 10, 64); err == nil {
		e.ts = time.UnixMicro(usec)
	} else {
		e.ts = time.Now()
	}
	e.severity, e.sevText = severity(fieldString(fields, "PRIORITY"))
	if pid, err := strconv.ParseInt(fieldString(fields, "_PID"), 10, 64); err == nil {
		e.pid = pid
	}
	if cursor := fieldString(fields, "__CURSOR"); cursor != "" {
		r.batchCursor = cursor
	}
	r.batch = append(r.batch, e)
}

// flush exports the batch; on success the newest cursor is committed.
func (r *Reader) flush(ctx context.Context) error {
	if len(r.batch) == 0 {
		return nil
	}
	ld := r.convert()
	if err := r.cfg.Exporter.ExportLogs(ctx, ld); err != nil {
		obs.LogExportFailures.Inc()
		return fmt.Errorf("exporting journal batch: %w", err)
	}
	obs.JournalEntries.Add(float64(len(r.batch)))
	r.batch = r.batch[:0]
	r.lastFlush = time.Now()
	if r.batchCursor != "" {
		r.cursor = r.batchCursor
		r.saveCursor()
	}
	return nil
}

// convert groups the batch into one resource per unit.
func (r *Reader) convert() plog.Logs {
	ld := plog.NewLogs()
	scopes := make(map[string]plog.ScopeLogs, 4)
	for _, e := range r.batch {
		var extracted logattrs.Result
		if r.cfg.LogAttrs != nil {
			extracted = r.cfg.LogAttrs.Extract(e.body)
		}
		unit := e.unit
		if unit == "" {
			unit = e.ident
		}
		// Line-derived resource/scope attributes split records into their own
		// resources, so they participate in the grouping key.
		key := unit + "\x01" + logattrs.Key(extracted.Resource) + "\x01" + logattrs.Key(extracted.Scope)
		sl, ok := scopes[key]
		if !ok {
			rl := ld.ResourceLogs().AppendEmpty()
			res := rl.Resource()
			// Identity attributes go in before Build so templates and the
			// filter see them.
			name := unit
			if name == "" {
				name = "journald"
			}
			res.Attributes().PutStr("service.name", strings.TrimSuffix(name, ".service"))
			if e.unit != "" {
				res.Attributes().PutStr("systemd.unit", e.unit)
			}
			actx := attrs.Context{}
			if r.cfg.NodeInfo != nil {
				actx.Node = r.cfg.NodeInfo()
			}
			r.cfg.Attrs.Build(res, actx)
			logattrs.Put(res.Attributes(), extracted.Resource)
			sl = rl.ScopeLogs().AppendEmpty()
			logattrs.Put(sl.Scope().Attributes(), extracted.Scope)
			scopes[key] = sl
		}
		lr := sl.LogRecords().AppendEmpty()
		lr.SetTimestamp(pcommon.NewTimestampFromTime(e.ts))
		lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		lr.SetSeverityNumber(e.severity)
		lr.SetSeverityText(e.sevText)
		lr.Body().SetStr(e.body)
		if e.ident != "" {
			lr.Attributes().PutStr("syslog.identifier", e.ident)
		}
		if e.pid != 0 {
			lr.Attributes().PutInt("process.pid", e.pid)
		}
		logattrs.Put(lr.Attributes(), extracted.Log)
		if r.cfg.Enrich {
			logenrich.Apply(lr, e.body)
		}
	}
	return ld
}

// severity maps a syslog priority (0-7) to OTLP severity.
func severity(priority string) (plog.SeverityNumber, string) {
	switch priority {
	case "0":
		return plog.SeverityNumberFatal, "emerg"
	case "1":
		return plog.SeverityNumberError3, "alert"
	case "2":
		return plog.SeverityNumberError2, "crit"
	case "3":
		return plog.SeverityNumberError, "err"
	case "4":
		return plog.SeverityNumberWarn, "warning"
	case "5":
		return plog.SeverityNumberInfo2, "notice"
	case "6":
		return plog.SeverityNumberInfo, "info"
	case "7":
		return plog.SeverityNumberDebug, "debug"
	}
	return plog.SeverityNumberUnspecified, ""
}

// fieldString extracts a journal field that may be a string or (for
// non-UTF-8 payloads) an array of bytes; multi-valued fields yield the
// first value.
func fieldString(fields map[string]any, key string) string {
	switch v := fields[key].(type) {
	case string:
		return v
	case []any:
		if len(v) == 0 {
			return ""
		}
		// Either a byte array (numbers) or multiple values.
		if _, ok := v[0].(float64); ok {
			b := make([]byte, 0, len(v))
			for _, n := range v {
				f, ok := n.(float64)
				if !ok {
					return ""
				}
				b = append(b, byte(int(f)))
			}
			return string(b)
		}
		if s, ok := v[0].(string); ok {
			return s
		}
	}
	return ""
}

func (r *Reader) loadCursor() string {
	if r.cfg.Positions != nil {
		return r.cfg.Positions.JournalCursor()
	}
	return ""
}

// saveCursor persists the committed cursor to the shared positions store.
func (r *Reader) saveCursor() {
	if r.cfg.Positions == nil {
		return
	}
	if err := r.cfg.Positions.SetJournalCursor(r.cursor); err != nil {
		r.log.Warn("writing journal cursor to positions file", "error", err)
	}
}
