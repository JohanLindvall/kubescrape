// Package journald reads the systemd journal through libsystemd (via
// github.com/coreos/go-systemd/v22/sdjournal — cgo) and exports the entries as
// OTLP log records. Delivery is at-least-once: the cursor of the newest
// exported entry is persisted only after a successful export, and on any
// failure the reader restarts from the persisted cursor, re-reading whatever
// was in flight.
//
// Because it links libsystemd, the agent binary is built with cgo and the
// image must provide libsystemd (see the Dockerfile). The journal itself is
// read directly — no journalctl subprocess.
package journald

import (
	"context"
	"fmt"
	"log/slog"
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
	// Dir reads a specific journal directory; "" opens the default system
	// journal.
	Dir string
	// Units restricts to these systemd units (matched on _SYSTEMD_UNIT);
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

	// RestartBackoff is the initial delay before restarting a failed reader or
	// retrying a failed export, doubled up to 30s (default 1s; tests shorten
	// it).
	RestartBackoff time.Duration
}

// rawEntry is one journal entry as read from the source: its fields (systemd
// journal field names → values), opaque cursor, and realtime timestamp.
type rawEntry struct {
	fields   map[string]string
	cursor   string
	realtime time.Time
}

// source streams journal entries in order and supports cursor resume.
type source interface {
	// next returns the next entry, blocking until one is available or ctx is
	// done. ok is false with a nil error when the source ends cleanly.
	next(ctx context.Context) (rawEntry, bool, error)
	close() error
}

// openFunc opens a source positioned just after afterCursor ("" = start at the
// journal tail). It is a field so tests can inject a fake journal.
type openFunc func(cfg Config, afterCursor string) (source, error)

// Reader reads the journal and exports its entries. All fields are owned by the
// single Run goroutine.
type Reader struct {
	cfg  Config
	log  *slog.Logger
	open openFunc

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
	return &Reader{cfg: cfg, log: cfg.Logger, open: openJournal}
}

// Run reads until ctx is done, restarting the reader on any failure.
func (r *Reader) Run(ctx context.Context) {
	r.cursor = r.loadCursor()
	backoff := r.cfg.RestartBackoff
	for ctx.Err() == nil {
		started := time.Now()
		err := r.stream(ctx)
		if ctx.Err() != nil {
			break
		}
		if time.Since(started) >= 30*time.Second {
			// A stream that ran healthily resets the backoff; otherwise a few
			// hiccups spread over the agent's lifetime would pin every future
			// restart at the 30s worst case.
			backoff = r.cfg.RestartBackoff
		}
		obs.JournalRestarts.Inc()
		r.log.Warn("journal reader stopped; restarting", "error", err, "backoff", backoff)
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}
	// Final flush of whatever is buffered.
	if err := r.flush(context.Background()); err != nil {
		r.log.Warn("final journal flush failed", "error", err)
	}
}

// stream opens one journal source and reads until it ends or an export fails.
// On export failure the buffered entries are dropped; the caller restarts from
// the committed cursor, re-reading them.
func (r *Reader) stream(ctx context.Context) error {
	src, err := r.open(r.cfg, r.cursor)
	if err != nil {
		return err
	}
	defer func() { _ = src.close() }()

	r.batch = r.batch[:0]
	r.batchCursor = ""
	r.lastFlush = time.Now()

	// A reader goroutine bound to this source hands entries over so the flush
	// ticker still fires while no entries arrive. It must stop before src.close
	// (the journal handle is not safe for concurrent use), so cancel its context
	// and wait for done before returning.
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	entries := make(chan rawEntry)
	readErr := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(entries)
		for {
			e, ok, err := src.next(cctx)
			if err != nil {
				readErr <- err
				return
			}
			if !ok {
				return
			}
			select {
			case entries <- e:
			case <-cctx.Done():
				return
			}
		}
	}()
	// Ensure the goroutine has fully exited before src.close runs.
	defer func() { cancel(); <-done }()

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
		case e, ok := <-entries:
			if !ok {
				if err := r.flush(ctx); err != nil {
					return err
				}
				select {
				case err := <-readErr:
					return fmt.Errorf("reading journal: %w", err)
				default:
					return fmt.Errorf("journal source ended")
				}
			}
			r.ingest(e)
			if len(r.batch) >= r.cfg.BatchSize {
				if err := r.flush(ctx); err != nil {
					return err
				}
			}
		}
	}
}

// ingest converts one raw journal entry into the batch.
func (r *Reader) ingest(re rawEntry) {
	msg := re.fields["MESSAGE"]
	if len(msg) > r.cfg.MaxEntryBytes {
		msg = msg[:r.cfg.MaxEntryBytes]
	}
	e := entry{
		unit:  re.fields["_SYSTEMD_UNIT"],
		ident: re.fields["SYSLOG_IDENTIFIER"],
		body:  msg,
		ts:    re.realtime,
	}
	if e.ts.IsZero() {
		e.ts = time.Now()
	}
	e.severity, e.sevText = severity(re.fields["PRIORITY"])
	if pid, err := strconv.ParseInt(re.fields["_PID"], 10, 64); err == nil {
		e.pid = pid
	}
	if re.cursor != "" {
		r.batchCursor = re.cursor
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
