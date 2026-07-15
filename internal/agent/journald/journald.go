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
	"unicode/utf8"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/logenrich"
	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/agent/positions"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
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
	MaxBatchBytes int           // flush before a batch's summed bodies exceed this (default 1 MiB)

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
	batchBytes  int // summed body sizes of the buffered entries
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
	// origLen is the message's byte length before truncation, or 0 if it was
	// not truncated. A truncated record carries log.truncated + this length so a
	// consumer can tell a cut body from a whole one.
	origLen int
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
	if cfg.MaxBatchBytes <= 0 {
		cfg.MaxBatchBytes = 1 << 20
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
		// The restart path recovers unexported entries by RE-READING them from
		// the committed cursor. That only works once a cursor exists: with none
		// (the first run, or no positions store at all) a reopen seeks to the
		// journal TAIL and the buffered entries are gone. So while no cursor has
		// been committed, keep retrying the pending batch's export instead of
		// reopening — the journal itself is the buffer, and the moment one
		// export lands the cursor covers everything that follows.
		if r.cursor == "" && len(r.batch) > 0 {
			if err := r.flush(ctx); err != nil {
				r.log.Warn("journal export failed with no committed cursor; retrying (a reopen would seek to the tail and lose the entries)",
					"entries", len(r.batch), "error", err, "backoff", backoff)
				select {
				case <-ctx.Done():
				case <-time.After(backoff):
				}
				if backoff *= 2; backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
				continue
			}
			backoff = r.cfg.RestartBackoff
		}
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

	clear(r.batch)
	r.batch = r.batch[:0]
	r.batchBytes = 0
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
			body, origLen := r.sanitize(e.fields["MESSAGE"])
			if origLen > 0 {
				obs.JournalTruncated.Inc()
			}
			// Flush BEFORE the entry that would push the batch over the byte
			// cap. A single entry already over the cap still exports alone
			// (entries are never split), so one payload can exceed it by up
			// to MaxEntryBytes.
			if len(r.batch) > 0 && r.batchBytes+len(body) > r.cfg.MaxBatchBytes {
				if err := r.flush(ctx); err != nil {
					return err
				}
			}
			r.ingest(e, body, origLen)
			if len(r.batch) >= r.cfg.BatchSize {
				if err := r.flush(ctx); err != nil {
					return err
				}
			}
		}
	}
}

// sanitize makes one journal message exportable: valid UTF-8 (the journal
// stores raw bytes) and capped at MaxEntryBytes without splitting a rune.
func (r *Reader) sanitize(msg string) (body string, origLen int) {
	msg = strings.ToValidUTF8(msg, "�")
	if len(msg) > r.cfg.MaxEntryBytes {
		return truncateRunes(msg, r.cfg.MaxEntryBytes), len(msg)
	}
	return msg, 0
}

// truncateRunes cuts s to at most n bytes on a rune boundary.
func truncateRunes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// ingest converts one raw journal entry (body already sanitized) into the
// batch.
func (r *Reader) ingest(re rawEntry, body string, origLen int) {
	e := entry{
		unit:    re.fields["_SYSTEMD_UNIT"],
		ident:   re.fields["SYSLOG_IDENTIFIER"],
		body:    body,
		ts:      re.realtime,
		origLen: origLen,
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
	r.batchBytes += len(body)
}

// flush exports the batch; on success the newest cursor is committed. A batch
// the collector permanently rejects is dropped and its cursor committed too —
// re-reading it forever (the restart path) would wedge the reader on one
// poison batch. Transient failures return the error; the caller restarts from
// the committed cursor.
func (r *Reader) flush(ctx context.Context) error {
	if len(r.batch) == 0 {
		return nil
	}
	ld := r.convert()
	if err := r.cfg.Exporter.ExportLogs(ctx, ld); err != nil {
		if otlpexport.IsPermanent(err) {
			obs.JournalDropped.Inc()
			r.log.Warn("journal batch permanently rejected, skipping past it", "entries", len(r.batch), "error", err)
			r.settleBatch()
			return nil
		}
		obs.LogExportFailures.Inc()
		return fmt.Errorf("exporting journal batch: %w", err)
	}
	obs.JournalEntries.Add(float64(len(r.batch)))
	r.settleBatch()
	return nil
}

// settleBatch clears the batch (releasing the bodies pinned by the backing
// array) and commits its newest cursor.
func (r *Reader) settleBatch() {
	clear(r.batch)
	r.batch = r.batch[:0]
	r.batchBytes = 0
	r.lastFlush = time.Now()
	if r.batchCursor != "" {
		r.cursor = r.batchCursor
		r.saveCursor()
	}
}

// convert groups the batch into one resource per unit.
func (r *Reader) convert() plog.Logs {
	ld := plog.NewLogs()
	scopes := make(map[string]plog.ScopeLogs, 4)
	observed := pcommon.NewTimestampFromTime(time.Now())
	for _, e := range r.batch {
		var extracted logattrs.Result
		unit := e.unit
		if unit == "" {
			unit = e.ident
		}
		// Line-derived resource/scope attributes split records into their own
		// resources, so they participate in the grouping key. Without an
		// extractor the unit alone is the key (no per-entry concatenation).
		key := unit
		if r.cfg.LogAttrs != nil {
			extracted = r.cfg.LogAttrs.Extract(e.body)
			key = unit + "\x01" + logattrs.Key(extracted.Resource) + "\x01" + logattrs.Key(extracted.Scope)
		}
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
		lr.SetObservedTimestamp(observed)
		lr.SetSeverityNumber(e.severity)
		lr.SetSeverityText(e.sevText)
		lr.Body().SetStr(e.body)
		if e.origLen > 0 {
			lr.Attributes().PutBool("log.truncated", true)
			lr.Attributes().PutInt("log.original_length", int64(e.origLen))
		}
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
