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
	"github.com/JohanLindvall/kubescrape/internal/agent/backoff"
	"github.com/JohanLindvall/kubescrape/internal/agent/logchain"
	"github.com/JohanLindvall/kubescrape/internal/agent/logscrub"
	"github.com/JohanLindvall/kubescrape/internal/agent/positions"
	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
)

// ScopeName is the OTLP instrumentation-scope name on every journal record
// (wire-visible: Loki/Elastic see it as a label, so changing it splits every
// journal stream at the upgrade boundary).
const ScopeName = "github.com/JohanLindvall/kubescrape/agent/journald"

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
	// MaxBatchBytes flushes before the batch's summed message BODY bytes exceed
	// this (default 1 MiB) — a soft bound that keeps a batch from growing large
	// in memory. It counts bodies only, not enrichment attributes or framing, so
	// the marshaled payload runs larger; the hard guarantee that no payload
	// exceeds the collector's gRPC receive limit lives in the exporter
	// (otlpexport.Config.MaxSendBytes), which splits an over-cap payload into
	// parts before sending.
	MaxBatchBytes int

	// Enrich parses metadata out of each message (timestamp, severity,
	// trace/span IDs, exception details, ...) into the record's OTLP fields
	// and attributes; an explicit level in the message wins over the journal
	// priority.
	Enrich bool
	// LogAttrs lifts configured keys out of structured messages onto the
	// record as resource/scope/log attributes (nil = none).
	LogAttrs *logattrs.Extractor
	// Scrub redacts sensitive values from message bodies before anything
	// copies from them (nil disables).
	Scrub *logscrub.Scrubber
	// Rules are ordered keep/drop/sample rules over journal entries, evaluated
	// AFTER enrichment (so the synthetic __severity__ key sees the enriched
	// severity) and AFTER LogMetrics (so metrics observe every entry, including
	// dropped ones) — the same order and semantics as the tailer's logs.rules.
	Rules *logline.LineFilter
	// LogMetrics derives configured metrics from each journal entry, before the
	// rules run. The journal is dominated by kubelet/containerd chatter, which
	// is exactly the volume you want to count and then drop.
	LogMetrics *metrics.DynamicMetricSet

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

// rawEntry is one journal entry as read from the source: exactly the fields
// the converter consumes, plus the opaque cursor and realtime timestamp. The
// source reads these individually (sdjournal GetDataValue) rather than via
// GetEntry, which enumerates EVERY field of the entry into a fresh map —
// 20-30 cgo string copies per entry where five suffice.
type rawEntry struct {
	message  string
	unit     string // _SYSTEMD_UNIT
	ident    string // SYSLOG_IDENTIFIER
	priority string
	pid      string // _PID
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
	// pending is the batch converted to OTLP, held across export retries
	// under logchain.Pending's convert-once/clear-with-the-batch discipline.
	pending logchain.Pending
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
	bo := backoff.New(r.cfg.RestartBackoff)
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
					"entries", len(r.batch), "error", err, "backoff", bo.Delay())
				bo.Sleep(ctx)
				continue
			}
			bo.Reset()
		}
		started := time.Now()
		err := r.stream(ctx)
		if ctx.Err() != nil {
			break
		}
		bo.ResetIfHealthy(started)
		obs.JournalRestarts.Inc()
		r.log.Warn("journal reader stopped; restarting", "error", err, "backoff", bo.Delay())
		bo.Sleep(ctx)
	}
	// Final flush of whatever is buffered, on a DETACHED but BOUNDED context.
	// ctx is already cancelled, so the flush needs a deadline of its own — and
	// this was the only one of the three shutdown flushes (tailer, events,
	// here) that had none: an unreachable collector held it for as long as the
	// export's own retries took, while the agent's remaining shutdown work
	// (the final log-metrics window, span metrics, self-metrics, the
	// disk-buffer drain) waited behind it inside a wg the main budgets at
	// shutdownDrain. (azurediag deliberately has NO final flush at all: its
	// position is the consumer group's committed offsets, so an uncommitted
	// poll simply replays.) Missing the deadline loses nothing this reader
	// owns either: the cursor is committed only on a successful export, so a
	// dropped final batch is re-read from the journal after the restart.
	//
	// WithoutCancel, not Background: the values the caller put on ctx (the
	// otlpexport ownership marker rides there) must survive.
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownFlushBudget)
	defer cancel()
	if err := r.flush(fctx); err != nil {
		r.log.Warn("final journal flush failed", "error", err)
	}
}

// shutdownFlushBudget bounds the final flush above. It matches the tailer's
// defaultShutdownBudget and the agent's other final exports; the sum has to fit
// inside the pod's terminationGracePeriodSeconds.
const shutdownFlushBudget = 10 * time.Second

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
	// The CONVERTED payload belongs to the batch just discarded, and must go
	// with it: this restart re-reads those entries from the committed cursor
	// (logchain.Pending's restart-clear case). This is the one place the pair
	// may be cleared without a delivery — Run's no-cursor-yet branch
	// deliberately keeps retrying r.pending INSTEAD of reopening, precisely
	// because nothing could be re-read there.
	r.pending.Discard()
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
			body, origLen := r.sanitize(e.message)
			if r.cfg.Scrub != nil {
				// Scrub before anything copies from the body (logattrs
				// lifting, enrich's exception attributes, batch accounting).
				body = r.cfg.Scrub.Scrub(body)
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
// origLen reports the RAW journal length — captured before the UTF-8
// replacement, whose replacement runes would otherwise inflate/deflate the
// advertised original size.
func (r *Reader) sanitize(msg string) (body string, origLen int) {
	raw := len(msg)
	msg = strings.ToValidUTF8(msg, "�")
	if len(msg) > r.cfg.MaxEntryBytes {
		return logchain.TruncateRunes(msg, r.cfg.MaxEntryBytes), raw
	}
	return msg, 0
}

// ingest converts one raw journal entry (body already sanitized) into the
// batch.
func (r *Reader) ingest(re rawEntry, body string, origLen int) {
	e := entry{
		unit:    re.unit,
		ident:   re.ident,
		body:    body,
		ts:      re.realtime,
		origLen: origLen,
	}
	if e.ts.IsZero() {
		e.ts = time.Now()
	}
	e.severity, e.sevText = severity(re.priority)
	if pid, err := strconv.ParseInt(re.pid, 10, 64); err == nil {
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
	// Convert ONCE per batch, not once per export ATTEMPT: convert() runs the
	// log-metrics observation, and re-running it per retry inflated
	// user-configured counters (logchain.Pending owns that discipline).
	ld := r.pending.Render(r.convert)
	if ld.LogRecordCount() == 0 {
		// Every entry was dropped by the rules. Committing without exporting is
		// the tailer's behaviour too: an empty payload still costs a wire RPC
		// per flush interval — and, with -buffer-dir, an fsync'd spool frame —
		// on exactly the heavily-sampled journal this feature exists for.
		r.settleBatch()
		return nil
	}
	if err := r.cfg.Exporter.ExportLogs(ctx, ld); err != nil {
		if logchain.SettlePermanent(err, r.log, "journal batch", ld.LogRecordCount(),
			logchain.SettleCounters{Batches: obs.JournalDropped, Records: obs.JournalDroppedRecords},
			"entries", len(r.batch)) {
			r.settleBatch()
			return nil
		}
		obs.LogExportFailures.Inc()
		return fmt.Errorf("exporting journal batch: %w", err)
	}
	// Delivered records, not ingested entries: the rules may have dropped some,
	// and the metric documents itself as entries EXPORTED.
	obs.JournalEntries.Add(float64(ld.LogRecordCount()))
	// Count truncations here, not at read time: a batch whose export fails is
	// re-read from the committed cursor and re-sanitized, so a per-read counter
	// would double-count. Like JournalEntries, this reflects delivered records.
	truncated := 0
	for i := range r.batch {
		if r.batch[i].origLen > 0 {
			truncated++
		}
	}
	if truncated > 0 {
		obs.JournalTruncated.Add(float64(truncated))
	}
	r.settleBatch()
	return nil
}

// settleBatch clears the batch (releasing the bodies pinned by the backing
// array) and commits its newest cursor.
func (r *Reader) settleBatch() {
	clear(r.batch)
	r.batch = r.batch[:0]
	r.batchBytes = 0
	// The converted payload belongs to the batch that is now gone.
	r.pending.Discard()
	r.lastFlush = time.Now()
	if r.batchCursor != "" {
		r.cursor = r.batchCursor
		r.saveCursor()
	}
}

// convert groups the batch into one resource per unit.
//
// The per-record half — line attributes, enrichment, log-metrics (which see
// EVERY entry) and the keep/drop rules (which run after enrichment, so
// __severity__ selects on the ENRICHED severity) — is the chain every log
// producer in this repo runs (internal/agent/logchain). What stays here is the
// unit's RESOURCE and the grouping.
//
// Bodies are already scrubbed: journald redacts where it builds the batch
// entry, before the record exists, so the chain's Scrub is nil.
func (r *Reader) convert() plog.Logs {
	ld := plog.NewLogs()
	// Every other log producer names its scope (tailer, events); the
	// journal's records once shipped with an empty otel_scope_name.
	groups := logchain.NewGroups(ld, ScopeName, 4)
	sink := &recordSink{r: r, observed: pcommon.NewTimestampFromTime(time.Now())}
	chain := logchain.NewChain[string](logchain.Config{
		LogAttrs:   r.cfg.LogAttrs,
		Enrich:     r.cfg.Enrich,
		LogMetrics: r.cfg.LogMetrics,
		Rules:      r.cfg.Rules,
	}, false)
	for _, e := range r.batch {
		body, extracted := chain.Line(e.body)
		unit := e.unit
		groupKey := unit
		if unit == "" {
			unit = e.ident
			// Tag ident-derived groups: a syslog identifier that happens to
			// equal another entry's full unit name must not share its group —
			// the group's resource carries systemd.unit only for real units,
			// so sharing mis-attributes whichever entry arrives second.
			groupKey = "i\x02" + unit
		}
		key := chain.GroupKey(groupKey, extracted)
		// The group is built BEFORE the record because metric and rule
		// resolution reads the group's own resource; a group the rules empty is
		// pruned below rather than never created (the tailer, whose resolution
		// uses the FILE's resource, can be lazy instead — same payload).
		sink.e, sink.unit = e, unit
		sink.e.body = body // identical while Scrub is nil; not a fact to rely on
		ent := groups.Get(key, extracted, sink)
		sink.sl = ent.SL
		chain.Emit(sink, logchain.Input[string]{
			Body: body, Lifted: extracted, Resource: ent.Res, BoundKey: key,
		})
	}
	// An all-dropped unit leaves an empty group behind; prune so the payload
	// carries no record-less ResourceLogs.
	logchain.Prune(ld)
	return ld
}

// recordSink is the chain's Producer for journal entries: the group a kept
// record lands in, and what the journal knows about the record.
type recordSink struct {
	r        *Reader
	sl       plog.ScopeLogs
	e        entry
	unit     string // e.unit with the ident fallback applied
	observed pcommon.Timestamp
}

func (s *recordSink) Dest() plog.LogRecordSlice { return s.sl.LogRecords() }

// FillResource builds a fresh unit group's resource. Identity attributes go
// in before Build so templates and the filter see them.
func (s *recordSink) FillResource(res pcommon.Resource) {
	name := s.unit
	if name == "" {
		name = "journald"
	}
	res.Attributes().PutStr("service.name", strings.TrimSuffix(name, ".service"))
	if s.e.unit != "" {
		res.Attributes().PutStr("systemd.unit", s.e.unit)
	}
	actx := attrs.Context{}
	if s.r.cfg.NodeInfo != nil {
		actx.Node = s.r.cfg.NodeInfo()
	}
	s.r.cfg.Attrs.Build(res, actx)
}

func (s *recordSink) Stamp(lr plog.LogRecord) {
	e := s.e
	lr.SetTimestamp(pcommon.NewTimestampFromTime(e.ts))
	lr.SetObservedTimestamp(s.observed)
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
}

// severity maps a syslog priority (0-7) to OTLP severity, following the
// mapping in the OpenTelemetry logs data model.
//
// The top three syslog severities are FATAL, not ERROR: emergency is FATAL3
// (23), alert FATAL2 (22), critical FATAL (21). This used to map them onto
// FATAL/ERROR3/ERROR2 — one grade too low across the board, and contradicted
// one line away by the enrich package's own syslogSeverity table, which
// logenrich.Apply then OVERWROTE the number with whenever it managed to parse
// the message. The same journal entry therefore reported a different severity
// depending on whether its body happened to look like a log line to a parser,
// which is the one thing a severity must not depend on.
//
// The text stays the source's own syslog word (the data model asks SeverityText
// to carry the original), lowercase — which is now the casing every producer in
// the repo uses, and the casing enrich writes when it overwrites. The grading
// the six canonical level names flatten away survives in the NUMBER: emerg,
// alert and crit are all "fatal" to enrich but 23/22/21 here, and notice is
// INFO2 rather than INFO.
func severity(priority string) (plog.SeverityNumber, string) {
	switch priority {
	case "0":
		return plog.SeverityNumberFatal3, "emerg"
	case "1":
		return plog.SeverityNumberFatal2, "alert"
	case "2":
		return plog.SeverityNumberFatal, "crit"
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
