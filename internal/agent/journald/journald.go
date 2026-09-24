// Package journald reads the systemd journal through libsystemd (via
// github.com/coreos/go-systemd/v22/sdjournal — cgo) and exports the entries as
// OTLP log records. Delivery is at-least-once: the cursor of the newest
// exported entry is persisted only after a successful export, and on a SOURCE
// failure the reader restarts from the persisted cursor, re-reading whatever
// was in flight. An EXPORT failure does not restart the reader — the batch is
// retried in place (flushRetry), which is what keeps the per-record chain from
// running twice over one entry.
//
// Because it links libsystemd, the agent binary is built with cgo and the
// image must provide libsystemd (see the Dockerfile). The journal itself is
// read directly — no journalctl subprocess.
package journald

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/backoff"
	"github.com/JohanLindvall/kubescrape/internal/agent/logchain"
	"github.com/JohanLindvall/kubescrape/internal/agent/positions"
	"github.com/JohanLindvall/kubescrape/internal/agent/route"
	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
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
	// persistence; every start then begins at the tail). It is written at
	// most every cursorPersistEvery and once more at shutdown, so a hard kill
	// replays up to that much (saveCursor).
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

	// Chain is the per-record log chain every producer runs (scrub → lift →
	// enrich → log-metrics → rules; see logchain.Config for each lever): the
	// same levers, in the same order and with the same semantics, as the
	// tailer's. Enrichment's explicit level wins over the journal priority;
	// rules run after log-metrics, so a metric counts every entry, and the
	// journal is dominated by kubelet/containerd chatter, which is exactly the
	// volume to count and then drop. Chain.Scrub is applied where the batch
	// entry is BUILT, before the record exists (stream), not by the chain.
	Chain logchain.Config

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
// 20-30 cgo string copies per entry where six suffice.
type rawEntry struct {
	message   string
	unit      string // _SYSTEMD_UNIT
	ident     string // SYSLOG_IDENTIFIER
	priority  string
	pid       string // _PID
	transport string // _TRANSPORT (journal/stdout/kernel/syslog/audit/driver)
	cursor    string
	realtime  time.Time
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
	cfg Config
	log *slog.Logger
	// defectWarn throttles the read-side repair warnings, ONE GATE PER DEFECT
	// CLASS (invalid UTF-8, a missing timestamp), indexed by the defect's label
	// value. A unit logging raw bytes does it on EVERY message, so an
	// unthrottled line would be one per entry from precisely the noisiest
	// producer; the rate is kubescrape_journal_entry_defects_total.
	//
	// Separate gates because the two classes are INDEPENDENT conditions with
	// different remedies (a producer writing binary; a producer or transport
	// handing over no realtime stamp) and they co-occur — an audit or
	// raw-byte-emitting unit is exactly the kind that also arrives stampless.
	// One shared gate let whichever fired first suppress the other for the
	// whole five minutes, so the second condition could be permanently
	// invisible in the logs while its counter climbed. Same rule, and same
	// reason, as transform/hooks.go giving the parse-shape complaint its own
	// gate and the tailer giving budgetWarn one beside metaWarn.
	defectWarn map[string]*logdedupe.Throttle
	// unitDebug bounds the per-unit Debug line that answers the one question
	// this reader could not answer below the aggregate counters: "why is
	// kubelet.service not shipping?" — is it reaching us at all, and are its
	// entries surviving the rules? Keyed by unit because that IS the question,
	// bounded because SYSLOG_IDENTIFIER (the fallback for a transport with no
	// unit) is producer-chosen and therefore unbounded, and re-reported on a
	// window so a unit that goes quiet stops appearing rather than being
	// remembered forever as live.
	unitDebug *logdedupe.Table
	// unitCounts is the per-batch scratch behind that report, keyed by group
	// (entry.groupUnit) and reused so the summary costs no allocation per
	// settle. It is only ever filled when Debug is enabled.
	unitCounts map[string]unitTally
	// exportOutage is the run of consecutive failed export attempts of the
	// batch currently being retried in place, so the recovery can say how long
	// the collector was refusing it. Ended by a successful flush.
	exportOutage logdedupe.Outage
	open         openFunc

	batch       []entry
	batchBytes  int    // summed body sizes of the buffered entries
	cursor      string // last successfully exported cursor
	batchCursor string // cursor of the newest buffered entry
	// savedCursor is the cursor the positions file holds as far as this reader
	// knows (the one it loaded, or the last it wrote), and lastCursorSave when
	// it last wrote one. cursorPersistEvery rate-limits those writes — see
	// saveCursor — and now is the clock it reads, injectable for tests (the
	// store.now pattern).
	savedCursor        string
	lastCursorSave     time.Time
	cursorPersistEvery time.Duration
	now                func() time.Time
	// cursorOutage narrates an unwritable positions file: the first failure of
	// each outage warns, the repeats at most every cursorWarnEvery, and the
	// first write that lands after one says so (saveCursor).
	cursorOutage logdedupe.Outage
	// pending is the batch converted to OTLP, held across export retries
	// under logchain.Pending's convert-once/clear-with-the-batch discipline.
	pending logchain.Pending
}

// unitTally is one group's line in the per-unit Debug report.
type unitTally struct {
	name    string // the name the group's resource carries (entry.groupUnit)
	entries int
}

type entry struct {
	unit      string // _SYSTEMD_UNIT; see groupUnit for the resource's name
	body      string
	ts        time.Time
	severity  plog.SeverityNumber
	sevText   string
	pid       int64
	ident     string // SYSLOG_IDENTIFIER
	transport string // _TRANSPORT; distinguishes kernel/stdout/syslog streams
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
	return &Reader{
		cfg: cfg, log: cfg.Logger, open: openJournal,
		cursorPersistEvery: cursorPersistEvery, now: time.Now,
		// One gate per defect class, built here so reportDefect's lookup can
		// never mint one lazily on the reader goroutine's hot path — and so a
		// class added without a gate reports its counter and stays silent
		// rather than sharing another class's window.
		defectWarn: map[string]*logdedupe.Throttle{
			defectInvalidUTF8: {},
			defectNoTimestamp: {},
		},
		unitDebug:  logdedupe.New(maxDebugUnits, unitDebugEvery),
		unitCounts: make(map[string]unitTally, 16),
	}
}

// Run reads until ctx is done, restarting the reader on any failure.
func (r *Reader) Run(ctx context.Context) {
	r.cursor = r.loadCursor()
	r.savedCursor = r.cursor
	// Lifecycle, once, and the one journald fact an operator needs on a first
	// live run: WHERE this reader starts. With no stored cursor the source
	// seeks to the journal TAIL, so everything already in the journal is never
	// exported — which is correct (it is history) but is indistinguishable
	// from a broken reader if you are looking for last night's kubelet logs
	// and nobody said so. A resumed cursor is the other half of the same
	// answer: entries between the cursor and now WILL be re-read.
	//
	// The cursor is opaque and can be long, so its length stands in for it —
	// enough to tell "resuming" from "empty" without putting an unbounded
	// token on every startup line.
	r.log.Info("journal reader starting", "start", r.startLabel(), "dir", r.cfg.Dir,
		"units", len(r.cfg.Units), "interval", r.cfg.FlushInterval,
		"cursorPersisted", r.cfg.Positions != nil)
	bo := backoff.New(r.cfg.RestartBackoff)
	for ctx.Err() == nil {
		// No batch can reach this point: stream retries a failed export IN
		// PLACE and only ever returns once the batch is settled or ctx is dead
		// (flushRetry), so the loop restarts the SOURCE and nothing else. This
		// used to carry a special case for the cursor-less first run — a reopen
		// with an empty cursor seeks to the journal TAIL, so the buffered
		// entries would have been gone — and flushRetry generalises it: no
		// batch is discarded for an export failure, cursor or not.
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
	// owns ONCE A CURSOR EXISTS: the cursor is committed only on a successful
	// export, so a dropped final batch is re-read from the journal after the
	// restart. The exception is a shutdown before ANY cursor has been committed
	// (first run, or no positions store) — a reopen with an empty cursor seeks
	// to the journal TAIL, so a lost final batch cannot be recovered; the
	// in-place retry inside stream narrows that window but the final flush here
	// is single-shot, because it is the one flush that must fit a budget.
	//
	// WithoutCancel, not Background: the values the caller put on ctx (the
	// otlpexport ownership marker rides there) must survive.
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownFlushBudget)
	defer cancel()
	if err := r.flush(fctx); err != nil {
		r.log.Warn("final journal flush failed", "error", err)
	}
	// Whatever the final flush did: r.cursor only ever holds a cursor whose
	// batch settled, so writing it is always safe, and it is what the
	// rate-limited saves during the run may still owe (saveCursor).
	r.saveCursor(true)
}

// cursorPersistEvery is how often a committed cursor is written to the
// positions file — the tailer's checkpoint cadence, and for the same reason
// (saveCursor).
const cursorPersistEvery = 10 * time.Second

// shutdownFlushBudget bounds the final flush above. It matches the tailer's
// defaultShutdownBudget and the agent's other final exports; the sum has to fit
// inside the pod's terminationGracePeriodSeconds.
const shutdownFlushBudget = 10 * time.Second

// maxDebugUnits and unitDebugEvery bound the per-unit Debug report (see
// Reader.unitDebug). A node runs tens of units, so the cap is only ever reached
// through SYSLOG_IDENTIFIER, which a producer chooses; past it the table
// suppresses new keys and says so once, per logdedupe's saturation rule. The
// window is short because this line exists to be watched DURING an incident,
// where a five-minute silence about a unit reads as the unit having stopped.
const (
	maxDebugUnits  = 256
	unitDebugEvery = time.Minute
)

// stream opens one journal source and reads until it ends, the source errors,
// or ctx is done. An export failure does NOT end it: flushRetry keeps the batch
// and retries it in place, so the entries are never re-read and the per-record
// chain never runs over them twice.
func (r *Reader) stream(ctx context.Context) error {
	src, err := r.open(r.cfg, r.cursor)
	if err != nil {
		return err
	}
	defer func() { _ = src.close() }()
	// Where this source is positioned, and — the half the startup line reports
	// only as a COUNT — which units the journal itself will hand over. Matches
	// are applied inside libsystemd, so an entry from a unit outside the list
	// never reaches this process at all: with the names logged, "why is
	// containerd.service not shipping?" is answerable from the log instead of
	// from re-reading the ConfigMap. Guarded because the Join allocates, and
	// once per source open (a restart, not a batch) either way.
	if r.log.Enabled(ctx, slog.LevelDebug) {
		r.log.Debug("journal source opened", "start", r.startLabel(), "dir", r.cfg.Dir,
			"units", strings.Join(r.cfg.Units, ","))
	}

	// The CONVERTED payload belongs to the batch discarded here, and goes with
	// it (resetBatch): this reopen re-reads those entries from the committed
	// cursor (logchain.Pending's restart-clear case). Only a SOURCE failure gets
	// here with anything buffered — an export failure retries in place — and
	// even that path flushes first (the !ok arm below runs before the read error
	// is returned), so the clear is normalisation rather than a live loss path.
	// It stays because the alternative, a payload outliving the batch it
	// describes, exports the PREVIOUS batch and then commits the NEW one's
	// cursor.
	r.resetBatch()

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

	// The ticker is the "flush at least this often" promise, and it is measured
	// from the LAST FLUSH, not from the last tick: flushNow resets it after
	// every flush, whatever triggered that flush. The guard this replaced —
	// tick AND time.Since(lastFlush) >= FlushInterval, with lastFlush stamped
	// after the flush completed — could never be satisfied on the tick it was
	// meant for, because a fixed-period ticker fires exactly FlushInterval after
	// the PREVIOUS TICK, which is microseconds BEFORE that interval has elapsed
	// since the flush the tick caused. Every tick was therefore skipped and the
	// flush landed on the next one: a measured 2.00x the configured interval
	// (2s -> ~3.9s, 10s -> ~20s), for a flag documented as an upper bound.
	ticker := time.NewTicker(r.cfg.FlushInterval)
	defer ticker.Stop()
	flushNow := func() error {
		if err := r.flushRetry(ctx); err != nil {
			return err
		}
		ticker.Reset(r.cfg.FlushInterval)
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if len(r.batch) > 0 {
				if err := flushNow(); err != nil {
					return err
				}
			}
		case e, ok := <-entries:
			if !ok {
				if err := flushNow(); err != nil {
					return err
				}
				select {
				case err := <-readErr:
					return fmt.Errorf("reading journal: %w", err)
				default:
					return errors.New("journal source ended")
				}
			}
			body, origLen := r.sanitize(e.message, e.unit)
			if r.cfg.Chain.Scrub != nil {
				// Scrub before anything copies from the body (logattrs
				// lifting, enrich's exception attributes, batch accounting).
				body = r.cfg.Chain.Scrub.Scrub(body)
			}
			// Flush BEFORE the entry that would push the batch over the byte
			// cap. A single entry already over the cap still exports alone
			// (entries are never split), so one payload can exceed it by up
			// to MaxEntryBytes.
			if len(r.batch) > 0 && r.batchBytes+len(body) > r.cfg.MaxBatchBytes {
				if err := flushNow(); err != nil {
					return err
				}
			}
			r.ingest(e, body, origLen)
			if len(r.batch) >= r.cfg.BatchSize {
				if err := flushNow(); err != nil {
					return err
				}
			}
		}
	}
}

// ingest converts one raw journal entry (body already sanitized) into the
// batch.
func (r *Reader) ingest(re rawEntry, body string, origLen int) {
	e := entry{
		unit:      re.unit,
		ident:     re.ident,
		transport: re.transport,
		body:      body,
		ts:        re.realtime,
		origLen:   origLen,
	}
	if e.ts.IsZero() {
		// The journal handed over no realtime stamp, so the record is dated
		// with OUR clock at read time. That is a silent substitution — the
		// exported timestamp looks exactly as authoritative as a real one — and
		// it is what makes a backlog read after a restart appear to have
		// happened all at once.
		r.reportDefect(defectNoTimestamp, re.unit)
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

// flushRetry exports the batch, RETRYING THE SAME BATCH IN PLACE until it
// settles (delivered, all-dropped, or permanently rejected) or ctx is done. It
// is the only flush the read loop uses.
//
// Why in place rather than tearing the reader down and re-reading from the
// committed cursor, which is what an export failure used to do:
//
// Re-reading rebuilds the batch, and rebuilding re-runs the per-record chain —
// the log-metrics observations and the keep/drop rules. Delivery is
// at-least-once and duplicate RECORDS are fine (the collector dedupes nothing,
// but the data is the same); duplicate OBSERVATIONS are not, because a counter
// or histogram the operator configured is cumulative. Measured on the shape
// TestJournaldObservesOncePerDeliveryAcrossACollectorOutage drives — six
// entries, a collector refusing eight attempts — the old restart-and-re-read
// path counted 46 observations for those six entries and nine rule drops for
// the one entry the rules actually dropped: the metric lying by ~8x at exactly
// the moment it was being read to diagnose the outage, and worse the longer the
// outage ran. Retrying in place converts once (logchain.Pending) and observes
// once.
//
// The batch is bounded (BatchSize / MaxBatchBytes) and the journal is the
// buffer behind it — the reader goroutine simply blocks handing over the next
// entry — so holding it costs a bounded amount of memory and loses nothing: the
// cursor is committed only on a successful export, so a crash mid-retry re-reads
// from the journal exactly as a crash mid-stream would.
//
// The retry is deliberately unbounded. Giving up would mean discarding the
// batch or advancing past it, and both are worse than waiting for a collector
// that will come back; a genuinely undeliverable payload is the PERMANENT case,
// which flush settles on its own.
func (r *Reader) flushRetry(ctx context.Context) error {
	bo := backoff.New(r.cfg.RestartBackoff)
	for {
		// A dead context is a SHUTDOWN, not a collector problem, and the check
		// belongs at the TOP: backoff.Sleep returns EARLY when ctx is done, so
		// without it the cancellation that ends the wait was immediately spent
		// on one more ExportLogs that could only fail with context.Canceled —
		// an attempt against a collector that was never asked, and a spike on
		// kubescrape_journal_export_failures_total on every rolling update.
		// Returning here leaves the batch and its rendered payload intact for
		// Run's final flush, which carries them on a detached budgeted context.
		if err := ctx.Err(); err != nil {
			return err
		}
		err := r.flush(ctx)
		if err == nil {
			// The recovery half. Without it a collector outage produces a
			// warning per retry and then silence, and the only way to learn
			// that delivery resumed is to watch a counter stop moving.
			if failures, lasted, ok := r.exportOutage.Recover(time.Now()); ok {
				r.log.Info("journal export recovered", "failures", failures, "outage", lasted)
			}
			return nil
		}
		now := time.Now()
		r.exportOutage.Fail(now, 0) // every attempt is loud: the backoff spaces them
		if ctx.Err() != nil {
			// Cancelled DURING the export: that attempt really was made and its
			// failure really was counted, so this arm only avoids the backoff.
			return err
		}
		// Not throttled: the backoff already spaces this out (it grows to the
		// cap and the batch is held, so the line rate falls as the outage
		// lasts), and every line carries the attempt count an operator sizes
		// the outage by.
		r.log.Warn("journal export failed; retrying the same batch (re-reading it would re-observe its log metrics)",
			"entries", len(r.batch), "error", err, "backoff", bo.Delay(),
			"failures", r.exportOutage.Failures(),
			"outage", r.exportOutage.Lasted(now))
		bo.Sleep(ctx)
	}
}

// flush exports the batch once; on success the newest cursor is committed. A
// batch the collector permanently rejects is dropped and its cursor committed
// too — retrying it forever would wedge the reader on one poison batch.
// Transient failures return the error and leave the batch (and its converted
// payload) intact for flushRetry; the only single-attempt caller is Run's final
// flush, which has a budget to fit.
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
		r.settleBatch(ctx, 0)
		return nil
	}
	// route.Reoffer: flushRetry re-sends this same payload until it settles, so
	// a router splitting it (only a transform script's route() does — journal
	// resources carry no namespace) may hold the default share back while a
	// tenant route fails instead of spooling one copy per attempt
	// (route/reoffer.go).
	if err := r.cfg.Exporter.ExportLogs(route.Reoffer(ctx), ld); err != nil {
		if logchain.SettlePermanent(err, r.log, "journal batch", ld.LogRecordCount(),
			logchain.SettleCounters{Batches: obs.JournalDropped, Records: obs.JournalDroppedRecords},
			"entries", len(r.batch)) {
			r.settleBatch(ctx, 0)
			return nil
		}
		// The journal's OWN failure counter, not the tailer's
		// kubescrape_log_export_failures_total: that one documents itself as
		// "files rewound", and this reader rewinds no file — it does not even
		// need -logs to be enabled, so on a journal-only agent every increment
		// of it named a file that could not exist.
		obs.JournalExportFailures.Inc()
		return fmt.Errorf("exporting journal batch: %w", err)
	}
	// Delivered records, not ingested entries: the rules may have dropped some,
	// and the metric documents itself as entries EXPORTED.
	obs.JournalEntries.Add(float64(ld.LogRecordCount()))
	r.settleBatch(ctx, ld.LogRecordCount())
	return nil
}

// settleBatch clears the batch (releasing the bodies pinned by the backing
// array), counts its truncations and commits its newest cursor.
func (r *Reader) settleBatch(ctx context.Context, delivered int) {
	// Truncations are counted on SETTLE because settle is the one point every
	// terminal path meets — delivered, emptied by the rules, permanently
	// rejected — and each batch reaches it exactly once. Counting after the
	// SUCCESSFUL export instead made the metric depend on BATCH COMPOSITION:
	// the same cut message was tallied or not according to whether some
	// unrelated sibling entry survived the rules, and a journal the rules empty
	// (the heavily-sampled node this feature exists for) reported no
	// truncations at all. It scans r.batch — every entry the batch held — so
	// unlike JournalEntries (delivered records) it also counts a truncation on
	// an entry the rules dropped: truncation is a read-side sanitation event
	// and the rules run downstream of it.
	//
	// This used to be argued from the transient-failure RE-READ, which a
	// read-time counter would have double-counted. That path is gone — an
	// export failure retries the same batch in place (flushRetry) and the
	// entries are never re-read — so a read-time counter would now be correct
	// too. The batch-composition argument above is the one that still holds,
	// and it is why this did not move back.
	truncated := 0
	for i := range r.batch {
		if r.batch[i].origLen > 0 {
			truncated++
		}
	}
	if truncated > 0 {
		obs.JournalTruncated.Add(float64(truncated))
	}
	// Per BATCH and per UNIT, never per entry: this is the reader's only
	// answer, below the aggregate counters, to "this unit is in the journal —
	// where are its logs?". entries vs delivered separates "nothing arrived"
	// from "the rules dropped it", the per-unit lines say which units the
	// journal is actually handing over, and settle is the one point every
	// terminal path meets exactly once per batch (a retried export must not
	// re-report the batch it is still holding).
	r.debugBatch(ctx, delivered, truncated)
	cursor := r.batchCursor
	r.resetBatch()
	if cursor != "" {
		// The commit is IN MEMORY at once — it is where a reader restart
		// reopens — and durable at the positions file's cadence (saveCursor).
		r.cursor = cursor
		persisted := r.saveCursor(false)
		// The commit is what a restart resumes from, so an operator chasing a
		// gap needs to see that it moved (and whether the positions file holds
		// it yet: with no positions store it never will). The cursor itself is
		// opaque and unbounded, so its length stands in for it — the same
		// substitution the startup line makes. Every argument is a field read
		// or a len.
		r.log.Debug("journal cursor committed", "cursorLen", len(r.cursor),
			"persisted", persisted)
	}
}

// resetBatch empties the batch and everything that describes it — the bodies
// its backing array pins, the byte and cursor accounting, and the converted
// payload (logchain.Pending's clear-with-the-batch discipline). A stream
// (re)open and a settle are the two places a batch ends.
func (r *Reader) resetBatch() {
	clear(r.batch)
	r.batch = r.batch[:0]
	r.batchBytes = 0
	r.batchCursor = ""
	r.pending.Discard()
}

// startLabel names where the source opens: at the committed cursor, or — with
// none stored — at the journal TAIL, so nothing already in the journal is
// exported (see Run).
func (r *Reader) startLabel() string {
	if r.cursor != "" {
		return "cursor"
	}
	return "tail"
}

// debugBatch reports one settled batch and the units in it, at Debug.
//
// Everything here — the per-unit tally included — happens only when Debug is
// enabled: slog evaluates arguments eagerly, so an unguarded version would pay
// the walk and the map on every flush at Info, which is the exact defect this
// campaign found elsewhere. The tally map is reused across batches, so the
// report allocates nothing steady-state even when it IS enabled.
func (r *Reader) debugBatch(ctx context.Context, delivered, truncated int) {
	if !r.log.Enabled(ctx, slog.LevelDebug) {
		return
	}
	clear(r.unitCounts)
	for i := range r.batch {
		// Tallied by convert's GROUP key and named by the same rule, so one
		// line here is one resource there, named as that resource is: a syslog
		// identifier equal to a real unit's name is two groups, and an entry
		// carrying neither is "journald".
		name, key := r.batch[i].groupUnit()
		t := r.unitCounts[key]
		t.name, t.entries = name, t.entries+1
		r.unitCounts[key] = t
	}
	r.log.Debug("journal batch settled", "entries", len(r.batch), "records", delivered,
		"units", len(r.unitCounts), "bytes", r.batchBytes, "truncated", truncated)
	for key, t := range r.unitCounts {
		allow, saturated := r.unitDebug.Allow(key)
		if saturated {
			// logdedupe's rule: the table suppresses new keys rather than
			// clearing, and says so once. Reached only through a producer
			// minting syslog identifiers, never through real units.
			r.log.Debug("journal per-unit reporting is truncated; further units are not named",
				"count", maxDebugUnits)
		}
		if allow {
			r.log.Debug("journal unit active", "unit", t.name, "entries", t.entries)
		}
	}
}

func (r *Reader) loadCursor() string {
	if r.cfg.Positions != nil {
		return r.cfg.Positions.JournalCursor()
	}
	return ""
}

// saveCursor persists the committed cursor to the shared positions store and
// reports whether the store now holds it.
//
// It is RATE-LIMITED to cursorPersistEvery unless forced (Run's end), because
// the write is not a cursor write: positions.Store rewrites the WHOLE shared
// document — every tailed file's offset as well — with write + fsync + rename +
// directory fsync, under the mutex the tailer's own saves take. Measured at
// ~11 ms per save (12 ms with 1000 tracked files), and the byte-identical skip
// inside the store can never fire here because the cursor moves on every batch.
// Once per batch was 5x the tailer's deliberate 10s cadence at the default 2s
// flush, and one fsync'd rewrite per 1024 entries serialised with the export
// while a cursor resume reads a backlog.
//
// What the limit costs is replay, never loss: the stored cursor stays a lower
// bound on what was delivered (r.cursor only ever holds a settled batch's
// cursor), so a hard kill re-reads at most one interval of entries — the
// at-least-once duplicates a crash already produced. The ONE exception is a
// store holding NO cursor yet: a reopen with an empty cursor seeks to the
// journal TAIL, so everything delivered before the first write would be
// unrecoverable-and-unreplayed rather than replayed. The first commit is
// therefore written at once, keeping that window exactly as narrow as it was.
func (r *Reader) saveCursor(force bool) bool {
	if r.cfg.Positions == nil {
		return false
	}
	switch r.cursor {
	case "":
		return false
	case r.savedCursor:
		return true
	}
	now := r.now()
	if !force && r.savedCursor != "" && now.Sub(r.lastCursorSave) < r.cursorPersistEvery {
		return false
	}
	r.lastCursorSave = now
	if err := r.cfg.Positions.SetJournalCursor(r.cursor); err != nil {
		// Throttled, like the tailer's write to the SAME store: a read-only
		// mount or a full disk persists, and the write is retried every
		// cursorPersistEvery — six identical lines a minute from every node for
		// one unchanging fact, which kubescrape_positions_save_errors_total
		// already counts. The first failure of every run warns.
		if _, loud := r.cursorOutage.Fail(now, cursorWarnEvery); loud {
			r.log.Warn("writing journal cursor to positions file", "error", err)
		}
		return false
	}
	if failures, lasted, ok := r.cursorOutage.Recover(now); ok {
		r.log.Info("journal cursor write recovered", "failures", failures, "outage", lasted)
	}
	r.savedCursor = r.cursor
	return true
}

// cursorWarnEvery re-warns about an unwritable positions file at this cadence
// — the tailer's, for the same store.
const cursorWarnEvery = time.Minute
