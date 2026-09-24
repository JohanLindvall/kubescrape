// Package azurediag consumes Azure diagnostic-settings output — resource
// logs AND platform metrics — from an Event Hubs namespace over its Kafka
// surface (franz-go) and exports it as OTLP: logs through the same shared
// chain as every other log pipeline, metrics converted to real OTLP gauge
// data points rather than log records.
//
// Like the Kubernetes events reader it is a cluster-scoped pipeline that
// belongs in the singleton Deployment, NOT the DaemonSet — but unlike
// events it needs no leader election: Kafka's consumer-group protocol IS
// the coordination (each partition is owned by exactly one member), so it
// runs ungated and replicas > 1 simply share partitions.
//
// Delivery is at-least-once with the offsets as the position store: a poll's
// records are converted and exported, and the group offsets are committed
// only after the collector (or the disk buffer, when -buffer-dir is on)
// acknowledges every signal. A crash or rebalance before the commit replays
// the poll — duplicates, never loss. A payload the collector PERMANENTLY
// rejects is dropped, counted, and committed past, exactly as the events
// and journald pipelines skip poison batches.
package azurediag

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/backoff"
	"github.com/JohanLindvall/kubescrape/internal/agent/logchain"
	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/agent/route"
	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// Start modes for a consumer group with no committed offsets, mirroring the
// tailer's -logs-unknown-files and the events reader's -events-start.
const (
	// StartEnd begins at each partition's high watermark (skip the backlog).
	StartEnd = "end"
	// StartBeginning replays everything the hub retains.
	StartBeginning = "start"
)

// ValidateStartMode reports an unknown start mode.
func ValidateStartMode(mode string) error {
	switch mode {
	case "", StartEnd, StartBeginning:
		return nil
	}
	return fmt.Errorf("invalid azure start mode %q (want end or start)", mode)
}

// Config configures the consumer.
type Config struct {
	// Kafka connectivity — resolved by ResolveSources in production (one
	// KafkaConfig per consumer), filled directly by tests (plaintext, no SASL).
	Kafka KafkaConfig

	// MetricPrefix prefixes converted Azure metric names (default "azure.").
	MetricPrefix string

	// Chain is the per-record log chain the tailer, journald and events run
	// too — the same levers, in the same order (logchain.Config). Unlike those
	// two, the chain itself scrubs here: the record body is the raw envelope.
	Chain logchain.Config

	Attrs *attrs.Builder

	Exporter otlpexport.Exporter
	Logger   *slog.Logger
	// Ready is called once, after the first successful poll (empty or not):
	// the group is joined and the hub reachable.
	Ready func()
	// RetryBackoff is the initial delay before retrying a failed export or
	// poll, doubled to a 30s cap.
	RetryBackoff time.Duration
}

// source is the Kafka consumer, decoupled so the reader logic is fully
// unit-tested without a broker (the journald source pattern). poll blocks
// until records, an error, or ctx; commit commits everything polled so far.
type source interface {
	// poll blocks for the next fetch. healthy reports that it was a REAL
	// fetch — records, or a genuinely clean empty one — as opposed to one
	// carrying only errors, which by its records alone is indistinguishable
	// from a clean empty poll.
	poll(ctx context.Context) (msgs [][]byte, healthy bool, err error)
	commit(ctx context.Context) error
	close()
}

// Reader consumes diagnostics and exports them. All fields are owned by the
// single Run goroutine.
type Reader struct {
	cfg Config
	log *slog.Logger
	// open creates the source; a var so tests inject a fake.
	open func() (source, error)
	// decodeWarn throttles the undecodable-data warning (see reportDecodeError),
	// commitWarn the offset-commit failure (once per poll otherwise).
	decodeWarn logdedupe.Throttle
	commitWarn logdedupe.Throttle
	// logsExport and metricsExport size each signal's export outage, so the
	// attempt that lands can say how long the collector refused and how often
	// (see export). Every failure is loud (an interval of 0): deliver's
	// back-off already spaces the attempts.
	logsExport, metricsExport logdedupe.Outage

	scratch [][]byte // GetPaths output, reused across records
}

// New creates a Reader.
func New(cfg Config) *Reader {
	if cfg.MetricPrefix == "" {
		cfg.MetricPrefix = "azure."
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	r := &Reader{cfg: cfg, log: cfg.Logger}
	r.open = func() (source, error) { return newKafkaSource(&r.cfg) }
	return r
}

// Run consumes until ctx is done. The source is (re)opened with backoff on
// unrecoverable consumer errors; per-poll export failures retry in place
// without disturbing the group membership.
func (r *Reader) Run(ctx context.Context) {
	bo := backoff.New(r.cfg.RetryBackoff)
	for ctx.Err() == nil {
		src, err := r.open()
		if err != nil {
			r.log.Warn("opening the event hubs consumer", "error", err, "backoff", bo.Delay())
			bo.Sleep(ctx)
			continue
		}
		started := time.Now()
		err = r.consume(ctx, src)
		src.close()
		if ctx.Err() != nil {
			return
		}
		bo.ResetIfHealthy(started)
		// The consumer is rebuilt for exactly one class of error — one no
		// further fetch can clear, which fatalFetchErr admits as a group or
		// cluster authorization refusal (or a closed client) — and both that
		// line and this one say the rebuild reads credentials afresh. Make it
		// so: the connection string is re-read by the SASL session itself,
		// while a cached Entra token would otherwise be re-presented for up to
		// ~55 minutes, so an operator who FIXES the role assignment would see
		// no recovery. A credential rejected at the SASL handshake does NOT
		// come through here: kgo retries that inside the connection and no
		// fetch reports it (see credentialRefusal).
		r.cfg.Kafka.invalidateCredentials()
		r.log.Warn("event hubs consumer stopped; reopening", "error", err, "backoff", bo.Delay())
		bo.Sleep(ctx)
	}
}

// consume is the poll → convert → export → commit loop over one source.
func (r *Reader) consume(ctx context.Context, src source) error {
	ready := r.cfg.Ready
	for ctx.Err() == nil {
		msgs, healthy, err := src.poll(ctx)
		if err != nil {
			return err
		}
		if ready != nil && healthy {
			// The first HEALTHY poll (even empty) means the group is joined
			// and the hub reachable. A fetch carrying only errors is not one:
			// it leaves the gate closed, so an unconsumable hub — an identity
			// without read permission on it — never reports ready, while
			// staying non-fatal, because rebuilding the client cannot fix a
			// permission problem and the LeaveGroup/JoinGroup churn would take
			// every OTHER hub in the namespace down with it.
			ready()
			ready = nil
		}
		if len(msgs) == 0 {
			continue
		}
		recs := r.decode(msgs)
		if len(recs) > 0 && !r.deliver(ctx, recs) {
			return ctx.Err()
		}
		// Commit even an all-undecodable poll: poison data must not wedge
		// the partition.
		if err := src.commit(ctx); err != nil && ctx.Err() == nil {
			obs.AzureCommitErrors.Inc()
			// Throttled: the commit runs once per poll, so on a busy hub an
			// unwritable offset is a line per fetch. The consequence is
			// redelivery (duplicates), not loss, which is why it is a Warn
			// rather than an Error — but a persistent one means the group's
			// resume point has stopped advancing and a restart will re-consume
			// everything since.
			if r.commitWarn.Allow(commitWarnEvery) {
				r.log.Warn("committing event hubs offsets failed; the records were delivered, so a redelivery produces duplicates and a restart re-consumes from the last committed offset",
					"error", err)
			}
		}
	}
	return ctx.Err()
}

// decode splits every message into records. Undecodable messages or records
// are counted and skipped — they will be committed past, as one malformed
// producer must not stall the hub. A syntax error mid-array keeps the
// records already decoded and drops the rest of that message as one error.
//
// The skip is COUNTED (obs.AzureDecodeErrors) and, throttled, LOGGED: a
// counter says data is being discarded but not what is wrong with it, and this
// is committed-past loss, so the message is gone by the time anyone looks. The
// line carries the decoder's error and the message's SIZE — never its bytes: a
// diagnostic-settings record is customer data (an activity log carries caller
// identities and request bodies), and a hub misconfigured to carry something
// else is exactly when a body would end up in the log aggregator forever.
func (r *Reader) decode(msgs [][]byte) []record {
	var recs []record
	each := func(raw []byte) error {
		rec, scratch, err := decodeRecord(raw, r.scratch)
		r.scratch = scratch
		if err != nil {
			obs.AzureDecodeErrors.Inc()
			r.reportDecodeError("record", len(raw), err)
			return nil // skip the record, keep the rest of the envelope
		}
		// signal/plural, the dimension name and values every other producer
		// uses (and the ones AzureExported already used one counter over).
		if rec.metric {
			obs.AzureRecords.WithLabelValues("metrics").Inc()
		} else {
			obs.AzureRecords.WithLabelValues("logs").Inc()
		}
		recs = append(recs, rec)
		return nil
	}
	for _, msg := range msgs {
		if err := splitEnvelope(msg, each); err != nil {
			obs.AzureDecodeErrors.Inc()
			r.reportDecodeError("envelope", len(msg), err)
		}
	}
	return recs
}

// commitWarnEvery re-warns while offset commits keep failing.
const commitWarnEvery = time.Minute

// decodeWarnEvery re-warns about undecodable Event Hubs data at this cadence. A
// producer writing the wrong shape into a hub does it for every message, so the
// unthrottled line is one per message forever.
const decodeWarnEvery = 5 * time.Minute

// reportDecodeError logs the throttled half of a decode skip. See decode on why
// the message body itself is never in it.
func (r *Reader) reportDecodeError(what string, size int, err error) {
	r.log.Debug("skipping undecodable event hubs data", "reason", what, "bytes", size, "error", err)
	if !r.decodeWarn.Allow(decodeWarnEvery) {
		return
	}
	r.log.Warn("event hubs data could not be decoded as azure diagnostics JSON; it is skipped and committed past, so it is not retried",
		"reason", what, "bytes", size, "error", err)
}

// deliver converts and exports both signals, retrying transient failures in
// place. Returns false only when ctx ended. A permanent rejection drops that
// signal's payload (counted) so the offsets can advance past the poison.
//
// Both exports are marked route.Reoffer: each signal is retried in place until
// it settles and the offsets commit only after both, so a router splitting a
// payload (only a transform script's route() does — ARM resources carry no
// namespace) may hold its default share back while a tenant route fails
// instead of spooling one copy per attempt (route/reoffer.go).
func (r *Reader) deliver(ctx context.Context, recs []record) bool {
	ctx = route.Reoffer(ctx)
	ld := r.convertLogs(recs)
	md := r.convertMetrics(recs)
	logsDone := ld.LogRecordCount() == 0
	metricsDone := md.DataPointCount() == 0
	bo := backoff.New(r.cfg.RetryBackoff)
	for !logsDone || !metricsDone {
		if ctx.Err() != nil {
			return false
		}
		if !logsDone {
			logsDone = r.export(ctx, "logs", ld.LogRecordCount(), &r.logsExport, func() error { return r.cfg.Exporter.ExportLogs(ctx, ld) })
		}
		if !metricsDone {
			metricsDone = r.export(ctx, "metrics", md.DataPointCount(), &r.metricsExport, func() error { return r.cfg.Exporter.ExportMetrics(ctx, md) })
		}
		if !logsDone || !metricsDone {
			bo.Sleep(ctx)
		}
	}
	return true
}

// export sends one signal once; true when it needs no further attempts
// (delivered, or permanently rejected and dropped — a payload the collector
// definitively refuses would wedge the partition, as everywhere else;
// logchain.SettlePermanent owns that arm).
//
// The per-attempt Warn is not throttled: deliver's backoff already spaces the
// attempts (the journald shape). What it needs besides is the END of an outage:
// without a recovery line a collector outage is a run of warnings and then
// silence, and the only way to learn that delivery resumed is to watch a counter
// stop moving — journald, events and the tailer each say so, and this reader
// did not.
func (r *Reader) export(ctx context.Context, signal string, count int, outage *logdedupe.Outage, send func() error) bool {
	err := send()
	if err == nil {
		obs.AzureExported.WithLabelValues(signal).Add(float64(count))
		r.exportSettled(signal, outage)
		return true
	}
	if logchain.SettlePermanent(err, r.log, "azure payload", count,
		logchain.SettleCounters{Batches: obs.AzureDropped, Records: obs.AzureDroppedRecords.WithLabelValues(signal)},
		"signal", signal) {
		r.exportSettled(signal, outage)
		return true
	}
	if ctx.Err() == nil {
		now := time.Now()
		outage.Fail(now, 0)
		// This pipeline's OWN transient-failure counter, per signal.
		// kubescrape_log_export_failures_total documents itself as the tailer's
		// "files rewound"; this reader owns no file and runs in the singleton
		// Deployment with -logs=false, so those increments described something
		// that had not happened — and only the LOGS signal was ever counted
		// there (the metrics one was left to the client layer's generic
		// obs.Exports{metrics,error}, which cannot say WHICH pipeline retried),
		// so a hub carrying platform metrics retried invisibly.
		obs.AzureExportFailures.WithLabelValues(signal).Inc()
		r.log.Warn("exporting azure diagnostics", "signal", signal, "error", err,
			"failures", outage.Failures(), "outage", outage.Lasted(now))
	}
	return false
}

// exportSettled ends a signal's outage, if one was running, with the one line
// that says delivery resumed.
func (r *Reader) exportSettled(signal string, outage *logdedupe.Outage) {
	if failures, lasted, ok := outage.Recover(time.Now()); ok {
		r.log.Info("azure diagnostics export recovered", "signal", signal,
			"failures", failures, "outage", lasted)
	}
}
