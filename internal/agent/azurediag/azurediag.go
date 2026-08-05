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
	"github.com/JohanLindvall/kubescrape/internal/agent/logscrub"
	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
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
	// Kafka connectivity — filled by ApplyKafka in production, directly by
	// tests (plaintext, no SASL).
	Kafka KafkaConfig

	// MetricPrefix prefixes converted Azure metric names (default "azure.").
	MetricPrefix string

	// Enrich, Scrub, LogAttrs, Rules and LogMetrics are the same levers the
	// tailer, journald and events apply, in the same order.
	Enrich     bool
	Scrub      *logscrub.Scrubber
	LogAttrs   *logattrs.Extractor
	Rules      *logline.LineFilter
	LogMetrics *metrics.DynamicMetricSet

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
	poll(ctx context.Context) ([][]byte, error)
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
		r.log.Warn("event hubs consumer stopped; reopening", "error", err, "backoff", bo.Delay())
		bo.Sleep(ctx)
	}
}

// consume is the poll → convert → export → commit loop over one source.
func (r *Reader) consume(ctx context.Context, src source) error {
	ready := r.cfg.Ready
	for ctx.Err() == nil {
		msgs, err := src.poll(ctx)
		if err != nil {
			return err
		}
		if ready != nil {
			// The first poll returning at all (even empty) means the group
			// is joined and the hub reachable.
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
			r.log.Warn("committing event hubs offsets", "error", err)
		}
	}
	return ctx.Err()
}

// decode splits every message into records. Undecodable messages or records
// are counted and skipped — they will be committed past, as one malformed
// producer must not stall the hub. A syntax error mid-array keeps the
// records already decoded and drops the rest of that message as one error.
func (r *Reader) decode(msgs [][]byte) []record {
	var recs []record
	each := func(raw []byte) error {
		rec, scratch, err := decodeRecord(raw, r.scratch)
		r.scratch = scratch
		if err != nil {
			obs.AzureDecodeErrors.Inc()
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
		}
	}
	return recs
}

// deliver converts and exports both signals, retrying transient failures in
// place. Returns false only when ctx ended. A permanent rejection drops that
// signal's payload (counted) so the offsets can advance past the poison.
func (r *Reader) deliver(ctx context.Context, recs []record) bool {
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
			logsDone = r.export(ctx, "logs", ld.LogRecordCount(), func() error { return r.cfg.Exporter.ExportLogs(ctx, ld) })
		}
		if !metricsDone {
			metricsDone = r.export(ctx, "metrics", md.DataPointCount(), func() error { return r.cfg.Exporter.ExportMetrics(ctx, md) })
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
func (r *Reader) export(ctx context.Context, signal string, count int, send func() error) bool {
	err := send()
	if err == nil {
		obs.AzureExported.WithLabelValues(signal).Add(float64(count))
		return true
	}
	if logchain.SettlePermanent(err, r.log, "azure payload", count,
		logchain.SettleCounters{Batches: obs.AzureDropped, Records: obs.AzureDroppedRecords.WithLabelValues(signal)},
		"signal", signal) {
		return true
	}
	if ctx.Err() == nil {
		if signal == "logs" {
			// The metrics signal is already counted by the client layer's
			// obs.Exports{metrics,error}; kubescrape_log_export_failures
			// must not absorb it.
			obs.LogExportFailures.Inc()
		}
		r.log.Warn("exporting azure diagnostics", "signal", signal, "error", err)
	}
	return false
}
