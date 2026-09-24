package obs

import (
	"github.com/JohanLindvall/kubescrape/internal/metrics"
)

// Disk buffer and positions (agent).
var (
	// BufferTruncated counts bytes the disk buffer lost to damage discovered
	// at OPEN (truncated tails, dropped or foreign segments — diskqueue's
	// open-time loss counters). A crash mid-append costs one torn record;
	// anything larger means corruption cost fsynced records.
	BufferTruncated = Registry.CounterVec("kubescrape_buffer_truncated_bytes_total",
		"Bytes the disk buffer lost to damage discovered at open (truncated, dropped or foreign segments).", "signal")

	// Data loss is counted TWICE over: once per batch and once per record.
	//
	// A batch here is 1..1024 records, so a batch counter alone answers "did we
	// lose anything" and cannot answer "how much" — and this pair is the alert
	// the documented durable configuration points at (with -buffer-dir the
	// tailer sees the ENQUEUE verdict, so the collector's permanent rejections
	// surface here rather than on kubescrape_log_permanent_dropped_total, which
	// has always counted records). Every other producer's drop counter gets the
	// same treatment, and the ones that count batches now say so in the name.
	BufferDroppedBatches = Registry.CounterVec("kubescrape_buffer_dropped_batches_total",
		"Buffered batches dropped after a permanent collector rejection (bad payload, auth, unimplemented).", "signal")
	BufferDroppedRecords = Registry.CounterVec("kubescrape_buffer_dropped_records_total",
		"Records lost with those batches: log records, metric data points or spans, by signal. A batch whose payload no longer DECODES is counted in kubescrape_buffer_dropped_batches_total only — its record count is not recoverable — so this is a lower bound whenever kubescrape_buffer_read_errors_total is also moving.", "signal")
	BufferRequeued = Registry.CounterVec("kubescrape_buffer_requeued_total",
		"Buffered batches moved to the back of the queue after repeated transient failures (keeps one stuck batch from blocking the signal).", "signal")
	BufferFull = Registry.CounterVec("kubescrape_buffer_full_total",
		"Batches the disk buffer refused: the undelivered backlog is at its cap, or one batch exceeds the whole cap. Back-pressure for logs (the tailer rewinds and re-reads). A lost batch for scrape and self-metrics payloads, though the next one carries their current values again. Log-metrics treats a full backlog as transient and re-offers the samples on its next export, losing only what its re-offer buffer cannot hold; a batch larger than the whole cap is permanent for it and dropped (both land on kubescrape_log_metrics_dropped_undelivered_total). Counted per refusal, not per batch: the tailer's in-flush retries can refuse one batch up to three times.", "signal")
	// BufferEnqueueErrors counts write-side refusals that are NOT capacity:
	// a latched fsync failure, a closed queue, ENOSPC from segment
	// preallocation. For scrape and self-metrics payloads the batch is gone
	// (the next carries current values again); log-metrics re-offers its
	// samples, because otlpexport.IsPermanent classifies all three as
	// transient. Either way every other buffer metric stays flat while it
	// happens.
	BufferEnqueueErrors = Registry.CounterVec("kubescrape_buffer_enqueue_errors_total",
		"Batches the disk buffer refused for a reason other than capacity (I/O error, closed queue, no space left on device).", "signal")
	BufferReadErrors = Registry.CounterVec("kubescrape_buffer_read_errors_total",
		"Disk-buffer read failures while draining. lost=true is reported corruption the queue advanced past (its Stats carry the magnitude); lost=false left the queue in place for a retry.", "signal", "lost")
	PositionsCorrupt = Registry.Counter("kubescrape_positions_corrupt_total",
		"Positions files that failed to parse at startup (whatever decoded is kept; the affected inputs re-read "+
			"their window). Recurring bumps across restarts point at a failing disk, not a one-off crash.")
	// PositionsSaveErrors is the write-side counterpart: offsets are silently
	// NOT being persisted, so a restart re-reads (or, with an empty store,
	// skips) per -logs-unknown-files while every other metric stays flat — the
	// same dark-node failure kubescrape_buffer_enqueue_errors_total exists for.
	PositionsSaveErrors = Registry.Counter("kubescrape_positions_save_errors_total",
		"Failed writes of the positions file (committed offsets and the journald cursor are not being persisted). Any sustained rate means a bad path, a read-only mount or a full disk.")
)

// RegisterBufferStats exposes the disk buffer's per-signal backlog as gauges
// evaluated at export time.
//
// Every other buffer metric is a counter that only moves once data is ALREADY
// being dropped or refused (dropped/full/read_errors): by then the collector
// has been degrading for a while and the tailer is back-pressured. The backlog
// against its cap is the leading indicator — "the spool is at 70% and
// climbing" — and it was previously unobservable even though the spool had
// tracked the number all along.
//
// stats is sampled once per export or scrape (metrics.PerPass): each call
// walks every signal's queue Stats, and the three families must describe one
// instant.
func RegisterBufferStats(stats func() map[string]BufferStat) {
	stats = metrics.PerPass(Registry, stats)
	Registry.GaugeFuncVec("kubescrape_buffer_backlog_bytes",
		"Undelivered bytes currently queued in the disk buffer, per signal (what -buffer-max-bytes caps). signal=\"traces\" exists only on the trace tier with tail sampling on — the one trace payload this agent owns rather than forwards.",
		"signal", perSignal(stats, func(st BufferStat) float64 { return float64(st.Backlog) }))
	Registry.GaugeFuncVec("kubescrape_buffer_max_bytes",
		"Configured disk-buffer cap per signal (0 = uncapped); backlog/max is the utilisation to alert on.",
		"signal", perSignal(stats, func(st BufferStat) float64 { return float64(st.Cap) }))
	Registry.GaugeFuncVec("kubescrape_buffer_segments",
		"Disk-buffer segment files on disk per signal. Physical footprint can exceed the backlog by up to one segment (a delivered but unreclaimed prefix).",
		"signal", perSignal(stats, func(st BufferStat) float64 { return float64(st.Segments) }))
}

// perSignal projects one BufferStat field into a GaugeFuncVec's per-signal
// values.
func perSignal(stats func() map[string]BufferStat, field func(BufferStat) float64) func() map[string]float64 {
	return func() map[string]float64 {
		out := map[string]float64{}
		for sig, st := range stats() {
			out[sig] = field(st)
		}
		return out
	}
}

// BufferStat is one signal's disk-buffer occupancy.
type BufferStat struct {
	Backlog  int64
	Cap      int64
	Segments int
}
