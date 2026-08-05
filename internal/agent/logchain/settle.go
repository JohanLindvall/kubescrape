package logchain

// The poison-batch settle policy's PERMANENT arm, shared by the batching
// producers (journald, the events reader, azurediag). Each spelled the same
// decision: a payload the collector DEFINITIVELY refuses is counted, warned,
// and committed past — retrying it forever (or re-reading it via the restart
// path, which reproduces the same payload) would wedge the whole pipeline on
// one bad batch.
//
// The TRANSIENT arm deliberately stays with each caller: the producers do not
// count transient failures identically — azurediag's metrics-signal failures
// are already counted by the client layer's obs.Exports{metrics,error} and
// must NOT also land on kubescrape_log_export_failures — so a helper that
// owned both arms would either merge that distinction or grow a flag per
// nuance. Returning false for "transient, yours to retry" keeps the callers'
// counting exactly where it was.

import (
	"log/slog"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
)

// SettleCounters is one producer's poison-batch loss accounting. The pairs
// stay separate per pipeline BY DESIGN — an operator must see WHICH producer
// lost data — so callers pass their own counters rather than sharing one.
type SettleCounters struct {
	// Batches counts payloads dropped after a permanent rejection.
	Batches *metrics.RegCounter
	// Records counts the records lost WITH those payloads — the records, not
	// just the batch: a payload is up to a whole batch/poll of entries, so the
	// batch counter alone cannot say how much was lost. Callers count
	// delivered records the same way on success.
	Records *metrics.RegCounter
}

// SettlePermanent counts, warns and returns true when err is a PERMANENT
// rejection the caller must settle (advance its cursor/position/offsets) past;
// false means transient — the caller owns the retry and its own transient-side
// counting (see the package comment on why that arm is not unified).
//
// what names the payload in the warning ("journal batch", "event batch",
// "azure payload"); context is the caller's leading log fields, logged ahead
// of the shared records/error pair so each pipeline's line keeps its fields.
func SettlePermanent(err error, log *slog.Logger, what string, records int, c SettleCounters, context ...any) bool {
	if !otlpexport.IsPermanent(err) {
		return false
	}
	c.Batches.Inc()
	c.Records.Add(float64(records))
	log.Warn(what+" permanently rejected, skipping past it",
		append(context, "records", records, "error", err)...)
	return true
}
