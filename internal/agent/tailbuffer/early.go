package tailbuffer

import "time"

// earlyReason is the bound that forced an early decision. Every per-reason
// table (earlyReasonLabel and earlyReport.by here, Buffer.earlyCount and
// Buffer.earlyPending) is an ARRAY indexed by it, so a typo'd reason cannot
// compile, where the string-keyed maps this replaced accepted one and counted
// it into a nil counter. A NEW reason left out of a table does compile —
// earlyReasonLabel is a keyed literal, so a missing entry is "", and
// reportEarly names its reasons one by one — so the tests are what catch it:
// TestEveryEarlyReasonHasALabelAndACounter for the label and counter,
// TestEveryEarlyReasonIsReportedOnTheLine for the line.
type earlyReason uint8

const (
	// reasonNone is a decision made because the window elapsed — not early.
	reasonNone earlyReason = iota
	reasonSpansPerTrace
	reasonMaxTraces
	reasonMaxSpans
	reasonShutdown
	numEarlyReasons
)

// earlyReasonLabel is each reason as the metric label reports it
// (kubescrape_tail_sampling_early_decisions_total{reason}). reasonNone has no
// series: a timely decision is not an early one.
var earlyReasonLabel = [numEarlyReasons]string{
	reasonNone:          "",
	reasonSpansPerTrace: "spans_per_trace",
	reasonMaxTraces:     "max_traces",
	reasonMaxSpans:      "max_spans",
	reasonShutdown:      "shutdown",
}

func (r earlyReason) String() string { return earlyReasonLabel[r] }

// earlyReport is one sweep's worth of early decisions, taken out of the buffer
// so the line can be written with the mutex released.
type earlyReport struct {
	// by is the window's early decisions per reason (reasonNone stays 0).
	by [numEarlyReasons]int
	// slowestExport is the longest decision-loop export in the window: the
	// loop decides nothing while it sends, so a value comparable to
	// decisionWait says the bound bound because the collector was slow, not
	// because it is sized below the shard's span rate.
	slowestExport time.Duration
}

func (e earlyReport) any() bool {
	// shutdown is deliberately excluded from "is there anything to say": a
	// graceful stop decides every buffered trace early BY DESIGN, so warning
	// about it would put a scary line in every rolling update. The count still
	// rides on the line when one of the real bounds also bound, and
	// kubescrape_tail_sampling_early_decisions_total{reason="shutdown"} carries
	// it either way.
	for r := reasonNone + 1; r < numEarlyReasons; r++ {
		if r != reasonShutdown && e.by[r] > 0 {
			return true
		}
	}
	return false
}

// takeEarlyLocked decides whether this drain may emit the aggregate line and,
// ONLY IF IT MAY, drains the pending tallies. Called with the mutex held.
//
// The order is the whole point. Draining unconditionally and throttling the
// line afterwards zeroes the tallies on every sweep — a 100ms-1s ticker — while
// the line that eventually escapes the throttle claims to describe a minute, so
// the counts it carries understate the window it names by the tick ratio
// (60-600x). Claiming the throttle slot first and zeroing only on the emitting
// drain makes the numbers describe the window they are printed against; the
// suppressed drains simply keep accumulating into it.
//
// Allow() is asked only when there is something to say, or a quiet minute would
// spend the slot and suppress the first drain that actually binds.
//
// A quiet drain DOES reset slowestExport, though, or the value the line carries
// is the slowest export since the last line, however many hours ago that was —
// and a bind caused by an undersized bound would then be blamed on a stall that
// ended long before it. The window slowestExport describes therefore opens at
// the last drain that had nothing to report. That keeps the export this
// diagnostic exists for: this runs BEFORE the drain's own send, so an export
// that stalls the loop is recorded after the reset and read by the next drain,
// which is the one that sees the binds the stall caused. A throttled window
// (something pending, Allow refused) keeps accumulating, like the tallies.
func (b *Buffer) takeEarlyLocked() (earlyReport, bool) {
	r := earlyReport{by: b.earlyPending}
	if !r.any() {
		b.slowestExport.Store(0)
		return earlyReport{}, false
	}
	if !b.earlyWarn.Allow(b.earlyEvery) {
		return earlyReport{}, false
	}
	b.earlyPending = [numEarlyReasons]int{}
	// Taken with the tallies, so it describes the same window.
	r.slowestExport = time.Duration(b.slowestExport.Swap(0))
	return r, true
}

// reportEarly is the throttled aggregate line for early decisions.
//
// An early decision is not loss — the engine reads a partial trace as a LOWER
// BOUND, so a slow trace can be missed and a fast one is never invented — but a
// sustained rate means a bound is sized below this shard's span rate, and the
// sampling an operator configured is not the sampling they are getting — or
// that the decision loop was stalled behind its own export, which fills the
// buffer just as surely (the package doc's TIME bound). The counter alone
// cannot say WHICH bound, how big the shard's backlog was when it bound, or
// which of the two causes it was, so the line carries all three.
// The throttle and the emptiness check live in takeEarlyLocked, which is what
// couples them to the drain that zeroed the tallies.
func (b *Buffer) reportEarly(r earlyReport) {
	st := b.Stats() // re-takes the mutex: the caller has released it by now
	b.log.Warn("traces are being decided before their decisionWait elapsed because a tail-sampling bound bound; the verdicts are made on the spans present, so slow traces can be missed. If slowestDecisionExport is comparable to decisionWait, the cause is export latency (the decision loop decides nothing while it sends), not a bound sized below this shard's span rate",
		// bySomething = how many this window; the bare config name = the bound
		// that forced them. The two must not share a key — a line carrying
		// maxTraces twice with different meanings is worse than either.
		// byShutdown is on the line for the reason any() gives for leaving it
		// out of the decision to WRITE one: a graceful stop decides everything
		// early by design and is not news on its own, but once a real bound
		// has forced the line out, how much of the burst was the stop and how
		// much was the bound is exactly what the operator is reading it for.
		"bySpansPerTrace", r.by[reasonSpansPerTrace], "byMaxTraces", r.by[reasonMaxTraces],
		"byMaxSpans", r.by[reasonMaxSpans], "byShutdown", r.by[reasonShutdown],
		"maxTraces", b.set.maxTraces, "maxSpans", b.set.maxSpans,
		"maxSpansPerTrace", b.set.maxSpansPerTrace,
		"bufferedTraces", st.Traces, "bufferedSpans", st.Spans,
		"slowestDecisionExport", r.slowestExport, "decisionWait", b.set.wait)
}

// earlyWarnEvery re-warns while a tail-sampling bound keeps binding.
const earlyWarnEvery = time.Minute
