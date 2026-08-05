package logchain

import "go.opentelemetry.io/collector/pdata/plog"

// Pending is a retained batch's converted OTLP payload, owning the pair
// invariant journald and the events reader each spelled with a (plog.Logs,
// bool) field pair:
//
// The payload is rendered at most ONCE per batch epoch. Conversion runs the
// shared chain — INCLUDING the log-metrics observation — so converting per
// export ATTEMPT re-observed every retained record on every failed cycle: a
// transient export failure (a collector rollout, a 503, a deadline) inflated
// user-configured counters and histograms by the number of attempts spanning
// the batch, permanently biasing cumulative series upward and showing a
// rate() spike during exactly the outage an operator is investigating.
//
// The converted flag, not a record count, says whether the payload is
// current: an all-dropped batch legitimately converts to zero records, so the
// count cannot stand in for the flag.
//
// The payload is cleared in exactly TWO situations, always WITH the batch it
// described: settling the batch (delivered, all-dropped, or permanently
// rejected), and a restart that will RE-READ the batch's entries from the
// committed cursor/position. The restart clear matters as much as the render
// discipline: the old payload describes bytes the re-read batch will carry
// again, and leaving it set made the first flush after a restart export the
// PREVIOUS payload and then commit the NEW batch's cursor — every entry the
// new batch held beyond the stale payload was skipped, permanently and
// silently. (A restart path that CANNOT re-read — journald before its first
// committed cursor — must keep the pair and keep retrying instead.)
type Pending struct {
	payload   plog.Logs
	converted bool
}

// Render returns the batch's payload, converting once per batch epoch and
// serving the same rendering to every retry after that.
func (p *Pending) Render(convert func() plog.Logs) plog.Logs {
	if !p.converted {
		p.payload = convert()
		p.converted = true
	}
	return p.payload
}

// Discard drops the payload with the batch it described (see the type comment
// for the only two legal call sites: settle, and a restart that re-reads).
func (p *Pending) Discard() {
	p.payload = plog.NewLogs()
	p.converted = false
}
