package transform

// Handoff: which side of the transform seam still needs the payload OBJECT.
//
// Scripts mutate pdata in place through the host objects, so the wrapper's
// default is to run them on a deep COPY of every batch: several producers
// re-offer the SAME object after a failed export, and an in-place run would
// re-transform a script's own output on the retry (scripts are not
// idempotent — one appending to a body, or dropping on a condition its own
// output satisfies, corrupts the payload a little more per attempt). The copy
// is also what keeps a payload's other readers honest — the spanmetrics tap
// Consumes the batch it forwarded, and must see what the sender sent.
//
// But the copy is priced per BATCH, and for the producers that dominate the
// agent's volume it buys nothing: a promscrape chunk is take()n out of its
// batcher and never seen again — a failed chunk is dropped and the next scrape
// re-scrapes; a cumagg render is fresh pdata every export — a failed send
// leaves the series Rendered and the next export re-renders; an ingest push's
// decoded object dies with the failed RPC — the sender retransmits BYTES that
// are re-decoded. For those, the wrapper was deep-copying a payload whose only
// other reference was about to be garbage.
//
// So the answer is per PAYLOAD and rides the CALL, exactly like
// otlpexport.Own: the layers in between (the router, the buffer) are shared by
// both classes and must not know which they carry. Handoff(ctx) is the
// caller's promise that it will neither RE-SEND the object's contents nor READ
// them back after the call returns, success or failure — on failure it
// rebuilds from source data (a re-scrape, a re-render, a re-decode). Under
// that promise the wrapper runs the script IN PLACE; a script ERROR may then
// leave the payload half-transformed, which is fine precisely because the
// promise says nobody looks at it again.
//
// Who hands off (each verified against its failure path, not assumed):
//
//   - agent/promscrape — every chunk export and the health export: take()
//     resets the batcher, exportFailed latches so even salvage never re-sends,
//     the next scrape cycle rebuilds from a fresh scrape.
//   - agent/cumagg (Store.Export, which spanmetrics AND servicegraph ride):
//     Render is fresh pdata per export; a failed send leaves series in
//     Rendered and the next export renders again.
//   - agent/otlpingest — the logs and metrics forwards (both transports): a
//     failed forward goes back to the pushing SDK as a status, the decoded
//     pdata is dropped, and the retry re-decodes retransmitted bytes.
//   - internal/metrics (the Registry and DynamicMetricSet) — marked at the
//     agent's CALL SITES in cmd/kubescrape-agent, because the package cannot
//     import this one (transform → obs → metrics is already an import chain):
//     both render fresh pdata per export, and the set's failed-chunk retention
//     (export.go's retain) keeps raw SAMPLES that the next export re-renders —
//     never the pdata subtree, so a transformed payload cannot re-enter here.
//
// Who must NOT hand off, and keeps the copy:
//
//   - agent/tailbuffer — its drain and forwarded keeps retry the SAME payload
//     through the shared chain.
//   - agent/journald, agent/events, agent/azurediag — logchain.Pending's
//     render-once discipline deliberately re-exports the SAME payload across
//     attempts, so LogMetrics never re-observes a retained record.
//   - agent/tailer — its retry loop re-sends the same batch, so it cannot
//     mark; instead it applies the transform ITSELF, once per just-built
//     batch, via Wrapper.TransformLogs, and exports through Wrapper.Inner()
//     (the chain below the transform layer).

import "context"

// handoffKey is the context key for the handoff marker. Unexported and of a
// private type, so nothing outside this package can set it by accident.
type handoffKey struct{}

// Handoff marks ctx as carrying a payload its caller relinquishes: after the
// export call returns — success or failure — the caller neither re-sends the
// object's contents nor reads them back, and any retry rebuilds the payload
// from source data. The Wrapper then runs the transform script IN PLACE
// instead of on a deep copy. See the package's handoff.go doc for who may
// mark and why; a producer that re-offers the same object on retry must not.
//
// The marker is a context VALUE, so it survives context.WithoutCancel and
// deadline wrappers — the same contract otlpexport.Own rides on.
func Handoff(ctx context.Context) context.Context {
	if HandedOff(ctx) {
		return ctx
	}
	return context.WithValue(ctx, handoffKey{}, true)
}

// HandedOff reports whether ctx was marked by Handoff.
func HandedOff(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(handoffKey{}).(bool)
	return v
}
