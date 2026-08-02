package otlpexport

// Ownership: which side of the export seam still holds a copy.
//
// Every producer in this agent falls into one of two classes, and the disk
// buffer's behaviour for a signal follows from which one it is:
//
//   - The producer OWNS its data and can re-read it. The tailer rewinds to a
//     committed offset, journald reopens at its cursor, the events reader
//     re-lists. A failed export is back-pressure: nothing is lost, the producer
//     tries again. Spooling helps these (it lets them commit progress early and
//     let rotation take the source away), but it is not what keeps them safe.
//   - The producer is a FORWARDER of somebody else's push. The OTLP ingest
//     server and the trace tier's re-shard hop hand the failure back to the
//     application, whose SDK retries. Spooling one of these is actively wrong:
//     it acks a sender for data that has not shipped, and the sender then stops
//     holding the only other copy.
//
// Traces are the second class — which is why *Buffered passes them through
// untouched — with exactly one exception: the tail-sampling buffer
// (agent/tailbuffer) acks its senders when it BUFFERS a span, because a
// decision cannot be made until the decision window has elapsed and holding the
// RPC open that long would pin a receiver slot per in-flight trace. By the time
// a decided trace reaches the exporter, the sender has been gone for seconds
// and holds nothing. The forwarder's justification is void: a failed export
// there is loss, not back-pressure.
//
// So the answer is not a global switch but a per-payload one, and it travels on
// the call rather than in the wiring, because the wiring in between (the
// transform wrapper, the router) is shared by both classes and neither knows
// nor should know which one it is carrying. Own(ctx) says "this payload is
// ours now"; Buffered spools an owned trace payload and passes through every
// other one.

import "context"

// ownedKey is the context key for the ownership marker. Unexported and of a
// private type, so nothing outside this package can set it by accident.
type ownedKey struct{}

// Own marks ctx as carrying a payload this process OWNS: its sender has already
// been acked and holds no copy of it, so a failed export is data loss rather
// than back-pressure. Buffered spools an owned traces payload to disk instead of
// passing it through to the collector.
//
// Only a layer that has ALREADY acked the sender may call it. Marking a payload
// the sender is still waiting on would spool it AND leave the sender's retry to
// deliver it again — duplicates for nothing, since the sender's retry was
// already the durability.
func Own(ctx context.Context) context.Context {
	if Owned(ctx) {
		return ctx
	}
	return context.WithValue(ctx, ownedKey{}, true)
}

// Owned reports whether ctx was marked by Own.
func Owned(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(ownedKey{}).(bool)
	return v
}
