package otlpingest

// EffectiveMaxInFlight is the concurrency bound a Server built with
// ServerConfig.MaxInFlight = n enforces: n itself, or defaultMaxInFlight when
// n is 0 (or negative). NewServer resolves it through this function, so a
// caller REPORTING the bound (the agent's effective-limits line) prints the
// number in force rather than the 0 it was configured with, which reads as
// "unbounded".
func EffectiveMaxInFlight(n int) int {
	if n <= 0 {
		return defaultMaxInFlight
	}
	return n
}

// EffectiveMaxRecvBytes is the per-message gRPC cap a Server built with
// ServerConfig.MaxRecvBytes = n enforces: n itself, maxIngestGRPCMessage
// (gRPC's own 4 MiB) when n is 0 (or negative), clamped to the largest message
// gRPC can frame. NewServer resolves it through this function — one
// derivation, so a reported cap cannot drift from the enforced one.
func EffectiveMaxRecvBytes(n int) int {
	if n <= 0 {
		n = maxIngestGRPCMessage
	}
	return int(min(int64(n), maxGRPCFrameBytes))
}

// MaxPushBytes is the largest decoded payload ONE application push can carry
// into a Server built with ServerConfig.MaxRecvBytes = grpcMaxRecv: the
// OTLP/HTTP body cap or the resolved per-message gRPC cap
// (EffectiveMaxRecvBytes, exactly as NewServer resolves it), whichever is
// larger.
//
// It exists for a hop DOWNSTREAM of this receiver that must accept anything the
// receiver admitted: the trace tier's authenticated re-shard hop, where a
// single span over the sender's split cap ships alone (otlpsplit), and sizing
// that hop's receive cap from anything smaller made delivery depend on which
// shard owns the trace. Spelled here, beside the constants it reads, so the
// downstream cap cannot drift from them.
func MaxPushBytes(grpcMaxRecv int) int {
	return max(maxIngestBody, EffectiveMaxRecvBytes(grpcMaxRecv))
}
