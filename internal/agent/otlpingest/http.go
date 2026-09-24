package otlpingest

// The OTLP/HTTP arm of the ingest receiver: the one push handler body
// (servePush) and its three per-signal entry points. Reading the body is
// httpbody.go's (BodyReader).

import (
	"context"
	"errors"
	"net/http"

	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

// servePush is the ONE body of the three HTTP push handlers; they differ only
// in the codec (unmarshal) and the enrich-and-forward step (handle). The
// sequence is load-bearing and its steps are ordered deliberately:
//
//  1. Read the body (BodyReader: media type, gzip, caps, byte budget) —
//     failures answer through WriteBodyError: 413 over either cap, 415 on the
//     media type, 429 + Retry-After when the byte budget is full, 503 for an
//     upload that ENDED rather than arrived (a killed pod, a rolled
//     deployment, the server's own ReadTimeout — retryable, and deliberately
//     not 400 or 408; see BodyErrorStatus), 400 for everything else.
//  2. The charge is released when the HANDLER returns, not when the read
//     ends: the body stays alive through enrichment and the forward, so
//     releasing earlier would leave the bytes resident and unaccounted.
//  3. The in-flight slot is acquired AFTER the read: holding a slot across
//     the upload let 32 trickled 16 MiB bodies shed every other sender on
//     the node for a ReadTimeout (60s) — no credentials required, on an
//     unauthenticated listener, which is the threat the bound exists for.
//     What bounds the read itself is the byte budget the body was charged
//     against (admit.go), not this slot. The gRPC arm is naturally on this
//     side of the decode. A refusal is RETRYABLE by design (429 +
//     Retry-After: 1): the sender still holds the payload.
//  4. What the payload WILL decode into is charged its own budget BEFORE the
//     unmarshal, estimated from the wire bytes (decodedsize.go), and held for
//     the rest of the handler exactly as the raw body's charge is: the raw
//     bytes bound what arrives, not what it inflates into, and 30 wire bytes
//     can mint a ResourceLogs (admit.go). Over it, 429 + Retry-After, like the
//     byte budget — the sender still holds an intact payload, and nothing was
//     decoded, which is what makes it a bound on the peak rather than on how
//     long a decoded payload is kept.
//  5. A payload that does not unmarshal is 400 — permanent; retrying a
//     malformed batch can never succeed.
//  6. A forward failure maps through HTTPForwardStatus (permanent 400 vs
//     retryable 503), and success answers with the OTLP proto response.
func (s *Server) servePush(w http.ResponseWriter, r *http.Request,
	signal string,
	// size estimates what the body decodes into (decodedLogsSize and its
	// siblings), which only the caller knows the signal of.
	size func(body []byte) int64,
	decode func(body []byte) error,
	handle func(ctx context.Context) (ProtoMarshaler, error),
) {
	body, charged, err := s.body.Read(r)
	if err != nil {
		// The door's own refusals (media type, caps, a truncated upload) are
		// counted AND warned inside the reader, per reason. The byte BUDGET is
		// not one of those — it is this receiver protecting itself rather than
		// the request being wrong — so it is narrated here, on the arm that
		// still holds the request and can name the sender.
		if errors.Is(err, errBufferBudget) {
			s.noteShed(shedBuffer, r.RemoteAddr)
		}
		WriteBodyError(w, err)
		return
	}
	defer s.body.Release(charged)
	if !s.acquire() {
		s.noteShed(shedInFlight, r.RemoteAddr)
		writeShed(w, errInFlight)
		return
	}
	defer s.release()
	decoded := size(body)
	if !s.chargeDecoded(decoded) {
		s.noteShed(shedDecoded, r.RemoteAddr)
		writeShed(w, errDecodedBudget)
		return
	}
	defer s.decoded.release(decoded)
	if err := decode(body); err != nil {
		// The same door as the read above, and the same reason label: a body
		// that arrived intact and is not OTLP. This was answered and counted by
		// NOTHING while the seam three lines up owned a reason literally called
		// "malformed" — the likelier of the two ways to be wrong was the
		// invisible one.
		s.body.noteMalformed(r, err)
		http.Error(w, "malformed OTLP "+signal+" payload", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	if s.stampPeer {
		ctx = withPeerIP(ctx, r.RemoteAddr)
	}
	resp, err := handle(ctx)
	if err != nil {
		// This receiver's own receive-path refusal keeps its words; a FORWARD
		// failure does not (see forwardFailureText — its text named the
		// collector, quoted its response body and, with routing on, enumerated
		// every tenant destination, to whoever could reach an unauthenticated
		// port). noteForwardFailure keeps that detail in the log.
		if r, ok := errors.AsType[receiveRefusal](err); ok {
			http.Error(w, r.err.Error(), HTTPForwardStatus(r.err))
			return
		}
		http.Error(w, s.noteForwardFailure(signal, r.RemoteAddr, err), HTTPForwardStatus(err))
		return
	}
	WriteProto(w, resp)
}

func (s *Server) handleHTTPLogs(w http.ResponseWriter, r *http.Request) {
	req := plogotlp.NewExportRequest()
	s.servePush(w, r, "logs", decodedLogsSize, req.UnmarshalProto, func(ctx context.Context) (ProtoMarshaler, error) {
		if err := s.forwardLogs(ctx, req.Logs()); err != nil {
			return nil, err
		}
		return plogotlp.NewExportResponse(), nil
	})
}

func (s *Server) handleHTTPMetrics(w http.ResponseWriter, r *http.Request) {
	req := pmetricotlp.NewExportRequest()
	s.servePush(w, r, "metrics", decodedMetricsSize, req.UnmarshalProto, func(ctx context.Context) (ProtoMarshaler, error) {
		if err := s.forwardMetrics(ctx, req.Metrics()); err != nil {
			return nil, err
		}
		return pmetricotlp.NewExportResponse(), nil
	})
}

func (s *Server) handleHTTPTraces(w http.ResponseWriter, r *http.Request) {
	req := ptraceotlp.NewExportRequest()
	s.servePush(w, r, "traces", decodedTracesSize, req.UnmarshalProto, func(ctx context.Context) (ProtoMarshaler, error) {
		if err := s.forwardTraces(ctx, req.Traces()); err != nil {
			return nil, err
		}
		return ptraceotlp.NewExportResponse(), nil
	})
}
