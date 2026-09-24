package otlpingest

// What a sender is answered when its push could not be forwarded, on both
// transports: the retryability classification (otlpexport.IsPermanent first,
// then the upstream status), the redaction of the destination's own words on a
// listener with no credentials, and the receive-path refusal that keeps its
// words (receiveRefusal).

import (
	"net/http"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/peerip"
)

// GRPCForwardStatus maps a forwarding failure onto a gRPC status the sender's
// SDK retries correctly. It is grpcForwardStatus exported, and that function's
// doc (with grpcForwardCode's) is the contract; in short:
//
//   - a permanent failure (otlpexport.IsPermanent) becomes InvalidArgument;
//   - an upstream status the sender reads as retryable
//     (otlpexport.RetryableStatus) keeps
//     its code and is relayed with its details, RetryInfo included;
//   - everything else — an upstream Unauthenticated, PermissionDenied, Internal
//     or bare ResourceExhausted among them — is rewritten to Unavailable.
//
// An upstream status is therefore NOT passed through on sight: doing that made
// IsPermanent dead code for a gRPC upstream and lost data (see there).
func GRPCForwardStatus(err error) error { return grpcForwardStatus(err) }

// grpcForwardStatus maps a forwarding failure onto a gRPC status the sender's
// SDK retries correctly. A bare error would surface as codes.Unknown —
// NON-retryable per the OTLP spec — making senders permanently drop batches on
// transient conditions (a full disk buffer, an upstream 5xx).
//
// Permanence is classified by otlpexport.IsPermanent, the single source of
// truth, and that classification has to come FIRST. Passing an upstream status
// through on sight — which is what this used to do — made IsPermanent dead code
// for the DEFAULT gRPC upstream, because virtually every failure from it is a
// status error. So an upstream Unauthenticated/PermissionDenied (a collector
// token mid-rotation) or Internal reached the pushing application, which reads
// all three as NON-retryable and drops the batch — while IsPermanent
// deliberately classifies auth failures and 404 as TRANSIENT, and the HTTP arm
// of this same receiver answered 503 for the identical error. One receiver, two
// answers, and the wrong one lost data.
//
// An upstream status is still preserved where it is genuinely more informative
// than Unavailable, but only when the sender will also read it as retryable —
// otherwise the code is rewritten rather than relayed.
func grpcForwardStatus(err error) error {
	code := grpcForwardCode(err)
	// Already the answer this function would build (the trace tier's receive
	// guard returns exactly this): relay it verbatim. Re-wrapping rendered the
	// sender `code = InvalidArgument desc = rpc error: code = InvalidArgument
	// desc = …`, burying the reason it needs one nesting deep inside the field
	// it reads first — and for a retryable upstream status it would also drop
	// the details, of which RetryInfo is load-bearing (see
	// otlpexport.RetryableStatus).
	if st, ok := status.FromError(err); ok && st.Code() == code {
		return err
	}
	return status.Error(code, err.Error())
}

// grpcForwardCode is grpcForwardStatus' classification on its own, so the
// redacting arm (redactedForwardStatus) cannot drift from the relaying one.
//
// Only definitive upstream rejections become InvalidArgument (do not retry).
// Everything else — diskqueue.ErrFull back-pressure, upstream 5xx, 401/403/404
// windows, timeouts, unclassified failures — is retryable: the receiver is a
// proxy, and the sender retrying is the safe default. An upstream status keeps
// its own code where that is genuinely more informative than Unavailable, but
// only when the sender will also read it as retryable.
func grpcForwardCode(err error) codes.Code {
	if otlpexport.IsPermanent(err) {
		return codes.InvalidArgument
	}
	// otlpexport.RetryableStatus is the spec's list, shared with the disk
	// buffer's poison gate. A bare ResourceExhausted is NOT on it: without
	// RetryInfo both the OTel SDK and the Collector drop the batch (the rule
	// this receiver honours when IT sheds a push, see exhaustedStatus), so
	// relaying one verbatim would be a silent data loss — it is rewritten to
	// Unavailable below instead.
	if st, ok := status.FromError(err); ok && otlpexport.RetryableStatus(st) {
		return st.Code()
	}
	return codes.Unavailable
}

// HTTPForwardStatus maps a forwarding failure onto the HTTP status the sender
// retries correctly (the HTTP counterpart of GRPCForwardStatus): a permanent
// upstream rejection is 400 (the sender must not retry the batch), everything
// else — diskqueue.ErrFull back-pressure, upstream 5xx, timeouts — is 503
// (retryable).
func HTTPForwardStatus(err error) int {
	if otlpexport.IsPermanent(err) {
		return http.StatusBadRequest
	}
	return http.StatusServiceUnavailable
}

// receiveRefusal marks an error the RECEIVE-path guard produced
// (ServerConfig.RejectTraces) rather than the forward, so the two can be
// answered differently — which they must be.
//
// A FORWARD failure's text belongs to the collector: the endpoint it names, the
// address it resolved to, the body it answered with, and with routing on every
// tenant destination's error. None of that is the sender's business and all of
// it is disclosure on a listener with no credentials, so forwardFailureText
// replaces it.
//
// A RECEIVE refusal is kubescrape's OWN sentence about the payload in front of
// it. The tier's loop guard is the case that exists today: it names the
// re-shard marker and nothing else, the sender that trips it is a misconfigured
// kubescrape hop pointed at an application port, and the marker's name is the
// only thing that tells an operator which hop to fix. So it is relayed verbatim
// — which also means a guard OWNS what its text says to an unauthenticated
// sender, and must not put a destination in it.
//
// The wrapper never reaches a classifier: both transports unwrap it
// (errors.AsType) before anything calls otlpexport.IsPermanent or
// status.FromError, so marking a refusal cannot change how it is graded.
type receiveRefusal struct{ err error }

func (r receiveRefusal) Error() string { return r.err.Error() }

func (r receiveRefusal) Unwrap() error { return r.err }

// forwardFailureText is the whole of what an application-facing listener tells
// a sender about a failed forward: the classification, and nothing else.
//
// What it replaces is the error's own rendering, and that was a disclosure on a
// listener with no credentials. The text named the collector — net/http renders
// a failed POST as `Post "https://otel-collector.monitoring:4318/v1/logs": dial
// tcp 10.96.4.7:4318: connect: connection refused`, i.e. host, port, path and
// resolved address — quoted the collector's own response BODY verbatim
// (otlpexport.HTTPStatusError), and, with routing configured, flattened EVERY
// tenant destination's error onto one line (route's partialFailure), so a
// single push from any pod that could reach the port enumerated the fleet's
// downstream topology. None of it is actionable by a sender: the only thing it
// can do with a forward failure is honour the status, which the status already
// says. The detail goes to the operator's side of the door instead
// (Server.noteForwardFailure).
func forwardFailureText(err error) string {
	if otlpexport.IsPermanent(err) {
		return "the payload was rejected downstream; retrying it will not help"
	}
	return "could not forward the payload; retry"
}

// redactedForwardStatus is grpcForwardStatus with the sender-facing message
// replaced by forwardFailureText's fixed one — and with every upstream status
// DETAIL dropped except RetryInfo.
//
// RetryInfo is the one detail kept, and it is kept because
// otlpexport.RetryableStatus depends on it: an upstream ResourceExhausted is
// relayed only because it carries RetryInfo, and both the OTel SDK and the Collector drop a batch on a
// bare one — so a redaction that lost it would answer a retryable condition
// with a status the sender treats as permanent, the one way this redaction
// could lose data.
//
// Everything else is dropped for the reason the message is: it is the
// DESTINATION's text, not this receiver's. An ErrorInfo names the backend's
// domain and carries its metadata, a DebugInfo its stack entries and detail
// string, a LocalizedMessage or BadRequest its own prose — a SaaS route
// destination that attaches any of them would otherwise have them relayed
// verbatim to whoever can reach an unauthenticated port, which is exactly what
// forwardFailureText's "the classification, and nothing else" promises not to
// do. The status used to be rebuilt from its proto with only the message
// overwritten, which kept them all.
func redactedForwardStatus(err error, msg string) error {
	code := grpcForwardCode(err)
	out := status.New(code, msg)
	if st, ok := status.FromError(err); ok && st.Code() == code {
		for _, d := range st.Details() {
			ri, ok := d.(*errdetails.RetryInfo)
			if !ok {
				continue
			}
			if withRetry, err := out.WithDetails(ri); err == nil {
				out = withRetry
			}
		}
	}
	return out.Err()
}

// forwardWarnEvery paces the forward-failure narration. A collector that cannot
// be reached is a STATE, and every sender on the node pushes into it, so the
// useful information is one line per signal per window rather than one per
// push. The exporter's own destination-health report (otlpexport/report.go)
// narrates the same outage from the sending side; this line exists because the
// detail it carries — which tenant destination failed, what the collector
// actually said — is the detail forwardFailureText no longer gives the sender,
// and it must not simply vanish.
const forwardWarnEvery = time.Minute

// noteForwardFailure keeps a failed forward's detail on the operator's side of
// the door and returns the fixed text the sender gets instead.
func (s *Server) noteForwardFailure(signal, peer string, err error) string {
	if allow, _ := s.forwardWarns.Allow(signal); allow {
		s.log.Warn("ingest: forwarding a pushed payload failed; the sender is answered a status and no detail, so the detail is here",
			"signal", signal,
			"peer", peerip.ForLog(peer),
			"outcome", forwardOutcome(err),
			"error", err)
	}
	return forwardFailureText(err)
}

// forwardOutcome labels the failure the way obs.Exports does, so the log line
// and the export counters read the same way.
func forwardOutcome(err error) string {
	if otlpexport.IsPermanent(err) {
		return "permanent"
	}
	return "transient"
}
