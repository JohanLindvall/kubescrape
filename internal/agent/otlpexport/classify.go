package otlpexport

// The export-failure classification every producer's retry path takes:
// permanent (a verdict on the payload, retrying cannot help) or transient.

import (
	"errors"
	"strings"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/JohanLindvall/diskqueue"
)

// errUnmarshalable marks a payload that cannot serialize for the disk buffer:
// permanent by definition — retrying cannot change the payload's own
// encodability.
var errUnmarshalable = errors.New("payload does not marshal")

// IsPermanent reports whether err is a definitive collector rejection that
// retrying cannot fix (bad payload, unimplemented signal). Everything
// ambiguous is transient. Deliberately transient despite the OTLP spec
// listing them non-retryable: auth failures (401/403, Unauthenticated,
// PermissionDenied) and 404 — a rotating bearer token, a collector rolling
// out behind an ingress, or a route being reprogrammed produces them for
// windows the disk buffer exists to survive; the bounded requeue path caps
// their cost, whereas classifying them permanent drains the whole backlog
// into drops. OutOfRange is retryable per the OTLP failure table.
//
// ErrRecordTooLarge is the one NON-collector error classified permanent:
// with -buffer-dir the producer's Export returns the enqueue error rather
// than any collector verdict, and a batch larger than the whole buffer cap
// can never fit however often it is retried. Left transient, the tailer
// rewound and rebuilt the identical batch every sweep - wedging its single
// sweep goroutine, and with it log shipping for every file on the node -
// while the counter added for exactly this class stayed at 0. ErrFull is
// deliberately NOT here: that one drains.
func IsPermanent(err error) bool {
	if errors.Is(err, diskqueue.ErrRecordTooLarge) || errors.Is(err, errUnmarshalable) {
		return true
	}
	if he, ok := errors.AsType[*HTTPStatusError](err); ok {
		switch he.Code {
		// 501 is the HTTP spelling of gRPC Unimplemented, which is permanent
		// below: without it the same collector-serves-no-such-signal mistake
		// was a counted drop over gRPC and an unbounded rewind-and-rebuild
		// loop over OTLP/HTTP. (404 stays transient DELIBERATELY: a
		// collector's routes reprogram during a rollout, and draining a
		// backlog into drops for a path that returns tomorrow is the wrong
		// trade.)
		case 400, 405, 413, 414, 415, 422, 431, 501:
			return true
		}
		return false
	}
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Unimplemented:
			// Unless the collector never said it: see grpcHTTP404.
			return !grpcHTTP404(st)
		case codes.InvalidArgument, codes.FailedPrecondition:
			return true
		}
	}
	return false
}

// grpcHTTP404Prefix is how grpc-go words the status it SYNTHESIZES when the
// peer answers a gRPC request with a plain HTTP response instead of a gRPC one
// (internal/transport/http2_client.go, operateHeaders: "unexpected HTTP status
// code received from server: %d (%s)"), which is what a proxy that is not
// gRPC-aware sends on a route miss — the ingress-nginx default backend, a
// host or path that does not match yet during a Helm upgrade.
const grpcHTTP404Prefix = "unexpected HTTP status code received from server: 404"

// grpcHTTP404 reports whether st is grpc-go's translation of an HTTP 404 from
// something that is not a gRPC server. HTTPStatusConvTab maps 404 to
// codes.Unimplemented, so without this the default protocol read an ingress's
// route miss as a collector with no pipeline for the signal — PERMANENT — and
// broke the rule IsPermanent documents: 404 stays transient because routes
// reprogram during a rollout. The buffered drain dropped the whole backlog
// with no backoff, the tailer took its permanent-drop path, and ingest told
// senders to discard what they pushed. The HTTP arm has always kept 404
// transient; this is that arm's rule reaching the gRPC spelling of the same
// response.
//
// Only the SYNTHESIZED status is recognised, by its message, and that is the
// whole of what can be recognised: a collector's own Unimplemented (no
// pipeline for this signal) stays permanent, which is the 501 parity the HTTP
// arm keeps. The known residual: an Envoy-based gateway (Istio, Contour, the
// Gateway API) answers a gRPC route miss with a REAL trailers-only
// grpc-status 12, which carries no such prefix, cannot be told apart from a
// collector's own verdict, and therefore stays permanent. If a grpc-go
// upgrade rewords the message, TestGRPC404FromANonGRPCProxyIsTransient fails.
func grpcHTTP404(st *status.Status) bool {
	return strings.HasPrefix(st.Message(), grpcHTTP404Prefix)
}

// Class is how an export failure is classified for the payload's sake:
// "permanent" means the collector rejected THIS payload and retrying it cannot
// help (the producer drops it, or the disk buffer eventually does), "transient"
// means it is coming back. It is IsPermanent's answer, named — the same
// classification every producer's retry path already takes, made visible so an
// operator does not have to infer it from a drop counter.
func Class(err error) string {
	if err == nil {
		return "ok"
	}
	if IsPermanent(err) {
		return "permanent"
	}
	return "transient"
}

// RetryableStatus reports whether an OTLP sender retries a batch that came back
// with this gRPC status, per the OTLP specification's list: Canceled,
// DeadlineExceeded, Aborted, OutOfRange, Unavailable, DataLoss — and
// ResourceExhausted ONLY when it carries RetryInfo (without the detail both the
// OTel SDK and the Collector drop the batch; grpc-go's own over-limit refusal
// is a bare one).
//
// ONE list for the two places that ask: otlpingest relays such a status to its
// pushing application instead of rewriting it, and respondedError treats one
// from the collector as back-pressure rather than poison evidence. The two
// questions are the same question — "is this the collector telling me to come
// back later?" — and while each package had its own list, a throttling
// collector's ResourceExhausted+RetryInfo counted toward the poison budget
// here although the HTTP arm's 429 never did.
func RetryableStatus(st *status.Status) bool {
	switch st.Code() {
	case codes.Canceled, codes.DeadlineExceeded, codes.Aborted,
		codes.OutOfRange, codes.Unavailable, codes.DataLoss:
		return true
	case codes.ResourceExhausted:
		for _, d := range st.Details() {
			if _, ok := d.(*errdetails.RetryInfo); ok {
				return true
			}
		}
	}
	return false
}
