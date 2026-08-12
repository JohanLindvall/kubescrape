package otlpingest

// The OTLP/HTTP request seam, shared by every receiver in this repo.
//
// There were two copies of this: the ingest server's and the trace tier's
// internal receiver (cmd/kubescrape-agent/servicegraph.go), and the bug fix
// landed in only one. The ingest reader wraps the COMPRESSED body in a
// cappedReader specifically so an over-cap gzip reports 413 — its truncation at
// the cap otherwise surfaces as a gzip parse error and answers 400 "malformed"
// for a payload that is merely too big. The tier's copy still used the
// io.LimitReader shape that fix replaced, on the ONE hop whose sender is
// another kubescrape: a shard's own exporter, which reads 400 as PERMANENT and
// drops the batch rather than splitting or retrying it.
//
// What is parameterised rather than shared, in each case because the two
// listeners genuinely differ:
//
//   - the CAP (16 MiB for application pushes, 4 MiB on the internal hop, where
//     the agents' own -otlp-max-send-bytes splits at ~3.75 MiB so anything
//     larger is a misconfigured sender);
//   - the byte BUDGET (admit.go), which the tier's receiver deliberately does
//     not have — it is authenticated, its senders are siblings, and its gRPC
//     message cap bounds one push. A nil budget charges nothing;
//   - the OBSERVATION (NewBodyReader vs newBodyReader): the counter says
//     "an application push was refused at a listener nothing authenticates",
//     and the trace tier serves BOTH kinds of listener in one process.
//
// The DECISIONS — what is a refusal, which status it gets, and whether the
// sender or the network is responsible for it — are shared, and that is the
// point of the file.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"os"
	"syscall"
	"time"

	"github.com/klauspost/compress/gzip"

	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/obs"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
)

var (
	// ErrBodyTooLarge maps to 413; truncating silently could ACK a payload
	// whose tail was dropped.
	ErrBodyTooLarge = errors.New("request body exceeds the receiver's limit")
	// ErrUnsupportedType maps to 415 (wrong media type, not a malformed
	// request).
	ErrUnsupportedType = errors.New("unsupported Content-Type")
	// errUnsupportedEncoding keeps its 400 (BodyErrorStatus's default arm) and
	// exists so the refusal can be told apart from a body that is merely
	// malformed — one is a sender configured for a codec this receiver does not
	// implement, the other is a corrupt or truncated payload, and an operator
	// reading a counter needs to know which.
	errUnsupportedEncoding = errors.New("unsupported Content-Encoding")
	// errClientAborted is the upload that ENDED rather than arrived: the peer
	// went away mid-body (a killed pod, a rolling deployment, an SDK export
	// timeout cancelling its own request) or the server's ReadTimeout expired
	// on a trickle. NOTHING was wrong with the request, and the retry that
	// follows is exactly the right thing for the sender to do — so this is the
	// one door refusal that must not be counted as "malformed" and must not be
	// warned about. It maps to 503, not 400 and not 408: see BodyErrorStatus,
	// where the choice is argued against the spec's own list.
	errClientAborted = errors.New("the sender's upload did not complete")
)

// BodyReader reads one OTLP/HTTP protobuf request body — media type checked,
// gzip decompressed, capped in both the compressed and decompressed direction —
// optionally charging a byte budget for what it accumulates.
type BodyReader struct {
	// max caps the body in BOTH directions: the compressed upload (so an
	// oversized one is 413 rather than a gzip error) and the decompressed
	// result (the zip-bomb guard).
	max int64
	// budget, when non-nil, is charged for the bytes held while reading; the
	// CALLER releases the returned charge once the body is no longer
	// referenced. nil disables the accounting entirely.
	budget *byteBudget
	// observe gates the counter AND the warning together (noteRejected): see
	// NewBodyReader for the one receiver that turns them off.
	observe bool
	// log and warns carry the refusal warning: one line per reason per
	// bodyRejectWarnEvery, beside the counter (noteRejected).
	log   *slog.Logger
	warns *logdedupe.Table
}

// NewBodyReader returns a reader capping bodies at max bytes with no byte
// budget and NO observation — the shape the trace tier's AUTHENTICATED
// internal hop wants (cmd/kubescrape-agent/servicegraph.go).
//
// The counter it stays out of is deliberate, not an oversight.
// kubescrape_ingest_body_rejected_total means one thing: an APPLICATION push
// was refused at a listener nothing authenticates, which is the only way to
// tell a misconfigured sender apart from a probe. The trace tier runs both
// kinds of listener IN ONE PROCESS — the application ports (:4317/:4318) and
// this hop (:4319) — so feeding both into one series would put sibling-shard
// traffic, admitted only with a bearer token, into the series an operator reads
// as "somebody out there is pushing wrong". The refusals this hop can produce
// are already visible where they are actionable and where the identity of the
// peer is known: every failed hop is one
// kubescrape_service_graph_sends_failed_total on the SENDING shard, and it
// fails that shard's own application push with it.
//
// The alternative was one counter with a `listener` label, keeping both
// refusals visible while telling them apart. It was not taken because the two
// are not summable: a stranger's malformed push and a sibling shard's are
// different events with different responses, and a label on one series invites
// exactly the aggregation that erases the distinction.
func NewBodyReader(max int64) *BodyReader { return &BodyReader{max: max} }

func newBodyReader(max int64, budget *byteBudget, log *slog.Logger) *BodyReader {
	if log == nil {
		log = slog.Default()
	}
	return &BodyReader{
		max:     max,
		budget:  budget,
		observe: true,
		log:     log,
		// One key per reason, so the table can never saturate and the
		// suppression state is exactly "this reason has warned recently".
		warns: logdedupe.New(len(bodyRejectReasons), bodyRejectWarnEvery),
	}
}

// bodyRejectWarnEvery paces the per-reason refusal warning. A misconfigured
// sender retries forever, so the condition persists and the useful information
// in it is one line.
const bodyRejectWarnEvery = time.Minute

// The reason label of obs.IngestBodyRejected. reasonAborted is the odd one out
// and the rest share a property it does not have: they describe a request that
// is WRONG, so the sender must change something before a retry can work.
const (
	reasonTooLarge  = "too_large"
	reasonMedia     = "media_type"
	reasonEncoding  = "content_encoding"
	reasonMalformed = "malformed"
	reasonAborted   = "aborted"
)

// bodyRejectReasons is the label set of obs.IngestBodyRejected, listed so the
// throttle table is sized to it and so the reasons have one home.
var bodyRejectReasons = []string{reasonTooLarge, reasonMedia, reasonEncoding, reasonMalformed, reasonAborted}

// bodyRejectReason classifies a refusal for the counter's reason label. The
// byte-budget refusal never reaches here: it is the receiver protecting itself
// rather than the request being wrong, it is retryable, and it counts into
// obs.IngestRejected with the other admission bounds.
func bodyRejectReason(err error) string {
	switch {
	case errors.Is(err, ErrBodyTooLarge):
		return reasonTooLarge
	case errors.Is(err, ErrUnsupportedType):
		return reasonMedia
	case errors.Is(err, errUnsupportedEncoding):
		return reasonEncoding
	case errors.Is(err, errClientAborted):
		return reasonAborted
	}
	return reasonMalformed
}

// noteRejected counts a body refused at the door and warns about it, throttled
// per reason.
//
// These refusals used to be answered with the right status and counted by
// NOTHING, on listeners that are unauthenticated by design — so a fleet whose
// senders all gzip against a receiver that cannot read their media type looked
// exactly like a fleet that was sending nothing, and a scanner probing the port
// left no trace at all. The peer is on the LOG LINE and never a metric label:
// it is sender-chosen and unbounded, which is the one thing a label must not be.
//
// An ABORTED upload is counted and NOT warned about, and that asymmetry is the
// point. logdedupe throttles a complaint about a persisting STATE — a sender
// that gzips against a receiver which cannot read its media type keeps doing it
// until someone changes the config, and one line says everything. A vanished
// peer is an EVENT, and a routine one: every rolling deployment produces one per
// evicted pod that was mid-export. There is no configuration to fix, the sender
// (or its successor) retries, and a Warn per occurrence would say "look at this"
// about a cluster doing exactly what it is supposed to do. What DOES carry
// information is the rate, which is what the counter is for.
func (br *BodyReader) noteRejected(r *http.Request, err error) {
	if !br.observe {
		return // see NewBodyReader
	}
	reason := bodyRejectReason(err)
	obs.IngestBodyRejected.WithLabelValues(reason).Inc()
	if reason == reasonAborted {
		return
	}
	if allow, _ := br.warns.Allow(reason); !allow {
		return
	}
	br.log.Warn("ingest: refused an OTLP/HTTP push at the door; the sender's request is wrong and retrying it will not help",
		"reason", reason,
		"status", BodyErrorStatus(err),
		"peer", r.RemoteAddr,
		"path", r.URL.Path,
		"content_type", r.Header.Get("Content-Type"),
		"content_encoding", r.Header.Get("Content-Encoding"),
		"content_length", r.ContentLength,
		"err", err)
}

// noteMalformed counts a body that READ cleanly and then did not decode as OTLP
// protobuf. It is the same door and the same reason as a body that would not
// decompress: the request is wrong and no retry of it will work.
//
// It lives here rather than at the handler because the handler is where it went
// uncounted — the read seam grew a counter with a "malformed" reason while the
// unmarshal three lines below it, which is the likelier way to be wrong, still
// answered 400 and moved nothing.
func (br *BodyReader) noteMalformed(r *http.Request, err error) {
	br.noteRejected(r, err) // no sentinel matches: bodyRejectReason's default arm
}

// tooLarge is the 413 error, naming the cap that was exceeded.
func (br *BodyReader) tooLarge() error {
	return fmt.Errorf("%w of %d bytes", ErrBodyTooLarge, br.max)
}

// Release returns a charge taken by Read to the budget (a no-op without one).
func (br *BodyReader) Release(charged int64) {
	if br.budget != nil {
		br.budget.release(charged)
	}
}

// Read reads and decompresses one request body, charging the byte budget for
// what it accumulates. It returns the bytes charged, which the CALLER releases
// once the body is no longer referenced; on any error it releases them itself
// and returns 0.
func (br *BodyReader) Read(r *http.Request) ([]byte, int64, error) {
	// Nothing is charged for a declared Content-Length. It is the sender's
	// unverified claim, and crediting it made the budget spendable by a peer
	// that sends no bytes at all: four sockets announcing 16 MiB each took the
	// whole budget and locked out every honest push, which is a cheaper denial
	// of service than the memory exhaustion the budget exists to prevent. The
	// budget is charged AS THE BODY IS READ (budgetReader, in 64 KiB granules),
	// which is what admit.go's comment always claimed the design does, so a
	// sender is refused for bytes it has actually produced. The refusal simply
	// lands mid-read instead of before it — still retryable, still with the
	// payload in the sender's hands.
	bd := &budgetReader{b: br.budget}
	// body records a failure of the TRANSPORT, underneath everything this
	// function layers on top of it (the compressed cap, gzip, the decompressed
	// limit). Which layer produced an error is the only reliable way to tell a
	// payload that is wrong from an upload that stopped: a client that
	// disappears mid-gzip and a genuinely corrupt gzip stream surface the SAME
	// io.ErrUnexpectedEOF out of the decompressor, because flate maps a short
	// read to it either way. See requestBody.classify.
	body := &requestBody{r: r.Body}
	// Every refusal in this function goes through fail, so the door cannot grow
	// a new one that is answered but counted by nothing — which is what the
	// media-type and Content-Encoding checks were before they moved below it.
	fail := func(err error) ([]byte, int64, error) {
		br.Release(bd.held)
		if errors.Is(err, errBufferBudget) {
			obs.IngestRejected.Inc()
			return nil, 0, err
		}
		br.noteRejected(r, err)
		return nil, 0, err
	}
	if ct := r.Header.Get("Content-Type"); ct != "" {
		// Parameterized types ("application/x-protobuf; charset=...") are fine;
		// only the media type itself must match.
		if mt, _, err := mime.ParseMediaType(ct); err != nil || mt != "application/x-protobuf" {
			return fail(fmt.Errorf("%w %q (want application/x-protobuf)", ErrUnsupportedType, ct))
		}
	}
	var src io.Reader = body
	var capped *cappedReader
	switch enc := r.Header.Get("Content-Encoding"); enc {
	case "", "identity":
	case "gzip": // OTel SDKs commonly gzip OTLP/HTTP
		// Allow one byte over the cap so an exactly-at-cap compressed body is
		// not misreported as oversized; the decompressed cap below still holds.
		capped = &cappedReader{r: body, remain: br.max + 1}
		zr, err := gzip.NewReader(capped)
		if err != nil {
			if capped.remain <= 0 {
				// See below: our own truncation, not the sender's payload.
				return fail(br.tooLarge())
			}
			return fail(body.classify(r, fmt.Errorf("gzip body: %w", err)))
		}
		defer func() { _ = zr.Close() }()
		src = zr
	default:
		return fail(fmt.Errorf("%w %q (want gzip or identity)", errUnsupportedEncoding, enc))
	}
	// The cap applies to the decompressed size too (zip-bomb guard). Read one
	// byte beyond it to distinguish at-cap from over-cap and reject the latter.
	bd.r = io.LimitReader(src, br.max+1)
	// Offer Content-Length as a size HINT where it is meaningful (an
	// identity-encoded body), so a read that gets that far does not pay
	// io.ReadAll's grow-and-copy doubling — which would put twice the CHARGED
	// bytes on the heap and make the budget mean half what it says. It is only a
	// hint: readAllCapped allocates at most maxPresizeBytes on the strength of
	// it, then doubles from there and lets the declaration trim the final step
	// to an exact fit. A gzip body's declared length is the compressed one, so
	// it is no hint at all and that case doubles throughout.
	hint := int64(0)
	if capped == nil {
		hint = r.ContentLength
	}
	buf, err := readAllCapped(bd, hint, br.max)
	if err != nil {
		if capped != nil && capped.remain <= 0 {
			// The compressed body hit the cap: the "gzip" failure is our own
			// truncation, not the sender's payload — report 413, not 400.
			return fail(br.tooLarge())
		}
		return fail(body.classify(r, err))
	}
	if int64(len(buf)) > br.max {
		return fail(br.tooLarge())
	}
	return buf, bd.held, nil
}

// requestBody is r.Body with the first transport-level failure remembered.
//
// Reading the request body can only fail two ways: the connection did (the peer
// went away, or the server's ReadTimeout expired), or the HTTP framing the peer
// wrote is invalid. Everything else that can go wrong with a push happens in a
// layer this one sits UNDER — the compressed cap, gzip, the decompressed limit,
// the byte budget — which is what makes "did the error come from here?" the
// question worth recording.
type requestBody struct {
	r   io.ReadCloser
	err error // the first non-EOF error the transport reported
}

func (b *requestBody) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	if err != nil && b.err == nil && !errors.Is(err, io.EOF) {
		b.err = err
	}
	return n, err
}

// classify rewrites a read failure that is in fact the sender's upload ending
// early, so it stops being counted — and complained about — as a malformed
// payload. Everything else passes through untouched.
//
// Measured, against a real net/http server (all three leave the request context
// cancelled):
//
//	FIN mid-body    -> io.ErrUnexpectedEOF
//	RST mid-body    -> *net.OpError wrapping syscall.ECONNRESET
//	ReadTimeout     -> *net.OpError wrapping os.ErrDeadlineExceeded
//	bad chunk header-> "invalid byte in chunk length", context NOT cancelled
//
// The last one is a request that really is wrong, and it stays malformed: the
// named identities are the classifier, and a cancelled request context is the
// backstop for whatever transport shape is not on that list.
//
// A refusal this receiver DECIDED outranks a transport failure that coincided
// with it, and the two do coincide: budgetReader returns errBufferBudget for the
// same Read that delivered bytes AND an error, so a peer going away exactly as
// the budget fills arrives here carrying both facts. Reclassifying that as an
// aborted upload answered it the abort status instead of 429, dropped the
// Retry-After, and moved obs.IngestBodyRejected{aborted} in place of
// obs.IngestRejected — the back-pressure signal disappearing precisely when the
// receiver is under the pressure it measures. errBufferBudget is the only such
// refusal that can reach here; the two caps are caught by their own branches
// above, before any classification.
func (b *requestBody) classify(r *http.Request, err error) error {
	if b.err == nil {
		return err // the failure belongs to a layer above the transport
	}
	if errors.Is(err, errBufferBudget) {
		return err
	}
	if !isTransportAbort(b.err) && r.Context().Err() == nil {
		return err
	}
	return fmt.Errorf("%w: %w", errClientAborted, b.err)
}

func isTransportAbort(err error) bool {
	switch {
	case errors.Is(err, io.ErrUnexpectedEOF), // fewer bytes than Content-Length declared
		errors.Is(err, syscall.ECONNRESET),
		errors.Is(err, syscall.EPIPE),
		errors.Is(err, os.ErrDeadlineExceeded), // the server's ReadTimeout
		errors.Is(err, net.ErrClosed),
		errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded):
		return true
	}
	return false
}

// cappedReader bounds the compressed request body. Reads past the cap fail
// with ErrBodyTooLarge so an oversized upload surfaces as 413 rather than as
// the gzip parse error its truncation would otherwise produce.
type cappedReader struct {
	r      io.Reader
	remain int64
}

func (c *cappedReader) Read(p []byte) (int, error) {
	if c.remain <= 0 {
		return 0, ErrBodyTooLarge
	}
	if int64(len(p)) > c.remain {
		p = p[:c.remain]
	}
	n, err := c.r.Read(p)
	c.remain -= int64(n)
	return n, err
}

// BodyErrorStatus maps a BodyReader.Read failure to its HTTP status.
func BodyErrorStatus(err error) int {
	switch {
	case errors.Is(err, ErrBodyTooLarge):
		return http.StatusRequestEntityTooLarge
	case errors.Is(err, ErrUnsupportedType):
		return http.StatusUnsupportedMediaType
	case errors.Is(err, errBufferBudget):
		return http.StatusTooManyRequests
	case errors.Is(err, errClientAborted):
		// Not the default 400, and not 408 either. Usually nobody is left to
		// read the status — but a half-closed peer does read it (measured),
		// and so does any proxy between the sender and this port, and for
		// those the status decides whether an intact batch is resent or
		// thrown away.
		//
		// OTLP/HTTP names exactly four retryable codes — 429, 502, 503, 504 —
		// and says "All other 4xx or 5xx response status codes MUST NOT be
		// retried" (otlpRetryableStatuses in the tests holds the same four, so
		// this citation is checked rather than asserted). 408 is not among
		// them and the spec does not mention it at all, so it instructs a
		// conformant SDK to drop the batch exactly as 400 does — which made
		// the special case buy nothing for the senders it was added for. This
		// is the same trade the gRPC arm already makes by attaching RetryInfo
		// to its ResourceExhausted: pick the wire signal that makes conformant
		// senders do the right thing.
		//
		// 503 over 429 because 429 is this door's back-pressure answer
		// (errBufferBudget) and an operator reading statuses must be able to
		// tell "I am shedding" from "your upload stopped"; over 502/504
		// because neither is about an upstream here. The honest cost: 503 is a
		// 5xx for a condition the receiver did not cause, so these land in
		// server-error dashboards and can feed a mesh's outlier detection.
		// kubescrape_ingest_body_rejected_total{reason="aborted"} is the
		// series that keeps them separable.
		return http.StatusServiceUnavailable
	}
	return http.StatusBadRequest
}

// WriteBodyError answers a failed read. The budget refusal is the one that
// carries Retry-After: the sender still holds an intact payload, and the
// receiver knows it is shedding and roughly for how long, so it says so —
// exactly like the in-flight shed. The aborted upload is retryable too
// (BodyErrorStatus answers 503) and deliberately carries no Retry-After:
// nothing here knows when that sender's connection will behave, and OTLP/HTTP
// has a client fall back to its own exponential backoff when the header is
// absent.
func WriteBodyError(w http.ResponseWriter, err error) {
	if errors.Is(err, errBufferBudget) {
		w.Header().Set("Retry-After", "1")
	}
	http.Error(w, err.Error(), BodyErrorStatus(err))
}

// ProtoMarshaler is any OTLP export response.
type ProtoMarshaler interface{ MarshalProto() ([]byte, error) }

// WriteProto answers a successful push with the OTLP protobuf response.
func WriteProto(w http.ResponseWriter, m ProtoMarshaler) {
	b, err := m.MarshalProto()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	_, _ = w.Write(b)
}

// GRPCForwardStatus maps a forwarding failure onto a gRPC status the sender's
// SDK retries correctly. A bare error would surface as codes.Unknown —
// NON-retryable per the OTLP spec — making senders permanently drop batches on
// transient conditions (a full disk buffer, an upstream 5xx). A status error
// from a gRPC upstream passes through unchanged.
//
// Permanence is classified by otlpexport.IsPermanent (the single source of
// truth): only definitive upstream rejections become InvalidArgument (do not
// retry). Everything else — diskqueue.ErrFull back-pressure, upstream 5xx,
// 401/403/404 windows, timeouts, unclassified failures — is Unavailable: the
// receiver is a proxy, and the sender retrying is the safe default.
func GRPCForwardStatus(err error) error { return grpcForwardStatus(err) }

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
