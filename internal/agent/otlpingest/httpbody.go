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
// What is parameterised rather than shared: the CAP (16 MiB for application
// pushes, 4 MiB on the internal hop, where the agents' own -otlp-max-send-bytes
// splits at ~3.75 MiB so anything larger is a misconfigured sender) and the
// byte BUDGET (admit.go), which the tier's receiver deliberately does not have —
// it is authenticated, its senders are siblings, and its gRPC message cap
// bounds one push. A nil budget charges nothing.

import (
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"

	"github.com/klauspost/compress/gzip"

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
}

// NewBodyReader returns a reader capping bodies at max bytes with no byte
// budget — the shape a receiver that bounds its senders some other way wants
// (the trace tier's authenticated internal hop).
func NewBodyReader(max int64) *BodyReader { return &BodyReader{max: max} }

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
	if ct := r.Header.Get("Content-Type"); ct != "" {
		// Parameterized types ("application/x-protobuf; charset=...") are fine;
		// only the media type itself must match.
		if mt, _, err := mime.ParseMediaType(ct); err != nil || mt != "application/x-protobuf" {
			return nil, 0, fmt.Errorf("%w %q (want application/x-protobuf)", ErrUnsupportedType, ct)
		}
	}
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
	fail := func(err error) ([]byte, int64, error) {
		br.Release(bd.held)
		if errors.Is(err, errBufferBudget) {
			obs.IngestRejected.Inc()
		}
		return nil, 0, err
	}
	var src io.Reader = r.Body
	var capped *cappedReader
	switch enc := r.Header.Get("Content-Encoding"); enc {
	case "", "identity":
	case "gzip": // OTel SDKs commonly gzip OTLP/HTTP
		// Allow one byte over the cap so an exactly-at-cap compressed body is
		// not misreported as oversized; the decompressed cap below still holds.
		capped = &cappedReader{r: r.Body, remain: br.max + 1}
		zr, err := gzip.NewReader(capped)
		if err != nil {
			if capped.remain <= 0 {
				// See below: our own truncation, not the sender's payload.
				return fail(br.tooLarge())
			}
			return fail(fmt.Errorf("gzip body: %w", err))
		}
		defer func() { _ = zr.Close() }()
		src = zr
	default:
		return fail(fmt.Errorf("unsupported Content-Encoding %q (want gzip or identity)", enc))
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
	body, err := readAllCapped(bd, hint, br.max)
	if err != nil {
		if capped != nil && capped.remain <= 0 {
			// The compressed body hit the cap: the "gzip" failure is our own
			// truncation, not the sender's payload — report 413, not 400.
			return fail(br.tooLarge())
		}
		return fail(err)
	}
	if int64(len(body)) > br.max {
		return fail(br.tooLarge())
	}
	return body, bd.held, nil
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
	}
	return http.StatusBadRequest
}

// WriteBodyError answers a failed read. A budget refusal is the one case that
// must stay RETRYABLE — the sender still holds an intact payload — so it
// carries Retry-After exactly like the in-flight shed does.
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
