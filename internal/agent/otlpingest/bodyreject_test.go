package otlpingest

// Refusals at the OTLP/HTTP door. A malformed body, an oversize body, a wrong
// media type and an unimplemented Content-Encoding were each answered with the
// right status and counted by NOTHING — on listeners that are unauthenticated
// by design, which makes "nobody is pushing" and "everybody is pushing wrong"
// the same picture from the outside.
//
// The tests below pin the counting, the per-reason throttled warn and — just as
// importantly — that the RESPONSES did not change while the observability was
// added.
//
// They also pin the line between the reasons that accuse the sender and the one
// that does not. An upload that ENDED — a killed pod, a rolled deployment, an
// SDK export timeout — is not a request that is wrong, and counting it as
// "malformed" under a Warn saying "retrying it will not help" was two false
// statements per evicted pod.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	mathrand "math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/klauspost/compress/gzip"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

func bodyRejectCount(reason string) float64 {
	return obs.IngestBodyRejected.WithLabelValues(reason).Value()
}

// snapshotRejects reads every reason at once: a refusal must move ONE of them,
// and the others must stay put (a reason that catches everything is no better
// than no label at all).
func snapshotRejects() map[string]float64 {
	m := make(map[string]float64, len(bodyRejectReasons))
	for _, r := range bodyRejectReasons {
		m[r] = bodyRejectCount(r)
	}
	return m
}

func gzipped(tb testing.TB, b []byte) []byte {
	tb.Helper()
	var out bytes.Buffer
	z := gzip.NewWriter(&out)
	if _, err := z.Write(b); err != nil {
		tb.Fatal(err)
	}
	if err := z.Close(); err != nil {
		tb.Fatal(err)
	}
	return out.Bytes()
}

// The four refusals that mean the sender's REQUEST is wrong, each counted under
// its own reason and each still answered with the status it always was. (The
// fifth reason, aborted, is the sender's upload ending rather than its request
// being wrong, and has its own tests further down.)
func TestBodyRefusalsAreCountedByReasonAndKeepTheirStatus(t *testing.T) {
	const limit = 64
	cases := []struct {
		name       string
		reason     string
		wantStatus int
		req        func() *http.Request
	}{
		{
			name: "media type", reason: "media_type", wantStatus: http.StatusUnsupportedMediaType,
			req: func() *http.Request {
				r := httptest.NewRequest("POST", "/v1/logs", strings.NewReader("{}"))
				r.Header.Set("Content-Type", "application/json")
				return r
			},
		},
		{
			name: "content encoding", reason: "content_encoding", wantStatus: http.StatusBadRequest,
			req: func() *http.Request {
				r := httptest.NewRequest("POST", "/v1/logs", strings.NewReader("payload"))
				r.Header.Set("Content-Type", "application/x-protobuf")
				r.Header.Set("Content-Encoding", "deflate")
				return r
			},
		},
		{
			name: "malformed gzip", reason: "malformed", wantStatus: http.StatusBadRequest,
			req: func() *http.Request {
				r := httptest.NewRequest("POST", "/v1/logs", strings.NewReader("this is not gzip"))
				r.Header.Set("Content-Type", "application/x-protobuf")
				r.Header.Set("Content-Encoding", "gzip")
				return r
			},
		},
		{
			name: "too large", reason: "too_large", wantStatus: http.StatusRequestEntityTooLarge,
			req: func() *http.Request {
				r := httptest.NewRequest("POST", "/v1/logs", bytes.NewReader(bytes.Repeat([]byte{0x0a}, limit*4)))
				r.Header.Set("Content-Type", "application/x-protobuf")
				return r
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			br := newBodyReader(limit, nil, discardLogger())
			before := snapshotRejects()
			_, _, err := br.Read(tc.req())
			if err == nil {
				t.Fatal("the request must be refused")
			}
			if got := BodyErrorStatus(err); got != tc.wantStatus {
				t.Errorf("status = %d, want %d (the responses must not change)", got, tc.wantStatus)
			}
			after := snapshotRejects()
			for _, r := range bodyRejectReasons {
				want := before[r]
				if r == tc.reason {
					want++
				}
				if after[r] != want {
					t.Errorf("kubescrape_ingest_body_rejected_total{reason=%q} = %v, want %v", r, after[r], want)
				}
			}
		})
	}
}

// compressedOnlyOverCap builds a gzip body that is over the cap in the
// COMPRESSED direction and inside it once decompressed — the only payload shape
// that can exercise the cap on the UPLOAD, because it is the only one the
// decompressed cap cannot also refuse.
//
// The obvious payload is repetitive text compressed small, and it is over BOTH
// caps at once: the zip-bomb guard on the decompressed size answers 413 for it
// unaided, which is why the tests below used to pass with cappedReader deleted
// outright — as did the whole package. Random bytes are what deflate cannot
// shrink; it falls back to a stored block and the gzip header, block framing
// and trailer are pure growth, so nothing but the cap on the uploaded bytes can
// refuse this body.
//
// Both premises are asserted rather than assumed — a compression ratio is the
// library's business, and if either stops holding these tests must say so
// instead of quietly going back to proving nothing.
func compressedOnlyOverCap(tb testing.TB, max int64) []byte {
	tb.Helper()
	// A few bytes under the cap, so the decompressed guard has no claim on it.
	raw := make([]byte, max-4)
	if _, err := mathrand.New(mathrand.NewSource(1)).Read(raw); err != nil {
		tb.Fatal(err)
	}
	gz := gzipped(tb, raw)
	if int64(len(gz)) <= max {
		tb.Fatalf("the compressed body is %d bytes, inside the %d-byte cap: nothing would refuse it", len(gz), max)
	}
	if int64(len(raw)) > max {
		tb.Fatalf("the decompressed body is %d bytes, over the %d-byte cap as well: the decompressed guard would answer 413 on its own and this payload would prove nothing about the compressed one", len(raw), max)
	}
	return gz
}

// The cap is on the UPLOAD as well as on what it decompresses to — a sender
// that gzips must not be able to push past it — and the refusal's CLASS is not
// what it looks like: the gzip reader fails because WE truncated the stream, so
// it is answered and counted as too_large rather than as a malformed payload.
func TestOversizeCompressedBodyCountsAsTooLargeNotMalformed(t *testing.T) {
	const limit = 64
	br := newBodyReader(limit, nil, discardLogger())
	r := httptest.NewRequest("POST", "/v1/logs", bytes.NewReader(compressedOnlyOverCap(t, limit)))
	r.Header.Set("Content-Type", "application/x-protobuf")
	r.Header.Set("Content-Encoding", "gzip")

	before := snapshotRejects()
	_, _, err := br.Read(r)
	if err == nil {
		t.Fatal("a compressed upload over the cap was accepted: the cap is on the bytes the sender sends, and without it a gzipping sender decides how much this receiver reads")
	}
	if got := BodyErrorStatus(err); got != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", got)
	}
	after := snapshotRejects()
	if after["too_large"] != before["too_large"]+1 || after["malformed"] != before["malformed"] {
		t.Errorf("an over-cap gzip counted as %v; it must be too_large, not malformed",
			map[string]float64{"too_large": after["too_large"] - before["too_large"], "malformed": after["malformed"] - before["malformed"]})
	}
}

// The byte-budget refusal is the receiver protecting itself, it is RETRYABLE,
// and it belongs to kubescrape_ingest_rejected_total with the other admission
// bounds. Folding it into the door counter would put a back-pressure signal in
// the series an operator reads as "senders are misconfigured".
func TestBudgetRefusalStaysOnTheAdmissionCounter(t *testing.T) {
	br := newBodyReader(1<<20, &byteBudget{limit: 8}, discardLogger())
	r := httptest.NewRequest("POST", "/v1/logs", bytes.NewReader(bytes.Repeat([]byte{0x0a}, 4096)))
	r.Header.Set("Content-Type", "application/x-protobuf")

	before, beforeAdmission := snapshotRejects(), ingestRejectedTotal()
	if _, _, err := br.Read(r); err == nil {
		t.Fatal("the request must be refused")
	}
	if got := ingestRejectedTotal(); got != beforeAdmission+1 {
		t.Errorf("kubescrape_ingest_rejected_total = %v, want %v", got, beforeAdmission+1)
	}
	for _, reason := range bodyRejectReasons {
		if bodyRejectCount(reason) != before[reason] {
			t.Errorf("a budget refusal moved kubescrape_ingest_body_rejected_total{reason=%q}", reason)
		}
	}
}

// ...and it keeps that class when a transport failure lands in the SAME Read.
// budgetReader returns errBufferBudget for a Read that delivered bytes and an
// error together, so a peer going away exactly as the budget fills reaches the
// classifier carrying both facts. The receiver's own decision has to win: read
// as an aborted upload instead, the refusal was answered with the abort status
// rather than 429, carried no Retry-After, and moved the door counter in place
// of kubescrape_ingest_rejected_total — the back-pressure signal vanishing
// exactly when the receiver is under the pressure it measures.
func TestBudgetRefusalKeepsItsClassWhenTheUploadAlsoAborted(t *testing.T) {
	// limit 1 refuses the first granule, so the refusal lands on the same Read
	// the transport fails on rather than a later one.
	br := newBodyReader(1<<20, &byteBudget{limit: 1}, discardLogger())
	r := httptest.NewRequest("POST", "/v1/logs", &abortingBody{err: syscall.ECONNRESET})
	r.Header.Set("Content-Type", "application/x-protobuf")

	before, beforeAdmission := snapshotRejects(), ingestRejectedTotal()
	_, _, err := br.Read(r)
	if !errors.Is(err, errBufferBudget) {
		t.Fatalf("Read returned %v, want the budget refusal", err)
	}
	if got := BodyErrorStatus(err); got != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", got)
	}
	w := httptest.NewRecorder()
	WriteBodyError(w, err)
	if got := w.Header().Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q, want \"1\": the sender still holds an intact payload", got)
	}
	if got := ingestRejectedTotal(); got != beforeAdmission+1 {
		t.Errorf("kubescrape_ingest_rejected_total = %v, want %v", got, beforeAdmission+1)
	}
	for _, reason := range bodyRejectReasons {
		if bodyRejectCount(reason) != before[reason] {
			t.Errorf("a budget refusal moved kubescrape_ingest_body_rejected_total{reason=%q}", reason)
		}
	}
}

// abortingBody delivers bytes and fails in the SAME Read, which is what makes
// the two facts coincide: requestBody records the transport failure while
// budgetReader refuses the bytes that Read just delivered.
type abortingBody struct{ err error }

func (b *abortingBody) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0x0a
	}
	return len(p), b.err
}

// The warn names the peer — the whole point on an unauthenticated listener is
// telling a misconfigured sender apart from a probe — and repeats at most once
// per reason per window, because a misconfigured sender retries forever.
func TestDoorRefusalWarnsOncePerReasonAndNamesThePeer(t *testing.T) {
	var logged bytes.Buffer
	br := newBodyReader(64, nil, slog.New(slog.NewTextHandler(&logged, nil)))

	badType := func() *http.Request {
		r := httptest.NewRequest("POST", "/v1/logs", strings.NewReader("{}"))
		r.Header.Set("Content-Type", "application/json")
		r.RemoteAddr = "10.42.7.9:54321"
		return r
	}
	for i := 0; i < 5; i++ {
		if _, _, err := br.Read(badType()); err == nil {
			t.Fatal("the request must be refused")
		}
	}
	if got := strings.Count(logged.String(), "reason=media_type"); got != 1 {
		t.Errorf("five identical refusals logged %d lines, want 1: a misconfigured sender retries forever", got)
	}
	if !strings.Contains(logged.String(), "10.42.7.9") {
		t.Errorf("the warning does not name the peer, which is the only way to find the sender:\n%s", logged.String())
	}

	// A DIFFERENT reason is a different condition and must not be suppressed by
	// the first one's window.
	enc := httptest.NewRequest("POST", "/v1/logs", strings.NewReader("payload"))
	enc.Header.Set("Content-Type", "application/x-protobuf")
	enc.Header.Set("Content-Encoding", "deflate")
	if _, _, err := br.Read(enc); err == nil {
		t.Fatal("the request must be refused")
	}
	if got := strings.Count(logged.String(), "reason=content_encoding"); got != 1 {
		t.Errorf("a second, different reason logged %d lines, want 1", got)
	}
}

// The counting lives in the shared BodyReader, so it must reach the real
// receiver without the handlers doing anything: end-to-end through the ingest
// server's own HTTP door.
func TestServerDoorRefusalIsCounted(t *testing.T) {
	srv := httpTestServer(t, &captureExporter{})
	req, err := http.NewRequest("POST", srv.URL+"/v1/logs", strings.NewReader("this is not gzip"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "gzip")

	before := bodyRejectCount("malformed")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	if got := bodyRejectCount("malformed"); got != before+1 {
		t.Errorf("kubescrape_ingest_body_rejected_total{reason=\"malformed\"} = %v, want %v", got, before+1)
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
}

// --- the upload that ENDED rather than arrived ---
//
// A killed pod, a rolling deployment, an SDK cancelling its own export: the
// sender goes away with the body half sent. Nothing about the request was
// wrong, and the retry that follows is exactly right — so counting it as
// "malformed" and warning that "the sender's request is wrong and retrying it
// will not help" is two false statements per evicted pod. These tests pin the
// three shapes a transport failure actually takes here (measured against a real
// net/http server), the one that must NOT be reclassified, and the silence.

// doorServer serves the ingest server's REAL /v1/logs handler and reports when
// a handler has returned: a counter read that races the upload it is about
// would be a test that passes for the wrong reason.
func doorServer(t *testing.T, readTimeout time.Duration) (addr string, served <-chan struct{}, logged *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	s := NewServer(ServerConfig{
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Exporter: &captureExporter{},
		Logger:   slog.New(slog.NewTextHandler(&buf, nil)),
	})
	done := make(chan struct{}, 8)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/logs", func(w http.ResponseWriter, r *http.Request) {
		defer func() { done <- struct{}{} }()
		s.handleHTTPLogs(w, r)
	})
	srv := httptest.NewUnstartedServer(mux)
	if readTimeout > 0 {
		srv.Config.ReadTimeout = readTimeout
	}
	srv.Start()
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().String(), done, &buf
}

// waitServed blocks until a handler has returned. A refusal that never reaches
// a handler at all would otherwise hang the test binary instead of failing it.
func waitServed(t *testing.T, served <-chan struct{}) {
	t.Helper()
	select {
	case <-served:
	case <-time.After(30 * time.Second):
		t.Fatal("no handler ran: the request never reached one")
	}
}

// rawPost writes a request head and a partial body over a raw socket, then
// hands the connection to finish — which is where each abort shape happens.
func rawPost(t *testing.T, addr, head string, body []byte, finish func(*net.TCPConn)) {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(c, head); err != nil {
		t.Fatal(err)
	}
	if len(body) > 0 {
		if _, err := c.Write(body); err != nil {
			t.Fatal(err)
		}
	}
	finish(c.(*net.TCPConn))
}

func TestAbortedUploadIsNotCountedAsMalformed(t *testing.T) {
	const readTimeout = 300 * time.Millisecond
	addr, served, logged := doorServer(t, readTimeout)

	cases := []struct {
		name   string
		head   string
		body   []byte
		finish func(*net.TCPConn)
	}{
		{
			// The killed pod: FIN with the declared body unfinished.
			name:   "peer closes mid-body",
			head:   "POST /v1/logs HTTP/1.1\r\nHost: x\r\nContent-Type: application/x-protobuf\r\nContent-Length: 4096\r\n\r\n",
			body:   bytes.Repeat([]byte{0x0a}, 16),
			finish: func(c *net.TCPConn) { _ = c.Close() },
		},
		{
			// The evicted pod whose socket is reset rather than closed.
			name: "peer resets mid-body",
			head: "POST /v1/logs HTTP/1.1\r\nHost: x\r\nContent-Type: application/x-protobuf\r\nContent-Length: 4096\r\n\r\n",
			body: bytes.Repeat([]byte{0x0a}, 16),
			finish: func(c *net.TCPConn) {
				_ = c.SetLinger(0) // RST, not FIN
				_ = c.Close()
			},
		},
		{
			// The trickle the server's own ReadTimeout gives up on.
			name: "server read timeout",
			head: "POST /v1/logs HTTP/1.1\r\nHost: x\r\nContent-Type: application/x-protobuf\r\nContent-Length: 4096\r\n\r\n",
			body: bytes.Repeat([]byte{0x0a}, 16),
			finish: func(c *net.TCPConn) {
				time.Sleep(readTimeout + 500*time.Millisecond)
				_ = c.Close()
			},
		},
		{
			// A gzip upload that stops mid-stream: the decompressor reports the
			// same io.ErrUnexpectedEOF a corrupt payload would, so only the
			// layer the error came from can tell these apart.
			name:   "peer closes mid-gzip",
			head:   "POST /v1/logs HTTP/1.1\r\nHost: x\r\nContent-Type: application/x-protobuf\r\nContent-Encoding: gzip\r\nContent-Length: 4096\r\n\r\n",
			body:   gzipped(t, bytes.Repeat([]byte("a log line worth compressing "), 200))[:64],
			finish: func(c *net.TCPConn) { _ = c.Close() },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := snapshotRejects()
			rawPost(t, addr, tc.head, tc.body, tc.finish)
			waitServed(t, served)
			after := snapshotRejects()
			for _, r := range bodyRejectReasons {
				want := before[r]
				if r == reasonAborted {
					want++
				}
				if after[r] != want {
					t.Errorf("kubescrape_ingest_body_rejected_total{reason=%q} = %v, want %v (an upload that ended is not a request that is wrong)",
						r, after[r], want)
				}
			}
		})
	}
	// Not one Warn for any of them: a rolling deployment produces one of these
	// per evicted pod, there is nothing to fix, and the sender retries.
	if strings.Contains(logged.String(), "refused an OTLP/HTTP push") {
		t.Errorf("an aborted upload warned; the line asserts the sender is wrong and it is not:\n%s", logged.String())
	}
}

// The counterexample, and the reason the classifier is not just "the read
// failed": a request whose CHUNKED FRAMING is invalid is a request that really
// is wrong. It fails in the same place and must keep its own reason.
func TestMalformedFramingIsNotAnAbort(t *testing.T) {
	addr, served, _ := doorServer(t, 0)
	before := snapshotRejects()
	rawPost(t, addr,
		"POST /v1/logs HTTP/1.1\r\nHost: x\r\nContent-Type: application/x-protobuf\r\nTransfer-Encoding: chunked\r\n\r\n",
		[]byte("ZZZZ\r\n"),
		func(c *net.TCPConn) { t.Cleanup(func() { _ = c.Close() }) })
	waitServed(t, served)
	after := snapshotRejects()
	if after[reasonMalformed] != before[reasonMalformed]+1 || after[reasonAborted] != before[reasonAborted] {
		t.Errorf("bad chunk framing counted malformed+%v aborted+%v, want malformed+1 aborted+0",
			after[reasonMalformed]-before[reasonMalformed], after[reasonAborted]-before[reasonAborted])
	}
}

// otlpRetryableStatuses is the OTLP/HTTP spec's retryable set, verbatim: those
// four codes "SHOULD be retried" and "All other 4xx or 5xx response status
// codes MUST NOT be retried". BodyErrorStatus's comment cites this list, so it
// lives here as a value the tests check the answer against rather than as a
// claim in prose — the previous version of that comment said 408 was in the
// set, and nothing could contradict it.
var otlpRetryableStatuses = []int{
	http.StatusTooManyRequests,    // 429
	http.StatusBadGateway,         // 502
	http.StatusServiceUnavailable, // 503
	http.StatusGatewayTimeout,     // 504
}

func isOTLPRetryable(code int) bool {
	for _, c := range otlpRetryableStatuses {
		if c == code {
			return true
		}
	}
	return false
}

// A retryable status, not 400 and not 408. On a full disconnect nothing reads
// the status — but a half-closed peer does (and so does any proxy in between),
// and for those the answer decides whether an intact batch is resent or thrown
// away: everything outside the four codes above, 408 included, tells a
// conformant SDK to drop it.
func TestAbortedUploadAnswersRetryableStatus(t *testing.T) {
	addr, served, _ := doorServer(t, 0)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if _, err := fmt.Fprintf(c, "POST /v1/logs HTTP/1.1\r\nHost: x\r\nContent-Type: application/x-protobuf\r\nContent-Length: 4096\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write(bytes.Repeat([]byte{0x0a}, 16)); err != nil {
		t.Fatal(err)
	}
	if err := c.(*net.TCPConn).CloseWrite(); err != nil { // half close: still reading
		t.Fatal(err)
	}
	waitServed(t, served)
	if err := c.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	head := make([]byte, 15)
	if _, err := io.ReadFull(c, head); err != nil {
		t.Fatalf("the half-closed peer got no response at all: %v", err)
	}
	want := fmt.Sprintf("HTTP/1.1 %d ", http.StatusServiceUnavailable)
	if got := string(head); !strings.HasPrefix(got, want) {
		t.Errorf("response %q, want %q — anything outside the OTLP/HTTP retryable set makes the sender drop a batch it should resend", got, want)
	}
}

// The status is not a matter of taste, and it has to satisfy TWO senders. The
// SPEC's list governs every third-party SDK pushing at the unauthenticated
// application ports; this repo's own exporter governs the internal hop, which
// shares this mapping and is where a dropped batch costs data. 400 fails both
// (it is permanent in the spec and in otlpexport.IsPermanent's list), and 408
// fails the first — it is outside the spec's four codes, which is why the
// answer is not merely "some 4xx that reads like a timeout".
func TestTheAbortStatusIsRetryableToThisReposOwnSender(t *testing.T) {
	abort := fmt.Errorf("%w: %w", errClientAborted, io.ErrUnexpectedEOF)
	got := BodyErrorStatus(abort)
	if !isOTLPRetryable(got) {
		t.Fatalf("the abort status is %d, which is not one of OTLP/HTTP's retryable codes %v: a conformant SDK MUST NOT retry it, so the batch its upload was carrying is dropped", got, otlpRetryableStatuses)
	}
	if isOTLPRetryable(http.StatusRequestTimeout) {
		t.Error("408 has entered the spec's retryable set; the reason this door does not answer 408 is gone and the comment on BodyErrorStatus needs re-arguing")
	}
	if otlpexport.IsPermanent(&otlpexport.HTTPStatusError{Code: got}) {
		t.Error("the abort status is permanent to our own exporter: a shard whose upload was cut off would drop the spans instead of resending them")
	}
	if !otlpexport.IsPermanent(&otlpexport.HTTPStatusError{Code: http.StatusBadRequest}) {
		t.Error("400 is no longer permanent to our own exporter; this test's premise, and the reason for not answering 400, is gone")
	}
}

// The unit-level classifier, one shape per error identity. The end-to-end tests
// above prove these are the errors a real server produces; this one proves each
// is classified without needing a socket to produce it.
func TestTransportErrorsClassifyAsAborted(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"unexpected EOF", io.ErrUnexpectedEOF},
		{"connection reset", &net.OpError{Op: "read", Err: os.NewSyscallError("read", syscall.ECONNRESET)}},
		{"read deadline", &net.OpError{Op: "read", Err: os.ErrDeadlineExceeded}},
		{"closed connection", net.ErrClosed},
		{"context cancelled", context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			br := newBodyReader(1<<20, nil, discardLogger())
			r := httptest.NewRequest("POST", "/v1/logs", &failingBody{err: tc.err})
			r.Header.Set("Content-Type", "application/x-protobuf")
			before := snapshotRejects()
			_, _, err := br.Read(r)
			if !errors.Is(err, errClientAborted) {
				t.Fatalf("Read returned %v, which does not classify as an aborted upload", err)
			}
			if !errors.Is(err, tc.err) {
				t.Errorf("the cause was flattened away: %v", err)
			}
			if got := BodyErrorStatus(err); !isOTLPRetryable(got) {
				t.Errorf("status = %d, which is not in OTLP/HTTP's retryable set %v", got, otlpRetryableStatuses)
			}
			after := snapshotRejects()
			if after[reasonAborted] != before[reasonAborted]+1 || after[reasonMalformed] != before[reasonMalformed] {
				t.Errorf("counted aborted+%v malformed+%v, want aborted+1 malformed+0",
					after[reasonAborted]-before[reasonAborted], after[reasonMalformed]-before[reasonMalformed])
			}
		})
	}
}

// The gzip HEADER read is a second, separate place a body can fail, three
// statements above the one the tests above exercise: an upload that stops
// before ten bytes have arrived never reaches the decompressed read at all.
// Both sites have to classify, or a sender that dies early is guilty and one
// that dies late is not.
func TestUploadCutShortInsideTheGzipHeaderIsAnAbort(t *testing.T) {
	br := newBodyReader(1<<20, nil, discardLogger())
	r := httptest.NewRequest("POST", "/v1/logs", &failingBody{err: io.ErrUnexpectedEOF})
	r.Header.Set("Content-Type", "application/x-protobuf")
	r.Header.Set("Content-Encoding", "gzip")

	before := snapshotRejects()
	_, _, err := br.Read(r)
	if !errors.Is(err, errClientAborted) {
		t.Fatalf("Read returned %v, which does not classify as an aborted upload", err)
	}
	after := snapshotRejects()
	if after[reasonAborted] != before[reasonAborted]+1 || after[reasonMalformed] != before[reasonMalformed] {
		t.Errorf("counted aborted+%v malformed+%v, want aborted+1 malformed+0",
			after[reasonAborted]-before[reasonAborted], after[reasonMalformed]-before[reasonMalformed])
	}
}

// The backstop, and its limit. The identity list classifies the shapes a real
// net/http server was measured producing; a transport that fails some other way
// (an HTTP/2 stream reset, a future net error) is caught by the request CONTEXT,
// which net/http cancels when the peer goes away. It is consulted ONLY for a
// failure that came from the transport: a probe that hangs up the moment it has
// sent a complete malformed body must keep its reason, or the counter that
// exists to see probes would stop seeing them.
func TestUnrecognisedTransportFailureIsAnAbortOnlyWhenTheClientIsGone(t *testing.T) {
	odd := errors.New("stream error: stream ID 3; CANCEL")
	for _, tc := range []struct {
		name       string
		cancelled  bool
		wantReason string
	}{
		{"client gone", true, reasonAborted},
		{"client still there", false, reasonMalformed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			br := newBodyReader(1<<20, nil, discardLogger())
			r := httptest.NewRequest("POST", "/v1/logs", &failingBody{err: odd})
			r.Header.Set("Content-Type", "application/x-protobuf")
			if tc.cancelled {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				r = r.WithContext(ctx)
			}
			before := snapshotRejects()
			if _, _, err := br.Read(r); err == nil {
				t.Fatal("the request must be refused")
			}
			after := snapshotRejects()
			for _, reason := range bodyRejectReasons {
				want := before[reason]
				if reason == tc.wantReason {
					want++
				}
				if after[reason] != want {
					t.Errorf("kubescrape_ingest_body_rejected_total{reason=%q} = %v, want %v", reason, after[reason], want)
				}
			}
		})
	}
}

// An error that is NOT a transport failure keeps its class even though it
// surfaced from the same read: the classifier asks which LAYER failed, and a
// gzip stream that is corrupt in the middle of a body that arrived whole is the
// sender's payload being wrong.
func TestCorruptGzipFromAnIntactUploadStaysMalformed(t *testing.T) {
	br := newBodyReader(1<<20, nil, discardLogger())
	gz := gzipped(t, bytes.Repeat([]byte("payload "), 500))
	corrupt := append([]byte(nil), gz[:len(gz)-8]...) // truncated, but fully delivered
	r := httptest.NewRequest("POST", "/v1/logs", bytes.NewReader(corrupt))
	r.Header.Set("Content-Type", "application/x-protobuf")
	r.Header.Set("Content-Encoding", "gzip")

	before := snapshotRejects()
	if _, _, err := br.Read(r); err == nil {
		t.Fatal("a truncated gzip stream must be refused")
	} else if errors.Is(err, errClientAborted) {
		t.Fatalf("a body that arrived whole was classified as an aborted upload: %v", err)
	}
	after := snapshotRejects()
	if after[reasonMalformed] != before[reasonMalformed]+1 || after[reasonAborted] != before[reasonAborted] {
		t.Errorf("counted malformed+%v aborted+%v, want malformed+1 aborted+0",
			after[reasonMalformed]-before[reasonMalformed], after[reasonAborted]-before[reasonAborted])
	}
}

// failingBody delivers a few bytes and then fails the way a transport does.
type failingBody struct {
	err  error
	sent bool
}

func (f *failingBody) Read(p []byte) (int, error) {
	if !f.sent {
		f.sent = true
		n := copy(p, []byte{0x0a, 0x01, 0x02})
		return n, nil
	}
	return 0, f.err
}

// --- the defect three lines below the instrumented read ---

// A body that decompresses and reads fine and then is not OTLP protobuf is the
// same door and the same reason. It was answered 400 and counted by NOTHING
// while the read seam above it owned a reason label literally called
// "malformed".
func TestUndecodablePayloadIsCountedAtTheDoor(t *testing.T) {
	srv := httpTestServer(t, &captureExporter{})
	before := snapshotRejects()
	resp, err := http.Post(srv.URL+"/v1/logs", "application/x-protobuf", strings.NewReader("this reads fine and is not protobuf"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (the response must not change)", resp.StatusCode)
	}
	after := snapshotRejects()
	for _, r := range bodyRejectReasons {
		want := before[r]
		if r == reasonMalformed {
			want++
		}
		if after[r] != want {
			t.Errorf("kubescrape_ingest_body_rejected_total{reason=%q} = %v, want %v", r, after[r], want)
		}
	}
}

// ...and a payload that DOES decode still counts nothing, or the counter would
// measure traffic instead of refusals.
func TestDecodablePayloadCountsNothing(t *testing.T) {
	srv := httpTestServer(t, &captureExporter{})
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hi")
	body, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	before := snapshotRejects()
	resp, err := http.Post(srv.URL+"/v1/logs", "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	for _, r := range bodyRejectReasons {
		if got := bodyRejectCount(r); got != before[r] {
			t.Errorf("an accepted push moved kubescrape_ingest_body_rejected_total{reason=%q}", r)
		}
	}
}

// --- the authenticated internal hop ---

// The trace tier serves the unauthenticated application ports AND the
// authenticated internal hop in ONE process. The counter means "an application
// push was refused at a listener nothing authenticates"; sibling-shard traffic
// admitted only with a bearer token must not land in the series an operator
// reads as "somebody out there is pushing wrong".
func TestInternalHopBodyReaderIsNotOnTheApplicationCounter(t *testing.T) {
	br := NewBodyReader(64) // what cmd/kubescrape-agent/servicegraph.go builds
	cases := []func() *http.Request{
		func() *http.Request {
			r := httptest.NewRequest("POST", "/v1/traces", strings.NewReader("{}"))
			r.Header.Set("Content-Type", "application/json")
			return r
		},
		func() *http.Request {
			r := httptest.NewRequest("POST", "/v1/traces", strings.NewReader("not gzip"))
			r.Header.Set("Content-Type", "application/x-protobuf")
			r.Header.Set("Content-Encoding", "gzip")
			return r
		},
		func() *http.Request {
			r := httptest.NewRequest("POST", "/v1/traces", bytes.NewReader(bytes.Repeat([]byte{0x0a}, 4096)))
			r.Header.Set("Content-Type", "application/x-protobuf")
			return r
		},
		func() *http.Request {
			r := httptest.NewRequest("POST", "/v1/traces", &failingBody{err: io.ErrUnexpectedEOF})
			r.Header.Set("Content-Type", "application/x-protobuf")
			return r
		},
	}
	before := snapshotRejects()
	for _, mk := range cases {
		if _, _, err := br.Read(mk()); err == nil {
			t.Fatal("the request must still be refused")
		}
	}
	for _, r := range bodyRejectReasons {
		if got := bodyRejectCount(r); got != before[r] {
			t.Errorf("the authenticated internal hop moved kubescrape_ingest_body_rejected_total{reason=%q} by %v",
				r, got-before[r])
		}
	}
}

// The refusals themselves are unchanged there — only the observation is. The
// 413-for-an-over-cap-gzip fix is the reason this hop shares the reader at all.
func TestInternalHopKeepsItsStatuses(t *testing.T) {
	const limit = 64
	br := NewBodyReader(limit)
	r := httptest.NewRequest("POST", "/v1/traces", bytes.NewReader(compressedOnlyOverCap(t, limit)))
	r.Header.Set("Content-Type", "application/x-protobuf")
	r.Header.Set("Content-Encoding", "gzip")
	_, _, err := br.Read(r)
	if err == nil {
		t.Fatal("the internal hop accepted a compressed upload over its cap: it shares this reader precisely so the cap on the upload applies here too")
	}
	if got := BodyErrorStatus(err); got != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 — a kubescrape sender reads 400 as permanent and drops the batch", got)
	}
}
