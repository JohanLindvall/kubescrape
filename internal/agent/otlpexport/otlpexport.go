// Package otlpexport sends OTLP payloads to a collector over gRPC or HTTP.
package otlpexport

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/JohanLindvall/bufpool"

	"github.com/JohanLindvall/kubescrape/internal/bearer"
	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/obs"

	"github.com/JohanLindvall/kubescrape/pkg/otlpsplit"
)

// Config configures the exporter.
type Config struct {
	// Endpoint is host:port for gRPC, or a base URL (scheme required) for
	// HTTP, e.g. "otel-collector:4317" or "https://ingest.example.com:443".
	Endpoint string
	// Protocol is "grpc" or "http" (OTLP/HTTP protobuf on /v1/logs,
	// /v1/metrics and /v1/traces).
	Protocol string
	// Insecure uses plaintext for gRPC (ignored for HTTP; use an http:// or
	// https:// endpoint).
	Insecure bool
	// InsecureSkipVerify disables TLS certificate verification.
	InsecureSkipVerify bool
	// CAFile adds a PEM CA bundle for verifying the collector.
	CAFile string
	// ClientCertFile/ClientKeyFile present a client certificate (mTLS) to the
	// collector. Both or neither; loaded once at startup (rotation needs a
	// restart — acceptable for certs, whose lifetimes are months not minutes,
	// where the bearer token deliberately re-reads).
	ClientCertFile string
	ClientKeyFile  string
	// Headers are static headers sent on every export (HTTP request headers /
	// gRPC metadata) — e.g. a multi-tenant collector's X-Scope-OrgID.
	Headers map[string]string
	// BearerTokenFile is re-read every minute and sent as
	// "Authorization: Bearer <token>". Empty disables.
	BearerTokenFile string
	// Compression is "gzip" (the default, matching collector exporters —
	// telemetry compresses 5-10x) or "none".
	Compression string
	// CompressionLevel is the gzip level (1 fastest .. 9 smallest; 0 = the
	// library default). Level 1 costs ~2-3x less CPU than the default for
	// ~10% larger payloads — the right trade on busy nodes.
	CompressionLevel int
	// Timeout bounds one export attempt.
	Timeout time.Duration
	// RetryAttempts is the number of tries per metrics export (logs have
	// their own at-least-once retry in the tailer). Minimum 1.
	RetryAttempts int
	// RetryBackoff is the initial backoff between metric retries, doubled
	// per attempt.
	RetryBackoff time.Duration
	// MaxSendBytes caps one exported payload's encoded (uncompressed protobuf)
	// size; a larger payload is split into parts each within the cap before
	// sending, so a batch that a non-chunking producer (journald, the tailer)
	// let grow past the collector's gRPC receive limit is delivered in pieces
	// instead of rejected wholesale. 0 uses the default (just under 4 MiB);
	// negative disables splitting.
	MaxSendBytes int
}

// Client exports logs and metrics to one OTLP endpoint.
type Client struct {
	cfg Config

	// partialLog rate-limits the partial_success warning: a collector that
	// rejects one class of record rejects it on every export, and
	// obs.ExportRejected already carries the magnitude.
	partialLog *logdedupe.Table

	// gRPC transport.
	conn    *grpc.ClientConn
	logs    plogotlp.GRPCClient
	metrics pmetricotlp.GRPCClient
	traces  ptraceotlp.GRPCClient

	// HTTP transport.
	httpClient *http.Client
	logsURL    string
	metricsURL string
	tracesURL  string

	// token is the bearer credential this client PRESENTS (nil when
	// BearerTokenFile is unset). Re-read per minute with the last good value
	// kept across a failed re-read: this used to fail the whole export on a
	// transient read error, so a ServiceAccount/Secret projection swap could
	// drop a batch for a credential the process still held.
	token *bearer.File
}

// New creates a Client for cfg.
// Validate checks everything about a Config that can be judged WITHOUT
// touching the filesystem, the network or a TLS keypair — the same checks New
// makes before it builds anything.
//
// It exists so -check-config can cover the OTLP transport flags. That dry run
// returns before the exporter is assembled, so it used to accept
// -otlp-protocol=htttp, -otlp-compression=zstd, an out-of-range compression
// level and TLS material on a plaintext connection: every one of them aborts a
// real start, which is precisely what the dry run promises to catch.
func (cfg Config) Validate() error {
	if cfg.Protocol == "" {
		cfg.Protocol = "grpc"
	}
	switch cfg.Protocol {
	case "grpc", "http":
	default:
		return fmt.Errorf("protocol %q (want grpc or http)", cfg.Protocol)
	}
	switch cfg.Compression {
	case "", "gzip", "none":
	default:
		return fmt.Errorf("compression %q (want gzip or none)", cfg.Compression)
	}
	if cfg.CompressionLevel < 0 || cfg.CompressionLevel > 9 {
		return fmt.Errorf("compression level %d (want 0-9)", cfg.CompressionLevel)
	}
	if cfg.Endpoint == "" {
		return fmt.Errorf("no endpoint")
	}
	if cfg.Protocol == "http" && !strings.HasPrefix(cfg.Endpoint, "http://") && !strings.HasPrefix(cfg.Endpoint, "https://") {
		return fmt.Errorf("endpoint %q needs an http:// or https:// scheme for the http protocol", cfg.Endpoint)
	}
	// TLS material on a plaintext connection is a security control that
	// silently does nothing: gRPC with Insecure discards the whole TLS config,
	// and an http:// endpoint never handshakes. Refuse loudly — an operator who
	// mounted a CA bundle or a client certificate wants TLS, not a no-op, and
	// the failure they would otherwise get is "telemetry shipped in cleartext",
	// which nothing surfaces. (New runs these checks too, via Validate — the
	// constructor is the only gate the metadata service's flag path has.)
	if material := tlsMaterial(cfg); material != "" {
		if cfg.Protocol == "grpc" && cfg.Insecure {
			return fmt.Errorf("%s is set but the gRPC connection is plaintext; set insecure=false (-otlp-insecure=false) for this destination", material)
		}
		if cfg.Protocol == "http" && strings.HasPrefix(cfg.Endpoint, "http://") {
			return fmt.Errorf("%s is set but endpoint %q is plain http; use https://", material, cfg.Endpoint)
		}
	}
	return nil
}

func New(cfg Config) (*Client, error) {
	if cfg.Protocol == "" {
		cfg.Protocol = "grpc"
	}
	if cfg.Compression == "" {
		cfg.Compression = "gzip"
	}
	// ONE set of config checks: New used to re-spell Validate's protocol/
	// compression/level/TLS-material refusals and the two had drifted — New
	// accepted an empty endpoint (gRPC dials lazily, so construction succeeded
	// and every export then failed with a resolver error, transient forever)
	// and any "://"-bearing scheme for HTTP. The drift was reachable: the
	// metadata service constructs its exporter straight from flags with no
	// separate Validate call, so the constructor's checks are the ones it gets.
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	partialLog := logdedupe.New(8, time.Minute)
	if cfg.RetryAttempts < 1 {
		cfg.RetryAttempts = 1
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = time.Second
	}
	// A zero Timeout must NOT reach context.WithTimeout: it yields an
	// already-expired context, so every send fails instantly with
	// DeadlineExceeded. That error is transient AND not a collector response,
	// so with -buffer-dir the poison budget can never arm — the spool retries
	// forever, fills to its cap and back-pressures every producer: a total,
	// permanent telemetry outage with no drop counted and nothing logged as a
	// rejection. Defaulting matches every other field here (and the
	// -otlp-timeout flag's own default); "no timeout at all" is deliberately
	// not offered, since a hung send would stall the tailer's single sweep
	// goroutine indefinitely.
	if cfg.Timeout <= 0 {
		cfg.Timeout = 15 * time.Second
	}
	if cfg.MaxSendBytes == 0 {
		cfg.MaxSendBytes = otlpsplit.DefaultMaxBytes
	}
	if cfg.CompressionLevel != 0 {
		setGzipLevel(cfg.CompressionLevel)
	}
	c := &Client{cfg: cfg, partialLog: partialLog}
	if cfg.BearerTokenFile != "" {
		// Not read here: a collector credential that is not yet projected must
		// not stop the agent from starting, and every export path already
		// surfaces the read error.
		c.token = bearer.NewFile(cfg.BearerTokenFile, nil)
	}

	tlsCfg, err := buildTLS(cfg)
	if err != nil {
		return nil, err
	}

	switch cfg.Protocol {
	case "grpc":
		creds := credentials.NewTLS(tlsCfg)
		if cfg.Insecure {
			creds = insecure.NewCredentials()
		}
		callOpts := []grpc.CallOption{grpc.MaxCallSendMsgSize(64 * 1024 * 1024)}
		if cfg.Compression == "gzip" {
			if err := assertGzipCodec(); err != nil {
				return nil, err
			}
			callOpts = append(callOpts, grpc.UseCompressor(gzipName))
		}
		conn, err := grpc.NewClient(cfg.Endpoint,
			grpc.WithTransportCredentials(creds),
			grpc.WithDefaultCallOptions(callOpts...),
		)
		if err != nil {
			return nil, err
		}
		c.conn = conn
		c.logs = plogotlp.NewGRPCClient(conn)
		c.metrics = pmetricotlp.NewGRPCClient(conn)
		c.traces = ptraceotlp.NewGRPCClient(conn)
	case "http":
		base := strings.TrimRight(cfg.Endpoint, "/") // scheme checked by Validate
		c.logsURL = base + "/v1/logs"
		c.metricsURL = base + "/v1/metrics"
		c.tracesURL = base + "/v1/traces"
		c.httpClient = &http.Client{
			Timeout: cfg.Timeout,
			// One collector host; the ingest receiver forwards concurrently,
			// so two idle connections would re-dial under load.
			Transport: &http.Transport{TLSClientConfig: tlsCfg, MaxIdleConnsPerHost: 16, MaxIdleConns: 16, IdleConnTimeout: 90 * time.Second},
			// Never follow redirects: Go converts a redirected POST to a
			// body-less GET (301/302/303), so an http→https-redirecting
			// ingress would either get a 200 from the wrong endpoint (a
			// false ack — offsets commit, records were never delivered) or
			// a 405 from the collector's GET handler (classified permanent
			// — the buffered drain would drop the whole backlog). Surfacing
			// the 3xx as a status error keeps it transient: the backlog
			// survives until the endpoint config is fixed.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	default:
		return nil, fmt.Errorf("protocol %q (want grpc or http)", cfg.Protocol)
	}
	return c, nil
}

// tlsMaterial names the TLS setting a destination carries, or "" when it
// carries none. Only material that is USED for the handshake counts:
// InsecureSkipVerify is a relaxation, not a control, so a plaintext
// connection carrying it is not a misconfiguration worth refusing.
func tlsMaterial(cfg Config) string {
	switch {
	case cfg.ClientCertFile != "":
		return "clientCertFile"
	case cfg.CAFile != "":
		return "caFile"
	}
	return ""
}

func buildTLS(cfg Config) (*tls.Config, error) {
	tlsCfg := &tls.Config{InsecureSkipVerify: cfg.InsecureSkipVerify}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("reading CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificates in %s", cfg.CAFile)
		}
		tlsCfg.RootCAs = pool
	}
	if cfg.ClientCertFile != "" || cfg.ClientKeyFile != "" {
		if cfg.ClientCertFile == "" || cfg.ClientKeyFile == "" {
			return nil, fmt.Errorf("client certificate needs BOTH clientCertFile and clientKeyFile")
		}
		pair, err := tls.LoadX509KeyPair(cfg.ClientCertFile, cfg.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("loading client certificate: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{pair}
	}
	return tlsCfg, nil
}

// Close tears down the connection.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// bearer returns the current token, re-reading the file at most once per
// minute (ServiceAccount tokens rotate).
//
// The re-read semantics are internal/bearer's: a failure with a last good value
// keeps that value, so a Secret swap costs a warning rather than a dropped
// batch; a failure with nothing cached is still an error, because there is no
// credential to present.
func (c *Client) bearer() (string, error) {
	if c.token == nil {
		return "", nil
	}
	tok, err := c.token.Token()
	if err != nil {
		return "", fmt.Errorf("reading bearer token: %w", err)
	}
	return tok, nil
}

// ExportLogs sends one logs payload (single attempt; the tailer retries and
// rewinds).
func (c *Client) ExportLogs(ctx context.Context, ld plog.Logs) error {
	return c.exportLogsCounted(ctx, ld)
}

// exportLogsCounted is the single-attempt send plus the obs.Exports outcome
// count — the unit both the public method and the buffered drain use, so
// wire-send outcomes stay counted when -buffer-dir routes around ExportLogs.
func (c *Client) exportLogsCounted(ctx context.Context, ld plog.Logs) error {
	err := c.exportLogsOnce(ctx, ld)
	obs.Exports.WithLabelValues("logs", outcome(err)).Inc()
	return err
}

func (c *Client) exportLogsOnce(ctx context.Context, ld plog.Logs) error {
	parts := otlpsplit.Logs(ld, c.cfg.MaxSendBytes)
	for _, part := range parts {
		// A part-send failure returns immediately; the caller retries the whole
		// payload, re-sending the parts already delivered — at-least-once
		// tolerates the duplicates, and nothing commits until every part lands.
		if err := c.sendLogsOnce(ctx, part); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) sendLogsOnce(ctx context.Context, ld plog.Logs) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	if c.conn != nil {
		ctx, err := c.grpcAuth(ctx)
		if err != nil {
			return err
		}
		resp, err := c.logs.Export(ctx, plogotlp.NewExportRequestFromLogs(ld))
		if err == nil {
			c.notePartial("logs", resp.PartialSuccess().RejectedLogRecords(), resp.PartialSuccess().ErrorMessage())
		}
		return err
	}
	body, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	if err != nil {
		return err
	}
	var raw []byte
	if err := c.httpPost(ctx, c.logsURL, body, &raw); err != nil {
		return err
	}
	if len(raw) > 0 {
		resp := plogotlp.NewExportResponse()
		if resp.UnmarshalProto(raw) == nil {
			c.notePartial("logs", resp.PartialSuccess().RejectedLogRecords(), resp.PartialSuccess().ErrorMessage())
		}
	}
	return nil
}

// ExportTraces sends one traces payload (single attempt; the pushing sender
// retries via the ingest receiver's retryable status).
func (c *Client) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	err := c.exportTracesOnce(ctx, td)
	obs.Exports.WithLabelValues("traces", outcome(err)).Inc()
	return err
}

func (c *Client) exportTracesOnce(ctx context.Context, td ptrace.Traces) error {
	for _, part := range otlpsplit.Traces(td, c.cfg.MaxSendBytes) {
		if err := c.sendTracesOnce(ctx, part); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) sendTracesOnce(ctx context.Context, td ptrace.Traces) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	if c.conn != nil {
		ctx, err := c.grpcAuth(ctx)
		if err != nil {
			return err
		}
		resp, err := c.traces.Export(ctx, ptraceotlp.NewExportRequestFromTraces(td))
		if err == nil {
			c.notePartial("traces", resp.PartialSuccess().RejectedSpans(), resp.PartialSuccess().ErrorMessage())
		}
		return err
	}
	body, err := ptraceotlp.NewExportRequestFromTraces(td).MarshalProto()
	if err != nil {
		return err
	}
	var raw []byte
	if err := c.httpPost(ctx, c.tracesURL, body, &raw); err != nil {
		return err
	}
	if len(raw) > 0 {
		resp := ptraceotlp.NewExportResponse()
		if resp.UnmarshalProto(raw) == nil {
			c.notePartial("traces", resp.PartialSuccess().RejectedSpans(), resp.PartialSuccess().ErrorMessage())
		}
	}
	return nil
}

func outcome(err error) string {
	if err != nil {
		return "error"
	}
	return "ok"
}

// ExportMetrics sends one metrics payload with bounded retries.
func (c *Client) ExportMetrics(ctx context.Context, md pmetric.Metrics) error {
	var err error
	backoff := c.cfg.RetryBackoff
	for attempt := 0; attempt < c.cfg.RetryAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return err
			case <-time.After(backoff):
			}
			backoff *= 2
		}
		err = c.exportMetricsCounted(ctx, md)
		if err == nil {
			return nil
		}
		if IsPermanent(err) {
			// A definitive rejection cannot change between attempts; the
			// sibling loops (the tailer's exportWithRetry, the drain's
			// trySend) already short-circuit it, and re-sending a 400 payload
			// RetryAttempts times every scrape cycle was wasted wire and
			// counter inflation for an outcome that cannot move.
			return err
		}
	}
	return err
}

// exportMetricsCounted is one attempt plus the obs.Exports outcome count (see
// exportLogsCounted).
func (c *Client) exportMetricsCounted(ctx context.Context, md pmetric.Metrics) error {
	err := c.exportMetricsOnce(ctx, md)
	obs.Exports.WithLabelValues("metrics", outcome(err)).Inc()
	return err
}

func (c *Client) exportMetricsOnce(ctx context.Context, md pmetric.Metrics) error {
	for _, part := range otlpsplit.Metrics(md, c.cfg.MaxSendBytes) {
		if err := c.sendMetricsOnce(ctx, part); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) sendMetricsOnce(ctx context.Context, md pmetric.Metrics) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	if c.conn != nil {
		ctx, err := c.grpcAuth(ctx)
		if err != nil {
			return err
		}
		resp, err := c.metrics.Export(ctx, pmetricotlp.NewExportRequestFromMetrics(md))
		if err == nil {
			c.notePartial("metrics", resp.PartialSuccess().RejectedDataPoints(), resp.PartialSuccess().ErrorMessage())
		}
		return err
	}
	body, err := pmetricotlp.NewExportRequestFromMetrics(md).MarshalProto()
	if err != nil {
		return err
	}
	var raw []byte
	if err := c.httpPost(ctx, c.metricsURL, body, &raw); err != nil {
		return err
	}
	if len(raw) > 0 {
		resp := pmetricotlp.NewExportResponse()
		if resp.UnmarshalProto(raw) == nil {
			c.notePartial("metrics", resp.PartialSuccess().RejectedDataPoints(), resp.PartialSuccess().ErrorMessage())
		}
	}
	return nil
}

// grpcAuth attaches the bearer token as outgoing gRPC metadata.
func (c *Client) grpcAuth(ctx context.Context) (context.Context, error) {
	token, err := c.bearer()
	if err != nil {
		return nil, err
	}
	for k, v := range c.cfg.Headers {
		ctx = metadata.AppendToOutgoingContext(ctx, k, v)
	}
	if token == "" {
		return ctx, nil
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token), nil
}

// pooledBody hands a pooled buffer to net/http without letting net/http
// decide when it goes back to the pool.
//
// net/http closes a request body TWICE on a 301/302/303: the transport closes
// it when it finishes writing, and Client.do closes it again at the bottom of
// its redirect loop — BEFORE CheckRedirect is consulted, so refusing to follow
// redirects (which this client does) cannot prevent it. bufpool's Close IS
// Release, and while a second Release on the same handle is a no-op (it
// detaches), the second CLOSE is not harmless: a redirect is answered before
// the body has been read, so do()'s close can land while the transport's
// writeLoop is still Reading — and Release resets the handle and hands the
// storage back to a PACKAGE-LEVEL pool, under that read. The pool is shared
// by every client in the process, so the buffer another export then gets is
// one this one is still streaming.
//
// So Close is a no-op here and the release is ours. It happens only once the
// buffer has been read to EOF — the transport is the sole reader and never
// reads again after that — which is also what proves the writeLoop is done
// with it.
//
// A body that never reached EOF is left to the GC rather than pooled. That
// covers a redirect answered early, but ALSO every export that fails before
// the body is written — a refused dial, a TLS error, a cancelled context —
// so with an unreachable collector on the HTTP protocol the gzip pool goes
// cold and each attempt allocates its buffer afresh. That is the price of
// never handing out memory a writeLoop may still be reading: the alternative
// needs proof that no goroutine holds the body, which the net/http API does
// not offer.
type pooledBody struct {
	buf      *bufpool.Buffer
	consumed atomic.Bool
}

func (p *pooledBody) Read(b []byte) (int, error) {
	n, err := p.buf.Read(b)
	if err != nil { // io.EOF: nobody will read this buffer again
		p.consumed.Store(true)
	}
	return n, err
}

// Close is what net/http calls — possibly twice. It must not release.
func (p *pooledBody) Close() error { return nil }

// release pools the buffer if the transport provably finished with it. The
// CompareAndSwap makes it idempotent.
func (p *pooledBody) release() {
	if p.consumed.CompareAndSwap(true, false) {
		p.buf.Release()
	}
}

func (c *Client) httpPost(ctx context.Context, url string, body []byte, out *[]byte) error {
	token, err := c.bearer()
	if err != nil {
		return err
	}
	var bodyReader io.Reader = bytes.NewReader(body)
	compressed := c.cfg.Compression == "gzip"
	var pb *pooledBody
	if compressed {
		gz, gerr := gzipBody(body)
		if gerr != nil {
			return gerr
		}
		pb = &pooledBody{buf: gz}
		bodyReader = pb
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bodyReader)
	if err != nil {
		if pb != nil {
			pb.buf.Release() // nothing has read it; safe to pool immediately
		}
		return err
	}
	if pb != nil {
		// NewRequest doesn't recognize the pooled buffer type, so set the
		// length explicitly (the body would go out chunked otherwise). Do NOT
		// set req.GetBody: a transport-level replay after the first attempt
		// closes (releases) the buffer would read reused memory.
		req.ContentLength = int64(pb.buf.Len())
		// Released HERE, never by net/http: see pooledBody.
		defer pb.release()
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	if compressed {
		req.Header.Set("Content-Encoding", "gzip")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range c.cfg.Headers {
		req.Header.Set(k, v)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return &HTTPStatusError{Code: resp.StatusCode, Body: strings.TrimSpace(string(msg))}
	}
	// A 2xx may still carry partial_success — the protocol's channel for "I
	// accepted the request and REJECTED n records". Discarding it made every
	// producer treat the send as fully delivered and advance its offset,
	// cursor or position past records the collector threw away: silent,
	// uncounted, permanent loss. Bounded: an ExportResponse is a handful of
	// bytes and a message.
	if out != nil {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, maxExportRespBytes))
		*out = b
	}
	return nil
}

// maxExportRespBytes bounds the success body read for partial_success. The
// response is a rejected count and an error string; anything larger is a
// collector misbehaving and is not worth the allocation.
const maxExportRespBytes = 16 << 10

// HTTPStatusError is a non-2xx response from the OTLP/HTTP collector, typed so
// callers (the buffered drain, the ingest receiver) can classify permanent
// rejections vs transient failures.
type HTTPStatusError struct {
	Code int
	Body string
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("status %d: %s", e.Code, e.Body)
}

// notePartial reports an OTLP partial_success: the collector ACCEPTED the
// request and rejected n records within it.
//
// It cannot fail the export. partial_success means the rest landed, and OTLP
// defines the rejected records as invalid rather than deferred — a retry
// re-sends the whole payload and the collector rejects the same records again,
// so treating it as failure would duplicate everything that succeeded. So the
// honest handling is the same one the tailer's permanent-rejection path uses:
// let the producer advance, and COUNT the loss so it is never silent. It used
// to be discarded entirely on both transports, which made every producer report
// full delivery for a payload the collector had partly thrown away.
func (c *Client) notePartial(signal string, rejected int64, msg string) {
	if rejected <= 0 && msg == "" {
		return
	}
	if rejected > 0 {
		obs.ExportRejected.WithLabelValues(signal).Add(float64(rejected))
	}
	// Rate-limited: a collector rejecting a class of record does so on every
	// export, and the counter carries the magnitude. The Client has no logger
	// of its own (it is constructed from Config alone), so this uses the
	// process default, as the rest of the package's diagnostics do.
	if allow, _ := c.partialLog.Allow(signal); allow {
		slog.Default().Warn("the collector accepted the payload but rejected records within it (partial_success)",
			"signal", signal, "rejected", rejected, "collector_message", msg)
	}
}
