// Package otlpexport sends OTLP payloads to a collector over gRPC or HTTP.
package otlpexport

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
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

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// Config configures the exporter.
type Config struct {
	// Endpoint is host:port for gRPC, or a base URL (scheme required) for
	// HTTP, e.g. "otel-collector:4317" or "https://ingest.example.com:443".
	Endpoint string
	// Protocol is "grpc" or "http" (OTLP/HTTP protobuf on /v1/logs and
	// /v1/metrics).
	Protocol string
	// Insecure uses plaintext for gRPC (ignored for HTTP; use an http:// or
	// https:// endpoint).
	Insecure bool
	// InsecureSkipVerify disables TLS certificate verification.
	InsecureSkipVerify bool
	// CAFile adds a PEM CA bundle for verifying the collector.
	CAFile string
	// BearerTokenFile is re-read every minute and sent as
	// "Authorization: Bearer <token>". Empty disables.
	BearerTokenFile string
	// Compression is "gzip" (the default, matching collector exporters —
	// telemetry compresses 5-10x) or "none".
	Compression string
	// Timeout bounds one export attempt.
	Timeout time.Duration
	// RetryAttempts is the number of tries per metrics export (logs have
	// their own at-least-once retry in the tailer). Minimum 1.
	RetryAttempts int
	// RetryBackoff is the initial backoff between metric retries, doubled
	// per attempt.
	RetryBackoff time.Duration
}

// Client exports logs and metrics to one OTLP endpoint.
type Client struct {
	cfg Config

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

	tokenMu      sync.Mutex
	token        string
	tokenFetched time.Time
}

// New creates a Client for cfg.
func New(cfg Config) (*Client, error) {
	if cfg.Protocol == "" {
		cfg.Protocol = "grpc"
	}
	if cfg.RetryAttempts < 1 {
		cfg.RetryAttempts = 1
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = time.Second
	}
	switch cfg.Compression {
	case "":
		cfg.Compression = "gzip"
	case "gzip", "none":
	default:
		return nil, fmt.Errorf("compression %q (want gzip or none)", cfg.Compression)
	}
	c := &Client{cfg: cfg}

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
		base := strings.TrimRight(cfg.Endpoint, "/")
		if !strings.Contains(base, "://") {
			return nil, fmt.Errorf("http endpoint %q needs a scheme (http:// or https://)", cfg.Endpoint)
		}
		c.logsURL = base + "/v1/logs"
		c.metricsURL = base + "/v1/metrics"
		c.tracesURL = base + "/v1/traces"
		c.httpClient = &http.Client{
			Timeout:   cfg.Timeout,
			Transport: &http.Transport{TLSClientConfig: tlsCfg, MaxIdleConnsPerHost: 2},
		}
	default:
		return nil, fmt.Errorf("protocol %q (want grpc or http)", cfg.Protocol)
	}
	return c, nil
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
func (c *Client) bearer() (string, error) {
	if c.cfg.BearerTokenFile == "" {
		return "", nil
	}
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	if time.Since(c.tokenFetched) < time.Minute && c.token != "" {
		return c.token, nil
	}
	data, err := os.ReadFile(c.cfg.BearerTokenFile)
	if err != nil {
		return "", fmt.Errorf("reading bearer token: %w", err)
	}
	c.token = strings.TrimSpace(string(data))
	c.tokenFetched = time.Now()
	return c.token, nil
}

// ExportLogs sends one logs payload (single attempt; the tailer retries and
// rewinds).
func (c *Client) ExportLogs(ctx context.Context, ld plog.Logs) error {
	err := c.exportLogsOnce(ctx, ld)
	obs.Exports.WithLabelValues("logs", outcome(err)).Inc()
	return err
}

func (c *Client) exportLogsOnce(ctx context.Context, ld plog.Logs) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	if c.conn != nil {
		ctx, err := c.grpcAuth(ctx)
		if err != nil {
			return err
		}
		_, err = c.logs.Export(ctx, plogotlp.NewExportRequestFromLogs(ld))
		return err
	}
	body, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	if err != nil {
		return err
	}
	return c.httpPost(ctx, c.logsURL, body)
}

// ExportTraces sends one traces payload (single attempt; the pushing sender
// retries via the ingest receiver's retryable status).
func (c *Client) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	err := c.exportTracesOnce(ctx, td)
	obs.Exports.WithLabelValues("traces", outcome(err)).Inc()
	return err
}

func (c *Client) exportTracesOnce(ctx context.Context, td ptrace.Traces) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	if c.conn != nil {
		ctx, err := c.grpcAuth(ctx)
		if err != nil {
			return err
		}
		_, err = c.traces.Export(ctx, ptraceotlp.NewExportRequestFromTraces(td))
		return err
	}
	body, err := ptraceotlp.NewExportRequestFromTraces(td).MarshalProto()
	if err != nil {
		return err
	}
	return c.httpPost(ctx, c.tracesURL, body)
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
		err = c.exportMetricsOnce(ctx, md)
		obs.Exports.WithLabelValues("metrics", outcome(err)).Inc()
		if err == nil {
			return nil
		}
	}
	return err
}

func (c *Client) exportMetricsOnce(ctx context.Context, md pmetric.Metrics) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	if c.conn != nil {
		ctx, err := c.grpcAuth(ctx)
		if err != nil {
			return err
		}
		_, err = c.metrics.Export(ctx, pmetricotlp.NewExportRequestFromMetrics(md))
		return err
	}
	body, err := pmetricotlp.NewExportRequestFromMetrics(md).MarshalProto()
	if err != nil {
		return err
	}
	return c.httpPost(ctx, c.metricsURL, body)
}

// grpcAuth attaches the bearer token as outgoing gRPC metadata.
func (c *Client) grpcAuth(ctx context.Context) (context.Context, error) {
	token, err := c.bearer()
	if err != nil {
		return nil, err
	}
	if token == "" {
		return ctx, nil
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token), nil
}

func (c *Client) httpPost(ctx context.Context, url string, body []byte) error {
	compressed := c.cfg.Compression == "gzip"
	if compressed {
		var err error
		if body, err = gzipBody(body); err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	if compressed {
		req.Header.Set("Content-Encoding", "gzip")
	}
	token, err := c.bearer()
	if err != nil {
		return err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
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
	return nil
}

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
