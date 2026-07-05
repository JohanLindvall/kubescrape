// Package otlpexport sends OTLP payloads to a collector over gRPC.
package otlpexport

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Client exports logs and metrics to one OTLP/gRPC endpoint.
type Client struct {
	conn    *grpc.ClientConn
	logs    plogotlp.GRPCClient
	metrics pmetricotlp.GRPCClient
	timeout time.Duration
}

// New connects (lazily) to an OTLP gRPC endpoint such as
// "otel-collector.monitoring:4317".
func New(endpoint string, insecureConn bool, timeout time.Duration) (*Client, error) {
	creds := credentials.NewTLS(nil)
	if insecureConn {
		creds = insecure.NewCredentials()
	}
	conn, err := grpc.NewClient(endpoint,
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(64*1024*1024)),
	)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:    conn,
		logs:    plogotlp.NewGRPCClient(conn),
		metrics: pmetricotlp.NewGRPCClient(conn),
		timeout: timeout,
	}, nil
}

// Close tears down the connection.
func (c *Client) Close() error { return c.conn.Close() }

// ExportLogs sends one logs payload.
func (c *Client) ExportLogs(ctx context.Context, ld plog.Logs) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	_, err := c.logs.Export(ctx, plogotlp.NewExportRequestFromLogs(ld))
	return err
}

// ExportMetrics sends one metrics payload.
func (c *Client) ExportMetrics(ctx context.Context, md pmetric.Metrics) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	_, err := c.metrics.Export(ctx, pmetricotlp.NewExportRequestFromMetrics(md))
	return err
}
