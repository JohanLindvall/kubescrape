package otlpexport

// Per-signal export destinations — the piece that makes COLLECTORLESS
// deployment expressible: Mimir, Loki and Tempo all ingest OTLP natively but
// on different hosts/paths, while the flags configure exactly one endpoint.
// The `export` config section overlays per-signal endpoint/protocol/headers/
// auth/TLS onto the flag-built base config, and PerSignal routes each signal
// to its client. It slots in where the raw Client does — UNDER Buffered, so
// each signal's spool drains to that signal's destination and the default
// chain finally carries static headers (X-Scope-OrgID tenancy) WITH the disk
// buffer, which routing alone (direct, unbuffered by design) never could.

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// ExportOverride is one signal's destination overrides; every empty/nil field
// inherits the base (flag-built) config. Headers MERGE over the base's (the
// override winning per key) — replacing wholesale would silently drop a
// base tenancy header the moment a signal override added an unrelated one.
type ExportOverride struct {
	Endpoint           string            `json:"endpoint,omitempty"`
	Protocol           string            `json:"protocol,omitempty"`
	Headers            map[string]string `json:"headers,omitempty"`
	BearerTokenFile    string            `json:"bearerTokenFile,omitempty"`
	CAFile             string            `json:"caFile,omitempty"`
	Insecure           *bool             `json:"insecure,omitempty"`
	InsecureSkipVerify *bool             `json:"insecureSkipVerify,omitempty"`
	Compression        string            `json:"compression,omitempty"`
	ClientCertFile     string            `json:"clientCertFile,omitempty"`
	ClientKeyFile      string            `json:"clientKeyFile,omitempty"`
}

// ExportConfig is the agent config's `export` section: base additions that
// flags do not carry (static headers, an mTLS client certificate) plus the
// per-signal destination overrides.
type ExportConfig struct {
	// Headers are sent on every export of every signal (HTTP request headers /
	// gRPC metadata) — e.g. a multi-tenant collector's X-Scope-OrgID. Unlike
	// the routing section's headers these ride the DEFAULT chain, disk buffer
	// included.
	Headers map[string]string `json:"headers,omitempty"`
	// ClientCertFile/ClientKeyFile present a client certificate (mTLS) on
	// every signal unless a per-signal override replaces it.
	ClientCertFile string `json:"clientCertFile,omitempty"`
	ClientKeyFile  string `json:"clientKeyFile,omitempty"`

	Logs    *ExportOverride `json:"logs,omitempty"`
	Metrics *ExportOverride `json:"metrics,omitempty"`
	Traces  *ExportOverride `json:"traces,omitempty"`
}

// Validate checks the section's SHAPE without touching the filesystem or the
// network, so -check-config stays a pure dry run (file errors surface at the
// real start, where the clients are built).
func (c *ExportConfig) Validate() error {
	if c == nil {
		return nil
	}
	if (c.ClientCertFile == "") != (c.ClientKeyFile == "") {
		return fmt.Errorf("clientCertFile and clientKeyFile must be set together")
	}
	for name, o := range map[string]*ExportOverride{"logs": c.Logs, "metrics": c.Metrics, "traces": c.Traces} {
		if o == nil {
			continue
		}
		switch o.Protocol {
		case "", "grpc", "http":
		default:
			return fmt.Errorf("export.%s.protocol %q (want grpc or http)", name, o.Protocol)
		}
		switch o.Compression {
		case "", "gzip", "none":
		default:
			return fmt.Errorf("export.%s.compression %q (want gzip or none)", name, o.Compression)
		}
		if (o.ClientCertFile == "") != (o.ClientKeyFile == "") {
			return fmt.Errorf("export.%s: clientCertFile and clientKeyFile must be set together", name)
		}
		// The same scheme rule New enforces at the real start, checked here so
		// -check-config catches it too (only when the override itself declares
		// the http protocol — an inherited base protocol is a flag Validate
		// cannot see, and New still refuses at startup).
		if o.Protocol == "http" && o.Endpoint != "" && !strings.Contains(o.Endpoint, "://") {
			return fmt.Errorf("export.%s.endpoint %q needs a scheme (http:// or https://)", name, o.Endpoint)
		}
	}
	return nil
}

// merged overlays o onto base and returns the per-signal client config.
func (o *ExportOverride) merged(base Config) Config {
	out := base
	if o.Endpoint != "" {
		out.Endpoint = o.Endpoint
	}
	if o.Protocol != "" {
		out.Protocol = o.Protocol
	}
	if len(o.Headers) > 0 {
		merged := make(map[string]string, len(base.Headers)+len(o.Headers))
		for k, v := range base.Headers {
			merged[k] = v
		}
		for k, v := range o.Headers {
			merged[k] = v
		}
		out.Headers = merged
	}
	if o.BearerTokenFile != "" {
		out.BearerTokenFile = o.BearerTokenFile
	}
	if o.CAFile != "" {
		out.CAFile = o.CAFile
	}
	if o.Insecure != nil {
		out.Insecure = *o.Insecure
	}
	if o.InsecureSkipVerify != nil {
		out.InsecureSkipVerify = *o.InsecureSkipVerify
	}
	if o.Compression != "" {
		out.Compression = o.Compression
	}
	if o.ClientCertFile != "" {
		out.ClientCertFile = o.ClientCertFile
		out.ClientKeyFile = o.ClientKeyFile
	}
	return out
}

// PerSignal routes each signal to its own Client; a nil per-signal client
// falls back to Default. It implements Exporter and TracesExporter and sits
// exactly where the raw Client otherwise would (under Buffered, under the
// router), so the whole durability chain applies per destination.
type PerSignal struct {
	Default               *Client
	Logs, Metrics, Traces *Client
}

// ValidateAgainst checks the section's shape AND every merged per-signal
// destination — exactly the Configs BuildExporter hands to New.
//
// Validate alone is not enough for a dry run. Every rule that only becomes
// checkable AFTER the merge was invisible to -check-config: TLS material on a
// destination that inherits plaintext gRPC from the flags, and the http://
// scheme requirement when the protocol is inherited from -otlp-protocol rather
// than declared on the override (Validate deliberately skips that case because
// it cannot see the flags). So -check-config exited 0 and the same ConfigMap
// CrashLooped the fleet at `creating OTLP exporter`, from the check whose whole
// purpose is preventing that.
//
// Still shape-only in the sense that matters: Config.Validate touches neither
// the filesystem nor the network.
func (c *ExportConfig) ValidateAgainst(base Config) error {
	if err := c.Validate(); err != nil {
		return err
	}
	merged := c.ApplyBase(base)
	if c != nil {
		for _, s := range []struct {
			name string
			o    *ExportOverride
		}{{"logs", c.Logs}, {"metrics", c.Metrics}, {"traces", c.Traces}} {
			if s.o == nil {
				continue
			}
			if err := s.o.merged(merged).Validate(); err != nil {
				return fmt.Errorf("export.%s: %w", s.name, err)
			}
		}
	}
	// The default chain is only BUILT when a signal falls through to it, so it
	// is only validated then — BuildExporter skips it entirely once all three
	// are overridden (the collectorless case, where the flag endpoint still
	// points at the stock collector address nothing dials).
	if c == nil || c.Logs == nil || c.Metrics == nil || c.Traces == nil {
		if err := merged.Validate(); err != nil {
			return fmt.Errorf("otlp flags: %w", err)
		}
	}
	return nil
}

// ApplyBase overlays the section's BASE additions (headers, client cert) onto
// a flag-derived config — shared by BuildExporter and the routing clients, so
// a tenancy header set once in `export.headers` reaches route destinations
// too instead of silently applying to only the default chain.
func (c *ExportConfig) ApplyBase(base Config) Config {
	if c == nil {
		return base
	}
	if len(c.Headers) > 0 {
		merged := make(map[string]string, len(base.Headers)+len(c.Headers))
		for k, v := range base.Headers {
			merged[k] = v
		}
		for k, v := range c.Headers {
			merged[k] = v
		}
		base.Headers = merged
	}
	if c.ClientCertFile != "" {
		base.ClientCertFile = c.ClientCertFile
		base.ClientKeyFile = c.ClientKeyFile
	}
	return base
}

// BuildExporter builds the export stack's bottom layer from the flag-derived
// base config plus the optional `export` section. Always a *PerSignal (with
// only Default set when nothing is overridden) so callers have one shape.
func BuildExporter(base Config, cfg *ExportConfig) (*PerSignal, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	base = cfg.ApplyBase(base)
	ps := &PerSignal{}
	var err error
	build := func(name string, o *ExportOverride) (*Client, error) {
		if o == nil {
			return nil, nil
		}
		c, err := New(o.merged(base))
		if err != nil {
			return nil, fmt.Errorf("export.%s: %w", name, err)
		}
		return c, nil
	}
	if cfg != nil {
		if ps.Logs, err = build("logs", cfg.Logs); err != nil {
			return nil, err
		}
		if ps.Metrics, err = build("metrics", cfg.Metrics); err != nil {
			ps.closeBuilt()
			return nil, err
		}
		if ps.Traces, err = build("traces", cfg.Traces); err != nil {
			ps.closeBuilt()
			return nil, err
		}
	}
	// The default is the FALLBACK destination, so build it only when a signal
	// can actually reach it. With all three overridden it is unreachable, and
	// constructing it anyway would dial (and validate) a collector endpoint the
	// deployment deliberately does not use — which is exactly the collectorless
	// case, where the flag default still points at the stock collector address
	// and the base may legitimately be plaintext while every real destination
	// is TLS.
	if ps.Logs == nil || ps.Metrics == nil || ps.Traces == nil {
		if ps.Default, err = New(base); err != nil {
			ps.closeBuilt()
			return nil, err
		}
	}
	return ps, nil
}

// closeBuilt releases the clients constructed so far, for the error paths
// where BuildExporter gives up midway (the caller never sees the PerSignal
// and would otherwise leak their gRPC connections).
func (p *PerSignal) closeBuilt() {
	_ = p.Close()
}

// logsClient/metricsClient/tracesClient resolve the signal's destination.
func (p *PerSignal) logsClient() *Client {
	if p.Logs != nil {
		return p.Logs
	}
	return p.Default
}

func (p *PerSignal) metricsClient() *Client {
	if p.Metrics != nil {
		return p.Metrics
	}
	return p.Default
}

func (p *PerSignal) tracesClient() *Client {
	if p.Traces != nil {
		return p.Traces
	}
	return p.Default
}

// ExportLogs sends via the logs destination.
func (p *PerSignal) ExportLogs(ctx context.Context, ld plog.Logs) error {
	return p.logsClient().ExportLogs(ctx, ld)
}

// ExportMetrics sends via the metrics destination.
func (p *PerSignal) ExportMetrics(ctx context.Context, md pmetric.Metrics) error {
	return p.metricsClient().ExportMetrics(ctx, md)
}

// ExportTraces sends via the traces destination.
func (p *PerSignal) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	return p.tracesClient().ExportTraces(ctx, td)
}

// Close tears down every distinct client.
func (p *PerSignal) Close() error {
	var first error
	seen := map[*Client]bool{}
	for _, c := range []*Client{p.Default, p.Logs, p.Metrics, p.Traces} {
		if c == nil || seen[c] {
			continue
		}
		seen[c] = true
		if err := c.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
