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
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// ExportOverride is one signal's destination overrides. An empty/nil field
// inherits, but WHAT it inherits depends on whether the override names its own
// endpoint — see signalConfig, which owns that rule. Headers MERGE over what
// is inherited (the override winning per key) — replacing wholesale would
// silently drop a base tenancy header the moment a signal override added an
// unrelated one.
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
		return errors.New("clientCertFile and clientKeyFile must be set together")
	}
	// In signal order, never off a map: a section with two mistakes must name
	// the same one on every run, or a fixed error is followed by a different one
	// the previous run hid — and a test of the wording can only pin one.
	for _, sig := range c.overrides() {
		name, o := sig.name, sig.override
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

// signalConfig derives ONE signal's client config from the FLAG base plus this
// section: the section's own base additions (headers, client certificate)
// apply first, then the override's fields.
//
// A signal naming its OWN endpoint does NOT inherit the FLAG base's
// destination credentials. transport.go states the rule this implements: the
// bearer token, the CA bundle and the skip-verify decision that reach this
// process through -otlp-bearer-token-file / -otlp-tls-ca-file /
// -otlp-tls-insecure-skip-verify describe the deployment's OWN collector, and
// the whole point of the `export` section is naming a DIFFERENT host —
// typically a third-party SaaS backend. Copying the base wholesale and
// overwriting only what the override sets presented the collector's bearer
// token and mTLS client certificate to that backend on every export, and
// carried insecureSkipVerify=true over so the third party's certificate was
// not verified either, with no per-signal field that could have opted out.
// So the destination is rebuilt on Config.TransportOnly — the one spelling of
// the transport-vs-destination partition, shared with routeExportConfig and
// the reshard hop — and every credential is taken from the override or left
// unset. cmd/kubescrape-agent's routeExportConfig takes the same decision for
// the identical shape.
//
// Two deliberate carryovers, both argued rather than convenient:
//
//   - The SECTION's own base additions — `export.headers` and
//     `export.clientCertFile`/`clientKeyFile` — still apply. They are not
//     inherited from a field left empty: they are declared in the same
//     `export:` block, at every-signal scope, by the author who named this
//     endpoint, and the documented collectorless example relies on exactly
//     that (one tenancy header and one mTLS identity towards three backends).
//     Neither has a flag, so nothing collector-scoped can arrive this way; an
//     override's own headers still merge over them and its own client
//     certificate still replaces them.
//   - Insecure (plaintext gRPC) is carried from the flag base unless the
//     override sets it. Plaintext-ness is transport to the named host, not a
//     credential, and a bool zero value here would flip every own-endpoint
//     signal written against a plaintext in-cluster collector to TLS on
//     upgrade — routeExportConfig's argument, verbatim.
//
// An override REPEATING the base endpoint names the same destination, so
// nothing crosses a boundary there and the base applies unchanged.
func (c *ExportConfig) signalConfig(o *ExportOverride, flagBase Config) Config {
	out := c.ApplyBase(flagBase)
	if o == nil {
		return out
	}
	if o.Endpoint != "" && o.Endpoint != flagBase.Endpoint {
		// Rebuild: transport tuning, plus the section's own additions applied
		// onto nothing, plus the plaintext decision. Everything else — the
		// endpoint, the token, the CA, the trust decision — comes from the
		// override below or stays unset.
		out = c.ApplyBase(flagBase.TransportOnly())
		out.Insecure = flagBase.Insecure
		out.Endpoint = o.Endpoint
	} else if o.Endpoint != "" {
		out.Endpoint = o.Endpoint
	}
	if o.Protocol != "" {
		out.Protocol = o.Protocol
	}
	out.Headers = MergeHeaders(out.Headers, o.Headers)
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

// droppedBaseCredentials names the FLAGS whose values signalConfig refuses to
// carry to o's own endpoint, in a fixed order. Empty when the override
// inherits (no endpoint of its own, or the base's), when the flag carries
// nothing, or when the override supplies its own — so the caller's warning
// fires only where behaviour actually differs from a naive merge.
func droppedBaseCredentials(o *ExportOverride, flagBase Config) []string {
	if o == nil || o.Endpoint == "" || o.Endpoint == flagBase.Endpoint {
		return nil
	}
	var dropped []string
	if flagBase.BearerTokenFile != "" && o.BearerTokenFile == "" {
		dropped = append(dropped, "-otlp-bearer-token-file")
	}
	if flagBase.CAFile != "" && o.CAFile == "" {
		dropped = append(dropped, "-otlp-tls-ca-file")
	}
	if flagBase.InsecureSkipVerify && o.InsecureSkipVerify == nil {
		dropped = append(dropped, "-otlp-tls-insecure-skip-verify")
	}
	return dropped
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
		for _, sig := range c.overrides() {
			if sig.override == nil {
				continue
			}
			if err := c.signalConfig(sig.override, base).Validate(); err != nil {
				return fmt.Errorf("export.%s: %w", sig.name, err)
			}
			// Said out loud on the one seam BOTH -check-config and every real
			// start cross (validateConfig calls this): an own-endpoint signal
			// silently losing the flag base's collector credentials would
			// otherwise surface only as a transient export failure against a
			// backend the operator believes is authenticated. Warn, not refuse
			// — dropping the credential IS the correct destination, and the
			// override has a field for every one of them.
			if dropped := droppedBaseCredentials(sig.override, base); len(dropped) > 0 {
				slog.Default().Warn("this export destination names its own endpoint, so the flag base's collector credentials are NOT presented to it",
					"signal", sig.name, "endpoint", sig.override.Endpoint, "flag", strings.Join(dropped, ","),
					"note", "they authenticate this deployment to ITS collector and this is a different host; set bearerTokenFile / caFile / insecureSkipVerify on the export."+sig.name+" override itself if this destination needs them")
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
	base.Headers = MergeHeaders(base.Headers, c.Headers)
	if c.ClientCertFile != "" {
		base.ClientCertFile = c.ClientCertFile
		base.ClientKeyFile = c.ClientKeyFile
	}
	return base
}

// MergeHeaders overlays over onto base, per key, into a FRESH map — the base is
// shared by every destination derived from it and must not be written through.
// With nothing to overlay the base is returned as is (nil stays nil). One
// function for the three places a header layer is applied: the section's base
// additions (ApplyBase), a per-signal override (signalConfig) and a routing route's
// own headers (cmd/kubescrape-agent's routeExportConfig), which each spelled
// the same seven lines.
func MergeHeaders(base, over map[string]string) map[string]string {
	if len(over) == 0 {
		return base
	}
	merged := make(map[string]string, len(base)+len(over))
	for k, v := range base {
		merged[k] = v
	}
	for k, v := range over {
		merged[k] = v
	}
	return merged
}

// signalOverride pairs a per-signal override with the name the section spells
// it by, so a walk over the three is in a FIXED order.
type signalOverride struct {
	name     string
	override *ExportOverride
}

// overrides lists the section's per-signal overrides in signal order (logs,
// metrics, traces), nil entries included: every walk over "the three signals"
// goes through this so none of them can iterate a map.
func (c *ExportConfig) overrides() []signalOverride {
	return []signalOverride{{"logs", c.Logs}, {"metrics", c.Metrics}, {"traces", c.Traces}}
}

// BuildExporter builds the export stack's bottom layer from the flag-derived
// base config plus the optional `export` section. Always a *PerSignal (with
// only Default set when nothing is overridden) so callers have one shape.
func BuildExporter(base Config, cfg *ExportConfig) (*PerSignal, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	ps := &PerSignal{}
	var err error
	// base stays the FLAG base: signalConfig needs to tell the section's own
	// additions from the flag credentials, which an up-front ApplyBase would
	// have already fused.
	build := func(name string, o *ExportOverride) (*Client, error) {
		if o == nil {
			return nil, nil
		}
		c, err := New(cfg.signalConfig(o, base))
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
		if ps.Default, err = New(cfg.ApplyBase(base)); err != nil {
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

// singleAttemptSends implements the drain's singleAttempt seam (buffered.go):
// each signal's counted one-shot send, resolved to that signal's destination.
func (p *PerSignal) singleAttemptSends() (func(context.Context, plog.Logs) error, func(context.Context, pmetric.Metrics) error) {
	return p.logsClient().exportLogsCounted, p.metricsClient().exportMetricsCounted
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
