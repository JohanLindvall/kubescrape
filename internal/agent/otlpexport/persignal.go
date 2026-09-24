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
	"maps"
	"strings"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// ExportOverride is one signal's destination overrides. An empty/nil field
// inherits, but WHAT it inherits depends on whether the override names its own
// endpoint — see destinationConfig, which owns that rule. Headers MERGE over what
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
//
// It checks only what no MERGED destination can: the section's own client
// pair. ApplyBase takes that pair only when its certificate is set, so a lone
// key never reaches any Config.Validate — and with all three signals
// overridden no merged config carries the pair at all. Everything an OVERRIDE
// sets (protocol, compression, the http scheme, its own client pair) lands in
// the merged per-signal Config, and both callers validate every one of those:
// ValidateAgainst directly, BuildExporter through New. This used to repeat
// those rules per override, and the copied scheme test had already drifted
// looser than Config.Validate's (any "://" against http:// or https://).
func (c *ExportConfig) Validate() error {
	if c == nil {
		return nil
	}
	if (c.ClientCertFile == "") != (c.ClientKeyFile == "") {
		return errors.New("clientCertFile and clientKeyFile must be set together")
	}
	return nil
}

// signalConfig derives ONE signal's client config from the FLAG base plus this
// section: the section's own base additions (headers, client certificate)
// apply first, then the override's fields. It is destinationConfig with the
// section's mTLS identity carried to an own endpoint — see there.
func (c *ExportConfig) signalConfig(o *ExportOverride, flagBase Config) Config {
	return c.destinationConfig(o, flagBase, true)
}

// RouteConfig derives a routing route's client config: the same derivation as
// an export.<signal> override (destinationConfig), with the ONE deliberate
// difference that the section's clientCertFile/clientKeyFile never reach a
// route naming its own endpoint. o is the route's destination fields
// (route.Route.ExportOverride); a route has no protocol or compression of its
// own, so those are always the base's.
func (c *ExportConfig) RouteConfig(o *ExportOverride, flagBase Config) Config {
	return c.destinationConfig(o, flagBase, false)
}

// OwnEndpoint reports whether endpoint names a destination OTHER than the flag
// base's: non-empty and not -otlp-endpoint repeated. It is the one test for "a
// different host" — destinationConfig, DroppedBaseCredentials and
// BaseEndpointUnused all ask it — so an export.<signal> override and a routing
// route repeating -otlp-endpoint are the base destination alike, and never
// lose the credentials issued for it.
func OwnEndpoint(endpoint string, flagBase Config) bool {
	return endpoint != "" && endpoint != flagBase.Endpoint
}

// destinationConfig is the ONE derivation of a destination an override
// declares — an export.<signal> (signalConfig) or a routing route
// (RouteConfig). Two used to exist, and they drifted on every point that
// matters here: which endpoint counts as another host, whether skip-verify
// crosses to it, what an endpoint-less route does with its own credentials.
//
// A destination naming its OWN endpoint (OwnEndpoint) does NOT inherit the
// FLAG base's destination credentials. transport.go states the rule this
// implements: the bearer token, the CA bundle and the skip-verify decision
// that reach this process through -otlp-bearer-token-file / -otlp-tls-ca-file
// / -otlp-tls-insecure-skip-verify describe the deployment's OWN collector,
// and the whole point of the `export` section — and of a route with an
// endpoint — is naming a DIFFERENT host, typically a third-party SaaS backend
// or another tenant's collector. Copying the base wholesale and overwriting
// only what the override sets presented the collector's bearer token and mTLS
// client certificate to that host on every export, and carried
// insecureSkipVerify=true over so its certificate was not verified either,
// with no field that could have opted out. So the destination is rebuilt on
// Config.TransportOnly — the one spelling of the transport-vs-destination
// partition, shared with the reshard hop — and every credential is taken from
// the override or left unset.
//
// Two deliberate carryovers, both argued rather than convenient:
//
//   - The SECTION's own base additions. `export.headers` always applies. The
//     section's `clientCertFile`/`clientKeyFile` applies only when
//     sectionIdentity is set — for an export.<signal>, never for a route. They
//     are not inherited from a field left empty: they are declared in the same
//     `export:` block, at every-signal scope, by the author who named that
//     signal's endpoint, and the documented collectorless example relies on
//     exactly that (one tenancy header and one mTLS identity towards three
//     backends). A route is declared under `routing:`, usually for another
//     tenant, so the section's mTLS identity is not its author's to present
//     there — a route presents only its own pair. Neither addition has a flag,
//     so nothing collector-scoped arrives this way; the override's own headers
//     still merge over them and its own client pair still replaces them.
//   - Insecure (plaintext gRPC) is carried from the flag base unless the
//     override sets it. Plaintext-ness is transport to the named host, not a
//     credential, and a bool zero value here would flip every own-endpoint
//     destination written against a plaintext in-cluster collector to TLS on
//     upgrade — an endless transient export failure -check-config cannot see.
//
// An override with NO endpoint, or REPEATING the base's, names the base
// destination, so nothing crosses a boundary: the merged base applies
// (credentials included) and every field the override sets wins over it. For
// a route that means an endpoint-less route's own bearerTokenFile, caFile,
// client pair, insecure and insecureSkipVerify are applied to the default
// destination it is reached through, exactly as an export.<signal> without an
// endpoint applies them.
//
// The override's client pair is taken whole when EITHER half is set, so a half
// pair reaches Config.Validate and is refused by name there — taking it only
// when the certificate was set silently dropped a lone key and kept the base's
// pair in its place.
func (c *ExportConfig) destinationConfig(o *ExportOverride, flagBase Config, sectionIdentity bool) Config {
	out := c.ApplyBase(flagBase)
	if o == nil {
		return out
	}
	if OwnEndpoint(o.Endpoint, flagBase) {
		// Rebuild: transport tuning, plus the section's own additions applied
		// onto nothing, plus the plaintext decision. Everything else — the
		// endpoint, the token, the CA, the trust decision — comes from the
		// override below or stays unset.
		out = c.applyBase(flagBase.TransportOnly(), sectionIdentity)
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
	if o.ClientCertFile != "" || o.ClientKeyFile != "" {
		out.ClientCertFile = o.ClientCertFile
		out.ClientKeyFile = o.ClientKeyFile
	}
	return out
}

// DroppedBaseCredentials names the FLAGS whose values destinationConfig
// refuses to carry to o's own endpoint, in a fixed order — for an
// export.<signal> override and a routing route alike, so the two seams that
// drop the same flags report them the same way. Empty when the destination
// inherits (no endpoint of its own, or the base's), when the flag carries
// nothing, or when the override supplies its own — so the caller's warning
// fires only where behaviour actually differs from a naive merge.
func DroppedBaseCredentials(o *ExportOverride, flagBase Config) []string {
	if o == nil || !OwnEndpoint(o.Endpoint, flagBase) {
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
// Validate alone is not enough for a dry run: it checks only the section's own
// client pair, and every rule about an override is judged here, on the merged
// destination. That is the only place some of them CAN be judged — TLS
// material on a destination that inherits plaintext gRPC from the flags, the
// http:// scheme requirement when the protocol is inherited from
// -otlp-protocol rather than declared on the override — and before this
// existed -check-config exited 0 for them while the same ConfigMap
// CrashLooped the fleet at `creating OTLP exporter`, from the check whose whole
// purpose is preventing that. A refusal names the signal (`export.logs: ...`)
// and walks the signals in a fixed order, so a section with two mistakes names
// the same one on every run.
//
// Still shape-only in the sense that matters: Config.Validate touches neither
// the filesystem nor the network.
func (c *ExportConfig) ValidateAgainst(base Config) error {
	if err := c.Validate(); err != nil {
		return err
	}
	merged := c.ApplyBase(base)
	if c != nil {
		for _, sig := range c.Overrides() {
			if sig.Override == nil {
				continue
			}
			if err := c.signalConfig(sig.Override, base).Validate(); err != nil {
				return fmt.Errorf("export.%s: %w", sig.Name, err)
			}
			// Said out loud on the one seam BOTH -check-config and every real
			// start cross (validateConfig calls this): an own-endpoint signal
			// silently losing the flag base's collector credentials would
			// otherwise surface only as a transient export failure against a
			// backend the operator believes is authenticated. Warn, not refuse
			// — dropping the credential IS the correct destination, and the
			// override has a field for every one of them.
			if dropped := DroppedBaseCredentials(sig.Override, base); len(dropped) > 0 {
				slog.Default().Warn("this export destination names its own endpoint, so the flag base's collector credentials are NOT presented to it",
					"signal", sig.Name, "endpoint", sig.Override.Endpoint, "flag", strings.Join(dropped, ","),
					"note", "they authenticate this deployment to ITS collector and this is a different host; set bearerTokenFile / caFile / insecureSkipVerify on the export."+sig.Name+" override itself if this destination needs them")
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
	return c.applyBase(base, true)
}

// applyBase is ApplyBase with the section's client pair optional: identity
// false applies the headers alone (a route's own-endpoint rebuild, see
// destinationConfig).
func (c *ExportConfig) applyBase(base Config, identity bool) Config {
	if c == nil {
		return base
	}
	base.Headers = MergeHeaders(base.Headers, c.Headers)
	if identity && c.ClientCertFile != "" {
		base.ClientCertFile = c.ClientCertFile
		base.ClientKeyFile = c.ClientKeyFile
	}
	return base
}

// MergeHeaders overlays over onto base, per key, into a FRESH map — the base is
// shared by every destination derived from it and must not be written through.
// With nothing to overlay the base is returned as is (nil stays nil). One
// function for the three places a header layer is applied: the section's base
// additions (ApplyBase) and the override a destination declares — a
// per-signal one or a routing route's (destinationConfig) — which each spelled
// the same seven lines.
//
// Keys match CASE-INSENSITIVELY, because both transports fold them (gRPC
// lowercases metadata keys, HTTP canonicalises header names): an override
// spelled `x-scope-orgid` REPLACES a base `X-Scope-OrgID`, under the
// override's spelling. Matching the exact string kept both, and the pair was
// one header with two values on the wire — gRPC sent both, HTTP sent
// whichever its map iteration visited last, so a per-signal or per-route
// tenant changed at random between exports.
//
// The fold removes only BASE keys an override replaces. Two keys differing
// only in case WITHIN one layer are kept as they are, so Config.Validate can
// refuse the ambiguity by name instead of this function silently picking one.
func MergeHeaders(base, over map[string]string) map[string]string {
	if len(over) == 0 {
		return base
	}
	merged := make(map[string]string, len(base)+len(over))
	for k, v := range base {
		if !overridden(k, over) {
			merged[k] = v
		}
	}
	maps.Copy(merged, over)
	return merged
}

// overridden reports whether over holds a key naming the same header as k,
// whatever its case. Header maps are a handful of entries, so a scan is
// cheaper than building a folded index, and this runs once per derived config.
func overridden(k string, over map[string]string) bool {
	for o := range over {
		if strings.EqualFold(k, o) {
			return true
		}
	}
	return false
}

// SignalOverride pairs a per-signal override with the name the section spells
// it by ("logs", "metrics", "traces"), so a walk over the three is in a FIXED
// order.
type SignalOverride struct {
	Name string
	// Override is nil when the section leaves the signal to the flag base.
	Override *ExportOverride
}

// Overrides lists the section's per-signal overrides in signal order (logs,
// metrics, traces), nil entries included; nil for no section. Every walk over
// "the three signals", in this package and outside it (the agent's startup
// summary), goes through this so none of them can iterate a map or enumerate
// the signals a second time.
func (c *ExportConfig) Overrides() []SignalOverride {
	if c == nil {
		return nil
	}
	return []SignalOverride{{"logs", c.Logs}, {"metrics", c.Metrics}, {"traces", c.Traces}}
}

// BaseEndpointUnused reports whether NOTHING dials the flag base endpoint:
// every signal is overridden AND every override names its OWN endpoint
// (OwnEndpoint). False for no section.
//
// Struct presence is not the test. An override that sets only headers (or a
// bearer file, or TLS) inherits the base ENDPOINT through destinationConfig,
// so the base is still the address that signal reaches; the same holds for an
// override that REPEATS the base endpoint. A caller refusing to inherit an
// unused base (an endpoint-less routing route) would otherwise fail a config
// the exporter builds happily.
func (c *ExportConfig) BaseEndpointUnused(flagBase Config) bool {
	if c == nil {
		return false
	}
	for _, s := range c.Overrides() {
		if s.Override == nil || !OwnEndpoint(s.Override.Endpoint, flagBase) {
			return false
		}
	}
	return true
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
