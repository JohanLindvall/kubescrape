// Package route fans exported payloads out to multiple destinations/tenants
// by Kubernetes namespace: each route matches `k8s.namespace.name` globs and
// forwards to its own OTLP client (different endpoint and/or extra headers,
// e.g. X-Scope-OrgID); unmatched resources go to the default exporter.
//
// It sits between the transforms and the default delivery chain
// (producers → transform → router → {default buffered chain | route
// clients}), splitting each payload per destination. First-matching route
// wins. A payload where everything matches the default forwards untouched
// (no copy); a split COPIES resources into per-destination payloads and
// never mutates the caller's — the ingest batcher retries the same object in
// place and the spanmetrics tap Consumes it after the forward, so an
// in-place split would lose a retried batch and blind the tap. Delivery is
// at-least-once per destination: a failed destination fails the whole
// export, and the producer's retry re-splits the (untouched) payload
// deterministically — destinations that already succeeded receive
// duplicates, which OTLP consumers must tolerate anyway.
//
// The DEFAULT destination keeps whatever durability the chain has (disk
// buffer); per-route destinations are direct clients — a route outage
// surfaces to the producer as back-pressure/retry, not spooling. Routes are
// for tenancy/fan-out, not for doubling the durability machinery.
package route

import (
	"context"
	"errors"
	"log/slog"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// joinExportErrs combines the per-destination results of one split export.
//
// Permanence is carried through only when EVERY failed destination rejected
// permanently. otlpexport.IsPermanent classifies through errors.As/
// status.FromError, and both traverse a joined error's LEAVES — so a single
// route answering 400 made the whole payload look permanently rejected, and
// the tailer (which drops such a batch and advances, by design) discarded the
// default-destined records a retry would have delivered. A mixed failure is
// returned opaquely instead: same message, no Unwrap, hence transient.
func joinExportErrs(errs []error) error {
	var failed []error
	for _, err := range errs {
		if err != nil {
			failed = append(failed, err)
		}
	}
	switch len(failed) {
	case 0:
		return nil
	case 1:
		return failed[0] // single destination: classify it exactly as it is
	}
	for _, err := range failed {
		if !otlpexport.IsPermanent(err) {
			return &partialFailure{errs: failed}
		}
	}
	return &allPermanent{errs: failed}
}

// allPermanent is the every-destination-rejected error. It FLATTENS its message
// like partialFailure — this string is handed to http.Error and status.Error,
// where a newline is a malformed body/status — but, unlike partialFailure, it
// KEEPS Unwrap: every leaf is permanent, so IsPermanent must still read that
// verdict through them (errors.Join gave the traversal but a multi-line Error).
type allPermanent struct{ errs []error }

func (e *allPermanent) Error() string   { return flattenErrs("all", e.errs) }
func (e *allPermanent) Unwrap() []error { return e.errs }

// partialFailure reports a mixed multi-destination failure without exposing its
// leaves to a CLASSIFIER, so no one destination's permanent rejection is read as
// a verdict on the payload.
//
// Opacity here is the ABSENCE of Unwrap, not the absence of the errors:
// errors.Is/As traverse only what Unwrap gives them, so keeping the leaves in an
// unexported field costs nothing and leaves them available for logging and for a
// future accessor. Destroying them to a string (which this used to do) bought
// exactly the same opacity and threw the structure away with it.
//
// Error() FLATTENS: errors.Join separates with newlines, and this string is
// handed to http.Error and status.Error, where a multi-line body is a
// malformed header value or an unreadable gRPC status message.
type partialFailure struct{ errs []error }

func (e *partialFailure) Error() string {
	return flattenErrs(strconv.Itoa(len(e.errs))+" of the payload's", e.errs)
}

// flattenErrs renders a multi-destination failure on ONE line: the string is
// written into http.Error / status.Error, where a newline is a malformed body
// or an unreadable gRPC status. count names the scope ("all", "N of the
// payload's"). Shared by partialFailure and allPermanent.
func flattenErrs(count string, errs []error) string {
	var sb strings.Builder
	sb.WriteString("export failed for ")
	sb.WriteString(count)
	sb.WriteString(" destinations: ")
	for i, err := range errs {
		if i > 0 {
			sb.WriteString("; ")
		}
		sb.WriteString(strings.ReplaceAll(err.Error(), "\n", " "))
	}
	return sb.String()
}

// Errors returns the per-destination failures. It is deliberately NOT Unwrap:
// callers that want to log or inspect the leaves may, while errors.Is/As still
// cannot reach past this error and mistake one destination's permanent
// rejection for the payload's verdict.
func (e *partialFailure) Errors() []error { return slices.Clone(e.errs) }

// Config is the agent config's routing section.
type Config struct {
	Routes []Route `json:"routes"`
}

// Route is one destination.
type Route struct {
	// Name labels the route in metrics/logs.
	Name string `json:"name"`
	// Namespaces are glob patterns matched against k8s.namespace.name
	// (path.Match syntax: "team-a-*", "prod").
	Namespaces []string `json:"namespaces"`
	// Endpoint overrides the OTLP destination (empty = the default endpoint,
	// useful for header-only tenant routing).
	Endpoint string `json:"endpoint,omitempty"`
	// Headers are extra headers for this route (e.g. X-Scope-OrgID).
	Headers map[string]string `json:"headers,omitempty"`

	// Credentials for a route that names its OWN endpoint. They are not
	// optional decoration: a route with its own endpoint does NOT inherit the
	// default chain's BearerTokenFile / client certificate, because those
	// authenticate this deployment to ITS collector and a route is by
	// definition a different destination — frequently a different tenant's,
	// sometimes a different organization's. Inheriting them presented the
	// default collector's credentials to whatever host the route named, which
	// is a credential disclosure to a third party rather than a convenience.
	//
	// A route with no endpoint still inherits everything: it IS the default
	// destination, reached with extra headers (the header-only tenancy case).
	BearerTokenFile string `json:"bearerTokenFile,omitempty"`
	ClientCertFile  string `json:"clientCertFile,omitempty"`
	ClientKeyFile   string `json:"clientKeyFile,omitempty"`
	CAFile          string `json:"caFile,omitempty"`
	// Insecure allows plaintext gRPC to this route (for HTTP the scheme in
	// Endpoint decides). Unset (nil) INHERITS the merged flag base's
	// -otlp-insecure — the ExportOverride pattern, and what every route
	// written before this field existed relied on: a plain bool's zero value
	// flipped those routes to TLS on upgrade, turning a working plaintext
	// destination into an endless transient export failure with no startup
	// signal. Plaintext-ness is transport to the named host, not a credential,
	// so inheriting it is not the disclosure the fields above guard against.
	Insecure *bool `json:"insecure,omitempty"`
}

// Exporter is a full destination (logs+metrics; traces optional via
// TracesExporter).
type Exporter interface {
	ExportLogs(ctx context.Context, ld plog.Logs) error
	ExportMetrics(ctx context.Context, md pmetric.Metrics) error
}

// TracesExporter ships traces.
type TracesExporter interface {
	ExportTraces(ctx context.Context, td ptrace.Traces) error
}

// Destination pairs a compiled route with its exporter.
type Destination struct {
	Name       string
	Namespaces []string
	Exporter   Exporter
}

// ScriptMarker is the reserved resource attribute a transform script's
// route("name") verb stamps: the router honors it BEFORE the namespace
// globs and strips it from the outgoing copy, so kubescrape plumbing never
// reaches a collector. It exists as a sanctioned verb because scripts could
// already steer routing by rewriting k8s.namespace.name — an accidental
// contract worth replacing with an explicit one. A name matching no
// configured route falls to the default chain, warned (throttled) rather
// than dropped: a typo must degrade to the safe destination, not to loss.
const ScriptMarker = "kubescrape.route"

// Router splits payloads across destinations.
type Router struct {
	def   Exporter
	dests []Destination
	// unknownRouteGate throttles the typo'd-ScriptMarker warning: the script
	// stamps every record of a busy stream, and the condition is a state.
	unknownRouteGate logdedupe.Throttle
	// log is the router's logger; nil means the process default (logger() in
	// debug.go says why nothing wires one through New). It carries the Debug
	// narration of the routing DECISION — the one thing this package could not
	// answer during an incident.
	log *slog.Logger
	// health[i] narrates dests[i]'s outcomes — one line when a destination
	// starts refusing, one when it starts accepting again, with the class and a
	// remediation hint (otlpexport.FailureReporter, the same report the default
	// chain's client makes about the collector). A route destination is
	// UNBUFFERED by design, so its failure is felt by the producer immediately
	// and is worth saying out loud; it is also the destination an operator is
	// least likely to be watching, since a tenant route is usually somebody
	// else's collector.
	health []*otlpexport.FailureReporter
}

// New builds a Router forwarding unmatched resources to def.
func New(def Exporter, dests []Destination) *Router {
	r := &Router{def: def, dests: dests}
	r.health = make([]*otlpexport.FailureReporter, len(dests))
	for i, d := range dests {
		// The route NAME, not an endpoint: a Destination holds a built
		// Exporter and never its address. The startup summary's per-route
		// lines are what map a name to an endpoint, once.
		r.health[i] = otlpexport.NewFailureReporter(nil, "a routing destination", "route", d.Name)
	}
	return r
}

// noteDest counts and narrates one destination's result for one signal. Only a
// DELIVERED part is counted as routed (the producer retries the whole payload,
// so counting before the send would tally attempts); a refusal moves the
// failure counter, which is the half that used to be missing entirely — a route
// that never worked was indistinguishable from a route nothing matched.
func (r *Router) noteDest(i int, signal string, err error) error {
	if err == nil {
		obs.Routed.WithLabelValues(r.dests[i].Name, signal).Inc()
	} else {
		obs.RouteFailures.WithLabelValues(r.dests[i].Name, signal).Inc()
	}
	if i < len(r.health) && r.health[i] != nil {
		r.health[i].Note(signal, err)
	}
	return err
}

// match returns the destination index for a resource (-1 = default) and
// whether a ScriptMarker was present. marked forces the SPLIT path even for
// the default destination: the marker must be stripped before anything is
// sent, and stripping may only happen on the split's copy — the caller's
// payload is retried as-is by its producer.
//
// note gates the unknown-route REPORT, and it exists because split matches one
// resource twice: its scan stops at the first match and the grouping pass then
// re-matches from there, so the resource at that boundary passes through here
// twice for one payload. The throttled warning never showed it; a counter does
// (kubescrape_routed_unknown_total read 2 for one mis-routed payload). Only the
// grouping pass reports.
func (r *Router) match(res pcommon.Resource, note bool) (idx int, marked bool) {
	attrs := res.Attributes()
	if v, ok := attrs.Get(ScriptMarker); ok {
		want := v.Str()
		for i, d := range r.dests {
			if d.Name == want {
				return i, true
			}
		}
		if !note {
			return -1, true
		}
		obs.RouteUnknown.Inc()
		if r.unknownRouteGate.Allow(time.Minute) {
			r.logger().Warn("a transform script routed a payload to a name no route defines; it goes to the default "+
				"chain instead, so the records are delivered but not to the tenant the script asked for",
				"route", want)
		}
		return -1, true
	}
	ns, ok := attrs.Get(namespaceAttr)
	if !ok {
		return -1, false
	}
	name := ns.Str()
	for i, d := range r.dests {
		for _, pat := range d.Namespaces {
			if ok, _ := path.Match(pat, name); ok {
				return i, false
			}
		}
	}
	return -1, false
}

// ExportLogs splits by resource namespace and forwards each group.
func (r *Router) ExportLogs(ctx context.Context, ld plog.Logs) error {
	groups := r.split("logs", ld.ResourceLogs().Len(), func(i int) pcommon.Resource {
		return ld.ResourceLogs().At(i).Resource()
	})
	if groups == nil {
		return r.def.ExportLogs(ctx, ld) // fast path: everything default
	}
	parts := make([]plog.Logs, len(r.dests)+1)
	for i := range parts {
		parts[i] = plog.NewLogs()
	}
	// COPY into the destination parts; never mutate the caller's payload. A
	// producer's retry re-runs the whole export on the SAME object (the ingest
	// batcher retries in place, the spanmetrics tap Consumes it after
	// forwarding), so emptying the input would lose the retried batch and feed
	// the tap zero spans. The split path pays a copy; the all-default fast
	// path above forwards uncopied.
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		g := groups[i]
		dst := parts[g+1].ResourceLogs().AppendEmpty()
		rls.At(i).CopyTo(dst)
		stripMarker(dst.Resource())
	}
	var errs []error
	if parts[0].ResourceLogs().Len() > 0 {
		errs = append(errs, r.def.ExportLogs(ctx, parts[0]))
	}
	for i, d := range r.dests {
		if p := parts[i+1]; p.ResourceLogs().Len() > 0 {
			// noteDest counts the outcome (delivered vs refused) and narrates
			// a change in this destination's health.
			errs = append(errs, r.noteDest(i, "logs", d.Exporter.ExportLogs(ctx, p)))
		}
	}
	return joinExportErrs(errs)
}

// ExportMetrics splits by resource namespace and forwards each group.
func (r *Router) ExportMetrics(ctx context.Context, md pmetric.Metrics) error {
	groups := r.split("metrics", md.ResourceMetrics().Len(), func(i int) pcommon.Resource {
		return md.ResourceMetrics().At(i).Resource()
	})
	if groups == nil {
		return r.def.ExportMetrics(ctx, md)
	}
	parts := make([]pmetric.Metrics, len(r.dests)+1)
	for i := range parts {
		parts[i] = pmetric.NewMetrics()
	}
	// COPY, never move — see ExportLogs.
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		g := groups[i]
		dst := parts[g+1].ResourceMetrics().AppendEmpty()
		rms.At(i).CopyTo(dst)
		stripMarker(dst.Resource())
	}
	var errs []error
	if parts[0].ResourceMetrics().Len() > 0 {
		errs = append(errs, r.def.ExportMetrics(ctx, parts[0]))
	}
	for i, d := range r.dests {
		if p := parts[i+1]; p.ResourceMetrics().Len() > 0 {
			errs = append(errs, r.noteDest(i, "metrics", d.Exporter.ExportMetrics(ctx, p))) // see ExportLogs
		}
	}
	return joinExportErrs(errs)
}

// ExportTraces splits by resource namespace and forwards each group. Route
// destinations always support traces (they are otlpexport clients); the
// default may not — its group then errors only if non-empty.
func (r *Router) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	groups := r.split("traces", td.ResourceSpans().Len(), func(i int) pcommon.Resource {
		return td.ResourceSpans().At(i).Resource()
	})
	defTraces, defOK := r.def.(TracesExporter)
	if groups == nil {
		if !defOK {
			return errors.New("default exporter does not support traces")
		}
		return defTraces.ExportTraces(ctx, td)
	}
	parts := make([]ptrace.Traces, len(r.dests)+1)
	for i := range parts {
		parts[i] = ptrace.NewTraces()
	}
	// COPY, never move — see ExportLogs (the spanmetrics tap Consumes this
	// same payload after the forward and must still see every span).
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		g := groups[i]
		dst := parts[g+1].ResourceSpans().AppendEmpty()
		rss.At(i).CopyTo(dst)
		stripMarker(dst.Resource())
	}
	var errs []error
	if parts[0].ResourceSpans().Len() > 0 {
		if !defOK {
			errs = append(errs, errors.New("default exporter does not support traces"))
		} else {
			errs = append(errs, defTraces.ExportTraces(ctx, parts[0]))
		}
	}
	for i, d := range r.dests {
		if p := parts[i+1]; p.ResourceSpans().Len() > 0 {
			te, ok := d.Exporter.(TracesExporter)
			if !ok {
				// A wiring fault rather than a destination failure, and it
				// repeats on every export: count and narrate it like one, so
				// "this route ships no traces" is visible in the same two
				// places as every other route failure.
				errs = append(errs, r.noteDest(i, "traces", errors.New("route "+d.Name+" does not support traces")))
				continue
			}
			errs = append(errs, r.noteDest(i, "traces", te.ExportTraces(ctx, p))) // see ExportLogs
		}
	}
	return joinExportErrs(errs)
}

// split computes per-resource destinations; nil means "all default" (the
// caller then forwards the original payload untouched).
//
// The all-default answer must cost NOTHING to reach: it is the documented fast
// path, and it is what the self-metrics, node and cadvisor-rollup shapes (no
// k8s.namespace.name at all) take on every export. Building the group slice
// first and discarding it on !any allocated proportionally to the resource
// count — 128 KB per export at the 4,000-resource KSM/cadvisor shape, already
// half the cost of the copy the fast path exists to avoid. So: scan for the
// FIRST match with no allocation, and only then take a second pass into a
// slice sized exactly once. Resources before that match are all default, so
// re-matching them would be pure waste.
func (r *Router) split(signal string, n int, res func(int) pcommon.Resource) []int {
	// ONE Enabled call per export decides whether the narration below is built;
	// slog evaluates arguments eagerly, so an unguarded explainExport would walk
	// every resource of every payload at Info. The fast path's promise above is
	// kept: with Debug off this costs one interface call and nothing else.
	dbg := r.debugEnabled()
	first := -1
	for i := 0; i < n; i++ {
		if idx, marked := r.match(res(i), false); idx >= 0 || marked {
			first = i
			break
		}
	}
	if first < 0 {
		if dbg {
			r.explainExport(signal, n, res, nil)
		}
		return nil
	}
	groups := make([]int, n)
	for i := 0; i < first; i++ {
		groups[i] = -1
	}
	for i := first; i < n; i++ {
		groups[i], _ = r.match(res(i), true)
	}
	if dbg {
		r.explainExport(signal, n, res, groups)
	}
	return groups
}

// stripMarker removes the script-routing marker from a COPIED resource; it
// must never reach a destination.
func stripMarker(res pcommon.Resource) {
	res.Attributes().Remove(ScriptMarker)
}
