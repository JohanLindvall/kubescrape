// Package route fans exported payloads out to multiple destinations/tenants
// by Kubernetes namespace: each route matches `k8s.namespace.name` globs and
// forwards to its own OTLP client (different endpoint and/or extra headers,
// e.g. X-Scope-OrgID); unmatched resources go to the default exporter.
//
// It sits between the transforms and the default delivery chain
// (producers → transform → router → {default buffered chain | route
// clients}), splitting each payload per destination. First-matching route
// wins. A payload whose every resource goes to ONE destination, and carries
// no script marker, forwards untouched (no copy) — to the default chain, or
// to that route; a split COPIES resources into per-destination payloads and
// never mutates the caller's. Producers re-send the SAME object: the tailer's
// exportWithRetry reaches the router below the transform layer
// (transform.Wrapper.Inner) and retries its batch in place, the tail
// sampler's drain re-offers a decided payload, and the spanmetrics tap
// Consumes the forwarded payload after the export — so an in-place split
// would lose a retried batch and blind the tap. The uncopied forward relies
// on the same contract the default fast path always has: a destination only
// READS what it is handed. Delivery is at-least-once per destination: a
// failed destination fails the whole export, and the producer's retry
// re-splits the (untouched) payload deterministically — destinations that
// already succeeded receive duplicates, which OTLP consumers must tolerate
// anyway.
//
// With -buffer-dir the default destination "succeeds" by being ENQUEUED, which
// it does while the collector is down, so during a route outage every retry of
// a producer that re-offers its batch until it lands would spool ANOTHER copy
// of the default share. Withholding the default share while a route fails is
// safe only for such a producer — a single-shot one (promscrape) never re-sends
// and would lose the default tenant's data to spare a duplicate — so it is
// per-payload and opt-in, riding the context like otlpexport.Own: a payload
// marked Reoffer(ctx) has its route shares sent FIRST, and a transient route
// failure withholds its default share (reoffer.go, which also rosters who
// marks). An unmarked payload keeps sending every share every attempt.
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
	"github.com/JohanLindvall/kubescrape/internal/metrics"
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
// unexported field costs nothing and leaves them available for logging and for
// Errors(). Destroying them to a string (which this used to do) bought
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

	// Credentials for this route's destination. They are not optional
	// decoration for a route that names its OWN endpoint: such a route does
	// NOT inherit the default chain's BearerTokenFile / CA / client
	// certificate, because those authenticate this deployment to ITS
	// collector and the route is a different destination — frequently a
	// different tenant's, sometimes a different organization's. Inheriting them
	// presented the default collector's credentials to whatever host the route
	// named, which is a credential disclosure to a third party rather than a
	// convenience.
	//
	// A route with no endpoint, or one repeating -otlp-endpoint, IS the
	// default destination (reached with extra headers — the header-only
	// tenancy case): it inherits everything, and any of these fields it sets
	// wins over the inherited value, exactly as an export.<signal> override
	// without an endpoint of its own. One derivation serves both
	// (otlpexport.ExportConfig.RouteConfig).
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
	// InsecureSkipVerify skips verifying the route destination's TLS
	// certificate. Unlike Insecure it is NOT inherited when the route names its
	// own endpoint: -otlp-tls-insecure-skip-verify is a trust decision about
	// the deployment's OWN collector, and carrying it over sent this route's
	// bearer token and client certificate to a peer whose certificate nobody
	// checked — the carryover the export section's per-signal overrides drop
	// for the same reason. Unset = verify on an own-endpoint route, and the
	// inherited flag on a route that IS the default destination (no endpoint,
	// or -otlp-endpoint repeated), where a value set here wins like the
	// credentials above.
	InsecureSkipVerify *bool `json:"insecureSkipVerify,omitempty"`
}

// ExportOverride is the route's destination fields in the shape otlpexport's
// one destination derivation reads (ExportConfig.RouteConfig) — the same
// shape an export.<signal> override has, so a route and a per-signal override
// cannot derive a destination two different ways. A route has no protocol or
// compression of its own; those stay the base's.
func (rt Route) ExportOverride() otlpexport.ExportOverride {
	return otlpexport.ExportOverride{
		Endpoint:           rt.Endpoint,
		Headers:            rt.Headers,
		BearerTokenFile:    rt.BearerTokenFile,
		CAFile:             rt.CAFile,
		Insecure:           rt.Insecure,
		InsecureSkipVerify: rt.InsecureSkipVerify,
		ClientCertFile:     rt.ClientCertFile,
		ClientKeyFile:      rt.ClientKeyFile,
	}
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
	// else's collector. nil for a destination that narrates itself
	// (otlpexport.HealthReporter) — see New.
	health []*otlpexport.FailureReporter
	// nsMemo remembers namespace verdicts (nsmemo.go): dests never changes
	// after New, so a verdict never goes stale.
	nsMemo nsMemo
	// counters[i][signal] are dests[i]'s outcome counters, bound once in New.
	// WithLabelValues on a two-label vec builds its cache key and takes the
	// vec's mutex on EVERY call, which made it the router's only per-export
	// allocation on the uncopied forward — and a lock every concurrent export
	// to any route shared.
	counters [][numSignals]routeCounters
}

// routeCounters are one destination's two outcomes for one signal.
type routeCounters struct{ routed, failed *metrics.RegCounter }

// The signals a destination's counters are bound for, in signalIndex order.
const numSignals = 3

var signalNames = [numSignals]string{"logs", "metrics", "traces"}

// signalIndex maps an export's signal name to its counters.
func signalIndex(signal string) int {
	switch signal {
	case "logs":
		return 0
	case "metrics":
		return 1
	default:
		return 2
	}
}

// New builds a Router forwarding unmatched resources to def.
func New(def Exporter, dests []Destination) *Router {
	r := &Router{def: def, dests: dests}
	r.health = make([]*otlpexport.FailureReporter, len(dests))
	r.counters = make([][numSignals]routeCounters, len(dests))
	for i, d := range dests {
		// Bound up front, so a configured route publishes 0 for each signal
		// rather than being absent until its first part.
		for s, sig := range signalNames {
			r.counters[i][s] = routeCounters{
				routed: obs.Routed.WithLabelValues(d.Name, sig),
				failed: obs.RouteFailures.WithLabelValues(d.Name, sig),
			}
		}
		// A destination that narrates ITSELF gets no second narrator. The
		// production route destination is an *otlpexport.Client, which
		// reports every wire send it makes; the agent builds it with
		// otlpexport.WithReport so its lines name the route AND the endpoint.
		// Narrating it here as well said every transition twice — two warns,
		// two re-warns, two recoveries per outage, one of them under the
		// client's default "the OTLP collector".
		if hr, ok := d.Exporter.(otlpexport.HealthReporter); ok && hr.ReportsHealth() {
			continue
		}
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
	c := r.counters[i][signalIndex(signal)]
	if err == nil {
		c.routed.Inc()
	} else {
		c.failed.Inc()
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
// The decision itself is decide's; match adds only the unknown-route REPORT,
// which note gates. It exists because split matches one resource twice: its
// scan stops where the run of one unmarked destination breaks and the grouping
// pass then re-matches from there, so the resource at that boundary passes
// through here twice for one payload. The throttled warning never showed it; a
// counter does (kubescrape_routed_unknown_total read 2 for one mis-routed
// payload). Only the grouping pass reports.
func (r *Router) match(res pcommon.Resource, note bool) (idx int, marked bool) {
	v := r.decide(res.Attributes(), false)
	if note && v.reason == reasonMarkerNamesNoRoute {
		obs.RouteUnknown.Inc()
		if r.unknownRouteGate.Allow(time.Minute) {
			r.logger().Warn("a transform script routed a payload to a name no route defines; it goes to the default "+
				"chain instead, so the records are delivered but not to the tenant the script asked for",
				"route", v.want)
		}
	}
	return v.idx, v.marked
}

// The branches a routing decision can take, as the Debug line names them
// (reasonOrder renders them in its own fixed order).
const (
	reasonScriptMarker       = "scriptMarker"
	reasonMarkerNamesNoRoute = "scriptMarkerNamesNoRoute"
	reasonNoNamespace        = "noNamespaceAttribute"
	reasonNamespaceGlob      = "namespaceGlob"
	reasonNoGlobMatched      = "noGlobMatched"
)

// verdict is one resource's routing decision: where it goes (idx, -1 = the
// default chain), whether a ScriptMarker forced the split path (marked), and
// the branch that decided it (reason) with what that branch had to show for
// itself. Every string is a view of a payload attribute or a constant, so
// building one allocates nothing.
type verdict struct {
	idx    int
	marked bool
	reason string
	want   string // the ScriptMarker's value, when marked
	ns     string // the namespace attribute's value, when the decision read it
	pat    string // the glob that matched; filled only when narrating
}

// decide is THE routing decision — match (the split) and explainExport (the
// Debug narration) both call it, so the line's reasons and examples cannot
// describe a different router from the one that split the payload.
//
// narrate asks for what only the narration needs, and changes no idx: the
// glob that matched (the namespace memo remembers destinations, not patterns,
// so a narrated decision reads the globs directly — which also means a
// narration never teaches the memo anything), and the namespace of a resource
// a destination-less Router has no reason to read.
func (r *Router) decide(attrs pcommon.Map, narrate bool) verdict {
	if v, ok := attrs.Get(ScriptMarker); ok {
		want := v.Str()
		for i, d := range r.dests {
			if d.Name == want {
				return verdict{idx: i, marked: true, reason: reasonScriptMarker, want: want}
			}
		}
		return verdict{idx: -1, marked: true, reason: reasonMarkerNamesNoRoute, want: want}
	}
	// No route to match is no reason to look: the destination-less Router
	// (the self chain's preRoute, and the producers' Router with no routing:
	// section) exists only to strip the marker above, and without this every
	// resource of every export paid a second attribute scan for a namespace
	// nothing could use. (No glob matched: there are none.)
	if len(r.dests) == 0 && !narrate {
		return verdict{idx: -1, reason: reasonNoGlobMatched}
	}
	ns, ok := attrs.Get(namespaceAttr)
	if !ok {
		// The commonest surprise, and the one a counter cannot distinguish: the
		// self-metrics, node and cadvisor-rollup resources carry no namespace at
		// all, so they can only ever be default — routing them needs a
		// script marker, not another glob.
		return verdict{idx: -1, reason: reasonNoNamespace}
	}
	out := verdict{ns: ns.Str()}
	if narrate {
		out.idx, out.pat = r.globNamespace(out.ns)
	} else {
		out.idx = r.namespaceDest(out.ns)
	}
	out.reason = reasonNoGlobMatched
	if out.idx >= 0 {
		out.reason = reasonNamespaceGlob
	}
	return out
}

// ExportLogs splits by resource namespace and forwards each group.
func (r *Router) ExportLogs(ctx context.Context, ld plog.Logs) error {
	groups, whole := r.split("logs", ld.ResourceLogs().Len(), func(i int) pcommon.Resource {
		return ld.ResourceLogs().At(i).Resource()
	})
	if groups == nil { // fast path: one destination, forwarded uncopied
		if whole >= 0 {
			return r.noteDest(whole, "logs", r.dests[whole].Exporter.ExportLogs(ctx, ld))
		}
		return r.def.ExportLogs(ctx, ld)
	}
	parts := make([]plog.Logs, len(r.dests)+1)
	for i := range parts {
		parts[i] = plog.NewLogs()
	}
	// COPY into the destination parts; never mutate the caller's payload. A
	// producer's retry re-runs the whole export on the SAME object (the
	// tailer retries its batch in place, the tail sampler's drain re-offers
	// its payload, the spanmetrics tap Consumes it after forwarding), so
	// emptying the input would lose the retried batch and feed the tap zero
	// spans. The split path pays a copy; the one-destination fast path above
	// forwards uncopied.
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		g := groups[i]
		dst := parts[g+1].ResourceLogs().AppendEmpty()
		rls.At(i).CopyTo(dst)
		stripMarker(dst.Resource())
	}
	var def func() error
	if parts[0].ResourceLogs().Len() > 0 {
		def = func() error { return r.def.ExportLogs(ctx, parts[0]) }
	}
	return r.sendSplit(ctx, "logs", def,
		func(i int) bool { return parts[i+1].ResourceLogs().Len() > 0 },
		func(i int) error { return r.dests[i].Exporter.ExportLogs(ctx, parts[i+1]) })
}

// ExportMetrics splits by resource namespace and forwards each group.
func (r *Router) ExportMetrics(ctx context.Context, md pmetric.Metrics) error {
	groups, whole := r.split("metrics", md.ResourceMetrics().Len(), func(i int) pcommon.Resource {
		return md.ResourceMetrics().At(i).Resource()
	})
	if groups == nil { // see ExportLogs
		if whole >= 0 {
			return r.noteDest(whole, "metrics", r.dests[whole].Exporter.ExportMetrics(ctx, md))
		}
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
	var def func() error
	if parts[0].ResourceMetrics().Len() > 0 {
		def = func() error { return r.def.ExportMetrics(ctx, parts[0]) }
	}
	return r.sendSplit(ctx, "metrics", def,
		func(i int) bool { return parts[i+1].ResourceMetrics().Len() > 0 },
		func(i int) error { return r.dests[i].Exporter.ExportMetrics(ctx, parts[i+1]) })
}

// ExportTraces splits by resource namespace and forwards each group. Route
// destinations always support traces (they are otlpexport clients); the
// default may not — its group then errors only if non-empty.
func (r *Router) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	groups, whole := r.split("traces", td.ResourceSpans().Len(), func(i int) pcommon.Resource {
		return td.ResourceSpans().At(i).Resource()
	})
	defTraces, defOK := r.def.(TracesExporter)
	if groups == nil { // see ExportLogs
		if whole >= 0 {
			return r.noteDest(whole, "traces", r.exportRouteTraces(ctx, whole, td))
		}
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
	var def func() error
	if parts[0].ResourceSpans().Len() > 0 {
		def = func() error {
			if !defOK {
				return errors.New("default exporter does not support traces")
			}
			return defTraces.ExportTraces(ctx, parts[0])
		}
	}
	return r.sendSplit(ctx, "traces", def,
		func(i int) bool { return parts[i+1].ResourceSpans().Len() > 0 },
		func(i int) error { return r.exportRouteTraces(ctx, i, parts[i+1]) })
}

// sendSplit sends one split payload's non-empty shares and joins their
// results: def sends the default share (nil when it is empty), hasRoute and
// sendRoute route i's. Every route result goes through noteDest, which counts
// the outcome (delivered vs refused) and narrates a change in that
// destination's health.
//
// Unmarked, the default share goes first and every share goes on every
// attempt. For a Reoffered payload the route shares go FIRST, and a TRANSIENT
// failure among them withholds the default share: its producer will offer these
// records again, so sending it now would only spool a second copy on the retry
// (reoffer.go). The error returned then is the routes' — transient by
// construction, so the producer retries rather than dropping the batch whose
// default share has not gone anywhere yet.
func (r *Router) sendSplit(ctx context.Context, signal string, def func() error,
	hasRoute func(int) bool, sendRoute func(int) error) error {
	reoffer := def != nil && Reoffered(ctx)
	var errs []error
	if def != nil && !reoffer {
		errs = append(errs, def())
	}
	for i := range r.dests {
		if hasRoute(i) {
			errs = append(errs, r.noteDest(i, signal, sendRoute(i)))
		}
	}
	if reoffer {
		if anyTransient(errs) {
			if r.debugEnabled() {
				r.logger().Debug("a route share failed transiently; holding this payload's default share for the producer's retry",
					"signal", signal)
			}
			return joinExportErrs(errs)
		}
		errs = append(errs, def())
	}
	return joinExportErrs(errs)
}

// anyTransient reports whether any of errs is a failure a retry could clear.
func anyTransient(errs []error) bool {
	for _, err := range errs {
		if err != nil && !otlpexport.IsPermanent(err) {
			return true
		}
	}
	return false
}

// exportRouteTraces sends td to route i. A destination without the trace
// capability is a wiring fault rather than a destination failure, and it
// repeats on every export: the caller counts and narrates it through noteDest
// like one, so "this route ships no traces" is visible in the same two places
// as every other route failure — on the split path and the uncopied one alike.
func (r *Router) exportRouteTraces(ctx context.Context, i int, td ptrace.Traces) error {
	d := r.dests[i]
	te, ok := d.Exporter.(TracesExporter)
	if !ok {
		return errors.New("route " + d.Name + " does not support traces")
	}
	return te.ExportTraces(ctx, td)
}

// split computes the payload's routing. groups == nil means NO split: every
// resource goes to one destination — whole, -1 being the default chain — and
// none carries a script marker, so the caller forwards its payload untouched.
// Otherwise groups[i] is resource i's destination and the caller copies each
// resource into its destination's part (whole is then -1 and meaningless).
//
// The no-split answer must cost NOTHING to reach: it is the documented fast
// path, and it is what the self-metrics, node and cadvisor-rollup shapes (no
// k8s.namespace.name at all) take on every export — and, since a payload
// routed whole to ONE route is the usual shape of a routed tenant's push, what
// that takes too. Building the group slice first and discarding it allocated
// proportionally to the resource count — 128 KB per export at the
// 4,000-resource KSM/cadvisor shape, already half the cost of the copy the
// fast path exists to avoid. So: extend the run of resources sharing one
// unmarked destination with no allocation, and only where it breaks take a
// second pass into a slice sized exactly once. The resources before the break
// all share that destination, so re-matching them would be pure waste.
func (r *Router) split(signal string, n int, res func(int) pcommon.Resource) (groups []int, whole int) {
	// ONE Enabled call per export decides whether the narration below is built;
	// slog evaluates arguments eagerly, so an unguarded explainExport would walk
	// every resource of every payload at Info. The fast path's promise above is
	// kept: with Debug off this costs one interface call and nothing else.
	dbg := r.debugEnabled()
	whole = -1
	i := 0
	for ; i < n; i++ {
		idx, marked := r.match(res(i), false)
		// A marker forces the split: it must be stripped before anything is
		// sent, and only a COPY may be stripped.
		if marked || (i > 0 && idx != whole) {
			break
		}
		whole = idx
	}
	if i == n {
		if dbg {
			r.explainExport(signal, n, res, nil, whole)
		}
		return nil, whole
	}
	groups = make([]int, n)
	for j := 0; j < i; j++ {
		groups[j] = whole
	}
	for ; i < n; i++ {
		groups[i], _ = r.match(res(i), true)
	}
	if dbg {
		r.explainExport(signal, n, res, groups, -1)
	}
	return groups, -1
}

// stripMarker removes the script-routing marker from a COPIED resource; it
// must never reach a destination.
func stripMarker(res pcommon.Resource) {
	res.Attributes().Remove(ScriptMarker)
}
