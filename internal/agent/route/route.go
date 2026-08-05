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
	"path"
	"slices"
	"strconv"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
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
	return errors.Join(failed...)
}

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
	var sb strings.Builder
	sb.WriteString("export failed for ")
	sb.WriteString(strconv.Itoa(len(e.errs)))
	sb.WriteString(" of the payload's destinations: ")
	for i, err := range e.errs {
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
	// Endpoint decides).
	Insecure bool `json:"insecure,omitempty"`
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

// Router splits payloads across destinations.
type Router struct {
	def   Exporter
	dests []Destination
}

// New builds a Router forwarding unmatched resources to def.
func New(def Exporter, dests []Destination) *Router {
	return &Router{def: def, dests: dests}
}

// match returns the destination index for a namespace (-1 = default).
func (r *Router) match(res pcommon.Resource) int {
	ns, ok := res.Attributes().Get("k8s.namespace.name")
	if !ok {
		return -1
	}
	name := ns.Str()
	for i, d := range r.dests {
		for _, pat := range d.Namespaces {
			if ok, _ := path.Match(pat, name); ok {
				return i
			}
		}
	}
	return -1
}

// ExportLogs splits by resource namespace and forwards each group.
func (r *Router) ExportLogs(ctx context.Context, ld plog.Logs) error {
	groups := r.split(ld.ResourceLogs().Len(), func(i int) pcommon.Resource {
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
		rls.At(i).CopyTo(parts[g+1].ResourceLogs().AppendEmpty())
	}
	var errs []error
	if parts[0].ResourceLogs().Len() > 0 {
		errs = append(errs, r.def.ExportLogs(ctx, parts[0]))
	}
	for i, d := range r.dests {
		if p := parts[i+1]; p.ResourceLogs().Len() > 0 {
			err := d.Exporter.ExportLogs(ctx, p)
			if err == nil {
				// Count only DELIVERED parts, and only after the send: the
				// producer (ingest batcher) retries the whole payload on
				// failure, so counting before the send would tally failed
				// and retried attempts, not forwarded parts.
				obs.Routed.WithLabelValues(d.Name, "logs").Inc()
			}
			errs = append(errs, err)
		}
	}
	return joinExportErrs(errs)
}

// ExportMetrics splits by resource namespace and forwards each group.
func (r *Router) ExportMetrics(ctx context.Context, md pmetric.Metrics) error {
	groups := r.split(md.ResourceMetrics().Len(), func(i int) pcommon.Resource {
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
		rms.At(i).CopyTo(parts[g+1].ResourceMetrics().AppendEmpty())
	}
	var errs []error
	if parts[0].ResourceMetrics().Len() > 0 {
		errs = append(errs, r.def.ExportMetrics(ctx, parts[0]))
	}
	for i, d := range r.dests {
		if p := parts[i+1]; p.ResourceMetrics().Len() > 0 {
			err := d.Exporter.ExportMetrics(ctx, p)
			if err == nil {
				obs.Routed.WithLabelValues(d.Name, "metrics").Inc() // see ExportLogs
			}
			errs = append(errs, err)
		}
	}
	return joinExportErrs(errs)
}

// ExportTraces splits by resource namespace and forwards each group. Route
// destinations always support traces (they are otlpexport clients); the
// default may not — its group then errors only if non-empty.
func (r *Router) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	groups := r.split(td.ResourceSpans().Len(), func(i int) pcommon.Resource {
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
		rss.At(i).CopyTo(parts[g+1].ResourceSpans().AppendEmpty())
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
				errs = append(errs, errors.New("route "+d.Name+" does not support traces"))
				continue
			}
			err := te.ExportTraces(ctx, p)
			if err == nil {
				obs.Routed.WithLabelValues(d.Name, "traces").Inc() // see ExportLogs
			}
			errs = append(errs, err)
		}
	}
	return joinExportErrs(errs)
}

// split computes per-resource destinations; nil means "all default" (the
// caller then forwards the original payload untouched).
func (r *Router) split(n int, res func(int) pcommon.Resource) []int {
	var groups []int
	any := false
	for i := 0; i < n; i++ {
		g := r.match(res(i))
		if g >= 0 {
			any = true
		}
		groups = append(groups, g)
	}
	if !any {
		return nil
	}
	return groups
}
