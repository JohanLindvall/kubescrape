// Package route fans exported payloads out to multiple destinations/tenants
// by Kubernetes namespace: each route matches `k8s.namespace.name` globs and
// forwards to its own OTLP client (different endpoint and/or extra headers,
// e.g. X-Scope-OrgID); unmatched resources go to the default exporter.
//
// It sits between the transforms and the default delivery chain
// (producers → transform → router → {default buffered chain | route
// clients}), splitting each payload per destination. First-matching route
// wins. A payload where everything matches the default forwards untouched
// (no copy). Delivery is at-least-once per destination: a failed destination
// fails the whole export, and the producer's retry re-splits
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

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

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
	idx := 0
	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		g := groups[idx]
		idx++
		rl.MoveTo(parts[g+1].ResourceLogs().AppendEmpty())
		return true
	})
	var errs []error
	if parts[0].ResourceLogs().Len() > 0 {
		errs = append(errs, r.def.ExportLogs(ctx, parts[0]))
	}
	for i, d := range r.dests {
		if p := parts[i+1]; p.ResourceLogs().Len() > 0 {
			obs.Routed.WithLabelValues(d.Name, "logs").Inc()
			errs = append(errs, d.Exporter.ExportLogs(ctx, p))
		}
	}
	return errors.Join(errs...)
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
	idx := 0
	md.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		g := groups[idx]
		idx++
		rm.MoveTo(parts[g+1].ResourceMetrics().AppendEmpty())
		return true
	})
	var errs []error
	if parts[0].ResourceMetrics().Len() > 0 {
		errs = append(errs, r.def.ExportMetrics(ctx, parts[0]))
	}
	for i, d := range r.dests {
		if p := parts[i+1]; p.ResourceMetrics().Len() > 0 {
			obs.Routed.WithLabelValues(d.Name, "metrics").Inc()
			errs = append(errs, d.Exporter.ExportMetrics(ctx, p))
		}
	}
	return errors.Join(errs...)
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
	idx := 0
	td.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
		g := groups[idx]
		idx++
		rs.MoveTo(parts[g+1].ResourceSpans().AppendEmpty())
		return true
	})
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
			obs.Routed.WithLabelValues(d.Name, "traces").Inc()
			errs = append(errs, te.ExportTraces(ctx, p))
		}
	}
	return errors.Join(errs...)
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
