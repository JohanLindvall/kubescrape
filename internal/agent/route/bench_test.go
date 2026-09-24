package route

// The router is in EVERY export chain, twice: main builds a destination-less
// `preRoute` for the self-metrics fork and a second Router for the producers,
// so a payload with no routing configured is scanned by two of them. These
// measure that scan against the resource counts real payloads carry — a KSM
// split (thousands), a cadvisor batch (one per pod on the node), a tailer flush
// (one per file).

import (
	"context"
	"fmt"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// nopExporter is the default chain's stand-in: the benchmark measures the
// router, not the wire.
type nopExporter struct{}

func (nopExporter) ExportLogs(context.Context, plog.Logs) error          { return nil }
func (nopExporter) ExportMetrics(context.Context, pmetric.Metrics) error { return nil }

// enrichedResource is the attribute set attrs.Build stamps on a cadvisor or
// split resource: eighteen entries, with k8s.namespace.name in the middle of
// them. Both the marker probe and the namespace probe are linear scans over
// this, which is what the per-resource cost of a routing decision is made of.
func enrichedResource(a pcommon.Map, i int) {
	a.PutStr("k8s.node.name", "node1")
	a.PutStr("k8s.namespace.name", fmt.Sprintf("team-%d", i%16))
	a.PutStr("k8s.pod.name", fmt.Sprintf("checkout-%d-7f9c4b8d5-abcde", i))
	a.PutStr("k8s.pod.uid", fmt.Sprintf("pod-uid-%d", i))
	a.PutStr("k8s.container.name", "app")
	a.PutStr("container.id", fmt.Sprintf("%064x", i))
	a.PutStr("container.image.name", "registry.example.com/acme/checkout")
	a.PutStr("k8s.deployment.name", "checkout")
	a.PutStr("k8s.replicaset.name", "checkout-7f9c4b8d5")
	a.PutStr("service.name", "checkout")
	a.PutStr("service.namespace", fmt.Sprintf("team-%d", i%16))
	a.PutStr("service.instance.id", fmt.Sprintf("pod-uid-%d/app", i))
	a.PutStr("k8s.cluster.name", "eu-west-1-prod")
	a.PutStr("cloud.provider", "aws")
	a.PutStr("cloud.region", "eu-west-1")
	a.PutStr("host.name", "node1")
	a.PutStr("k8s.pod.ip", "10.1.2.3")
	a.PutStr("telemetry.distro.name", "kubescrape")
}

func routedMetrics(resources int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	for i := range resources {
		rm := md.ResourceMetrics().AppendEmpty()
		enrichedResource(rm.Resource().Attributes(), i)
		dp := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetIntValue(1)
	}
	return md
}

// The DEFAULT deployment: no routing section, so both Routers hold zero
// destinations and every export takes the uncopied fast path. This is what the
// scan costs a config that does not use the feature.
func BenchmarkRouterNoDestinations(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{1, 100, 4000} {
		md := routedMetrics(n)
		b.Run(fmt.Sprintf("resources=%d", n), func(b *testing.B) {
			r := New(nopExporter{}, nil)
			b.ReportAllocs()
			for b.Loop() {
				if err := r.ExportMetrics(ctx, md); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Routes configured that nothing matches: every resource's namespace is
// resolved and falls to the default. Without the namespace memo this walked
// every pattern of every route per resource, so it is measured at the route
// counts a tenant-per-route config reaches.
func BenchmarkRouterNoMatch(b *testing.B) {
	ctx := context.Background()
	for _, routes := range []int{1, 16, 64} {
		dests := make([]Destination, routes)
		for i := range dests {
			dests[i] = Destination{Name: fmt.Sprintf("tenant-%d", i),
				Namespaces: []string{fmt.Sprintf("other-%d-*", i), fmt.Sprintf("prod-%d", i)}, Exporter: nopExporter{}}
		}
		for _, n := range []int{100, 4000} {
			md := routedMetrics(n)
			b.Run(fmt.Sprintf("routes=%d/resources=%d", routes, n), func(b *testing.B) {
				r := New(nopExporter{}, dests)
				b.ReportAllocs()
				for b.Loop() {
					if err := r.ExportMetrics(ctx, md); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// Every resource to ONE route: forwarded uncopied, like the all-default fast
// path. This shape used to take the split and copy every resource.
func BenchmarkRouterSingleDestination(b *testing.B) {
	ctx := context.Background()
	dests := []Destination{{Name: "tenant-a", Namespaces: []string{"team-*"}, Exporter: nopExporter{}}}
	md := routedMetrics(100)
	r := New(nopExporter{}, dests)
	b.ReportAllocs()
	for b.Loop() {
		if err := r.ExportMetrics(ctx, md); err != nil {
			b.Fatal(err)
		}
	}
}

// The split path, for scale: a payload spanning the default chain and a route,
// so every resource is COPIED into its destination's part.
func BenchmarkRouterSplit(b *testing.B) {
	ctx := context.Background()
	// team-1 and team-10..team-15 of routedMetrics' sixteen namespaces.
	dests := []Destination{{Name: "tenant-a", Namespaces: []string{"team-1*"}, Exporter: nopExporter{}}}
	md := routedMetrics(100)
	r := New(nopExporter{}, dests)
	b.ReportAllocs()
	for b.Loop() {
		if err := r.ExportMetrics(ctx, md); err != nil {
			b.Fatal(err)
		}
	}
}
