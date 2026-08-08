package route

// The all-default fast path is documented as "forwards untouched (no copy)".
// It must also cost no ALLOCATION to reach: it is what every export of the
// self-metrics, node and cadvisor-rollup shapes takes (those resources carry
// no k8s.namespace.name at all), and the split path it guards is the one that
// copies. Building the group slice before discovering that nothing matched
// scaled the garbage with the resource count — 128 KB per export at the
// 4,000-resource KSM-splitter / 12k-pod cadvisor shape.

import (
	"context"
	"strconv"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// noopDest counts exports without retaining anything, so the allocation budget
// measures the router and not the destination.
type noopDest struct{ n int }

func (d *noopDest) ExportLogs(context.Context, plog.Logs) error          { d.n++; return nil }
func (d *noopDest) ExportMetrics(context.Context, pmetric.Metrics) error { d.n++; return nil }

// defaultLogs builds n resources that match no route: half carry a namespace
// no glob covers, half carry no namespace attribute at all (the self-metrics /
// node / cadvisor-rollup shape).
func defaultLogs(n int) plog.Logs {
	ld := plog.NewLogs()
	for i := 0; i < n; i++ {
		rl := ld.ResourceLogs().AppendEmpty()
		if i%2 == 0 {
			rl.Resource().Attributes().PutStr("k8s.namespace.name", "unrouted-"+strconv.Itoa(i))
		}
		rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("x")
	}
	return ld
}

func TestAllDefaultExportIsAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector changes escape analysis and adds bookkeeping allocations")
	}
	def := &noopDest{}
	r := New(def, []Destination{{Name: "tenant-a", Namespaces: []string{"team-a-*", "prod"}, Exporter: &noopDest{}}})
	for _, n := range []int{10, 200, 4000} {
		ld := defaultLogs(n)
		ctx := context.Background()
		// Warm the destination's own state before measuring.
		if err := r.ExportLogs(ctx, ld); err != nil {
			t.Fatal(err)
		}
		got := testing.AllocsPerRun(20, func() {
			if err := r.ExportLogs(ctx, ld); err != nil {
				t.Fatal(err)
			}
		})
		if got > 0 {
			t.Errorf("%d all-default resources: %.0f allocs/export, want 0 — the fast path must not build a group slice it discards", n, got)
		}
	}
}

// The second pass must still classify every resource correctly, including the
// ones BEFORE the first match (which it deliberately does not re-match).
func TestSplitClassifiesResourcesBeforeTheFirstMatch(t *testing.T) {
	dest := &capDest{}
	def := &capDest{}
	r := New(def, []Destination{{Name: "tenant-a", Namespaces: []string{"team-a"}, Exporter: dest}})
	// default, default, MATCH, default, match
	ld := nsLogs("other-1", "other-2", "team-a", "other-3", "team-a")
	if err := r.ExportLogs(context.Background(), ld); err != nil {
		t.Fatal(err)
	}
	if got, want := bodies(def.logs), []string{"from other-1", "from other-2", "from other-3"}; !equalStrings(got, want) {
		t.Errorf("default got %v, want %v", got, want)
	}
	if got, want := bodies(dest.logs), []string{"from team-a", "from team-a"}; !equalStrings(got, want) {
		t.Errorf("route got %v, want %v", got, want)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
