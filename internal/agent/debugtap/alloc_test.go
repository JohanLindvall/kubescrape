package debugtap

// The tap sits in EVERY export chain, so what it costs with nobody attached is
// paid by every export on every node forever. The package doc promises one
// atomic load; this is what makes that promise fail the build if it stops
// being true.

import (
	"context"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestZeroSubscriberExportIsAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector changes escape analysis and adds bookkeeping allocations")
	}
	ctx := context.Background()
	tp := New(&fakeInner{})

	// 512 resources, so a cost that walks the payload would show up as a large
	// number rather than a marginal one. Nothing here may look at the payload
	// at all: the render closure is built inside the subscriber check, so an
	// export with no subscriber must not even allocate that.
	ld := benchLogs(512, 16)
	if got := testing.AllocsPerRun(20, func() {
		if err := tp.ExportLogs(ctx, ld); err != nil {
			t.Fatal(err)
		}
	}); got != 0 {
		t.Errorf("ExportLogs with no subscriber allocates %.1f times, want 0", got)
	}

	md := pmetric.NewMetrics()
	for r := 0; r < 512; r++ {
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("k8s.namespace.name", "team-1")
		rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(1)
	}
	if got := testing.AllocsPerRun(20, func() {
		if err := tp.ExportMetrics(ctx, md); err != nil {
			t.Fatal(err)
		}
	}); got != 0 {
		t.Errorf("ExportMetrics with no subscriber allocates %.1f times, want 0", got)
	}
}
