package otlpingest

// Regression test for what maxSplitCopyBytes actually bounds. The estimators
// are charged against a MEMORY ceiling, but they were shaped like WIRE-size
// estimators: pcommon's CopyTo copies string HEADERS, so a copy's marginal cost
// is a per-attribute constant the wire never names, while the string bytes it
// charged are the ones a copy does not allocate. Estimating a 200-attribute
// resource at a fifth of its real cost made the ceiling bind five times too
// late — and the effective bound is that, times -ingest-max-in-flight.

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// copyHeapBytes is what one CopyTo of a really costs, measured.
func copyHeapBytes(a pcommon.Map, n int) int {
	dst := make([]pcommon.Map, n)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := range dst {
		m := pcommon.NewMap()
		a.CopyTo(m)
		dst[i] = m
	}
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(dst)
	return int(after.TotalAlloc-before.TotalAlloc) / n
}

func TestCopySizeEstimateBoundsRealMemory(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector's own bookkeeping distorts the allocation measurement")
	}
	for _, tc := range []struct {
		name string
		fill func(pcommon.Map)
	}{
		{"typical resource", func(a pcommon.Map) {
			a.PutStr("service.name", "checkout")
			a.PutStr("k8s.pod.name", "checkout-7d9f8b6c4d-x2k9p")
			a.PutStr("k8s.namespace.name", "shop")
			a.PutStr("k8s.node.name", "ip-10-0-3-17.eu-north-1.compute.internal")
			a.PutStr("container.id", strings.Repeat("a", 64))
		}},
		{"many short attributes", func(a pcommon.Map) {
			for i := 0; i < 200; i++ {
				a.PutStr(fmt.Sprintf("k%d", i), "v")
			}
		}},
		{"many int attributes", func(a pcommon.Map) {
			for i := 0; i < 200; i++ {
				a.PutInt(fmt.Sprintf("k%d", i), int64(i))
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := pcommon.NewResource()
			tc.fill(res.Attributes())
			est := resourceCopySize(res, "")
			got := copyHeapBytes(res.Attributes(), 4000)
			// The estimate may overshoot freely (over-charging binds the ceiling
			// early, which is the safe direction); what it must never do is
			// UNDERSTATE the real cost by a factor.
			if got > 2*est {
				t.Errorf("one copy allocates %d B against an estimate of %d B (%.2fx): maxSplitCopyBytes bounds memory, so the estimate must not trail it",
					got, est, float64(got)/float64(est))
			}
		})
	}
}
