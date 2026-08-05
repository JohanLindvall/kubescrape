package otlpingest

import (
	"context"
	"fmt"
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

// A push naming more than maxSplitGroups distinct objects with NO id-less
// point (so the "" fallback group is never created before the cap binds) must
// not recurse forever: resource("") missing the map while the cap is exceeded
// used to call itself unboundedly — a stack overflow crash on the
// unauthenticated ingest listener, triggerable by any pod.
func TestSplitCapWithoutIdlessPointDoesNotRecurse(t *testing.T) {
	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	const n = maxSplitGroups + 5
	for i := 0; i < n; i++ {
		m := sm.Metrics().AppendEmpty()
		m.SetName("m")
		dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetIntValue(1)
		dp.Attributes().PutStr("k8s.pod.uid", fmt.Sprintf("uid-%d", i))
	}
	out := NewEnricher(Config{Meta: newMeta(), MetricsMode: MetricsDatapoint}).
		EnrichMetrics(context.Background(), md)

	// Every point is still forwarded: the capped remainder folds into the one
	// "" fallback resource rather than crashing.
	got := 0
	rms := out.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				got += ms.At(k).Gauge().DataPoints().Len()
			}
		}
	}
	if got != n {
		t.Fatalf("forwarded %d points, want all %d (capped remainder folded, none dropped)", got, n)
	}
	// The fallback is a single bucket, so the group count is bounded at the cap.
	if rms.Len() > maxSplitGroups+1 {
		t.Fatalf("group count %d exceeds the cap+fallback bound", rms.Len())
	}
}
