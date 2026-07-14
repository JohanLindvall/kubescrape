package metrics

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

// flakyExporter fails the first chunk and records the rest.
type flakyExporter struct {
	calls int
	names []string
}

func (f *flakyExporter) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	f.calls++
	if f.calls == 1 {
		return errors.New("collector unavailable")
	}
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		if v, ok := rms.At(i).Resource().Attributes().Get("k8s.pod.name"); ok {
			f.names = append(f.names, v.Str())
		}
	}
	return nil
}

// TestExportContinuesPastFailedChunk: snapshot() has already sealed the
// aggregation windows and cleared the counters' initial flag by the time the
// first chunk is sent, so abandoning the remaining chunks on a failure would
// discard observations that no longer exist in the store. The export must keep
// going and report the first error.
func TestExportContinuesPastFailedChunk(t *testing.T) {
	set, err := NewDynamicMetricSet([]Dynamic{{
		Name:  "test_lines_total",
		Type:  CounterType,
		Value: "1",
	}})
	if err != nil {
		t.Fatal(err)
	}
	// Three distinct resources -> three chunks at a 1-byte chunk limit.
	for _, pod := range []string{"pod-a", "pod-b", "pod-c"} {
		set.Add(nil, labelsFrom(nil), res(map[string]string{"k8s.pod.name": pod}), "")
	}

	exp := &flakyExporter{}
	if err := set.Export(context.Background(), exp, 1); err == nil {
		t.Fatal("Export returned nil; the failed chunk must be reported")
	}
	if exp.calls != 3 {
		t.Fatalf("exporter called %d times, want 3: a failing chunk must not abandon the rest", exp.calls)
	}
	if len(exp.names) != 2 {
		t.Fatalf("delivered %v, want the 2 resources after the failing one", exp.names)
	}
}
