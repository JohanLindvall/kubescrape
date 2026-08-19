package otlpingest

import (
	"context"
	"sync"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/metrics"
)

// Empirical probe: does the metric store retain a reference into the pushed
// payload's resource map? The ingest path is transform.Handoff-marked, so a
// Starlark transform may mutate that pdata IN PLACE during ExportLogs — after
// applyLogChain bound the resource. If Bind retains the map, this mutation
// races a concurrent metric-set Export render.
func TestLogChainExportVsInPlaceTransformIsRaceFree(t *testing.T) {
	set, err := metrics.NewDynamicMetricSet([]metrics.Dynamic{{
		Name: "probe", Type: "counter", Value: "1",
		MatchRegexp: []string{"__line__=x"},
		Labels:      []string{"svc=$service.name"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	s := NewServer(ServerConfig{
		Enricher:   newEnricher(newMeta(), MetricsAuto),
		Exporter:   &captureExporter{},
		LogMetrics: set,
	})

	var pushers sync.WaitGroup
	stop := make(chan struct{})
	var exporter sync.WaitGroup
	// Exporter goroutine: renders the set continuously while pushers run.
	exporter.Add(1)
	go func() {
		defer exporter.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = set.Export(context.Background(), &copyMetricsExporter{}, 0)
			}
		}
	}()
	// Pusher goroutines: push, then mutate the payload's resource in place —
	// what an in-place transform does during ExportLogs.
	for g := 0; g < 4; g++ {
		pushers.Add(1)
		go func() {
			defer pushers.Done()
			for i := 0; i < 200; i++ {
				ld := pushLogs(map[string][]string{"web": {"x one", "x two"}})
				if _, forward := s.applyLogChain(ld); !forward {
					t.Error("payload unexpectedly emptied")
					return
				}
				// The in-place transform mutating resource attributes.
				rl := ld.ResourceLogs().At(0)
				rl.Resource().Attributes().PutStr("service.name", "transformed")
				rl.Resource().Attributes().PutStr("extra", "attr")
			}
		}()
	}
	pushers.Wait()
	close(stop)
	exporter.Wait()
}
