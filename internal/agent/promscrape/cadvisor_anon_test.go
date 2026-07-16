package promscrape

import (
	"context"
	"strings"
	"testing"
)

// Standalone (non-k8s) containers — a parseable container ID in the cgroup path
// but no namespace/pod/container labels — must each get their OWN resource
// carrying container.id: merging them into one anonymous resource would emit
// indistinguishable, conflicting series (their id/name/image labels are elided
// from pod-scoped rows).
func TestCadvisorStandaloneContainersStayDistinct(t *testing.T) {
	cidA := strings.Repeat("a", 64)
	cidB := strings.Repeat("b", 64)
	body := `# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{id="/system.slice/docker-` + cidA + `.scope"} 1
container_cpu_usage_seconds_total{id="/system.slice/docker-` + cidB + `.scope"} 2
`
	srv := serveBody(t, body)

	exp := &captureExporter{}
	s := newKubeletScraper(t, srv.URL, &fakeMetaSource{}, exp, false)
	if _, err := s.scrapeCadvisor(context.Background()); err != nil {
		t.Fatal(err)
	}

	vals := map[string]float64{} // container.id -> cpu value
	rms := exp.batches[0].ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		cid := attrStr(rm.Resource(), "container.id")
		ms := rm.ScopeMetrics().At(0).Metrics()
		for j := 0; j < ms.Len(); j++ {
			dps := ms.At(j).Sum().DataPoints()
			for k := 0; k < dps.Len(); k++ {
				vals[cid] += dps.At(k).DoubleValue()
			}
		}
	}
	if vals[cidA] != 1 || vals[cidB] != 2 {
		t.Fatalf("standalone containers merged/mislabeled: per-container.id values = %v", vals)
	}
}
