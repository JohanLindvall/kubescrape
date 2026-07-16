package promscrape

import (
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/promparse"
)

// A family name reused across incompatible metric shapes (a histogram family
// then a bare number sample of the same name) must skip the colliding sample,
// count it (obs.ScrapeCollisions), and leave the rest of the scrape intact —
// the numberDataPoint default branch.
func TestNumberSampleOnHistogramFamilySkipped(t *testing.T) {
	// The bare "lat 42" arrives AFTER the histogram family flushed (the family
	// switch at ok_total emits it), so the name is already claimed by a
	// Histogram-shaped metric when the number sample reaches the batcher.
	body := `# TYPE lat histogram
lat_bucket{le="1"} 5
lat_bucket{le="+Inf"} 7
lat_sum 3.5
lat_count 7
# TYPE ok counter
ok_total 1
lat 42
`
	before := obs.ScrapeCollisions.Value()
	bt := newBatcher(func(pcommon.Resource) {}, 1<<30, time.Unix(1, 0), time.Unix(2, 0))
	conv := newConverter(bt, nil)
	p := newParser(promparse.Options{MaxLineBytes: 1 << 20})
	if _, err := p.Parse(strings.NewReader(body), func(s Sample) error {
		return conv.add(s)
	}); err != nil {
		t.Fatal(err)
	}
	if err := conv.finish(); err != nil {
		t.Fatal(err)
	}

	if got := obs.ScrapeCollisions.Value() - before; got != 1 {
		t.Fatalf("collision delta = %v, want 1 (the bare lat sample)", got)
	}
	metrics := bt.take().ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	byName := map[string]pmetric.MetricType{}
	for i := 0; i < metrics.Len(); i++ {
		byName[metrics.At(i).Name()] = metrics.At(i).Type()
	}
	if byName["lat"] != pmetric.MetricTypeHistogram {
		t.Fatalf("lat = %v, want the Histogram (the number sample must not claim it)", byName["lat"])
	}
	if byName["ok_total"] != pmetric.MetricTypeSum {
		t.Fatalf("rest of the scrape lost: %v", byName)
	}
}

// Negative/NaN cumulative counts wrap uint64 into ~9.2e18 garbage; such
// exposition must be counted malformed, not exported.
func TestNegativeCountCountedMalformed(t *testing.T) {
	body := `# TYPE rpc summary
rpc_sum 8000
rpc_count -1
# TYPE lat histogram
lat_bucket{le="1"} -5
lat_bucket{le="+Inf"} 7
lat_count NaN
lat_sum 1
`
	bt := newBatcher(func(pcommon.Resource) {}, 1<<30, time.Unix(1, 0), time.Unix(2, 0))
	conv := newConverter(bt, nil)
	p := newParser(promparse.Options{MaxLineBytes: 1 << 20})
	if _, err := p.Parse(strings.NewReader(body), func(s Sample) error {
		return conv.add(s)
	}); err != nil {
		t.Fatal(err)
	}
	if err := conv.finish(); err != nil {
		t.Fatal(err)
	}
	if conv.malformed != 3 { // rpc_count, lat_bucket{le=1}, lat_count
		t.Fatalf("malformed = %d, want 3", conv.malformed)
	}
	// Nothing exported a wrapped ~9.2e18 count.
	md := bt.take()
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		ms := rms.At(i).ScopeMetrics().At(0).Metrics()
		for j := 0; j < ms.Len(); j++ {
			m := ms.At(j)
			switch m.Type() {
			case pmetric.MetricTypeSummary:
				for k := 0; k < m.Summary().DataPoints().Len(); k++ {
					if m.Summary().DataPoints().At(k).Count() > 1<<40 {
						t.Fatalf("summary count wrapped: %d", m.Summary().DataPoints().At(k).Count())
					}
				}
			case pmetric.MetricTypeHistogram:
				for k := 0; k < m.Histogram().DataPoints().Len(); k++ {
					if m.Histogram().DataPoints().At(k).Count() > 1<<40 {
						t.Fatalf("histogram count wrapped: %d", m.Histogram().DataPoints().At(k).Count())
					}
				}
			}
		}
	}
}
