package promscrape

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// exemplarWith builds a protobuf exemplar carrying n labels, named by nameOf.
func exemplarWith(n int, nameOf func(int) string) *dto.Exemplar {
	e := &dto.Exemplar{Value: new(1.0), Label: make([]*dto.LabelPair, 0, n)}
	for i := range n {
		e.Label = append(e.Label, &dto.LabelPair{Name: new(nameOf(i)), Value: new("v")})
	}
	return e
}

// runeExemplar builds a protobuf exemplar whose label set is exactly `runes`
// code points: a 1-rune name and a value of two-byte runes, so a BYTE bound
// would refuse what the spec allows.
func runeExemplar(runes int) *dto.Exemplar {
	return &dto.Exemplar{Value: new(1.0), Label: []*dto.LabelPair{
		{Name: new("t"), Value: new(strings.Repeat("é", runes-1))},
	}}
}

// An exemplar's label block is bounded by the OpenMetrics rule — 128 code
// points of names plus values, Prometheus's exemplar.ExemplarMaxLabelSetLength —
// exactly as the text front bounds it (promparse's
// TestExemplarLabelSetIsBoundedInRunes), and it needs the bound MORE than a
// metric needs protoLabels' cap: the labels end up in pmetric.Exemplar's
// FilteredAttributes through PutStr, which probes the map linearly before every
// insert, so the WRITE is quadratic on top of the duplicate scan — measured
// 10.5s at 80k labels. The per-sample label cap this used to take was not
// enough: 4096 labels still cost ~50 ms per exemplar, one exemplar per bucket
// row, so one metric of 85 exemplar-bearing buckets ran 4 s past its deadline.
func TestProtoExemplarLabelSetIsBoundedInRunes(t *testing.T) {
	ss := protoExemplarSession(t)
	var scratch Exemplar

	if e := ss.protoExemplar(runeExemplar(maxExemplarLabelSetRunes), &scratch); e == nil {
		t.Errorf("an exemplar of exactly %d runes was refused; the bound is inclusive and counted in runes, not bytes", maxExemplarLabelSetRunes)
	}
	if got := ss.badExemplars; got != 0 {
		t.Errorf("an accepted exemplar counted %d bad; want 0", got)
	}
	if e := ss.protoExemplar(runeExemplar(maxExemplarLabelSetRunes+1), &scratch); e != nil {
		t.Errorf("an exemplar of %d runes was accepted; it must be refused past the bound", maxExemplarLabelSetRunes+1)
	}
	if got := ss.badExemplars; got != 1 {
		t.Errorf("the refusal counted %d bad exemplars, want 1 — it is a bad exemplar, not a malformed sample", got)
	}

	// A label COUNT past the bound is refused before a single label is read
	// (every accepted name costs at least one rune), and cheaply: neither the
	// duplicate scan nor setExemplar's quadratic write ever runs. 200k labels
	// is half the count that fits inside one 4 MiB message.
	for _, n := range []int{maxExemplarLabelSetRunes + 1, maxLabelsPerSample, 200_000} {
		start := time.Now()
		if e := ss.protoExemplar(exemplarWith(n, seqName), &scratch); e != nil {
			t.Fatalf("a %d-label exemplar was accepted", n)
		}
		if d := time.Since(start); d > time.Second {
			t.Errorf("refusing a %d-label exemplar took %v; the bound is not short-circuiting the copy", n, d)
		}
	}
}

func seqName(i int) string { return "l" + strconv.Itoa(i) }

// A repeated label name is refused whole, exactly as the text front refuses the
// exemplar suffix that carries one: labelValue resolves the FIRST match while
// PutStr keeps the LAST, so shipping one of the two would attribute the
// exemplar to a value nothing else in the pipeline agrees on. An empty name is
// refused for the same reason protoLabels refuses it.
func TestProtoExemplarWithARepeatedLabelNameIsRefused(t *testing.T) {
	var scratch Exemplar
	for _, tc := range []struct {
		name string
		ex   *dto.Exemplar
	}{
		{"duplicate", exemplarWith(3, func(i int) string {
			if i == 2 {
				return "l0"
			}
			return seqName(i)
		})},
		{"empty name", exemplarWith(2, func(i int) string {
			if i == 1 {
				return ""
			}
			return seqName(i)
		})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ss := protoExemplarSession(t)
			if e := ss.protoExemplar(tc.ex, &scratch); e != nil {
				t.Fatalf("exemplar accepted with labels %v", e.Labels)
			}
			if got := ss.badExemplars; got != 1 {
				t.Fatalf("counted %d bad exemplars, want 1", got)
			}
		})
	}
}

// The whole guarantee end to end: the SAMPLE still ships (an exemplar is
// decoration, and kubescrape_scrape_malformed_total means data was DROPPED),
// the exemplar does not, and the protobuf front reports it on the same counter
// the text front uses — which it did not report at all before.
func TestProtoRefusedExemplarStillExportsItsSampleAndCounts(t *testing.T) {
	fam := &dto.MetricFamily{
		Name: new("http_requests_total"), Type: dto.MetricType_COUNTER.Enum(),
		Metric: []*dto.Metric{{
			Counter: &dto.Counter{Value: new(7.0), Exemplar: exemplarWith(maxLabelsPerSample+1, seqName)},
		}},
	}
	body := protoBody(t, fam)

	exp := &captureExporter{}
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		NativeHistograms: true, Exemplars: true, Targets: staticTargets{},
		Exporter: exp, StartTime: time.Now(),
	})
	before := obs.ScrapeExemplarsMalformed.WithLabelValues(pipelineTargets).Value()
	beforeMalformed := obs.ScrapeMalformed.WithLabelValues(pipelineTargets).Value()

	cb := newBatcher(func(pcommon.Resource) {}, time.Now(), time.Now())
	samples, err := s.scrapeProto(context.Background(), strings.NewReader(string(body)), cb, nil, "t", "t")
	if err != nil {
		t.Fatal(err)
	}
	if samples != 1 || exp.points() != 1 {
		t.Fatalf("parsed %d samples, exported %d points, want 1 and 1 — the sample must survive its exemplar", samples, exp.points())
	}
	got := exp.batches[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
	if n := got.Sum().DataPoints().At(0).Exemplars().Len(); n != 0 {
		t.Errorf("point carried %d exemplars, want 0 — the refused one must not ship", n)
	}
	if d := obs.ScrapeExemplarsMalformed.WithLabelValues(pipelineTargets).Value() - before; d != 1 {
		t.Errorf("malformed exemplars counted = %v, want 1", d)
	}
	if d := obs.ScrapeMalformed.WithLabelValues(pipelineTargets).Value() - beforeMalformed; d != 0 {
		t.Errorf("malformed samples counted = %v, want 0 — nothing was dropped", d)
	}
}

func protoExemplarSession(t *testing.T) *scrapeSession {
	t.Helper()
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		NativeHistograms: true, Exemplars: true, Targets: staticTargets{},
		Exporter: &captureExporter{}, StartTime: time.Now(),
	})
	cb := newBatcher(func(pcommon.Resource) {}, time.Now(), time.Now())
	return s.newScrapeSession(context.Background(), cb, pipelineTargets, "t", "t", nil, true)
}
