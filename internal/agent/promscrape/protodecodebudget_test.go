package promscrape

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// emptyMetricsMessage is the hostile shape the wire cap does not bound: one
// in-cap MetricFamily message of EMPTY Metric entries, each two bytes on the
// wire (field 4, length 0) and a whole decoded struct on the heap. Built by
// hand, because marshalling two million dto.Metric values would itself
// allocate the heap the guard exists to refuse.
func emptyMetricsMessage() []byte {
	head := []byte{
		0x0a, 0x01, 'x', // name = "x"
		0x18, 0x01, // type = GAUGE
	}
	msg := append(head, bytes.Repeat([]byte{0x22, 0x00}, (maxProtoMessageBytes-len(head))/2)...)
	return append(binary.AppendUvarint(nil, uint64(len(msg))), msg...)
}

// A protobuf message is bounded by what it would DECODE to, not only by its
// wire size: proto.Unmarshal materialises the whole message, and a message of
// empty Metric entries (~4 KiB gzipped) measured 265 MiB of live heap per
// scrape — the maxProtoMessageBytes comment claimed this shape was refused, and
// it was not. It must be refused BEFORE the decode (so the heap is never
// built), classified body, and counted malformed.
func TestProtoMessageRefusedBeforeDecodePastItsDecodedBudget(t *testing.T) {
	body := emptyMetricsMessage()
	if n, k := binary.Uvarint(body); n > maxProtoMessageBytes || k <= 0 {
		t.Fatalf("fixture is %d bytes, over the wire cap: it would test the wrong guard", n)
	}
	exp := &captureExporter{}
	s := New(Config{Node: "n1", Interval: time.Hour, Timeout: time.Hour, NativeHistograms: true,
		Targets: staticTargets{}, Exporter: exp, StartTime: time.Now()})
	cb := newBatcher(func(pcommon.Resource) {}, time.Now(), time.Now())
	ss := s.newScrapeSession(context.Background(), cb, pipelineTargets, "t", "t", nil, true)

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	malformed, err := ss.parseProtoAndExport(bytes.NewReader(body))
	runtime.ReadMemStats(&after)

	if got := failureReason(err); got != reasonBody {
		t.Fatalf("err = %v (reason %q), want a %q refusal", err, got, reasonBody)
	}
	if malformed != 1 {
		t.Errorf("malformed = %d, want 1 (the refused family)", malformed)
	}
	// The body and the one message buffer are ~8 MiB between them; the decode
	// alone would have allocated hundreds.
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > 48<<20 {
		t.Errorf("refusing the message allocated %d MiB: it was decoded before it was refused", alloc>>20)
	}
}

// And the budget must ADMIT what a real target sends. A dense classic-histogram
// family (3 labels, 12 buckets per metric) is the most submessage-heavy shape a
// real exposition has — measured 9x wire-to-heap — so a message of that density
// at the full wire cap must estimate inside the decoded budget, or the guard
// refuses legitimate traffic outright.
func TestDenseRealisticFamilyFitsTheDecodedBudget(t *testing.T) {
	body := protoHistBody(t, 2000)
	n, k := binary.Uvarint(body)
	msg := body[k:]
	if int(n) != len(msg) {
		t.Fatalf("fixture framing: prefix %d, message %d bytes", n, len(msg))
	}
	est := protoDecodedSize(msg, maxProtoDecodedBytes)
	// Scale to a message of the same density filling the whole wire cap.
	atCap := est * maxProtoMessageBytes / len(msg)
	if atCap > maxProtoDecodedBytes {
		t.Fatalf("a %d-byte message of this density estimates at %d decoded bytes, over the %d budget: "+
			"a legitimate full-size family would be refused", maxProtoMessageBytes, atCap, maxProtoDecodedBytes)
	}
	// And the estimate is not vacuous: it must see the family's submessages.
	if est < 2000*14*protoSubmessageBytes {
		t.Fatalf("estimate %d is below one submessage per metric, bucket and label (%d): the walk is not descending",
			est, 2000*14*protoSubmessageBytes)
	}
}

// protoMetricsByName collects every exported metric by name.
func protoMetricsByName(exp *captureExporter) map[string]pmetric.Metric {
	out := map[string]pmetric.Metric{}
	for _, md := range exp.batches {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			ms := rms.At(i).ScopeMetrics().At(0).Metrics()
			for j := 0; j < ms.Len(); j++ {
				out[ms.At(j).Name()] = ms.At(j)
			}
		}
	}
	return out
}

// HELP and UNIT ride on every point a family emits, and the text front bounds
// the text it keeps per exposition (promparse.MaxMetaBytes): past it, a field
// simply is not carried. The protobuf front spends the same budget — a refused
// field is not charged, so a later small one is still admitted (too_long keeps
// its unit), while a budget SPENT by earlier families leaves the next one
// undescribed. This is parity, not a guarantee against an oversized OTLP leaf
// (a bucket count does that on either front).
func TestProtoHelpAndUnitShareTheTextFrontsBudget(t *testing.T) {
	gauge := func(name, help, unit string) *dto.MetricFamily {
		return &dto.MetricFamily{Name: new(name), Help: new(help), Unit: new(unit), Type: dto.MetricType_GAUGE.Enum(),
			Metric: []*dto.Metric{{Gauge: &dto.Gauge{Value: new(1.0)}}}}
	}
	half := strings.Repeat("h", maxMetaBytes/2)
	body := protoBody(t,
		gauge("too_long", strings.Repeat("h", maxMetaBytes), "u"), // HELP alone over the budget: refused, not charged
		gauge("small", "described", "seconds"),
		gauge("first_half", half, ""),
		gauge("second_half", half, ""), // the budget is spent by now
	)
	exp := &captureExporter{}
	s := New(Config{Node: "n1", Interval: time.Hour, Timeout: time.Hour, NativeHistograms: true,
		Targets: staticTargets{}, Exporter: exp, StartTime: time.Now()})
	cb := newBatcher(func(pcommon.Resource) {}, time.Now(), time.Now())
	if _, err := s.scrapeProto(context.Background(), bytes.NewReader(body), cb, nil, "t", "t"); err != nil {
		t.Fatal(err)
	}
	got := protoMetricsByName(exp)
	for _, tc := range []struct{ name, help, unit string }{
		{"too_long", "", "u"},
		{"small", "described", "seconds"},
		{"first_half", half, ""},
		{"second_half", "", ""},
	} {
		m, ok := got[tc.name]
		if !ok {
			t.Fatalf("%s was not exported: the budget must drop the text, never the family", tc.name)
		}
		if m.Description() != tc.help || m.Unit() != tc.unit {
			t.Errorf("%s: description %d bytes, unit %q; want %d bytes, %q",
				tc.name, len(m.Description()), m.Unit(), len(tc.help), tc.unit)
		}
	}
}

// metaFamily is one gauge family as both fronts receive it.
type metaFamily struct{ name, help, unit string }

// frontMeta scrapes families through one front and returns the description and
// unit each exported metric carries, by name. The text body is OpenMetrics
// with HELP before UNIT, the order client_golang's encoder writes them.
func frontMeta(t *testing.T, protobuf bool, families []metaFamily) map[string][2]string {
	t.Helper()
	exp := &captureExporter{}
	s := New(Config{Node: "n1", Interval: time.Hour, Timeout: time.Hour, NativeHistograms: true,
		Targets: staticTargets{}, Exporter: exp, StartTime: time.Now()})
	cb := newBatcher(func(pcommon.Resource) {}, time.Now(), time.Now())
	var err error
	if protobuf {
		mfs := make([]*dto.MetricFamily, len(families))
		for i, f := range families {
			mfs[i] = &dto.MetricFamily{Name: new(f.name), Help: new(f.help), Type: dto.MetricType_GAUGE.Enum(),
				Metric: []*dto.Metric{{Gauge: &dto.Gauge{Value: new(1.0)}}}}
			if f.unit != "" {
				mfs[i].Unit = new(f.unit)
			}
		}
		_, err = s.scrapeProto(context.Background(), bytes.NewReader(protoBody(t, mfs...)), cb, nil, "t", "t")
	} else {
		var sb strings.Builder
		for _, f := range families {
			fmt.Fprintf(&sb, "# HELP %s %s\n# TYPE %s gauge\n", f.name, f.help, f.name)
			if f.unit != "" {
				fmt.Fprintf(&sb, "# UNIT %s %s\n", f.name, f.unit)
			}
			fmt.Fprintf(&sb, "%s 1\n", f.name)
		}
		sb.WriteString("# EOF\n")
		_, err = s.parseAndExport(context.Background(), strings.NewReader(sb.String()), true, false, cb, pipelineTargets, "t")
	}
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][2]string{}
	for name, m := range protoMetricsByName(exp) {
		out[name] = [2]string{m.Description(), m.Unit()}
	}
	if len(out) != len(families) {
		t.Fatalf("protobuf=%v exported %d of %d families: the budget must drop text, never a family", protobuf, len(out), len(families))
	}
	return out
}

// sameDescriptions fails unless both fronts give every family the same
// description and unit, and returns what they gave.
func sameDescriptions(t *testing.T, families []metaFamily) map[string][2]string {
	t.Helper()
	text, proto := frontMeta(t, false, families), frontMeta(t, true, families)
	diff := 0
	for _, f := range families {
		if text[f.name] != proto[f.name] {
			if diff++; diff <= 3 {
				t.Errorf("%s: text front carries description %d bytes, unit %q; protobuf front %d bytes, %q",
					f.name, len(text[f.name][0]), text[f.name][1], len(proto[f.name][0]), proto[f.name][1])
			}
		}
	}
	if diff > 0 {
		t.Fatalf("%d of %d families are described differently depending on the format the target negotiated", diff, len(families))
	}
	return text
}

// Which families carry a Description and a Unit must not depend on the format
// a target negotiated: -scrape-native-histograms moves every client_golang
// target to the protobuf front at once. The text front charges a family's NAME
// against the HELP/UNIT budget on its first admitted field (it keeps the name
// as a table key), and admits HELP and UNIT separately; a protobuf front that
// charged only the text, and both fields as one, described every family of the
// first exposition here and a different set in the second.
func TestProtoFrontDescribesTheSameFamiliesAsTheTextFront(t *testing.T) {
	t.Run("family names are charged", func(t *testing.T) {
		families := make([]metaFamily, 20_000)
		for i := range families {
			families[i] = metaFamily{name: fmt.Sprintf("f%031d", i), help: strings.Repeat("h", 50)}
			if i%3 == 0 {
				families[i].unit = "seconds"
			}
		}
		got := sameDescriptions(t, families)
		described := 0
		for _, f := range families {
			if got[f.name][0] != "" {
				described++
			}
		}
		// Neither none nor all: the fixture must straddle the budget, or it
		// cannot tell the two accountings apart.
		if described == 0 || described == len(families) {
			t.Fatalf("%d of %d families described: the fixture does not reach the budget's edge", described, len(families))
		}
	})
	t.Run("a field that does not fit is dropped alone", func(t *testing.T) {
		fillA, fillB := strings.Repeat("h", 600<<10), strings.Repeat("h", 400<<10)
		left := maxMetaBytes - (len("filler_a") + len(fillA)) - (len("filler_b") + len(fillB))
		tooLong := strings.Repeat("h", left+1) // refused, yet its unit still fits
		left -= len("unit_only") + len("bytes")
		exact := strings.Repeat("h", left-len("straddle")) // spends the budget to the byte
		got := sameDescriptions(t, []metaFamily{
			{"filler_a", fillA, ""},
			{"filler_b", fillB, ""},
			{"unit_only", tooLong, "bytes"},
			{"straddle", exact, "seconds"},
			{"after", "x", "s"},
		})
		for name, want := range map[string][2]string{
			"filler_a":  {fillA, ""},
			"filler_b":  {fillB, ""},
			"unit_only": {"", "bytes"},
			"straddle":  {exact, ""},
			"after":     {"", ""},
		} {
			if got[name] != want {
				t.Errorf("%s: description %d bytes, unit %q; want %d bytes, %q",
					name, len(got[name][0]), got[name][1], len(want[0]), want[1])
			}
		}
	})
}

// The text front stops describing once MaxTrackedFamilies families hold an
// entry, however much of the byte budget is left, and the protobuf front counts
// the same families. Driven on the budget directly: an exposition of 100,001
// families through both fronts would pin the same branch at a hundred times
// the cost.
func TestProtoMetaBudgetStopsDescribingAtTheFamilyCap(t *testing.T) {
	b := protoMetaBudget{families: maxTrackedFamilies - 1}
	if help, unit := b.admit("last", "help", "unit"); help != "help" || unit != "unit" {
		t.Fatalf("the last family under the cap got %q/%q, want both fields: one family is one slot, however many fields it has", help, unit)
	}
	if help, unit := b.admit("over", "help", "unit"); help != "" || unit != "" {
		t.Fatalf("a family past MaxTrackedFamilies got %q/%q, want neither", help, unit)
	}
	if b.families != maxTrackedFamilies {
		t.Fatalf("families = %d, want %d", b.families, maxTrackedFamilies)
	}
}
