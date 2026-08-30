package promscrape

// The warnOnce dedupe table is the one table in this repo with a ZERO re-warn
// window: nothing in it ever expires, so reaching maxWarnKeys suppresses every
// FUTURE distinct warning for the life of the process (internal/logdedupe's
// rule, and the right one — clearing instead is a permanent storm).
//
// That makes the KEY a security boundary. Anything that can mint distinct keys
// can shut this package's diagnostics: the kubelet's RBAC refusal, a monitor's
// uncompilable regex, a target refusing to honour the Accept header. Two doors
// used to be open — a metric family name taken straight off the SCRAPED BODY,
// and the free-form VALUE of a monitor's duration field — and each test here is
// that attack, not a paraphrase of it.

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// recordingLogger captures rendered lines so a test can assert both HOW MANY
// warnings were emitted and WHICH — the saturation notice in particular, which
// is the tell that the table has been wedged shut.
func recordingLogger() (*slog.Logger, func() string) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(h), buf.String
}

// mixedHistogramFamily is a family carrying BOTH representations, which is what
// makes protoFamily report. The name is the attacker-controlled part.
func mixedHistogramFamily(name string) *dto.MetricFamily {
	native := &dto.Metric{
		Label: []*dto.LabelPair{{Name: ptr("svc"), Value: ptr("a")}},
		Histogram: &dto.Histogram{
			SampleCount: ptr(uint64(3)), SampleSum: ptr(0.9),
			Schema: ptr(int32(2)), ZeroThreshold: ptr(1e-9), ZeroCount: ptr(uint64(1)),
			PositiveSpan:  []*dto.BucketSpan{{Offset: ptr(int32(1)), Length: ptr(uint32(1))}},
			PositiveDelta: []int64{2},
		},
	}
	classic := &dto.Metric{
		Label: []*dto.LabelPair{{Name: ptr("svc"), Value: ptr("b")}},
		Histogram: &dto.Histogram{
			SampleCount: ptr(uint64(4)), SampleSum: ptr(1.5),
			Bucket: []*dto.Bucket{
				{UpperBound: ptr(0.5), CumulativeCount: ptr(uint64(3))},
				{UpperBound: ptr(math.Inf(1)), CumulativeCount: ptr(uint64(4))},
			},
		},
	}
	return &dto.MetricFamily{Name: ptr(name), Type: dto.MetricType_HISTOGRAM.Enum(), Metric: []*dto.Metric{native, classic}}
}

// THE ATTACK: one target serves a protobuf exposition whose every family is
// mixed and uniquely named. Each family used to mint a permanent key, so a
// single scrape of a churning (or hostile) target filled the table and every
// later complaint in this package — from any pipeline, about any target — was
// suppressed for the process' life.
func TestScrapedBodyCannotMintWarnKeys(t *testing.T) {
	log, dump := recordingLogger()
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		NativeHistograms: true, Targets: staticTargets{},
		Exporter: &captureExporter{}, StartTime: time.Now(), Logger: log,
	})

	fams := make([]*dto.MetricFamily, 0, maxWarnKeys*2)
	for i := 0; i < maxWarnKeys*2; i++ {
		fams = append(fams, mixedHistogramFamily(fmt.Sprintf("attack_bucket_shape_%d", i)))
	}
	cb := newBatcher(func(pcommon.Resource) {}, time.Now(), time.Now())
	ss := s.newScrapeSession(context.Background(), cb, pipelineTargets,
		"http://10.4.0.1:9090/metrics", "servicemonitor:monitoring/api", nil, true)
	if _, err := s.parseProtoAndExport(ss, strings.NewReader(string(protoBody(t, fams...)))); err != nil {
		t.Fatal(err)
	}

	if got := s.warned.Len(); got != 1 {
		t.Errorf("the dedupe table holds %d keys after one scrape of %d uniquely named mixed families, want 1: the target is choosing the keys",
			got, len(fams))
	}
	if strings.Contains(dump(), "dedupe table is full") {
		t.Errorf("a single scrape saturated the warning table:\n%s", firstLines(dump(), 3))
	}

	// The consequence is what the table is FOR: an unrelated complaint, later,
	// must still reach the operator.
	before := strings.Count(dump(), "kubelet refused")
	s.warnOnce("kubeletauth:https://node:10250:cadvisor:403", "the kubelet refused the scrape")
	if strings.Count(dump(), "kubelet refused")-before != 1 {
		t.Errorf("an unrelated warning was suppressed after the scrape: the table has been wedged shut:\n%s", firstLines(dump(), 3))
	}
}

// The family name still has to REACH the operator — it is the whole diagnosis —
// so it rides as an attribute, clipped: a target that names a family with a
// megabyte of text must not put a megabyte into a log record either.
func TestMixedHistogramNamesTheFamilyOnTheLineClipped(t *testing.T) {
	log, dump := recordingLogger()
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		NativeHistograms: true, Targets: staticTargets{},
		Exporter: &captureExporter{}, StartTime: time.Now(), Logger: log,
	})
	huge := "h_" + strings.Repeat("x", 64*1024)
	cb := newBatcher(func(pcommon.Resource) {}, time.Now(), time.Now())
	ss := s.newScrapeSession(context.Background(), cb, pipelineTargets, "http://10.4.0.1:9090/metrics", "wk", nil, true)
	if _, err := s.parseProtoAndExport(ss, strings.NewReader(string(protoBody(t, mixedHistogramFamily(huge))))); err != nil {
		t.Fatal(err)
	}
	out := dump()
	if !strings.Contains(out, "metric=h_xxxx") {
		t.Errorf("the mixed-representation line does not name the family:\n%s", firstLines(out, 2))
	}
	if len(out) > 4096 {
		t.Errorf("the line carried the whole family name: %d bytes of log for one warning", len(out))
	}
}

// The same door on the CONFIGURATION side: the value of a monitor's duration
// field is free-form text somebody with edit rights on one ServiceMonitor can
// rewrite at will, and it used to be part of the key.
func TestMonitorFieldValueCannotMintWarnKeys(t *testing.T) {
	log, dump := recordingLogger()
	s := &Scraper{cfg: Config{Interval: time.Minute, Timeout: 5 * time.Second}, log: log}

	for i := 0; i < maxWarnKeys*2; i++ {
		tgt := testTarget("http://10.4.0.1:9090/metrics")
		tgt.Source, tgt.Monitor = "servicemonitor", "monitoring/api"
		tgt.Interval = fmt.Sprintf("%d pancakes", i)
		if got := s.targetInterval(tgt); got != time.Minute {
			t.Fatalf("targetInterval = %v, want the default", got)
		}
	}
	if got := s.warned.Len(); got != 1 {
		t.Errorf("the dedupe table holds %d keys after %d edits of ONE monitor field, want 1", got, maxWarnKeys*2)
	}
	if strings.Contains(dump(), "dedupe table is full") {
		t.Errorf("editing one CR field saturated the warning table:\n%s", firstLines(dump(), 3))
	}
	// A DIFFERENT monitor is a different problem and still gets through: the
	// key is identity, and identity is bounded by the cluster's own objects.
	other := testTarget("http://10.9.9.9:9090/metrics")
	other.Source, other.Monitor, other.Interval = "servicemonitor", "monitoring/other", "10 pancakes"
	s.targetInterval(other)
	if got := s.warned.Len(); got != 2 {
		t.Errorf("a second monitor's broken field was suppressed: table holds %d keys, want 2", got)
	}
}

func TestClipForLogCutsOnARuneBoundary(t *testing.T) {
	// One ASCII byte then 3-byte runes, so the cut at maxLoggedValueBytes lands
	// INSIDE a rune rather than conveniently between two.
	v := "x" + strings.Repeat("€", maxLoggedValueBytes)
	got := clipForLog(v)
	if len(got) > maxLoggedValueBytes+len("…") {
		t.Errorf("clipped value is %d bytes, want <= %d", len(got), maxLoggedValueBytes+len("…"))
	}
	if strings.ContainsRune(got, '�') {
		t.Errorf("clip cut a rune in half: %q", got)
	}
	if short := "10 pancakes"; clipForLog(short) != short {
		t.Errorf("clipForLog rewrote a short value: %q", clipForLog(short))
	}
}

func firstLines(s string, n int) string {
	lines := strings.SplitN(s, "\n", n+1)
	if len(lines) > n {
		lines = lines[:n]
	}
	return strings.Join(lines, "\n")
}
