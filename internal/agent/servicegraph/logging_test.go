package servicegraph

// The pairing store's back-pressure, seen from the operator's side.

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

func capturedLog() (*slog.Logger, func() string) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(h), buf.String
}

// At MaxItems every span is refused and its partner will later expire unpaired,
// so one request is lost per two counted spans — and a missing edge in Grafana
// looks exactly like a call that never happened. The counter carries the rate;
// the line has to carry the cap and the remedy, and must not repeat per span.
func TestPairingStoreFullIsWarnedOnceWithTheCap(t *testing.T) {
	log, dump := capturedLog()
	cfg := Config{MaxItems: 1}.withDefaults()
	wait, err := cfg.wait()
	if err != nil {
		t.Fatal(err)
	}
	st := newEdgeStore(cfg, wait, func(Edge) {}, log)

	now := time.Unix(1000, 0)
	// One entry fills it; every later distinct key is refused.
	for i := 0; i < 5; i++ {
		st.upsert(now, makeEdgeKey(traceID(byte(i+1)), spanID(1)), sideClient, halfSpan{service: "a"}, nil)
	}
	if st.stats().Dropped == 0 {
		t.Fatal("the store refused nothing; the test does not exercise the cap")
	}
	out := dump()
	if n := strings.Count(out, "pairing store is full"); n != 1 {
		t.Errorf("want exactly one throttled warning, got %d:\n%s", n, out)
	}
	if !strings.Contains(out, "maxItems=1") {
		t.Errorf("the warning does not name the cap that bound:\n%s", out)
	}
}

// A store that is not full says nothing: this is a Warn on the ingest path and
// must stay silent in steady state.
func TestPairingStoreBelowTheCapIsSilent(t *testing.T) {
	log, dump := capturedLog()
	cfg := Config{MaxItems: 64}.withDefaults()
	wait, _ := cfg.wait()
	st := newEdgeStore(cfg, wait, func(Edge) {}, log)
	st.upsert(time.Unix(1000, 0), makeEdgeKey(traceID(1), spanID(1)), sideClient, halfSpan{service: "a"}, nil)
	if out := dump(); out != "" {
		t.Errorf("a healthy store logged:\n%s", out)
	}
}

// kubescrape_service_graph_unnamed_spans_total says requests are missing from
// the graph; only a line can say whose — and a resource with no service.name is
// precisely the one an operator cannot select by.
func TestUnnamedResourceIsWarnedWithIdentityHints(t *testing.T) {
	log, dump := capturedLog()
	p := NewProcessor(Config{}, log)
	base := len(dump())

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("k8s.namespace.name", "shop")
	rs.Resource().Attributes().PutStr("k8s.pod.name", "legacy-7d9")
	sp := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetTraceID(traceID(1))
	sp.SetSpanID(spanID(1))
	sp.SetKind(ptrace.SpanKindClient)

	for i := 0; i < 3; i++ {
		p.Consume(td)
	}
	out := dump()[base:]
	if n := strings.Count(out, "no service.name"); n != 1 {
		t.Errorf("want one throttled warning, got %d:\n%s", n, out)
	}
	if !strings.Contains(out, "k8s.pod.name=legacy-7d9") {
		t.Errorf("the warning carries nothing that identifies the sender:\n%s", out)
	}
}

// A named resource says nothing: this sits on the receive path.
func TestNamedResourceIsSilent(t *testing.T) {
	log, dump := capturedLog()
	p := NewProcessor(Config{}, log)
	base := len(dump())
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "checkout")
	sp := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetTraceID(traceID(1))
	sp.SetSpanID(spanID(1))
	sp.SetKind(ptrace.SpanKindClient)
	p.Consume(td)
	if out := dump()[base:]; out != "" {
		t.Errorf("a named resource logged:\n%s", out)
	}
}

// The identity hints are the SENDER's values, arriving on the tier's
// unauthenticated application ports. The throttle bounds how OFTEN this line is
// written; only a clip bounds how LARGE it is, and a 4 MiB k8s.namespace.name
// costs a 4 MiB log record every unnamedWarnEvery on every shard.
//
// Reverse-patch check: drop the clipForLog call in reportUnnamed and this fails
// on the record size.
func TestUnnamedWarningClipsSenderSuppliedValues(t *testing.T) {
	log, dump := capturedLog()
	p := NewProcessor(Config{}, log)
	base := len(dump())

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	// Multi-byte runes, so a byte-boundary cut would produce invalid UTF-8.
	rs.Resource().Attributes().PutStr("k8s.namespace.name", strings.Repeat("é", 200_000))
	rs.Resource().Attributes().PutStr("k8s.pod.name", strings.Repeat("ω", 200_000))
	sp := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetTraceID(traceID(1))
	sp.SetSpanID(spanID(1))
	sp.SetKind(ptrace.SpanKindClient)
	p.Consume(td)

	out := dump()[base:]
	if !strings.Contains(out, "no service.name") {
		t.Fatalf("the warning did not fire:\n%.200s", out)
	}
	// Two clipped values plus the fixed message; nowhere near the 800 KB the
	// sender chose.
	if len(out) > 1024 {
		t.Errorf("the warning record is %d bytes; sender values reached the line unclipped", len(out))
	}
	if !utf8.ValidString(out) {
		t.Error("the clip cut a UTF-8 sequence in half")
	}
	if !strings.Contains(out, "…") {
		t.Errorf("the clip left no marker, so the operator cannot tell the value was cut:\n%s", out)
	}
}

// Short values ride whole — the clip must not damage the diagnostic it exists
// to deliver.
func TestClipForLogLeavesShortValuesAlone(t *testing.T) {
	if got := clipForLog("shop"); got != "shop" {
		t.Errorf("clipForLog rewrote a short value: %q", got)
	}
	for _, in := range []string{
		strings.Repeat("é", maxLoggedValueBytes),
		strings.Repeat("ω", maxLoggedValueBytes),
		strings.Repeat("😀", maxLoggedValueBytes),
		strings.Repeat("a", maxLoggedValueBytes) + "é",
	} {
		got := clipForLog(in)
		if !utf8.ValidString(got) {
			t.Errorf("clipForLog(%.8q...) produced invalid UTF-8: %q", in, got)
		}
		// The marker is 3 bytes and rides on top of the bound; the VALUE half
		// is what the bound governs.
		if len(strings.TrimSuffix(got, "…")) > maxLoggedValueBytes {
			t.Errorf("clipForLog returned %d bytes, over the %d-byte bound", len(got), maxLoggedValueBytes)
		}
	}
}
