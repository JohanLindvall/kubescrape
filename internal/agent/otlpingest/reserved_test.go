package otlpingest

// Wire-supplied copies of kubescrape's reserved plumbing keys die at first
// receipt (ServerConfig.ReservedAttrs). The keys under test are TEST-LOCAL:
// which keys are reserved is the caller's wiring — the real lists name
// route.ScriptMarker and transform.DropMarker, and this package must not know
// that (the RejectTraces rule; cmd/kubescrape-agent pins the real spellings).

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

const (
	testResKey  = "test.plumbing.route"
	testElemKey = "__test_plumbing_drop__"
)

func testReserved() ReservedAttrs {
	return ReservedAttrs{Resource: []string{testResKey}, Element: []string{testElemKey}}
}

// stripDeltas snapshots the per-key strip counter and returns a delta reader.
func stripDeltas() func() (res, elem float64) {
	r0 := obs.IngestReservedStripped.WithLabelValues(testResKey).Value()
	e0 := obs.IngestReservedStripped.WithLabelValues(testElemKey).Value()
	return func() (float64, float64) {
		return obs.IngestReservedStripped.WithLabelValues(testResKey).Value() - r0,
			obs.IngestReservedStripped.WithLabelValues(testElemKey).Value() - e0
	}
}

// The logs seam over HTTP: the resource-level key and the record-level key are
// both gone from the forwarded payload, everything else — the sender's own
// attributes AND the enrichment that runs after the strip — survives, and each
// removed occurrence is counted under its key.
func TestReservedKeysStrippedFromLogsHTTP(t *testing.T) {
	exp := &captureExporter{}
	s := NewServer(ServerConfig{
		Enricher:      newEnricher(newMeta(), MetricsAuto),
		Exporter:      exp,
		ReservedAttrs: testReserved(),
	})
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/logs", s.handleHTTPLogs)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	delta := stripDeltas()

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("container.id", "cafe01")
	rl.Resource().Attributes().PutStr(testResKey, "tenant-b")
	rl.Resource().Attributes().PutStr("keep.me", "yes")
	lrs := rl.ScopeLogs().AppendEmpty().LogRecords()
	marked := lrs.AppendEmpty()
	marked.Body().SetStr("hello")
	marked.Attributes().PutBool(testElemKey, true)
	marked.Attributes().PutStr("app.attr", "v")
	lrs.AppendEmpty().Body().SetStr("clean")

	body, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(srv.URL+"/v1/logs", "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(exp.logs) != 1 {
		t.Fatalf("forwarded %d payloads, want 1", len(exp.logs))
	}
	got := exp.logs[0].ResourceLogs().At(0)
	ra := got.Resource().Attributes()
	if v, ok := ra.Get(testResKey); ok {
		t.Errorf("%s = %q survived onto the forwarded resource; a wire copy steers the router", testResKey, v.Str())
	}
	if v, ok := ra.Get("keep.me"); !ok || v.Str() != "yes" {
		t.Error("the strip took a sender attribute with it")
	}
	if v, ok := ra.Get("k8s.pod.name"); !ok || v.Str() != "web-1" {
		t.Error("the strip broke enrichment (it runs before EnrichLogs, not instead of it)")
	}
	gotRec := got.ScopeLogs().At(0).LogRecords().At(0)
	if _, ok := gotRec.Attributes().Get(testElemKey); ok {
		t.Errorf("%s survived onto a forwarded record; the transform prune would delete it as an operator drop", testElemKey)
	}
	if v, ok := gotRec.Attributes().Get("app.attr"); !ok || v.Str() != "v" {
		t.Error("the strip took a record attribute with it")
	}
	if res, elem := delta(); res != 1 || elem != 1 {
		t.Errorf("strip counters moved (res=%v, elem=%v), want (1, 1): one occurrence each", res, elem)
	}
}

// reservedMetricsPush is one resource carrying the resource key and one metric
// of EVERY pdata type, each with the element key in its Metadata and on one
// data point — every place the transform prune reads its marker.
func reservedMetricsPush() pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr(testResKey, "tenant-b")
	rm.Resource().Attributes().PutStr("service.name", "app")
	ms := rm.ScopeMetrics().AppendEmpty().Metrics()
	var points []pcommon.Map
	g := ms.AppendEmpty()
	g.SetName("g")
	points = append(points, g.SetEmptyGauge().DataPoints().AppendEmpty().Attributes())
	sm := ms.AppendEmpty()
	sm.SetName("s")
	points = append(points, sm.SetEmptySum().DataPoints().AppendEmpty().Attributes())
	h := ms.AppendEmpty()
	h.SetName("h")
	points = append(points, h.SetEmptyHistogram().DataPoints().AppendEmpty().Attributes())
	eh := ms.AppendEmpty()
	eh.SetName("eh")
	points = append(points, eh.SetEmptyExponentialHistogram().DataPoints().AppendEmpty().Attributes())
	su := ms.AppendEmpty()
	su.SetName("su")
	points = append(points, su.SetEmptySummary().DataPoints().AppendEmpty().Attributes())
	for i := 0; i < ms.Len(); i++ {
		ms.At(i).Metadata().PutBool(testElemKey, true)
	}
	for _, p := range points {
		p.PutBool(testElemKey, true)
		p.PutStr("le", "0.5") // a benign point attribute the strip must not touch
	}
	return md
}

// The metrics seam over gRPC: the marker is gone from the resource, from every
// metric's Metadata and from every data point of all five types, counted once
// per occurrence.
func TestReservedKeysStrippedFromMetricsEverywhere(t *testing.T) {
	exp := &captureExporter{}
	s := NewServer(ServerConfig{
		Enricher:      newEnricher(newMeta(), MetricsResource),
		Exporter:      exp,
		ReservedAttrs: testReserved(),
	})
	g := &metricsGRPC{s: s}
	delta := stripDeltas()

	if _, err := g.Export(context.Background(),
		pmetricotlp.NewExportRequestFromMetrics(reservedMetricsPush())); err != nil {
		t.Fatal(err)
	}
	if len(exp.metrics) != 1 {
		t.Fatalf("forwarded %d payloads, want 1", len(exp.metrics))
	}
	rm := exp.metrics[0].ResourceMetrics().At(0)
	if _, ok := rm.Resource().Attributes().Get(testResKey); ok {
		t.Errorf("%s survived onto the forwarded resource", testResKey)
	}
	ms := rm.ScopeMetrics().At(0).Metrics()
	for i := 0; i < ms.Len(); i++ {
		m := ms.At(i)
		if _, ok := m.Metadata().Get(testElemKey); ok {
			t.Errorf("metric %s Metadata still carries %s", m.Name(), testElemKey)
		}
	}
	assertPoint := func(name string, a pcommon.Map) {
		t.Helper()
		if _, ok := a.Get(testElemKey); ok {
			t.Errorf("a %s data point still carries %s", name, testElemKey)
		}
		if v, ok := a.Get("le"); !ok || v.Str() != "0.5" {
			t.Errorf("the strip took a %s point's own attribute with it", name)
		}
	}
	assertPoint("gauge", ms.At(0).Gauge().DataPoints().At(0).Attributes())
	assertPoint("sum", ms.At(1).Sum().DataPoints().At(0).Attributes())
	assertPoint("histogram", ms.At(2).Histogram().DataPoints().At(0).Attributes())
	assertPoint("exponential histogram", ms.At(3).ExponentialHistogram().DataPoints().At(0).Attributes())
	assertPoint("summary", ms.At(4).Summary().DataPoints().At(0).Attributes())
	if res, elem := delta(); res != 1 || elem != 10 {
		t.Errorf("strip counters moved (res=%v, elem=%v), want (1, 10): five Metadata + five point occurrences", res, elem)
	}
}

// The traces seam, and its ORDER against the receive-path guard: a payload
// RejectTraces refuses is never sanitized (nothing downstream will read it, so
// the strip counter must not move for it), while an accepted push is stripped
// before enrichment like the other signals.
func TestReservedKeysStrippedFromSpansAfterTheRejectGuard(t *testing.T) {
	texp := &captureTraces{}
	s := NewServer(ServerConfig{
		Enricher:      newEnricher(newMeta(), MetricsAuto),
		Traces:        texp,
		RejectTraces:  refuseMarked,
		ReservedAttrs: testReserved(),
	})
	g := &tracesGRPC{s: s}
	delta := stripDeltas()

	refused := tracesWith(map[string]string{"container.id": "cafe01", refuseAttr: "yes", testResKey: "tenant-b"})
	refused.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().PutBool(testElemKey, true)
	_, err := g.Export(context.Background(), ptraceotlp.NewExportRequestFromTraces(refused))
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("refusal code = %v, want InvalidArgument", got)
	}
	if res, elem := delta(); res != 0 || elem != 0 {
		t.Errorf("a REFUSED payload moved the strip counters (res=%v, elem=%v); the guard runs first so a refusal costs nothing", res, elem)
	}

	accepted := tracesWith(map[string]string{"container.id": "cafe01", testResKey: "tenant-b"})
	span := accepted.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	span.Attributes().PutBool(testElemKey, true)
	span.Attributes().PutStr("app.attr", "v")
	if _, err := g.Export(context.Background(), ptraceotlp.NewExportRequestFromTraces(accepted)); err != nil {
		t.Fatal(err)
	}
	if len(texp.traces) != 1 {
		t.Fatalf("forwarded %d payloads, want 1", len(texp.traces))
	}
	rs := texp.traces[0].ResourceSpans().At(0)
	if _, ok := rs.Resource().Attributes().Get(testResKey); ok {
		t.Errorf("%s survived onto the forwarded resource", testResKey)
	}
	if v, ok := rs.Resource().Attributes().Get("k8s.pod.name"); !ok || v.Str() != "web-1" {
		t.Error("the strip broke trace enrichment")
	}
	gotSpan := rs.ScopeSpans().At(0).Spans().At(0)
	if _, ok := gotSpan.Attributes().Get(testElemKey); ok {
		t.Errorf("%s survived onto a forwarded span", testElemKey)
	}
	if v, ok := gotSpan.Attributes().Get("app.attr"); !ok || v.Str() != "v" {
		t.Error("the strip took a span attribute with it")
	}
	if res, elem := delta(); res != 1 || elem != 1 {
		t.Errorf("strip counters moved (res=%v, elem=%v), want (1, 1)", res, elem)
	}
}

// A payload carrying none of the reserved keys walks through clean: nothing
// removed, nothing counted — the counter means "a sender shipped a reserved
// key", never "the walk ran".
func TestCleanPayloadMovesNoStripCounter(t *testing.T) {
	s := NewServer(ServerConfig{ReservedAttrs: testReserved()})
	delta := stripDeltas()

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "app")
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Attributes().PutStr("app.attr", "v")
	s.sanitizeLogs(ld)
	if v, ok := lr.Attributes().Get("app.attr"); !ok || v.Str() != "v" {
		t.Error("a clean record was modified")
	}

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "app")
	dp := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetEmptyGauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("le", "1")
	s.sanitizeMetrics(md)

	td := tracesWith(map[string]string{"service.name": "app"})
	s.sanitizeTraces(td)

	if res, elem := delta(); res != 0 || elem != 0 {
		t.Errorf("a clean payload moved the strip counters (res=%v, elem=%v)", res, elem)
	}
}

// The zero-value config strips NOTHING: only the two application-facing
// receivers wire the lists, and every other construction (the tier's internal
// receiver above all) must forward attribute-identical payloads.
func TestEmptyReservedConfigStripsNothing(t *testing.T) {
	s := NewServer(ServerConfig{})
	delta := stripDeltas()

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr(testResKey, "tenant-b")
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Attributes().PutBool(testElemKey, true)
	s.sanitizeLogs(ld)

	if _, ok := rl.Resource().Attributes().Get(testResKey); !ok {
		t.Error("an unconfigured server stripped a resource attribute")
	}
	if _, ok := lr.Attributes().Get(testElemKey); !ok {
		t.Error("an unconfigured server stripped a record attribute")
	}
	if res, elem := delta(); res != 0 || elem != 0 {
		t.Errorf("an unconfigured server counted strips (res=%v, elem=%v)", res, elem)
	}
}

// The element walk runs per record on the request path of an unauthenticated
// listener; the clean case — every payload from a well-behaved sender — must
// stay free.
func TestSanitizeCleanLogsIsAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector's bookkeeping allocations make the ceiling meaningless")
	}
	// The production shape has BOTH resource lists non-empty — the plumbing
	// marker and the enricher's identity keys — so the clean walk probes each
	// resource twice over.
	ra := testReserved()
	ra.Identity = NewEnricher(Config{}).SenderIdentityStrip()
	s := NewServer(ServerConfig{ReservedAttrs: ra})
	ld := plog.NewLogs()
	for i := 0; i < 2; i++ {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", "app")
		lrs := rl.ScopeLogs().AppendEmpty().LogRecords()
		for j := 0; j < 4; j++ {
			lr := lrs.AppendEmpty()
			lr.Attributes().PutStr("app.attr", "v")
		}
	}
	if got := testing.AllocsPerRun(100, func() { s.sanitizeLogs(ld) }); got != 0 {
		t.Errorf("sanitizing a clean payload allocates %.1f times, want 0", got)
	}
}

// The identity strip fires for HONEST senders on every push — an
// OpenTelemetry-Operator-instrumented pod ships several of these keys on every
// resource — so its throttle must not be consulted unless the Debug line it
// gates would actually be written. logdedupe.Table.Allow is a process-wide
// mutex plus a map lookup, and Table.Len is how "was it consulted?" is
// observable: a consulted table holds the key.
//
// The plumbing sibling (stripReserved) deliberately keeps the opposite order —
// its keys are ones no conformant sender sets, so it essentially never probes.
func TestIdentityStripSkipsTheThrottleWhenDebugIsOff(t *testing.T) {
	ra := ReservedAttrs{Identity: []string{"k8s.pod.name", "k8s.namespace.name"}}

	identity := func() plog.Logs {
		ld := plog.NewLogs()
		a := ld.ResourceLogs().AppendEmpty().Resource().Attributes()
		a.PutStr("k8s.pod.name", "claimed")
		a.PutStr("k8s.namespace.name", "payments")
		return ld
	}

	// Info: the line is discarded, so the throttle must never be touched — and
	// the counter, which is the observable, must still move.
	s := NewServer(ServerConfig{
		ReservedAttrs: ra,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo})),
	})
	before := obs.IngestIdentityStripped.WithLabelValues("k8s.pod.name").Value()
	s.sanitizeLogs(identity())
	if got := s.reservedWarns.Len(); got != 0 {
		t.Errorf("the identity strip consulted its throttle %d time(s) with Debug off", got)
	}
	if got := obs.IngestIdentityStripped.WithLabelValues("k8s.pod.name").Value() - before; got != 1 {
		t.Errorf("kubescrape_ingest_identity_stripped_total moved %v, want 1: the counter is the observable and must not depend on the log level", got)
	}

	// Debug: the throttle is still what keeps one chatty sender from minting a
	// line per push, so it must be consulted then.
	s = NewServer(ServerConfig{
		ReservedAttrs: ra,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	s.sanitizeLogs(identity())
	if got := s.reservedWarns.Len(); got != 2 {
		t.Errorf("the identity strip claimed %d throttle slot(s) with Debug on, want 2", got)
	}
}
