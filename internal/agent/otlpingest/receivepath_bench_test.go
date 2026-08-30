package otlpingest

// Benchmarks for the RECEIVE path against the payload shapes real senders
// produce, not the hostile ones the guards were sized against. The guards are
// paid on every ordinary push, so their cost on an ordinary push is the number
// that matters; depth_test.go's BenchmarkNestingWalkFlatBody covers the other
// end.

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// sdkResourceAttrs is what an OpenTelemetry-Operator-instrumented pod actually
// ships on every resource: the SDK's own five, the operator's Kubernetes
// injection, and a couple the application sets. Fifteen entries, which is the
// width the strip walks and the enricher merges into.
func sdkResourceAttrs(a pcommon.Map, i int) {
	a.PutStr("service.name", fmt.Sprintf("checkout-%d", i%7))
	a.PutStr("service.version", "1.14.2")
	a.PutStr("service.instance.id", fmt.Sprintf("checkout-%d-7f9c4b8d5-abcde", i%7))
	a.PutStr("telemetry.sdk.name", "opentelemetry")
	a.PutStr("telemetry.sdk.language", "go")
	a.PutStr("telemetry.sdk.version", "1.38.0")
	a.PutStr("telemetry.distro.name", "opentelemetry-operator")
	a.PutStr("k8s.namespace.name", "shop")
	a.PutStr("k8s.pod.name", fmt.Sprintf("checkout-%d-7f9c4b8d5-abcde", i%7))
	a.PutStr("k8s.pod.uid", "pod-uid-1")
	a.PutStr("k8s.container.name", "app")
	a.PutStr("k8s.node.name", "node1")
	a.PutStr("container.id", "cafe01")
	a.PutStr("host.name", "node1")
	a.PutStr("deployment.environment", "production")
}

const sdkLogBody = `level=info ts=2026-08-18T09:31:07.442Z caller=handler.go:214 msg="request completed" method=GET path=/api/v2/cart status=200 duration_ms=17.4 user_agent="Mozilla/5.0"`

// sdkLogs is a batch as an SDK's BatchLogRecordProcessor emits it: one
// resource, a scope per instrumentation library, and up to the default 512
// records, each with a handful of attributes and a ~180-byte line.
func sdkLogs(resources, scopes, records int) plog.Logs {
	ld := plog.NewLogs()
	for r := 0; r < resources; r++ {
		rl := ld.ResourceLogs().AppendEmpty()
		sdkResourceAttrs(rl.Resource().Attributes(), r)
		for s := 0; s < scopes; s++ {
			sl := rl.ScopeLogs().AppendEmpty()
			sl.Scope().SetName(fmt.Sprintf("github.com/acme/shop/internal/lib%d", s))
			sl.Scope().SetVersion("0.4.1")
			for i := 0; i < records; i++ {
				lr := sl.LogRecords().AppendEmpty()
				lr.Body().SetStr(sdkLogBody)
				lr.SetSeverityNumber(plog.SeverityNumberInfo)
				lr.SetSeverityText("INFO")
				lr.SetTimestamp(pcommon.Timestamp(1755500000000000000 + int64(i)))
				lr.SetObservedTimestamp(pcommon.Timestamp(1755500000000000000 + int64(i)))
				a := lr.Attributes()
				a.PutStr("http.request.method", "GET")
				a.PutStr("url.path", "/api/v2/cart")
				a.PutInt("http.response.status_code", 200)
				a.PutStr("thread.name", "worker-3")
			}
		}
	}
	return ld
}

// sdkMetrics is a periodic-reader export: one resource, one scope, metrics with
// a modest label set. 50 metrics x 20 points is a typical service's minute.
func sdkMetrics(resources, metricsN, points int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	for r := 0; r < resources; r++ {
		rm := md.ResourceMetrics().AppendEmpty()
		sdkResourceAttrs(rm.Resource().Attributes(), r)
		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName("github.com/acme/shop/internal/metrics")
		for m := 0; m < metricsN; m++ {
			me := sm.Metrics().AppendEmpty()
			me.SetName(fmt.Sprintf("http.server.request.duration.%d", m))
			me.SetUnit("s")
			me.SetDescription("Duration of inbound HTTP requests")
			dps := me.SetEmptySum().DataPoints()
			for p := 0; p < points; p++ {
				dp := dps.AppendEmpty()
				dp.SetDoubleValue(float64(p))
				dp.SetTimestamp(pcommon.Timestamp(1755500000000000000))
				a := dp.Attributes()
				a.PutStr("http.route", fmt.Sprintf("/api/v2/item/%d", p))
				a.PutStr("http.request.method", "GET")
				a.PutInt("http.response.status_code", 200)
			}
		}
	}
	return md
}

func sdkTraces(resources, spans int) ptrace.Traces {
	td := ptrace.NewTraces()
	for r := 0; r < resources; r++ {
		rs := td.ResourceSpans().AppendEmpty()
		sdkResourceAttrs(rs.Resource().Attributes(), r)
		ss := rs.ScopeSpans().AppendEmpty()
		for i := 0; i < spans; i++ {
			sp := ss.Spans().AppendEmpty()
			sp.SetName("GET /api/v2/cart")
			sp.SetKind(ptrace.SpanKindServer)
			sp.SetTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, byte(i)})
			sp.SetSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, byte(i)})
			a := sp.Attributes()
			a.PutStr("http.request.method", "GET")
			a.PutStr("url.path", "/api/v2/cart")
			a.PutInt("http.response.status_code", 200)
		}
	}
	return td
}

// benchServer is the DaemonSet receiver's shape: the reserved lists main.go
// builds (one plumbing key on resources, one on elements, the enricher's
// identity strip), no rules and no log-metrics — the default config.
func benchServer(tb testing.TB) *Server {
	tb.Helper()
	enr := NewEnricher(Config{Meta: newMeta(), MetricsMode: MetricsAuto})
	return NewServer(ServerConfig{
		Enricher: enr,
		ReservedAttrs: ReservedAttrs{
			Resource: []string{"kubescrape.route"},
			Element:  []string{"kubescrape.drop"},
			Identity: enr.SenderIdentityStrip(),
		},
	})
}

// --- the wire-shape guard, on shapes a real sender produces ---

func BenchmarkNestingWalkRealBody(b *testing.B) {
	for _, c := range []struct {
		name string
		body []byte
	}{
		{"logs-1x1x512", marshalLogs(sdkLogs(1, 1, 512))},
		{"logs-4x8x256", marshalLogs(sdkLogs(4, 8, 256))},
		{"metrics-1x50x20", marshalMetrics(sdkMetrics(1, 50, 20))},
	} {
		b.Run(fmt.Sprintf("%s/%dKiB", c.name, len(c.body)>>10), func(b *testing.B) {
			b.SetBytes(int64(len(c.body)))
			for b.Loop() {
				if err := checkNesting(c.body); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// A body carrying long strings is the shape the walk's prune cannot skip: a
// string over 2*(max-depth) bytes is descended into as if it were a message.
// Log lines routinely run past 200 bytes, so this is the ordinary case, not a
// hostile one.
func BenchmarkNestingWalkLongLines(b *testing.B) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sdkResourceAttrs(rl.Resource().Attributes(), 0)
	sl := rl.ScopeLogs().AppendEmpty()
	line := strings.Repeat(sdkLogBody+" ", 8) // ~1.4 KiB, well past the prune
	for i := 0; i < 512; i++ {
		sl.LogRecords().AppendEmpty().Body().SetStr(line)
	}
	body := marshalLogs(ld)
	b.SetBytes(int64(len(body)))
	b.ReportMetric(float64(len(body))/1024, "KiB")
	for b.Loop() {
		if err := checkNesting(body); err != nil {
			b.Fatal(err)
		}
	}
}

func marshalLogs(ld plog.Logs) []byte {
	buf, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	if err != nil {
		panic(err)
	}
	return buf
}

func marshalMetrics(md pmetric.Metrics) []byte {
	buf, err := pmetricotlp.NewExportRequestFromMetrics(md).MarshalProto()
	if err != nil {
		panic(err)
	}
	return buf
}

// --- the per-resource / per-element sweeps ---

func BenchmarkSanitizeLogs(b *testing.B) {
	s := benchServer(b)
	for b.Loop() {
		b.StopTimer()
		ld := sdkLogs(1, 4, 256)
		b.StartTimer()
		s.sanitizeLogs(ld)
	}
}

// sanitize over an already-stripped payload is the steady state for the
// per-ELEMENT half: a resource is stripped once, but every record and every
// data point is probed on every push whether or not it carries anything.
func BenchmarkSanitizeLogsClean(b *testing.B) {
	s := benchServer(b)
	ld := sdkLogs(1, 4, 256)
	s.sanitizeLogs(ld)
	b.ReportAllocs()
	for b.Loop() {
		s.sanitizeLogs(ld)
	}
}

func BenchmarkSanitizeMetricsClean(b *testing.B) {
	s := benchServer(b)
	md := sdkMetrics(1, 50, 20)
	s.sanitizeMetrics(md)
	b.ReportAllocs()
	for b.Loop() {
		s.sanitizeMetrics(md)
	}
}

func BenchmarkSanitizeTracesClean(b *testing.B) {
	s := benchServer(b)
	td := sdkTraces(1, 512)
	s.sanitizeTraces(td)
	b.ReportAllocs()
	for b.Loop() {
		s.sanitizeTraces(td)
	}
}

// --- the decoded-size estimator ---

func BenchmarkDecodedSize(b *testing.B) {
	ld := sdkLogs(4, 8, 256)
	md := sdkMetrics(4, 50, 20)
	td := sdkTraces(4, 512)
	b.Run("logs", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = decodedLogsSize(ld)
		}
	})
	b.Run("metrics", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = decodedMetricsSize(md)
		}
	})
	b.Run("traces", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = decodedTracesSize(td)
		}
	})
}

// --- enrichment ---

func BenchmarkEnrichLogs(b *testing.B) {
	e := NewEnricher(Config{Meta: newMeta(), MetricsMode: MetricsAuto})
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		ld := sdkLogs(4, 8, 32)
		b.StartTimer()
		e.EnrichLogs(ctx, ld)
	}
}

// --- the log chain, in its DEFAULT (nothing configured) shape and with the
// operator's cost levers on ---

func BenchmarkApplyLogChainOff(b *testing.B) {
	s := benchServer(b)
	ld := sdkLogs(1, 4, 256)
	b.ReportAllocs()
	for b.Loop() {
		if _, fwd := s.applyLogChain(ld); !fwd {
			b.Fatal("payload emptied")
		}
	}
}

// BenchmarkDecodeVsNestingWalk is the proportion that decides whether the
// wire-shape guard is worth optimising at all: what the walk costs against
// what pdata's own decode of the SAME bytes costs, on an ordinary payload.
func BenchmarkDecodeVsNestingWalk(b *testing.B) {
	body := marshalLogs(sdkLogs(4, 8, 256))
	b.Run("walk", func(b *testing.B) {
		b.SetBytes(int64(len(body)))
		for b.Loop() {
			if err := checkNesting(body); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("decode", func(b *testing.B) {
		b.SetBytes(int64(len(body)))
		b.ReportAllocs()
		for b.Loop() {
			req := plogotlp.NewExportRequest()
			if err := req.UnmarshalProto(body); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// The per-RESOURCE half of the strip: fifteen SDK attributes, of which the
// identity list removes about half. Runs once per resource per push and moves
// a counter per removed key.
func BenchmarkStripIdentityResource(b *testing.B) {
	s := benchServer(b)
	keys := s.cfg.ReservedAttrs.Identity
	src := pcommon.NewMap()
	sdkResourceAttrs(src, 0)
	m := pcommon.NewMap()
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		src.CopyTo(m)
		b.StartTimer()
		s.stripIdentity(m, keys)
	}
}

// The DEFAULT ingest log path: -enrich is on, so the chain renders each body
// once and runs logenrich over it, per record. Nothing else is configured (no
// rules, no logMetrics, no logAttributes), which is the shipped shape.
func BenchmarkApplyLogChainEnrich(b *testing.B) {
	enr := NewEnricher(Config{Meta: newMeta(), MetricsMode: MetricsAuto, EnrichLines: true})
	s := NewServer(ServerConfig{Enricher: enr})
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		ld := sdkLogs(1, 4, 256)
		b.StartTimer()
		if _, fwd := s.applyLogChain(ld); !fwd {
			b.Fatal("payload emptied")
		}
	}
}

// chainBody is the size decision every record's body passes through. A STRING
// body is the common shape (an SDK logging a formatted line) and must be free;
// a structured one is estimated by a materialisation-free walk.
func BenchmarkChainBody(b *testing.B) {
	str := plog.NewLogRecord()
	str.Body().SetStr(sdkLogBody)

	structured := plog.NewLogRecord()
	m := structured.Body().SetEmptyMap()
	m.PutStr("msg", "request completed")
	m.PutStr("method", "GET")
	m.PutStr("path", "/api/v2/cart")
	m.PutInt("status", 200)
	m.PutDouble("duration_ms", 17.4)
	nested := m.PutEmptyMap("http")
	nested.PutStr("user_agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
	nested.PutStr("remote_addr", "10.1.2.3")

	b.Run("string", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, ok := chainBody(str); !ok {
				b.Fatal("rejected")
			}
		}
	})
	b.Run("map", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, ok := chainBody(structured); !ok {
				b.Fatal("rejected")
			}
		}
	})
	b.Run("map/estimate-only", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rem := maxChainBodyBytes
			if renderedSizeOver(structured.Body(), &rem, 0) {
				b.Fatal("over budget")
			}
		}
	})
}
