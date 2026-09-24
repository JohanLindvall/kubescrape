package otlpingest

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/klauspost/compress/gzip"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
)

// discardSink accepts every signal and keeps nothing: a fuzz run pushes
// millions of payloads, and a capturing exporter would grow without bound.
type discardSink struct{}

func (discardSink) ExportLogs(context.Context, plog.Logs) error          { return nil }
func (discardSink) ExportMetrics(context.Context, pmetric.Metrics) error { return nil }
func (discardSink) ExportTraces(context.Context, ptrace.Traces) error    { return nil }

// fuzzServer is a receiver wired the way cmd/kubescrape-agent wires the
// DaemonSet's (and the trace tier's) unauthenticated listeners — the reserved
// strips, an admission hook, logs.rules, a log-metrics set, a logAttributes
// lift, a traces exporter and the peer-IP fallback — so a fuzzed body reaches
// every stage of every signal's forward, not just the decode.
func fuzzServer(f *testing.F, mode MetricsMode) *Server {
	rules, err := logline.NewLineFilter([]logline.LineRule{
		{Action: "drop", Match: []string{"__severity__=debug"}},
		{Action: "drop", Match: []string{"k8s.namespace.name=kube-system"}},
		{Action: "keep", MatchRegexp: []string{"__line__=.*"}, Sample: 0.5},
	})
	if err != nil {
		f.Fatal(err)
	}
	set, err := metrics.NewDynamicMetricSet([]metrics.Dynamic{{
		Name: "fuzz_lines", Type: "counter", Value: "1", Labels: []string{"level=$level", "svc=$service.name"},
	}})
	if err != nil {
		f.Fatal(err)
	}
	lift, err := logattrs.New(&logattrs.Config{Rules: []logattrs.Rule{
		{Key: "lvl", Attribute: "level", Target: logattrs.TargetLog},
		{Key: "svc", Attribute: "service.name", Target: logattrs.TargetResource},
	}})
	if err != nil {
		f.Fatal(err)
	}
	enr := NewEnricher(Config{Meta: newMeta(), MetricsMode: mode, PeerIPFallback: true})
	return NewServer(ServerConfig{
		Enricher:    enr,
		EnrichLines: true,
		Exporter:    discardSink{},
		Traces:      discardSink{},
		ReservedAttrs: ReservedAttrs{
			Resource: []string{testResKey},
			Element:  []string{testElemKey},
			Identity: enr.SenderIdentityStrip(),
		},
		Admit: func(a pcommon.Map) bool {
			v, ok := a.Get("team")
			return !ok || v.Str() != "banned"
		},
		Rules:      rules,
		LogMetrics: set,
		LogAttrs:   lift,
		Logger:     slog.New(slog.DiscardHandler),
	})
}

// FuzzIngestHTTP feeds arbitrary bodies (raw and gzip-wrapped, with fuzzed
// Content-Type/Encoding) to the ingest HTTP log, metric and trace handlers.
// Invariant: the handler never panics and answers one of the statuses its
// contract names.
//
// The handlers are called IN-PROCESS, never through a listener, and that is
// load-bearing: net/http's server RECOVERS a handler panic (it logs the stack
// and closes the connection), so a client-side harness saw only an EOF and read
// it as "the server did not crash" — the one invariant this fuzz states could
// not fail. Called directly, a panic unwinds into the fuzz goroutine and fails
// the input. It matters beyond the test: AGENTS.md records that the same
// forward code runs on the gRPC arm, where grpc-go recovers nothing and a
// panic takes the whole agent down.
func FuzzIngestHTTP(f *testing.F) {
	// A valid payload per signal, and adversarial bodies.
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("container.id", "cafe01")
	rl.Resource().Attributes().PutStr(testResKey, "r")
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Body().SetStr(`{"lvl":"debug","svc":"x","msg":"hi"}`)
	lr.Attributes().PutStr(testElemKey, "1")
	validLogs, _ := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()

	md := pmetric.NewMetrics()
	mrm := md.ResourceMetrics().AppendEmpty()
	mrm.Resource().Attributes().PutStr("k8s.pod.uid", "pod-uid-2")
	mrm.Resource().Attributes().PutStr("team", "banned")
	g := mrm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	dp := g.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetIntValue(1)
	dp.Attributes().PutStr("container.id", "cafe01")
	mrm.ScopeMetrics().At(0).Metrics().AppendEmpty().SetEmptySum() // point-less: pruned at receipt
	validMetrics, _ := pmetricotlp.NewExportRequestFromMetrics(md).MarshalProto()

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("container.id", "cafe01")
	rs.Resource().Attributes().PutStr("k8s.namespace.name", "forged")
	rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("op")
	validTraces, _ := ptraceotlp.NewExportRequestFromTraces(td).MarshalProto()

	gz := func(b []byte) []byte {
		var buf bytes.Buffer
		w := gzip.NewWriter(&buf)
		_, _ = w.Write(b)
		_ = w.Close()
		return buf.Bytes()
	}

	// path: 0=/v1/logs 1=/v1/metrics 2=/v1/traces (and the byte also picks the
	// metrics mode); enc: 0=identity 1=gzip 2=garbage-gzip 3=bad
	for _, body := range [][]byte{validLogs, validMetrics, validTraces, nil, {0x00}, {0xff, 0xff, 0xff}, []byte("not protobuf"), bytes.Repeat([]byte{0x0a}, 64)} {
		for path := range byte(3) {
			f.Add(body, path, byte(0), "application/x-protobuf")
		}
		f.Add(gz(body), byte(0), byte(1), "application/x-protobuf")
	}
	// Truncated protobuf (valid prefix, cut off).
	if len(validLogs) > 4 {
		f.Add(validLogs[:len(validLogs)-2], byte(0), byte(0), "application/x-protobuf")
	}
	f.Add([]byte{0x1f, 0x8b, 0x08, 0x00}, byte(0), byte(1), "application/x-protobuf") // truncated gzip header

	// Built ONCE, outside the fuzz body: a Server per metrics mode, picked by
	// the path byte, so the split path is fuzzed as well as the resource one.
	modes := []MetricsMode{MetricsAuto, MetricsResource, MetricsDatapoint}
	servers := make([]*Server, len(modes))
	for i, m := range modes {
		servers[i] = fuzzServer(f, m)
	}

	f.Fuzz(func(t *testing.T, body []byte, path, enc byte, contentType string) {
		srv := servers[int(path/3)%len(servers)]
		target, handler := "/v1/logs", srv.handleHTTPLogs
		switch path % 3 {
		case 1:
			target, handler = "/v1/metrics", srv.handleHTTPMetrics
		case 2:
			target, handler = "/v1/traces", srv.handleHTTPTraces
		}
		req := httptest.NewRequest(http.MethodPost, target, bytes.NewReader(body))
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		switch enc % 4 {
		case 1:
			req.Header.Set("Content-Encoding", "gzip")
		case 2:
			req.Header.Set("Content-Encoding", "gzip") // body may not be gzip → parse error path
		case 3:
			req.Header.Set("Content-Encoding", "br") // unsupported
		}
		rec := httptest.NewRecorder()
		handler(rec, req) // a panic here fails the input: nothing recovers it
		switch rec.Code {
		case http.StatusOK, http.StatusBadRequest, http.StatusRequestEntityTooLarge,
			http.StatusUnsupportedMediaType, http.StatusTooManyRequests, http.StatusServiceUnavailable:
		default:
			t.Fatalf("status %d is not one this handler's contract names", rec.Code)
		}
	})
}

// FuzzEnrichDirect drives the in-process enrich paths with fuzz-built payloads
// (no HTTP), so malformed but structurally valid OTLP resources exercise the
// enricher's resource/data-point walking in every metrics mode. Invariant: no
// panic; forwarding never fails on the capture exporter.
func FuzzEnrichDirect(f *testing.F) {
	f.Add("cafe01", "", "", byte(0))
	f.Add("", "pod-uid-2", "", byte(1))
	f.Add("", "", "10.1.2.3", byte(2))
	f.Add("unknown", "unknown", "1.2.3.4", byte(3))
	f.Add("", "", "", byte(0))

	f.Fuzz(func(t *testing.T, containerID, podUID, peerIP string, mode byte) {
		modes := []MetricsMode{MetricsResource, MetricsDatapoint, MetricsAuto}
		m := modes[int(mode)%len(modes)]
		enr := NewEnricher(Config{Meta: newMeta(), MetricsMode: m, PeerIPFallback: true})
		ctx := withPeerIP(context.Background(), peerIP+":12345")

		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		if containerID != "" {
			rl.Resource().Attributes().PutStr("container.id", containerID)
		}
		if podUID != "" {
			rl.Resource().Attributes().PutStr("k8s.pod.uid", podUID)
		}
		rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("x")
		enr.EnrichLogs(ctx, ld)

		md := pmetric.NewMetrics()
		mrm := md.ResourceMetrics().AppendEmpty()
		if containerID != "" {
			mrm.Resource().Attributes().PutStr("container.id", containerID)
		}
		g := mrm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		dp := g.SetEmptyGauge().DataPoints().AppendEmpty()
		if podUID != "" {
			dp.Attributes().PutStr("k8s.pod.uid", podUID)
		}
		dp.SetIntValue(1)
		out := enr.EnrichMetrics(ctx, md)
		marshaler := &pmetric.ProtoMarshaler{}
		if _, err := marshaler.MarshalMetrics(out); err != nil {
			t.Fatalf("enriched metrics do not marshal: %v", err)
		}
	})
}
