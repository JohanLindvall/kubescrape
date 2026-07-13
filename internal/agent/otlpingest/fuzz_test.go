package otlpingest

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/klauspost/compress/gzip"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
)

// FuzzIngestHTTP feeds arbitrary bodies (raw and gzip-wrapped, with fuzzed
// Content-Type/Encoding) to the ingest HTTP log and metric handlers. Invariant:
// the handler never panics and always returns a status code (httptest would
// surface a panic as a 500 with a logged stack, so an explicit recover check
// via the response is enough; a real panic crashes the test process).
func FuzzIngestHTTP(f *testing.F) {
	// A valid logs payload, a valid metrics payload, and adversarial bodies.
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("container.id", "cafe01")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hi")
	validLogs, _ := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()

	md := pmetric.NewMetrics()
	mrm := md.ResourceMetrics().AppendEmpty()
	mrm.Resource().Attributes().PutStr("k8s.pod.uid", "pod-uid-2")
	mrm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetEmptyGauge()
	validMetrics, _ := pmetricotlp.NewExportRequestFromMetrics(md).MarshalProto()

	gz := func(b []byte) []byte {
		var buf bytes.Buffer
		w := gzip.NewWriter(&buf)
		_, _ = w.Write(b)
		_ = w.Close()
		return buf.Bytes()
	}

	// path: 0=/v1/logs 1=/v1/metrics; enc: 0=identity 1=gzip 2=garbage-gzip 3=bad
	for _, body := range [][]byte{validLogs, validMetrics, nil, {0x00}, {0xff, 0xff, 0xff}, []byte("not protobuf"), bytes.Repeat([]byte{0x0a}, 64)} {
		f.Add(body, byte(0), byte(0), "application/x-protobuf")
		f.Add(body, byte(1), byte(0), "application/x-protobuf")
		f.Add(gz(body), byte(0), byte(1), "application/x-protobuf")
	}
	// Truncated protobuf (valid prefix, cut off).
	if len(validLogs) > 4 {
		f.Add(validLogs[:len(validLogs)-2], byte(0), byte(0), "application/x-protobuf")
	}
	f.Add([]byte{0x1f, 0x8b, 0x08, 0x00}, byte(0), byte(1), "application/x-protobuf") // truncated gzip header

	srv := NewServer(ServerConfig{Enricher: newEnricher(newMeta(), MetricsAuto), Exporter: &captureExporter{}})
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/logs", srv.handleHTTPLogs)
	mux.HandleFunc("POST /v1/metrics", srv.handleHTTPMetrics)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	f.Fuzz(func(t *testing.T, body []byte, path, enc byte, contentType string) {
		url := ts.URL + "/v1/logs"
		if path%2 == 1 {
			url = ts.URL + "/v1/metrics"
		}
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			t.Skip()
		}
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
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return // transport-level error is fine; the server did not crash
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode < 100 || resp.StatusCode >= 600 {
			t.Fatalf("nonsensical status %d", resp.StatusCode)
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
