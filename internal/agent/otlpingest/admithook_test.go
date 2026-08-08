package otlpingest

// The ingest: admission hook (ServerConfig.Admit) on the TRACES arm. The
// contract covers all three signals; the trace-push seam is the one the trace
// tier's application listeners wire (cmd/kubescrape-agent/servicegraph.go),
// and it had no test at all — admitTraces existed, nothing exercised it.

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// admitAllButBanned is the test's per-sender policy: reject any resource whose
// team attribute is "banned", admit everything else (including no attribute).
func admitAllButBanned(attrs pcommon.Map) bool {
	v, ok := attrs.Get("team")
	return !ok || v.Str() != "banned"
}

// tracesTeams builds one span per team, each under its own resource.
func tracesTeams(teams ...string) ptrace.Traces {
	td := ptrace.NewTraces()
	for _, team := range teams {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("team", team)
		rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("op-" + team)
	}
	return td
}

// postTraces pushes td over OTLP/HTTP and returns the status code.
func postTraces(t *testing.T, url string, td ptrace.Traces) int {
	t.Helper()
	body, err := ptraceotlp.NewExportRequestFromTraces(td).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(url+"/v1/traces", "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode
}

// A rejected resource is removed pre-forward and counted; the admitted one
// still ships. An ALL-rejected push acks (200) without a forward.
func TestAdmitTracesHTTP(t *testing.T) {
	texp := &captureTraces{}
	s := NewServer(ServerConfig{
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Traces:   texp,
		Admit:    admitAllButBanned,
	})
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/traces", s.handleHTTPTraces)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	before := obs.IngestAdmissionRejected.Value()

	// One banned resource of two: the forward sees exactly the survivor.
	if code := postTraces(t, srv.URL, tracesTeams("banned", "good")); code != http.StatusOK {
		t.Fatalf("status = %d", code)
	}
	if len(texp.traces) != 1 {
		t.Fatalf("forwarded pushes = %d, want 1", len(texp.traces))
	}
	got := texp.traces[0].ResourceSpans()
	if got.Len() != 1 {
		t.Fatalf("forwarded resources = %d, want 1", got.Len())
	}
	if v, ok := got.At(0).Resource().Attributes().Get("team"); !ok || v.Str() != "good" {
		t.Fatalf("wrong survivor: %v", got.At(0).Resource().Attributes().AsRaw())
	}
	if moved := obs.IngestAdmissionRejected.Value() - before; moved != 1 {
		t.Fatalf("IngestAdmissionRejected moved %v, want 1", moved)
	}

	// Every resource rejected: acked without a send.
	if code := postTraces(t, srv.URL, tracesTeams("banned", "banned")); code != http.StatusOK {
		t.Fatalf("all-rejected status = %d, want 200 (acked)", code)
	}
	if len(texp.traces) != 1 {
		t.Fatalf("an all-rejected push was forwarded: %d pushes", len(texp.traces))
	}
	if moved := obs.IngestAdmissionRejected.Value() - before; moved != 3 {
		t.Fatalf("IngestAdmissionRejected moved %v, want 3", moved)
	}
}

// The gRPC arm runs the same admitTraces (after the loop guard, before
// enrichment); exercised at the handler level like the HTTP arm.
func TestAdmitTracesGRPC(t *testing.T) {
	texp := &captureTraces{}
	s := NewServer(ServerConfig{
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Traces:   texp,
		Admit:    admitAllButBanned,
	})
	g := &tracesGRPC{s: s}

	before := obs.IngestAdmissionRejected.Value()
	if _, err := g.Export(context.Background(),
		ptraceotlp.NewExportRequestFromTraces(tracesTeams("banned", "good"))); err != nil {
		t.Fatal(err)
	}
	if len(texp.traces) != 1 || texp.traces[0].ResourceSpans().Len() != 1 {
		t.Fatalf("forward after admission: %d pushes", len(texp.traces))
	}

	// All rejected: the RPC still acks, nothing forwarded.
	if _, err := g.Export(context.Background(),
		ptraceotlp.NewExportRequestFromTraces(tracesTeams("banned"))); err != nil {
		t.Fatalf("all-rejected push must ack, got %v", err)
	}
	if len(texp.traces) != 1 {
		t.Fatalf("an all-rejected push was forwarded: %d pushes", len(texp.traces))
	}
	if moved := obs.IngestAdmissionRejected.Value() - before; moved != 2 {
		t.Fatalf("IngestAdmissionRejected moved %v, want 2", moved)
	}
}
