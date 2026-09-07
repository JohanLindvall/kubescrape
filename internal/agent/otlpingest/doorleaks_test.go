package otlpingest

// What an unauthenticated listener gives back, and what it allocates on the way
// to refusing something. Both are the sender's to drive, so both are bounded.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
)

// --- F09-3: the clip must bound EVERY rendering of a sender-supplied header ---

// noteRejected clips the two Content-* headers into its own fields, and then
// logs the error beside them — and the errors this door raises formatted the
// SAME header verbatim, so the bound was walked around by the one attribute
// nobody clipped. The reader's returned error is also what WriteBodyError puts
// in the response body, so an unclipped one is echoed to the sender too.
func TestDoorRefusalErrorsCarryOnlyAClippedHeaderValue(t *testing.T) {
	huge := strings.Repeat("Z", 256*1024)
	cases := []struct {
		name, header, value string
	}{
		{"content type", "Content-Type", "application/x-" + huge},
		{"content encoding", "Content-Encoding", "br-" + huge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
			br := newBodyReader(maxIngestBody, &byteBudget{limit: maxBufferBytes}, log)

			req := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewReader(nil))
			req.Header.Set(tc.header, tc.value)
			_, _, err := br.Read(req)
			if err == nil {
				t.Fatal("the door admitted the request")
			}
			if strings.Contains(err.Error(), strings.Repeat("Z", maxLoggedValueBytes+1)) {
				t.Errorf("the refusal error is %d bytes: it re-renders the sender's header in full, "+
					"which is both the log line the clip exists to bound and the body the sender is sent",
					len(err.Error()))
			}
			// The diagnosis has to survive the bound: a refusal that no longer
			// names the offending value is worse than a long one. (The encoding
			// arm reports the FOLDED token, which is the value it matched on.)
			if !strings.Contains(strings.ToUpper(err.Error()), "ZZZ") {
				t.Errorf("the refusal dropped the value instead of clipping it: %v", err)
			}
			if out := buf.String(); len(out) > 4096 {
				t.Errorf("one refusal produced %d bytes of log", len(out))
			}
		})
	}
}

// --- F09-7: content-coding tokens are case-insensitive (RFC 9110 8.4.1) ---

// A proxy or SDK that title-cases its header values was answered a PERMANENT
// 400 and lost every batch, while the Content-Type check one line above folded
// case for free inside mime.ParseMediaType. `x-gzip` is the same coding under
// its pre-RFC-2616 name.
func TestContentEncodingIsMatchedCaseInsensitively(t *testing.T) {
	exp := &captureExporter{}
	s := NewServer(ServerConfig{Enricher: newEnricher(newMeta(), MetricsAuto), Exporter: exp})
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/logs", s.handleHTTPLogs)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("container.id", "cafe01")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hi")
	raw, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	body := gzipped(t, raw)

	for _, enc := range []string{"gzip", "Gzip", "GZIP", "x-gzip", "X-Gzip", " gzip "} {
		req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/logs", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set("Content-Encoding", enc)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("Content-Encoding: %q answered %d (%s); the token is case-insensitive and the "+
				"refusal is permanent, so the sender drops every batch it ever produces",
				enc, resp.StatusCode, strings.TrimSpace(string(got)))
		}
	}
	if len(exp.logs) != 6 {
		t.Errorf("forwarded %d payloads, want 6", len(exp.logs))
	}

	// The CONTROL: a coding this receiver genuinely does not implement is still
	// refused, so the fold is a fold and not a blanket accept.
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/logs", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "br")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("an unimplemented coding answered %d, want 400", resp.StatusCode)
	}
}

// --- F09-6: a forward failure names no collector ---

type failingExporter struct{ err error }

func (f *failingExporter) ExportLogs(context.Context, plog.Logs) error          { return f.err }
func (f *failingExporter) ExportMetrics(context.Context, pmetric.Metrics) error { return f.err }

// The endpoint URL, the collector's resolved address, its own response body and
// — with routing on — every tenant destination's error were rendered into the
// sender's 503 body and gRPC status, on listeners that authenticate nothing.
// The sender can do exactly one thing with a forward failure, which is honour
// the status; the detail belongs on the operator's side of the door.
func TestForwardFailureTellsTheSenderNothingAboutTheCollector(t *testing.T) {
	// The two shapes that actually reach here: net/http's rendering of a failed
	// POST (endpoint + resolved address) and the collector's own response body.
	secrets := []string{
		"otel-collector.monitoring", "10.96.4.7", "tenant-b-endpoint", "s3cr3t-detail",
	}
	cases := []struct {
		name      string
		err       error
		httpCode  int
		grpcCode  codes.Code
		permanent bool
	}{
		{
			name: "transient dial failure",
			err: errors.New(`Post "https://otel-collector.monitoring:4318/v1/logs": ` +
				`dial tcp 10.96.4.7:4318: connect: connection refused`),
			httpCode: http.StatusServiceUnavailable,
			grpcCode: codes.Unavailable,
		},
		{
			name:      "permanent upstream rejection quoting the collector",
			err:       &otlpexport.HTTPStatusError{Code: 400, Body: "s3cr3t-detail from tenant-b-endpoint"},
			httpCode:  http.StatusBadRequest,
			grpcCode:  codes.InvalidArgument,
			permanent: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
			s := NewServer(ServerConfig{
				Logger:   log,
				Enricher: newEnricher(newMeta(), MetricsAuto),
				Exporter: &failingExporter{err: tc.err},
			})
			mux := http.NewServeMux()
			mux.HandleFunc("POST /v1/logs", s.handleHTTPLogs)
			srv := httptest.NewServer(mux)
			defer srv.Close()

			ld := plog.NewLogs()
			rl := ld.ResourceLogs().AppendEmpty()
			rl.Resource().Attributes().PutStr("container.id", "cafe01")
			rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hi")
			raw, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
			if err != nil {
				t.Fatal(err)
			}

			resp, err := http.Post(srv.URL+"/v1/logs", "application/x-protobuf", bytes.NewReader(raw))
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()

			// The STATUS is unchanged: it is the whole of what the sender needs
			// and the only thing it can act on.
			if resp.StatusCode != tc.httpCode {
				t.Errorf("HTTP status = %d, want %d", resp.StatusCode, tc.httpCode)
			}
			for _, secret := range secrets {
				if strings.Contains(string(body), secret) {
					t.Errorf("the 503/400 body disclosed %q to an unauthenticated sender: %q",
						secret, strings.TrimSpace(string(body)))
				}
			}
			if strings.TrimSpace(string(body)) == "" {
				t.Error("the sender was told nothing at all; the fixed text is a replacement, not a removal")
			}

			// The gRPC arm answers the same way.
			gerr := grpcExportLogs(s, ld)
			if gerr == nil {
				t.Fatal("the gRPC arm did not fail")
			}
			st, ok := status.FromError(gerr)
			if !ok {
				t.Fatalf("a bare error reached the sender: %v", gerr)
			}
			if st.Code() != tc.grpcCode {
				t.Errorf("gRPC code = %s, want %s", st.Code(), tc.grpcCode)
			}
			for _, secret := range secrets {
				if strings.Contains(st.Message(), secret) {
					t.Errorf("the gRPC status disclosed %q: %q", secret, st.Message())
				}
			}

			// And the detail is not lost — it is on the operator's side.
			out := buf.String()
			if !strings.Contains(out, "otel-collector.monitoring") && !strings.Contains(out, "s3cr3t-detail") {
				t.Errorf("the failure detail reached neither the sender nor the log: %s", out)
			}
		})
	}
}

// The one thing the redaction must NOT drop. retryableStatus relays an upstream
// ResourceExhausted only because it carries RetryInfo, and both the OTel SDK and
// the Collector drop a batch on a bare one — so rebuilding the status through
// status.Error, rather than from its proto, would turn a retryable back-pressure
// signal into a permanent-looking one and lose the data.
func TestRedactedForwardStatusKeepsRetryInfo(t *testing.T) {
	upstream := exhaustedStatus("collector at https://otel-collector.monitoring:4317 is overloaded")

	got := redactedForwardStatus(upstream, forwardFailureText(upstream))
	st, ok := status.FromError(got)
	if !ok {
		t.Fatalf("not a status: %v", got)
	}
	if st.Code() != codes.ResourceExhausted {
		t.Errorf("code = %s, want ResourceExhausted (the sender must still see back-pressure)", st.Code())
	}
	if strings.Contains(st.Message(), "otel-collector.monitoring") {
		t.Errorf("the collector's endpoint survived the redaction: %q", st.Message())
	}
	if len(st.Details()) == 0 {
		t.Error("RetryInfo was dropped: without it a conformant sender treats ResourceExhausted as " +
			"permanent and discards the batch, which is worse than the disclosure this redaction fixes")
	}
}

// --- F09-5: the admission hook decides on what the receiver trusts ---

// The ingest: hook was handed the RAW attribute map, one line before the strip
// deleted the sender's identity claim. So an operator policy reading
// k8s.namespace.name gated nothing: any pod in the cluster could declare the
// namespace it wanted to be admitted as, and the receiver then threw the claim
// away — the policy decided on a value nothing had checked and nothing kept.
func TestAdmitHookSeesTheSanitizedResource(t *testing.T) {
	var seen []map[string]any
	enr := newEnricher(newMeta(), MetricsAuto)
	exp := &captureExporter{}
	s := NewServer(ServerConfig{
		Enricher: enr,
		Exporter: exp,
		Admit: func(a pcommon.Map) bool {
			seen = append(seen, a.AsRaw())
			return true
		},
		ReservedAttrs: ReservedAttrs{
			Resource: []string{testResKey},
			Identity: enr.SenderIdentityStrip(),
		},
	})

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	at := rl.Resource().Attributes()
	at.PutStr("container.id", "cafe01") // resolves to namespace "default"
	at.PutStr("k8s.namespace.name", "payments")
	at.PutStr(testResKey, "payments-route")
	at.PutStr("service.name", "attacker-app")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hi")

	if err := grpcExportLogs(s, ld); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 1 {
		t.Fatalf("the hook ran %d times, want 1", len(seen))
	}
	for _, forged := range []string{"k8s.namespace.name", testResKey} {
		if v, ok := seen[0][forged]; ok {
			t.Errorf("the hook was shown %s=%v — the sender's own unverified claim, which the receiver "+
				"deletes one step later, so a policy keyed on it gates nothing", forged, v)
		}
	}
	// It still sees what the sender legitimately owns, which is what a
	// per-sender policy has to key on: the lookup key and the service triple.
	for _, want := range []string{"container.id", "service.name"} {
		if _, ok := seen[0][want]; !ok {
			t.Errorf("the hook lost %q: the strip must not take the attribution input or the sender's "+
				"own name with it", want)
		}
	}
	if len(exp.logs) != 1 {
		t.Fatalf("forwarded %d payloads, want 1", len(exp.logs))
	}
	// And the ordering change did not cost enrichment: the resolved namespace
	// still lands on the forwarded resource.
	if v, ok := exp.logs[0].ResourceLogs().At(0).Resource().Attributes().Get("k8s.namespace.name"); !ok ||
		v.Str() != "default" {
		t.Errorf("k8s.namespace.name = %q on the forwarded payload, want the resolved \"default\"", v.Str())
	}
}

// The other half of the redaction, and the line it is drawn on: this receiver's
// OWN receive-path refusal keeps its words. The tier's loop guard names the
// re-shard marker and nothing else, the sender that trips it is a misconfigured
// kubescrape hop pointed at an application port, and that name is the only thing
// telling an operator which hop to fix — so redacting it would trade a real
// disclosure fix for a real diagnostic loss.
func TestReceivePathRefusalKeepsItsOwnWordsWhereAForwardFailureDoesNot(t *testing.T) {
	const marker = "kubescrape.service_graph.forwarded"
	s := NewServer(ServerConfig{
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Traces:   &captureTraces{},
		RejectTraces: func(context.Context, ptrace.Traces) error {
			return status.Error(codes.InvalidArgument,
				"refusing a payload carrying "+marker+" on an application port")
		},
	})

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("container.id", "cafe01")
	rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("s")

	g := &tracesGRPC{s: s}
	_, err := g.Export(context.Background(), ptraceotlp.NewExportRequestFromTraces(td))
	if err == nil {
		t.Fatal("the guard's refusal did not reach the caller")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("status = %v, want InvalidArgument", err)
	}
	if !strings.Contains(st.Message(), marker) {
		t.Errorf("the refusal %q no longer names the marker that caused it", st.Message())
	}
	if strings.Count(err.Error(), "rpc error:") != 1 {
		t.Errorf("the guard's status was re-wrapped in a status: %q", err)
	}
}
