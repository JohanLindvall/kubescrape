package otlpingest

// What an operator can see when the receiver refuses or degrades a push. These
// listeners are unauthenticated by design and everything here answers the
// sender retryably or forwards the data anyway, so the sender's own telemetry
// looks healthy — which is exactly why the receiver has to say what it did.
//
// The symmetry between the two transports is asserted deliberately: the same
// bound reached over gRPC and over HTTP must count into the same series AND
// produce the same line, or an operator debugging one transport learns nothing
// about the other.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"google.golang.org/grpc"

	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/metaclient"
)

func capturedLogger() (*slog.Logger, func() string) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(h), buf.String
}

func oneLogsPush() plog.Logs {
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("x")
	return ld
}

// The in-flight bound is the receiver's front door. It answered 429 /
// ResourceExhausted, counted kubescrape_ingest_rejected_total, and said
// nothing — so "the agent is shedding my telemetry" was diagnosable only from
// the sender's side, and the flag that fixes it appeared nowhere.
func TestSheddingAtTheInFlightBoundIsReportedOnBothTransports(t *testing.T) {
	for _, transport := range []string{"grpc", "http"} {
		t.Run(transport, func(t *testing.T) {
			log, dump := capturedLogger()
			s := NewServer(ServerConfig{
				Enricher:    newEnricher(newMeta(), MetricsAuto),
				Exporter:    exporterFunc(func(plog.Logs) error { return nil }),
				MaxInFlight: 1,
				Logger:      log,
			})
			// Hold the only slot, so the push under test is refused.
			if !s.acquire() {
				t.Fatal("could not take the only in-flight slot")
			}
			defer s.release()

			before := ingestRejectedTotal()
			beforeInFlight := obs.IngestRejected.WithLabelValues(shedInFlight).Value()
			switch transport {
			case "grpc":
				_, err := s.limitUnary(context.Background(), nil, &grpc.UnaryServerInfo{},
					func(context.Context, any) (any, error) { return nil, nil })
				if err == nil {
					t.Fatal("expected the gRPC arm to shed")
				}
			case "http":
				body, err := plogotlp.NewExportRequestFromLogs(oneLogsPush()).MarshalProto()
				if err != nil {
					t.Fatal(err)
				}
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/x-protobuf")
				req.RemoteAddr = "10.4.5.6:34567"
				s.handleHTTPLogs(rec, req)
				if rec.Code != http.StatusTooManyRequests {
					t.Fatalf("status = %d, want 429", rec.Code)
				}
			}
			if got := ingestRejectedTotal() - before; got != 1 {
				t.Errorf("kubescrape_ingest_rejected_total delta = %v, want 1", got)
			}
			if got := obs.IngestRejected.WithLabelValues(shedInFlight).Value() - beforeInFlight; got != 1 {
				t.Errorf("kubescrape_ingest_rejected_total{reason=%q} delta = %v, want 1: the label must name the bound", shedInFlight, got)
			}
			out := dump()
			if !strings.Contains(out, "shedding pushes at an admission bound") {
				t.Fatalf("no line for a shed push:\n%s", out)
			}
			for _, want := range []string{"reason=" + shedInFlight, "limit=1", "flag=-ingest-max-in-flight"} {
				if !strings.Contains(out, want) {
					t.Errorf("the shed line does not carry %q:\n%s", want, out)
				}
			}
			// The HTTP arm knows who was pushing; the gRPC pre-decode tap does
			// not (see noteShed), but the interceptor does when a peer is set.
			if transport == "http" && !strings.Contains(out, "peer=10.4.5.6:34567") {
				t.Errorf("the HTTP shed line must name the sender:\n%s", out)
			}
		})
	}
}

// A shed is a STATE — a receiver at a bound stays there for as long as the load
// does, and the listener is unauthenticated, so a line per refusal is a log
// volume a stranger chooses.
func TestTheShedLineIsThrottledPerBound(t *testing.T) {
	log, dump := capturedLogger()
	s := NewServer(ServerConfig{
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Exporter: exporterFunc(func(plog.Logs) error { return nil }),
		Logger:   log,
	})
	for i := 0; i < 50; i++ {
		s.noteShed(shedInFlight, "10.0.0.1:1")
	}
	if n := strings.Count(dump(), "shedding pushes"); n != 1 {
		t.Fatalf("want 1 line for 50 sheds, got %d", n)
	}
	// A DIFFERENT bound is a different problem and must not be suppressed by
	// the busy one.
	s.noteShed(shedDecoded, "")
	if n := strings.Count(dump(), "shedding pushes"); n != 2 {
		t.Errorf("a second bound must report even while the first is throttled, got %d lines", n)
	}
}

// The BYTE bounds derive from max(maxBufferBytes, 4 x MaxRecvBytes), so at the
// default 4 MiB receive cap the built-in floor is what binds and
// -ingest-grpc-max-recv-bytes moves neither limit until it is set above
// maxBufferBytes/4. The line named that flag unconditionally, which sends an
// operator to raise a value that cannot change the number printed beside it —
// and the only evidence that it did nothing is the shedding continuing.
func TestTheByteShedLineNamesTheFlagOnlyWhenItMovesTheBudget(t *testing.T) {
	for _, reason := range []string{shedBuffer, shedDecoded} {
		t.Run(reason, func(t *testing.T) {
			log, dump := capturedLogger()
			s := NewServer(ServerConfig{
				Enricher: newEnricher(newMeta(), MetricsAuto),
				Exporter: exporterFunc(func(plog.Logs) error { return nil }),
				Logger:   log,
			})
			s.noteShed(reason, "")
			out := dump()
			if strings.Contains(out, "flag=-ingest-grpc-max-recv-bytes") {
				t.Errorf("the line names a flag that cannot move this budget at the default receive cap:\n%s", out)
			}
			for _, want := range []string{
				"floorBytes=" + strconv.Itoa(maxBufferBytes),
				"recvBytes=" + strconv.Itoa(maxIngestGRPCMessage),
				"built-in floor",
			} {
				if !strings.Contains(out, want) {
					t.Errorf("the line does not carry %q:\n%s", want, out)
				}
			}

			// Raised past maxBufferBytes/4 the flag genuinely IS the knob, and
			// the line must say so.
			log2, dump2 := capturedLogger()
			s2 := NewServer(ServerConfig{
				Enricher:     newEnricher(newMeta(), MetricsAuto),
				Exporter:     exporterFunc(func(plog.Logs) error { return nil }),
				MaxRecvBytes: maxBufferBytes/4 + 1,
				Logger:       log2,
			})
			s2.noteShed(reason, "")
			if out := dump2(); !strings.Contains(out, "flag=-ingest-grpc-max-recv-bytes") {
				t.Errorf("a raised receive cap DOES size the budget; the line must name the flag:\n%s", out)
			}
		})
	}
}

// The line-derived chain's abuse bounds forward the data and skip the
// processing, so nothing 429s and the sender sees success — while a log-derived
// metric silently under-counts and a drop rule silently fails to fire.
func TestChainSkipsAreExplainedNotJustCounted(t *testing.T) {
	log, dump := capturedLogger()
	set := newCountingSet(t)
	s := NewServer(ServerConfig{
		Enricher:   newEnricher(newMeta(), MetricsAuto),
		Exporter:   exporterFunc(func(plog.Logs) error { return nil }),
		LogMetrics: set,
		Logger:     log,
	})

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	// One resource wider than the observation cap, carrying one body over the
	// render cap: two different bounds in one push.
	for i := 0; i < maxObservedResourceAttrs+1; i++ {
		rl.Resource().Attributes().PutStr("k"+string(rune('a'+i%26))+string(rune('a'+i/26)), "v")
	}
	lrs := rl.ScopeLogs().AppendEmpty().LogRecords()
	lrs.AppendEmpty().Body().SetStr(strings.Repeat("x", maxChainBodyBytes+1))

	beforeWide := obs.IngestChainSkipped.WithLabelValues(chainSkipTooWide).Value()
	beforeBody := obs.IngestChainSkipped.WithLabelValues(chainSkipBody).Value()
	if _, forward := s.applyLogChain(ld); !forward {
		t.Fatal("skipped records must still be forwarded")
	}
	if obs.IngestChainSkipped.WithLabelValues(chainSkipTooWide).Value() <= beforeWide {
		t.Error("kubescrape_ingest_log_chain_skipped_total{resource_too_wide} did not move")
	}
	if obs.IngestChainSkipped.WithLabelValues(chainSkipBody).Value() <= beforeBody {
		t.Error("kubescrape_ingest_log_chain_skipped_total{body_too_large} did not move")
	}
	out := dump()
	for _, want := range []string{"reason=" + chainSkipTooWide, "reason=" + chainSkipBody, "maxAttributes=", "maxBytes="} {
		if !strings.Contains(out, want) {
			t.Errorf("the skip lines do not carry %q:\n%s", want, out)
		}
	}
}

// A metadata service that cannot be reached costs ATTRIBUTION on every pushed
// resource, and the counter that moves (unresolved) says exactly the same thing
// it says for a stale container id — the ordinary case. Only the line separates
// them, and only a non-404 gets one.
func TestAnUnreachableMetadataServiceIsWarnedButAMissIsNot(t *testing.T) {
	t.Run("unreachable", func(t *testing.T) {
		log, dump := capturedLogger()
		e := NewEnricher(Config{Meta: brokenMeta{}, Logger: log})
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("container.id", "cafe01")
		rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("x")
		e.EnrichLogs(context.Background(), ld)

		out := dump()
		if !strings.Contains(out, "the metadata service is not answering lookups") {
			t.Errorf("a broken metadata service must be reported:\n%s", out)
		}
	})
	t.Run("unknown id", func(t *testing.T) {
		log, dump := capturedLogger()
		e := NewEnricher(Config{Meta: notFoundMeta{}, Logger: log})
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("container.id", "gone")
		rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("x")
		e.EnrichLogs(context.Background(), ld)

		if strings.Contains(dump(), "the metadata service is not answering") {
			t.Errorf("an id the store simply does not know is ordinary and must not read as an outage:\n%s", dump())
		}
	})
}

// brokenMeta is a metadata service that cannot be reached at all.
type brokenMeta struct{}

func (brokenMeta) Container(context.Context, string, time.Duration) (*kubemeta.ContainerMetadata, error) {
	return nil, errors.New("dial tcp 10.0.0.9:8080: connect: connection refused")
}
func (brokenMeta) PodByUID(context.Context, string) (*kubemeta.Pod, error) {
	return nil, errors.New("dial tcp 10.0.0.9:8080: connect: connection refused")
}
func (brokenMeta) PodByIP(context.Context, string) (*kubemeta.Pod, error) {
	return nil, errors.New("dial tcp 10.0.0.9:8080: connect: connection refused")
}

// notFoundMeta answers the way the metadata service does for an object it does
// not hold: a typed 404, which metaclient.IsNotFound recognises.
type notFoundMeta struct{}

func (notFoundMeta) Container(context.Context, string, time.Duration) (*kubemeta.ContainerMetadata, error) {
	return nil, notFound()
}
func (notFoundMeta) PodByUID(context.Context, string) (*kubemeta.Pod, error) { return nil, notFound() }
func (notFoundMeta) PodByIP(context.Context, string) (*kubemeta.Pod, error)  { return nil, notFound() }

func notFound() error {
	return &metaclient.StatusError{Code: http.StatusNotFound, URL: "/v1/containers/gone", Body: "not found"}
}

// newCountingSet is the smallest logMetrics set that makes the chain observe.
func newCountingSet(t *testing.T) *metrics.DynamicMetricSet {
	t.Helper()
	set, err := metrics.NewDynamicMetricSet([]metrics.Dynamic{{
		Name: "ingested_lines", Type: "counter", Value: "1",
	}})
	if err != nil {
		t.Fatal(err)
	}
	return set
}

// Past the splitter's caps the remaining objects' points fold onto the
// SENDER's resource, unenriched: the series keep flowing and describe one
// object while labelled with another. The counter alone reads as a small
// number beside a large one; the line says which bound bound and how far past
// it the sender is.
func TestTheSplitCapExplainsItself(t *testing.T) {
	log, dump := capturedLogger()
	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	const n = maxSplitGroups + 5
	for i := 0; i < n; i++ {
		m := sm.Metrics().AppendEmpty()
		m.SetName("m")
		dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetIntValue(1)
		dp.Attributes().PutStr("k8s.pod.uid", fmt.Sprintf("uid-%d", i))
	}
	before := obs.Ingested.WithLabelValues("split_capped").Value()
	NewEnricher(Config{Meta: newMeta(), MetricsMode: MetricsDatapoint, Logger: log}).
		EnrichMetrics(context.Background(), md)

	if obs.Ingested.WithLabelValues("split_capped").Value() <= before {
		t.Error("kubescrape_ingest_resources_total{split_capped} did not move")
	}
	out := dump()
	if !strings.Contains(out, "more objects than one payload may split into") {
		t.Fatalf("the split cap must explain itself:\n%s", out)
	}
	if !strings.Contains(out, "maxObjects=") {
		t.Errorf("the line must name the bound:\n%s", out)
	}
	// One line for thousands of refused objects: a push naming 12,000 pods must
	// not produce 10,000 lines.
	if n := strings.Count(out, "more objects than one payload"); n != 1 {
		t.Errorf("want the split-cap line throttled to one per push, got %d", n)
	}
}
