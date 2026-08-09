package main

// The reserved-plumbing strip wiring. otlpingest deliberately does not know
// WHICH keys are reserved (the RejectTraces decoupling rule), so the lists
// live here — and here is therefore where their content and their presence on
// the application-facing receivers must be pinned.

import (
	"bytes"
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/route"
	"github.com/JohanLindvall/kubescrape/internal/agent/transform"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// The lists both application-facing receivers wire (startIngest and the trace
// tier's application ports) must stay minimal and TRUE: the router reads its
// marker from RESOURCES only, the transform prune reads its marker from
// elements — a key on the wrong list either misses the channel it guards or
// strips something nothing reserved.
func TestIngestReservedAttrsNameTheReservedKeys(t *testing.T) {
	ra := ingestReservedAttrs()
	if len(ra.Resource) != 1 || ra.Resource[0] != route.ScriptMarker {
		t.Errorf("Resource = %v, want exactly [%s]: the router honors its marker on resources and nowhere else", ra.Resource, route.ScriptMarker)
	}
	if len(ra.Element) != 1 || ra.Element[0] != transform.DropMarker {
		t.Errorf("Element = %v, want exactly [%s]: the transform prune reads its marker per element", ra.Element, transform.DropMarker)
	}
}

// capIngestOut captures what the ingest server forwards, standing where the
// transform wrapper and router sit in production.
type capIngestOut struct {
	mu   sync.Mutex
	logs []plog.Logs
}

func (c *capIngestOut) ExportLogs(_ context.Context, ld plog.Logs) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logs = append(c.logs, ld)
	return nil
}

func (c *capIngestOut) ExportMetrics(context.Context, pmetric.Metrics) error { return nil }

func (c *capIngestOut) got() []plog.Logs {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]plog.Logs(nil), c.logs...)
}

// startIngest wires ingestReservedAttrs into its ServerConfig: a push carrying
// the REAL reserved keys comes out of the receiver without them, counted under
// their real spellings — before the payload ever reaches the chain where the
// router would have honored the route marker and a transform prune the drop
// marker.
func TestStartIngestStripsReservedPlumbingKeys(t *testing.T) {
	on, grpcAddr, httpAddr := *ingestOn, *ingestGRPC, *ingestHTTP
	defer func() { *ingestOn, *ingestGRPC, *ingestHTTP = on, grpcAddr, httpAddr }()
	*ingestOn, *ingestGRPC, *ingestHTTP = true, "", freeAddr(t)

	ctx, p := testPipelines(t)
	out := &capIngestOut{}
	p.out = out
	p.attrBuilders = &attrs.Builders{}
	if err := p.startIngest(ctx); err != nil {
		t.Fatal(err)
	}

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "app")
	rl.Resource().Attributes().PutStr(route.ScriptMarker, "tenant-b")
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Body().SetStr("hello")
	lr.Attributes().PutBool(transform.DropMarker, true)
	lr.Attributes().PutStr("app.attr", "v")
	body, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}

	routeBefore := obs.IngestReservedStripped.WithLabelValues(route.ScriptMarker).Value()
	dropBefore := obs.IngestReservedStripped.WithLabelValues(transform.DropMarker).Value()

	url := "http://" + *ingestHTTP + "/v1/logs"
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Post(url, "application/x-protobuf", bytes.NewReader(body))
		if err == nil {
			code := resp.StatusCode
			_ = resp.Body.Close()
			if code != http.StatusOK {
				t.Fatalf("status = %d", code)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the ingest listener never came up: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	logs := out.got()
	if len(logs) != 1 {
		t.Fatalf("forwarded %d payloads, want 1", len(logs))
	}
	got := logs[0].ResourceLogs().At(0)
	if v, ok := got.Resource().Attributes().Get(route.ScriptMarker); ok {
		t.Errorf("%s = %q reached the export chain; the router would have steered this payload onto that route", route.ScriptMarker, v.Str())
	}
	gotRec := got.ScopeLogs().At(0).LogRecords().At(0)
	if _, ok := gotRec.Attributes().Get(transform.DropMarker); ok {
		t.Errorf("%s reached the export chain; any active logs script would have pruned the record as an operator drop", transform.DropMarker)
	}
	if v, ok := gotRec.Attributes().Get("app.attr"); !ok || v.Str() != "v" {
		t.Error("the strip took a sender attribute with it")
	}
	if d := obs.IngestReservedStripped.WithLabelValues(route.ScriptMarker).Value() - routeBefore; d != 1 {
		t.Errorf("strip counter for %s moved %v, want 1", route.ScriptMarker, d)
	}
	if d := obs.IngestReservedStripped.WithLabelValues(transform.DropMarker).Value() - dropBefore; d != 1 {
		t.Errorf("strip counter for %s moved %v, want 1", transform.DropMarker, d)
	}
}
