package main

// The reserved-plumbing strip wiring. otlpingest deliberately does not know
// WHICH keys are reserved (the RejectTraces decoupling rule), so the lists
// live here — and here is therefore where their content and their presence on
// the application-facing receivers must be pinned.

import (
	"bytes"
	"context"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/otlpingest"
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
	ra := ingestReservedAttrs(otlpingest.NewEnricher(otlpingest.Config{}))
	if len(ra.Resource) != 1 || ra.Resource[0] != route.ScriptMarker {
		t.Errorf("Resource = %v, want exactly [%s]: the router honors it on resources and nowhere else, "+
			"and this list is the one whose counter and Warn say \"kubescrape's own plumbing\" — a key "+
			"honest senders set does not belong on it", ra.Resource, route.ScriptMarker)
	}
	if len(ra.Element) != 1 || ra.Element[0] != transform.DropMarker {
		t.Errorf("Element = %v, want exactly [%s]: the transform prune reads its marker per element", ra.Element, transform.DropMarker)
	}
}

// The sender's identity CLAIM is the OTHER half of the wiring, and it rides its
// own list: an unauthenticated push that declares another tenant's
// k8s.namespace.name must not reach the router with it. The list is the
// enricher's (reserved identity MINUS this receiver's own lookup keys, minus
// the sender's own service triple) — asking attrs for the whole reserved set
// instead would strip container.id and k8s.pod.uid, i.e. disable enrichment
// entirely, because the strip runs before it.
func TestIngestReservedAttrsStripTheSenderIdentityClaimButNotTheLookupKeys(t *testing.T) {
	enr := otlpingest.NewEnricher(otlpingest.Config{})
	ra := ingestReservedAttrs(enr)
	for _, want := range []string{"k8s.namespace.name", "k8s.pod.name"} {
		if !slices.Contains(ra.Identity, want) {
			t.Errorf("Identity = %v, missing %q: a sender declaring it steers or forges identity on a listener nothing authenticates", ra.Identity, want)
		}
		// It must be on the IDENTITY list and not the plumbing one: the two
		// carry different counters and different log levels, and every
		// conformant SDK ships these keys.
		if slices.Contains(ra.Resource, want) {
			t.Errorf("Resource = %v carries %q, whose counter and Warn describe kubescrape's own plumbing markers", ra.Resource, want)
		}
	}
	for _, lookup := range append(slices.Clone(otlpingest.DefaultContainerIDKeys), otlpingest.DefaultPodUIDKeys...) {
		if slices.Contains(ra.Identity, lookup) {
			t.Errorf("Identity = %v contains the lookup key %q: the strip runs BEFORE enrichment, so stripping it resolves nothing at all", ra.Identity, lookup)
		}
	}
	// The sender's own service triple is DESCRIPTIVE — naming itself is the
	// whole point of a sender, service.name is deliberately sender-controlled,
	// and an unresolvable sender gets no replacement for what is taken.
	for _, keep := range []string{"service.name", "service.namespace", "service.instance.id"} {
		if slices.Contains(ra.Identity, keep) || slices.Contains(ra.Resource, keep) {
			t.Errorf("%q is stripped, and every sender legitimately sets it (Identity = %v)", keep, ra.Identity)
		}
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

// The identity half of the same wiring, end to end: an unauthenticated push
// that CLAIMS another tenant's namespace must not reach the export chain with
// it — internal/agent/route keys tenancy on exactly that attribute, so the
// claim would pick the destination and its headers. Enrichment cannot stand in
// for the strip: this resource names no container id and no pod uid, so
// nothing resolves and there is no correction to apply.
func TestStartIngestStripsAForgedNamespaceClaim(t *testing.T) {
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
	at := rl.Resource().Attributes()
	at.PutStr("service.name", "app") // the service triple is descriptive: it must survive
	at.PutStr("service.namespace", "shop")
	at.PutStr("service.instance.id", "app-7c9f-abcde")
	at.PutStr("k8s.namespace.name", "another-tenant")
	at.PutStr("k8s.pod.name", "someone-elses-pod")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hello")

	nsKey := "k8s.namespace.name"
	idBefore := obs.IngestIdentityStripped.WithLabelValues(nsKey).Value()
	plumbBefore := obs.IngestReservedStripped.WithLabelValues(nsKey).Value()

	postLogs(t, "http://"+*ingestHTTP+"/v1/logs", ld)

	logs := out.got()
	if len(logs) != 1 {
		t.Fatalf("forwarded %d payloads, want 1", len(logs))
	}
	got := logs[0].ResourceLogs().At(0).Resource().Attributes()
	for _, forged := range []string{"k8s.namespace.name", "k8s.pod.name"} {
		if v, ok := got.Get(forged); ok {
			t.Errorf("%s = %q reached the export chain from an unresolvable sender; routing cannot tell that claim from a resolved namespace", forged, v.Str())
		}
	}
	// The sender names ITSELF with the service triple and kubescrape reads none
	// of it; nothing resolved here, so anything taken is gone for good.
	for _, keep := range [][2]string{{"service.name", "app"}, {"service.namespace", "shop"}, {"service.instance.id", "app-7c9f-abcde"}} {
		if v, ok := got.Get(keep[0]); !ok || v.Str() != keep[1] {
			t.Errorf("%s = %q (present=%v): the strip took a key the sender uses to name itself and "+
				"this receiver has no replacement for", keep[0], v.Str(), ok)
		}
	}
	// And the removal is reported as what it is. Every conformant SDK ships
	// these keys, so folding them into the plumbing counter (whose help says
	// "reserved for kubescrape's own plumbing") makes a healthy cluster look
	// like a fleet of misbehaving senders.
	if d := obs.IngestIdentityStripped.WithLabelValues(nsKey).Value() - idBefore; d != 1 {
		t.Errorf("kubescrape_ingest_identity_stripped_total{key=%q} moved %v, want 1", nsKey, d)
	}
	if d := obs.IngestReservedStripped.WithLabelValues(nsKey).Value() - plumbBefore; d != 0 {
		t.Errorf("kubescrape_ingest_reserved_stripped_total{key=%q} moved %v: an identity claim is not a plumbing marker", nsKey, d)
	}
}

// postLogs pushes an OTLP/HTTP protobuf logs payload, retrying until the
// listener is up (the servers bind asynchronously).
func postLogs(t *testing.T, url string, ld plog.Logs) {
	t.Helper()
	body, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Post(url, "application/x-protobuf", bytes.NewReader(body))
		if err == nil {
			code := resp.StatusCode
			_ = resp.Body.Close()
			if code != http.StatusOK {
				t.Fatalf("status = %d", code)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the ingest listener never came up: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
