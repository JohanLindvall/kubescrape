package otlpingest

// k8s.namespace.name is what internal/agent/route keys TENANCY on, so on a
// listener nothing authenticates it is not an attribute — it is a credential.
// These pin the two halves of the answer: enrichment OWNS the key for a
// resource it resolved, and the receipt-time strip list names the keys an
// application-facing receiver must not let a sender decide at all.

import (
	"bytes"
	"context"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/route"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// routeCapture is one tenant destination.
type routeCapture struct{ logs []plog.Logs }

func (c *routeCapture) ExportLogs(_ context.Context, ld plog.Logs) error {
	c.logs = append(c.logs, ld)
	return nil
}
func (c *routeCapture) ExportMetrics(context.Context, pmetric.Metrics) error { return nil }

// The A/B: the same push, differing only in a sender-set k8s.namespace.name,
// must not change which tenant's endpoint it lands on. Enrichment resolved the
// pod's REAL namespace and wrote it under service.namespace while the router
// keyed on the forgery — one resource, two answers, and the router read the
// sender's.
func TestResolvedNamespaceBeatsASenderSClaimForRouting(t *testing.T) {
	def, victim := &routeCapture{}, &routeCapture{}
	router := route.New(def, []route.Destination{
		{Name: "payments", Namespaces: []string{"payments"}, Exporter: victim},
	})
	s := NewServer(ServerConfig{
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Exporter: router,
	})

	push := func(forge bool) plog.Logs {
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("container.id", "cafe01") // resolves to ns default
		if forge {
			rl.Resource().Attributes().PutStr("k8s.namespace.name", "payments")
		}
		rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("forged audit line")
		return ld
	}

	if err := grpcExportLogs(s, push(true)); err != nil {
		t.Fatal(err)
	}
	if len(victim.logs) != 0 {
		res := victim.logs[0].ResourceLogs().At(0).Resource().Attributes()
		t.Fatalf("a sender in namespace default reached the payments route by declaring "+
			"k8s.namespace.name=payments; the resource it delivered was %v", res.AsRaw())
	}
	if len(def.logs) != 1 {
		t.Fatalf("default chain got %d payloads, want 1", len(def.logs))
	}
	got := def.logs[0].ResourceLogs().At(0).Resource().Attributes()
	if v, _ := got.Get("k8s.namespace.name"); v.Str() != "default" {
		t.Errorf("k8s.namespace.name = %q, want the resolved default", v.Str())
	}
	if v, _ := got.Get("service.namespace"); v.Str() != "default" {
		t.Errorf("service.namespace = %q: the receiver's two spellings of the same fact must agree",
			v.Str())
	}

	// CONTROL: the unforged push routes identically, so the fix is a
	// correction and not a blanket refusal.
	if err := grpcExportLogs(s, push(false)); err != nil {
		t.Fatal(err)
	}
	if len(def.logs) != 2 || len(victim.logs) != 0 {
		t.Errorf("control push: default=%d victim=%d, want 2 and 0", len(def.logs), len(victim.logs))
	}
}

// The strip list an application-facing receiver wires. It has to name the
// routing key and its identity siblings, and it must NOT name the keys the
// attribution is made by — the strip runs before enrichment, so stripping those
// turns the receiver into one that resolves nothing.
func TestSenderIdentityStripNamesTheForgeableKeysAndNotTheLookupKeys(t *testing.T) {
	e := NewEnricher(Config{Meta: newMeta()})
	strip := e.SenderIdentityStrip()

	for _, k := range []string{
		"k8s.namespace.name", "k8s.pod.name", "k8s.pod.ip",
		"k8s.container.name", "k8s.node.name", "container.name",
	} {
		if !slices.Contains(strip, k) {
			t.Errorf("%q is not stripped: a sender may still declare it", k)
		}
	}
	// The OTLP service triple is the sender's to name. service.name is
	// sender-controlled by design, so a sender that wants to masquerade as
	// another workload's series just declares that workload's service.name —
	// deleting the two dimensions beside it stops no attack and costs an honest
	// plain-SDK sender (no k8s attributes, nothing to resolve BY) half of its
	// job+instance pair permanently.
	for _, k := range []string{"service.namespace", "service.instance.id"} {
		if slices.Contains(strip, k) {
			t.Errorf("%q is stripped: nothing in kubescrape reads it, an attacker walks around it via "+
				"service.name, and an unresolvable sender loses it outright", k)
		}
	}
	for _, k := range append(slices.Clone(DefaultContainerIDKeys), DefaultPodUIDKeys...) {
		if slices.Contains(strip, k) {
			t.Errorf("%q is stripped, and it is the key enrichment resolves BY: the receiver would "+
				"attribute nothing at all", k)
		}
	}
	if slices.Contains(strip, "service.name") {
		t.Error("service.name is stripped: naming its own service is the sender's business")
	}
	// Every reserved-identity key is accounted for: stripped, or excluded
	// because it is a lookup key, or excluded because it is sender-controlled.
	// Nothing may fall between the three — the strip is derived from
	// attrs.ReservedIdentityKeys, so a new reserved key lands here by default
	// and an exemption has to be argued into one of the two lists.
	lookups := append(slices.Clone(DefaultContainerIDKeys), DefaultPodUIDKeys...)
	for _, k := range attrs.ReservedIdentityKeys() {
		if slices.Contains(strip, k) || slices.Contains(lookups, k) ||
			k == "service.namespace" || k == "service.instance.id" {
			continue
		}
		t.Errorf("%q is neither stripped, nor a lookup key, nor sender-controlled", k)
	}
}

// A custom -ingest-container-id-keys moves which keys are the lookup input, and
// the strip list has to follow it — otherwise the operator who renames the key
// gets a receiver that strips its own attribution input.
func TestSenderIdentityStripFollowsTheConfiguredLookupKeys(t *testing.T) {
	e := NewEnricher(Config{Meta: newMeta(), ContainerIDKeys: []string{"k8s.container.name"}})
	strip := e.SenderIdentityStrip()
	if slices.Contains(strip, "k8s.container.name") {
		t.Error("the configured lookup key is stripped")
	}
	if !slices.Contains(strip, "container.id") {
		t.Error("container.id is no longer a lookup key here and must be stripped")
	}
}

// The behaviour behind the list, on the path a plain-SDK sender actually takes:
// nothing resolves (no container id, no pod uid, no peer-IP fallback), so the
// strip is the only thing that touches the resource. The routing key must be
// gone and the sender's own service triple must survive whole — an application
// pointing an SDK at the trace tier's Service names itself with exactly those
// three keys, and losing service.instance.id costs it half of the job+instance
// pair for good.
func TestUnresolvableSenderKeepsItsServiceTripleAndLosesTheRoutingKey(t *testing.T) {
	e := NewEnricher(Config{Meta: newMeta()})
	s := NewServer(ServerConfig{
		Enricher:      e,
		ReservedAttrs: ReservedAttrs{Identity: e.SenderIdentityStrip()},
	})

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	at := rl.Resource().Attributes()
	at.PutStr("service.name", "checkout")
	at.PutStr("service.namespace", "shop")
	at.PutStr("service.instance.id", "checkout-7c9f-abcde")
	at.PutStr("k8s.namespace.name", "another-tenant")
	at.PutStr("k8s.pod.name", "someone-elses-pod")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hello")
	s.sanitizeLogs(ld)

	for _, keep := range []string{"service.name", "service.namespace", "service.instance.id"} {
		if _, ok := at.Get(keep); !ok {
			t.Errorf("%q was stripped from a sender nothing here can resolve, so nothing will put it "+
				"back: the sender names ITSELF with it and kubescrape reads none of it", keep)
		}
	}
	for _, gone := range []string{"k8s.namespace.name", "k8s.pod.name"} {
		if v, ok := at.Get(gone); ok {
			t.Errorf("%s = %q survived: routing cannot tell that claim from a resolved namespace", gone, v.Str())
		}
	}
}

// The identity strip and the plumbing strip are two DIFFERENT events, and
// conflating them is what made a healthy cluster look sick: the identity keys
// fire for every conformant sender (an OpenTelemetry-Operator-instrumented pod
// ships five of them per push), so sharing the plumbing counter climbs forever
// under a help text that says "reserved for kubescrape's own plumbing", and
// sharing the Warn accuses honest senders once a minute per key.
func TestIdentityStripIsReportedApartFromThePlumbingMarkers(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	const idKey = "k8s.namespace.name"
	s := NewServer(ServerConfig{
		Logger:        log,
		ReservedAttrs: ReservedAttrs{Resource: []string{testResKey}, Identity: []string{idKey}},
	})

	res0 := obs.IngestReservedStripped.WithLabelValues(idKey).Value()
	id0 := obs.IngestIdentityStripped.WithLabelValues(idKey).Value()

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr(idKey, "another-tenant")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hello")
	s.sanitizeLogs(ld)

	if d := obs.IngestIdentityStripped.WithLabelValues(idKey).Value() - id0; d != 1 {
		t.Errorf("kubescrape_ingest_identity_stripped_total{key=%q} moved %v, want 1", idKey, d)
	}
	if d := obs.IngestReservedStripped.WithLabelValues(idKey).Value() - res0; d != 0 {
		t.Errorf("the identity strip moved kubescrape_ingest_reserved_stripped_total{key=%q} by %v: "+
			"that series' help says these keys are kubescrape's own plumbing, and an operator alerting "+
			"on it would page on every well-behaved sender in the cluster", idKey, d)
	}
	out := buf.String()
	if strings.Contains(out, "level=WARN") {
		t.Errorf("stripping a sender's own identity logged a WARN: %s", out)
	}
	if strings.Contains(out, "plumbing") {
		t.Errorf("the log line calls a sender's own identity attribute kubescrape plumbing: %s", out)
	}
	if !strings.Contains(out, "level=DEBUG") || !strings.Contains(out, idKey) {
		t.Errorf("the strip went unreported at debug level: %s", out)
	}
}
