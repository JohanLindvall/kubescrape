package otlpingest

// A REPEATED attribute key must not survive the receipt-time strip.
//
// An OTLP attribute list is a protobuf `repeated KeyValue`, not a map: nothing
// on the wire forbids the same key twice and pdata's decoder keeps both copies.
// pcommon.Map.Remove, however, returns at the FIRST match — so the strip used to
// leave a survivor, and every consumer downstream reads through Get, which is
// FIRST-wins. Sending the attribute twice therefore walked straight through the
// one guard that stands between an unauthenticated listener and another tenant's
// endpoint, while the counter and the warn fired once and made it look handled.
//
// Unlike reserved_test.go, which deliberately uses TEST-LOCAL key names because
// which keys are reserved is the caller's wiring, this file reaches for the REAL
// spellings (route.ScriptMarker, k8s.namespace.name) and the REAL router — the
// same reason namespaceforgery_test.go does. The property is not "the map lost a
// key" but "the forged route was not taken", and only the thing that reads the
// attribute can say so.

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"

	"github.com/JohanLindvall/kubescrape/internal/agent/route"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// forgedNamespace is the tenant an attacker wants its payload routed to;
// victimRoute is the route an operator configured for that tenant.
const (
	forgedNamespace = "payments"
	victimRoute     = "payments-route"
)

// dupLogs builds a one-resource, one-record logs payload from an ORDERED list of
// key/value pairs, so a key may appear more than once.
//
// It goes through OTLP/JSON because that is the only encoder reachable from here
// that can EXPRESS a duplicate — every pcommon.Map setter resolves through Get
// and overwrites — and OTLP/JSON models attributes as the array they are on the
// wire. The result round-trips through protobuf so the fixture is exactly what a
// hand-rolled sender would put on the wire, duplicates and all.
func dupLogs(t *testing.T, resAttrs, recAttrs [][2]string) plog.Logs {
	t.Helper()
	list := func(pairs [][2]string) string {
		var b strings.Builder
		for i, kv := range pairs {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString(`{"key":"` + kv[0] + `","value":{"stringValue":"` + kv[1] + `"}}`)
		}
		return b.String()
	}
	doc := `{"resourceLogs":[{"resource":{"attributes":[` + list(resAttrs) +
		`]},"scopeLogs":[{"logRecords":[{"body":{"stringValue":"hi"},"attributes":[` +
		list(recAttrs) + `]}]}]}]}`

	req := plogotlp.NewExportRequest()
	if err := req.UnmarshalJSON([]byte(doc)); err != nil {
		t.Fatalf("building the duplicate-key fixture: %v", err)
	}
	raw, err := req.MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	back := plogotlp.NewExportRequest()
	if err := back.UnmarshalProto(raw); err != nil {
		t.Fatal(err)
	}
	ld := back.Logs()
	if got := ld.ResourceLogs().At(0).Resource().Attributes().Len(); got != len(resAttrs) {
		t.Fatalf("the fixture lost a duplicate in transit: %d resource attributes, want %d", got, len(resAttrs))
	}
	return ld
}

// countKey reports how many entries of m are keyed k. Get cannot: it is
// first-wins, which is exactly why a survivor was invisible.
func countKey(m pcommon.Map, k string) int {
	n := 0
	for key := range m.All() {
		if key == k {
			n++
		}
	}
	return n
}

// The end-to-end property, on the transport an attacker would use: a sender that
// repeats its forged identity and the router's own script marker gets NEITHER
// honoured, and the payload the router then sees takes the default chain.
//
// Before the fix the router assertion below read payments-route=1 for a pod that
// had done nothing cleverer than write k8s.namespace.name twice.
func TestRepeatedForgedAttributesDoNotSurviveTheStripOrSteerTheRouter(t *testing.T) {
	exp := &captureExporter{}
	enr := newEnricher(newMeta(), MetricsAuto)
	s := NewServer(ServerConfig{
		Enricher: enr,
		Exporter: exp,
		// The shape cmd/kubescrape-agent wires onto the DaemonSet's
		// unauthenticated listeners (ingestReservedAttrs); the element key stays
		// test-local, per this package's rule about transform's spelling.
		ReservedAttrs: ReservedAttrs{
			Resource: []string{route.ScriptMarker},
			Element:  []string{testElemKey},
			Identity: enr.SenderIdentityStrip(),
		},
	})
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/logs", s.handleHTTPLogs)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	nsBefore := obs.IngestIdentityStripped.WithLabelValues("k8s.namespace.name").Value()
	routeBefore := obs.IngestReservedStripped.WithLabelValues(route.ScriptMarker).Value()
	elemBefore := obs.IngestReservedStripped.WithLabelValues(testElemKey).Value()

	// Interleaved with honest attributes, and three copies rather than two: a
	// fix that drained only adjacent duplicates, or only one extra, would still
	// leave a survivor for route.match to read.
	ld := dupLogs(t,
		[][2]string{
			{"k8s.namespace.name", forgedNamespace},
			{"service.name", "attacker-app"},
			{route.ScriptMarker, victimRoute},
			{"k8s.namespace.name", forgedNamespace},
			{"app.attr", "keep me"},
			{route.ScriptMarker, victimRoute},
			{"k8s.namespace.name", forgedNamespace},
			{route.ScriptMarker, victimRoute},
		},
		// No resolvable id anywhere: this is the case the strip exists for,
		// where enrichment has nothing to overwrite the claim with.
		[][2]string{{testElemKey, "1"}, {"rec.attr", "v"}, {testElemKey, "1"}, {testElemKey, "1"}},
	)

	body, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(srv.URL+"/v1/logs", "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(exp.logs) != 1 {
		t.Fatalf("forwarded payloads = %d, want 1", len(exp.logs))
	}

	rl := exp.logs[0].ResourceLogs().At(0)
	res := rl.Resource().Attributes()
	for _, k := range []string{"k8s.namespace.name", route.ScriptMarker} {
		if n := countKey(res, k); n != 0 {
			t.Errorf("%d copies of %q survived the strip: pcommon.Map.Remove stops at the first match, "+
				"and route.match reads this key first-wins", n, k)
		}
	}
	if n := countKey(rl.ScopeLogs().At(0).LogRecords().At(0).Attributes(), testElemKey); n != 0 {
		t.Errorf("%d copies of the element marker %q survived: the transform prune is presence-only and "+
			"would delete the record as an operator-intended drop", n, testElemKey)
	}
	for _, keep := range [][2]string{{"service.name", "attacker-app"}, {"app.attr", "keep me"}} {
		if v, ok := res.Get(keep[0]); !ok || v.Str() != keep[1] {
			t.Errorf("%q did not survive: only the identity claim and the plumbing markers are refused", keep[0])
		}
	}

	// Every occurrence is counted, which is what both metrics' help already
	// promised ("attribute OCCURRENCES removed"). A sender repeating a key is
	// doing precisely the thing the strip exists to stop, so a count of one for
	// it would understate exactly the case worth finding.
	const times = 3
	if got := obs.IngestIdentityStripped.WithLabelValues("k8s.namespace.name").Value() - nsBefore; got != times {
		t.Errorf("identity strips counted = %v, want %d (one per occurrence)", got, times)
	}
	if got := obs.IngestReservedStripped.WithLabelValues(route.ScriptMarker).Value() - routeBefore; got != times {
		t.Errorf("script-marker strips counted = %v, want %d (one per occurrence)", got, times)
	}
	if got := obs.IngestReservedStripped.WithLabelValues(testElemKey).Value() - elemBefore; got != times {
		t.Errorf("element-marker strips counted = %v, want %d (one per occurrence)", got, times)
	}

	// And the reason all of that matters: the REAL router, offered the payload
	// this receiver forwarded, must not take the destination the sender named.
	def, tenant := &routeCapture{}, &routeCapture{}
	r := route.New(def, []route.Destination{
		{Name: victimRoute, Namespaces: []string{forgedNamespace}, Exporter: tenant},
	})
	if err := r.ExportLogs(context.Background(), exp.logs[0]); err != nil {
		t.Fatal(err)
	}
	if len(tenant.logs) != 0 {
		t.Errorf("the router sent %d payload(s) to %q, under that tenant's headers, for a pod that "+
			"simply wrote the attribute twice", len(tenant.logs), victimRoute)
	}
	if len(def.logs) != 1 {
		t.Errorf("the default chain received %d payloads, want 1", len(def.logs))
	}
}

// The same property one level down, on the map itself. The end-to-end test can
// only observe what a payload carries, so a strip helper that drained the
// resource but not a data point's attributes — or that counted one removal for
// three — would still pass it.
func TestStripDrainsEveryOccurrenceOfAKey(t *testing.T) {
	s := NewServer(ServerConfig{Enricher: newEnricher(newMeta(), MetricsAuto)})
	ld := dupLogs(t, [][2]string{
		{"a", "1"},
		{testResKey, "x"},
		{"b", "2"},
		{testResKey, "y"},
		{testResKey, "z"},
	}, nil)
	m := ld.ResourceLogs().At(0).Resource().Attributes()

	before := obs.IngestReservedStripped.WithLabelValues(testResKey).Value()
	s.stripReserved(m, []string{testResKey})

	if n := countKey(m, testResKey); n != 0 {
		t.Errorf("%d occurrences of %q left after the strip", n, testResKey)
	}
	if m.Len() != 2 {
		t.Errorf("map length = %d, want 2: the strip removed something it was not asked to", m.Len())
	}
	for _, k := range []string{"a", "b"} {
		if _, ok := m.Get(k); !ok {
			t.Errorf("the strip removed the unrelated key %q", k)
		}
	}
	if got := obs.IngestReservedStripped.WithLabelValues(testResKey).Value() - before; got != 3 {
		t.Errorf("counted %v removals, want 3", got)
	}
}
