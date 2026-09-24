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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/JohanLindvall/kubescrape/internal/agent/route"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/testrace"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
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
	doc := `{"resourceLogs":[{"resource":{"attributes":[` + jsonAttrList(resAttrs) +
		`]},"scopeLogs":[{"logRecords":[{"body":{"stringValue":"hi"},"attributes":[` +
		jsonAttrList(recAttrs) + `]}]}]}]}`

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

// jsonAttrList renders an ORDERED key/value list as an OTLP/JSON attribute
// array, so a key may appear more than once.
func jsonAttrList(pairs [][2]string) string {
	var b strings.Builder
	for i, kv := range pairs {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`{"key":"` + kv[0] + `","value":{"stringValue":"` + kv[1] + `"}}`)
	}
	return b.String()
}

// dupMetrics is dupLogs' metrics sibling: one resource carrying resAttrs in
// order (duplicates kept), one gauge with one data point per entry of points,
// each carrying its own ordered attribute list. OTLP/JSON, then a protobuf
// round trip, for the reason dupLogs gives.
func dupMetrics(t *testing.T, resAttrs [][2]string, points [][][2]string) pmetric.Metrics {
	t.Helper()
	var dps strings.Builder
	for i, pa := range points {
		if i > 0 {
			dps.WriteString(",")
		}
		dps.WriteString(`{"asInt":"1","attributes":[` + jsonAttrList(pa) + `]}`)
	}
	doc := `{"resourceMetrics":[{"resource":{"attributes":[` + jsonAttrList(resAttrs) +
		`]},"scopeMetrics":[{"metrics":[{"name":"described_object_info","gauge":{"dataPoints":[` +
		dps.String() + `]}}]}]}]}`

	req := pmetricotlp.NewExportRequest()
	if err := req.UnmarshalJSON([]byte(doc)); err != nil {
		t.Fatalf("building the duplicate-key fixture: %v", err)
	}
	raw, err := req.MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	back := pmetricotlp.NewExportRequest()
	if err := back.UnmarshalProto(raw); err != nil {
		t.Fatal(err)
	}
	md := back.Metrics()
	if got := md.ResourceMetrics().At(0).Resource().Attributes().Len(); got != len(resAttrs) {
		t.Fatalf("the fixture lost a duplicate in transit: %d resource attributes, want %d", got, len(resAttrs))
	}
	return md
}

// wireLogs builds a one-resource logs push straight from protobuf wire bytes:
// the resource carries every attribute resAttrs emits, IN ORDER and duplicates
// included, and `records` one-line records follow. It exists for the WIDE
// fixtures, where dupLogs' JSON would be the slow part and pcommon's Put —
// which scans for an existing key — would make building the fixture as
// quadratic as the defect under test.
func wireLogs(t testing.TB, resAttrs func(add func(k, v string)), records int) plog.Logs {
	t.Helper()
	var res []byte
	resAttrs(func(k, v string) {
		// Resource.attributes(1) = KeyValue{key(1), value(2): AnyValue{string_value(1)}}
		res = protoField(res, 1, concatBytes(wf(1, []byte(k)), wf(2, wf(1, []byte(v)))))
	})
	rec := wf(5, wf(1, []byte("x"))) // LogRecord{body: "x"}
	var sl []byte
	for range records {
		sl = protoField(sl, 2, rec) // ScopeLogs.log_records
	}
	rl := concatBytes(wf(1, res), wf(2, sl)) // ResourceLogs{resource, scope_logs}
	req := plogotlp.NewExportRequest()
	if err := req.UnmarshalProto(wf(1, rl)); err != nil {
		t.Fatalf("building the wire fixture: %v", err)
	}
	return req.Logs()
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

// The strip is ONE pass per key, however many copies a sender repeats. It used
// to loop pcommon.Map.Remove, which rescans from index 0 on every call, so a
// resource of F filler attributes followed by C copies of a stripped key cost
// F x C comparisons — on the default config (the identity strip is always
// wired), on an unauthenticated listener, before any width bound. This shape
// is ~120k attributes and a couple of KiB gzipped; measured before the fix at
// ~4.7 s of one core holding an in-flight slot, quadratically more per
// doubling. After it, a couple of milliseconds.
func TestStripIsLinearInRepeatedKeyCopies(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector's slowdown makes a wall-clock ceiling meaningless")
	}
	const fillers, copies = 100_000, 20_000
	ld := wireLogs(t, func(add func(k, v string)) {
		for range fillers {
			add("f", "")
		}
		for range copies {
			add("k8s.namespace.name", forgedNamespace)
		}
	}, 1)
	s := benchServer(t)
	before := obs.IngestIdentityStripped.WithLabelValues("k8s.namespace.name").Value()

	start := time.Now()
	s.sanitizeLogs(ld)
	took := time.Since(start)

	m := ld.ResourceLogs().At(0).Resource().Attributes()
	if n := countKey(m, "k8s.namespace.name"); n != 0 {
		t.Fatalf("%d copies of k8s.namespace.name survived the strip", n)
	}
	if n := countKey(m, "f"); n != fillers {
		t.Fatalf("%d filler attributes left, want all %d: the strip removed what it was not asked to", n, fillers)
	}
	if got := obs.IngestIdentityStripped.WithLabelValues("k8s.namespace.name").Value() - before; got != copies {
		t.Errorf("counted %v removals, want %d (one per occurrence)", got, copies)
	}
	// Generous: the quadratic loop took seconds on this shape, the single pass
	// takes milliseconds, so a loaded machine cannot put the two on the same
	// side of the line.
	if limit := 500 * time.Millisecond; took > limit {
		t.Errorf("stripping %d copies behind %d fillers took %v, want < %v: the strip is quadratic in the "+
			"copies a sender repeats", copies, fillers, took, limit)
	}
}

// The split path's strips drain EVERY copy too. stripSenderIdentity runs before
// overwriteAttrs re-labels the copied sender resource as the DESCRIBED object's,
// and overwriteAttrs Puts into the FIRST entry: a surviving copy of the
// exporter's own service.name would ride the wire beside the described pod's —
// the sender's identity on an object it merely reports on. The service triple is
// the reachable duplicate on production listeners: the receipt strip removes
// every k8s.pod.name copy, but it deliberately exempts the service triple
// (senderControlledIdentity) and the lookup keys.
func TestSplitDrainsRepeatedSenderIdentity(t *testing.T) {
	triple := []string{"service.name", "service.namespace", "service.instance.id"}
	var res [][2]string
	for i := range 3 { // three copies of each, interleaved with keys that stay
		for _, k := range triple {
			res = append(res, [2]string{k, "exporter"})
		}
		res = append(res, [2]string{fmt.Sprintf("sender.attr.%d", i), "keep"})
	}
	md := dupMetrics(t, res, [][][2]string{{{"k8s.pod.uid", "pod-uid-2"}}})

	out := newEnricher(newMeta(), MetricsDatapoint).EnrichMetrics(context.Background(), md)

	if out.ResourceMetrics().Len() != 1 {
		t.Fatalf("resources = %d, want the one described-object group", out.ResourceMetrics().Len())
	}
	a := out.ResourceMetrics().At(0).Resource().Attributes()
	if v, _ := a.Get("k8s.pod.name"); v.Str() != "web-2" {
		t.Fatalf("k8s.pod.name = %q, want web-2: the fixture no longer takes the described-object arm", v.Str())
	}
	for _, k := range triple {
		if n := countKey(a, k); n != 1 {
			t.Errorf("%d entries of %s on the described pod's resource, want exactly 1", n, k)
		}
		if v, _ := a.Get(k); v.Str() == "exporter" {
			t.Errorf("%s = exporter: the sender's identity was written onto the object it describes", k)
		}
		for key, v := range a.All() {
			if key == k && v.Str() == "exporter" {
				t.Errorf("a copy of the exporter's %s survived beside the described pod's", k)
			}
		}
	}
	for i := range 3 {
		if _, ok := a.Get(fmt.Sprintf("sender.attr.%d", i)); !ok {
			t.Errorf("sender.attr.%d was stripped: only identity is the described object's", i)
		}
	}
}

// The OVERFLOW group (a point the split budgets refused) and every admitted
// described-object group strip the sender's lookup keys through stripIDAttrs —
// EVERY copy, because putIDAttr / the overwrite re-stamp only the first entry,
// and a survivor leaves one resource naming two different objects, the second
// being the exporter itself.
func TestSplitDrainsRepeatedLookupKeysFromDescribedGroups(t *testing.T) {
	capped := obs.Ingested.WithLabelValues("split_capped").Value()
	meta := &fakeMeta{pods: map[string]*kubemeta.Pod{
		"ksm-uid": {Name: "ksm-0", Namespace: "monitoring", UID: "ksm-uid", NodeName: "node1"},
	}}
	res := [][2]string{
		{"k8s.pod.uid", "ksm-uid"},
		{"container.id", "ksm-container"}, // unresolvable, so the pod uid names the sender
		{"service.name", "kube-state-metrics"},
		{"k8s.pod.uid", "ksm-uid"},
		{"container.id", "ksm-container"},
		{"k8s.pod.uid", "ksm-uid"},
		{"container.id", "ksm-container"},
	}
	// The sender's own resource, over the byte budget's per-group seam, so the
	// later objects are refused into the overflow group (splitoverflow_test.go).
	pad := strings.Repeat("x", 2<<10)
	for i := range 42 {
		res = append(res, [2]string{fmt.Sprintf("sender.attr.%02d", i), pad})
	}
	const pods = 400
	points := make([][][2]string, pods)
	for i := range points {
		uid := fmt.Sprintf("uid-%d", i)
		meta.pods[uid] = &kubemeta.Pod{Name: "app-" + uid, Namespace: "apps", UID: uid, NodeName: "node1"}
		points[i] = [][2]string{{"k8s.pod.uid", uid}}
	}
	md := dupMetrics(t, res, points)

	out := NewEnricher(Config{Meta: meta, MetricsMode: MetricsDatapoint}).EnrichMetrics(context.Background(), md)

	if obs.Ingested.WithLabelValues("split_capped").Value() == capped {
		t.Fatal("nothing was refused: the payload no longer reaches the overflow group")
	}
	// Tallied rather than reported per resource: one defect here fails every
	// one of ~190 groups the same way.
	var overflow, withContainerID, overflowWithUID, describedWrongUID int
	rms := out.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		a := rms.At(i).Resource().Attributes()
		if countKey(a, "container.id") != 0 {
			withContainerID++
		}
		if _, described := a.Get("k8s.pod.name"); !described {
			overflow++
			if countKey(a, "k8s.pod.uid") != 0 {
				overflowWithUID++
			}
			continue
		}
		exporters := 0
		for key, v := range a.All() {
			if key == "k8s.pod.uid" && v.Str() == "ksm-uid" {
				exporters++
			}
		}
		if countKey(a, "k8s.pod.uid") != 1 || exporters != 0 {
			describedWrongUID++
		}
	}
	if overflow != 1 {
		t.Fatalf("overflow resources = %d, want 1", overflow)
	}
	if withContainerID != 0 {
		t.Errorf("%d of %d resources carry the exporter's container.id: it survived onto groups describing "+
			"other objects", withContainerID, rms.Len())
	}
	if overflowWithUID != 0 {
		t.Error("the overflow resource carries a k8s.pod.uid: the exporter's own, on points about other pods")
	}
	if describedWrongUID != 0 {
		t.Errorf("%d described-object resources carry other than exactly their own k8s.pod.uid (a surviving "+
			"copy of the exporter's names a second pod)", describedWrongUID)
	}
}
