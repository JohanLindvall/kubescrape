package main

import (
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/dynamicinformer"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
)

// An informer RESYNC re-delivers every object it holds, byte-identical, on a
// timer. Everything downstream of that already knew: the index refuses to move
// its change token for a re-delivery.
//
// The ignored-fields report was one that did not know (the namespace refusal,
// below, was the other). It is a statement
// about an EVENT — this monitor asks for something kubescrape does not
// interpret — and it fired per DELIVERY, so with -resync set, every monitor
// carrying a `relabelings` or a `sampleLimit` (ordinary in a prometheus-operator
// install) re-logged a WARN and re-incremented
// kubescrape_monitor_fields_ignored_total every resync period, forever. A
// counter whose rate is the resync period rather than the operator's edits
// cannot carry an alert, and fifty such monitors is fifty repeated WARN lines a
// period.
func TestIgnoredFieldsAreReportedPerChangeNotPerResync(t *testing.T) {
	// client-go clamps a resync below one second to one second (shared_informer's
	// "resync period is too small"), so this is the shortest period that is
	// actually the period. The test does not trust it: it counts the deliveries
	// reaching the handler under test (see deliveryCounter), since an assertion
	// that nothing moved passes on anything if no resync ever arrived.
	const resync = time.Second

	gvr := servicemonitors.GVR
	scheme := runtime.NewScheme()
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{gvr: "ServiceMonitorList"},
		monitorWithIgnoredFields("1"))

	ctx := t.Context()
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(client, resync, "", nil)
	index := servicemonitors.NewIndex()
	upsert, deliveries := deliveryCounter(index.UpsertChanged)
	synced, err := monitorInformer(factory, gvr, "servicemonitor", nil, slog.New(slog.DiscardHandler),
		upsert, index.Delete)
	if err != nil {
		t.Fatal(err)
	}
	before := obs.MonitorFieldsIgnored.WithLabelValues("servicemonitor").Value()
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	deadline := time.Now().Add(10 * time.Second)
	for !synced() {
		if time.Now().After(deadline) {
			t.Fatal("the monitor informer never synced")
		}
		time.Sleep(time.Millisecond)
	}
	// The first delivery is a real one and must report.
	for obs.MonitorFieldsIgnored.WithLabelValues("servicemonitor").Value() == before {
		if time.Now().After(deadline) {
			t.Fatal("the monitor's uninterpreted fields were never reported at all: this test can no longer " +
				"tell a resync-driven repeat from silence")
		}
		time.Sleep(time.Millisecond)
	}
	afterFirst := obs.MonitorFieldsIgnored.WithLabelValues("servicemonitor").Value()

	// …and then resyncs re-deliver the object untouched.
	awaitResyncs(t, deliveries, 2)
	if got := obs.MonitorFieldsIgnored.WithLabelValues("servicemonitor").Value(); got != afterFirst {
		t.Errorf("kubescrape_monitor_fields_ignored_total moved by %v across resync deliveries with no "+
			"monitor edited: the counter climbs with the resync period instead of with events, so its rate "+
			"is a standing alarm and the WARN beside it repeats for every such monitor forever",
			got-afterFirst)
	}

	// A REAL edit still reports: the fix must not have silenced the signal.
	edited := monitorWithIgnoredFields("2")
	if _, err := client.Resource(gvr).Namespace("monitoring").Update(ctx, edited, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for obs.MonitorFieldsIgnored.WithLabelValues("servicemonitor").Value() == afterFirst {
		if time.Now().After(deadline) {
			t.Fatal("an edited monitor's uninterpreted fields were not reported: gating the report on a real " +
				"change has silenced the change itself")
		}
		time.Sleep(time.Millisecond)
	}
}

// deliveryCounter wraps the handler's upsert so a test can count the deliveries
// reaching the handler UNDER TEST — resyncs included, since typedHandler's
// UpdateFunc upserts on every one. Counting through a second handler would
// prove only that the INFORMER resynced, and stay green if the monitor handler
// were ever registered with a resync period of its own.
func deliveryCounter(upsert monitorUpsert) (monitorUpsert, *atomic.Int32) {
	var calls atomic.Int32
	return func(u *unstructured.Unstructured) ([]servicemonitors.Endpoint, bool, error) {
		defer calls.Add(1)
		return upsert(u)
	}, &calls
}

// awaitResyncs waits until the first delivery (the initial Add) plus n resyncs
// have reached the upsert. The listener runs deliveries one at a time, so once
// delivery n+1 has passed the upsert, every earlier one has run the WHOLE
// handler — including the report under test, which follows the upsert.
func awaitResyncs(t *testing.T, deliveries *atomic.Int32, n int32) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for deliveries.Load() < 1+n {
		if time.Now().After(deadline) {
			t.Fatalf("only %d deliveries reached the monitor handler, want the initial one plus %d resyncs: "+
				"with no resync arriving, the assertion that nothing moved would pass on anything",
				deliveries.Load(), n)
		}
		time.Sleep(time.Millisecond)
	}
}

// monitorWithIgnoredFields is a ServiceMonitor whose endpoint uses a field
// kubescrape parses but does not interpret (relabelings), at resourceVersion rv.
func monitorWithIgnoredFields(rv string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "monitoring.coreos.com/v1",
		"kind":       "ServiceMonitor",
		"metadata": map[string]any{
			"name":            "web",
			"namespace":       "monitoring",
			"resourceVersion": rv,
		},
		"spec": map[string]any{
			"selector": map[string]any{"matchLabels": map[string]any{"app": "web"}},
			"endpoints": []any{map[string]any{
				"port": "metrics",
				"relabelings": []any{map[string]any{
					"action": "replace", "targetLabel": "env", "replacement": "prod",
				}},
			}},
		},
	}}
}

// The namespace refusal is the same shape of report as the two above it — a
// statement about an EVENT, this monitor was refused — and it was counted per
// DELIVERY: kubescrape_monitor_namespace_refused_total rose by one per refused
// monitor per resync period (and per relist), while its help text claimed the
// sibling counters did the same, which they had stopped doing. A refused monitor
// never reaches the index, so the index's news cannot gate it; the handler keeps
// its own record, and the DELETE arm must clear it, or a monitor deleted and
// re-created unchanged would never be reported again.
func TestNamespaceRefusalIsReportedPerChangeNotPerResync(t *testing.T) {
	const resync = time.Second // client-go's floor; see the test above

	// A refused monitor never reaches the upsert, so an ALLOWED sibling rides the
	// same handler purely to make its deliveries countable (deliveryCounter):
	// every resync re-delivers both objects through the one listener, so once
	// the sibling's second resync has passed the upsert, the refused monitor's
	// first resync has run the whole handler.
	allowed := monitorInNamespace("monitoring", "1")
	allowed.SetName("allowed")
	if err := unstructured.SetNestedSlice(allowed.Object, []any{map[string]any{"port": "metrics"}},
		"spec", "endpoints"); err != nil {
		t.Fatal(err)
	}

	gvr := servicemonitors.GVR
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{gvr: "ServiceMonitorList"},
		monitorInNamespace("team-a", "1"), allowed)

	ctx := t.Context()
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(client, resync, "", nil)
	index := servicemonitors.NewIndex()
	upsert, deliveries := deliveryCounter(index.UpsertChanged)
	synced, err := monitorInformer(factory, gvr, "servicemonitor", map[string]bool{"monitoring": true},
		slog.New(slog.DiscardHandler), upsert, index.Delete)
	if err != nil {
		t.Fatal(err)
	}
	refusals := func() float64 { return obs.MonitorNamespaceRefused.WithLabelValues("servicemonitor").Value() }
	awaitMove := func(from float64, why string) float64 {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for refusals() == from {
			if time.Now().After(deadline) {
				t.Fatal(why)
			}
			time.Sleep(time.Millisecond)
		}
		if got := refusals(); got != from+1 {
			t.Fatalf("the refusal counter moved by %v for one event, want 1", got-from)
		}
		return refusals()
	}

	before := refusals()
	factory.Start(ctx.Done())
	deadline := time.Now().Add(10 * time.Second)
	for !synced() {
		if time.Now().After(deadline) {
			t.Fatal("the monitor informer never synced")
		}
		time.Sleep(time.Millisecond)
	}
	counted := awaitMove(before, "the refused monitor was never reported at all: this test can no longer "+
		"tell a resync-driven repeat from silence")

	awaitResyncs(t, deliveries, 2)
	if got := refusals(); got != counted {
		t.Errorf("kubescrape_monitor_namespace_refused_total moved by %v across resync deliveries with no "+
			"monitor edited: it counts deliveries, so its rate is the resync period", got-counted)
	}

	// An edit is a new event and is reported.
	res := client.Resource(gvr).Namespace("team-a")
	if _, err := res.Update(ctx, monitorInNamespace("team-a", "2"), metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	counted = awaitMove(counted, "an edited refused monitor was not reported: gating on a real change has "+
		"silenced the change itself")

	// Deleted and re-created UNCHANGED — the same resourceVersion the record
	// holds — is still a new monitor, which only the delete arm can know.
	if err := res.Delete(ctx, "web", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := res.Create(ctx, monitorInNamespace("team-a", "2"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	awaitMove(counted, "a refused monitor deleted and re-created was not reported: the delete arm left its "+
		"record behind, which also grows by one entry per refused monitor ever seen")

	if eps := index.Endpoints("team-a", "web"); len(eps) != 0 {
		t.Fatalf("the refused monitor reached the index (%d endpoints)", len(eps))
	}
}

// The record the refusal gate keeps is bounded by the refused monitors that
// still exist: a deleted monitor's entry goes with it.
func TestMonitorRefusalRecordIsPerMonitorVersion(t *testing.T) {
	r := newMonitorRefusals()
	for _, step := range []struct {
		do   string // "see" or "forget"
		rv   string
		news bool
	}{
		{"see", "1", true},    // first delivery
		{"see", "1", false},   // resync / relist of the same object
		{"see", "2", true},    // edit
		{"see", "2", false},   // its re-delivery
		{"forget", "", false}, // deleted
		{"see", "2", true},    // re-created: news again whatever its version
		{"see", "", true},     // a versionless object cannot be told from a change
		{"see", "", true},
	} {
		if step.do == "forget" {
			r.forget("team-a", "web")
			if n := r.size(); n != 0 {
				t.Fatalf("%d records left after the only refused monitor was deleted", n)
			}
			continue
		}
		if got := r.news(monitorInNamespace("team-a", step.rv)); got != step.news {
			t.Fatalf("news(rv=%q) = %v, want %v", step.rv, got, step.news)
		}
	}
}

// size reports how many refused monitors are recorded.
func (r *monitorRefusals) size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.seen)
}

// monitorInNamespace is monitorWithIgnoredFields placed in namespace ns.
func monitorInNamespace(ns, rv string) *unstructured.Unstructured {
	u := monitorWithIgnoredFields(rv)
	u.SetNamespace(ns)
	return u
}
