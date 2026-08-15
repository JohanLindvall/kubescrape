package main

import (
	"context"
	"log/slog"
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

// The parse-error report is the SIBLING of the ignored-fields one three lines
// below it in the same handler — same shape, same informer, and its own comment
// compares the two ("strictly more severe than the 'some endpoint fields were
// ignored' case, which does get a metric"). It described an EVENT and fired per
// DELIVERY, so one monitor nobody ever fixes re-logged a WARN and
// re-incremented kubescrape_monitor_parse_errors_total once per resync period,
// forever: a counter whose rate is the resync period cannot carry an alert, and
// the alert this one is FOR — a monitor being dropped from the index, taking
// every target it contributed with it — is the one that most needs to.
//
// Gating it is the part that is easy to get wrong, which is why this test
// insists on all three: the first sighting reports (a monitor that never parsed
// was never in the index, so "did the index change?" is the wrong question),
// the resyncs do not, and a fresh mistake does.
func TestParseErrorsAreReportedPerChangeNotPerResync(t *testing.T) {
	// client-go clamps a resync below one second to one second, so this is the
	// shortest period that is actually the period (the sibling test says the
	// same, for the same reason).
	const resync = time.Second

	gvr := servicemonitors.GVR
	scheme := runtime.NewScheme()
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{gvr: "ServiceMonitorList"},
		unparseableMonitor("1"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(client, resync, "", nil)
	index := servicemonitors.NewIndex()
	synced, err := monitorInformer(factory, gvr, "servicemonitor", nil, slog.New(slog.DiscardHandler),
		index.UpsertChanged, index.Delete, index.Endpoints)
	if err != nil {
		t.Fatal(err)
	}
	count := func() float64 { return obs.MonitorParseErrors.WithLabelValues("servicemonitor").Value() }
	before := count()
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	deadline := time.Now().Add(10 * time.Second)
	for !synced() {
		if time.Now().After(deadline) {
			t.Fatal("the monitor informer never synced")
		}
		time.Sleep(time.Millisecond)
	}
	// The first delivery of a monitor that cannot be parsed is news and must be
	// reported — it is an operator's applied manifest doing nothing.
	for count() == before {
		if time.Now().After(deadline) {
			t.Fatal("a monitor that does not parse was never reported at all: an applied manifest is being " +
				"dropped in silence, and this test can no longer tell a resync-driven repeat from that")
		}
		time.Sleep(time.Millisecond)
	}
	afterFirst := count()

	// …and then several resync periods pass with the object untouched.
	time.Sleep(3 * resync)
	if got := count(); got != afterFirst {
		t.Errorf("kubescrape_monitor_parse_errors_total moved by %v across %d resync periods with nothing "+
			"edited: the counter climbs with the resync period instead of with events, so its rate is a "+
			"standing alarm and the WARN beside it repeats forever for one broken monitor",
			got-afterFirst, 3)
	}

	// A NEW mistake still reports: the gate must not have silenced the signal.
	if _, err := client.Resource(gvr).Namespace("monitoring").
		Update(ctx, unparseableMonitor("2"), metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for count() == afterFirst {
		if time.Now().After(deadline) {
			t.Fatal("an edited monitor that still does not parse was not reported: gating the report on a " +
				"real change has silenced the change itself")
		}
		time.Sleep(time.Millisecond)
	}
}

// unparseableMonitor is a ServiceMonitor whose selector is a string where the
// spec requires an object, at resourceVersion rv. It is well-formed enough for
// the API machinery and unparseable to servicemonitors.Parse — which is what
// the invalid-update-removes policy acts on.
func unparseableMonitor(rv string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "monitoring.coreos.com/v1",
		"kind":       "ServiceMonitor",
		"metadata": map[string]any{
			"name":            "broken",
			"namespace":       "monitoring",
			"resourceVersion": rv,
		},
		"spec": map[string]any{
			"selector":  "not-a-selector",
			"endpoints": []any{map[string]any{"port": "metrics"}},
		},
	}}
}
