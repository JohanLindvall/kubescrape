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

// An informer RESYNC re-delivers every object it holds, byte-identical, on a
// timer. Everything downstream of that already knew: the index refuses to move
// its change token for a re-delivery, and the namespace refusal in this very
// handler chain is logged at Debug with "an informer resync re-delivers every
// object, so Info would repeat the same line for every refused monitor on every
// resync" written above it.
//
// The ignored-fields report was the one that did not know. It is a statement
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
	// actually the period, and the wait below has to be a multiple of the REAL
	// one or the test sleeps through no resync at all and passes on anything.
	const resync = time.Second

	gvr := servicemonitors.GVR
	scheme := runtime.NewScheme()
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{gvr: "ServiceMonitorList"},
		monitorWithIgnoredFields("1"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(client, resync, "", nil)
	index := servicemonitors.NewIndex()
	synced, err := monitorInformer(factory, gvr, "servicemonitor", nil, slog.New(slog.DiscardHandler),
		index.UpsertChanged, index.Delete, index.Endpoints)
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

	// …and then several resync periods pass with the object untouched.
	time.Sleep(3 * resync)
	if got := obs.MonitorFieldsIgnored.WithLabelValues("servicemonitor").Value(); got != afterFirst {
		t.Errorf("kubescrape_monitor_fields_ignored_total moved by %v across %d resync periods with no "+
			"monitor edited: the counter climbs with the resync period instead of with events, so its rate "+
			"is a standing alarm and the WARN beside it repeats for every such monitor forever",
			got-afterFirst, 3)
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
