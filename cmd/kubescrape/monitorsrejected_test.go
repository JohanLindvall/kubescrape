package main

import (
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func brokenSM(ns, name, rv string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "monitoring.coreos.com/v1",
		"kind":       "ServiceMonitor",
		"metadata": map[string]any{
			"namespace": ns, "name": name, "resourceVersion": rv,
		},
		"spec": map[string]any{
			"selector": map[string]any{"matchLabels": "not-a-map"},
		},
	}}
}

// The hook is what turns the index's two counts into the exposition's kind
// label set, and its one rule is that an UNWATCHED kind is ABSENT — not a
// forever-0 series claiming a CRD nobody serves is clean. A published 0 must
// always mean "watched, and none rejected".
func TestMonitorsRejectedHookEmitsOnlyWatchedKinds(t *testing.T) {
	ix := servicemonitors.NewIndex()
	if _, err := ix.UpsertChanged(brokenSM("team-a", "web", "1")); err == nil {
		t.Fatal("the broken ServiceMonitor parsed; the fixture no longer exercises the rejected path")
	}

	smOnly := monitorsRejectedHook(ix, true, false)()
	if got, ok := smOnly[kindServiceMonitor]; !ok || got != 1 {
		t.Fatalf("servicemonitor-only hook = %v, want {%s: 1}", smOnly, kindServiceMonitor)
	}
	if _, leaked := smOnly[kindPodMonitor]; leaked {
		t.Fatalf("the hook emitted %q while that CRD is unwatched: a forever-0 series reads as watched-and-clean", kindPodMonitor)
	}

	both := monitorsRejectedHook(ix, true, true)()
	if both[kindServiceMonitor] != 1 || both[kindPodMonitor] != 0 {
		t.Fatalf("both-kinds hook = %v, want {%s: 1, %s: 0}", both, kindServiceMonitor, kindPodMonitor)
	}
}

// End to end through the registry: RegisterMonitorsRejected must publish the
// hook's map as kubescrape_monitors_rejected{kind}, live — the gauge is a
// func, so a later state change must be visible on the next read without any
// re-registration.
func TestMonitorsRejectedGaugePublishesTheIndexState(t *testing.T) {
	ix := servicemonitors.NewIndex()
	obs.RegisterMonitorsRejected(monitorsRejectedHook(ix, true, false))

	read := func() (float64, bool) {
		t.Helper()
		for _, m := range obs.Registry.Dump() {
			if m.Name != "kubescrape_monitors_rejected" {
				continue
			}
			for _, p := range m.Points {
				for _, kv := range p.Labels {
					if kv[0] == "kind" && kv[1] == kindServiceMonitor {
						return p.Value, true
					}
				}
			}
		}
		return 0, false
	}

	if v, ok := read(); !ok || v != 0 {
		t.Fatalf("fresh index: gauge = (%v, %v), want (0, published)", v, ok)
	}
	if _, err := ix.UpsertChanged(brokenSM("team-a", "web", "1")); err == nil {
		t.Fatal("the broken ServiceMonitor parsed")
	}
	if v, ok := read(); !ok || v != 1 {
		t.Fatalf("after a rejected upsert: gauge = (%v, %v), want (1, published)", v, ok)
	}
	ix.Delete("team-a", "web")
	if v, ok := read(); !ok || v != 0 {
		t.Fatalf("after the delete: gauge = (%v, %v), want (0, published) — the state must come back down", v, ok)
	}
}
