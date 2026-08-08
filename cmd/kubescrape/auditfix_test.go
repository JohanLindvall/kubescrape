package main

import (
	"sync"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/selfmeta"
)

// The namespace half of the Prometheus job must be present from the FIRST
// export. attrs.Identity is the sole producer of service.namespace, and the
// only OTHER call for this binary runs inside the self-metadata stamp — over a
// FRESH resource built from the resolved pod — so leaving it off here made the
// job depend entirely on a store lookup: `kubescrape` until the lookup landed
// (renaming already-running cumulative series when it did), and `kubescrape`
// forever where it never can (an overridden spec.hostname, hostNetwork). The
// agent's twin, agentSelfResource, has always called it.
func TestServiceSelfResourceDerivesTheJobAtStartup(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "monitoring")
	a := serviceSelfResource().Attributes()
	get := func(key string) string {
		t.Helper()
		v, ok := a.Get(key)
		if !ok {
			t.Fatalf("%s is absent from the service's own resource", key)
		}
		return v.AsString()
	}
	if got := get("k8s.namespace.name"); got != "monitoring" {
		t.Fatalf("k8s.namespace.name = %q at startup", got)
	}
	if got := get("service.namespace"); got != "monitoring" {
		t.Fatalf("service.namespace = %q at startup; the job must not change when the store lookup lands", got)
	}
	if got := get("service.name"); got != "kubescrape" {
		t.Fatalf("service.name = %q; the identity derivation must not rename the service", got)
	}
	// The hostname keeps naming the instance: Identity returns early on a key
	// that is already set, and the hostname is stable across restarts where
	// anything it would derive is not.
	if got := get("service.instance.id"); got == "" {
		t.Fatal("service.instance.id is empty: Identity's fallback displaced the hostname")
	}
}

// With no namespace available at all (neither $POD_NAMESPACE nor the
// ServiceAccount projection — `go run ./cmd/kubescrape` against a kubeconfig)
// the derivation must add nothing and refuse nothing.
func TestServiceSelfResourceWithoutANamespace(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "")
	prev := selfmeta.SetNamespaceProjectionForTest(t.TempDir() + "/absent")
	t.Cleanup(func() { selfmeta.SetNamespaceProjectionForTest(prev) })

	a := serviceSelfResource().Attributes()
	if _, ok := a.Get("k8s.namespace.name"); ok {
		t.Fatal("a namespace was invented with neither source available")
	}
	if v, ok := a.Get("service.namespace"); ok {
		t.Fatalf("service.namespace = %q with no namespace to derive it from", v.AsString())
	}
}

// The shutdown sequence spends three independent budgets (HTTP drain, the
// exporting goroutines' join, the final export) and the two deferred listener
// stoppers spend 5s each AFTER it. This Deployment names no
// terminationGracePeriodSeconds, so it runs on Kubernetes' 30s DEFAULT: summed
// literals put the worst case at 40s and the kubelet SIGKILLed the process
// mid-sequence, losing exactly the exports the budgets exist to fit in.
func TestShutdownBudgetFitsTheDefaultTerminationGrace(t *testing.T) {
	// internal/obs/runtime.go gives each of ServeMetrics/ServePprof's stoppers
	// its own 5s Shutdown, and they run deferred, after the sequence above.
	const listenerStoppers = 2 * 5 * time.Second
	const defaultGrace = 30 * time.Second
	if shutdownTotal+listenerStoppers >= defaultGrace {
		t.Fatalf("shutdownTotal %v + %v of listener stoppers leaves nothing of the kubelet's default %v grace",
			shutdownTotal, listenerStoppers, defaultGrace)
	}
	if shutdownStep > shutdownTotal {
		t.Fatalf("a single step (%v) may spend more than the whole sequence's budget (%v)", shutdownStep, shutdownTotal)
	}
}

func TestWaitForRespectsItsBudget(t *testing.T) {
	var wg sync.WaitGroup
	if !waitFor(&wg, time.Second) {
		t.Fatal("an already-done WaitGroup was reported as overrunning its budget")
	}
	wg.Add(1)
	t.Cleanup(wg.Done)
	start := time.Now()
	if waitFor(&wg, 20*time.Millisecond) {
		t.Fatal("a WaitGroup that never finished was reported as done")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("waitFor blocked for %v past a 20ms budget", elapsed)
	}
}
