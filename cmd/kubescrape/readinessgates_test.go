package main

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	discoveryfake "k8s.io/client-go/discovery/fake"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	coretesting "k8s.io/client-go/testing"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// awaitGates spins until every gate reports synced, so the caller observes the
// FIRST moment readiness would flip — a sleeping poll could let the handlers
// catch up in between and hide an informer-gated regression.
func awaitGates(t *testing.T, gates []syncGate) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		all := true
		for _, g := range gates {
			if !g.synced() {
				all = false
				break
			}
		}
		if all {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the readiness gates never synced")
		}
		runtime.Gosched()
	}
}

func gateNames(gates []syncGate) []string {
	names := make([]string, len(gates))
	for i, g := range gates {
		names[i] = g.name
	}
	return names
}

// The core readiness gates must be the HANDLER REGISTRATIONS' HasSynced, never
// the informer's: the informer's flips once its DeltaFIFO has drained the
// initial LIST, while the store and the service index are filled from inside
// our handlers, which the shared processor runs asynchronously behind it.
// Gated on the informer, /readyz answers 200 while the store is still filling
// and an agent's first poll gets a half-built target list. The initial LIST is
// large enough that the handlers visibly trail the informer (reverting either
// gate to the informer's HasSynced fails this in every trial measured).
func TestCoreGatesWaitForTheHandlersNotTheInformer(t *testing.T) {
	const nPods, nSvcs = 3000, 500
	objs := make([]k8sruntime.Object, 0, nPods+nSvcs)
	for i := range nPods {
		objs = append(objs, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns", Name: fmt.Sprintf("pod-%d", i), UID: types.UID(fmt.Sprintf("uid-%d", i)),
			},
			Spec:   corev1.PodSpec{NodeName: "node1"},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		})
	}
	for i := range nSvcs {
		objs = append(objs, &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns", Name: fmt.Sprintf("svc-%d", i), UID: types.UID(fmt.Sprintf("svc-uid-%d", i)),
			},
		})
	}

	for trial := range 3 {
		client := fake.NewSimpleClientset(objs...)
		factory := informers.NewSharedInformerFactory(client, 0)
		st := store.New(time.Minute)
		svcIndex := services.NewIndex()
		gates, err := registerCoreInformers(factory, st, svcIndex)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := gateNames(gates), []string{podGate, "services"}; !slices.Equal(got, want) {
			t.Fatalf("core gates = %v, want %v", got, want)
		}
		ctx, cancel := context.WithCancel(context.Background())
		factory.Start(ctx.Done())
		awaitGates(t, gates)

		pods, _ := st.Stats()
		svcs := len(svcIndex.All(nil))
		cancel()
		factory.Shutdown()
		if pods != nPods || svcs != nSvcs {
			t.Fatalf("trial %d: the gates reported synced with %d/%d pods and %d/%d services indexed: "+
				"readiness is gated on the informer rather than on the handlers that fill the store",
				trial, pods, nPods, svcs, nSvcs)
		}
	}
}

// startServiceMonitors must gate readiness on EVERY monitor cache it starts,
// and on the handlers that fill the index: /v1/nodes/{node}/targets reads the
// PodMonitor index too, and a PodMonitor informer that was started but never
// awaited let /readyz report 200 while that index was empty — or permanently
// so, when a missing podmonitors RBAC rule 403-loops its LIST.
func TestMonitorGatesCoverEveryWatchedKind(t *testing.T) {
	sm := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "monitoring.coreos.com/v1", "kind": "ServiceMonitor",
		"metadata": map[string]any{"name": "sm", "namespace": "monitoring", "resourceVersion": "1"},
		"spec": map[string]any{
			"selector":  map[string]any{"matchLabels": map[string]any{"app": "web"}},
			"endpoints": []any{map[string]any{"port": "metrics"}},
		},
	}}
	pm := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "monitoring.coreos.com/v1", "kind": "PodMonitor",
		"metadata": map[string]any{"name": "pm", "namespace": "monitoring", "resourceVersion": "1"},
		"spec": map[string]any{
			"selector":            map[string]any{"matchLabels": map[string]any{"app": "web"}},
			"podMetricsEndpoints": []any{map[string]any{"port": "metrics"}},
		},
	}}

	for _, tc := range []struct {
		name      string
		resources []string
		wantGates []string
	}{
		{"both CRDs", []string{"servicemonitors", "podmonitors"}, []string{kindServiceMonitor, kindPodMonitor}},
		{"PodMonitor CRD only", []string{"podmonitors"}, []string{kindPodMonitor}},
		{"ServiceMonitor CRD only", []string{"servicemonitors"}, []string{kindServiceMonitor}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakeDisco := &coretesting.Fake{}
			var apiResources []metav1.APIResource
			for _, r := range tc.resources {
				apiResources = append(apiResources, metav1.APIResource{Name: r})
			}
			fakeDisco.Resources = []*metav1.APIResourceList{{
				GroupVersion: "monitoring.coreos.com/v1", APIResources: apiResources,
			}}
			dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(k8sruntime.NewScheme(),
				map[schema.GroupVersionResource]string{
					servicemonitors.GVR:    "ServiceMonitorList",
					servicemonitors.PodGVR: "PodMonitorList",
				}, sm.DeepCopy(), pm.DeepCopy())

			ctx := t.Context()
			idx, gates, err := startServiceMonitors(ctx, dyn, &discoveryfake.FakeDiscovery{Fake: fakeDisco},
				0, nil, slog.New(slog.DiscardHandler))
			if err != nil {
				t.Fatal(err)
			}
			if idx == nil {
				t.Fatal("monitor discovery was disabled with a monitoring CRD served")
			}
			if got := gateNames(gates); !slices.Equal(got, tc.wantGates) {
				t.Fatalf("monitor gates = %v, want %v: readiness would not wait for every watched cache", got, tc.wantGates)
			}
			awaitGates(t, gates)
			if slices.Contains(tc.wantGates, kindServiceMonitor) && len(idx.Endpoints("monitoring", "sm")) == 0 {
				t.Error("the ServiceMonitor gate reported synced before its handler indexed the monitor")
			}
			if slices.Contains(tc.wantGates, kindPodMonitor) && len(idx.PodMonitors()) == 0 {
				t.Error("the PodMonitor gate reported synced before its handler indexed the monitor")
			}
		})
	}
}
