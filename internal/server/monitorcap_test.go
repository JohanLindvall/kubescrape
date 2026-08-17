package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/JohanLindvall/kubescrape/internal/scrape"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// The per-pod target ceiling has to hold on EVERY door, not just the pod
// annotation. A ServiceMonitor's endpoint list is the second door and needs no
// annotation on the pod at all: endpoints with distinct paths resolve to
// distinct URLs, so the URL dedup does not collapse them and each materialises
// a target embedding the WHOLE pod. Capping only the annotation door left this
// open and the O(N²) response with it (measured 20.8 MiB at 1024 endpoints,
// multiplied per pod the Service selects).
func TestMonitorEndpointsCannotExceedThePerPodCeiling(t *testing.T) {
	const endpoints = 400

	st := store.New(time.Minute)
	// A pod with a large annotation, so an uncapped response is visibly huge.
	filler := strings.Repeat("x", 8<<10)
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web-1", Namespace: "default",
			UID: types.UID("pod-uid"), ResourceVersion: "1",
			Labels:      map[string]string{"app": "web"},
			Annotations: map[string]string{"filler": filler},
		},
		Spec: corev1.PodSpec{
			NodeName: "node1",
			Containers: []corev1.Container{{
				Name: "app", Image: "img",
				Ports: []corev1.ContainerPort{{Name: "metrics", ContainerPort: 9090}},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.9.9.9"},
	})
	svcs := services.NewIndex()
	svcs.Upsert(&corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web", Namespace: "default", UID: types.UID("svc-uid"),
			Labels: map[string]string{"team": "obs"},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "web"},
			Ports:    []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromString("metrics")}},
		},
	})

	eps := make([]any, 0, endpoints)
	for i := 0; i < endpoints; i++ {
		eps = append(eps, map[string]any{"port": "http", "path": "/m" + strconv.Itoa(i)})
	}
	monitors := servicemonitors.NewIndex()
	if err := monitors.Upsert(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "sm-many", "namespace": "default"},
		"spec": map[string]any{
			"selector":  map[string]any{"matchLabels": map[string]any{"team": "obs"}},
			"endpoints": eps,
		},
	}}); err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(New(Config{
		Store: st, Services: svcs, Monitors: monitors, Resolver: stubResolver{},
		MaxWait: 500 * time.Millisecond, CacheTTL: 10 * time.Second, Ready: closedChan(),
	}).Handler())
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/v1/nodes/node1/targets")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		Targets []struct {
			URL string `json:"url"`
		} `json:"targets"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d monitor endpoints -> %d targets, %.2f MiB response",
		endpoints, len(out.Targets), float64(len(body))/(1<<20))
	if len(out.Targets) > scrape.MaxPortsPerPod {
		t.Errorf("a ServiceMonitor with %d endpoints produced %d targets for one pod; "+
			"the per-pod ceiling of %d must hold on the monitor door too",
			endpoints, len(out.Targets), scrape.MaxPortsPerPod)
	}
}
