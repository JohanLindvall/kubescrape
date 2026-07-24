package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// A PodMonitor selects pods directly: no Service, no annotations — the
// endpoint's container-port name resolves against the pod spec.
func TestNodeTargetsFromPodMonitor(t *testing.T) {
	st := store.New(time.Minute)
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pm-web-1", Namespace: "default",
			UID: types.UID("pmpod-uid"), ResourceVersion: "1",
			Labels: map[string]string{"app": "pm-web"},
		},
		Spec: corev1.PodSpec{
			NodeName: "node1",
			Containers: []corev1.Container{{
				Name: "app", Image: "img",
				Ports: []corev1.ContainerPort{{Name: "metrics", ContainerPort: 9191}},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.1.2.9"},
	})
	monitors := servicemonitors.NewIndex()
	if err := monitors.UpsertPodMonitor(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "pm1", "namespace": "default"},
		"spec": map[string]any{
			"selector":            map[string]any{"matchLabels": map[string]any{"app": "pm-web"}},
			"podMetricsEndpoints": []any{map[string]any{"port": "metrics", "path": "/pm"}},
		},
	}}); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(New(Config{
		Store: st, Services: services.NewIndex(), Monitors: monitors,
		Resolver: stubResolver{}, MaxWait: 500 * time.Millisecond, Ready: closedChan(),
	}).Handler())
	t.Cleanup(srv.Close)

	var out struct {
		Targets []struct {
			URL     string `json:"url"`
			Source  string `json:"source"`
			Monitor string `json:"monitor"`
		} `json:"targets"`
	}
	getJSON(t, srv.URL+"/v1/nodes/node1/targets", http.StatusOK, &out)
	if len(out.Targets) != 1 {
		t.Fatalf("targets: %+v", out.Targets)
	}
	tg := out.Targets[0]
	if tg.URL != "http://10.1.2.9:9191/pm" || tg.Source != "podmonitor" || tg.Monitor != "default/pm1" {
		t.Fatalf("target: %+v", tg)
	}
}
