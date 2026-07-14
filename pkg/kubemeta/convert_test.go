package kubemeta

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNormalizeContainerID(t *testing.T) {
	cases := map[string]string{
		"containerd://abc123": "abc123",
		"docker://abc123":     "abc123",
		"cri-o://abc123":      "abc123",
		"containerd:/abc123":  "abc123", // collapsed by HTTP path cleaning
		"abc123":              "abc123",
		" containerd://x ":    "x",
		"":                    "",
	}
	for in, want := range cases {
		if got := NormalizeContainerID(in); got != want {
			t.Errorf("NormalizeContainerID(%q) = %q, want %q", in, got, want)
		}
	}
}

func testCorePod() *corev1.Pod {
	created := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	started := created.Add(5 * time.Second)
	cStarted := created.Add(8 * time.Second)
	prevStart := created.Add(-time.Hour)
	prevEnd := created.Add(-30 * time.Minute)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pod1",
			Namespace:         "ns1",
			UID:               "uid-1",
			Labels:            map[string]string{"app": "web"},
			Annotations:       map[string]string{"prometheus.io/scrape": "true"},
			CreationTimestamp: metav1.Time{Time: created},
		},
		Spec: corev1.PodSpec{
			NodeName:       "node1",
			InitContainers: []corev1.Container{{Name: "init", Image: "init:1"}},
			Containers: []corev1.Container{{
				Name:  "app",
				Image: "app:2",
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080, Protocol: corev1.ProtocolTCP}},
			}},
			EphemeralContainers: []corev1.EphemeralContainer{{
				EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug", Image: "busybox:1"},
			}},
		},
		Status: corev1.PodStatus{
			Phase:     corev1.PodRunning,
			PodIP:     "10.0.0.5",
			HostIP:    "192.168.1.10",
			StartTime: &metav1.Time{Time: started},
			InitContainerStatuses: []corev1.ContainerStatus{{
				Name:        "init",
				ContainerID: "containerd://initid",
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ExitCode:   0,
					StartedAt:  metav1.Time{Time: created},
					FinishedAt: metav1.Time{Time: started},
				}},
			}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:         "app",
				ContainerID:  "containerd://appid2",
				ImageID:      "sha256:img",
				RestartCount: 1,
				Ready:        true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{
					StartedAt: metav1.Time{Time: cStarted},
				}},
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ContainerID: "containerd://appid1",
					ExitCode:    137,
					StartedAt:   metav1.Time{Time: prevStart},
					FinishedAt:  metav1.Time{Time: prevEnd},
				}},
			}},
			EphemeralContainerStatuses: []corev1.ContainerStatus{{
				Name:  "debug",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}},
			}},
		},
	}
}

func TestFromPod(t *testing.T) {
	pod, byID := FromPod(testCorePod())

	if pod.Name != "pod1" || pod.Namespace != "ns1" || pod.UID != "uid-1" ||
		pod.NodeName != "node1" || pod.PodIP != "10.0.0.5" || pod.HostIP != "192.168.1.10" ||
		pod.Phase != "Running" {
		t.Fatalf("pod = %+v", pod)
	}
	if pod.StartedAt == nil || pod.Labels["app"] != "web" || pod.Annotations["prometheus.io/scrape"] != "true" {
		t.Fatalf("pod meta = %+v", pod)
	}

	// Containers in spec order: init, app, debug — with type, state and ports.
	if len(pod.Containers) != 3 {
		t.Fatalf("containers = %d: %+v", len(pod.Containers), pod.Containers)
	}
	initC, app, debug := pod.Containers[0], pod.Containers[1], pod.Containers[2]
	if initC.Type != "init" || initC.State != "terminated" || initC.ID != "initid" ||
		initC.ExitCode == nil || *initC.ExitCode != 0 || initC.FinishedAt == nil {
		t.Fatalf("init = %+v", initC)
	}
	if app.Type != "container" || app.State != "running" || app.ID != "appid2" ||
		!app.Ready || app.RestartCount != 1 || app.StartedAt == nil ||
		app.RuntimeID != "containerd://appid2" || app.ImageID != "sha256:img" {
		t.Fatalf("app = %+v", app)
	}
	if len(app.Ports) != 1 || app.Ports[0].Name != "http" || app.Ports[0].Port != 8080 || app.Ports[0].Protocol != "TCP" {
		t.Fatalf("app ports = %+v", app.Ports)
	}
	if debug.Type != "ephemeral" || debug.State != "waiting" || debug.WaitingReason != "PodInitializing" || debug.ID != "" {
		t.Fatalf("debug = %+v", debug)
	}

	// The ID index carries the live incarnations AND the restarted container's
	// previous incarnation from lastState.
	if len(byID) != 3 {
		t.Fatalf("byID = %v", byID)
	}
	if c, ok := byID["appid2"]; !ok || c.State != "running" {
		t.Fatalf("byID[appid2] = %+v (%v)", c, ok)
	}
	prev, ok := byID["appid1"]
	if !ok || prev.State != "terminated" || prev.Ready ||
		prev.ExitCode == nil || *prev.ExitCode != 137 ||
		prev.RuntimeID != "containerd://appid1" ||
		prev.StartedAt == nil || prev.FinishedAt == nil {
		t.Fatalf("previous incarnation = %+v (%v)", prev, ok)
	}
	if _, ok := byID["initid"]; !ok {
		t.Fatal("init container missing from the ID index")
	}
}

// FromPod must share no memory with the informer object: mutating the source
// afterwards must not change the returned model.
func TestFromPodDeepCopies(t *testing.T) {
	src := testCorePod()
	pod, byID := FromPod(src)

	src.Labels["app"] = "MUTATED"
	src.Annotations["prometheus.io/scrape"] = "MUTATED"
	src.Spec.Containers[0].Ports[0].ContainerPort = 9999
	src.Status.ContainerStatuses[0].ContainerID = "containerd://MUTATED"

	if pod.Labels["app"] != "web" {
		t.Error("labels alias the informer object")
	}
	if pod.Annotations["prometheus.io/scrape"] != "true" {
		t.Error("annotations alias the informer object")
	}
	if pod.Containers[1].Ports[0].Port != 8080 {
		t.Error("ports alias the informer object")
	}
	if byID["appid2"].RuntimeID != "containerd://appid2" {
		t.Error("container status aliases the informer object")
	}
}

// A pod with no statuses yet (just scheduled) still yields containers from the
// spec, with no IDs indexed.
func TestFromPodNoStatuses(t *testing.T) {
	pod, byID := FromPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns", UID: "u"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "app:1"}}},
	})
	if len(pod.Containers) != 1 || pod.Containers[0].State != "" || pod.Containers[0].ID != "" {
		t.Fatalf("containers = %+v", pod.Containers)
	}
	if len(byID) != 0 {
		t.Fatalf("byID = %v", byID)
	}
	if pod.Labels != nil || pod.Annotations != nil {
		t.Fatalf("empty maps must stay nil: %+v", pod)
	}
}
