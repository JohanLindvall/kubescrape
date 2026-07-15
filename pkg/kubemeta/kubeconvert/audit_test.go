package kubeconvert

import (
	"encoding/json"
	"sort"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

func ptrTime(t time.Time) *metav1.Time { m := metav1.NewTime(t); return &m }

// richPod builds a pod exercising every field FromPod reads: labels,
// annotations, init/regular/ephemeral containers, ports, running/waiting/
// terminated states and a lastState.terminated (restarted container).
func richPod() *corev1.Pod {
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "p",
			Namespace:         "ns",
			UID:               "uid-1",
			Labels:            map[string]string{"app": "web", "tier": "front"},
			Annotations:       map[string]string{"prometheus.io/scrape": "true"},
			CreationTimestamp: metav1.NewTime(now),
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			InitContainers: []corev1.Container{
				{Name: "init", Image: "busybox", Ports: []corev1.ContainerPort{{Name: "ip", ContainerPort: 1, Protocol: corev1.ProtocolTCP}}},
			},
			Containers: []corev1.Container{
				{Name: "app", Image: "web:1", Ports: []corev1.ContainerPort{
					{Name: "http", ContainerPort: 8080, Protocol: corev1.ProtocolTCP},
					{Name: "metrics", ContainerPort: 9090, Protocol: corev1.ProtocolTCP},
				}},
				{Name: "side", Image: "side:1"},
			},
			EphemeralContainers: []corev1.EphemeralContainer{
				{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "dbg", Image: "dbg:1"}},
			},
		},
		Status: corev1.PodStatus{
			Phase:     corev1.PodRunning,
			PodIP:     "10.1.2.3",
			HostIP:    "192.168.1.1",
			StartTime: ptrTime(now),
			InitContainerStatuses: []corev1.ContainerStatus{{
				Name: "init", ContainerID: "containerd://initid", ImageID: "sha256:i",
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ExitCode: 0, StartedAt: metav1.NewTime(now), FinishedAt: metav1.NewTime(now.Add(time.Second)),
				}},
			}},
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "app", ContainerID: "containerd://appid2", ImageID: "sha256:a", RestartCount: 1, Ready: true,
					State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(now)}},
					LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
						ContainerID: "containerd://appid1", ExitCode: 137,
						StartedAt: metav1.NewTime(now.Add(-time.Hour)), FinishedAt: metav1.NewTime(now.Add(-time.Minute)),
					}},
				},
				{
					Name: "side", ContainerID: "", Ready: false,
					State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}},
				},
			},
			EphemeralContainerStatuses: []corev1.ContainerStatus{{
				Name: "dbg", ContainerID: "containerd://dbgid",
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(now)}},
			}},
		},
	}
}

// TestAudit_FromPodDeepCopy is the load-bearing invariant: the returned model
// must share NOTHING with the informer object. Mutate every nested map, slice,
// pointer and string of the source after conversion and require the model —
// and the byID index — to be byte-for-byte unchanged.
func TestAudit_FromPodDeepCopy(t *testing.T) {
	p := richPod()
	pod, byID := FromPod(p)

	before, err := json.Marshal(struct {
		P any
		B any
	}{pod, byID})
	if err != nil {
		t.Fatal(err)
	}

	// Now vandalize the source pod in every way an informer update could.
	p.Name = "MUTATED"
	p.Namespace = "MUTATED"
	p.UID = "MUTATED"
	p.Spec.NodeName = "MUTATED"
	p.Status.PodIP = "0.0.0.0"
	p.Status.HostIP = "0.0.0.0"
	p.Status.Phase = corev1.PodFailed
	p.Labels["app"] = "MUTATED"
	p.Labels["new"] = "MUTATED"
	delete(p.Labels, "tier")
	p.Annotations["prometheus.io/scrape"] = "false"
	p.CreationTimestamp = metav1.NewTime(time.Unix(0, 0))
	*p.Status.StartTime = metav1.NewTime(time.Unix(0, 0))
	p.Status.StartTime = nil

	for i := range p.Spec.Containers {
		p.Spec.Containers[i].Name = "MUTATED"
		p.Spec.Containers[i].Image = "MUTATED"
		for j := range p.Spec.Containers[i].Ports {
			p.Spec.Containers[i].Ports[j].Name = "MUTATED"
			p.Spec.Containers[i].Ports[j].ContainerPort = 1
			p.Spec.Containers[i].Ports[j].Protocol = corev1.ProtocolUDP
		}
	}
	for i := range p.Spec.InitContainers {
		p.Spec.InitContainers[i].Ports[0].Name = "MUTATED"
	}
	for i := range p.Status.ContainerStatuses {
		st := &p.Status.ContainerStatuses[i]
		st.ContainerID = "containerd://MUTATED"
		st.ImageID = "MUTATED"
		st.RestartCount = 99
		st.Ready = !st.Ready
		if st.State.Running != nil {
			st.State.Running.StartedAt = metav1.NewTime(time.Unix(0, 0))
		}
		if st.State.Waiting != nil {
			st.State.Waiting.Reason = "MUTATED"
		}
		if st.LastTerminationState.Terminated != nil {
			st.LastTerminationState.Terminated.ContainerID = "containerd://MUTATED"
			st.LastTerminationState.Terminated.ExitCode = 99
			st.LastTerminationState.Terminated.FinishedAt = metav1.NewTime(time.Unix(0, 0))
		}
	}
	for i := range p.Status.InitContainerStatuses {
		if tm := p.Status.InitContainerStatuses[i].State.Terminated; tm != nil {
			tm.ExitCode = 99
			tm.FinishedAt = metav1.NewTime(time.Unix(0, 0))
		}
	}

	after, err := json.Marshal(struct {
		P any
		B any
	}{pod, byID})
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatalf("BUG: FromPod result aliases the informer object.\nbefore: %s\nafter:  %s", before, after)
	}

	// And the returned model's own maps must not be the source's maps.
	pod.Labels["x"] = "y"
	if _, ok := p.Labels["x"]; ok {
		t.Fatal("BUG: the model's Labels map is the pod's Labels map")
	}
}

// TestAudit_FromPodByIDIndex pins the container-ID index contract: current and
// previous (lastState) incarnations, normalized, and no empty-ID entry.
func TestAudit_FromPodByIDIndex(t *testing.T) {
	pod, byID := FromPod(richPod())

	for _, want := range []string{"initid", "appid2", "appid1", "dbgid"} {
		if _, ok := byID[want]; !ok {
			t.Errorf("byID missing %q (keys: %v)", want, keys(byID))
		}
	}
	if _, ok := byID[""]; ok {
		t.Error("byID has an empty-ID entry (the waiting container with no runtime ID)")
	}
	if len(byID) != 4 {
		t.Errorf("byID has %d entries (%v), want 4", len(byID), keys(byID))
	}

	prev := byID["appid1"]
	if prev.State != "terminated" || prev.Ready || prev.ExitCode == nil || *prev.ExitCode != 137 {
		t.Errorf("previous incarnation = %+v, want terminated/not-ready/exit 137", prev)
	}
	if prev.RuntimeID != "containerd://appid1" {
		t.Errorf("previous RuntimeID = %q", prev.RuntimeID)
	}
	if prev.RestartCount != 1 {
		t.Errorf("previous RestartCount = %d, want the status's 1 (carried from the live container)", prev.RestartCount)
	}
	cur := byID["appid2"]
	if cur.State != "running" || !cur.Ready || cur.ExitCode != nil {
		t.Errorf("current incarnation = %+v, want running/ready/no exit code", cur)
	}
	if len(prev.Ports) != 2 || len(cur.Ports) != 2 {
		t.Fatalf("ports: prev=%v cur=%v", prev.Ports, cur.Ports)
	}

	// Types.
	types := map[string]string{}
	for _, c := range pod.Containers {
		types[c.Name] = c.Type
	}
	want := map[string]string{"init": "init", "app": "container", "side": "container", "dbg": "ephemeral"}
	for k, v := range want {
		if types[k] != v {
			t.Errorf("container %q type = %q, want %q", k, types[k], v)
		}
	}
}

// TestFromPodPortsNotAliased: FromPod deep-copies from the informer object,
// but WITHIN its own result the three views of one container share a single
// Ports backing array. `prev := c` copies the struct (slice header only), and
// `c` is also appended to pod.Containers and stored in byID under the current
// ID — so pod.Containers[app].Ports, byID["appid2"].Ports and
// byID["appid1"].Ports (the previous incarnation) are the SAME array. A
// consumer that mutates one container's ports silently mutates the others.
// Benign only because the store never mutates a returned Pod in place — but the
// package doc promises the result shares no memory it should not, and this is a
// latent trap for any importer that edits the model.
func TestFromPodPortsNotAliased(t *testing.T) {
	pod, byID := FromPod(richPod())
	prev := byID["appid1"]
	cur := byID["appid2"]
	prev.Ports[0].Name = "MUTATED"
	if cur.Ports[0].Name == "MUTATED" {
		t.Error("BUG: previous and current incarnation share one Ports backing array")
	}
	for _, c := range pod.Containers {
		if c.Name == "app" && c.Ports[0].Name == "MUTATED" {
			t.Error("BUG: byID entries share their Ports backing array with pod.Containers")
		}
	}
}

func keys(m map[string]kubemeta.Container) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestAudit_FromPodEmptyPod: the zero pod must not panic and must produce a
// usable zero model.
func TestAudit_FromPodEmptyPod(t *testing.T) {
	pod, byID := FromPod(&corev1.Pod{})
	if len(byID) != 0 {
		t.Errorf("byID = %v", byID)
	}
	if pod.Labels != nil || pod.Annotations != nil {
		t.Errorf("empty maps should convert to nil, got %v / %v", pod.Labels, pod.Annotations)
	}
	if pod.StartedAt != nil {
		t.Error("StartedAt should be nil")
	}
	if len(pod.Containers) != 0 {
		t.Errorf("Containers = %v", pod.Containers)
	}
}
