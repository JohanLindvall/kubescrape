// Package kubeconvert converts Kubernetes API objects into the kubemeta wire
// model. It lives apart from kubemeta so that clients which only decode the
// model (pkg/metaclient and anything like it) do not compile k8s.io/api and
// its dependency tree; only the service side, which watches real Kubernetes
// objects, needs this package.
package kubeconvert

import (
	corev1 "k8s.io/api/core/v1"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// FromPod converts an API pod into the served metadata model. It returns the
// pod plus an index of all known container runtime IDs (normalized) to their
// container metadata, including the previous incarnation of restarted
// containers when the kubelet still reports it in lastState.
//
// The returned kubemeta.Pod shares no memory with the input object; informer cache
// objects must not be retained or mutated.
func FromPod(p *corev1.Pod) (kubemeta.Pod, map[string]kubemeta.Container) {
	pod := kubemeta.Pod{
		Name:        p.Name,
		Namespace:   p.Namespace,
		UID:         string(p.UID),
		NodeName:    p.Spec.NodeName,
		PodIP:       p.Status.PodIP,
		HostIP:      p.Status.HostIP,
		Phase:       string(p.Status.Phase),
		Labels:      cloneMap(p.Labels),
		Annotations: cloneMap(p.Annotations),
		CreatedAt:   p.CreationTimestamp.Time,
	}
	if p.Status.StartTime != nil {
		t := p.Status.StartTime.Time
		pod.StartedAt = &t
	}

	statuses := make(map[string]*corev1.ContainerStatus)
	collect := func(list []corev1.ContainerStatus) {
		for i := range list {
			statuses[list[i].Name] = &list[i]
		}
	}
	// kubemeta.Container names are unique across all three lists in the pod spec.
	collect(p.Status.InitContainerStatuses)
	collect(p.Status.ContainerStatuses)
	collect(p.Status.EphemeralContainerStatuses)

	byID := make(map[string]kubemeta.Container)
	add := func(name, image string, ports []corev1.ContainerPort, typ string) {
		c := kubemeta.Container{Name: name, Type: typ, Image: image}
		for _, pt := range ports {
			c.Ports = append(c.Ports, kubemeta.ContainerPort{
				Name:     pt.Name,
				Port:     pt.ContainerPort,
				Protocol: string(pt.Protocol),
			})
		}
		st := statuses[name]
		if st != nil {
			c.RuntimeID = st.ContainerID
			c.ID = kubemeta.NormalizeContainerID(st.ContainerID)
			c.ImageID = st.ImageID
			c.RestartCount = st.RestartCount
			c.Ready = st.Ready
			switch {
			case st.State.Running != nil:
				c.State = "running"
				t := st.State.Running.StartedAt.Time
				c.StartedAt = &t
			case st.State.Terminated != nil:
				c.State = "terminated"
				fillTerminated(&c, st.State.Terminated)
			case st.State.Waiting != nil:
				c.State = "waiting"
				c.WaitingReason = st.State.Waiting.Reason
			}
		}
		pod.Containers = append(pod.Containers, c)
		if c.ID != "" {
			byID[c.ID] = c
		}
		// A restarted container gets a new runtime ID; the kubelet keeps the
		// previous incarnation in lastState. Index it too so lookups by the
		// old ID keep resolving while the pod is alive.
		if st != nil && st.LastTerminationState.Terminated != nil && st.LastTerminationState.Terminated.ContainerID != "" {
			prev := c
			prev.RuntimeID = st.LastTerminationState.Terminated.ContainerID
			prev.ID = kubemeta.NormalizeContainerID(prev.RuntimeID)
			prev.Ready = false
			prev.State = "terminated"
			prev.WaitingReason = ""
			prev.StartedAt = nil
			prev.FinishedAt = nil
			prev.ExitCode = nil
			fillTerminated(&prev, st.LastTerminationState.Terminated)
			byID[prev.ID] = prev
		}
	}

	for _, c := range p.Spec.InitContainers {
		add(c.Name, c.Image, c.Ports, "init")
	}
	for _, c := range p.Spec.Containers {
		add(c.Name, c.Image, c.Ports, "container")
	}
	for _, ec := range p.Spec.EphemeralContainers {
		add(ec.Name, ec.Image, ec.Ports, "ephemeral")
	}
	return pod, byID
}

func fillTerminated(c *kubemeta.Container, t *corev1.ContainerStateTerminated) {
	exit := t.ExitCode
	c.ExitCode = &exit
	if !t.StartedAt.IsZero() {
		st := t.StartedAt.Time
		c.StartedAt = &st
	}
	if !t.FinishedAt.IsZero() {
		ft := t.FinishedAt.Time
		c.FinishedAt = &ft
	}
}

func cloneMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
