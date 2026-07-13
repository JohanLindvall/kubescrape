package kubemeta

import (
	"strings"
	"unicode"

	corev1 "k8s.io/api/core/v1"
)

// NormalizeContainerID strips the runtime scheme prefix from a container ID,
// so "containerd://abc", "docker://abc" and "abc" all normalize to "abc".
// It also tolerates a collapsed "containerd:/abc" form (as produced by HTTP
// path cleaning). Container runtime IDs themselves never contain a colon.
//
// It is idempotent — the same ID may be normalized again on another path
// (agent, HTTP handler), which must be a no-op. Hence the cut at the LAST
// colon (the result can never retain one) and the space trim after the
// slashes (a malformed "scheme:// id" must not need two passes).
func NormalizeContainerID(id string) string {
	id = strings.TrimSpace(id)
	if i := strings.LastIndexByte(id, ':'); i >= 0 {
		id = strings.TrimLeftFunc(id[i+1:], func(r rune) bool { return r == '/' || unicode.IsSpace(r) })
	}
	return id
}

// FromPod converts an API pod into the served metadata model. It returns the
// pod plus an index of all known container runtime IDs (normalized) to their
// container metadata, including the previous incarnation of restarted
// containers when the kubelet still reports it in lastState.
//
// The returned Pod shares no memory with the input object; informer cache
// objects must not be retained or mutated.
func FromPod(p *corev1.Pod) (Pod, map[string]Container) {
	pod := Pod{
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
	// Container names are unique across all three lists in the pod spec.
	collect(p.Status.InitContainerStatuses)
	collect(p.Status.ContainerStatuses)
	collect(p.Status.EphemeralContainerStatuses)

	byID := make(map[string]Container)
	add := func(name, image string, ports []corev1.ContainerPort, typ string) {
		c := Container{Name: name, Type: typ, Image: image}
		for _, pt := range ports {
			c.Ports = append(c.Ports, ContainerPort{
				Name:     pt.Name,
				Port:     pt.ContainerPort,
				Protocol: string(pt.Protocol),
			})
		}
		st := statuses[name]
		if st != nil {
			c.RuntimeID = st.ContainerID
			c.ID = NormalizeContainerID(st.ContainerID)
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
			prev.ID = NormalizeContainerID(prev.RuntimeID)
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

func fillTerminated(c *Container, t *corev1.ContainerStateTerminated) {
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
