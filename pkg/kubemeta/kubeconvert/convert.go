// Package kubeconvert converts Kubernetes API objects into the kubemeta wire
// model. It lives apart from kubemeta so that clients which only decode the
// model (pkg/metaclient and anything like it) do not compile k8s.io/api and
// its dependency tree; only the service side, which watches real Kubernetes
// objects, needs this package.
package kubeconvert

import (
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// podIPs copies status.podIPs, falling back to the single status.podIP for
// clusters (and fakes) that only populate that one.
func podIPs(p *corev1.Pod) []string {
	if len(p.Status.PodIPs) == 0 {
		if p.Status.PodIP == "" {
			return nil
		}
		return []string{p.Status.PodIP}
	}
	out := make([]string, 0, len(p.Status.PodIPs))
	for _, ip := range p.Status.PodIPs {
		if ip.IP != "" {
			out = append(out, ip.IP)
		}
	}
	return out
}

// FromPod converts an API pod into the served metadata model. It returns the
// pod plus an index of all known container runtime IDs (normalized) to their
// container metadata, including the previous incarnation of restarted
// containers when the kubelet still reports it in lastState.
//
// The returned kubemeta.Pod shares no memory with the input object; informer
// cache objects must not be retained or mutated. That deep-copy guarantee is
// what the store's treat-as-immutable contract rests on, so it belongs here,
// on the exported function — it spent a while attached to the unexported
// podIPs below, where `go doc` and pkg.go.dev showed this function as a bare
// signature.
func FromPod(p *corev1.Pod) (kubemeta.Pod, map[string]kubemeta.Container) {
	labels, annotations := kubemeta.CopyMeta(p.Labels, p.Annotations)
	pod := kubemeta.Pod{
		Name:        p.Name,
		Namespace:   p.Namespace,
		UID:         string(p.UID),
		NodeName:    p.Spec.NodeName,
		PodIP:       p.Status.PodIP,
		PodIPs:      podIPs(p),
		HostIP:      p.Status.HostIP,
		HostNetwork: p.Spec.HostNetwork,
		Phase:       string(p.Status.Phase),
		Labels:      labels,
		Annotations: annotations,
		CreatedAt:   p.CreationTimestamp.Time,
	}
	if p.Status.StartTime != nil {
		t := p.Status.StartTime.Time
		pod.StartedAt = &t
	}
	if p.DeletionTimestamp != nil {
		// The pod is draining. Its phase stays Running for the whole grace
		// period, so this is the only signal that it is going away; the store
		// derives record.terminating from it and scrape.Scrapeable drops it
		// from targets.
		t := p.DeletionTimestamp.Time
		pod.DeletionTimestamp = &t
	}
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			pod.Ready = c.Status == corev1.ConditionTrue
			break
		}
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
		st := statuses[name]
		c := convertContainer(name, image, ports, typ, st)
		pod.Containers = append(pod.Containers, c)
		if c.ID != "" {
			byID[c.ID] = cloneContainer(c) // shares nothing: the model is self-contained
		}
		if prev, ok := previousIncarnation(c, st); ok {
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

// convertContainer builds the model for one declared container, folding in
// its runtime status (state, IDs, restart count) when the kubelet has
// reported one.
func convertContainer(name, image string, ports []corev1.ContainerPort, typ string, st *corev1.ContainerStatus) kubemeta.Container {
	c := kubemeta.Container{Name: name, Type: typ, Image: image}
	for _, pt := range ports {
		c.Ports = append(c.Ports, kubemeta.ContainerPort{
			Name:     pt.Name,
			Port:     pt.ContainerPort,
			Protocol: string(pt.Protocol),
		})
	}
	if st == nil {
		return c
	}
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
	return c
}

// previousIncarnation models the container's PREVIOUS runtime incarnation
// when the kubelet reports one in lastState: a restarted container gets a new
// runtime ID, and lookups by the old ID must keep resolving while the pod is
// alive.
func previousIncarnation(c kubemeta.Container, st *corev1.ContainerStatus) (kubemeta.Container, bool) {
	if st == nil || st.LastTerminationState.Terminated == nil || st.LastTerminationState.Terminated.ContainerID == "" {
		return kubemeta.Container{}, false
	}
	prevID := kubemeta.NormalizeContainerID(st.LastTerminationState.Terminated.ContainerID)
	// The "new runtime ID" premise above FAILS during CrashLoopBackOff: between
	// restarts the kubelet leaves status.containerID equal to
	// lastState.terminated.containerID (observed live), so there is no distinct
	// previous incarnation — the two describe one runtime container, and the
	// caller indexes both under the same key. Indexing the historical view would
	// clobber the live one, and GET /v1/containers/{id} would then answer
	// "terminated" + an exitCode for an ID the pod document in the SAME response
	// reports as waiting/CrashLoopBackOff (and would carry the CURRENT
	// restartCount and image on a record presented as history). The current
	// status is authoritative for its own ID; do not remove this guard.
	//
	// WHAT THE GUARD COSTS, said plainly because the API model cannot hold both
	// views at once (kubemeta.Container has no last-terminated section): for the
	// duration of the backoff window, that ID's entry carries the LIVE
	// container's state — waiting + waitingReason=CrashLoopBackOff — and
	// therefore serves NO exitCode, startedAt or finishedAt for the run that
	// just crashed. Attribution is unaffected (pod, namespace, container name,
	// image, labels, owners are the same either way); only the crashed run's
	// exit detail is missing, and nothing in this model reports it during the
	// window — a consumer that needs it must read the pod's
	// status.lastState.terminated from the Kubernetes API. The answer for that
	// one ID also CHANGES when the kubelet finally
	// starts the next incarnation: status.containerID becomes the new
	// container's, this ID becomes a genuine previous incarnation, and it is
	// served terminated + exitCode from then on (until the pod's tombstone TTL).
	// Both are deliberate. A single ID must have exactly ONE state in a response
	// — serving the terminated view under an ID the same body reports as live
	// makes the two halves contradict each other, and every consumer keying off
	// the container ID (the tailer attributing the log file the container is
	// about to write to again, the cadvisor router) would then attribute LIVE
	// data to a record marked terminated with an exit code. Missing history is
	// recoverable; a self-contradicting response is not.
	if prevID == c.ID {
		return kubemeta.Container{}, false
	}
	// A distinct incarnation: it shares nothing with the current one. Only the
	// Ports slice is cloned, not the whole container — the three time/exit
	// pointers cloneContainer copies are cleared on the next lines and refilled
	// from lastState, so cloning them allocates up to three values per restarted
	// container per upsert, on the informer callback path, to discard them.
	prev := c
	prev.Ports = clonePorts(c.Ports)
	prev.RuntimeID = st.LastTerminationState.Terminated.ContainerID
	prev.ID = prevID
	prev.Ready = false
	prev.State = "terminated"
	prev.WaitingReason = ""
	prev.StartedAt = nil
	prev.FinishedAt = nil
	prev.ExitCode = nil
	fillTerminated(&prev, st.LastTerminationState.Terminated)
	return prev, true
}

// cloneContainer returns a copy of c that shares no memory with it, so a
// container value stored in a second place (byID, or a prior incarnation) is
// independent of the one appended to pod.Containers. The struct copy alone
// leaves the slice and every POINTER field aliased — convertContainer allocates
// StartedAt/FinishedAt/ExitCode once per container and both views then point at
// it. Nothing mutates them today; this is a public model whose two views must
// not become a trap for the code that eventually does.
func cloneContainer(c kubemeta.Container) kubemeta.Container {
	c.Ports = clonePorts(c.Ports)
	c.StartedAt = cloneTime(c.StartedAt)
	c.FinishedAt = cloneTime(c.FinishedAt)
	if c.ExitCode != nil {
		code := *c.ExitCode
		c.ExitCode = &code
	}
	return c
}

// clonePorts copies a container's declared ports into their own array (nil for
// nil, so an absent list stays absent).
func clonePorts(ports []kubemeta.ContainerPort) []kubemeta.ContainerPort {
	if ports == nil {
		return nil
	}
	return append([]kubemeta.ContainerPort(nil), ports...)
}

func cloneTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	v := *t
	return &v
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
