// Package kubemeta defines the metadata model served over HTTP and the
// conversion from Kubernetes API objects into that model.
package kubemeta

import "time"

// Owner identifies one object in a pod's ownership chain, e.g. a
// ReplicaSet and the Deployment that owns it. Labels and Annotations are
// filled for kinds the service keeps metadata informers for (ReplicaSets,
// Deployments, Jobs, CronJobs).
type Owner struct {
	APIVersion  string            `json:"apiVersion"`
	Kind        string            `json:"kind"`
	Name        string            `json:"name"`
	UID         string            `json:"uid"`
	Controller  bool              `json:"controller,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

// ObjectMeta is the identifying metadata of a related object, e.g. the
// pod's namespace.
type ObjectMeta struct {
	UID         string            `json:"uid,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

// ContainerPort is a port declared on a container spec.
type ContainerPort struct {
	Name     string `json:"name,omitempty"`
	Port     int32  `json:"port"`
	Protocol string `json:"protocol,omitempty"`
}

// Container combines the spec and status of a single container.
type Container struct {
	Name string `json:"name"`
	// Type is "container", "init" or "ephemeral".
	Type string `json:"type"`
	// ID is the container runtime ID without the runtime scheme prefix.
	ID string `json:"id,omitempty"`
	// RuntimeID is the ID as reported by the kubelet, e.g. "containerd://abc...".
	RuntimeID     string          `json:"runtimeId,omitempty"`
	Image         string          `json:"image,omitempty"`
	ImageID       string          `json:"imageId,omitempty"`
	Ports         []ContainerPort `json:"ports,omitempty"`
	RestartCount  int32           `json:"restartCount"`
	Ready         bool            `json:"ready"`
	State         string          `json:"state,omitempty"` // running | waiting | terminated
	WaitingReason string          `json:"waitingReason,omitempty"`
	StartedAt     *time.Time      `json:"startedAt,omitempty"`
	FinishedAt    *time.Time      `json:"finishedAt,omitempty"`
	ExitCode      *int32          `json:"exitCode,omitempty"`
}

// Pod is the full metadata set for one pod.
type Pod struct {
	Name        string            `json:"name"`
	Namespace   string            `json:"namespace"`
	UID         string            `json:"uid"`
	NodeName    string            `json:"nodeName,omitempty"`
	PodIP       string            `json:"podIP,omitempty"`
	HostIP      string            `json:"hostIP,omitempty"`
	Phase       string            `json:"phase,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
	CreatedAt   time.Time         `json:"createdAt"`
	StartedAt   *time.Time        `json:"startedAt,omitempty"`
	// DeletedAt is set when the pod has been deleted from the cluster and
	// this metadata is served from the tombstone cache.
	DeletedAt *time.Time `json:"deletedAt,omitempty"`
	// NamespaceMetadata is the metadata of the pod's namespace.
	NamespaceMetadata *ObjectMeta `json:"namespaceMetadata,omitempty"`
	Owners            []Owner     `json:"owners,omitempty"`
	Containers        []Container `json:"containers"`
}

// ContainerMetadata is the response for a container-ID lookup.
type ContainerMetadata struct {
	ContainerID string    `json:"containerId"`
	Container   Container `json:"container"`
	Pod         Pod       `json:"pod"`
}

// Service identifies a Service whose annotations produced a scrape target.
type Service struct {
	Name        string            `json:"name"`
	Namespace   string            `json:"namespace"`
	UID         string            `json:"uid"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

// ScrapeTarget is one Prometheus endpoint, derived either from a pod's own
// prometheus.io/* annotations (source "pod") or from those of a Service
// selecting the pod (source "service").
type ScrapeTarget struct {
	URL     string `json:"url"`
	Scheme  string `json:"scheme"`
	Address string `json:"address"`
	Port    int32  `json:"port"`
	Path    string `json:"path"`
	Source  string `json:"source"`
	// Service is set when Source is "service".
	Service *Service `json:"service,omitempty"`
	Pod     Pod      `json:"pod"`
}
