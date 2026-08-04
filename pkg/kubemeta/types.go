// Package kubemeta is the metadata model the kubescrape service serves over
// HTTP — the wire contract for its API, so clients can decode responses
// without redeclaring the types (pkg/metaclient does exactly that).
//
// It also holds the conversion from Kubernetes API objects into that model
// (FromPod) and NormalizeContainerID, which reduces the runtime-prefixed
// container IDs Kubernetes reports ("containerd://<hex>", "docker://<hex>")
// to the bare ID the API and the container runtimes' log filenames use.
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

// NodeMetadata is the response of the node metadata endpoint.
type NodeMetadata struct {
	Name string `json:"name"`
	ObjectMeta
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
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	UID       string `json:"uid"`
	NodeName  string `json:"nodeName,omitempty"`
	PodIP     string `json:"podIP,omitempty"`
	// PodIPs are ALL addresses assigned to the pod — the same list Kubernetes
	// reports in status.podIPs, with PodIP as its first element. On a
	// dual-stack cluster a connection can arrive from the family that is not
	// status.podIP, so indexing only that one made /v1/self and /v1/pod-ips
	// (and with them agent self-attribution and the ingest peer-IP fallback)
	// silently unresolvable, with no signal distinguishing it from any other
	// 404.
	PodIPs      []string `json:"podIPs,omitempty"`
	HostIP      string   `json:"hostIP,omitempty"`
	HostNetwork bool     `json:"hostNetwork,omitempty"`
	Phase       string   `json:"phase,omitempty"`
	// Ready mirrors the pod's PodReady status condition: the pod passes its
	// readiness probes and its Service endpoints are serving traffic. The
	// phase alone does not say this — a Running pod may be failing every
	// probe.
	Ready       bool              `json:"ready"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
	CreatedAt   time.Time         `json:"createdAt"`
	StartedAt   *time.Time        `json:"startedAt,omitempty"`
	// DeletionTimestamp is set once the pod has been marked for deletion and
	// is draining. The phase stays Running for the whole termination grace
	// period, so this is the ONLY signal that a pod is on its way out —
	// terminating pods are excluded from scrape targets (as Prometheus'
	// endpoints discovery excludes terminating endpoints) but stay fully
	// resolvable by container ID, UID and name so their last logs and
	// telemetry remain attributable.
	DeletionTimestamp *time.Time `json:"deletionTimestamp,omitempty"`
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
	// Service is set when Source is "service" or "servicemonitor".
	Service *Service `json:"service,omitempty"`
	// Monitor names the ServiceMonitor/PodMonitor that produced the
	// target ("ns/name").
	Monitor string `json:"monitor,omitempty"`
	// InsecureSkipVerify scrapes an https target without verifying its
	// certificate (from the monitor endpoint's tlsConfig).
	InsecureSkipVerify bool `json:"insecureSkipVerify,omitempty"`
	// AuthSecret references a bearer-token Secret as "namespace/name/key";
	// agents resolve it via GET /v1/scrape-auth/{ns}/{name}/{key} (served
	// only when the metadata service runs with -scrape-auth-secrets).
	AuthSecret string `json:"authSecret,omitempty"`
	// BasicAuthUser/Pass, AuthType/AuthCredentials and the TLS* fields carry the
	// endpoint's remaining auth material as "namespace/name/key" secret
	// references, resolved by agents through the same /v1/scrape-auth channel as
	// AuthSecret (so they are served only when the service runs
	// -scrape-auth-secrets). TLSServerName is a literal, not a reference.
	BasicAuthUser   string `json:"basicAuthUser,omitempty"`
	BasicAuthPass   string `json:"basicAuthPass,omitempty"`
	AuthType        string `json:"authType,omitempty"`
	AuthCredentials string `json:"authCredentials,omitempty"`
	TLSCA           string `json:"tlsCA,omitempty"`
	TLSCert         string `json:"tlsCert,omitempty"`
	TLSKey          string `json:"tlsKey,omitempty"`
	TLSServerName   string `json:"tlsServerName,omitempty"`
	// Interval and ScrapeTimeout override the agent's -scrape-interval and
	// -scrape-timeout for this target (Go duration strings; empty = the agent's
	// default). Set from a ServiceMonitor/PodMonitor endpoint's own cadence.
	Interval string `json:"interval,omitempty"`
	// ScrapeTimeout bounds one scrape of this target; it is clamped to the
	// effective interval by the agent, since a scrape outliving its own period
	// would overlap the next one.
	ScrapeTimeout string `json:"scrapeTimeout,omitempty"`
	// MetricRelabelings is the keep/drop subset of the endpoint's
	// metricRelabelings, applied per sample by the agent.
	MetricRelabelings []RelabelRule `json:"metricRelabelings,omitempty"`
	Pod               Pod           `json:"pod"`
}

// RelabelRule is the keep/drop subset of a Prometheus relabel_config:
// sourceLabels values joined by ";" ("__name__" = metric name) matched
// against Regex (fully anchored, Prometheus semantics).
type RelabelRule struct {
	Action       string   `json:"action"` // keep | drop
	SourceLabels []string `json:"sourceLabels,omitempty"`
	Regex        string   `json:"regex,omitempty"`
}
