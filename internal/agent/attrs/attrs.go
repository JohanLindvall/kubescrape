// Package attrs maps kubescrape metadata onto OpenTelemetry resource
// attributes, following the k8s semantic conventions (and the
// k8sattributes-processor conventions for labels).
package attrs

import (
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// ServiceName derives the OTLP service.name for a pod: the name of its
// workload owner (Deployment/StatefulSet/DaemonSet/Job/CronJob), falling back
// to the pod name. A ReplicaSet owner is not used (its Deployment is).
func ServiceName(pod kubemeta.Pod) string {
	name := pod.Name
	for _, o := range pod.Owners {
		switch o.Kind {
		case "Deployment", "StatefulSet", "DaemonSet", "Job", "CronJob":
			name = o.Name
		}
	}
	return name
}

// KindAttribute maps a Kubernetes object kind to its k8s.<kind>.name resource
// attribute (e.g. "Deployment" -> "k8s.deployment.name"); ok is false for a kind
// with no such attribute. Shared by the pod owner-chain and the events exporter.
func KindAttribute(kind string) (string, bool) {
	switch kind {
	case "ReplicaSet":
		return "k8s.replicaset.name", true
	case "Deployment":
		return "k8s.deployment.name", true
	case "StatefulSet":
		return "k8s.statefulset.name", true
	case "DaemonSet":
		return "k8s.daemonset.name", true
	case "Job":
		return "k8s.job.name", true
	case "CronJob":
		return "k8s.cronjob.name", true
	case "Node":
		return "k8s.node.name", true
	}
	return "", false
}

// Pod sets the pod-level resource attributes.
func Pod(res pcommon.Resource, pod kubemeta.Pod) {
	a := res.Attributes()
	a.PutStr("k8s.namespace.name", pod.Namespace)
	a.PutStr("k8s.pod.name", pod.Name)
	a.PutStr("k8s.pod.uid", pod.UID)
	if pod.NodeName != "" {
		a.PutStr("k8s.node.name", pod.NodeName)
	}
	if pod.PodIP != "" {
		a.PutStr("k8s.pod.ip", pod.PodIP)
	}

	for _, o := range pod.Owners {
		if attr, ok := KindAttribute(o.Kind); ok {
			a.PutStr(attr, o.Name)
		}
	}
	a.PutStr("service.name", ServiceName(pod))

	for k, v := range pod.Labels {
		a.PutStr("k8s.pod.label."+k, v)
	}
	if pod.NamespaceMetadata != nil {
		for k, v := range pod.NamespaceMetadata.Labels {
			a.PutStr("k8s.namespace.label."+k, v)
		}
	}
}

// Container adds the container-level resource attributes on top of Pod's.
func Container(res pcommon.Resource, c kubemeta.Container) {
	a := res.Attributes()
	a.PutStr("k8s.container.name", c.Name)
	if c.ID != "" {
		a.PutStr("container.id", c.ID)
	}
	if c.Image != "" {
		a.PutStr("container.image.name", c.Image)
	}
	if c.RestartCount > 0 {
		a.PutInt("k8s.container.restart_count", int64(c.RestartCount))
	}
}

// Service adds attributes identifying the Service a scrape target was
// discovered through.
func Service(res pcommon.Resource, svc *kubemeta.Service) {
	if svc == nil {
		return
	}
	a := res.Attributes()
	a.PutStr("k8s.service.name", svc.Name)
	a.PutStr("k8s.service.uid", svc.UID)
}

// Identity sets service.namespace and service.instance.id so a Prometheus
// backend (e.g. Mimir) derives a unique job (service.namespace/service.name)
// and instance (service.instance.id). It reads the identity attributes already
// on the resource, so it works for pod/container/node resources as well as
// pre-populated ones (kube-state-metrics splitters, ingest, log metrics).
// Neither attribute is overwritten if already set, so a template still wins.
//
// service.instance.id falls back in order: container.id, pod.uid[/container],
// namespace/pod[/container], node.name — mirroring the cmb-alloy pipeline.
func Identity(res pcommon.Resource) {
	a := res.Attributes()
	get := func(k string) string {
		if v, ok := a.Get(k); ok {
			return v.AsString()
		}
		return ""
	}
	ns := get("k8s.namespace.name")
	if ns != "" {
		if _, ok := a.Get("service.namespace"); !ok {
			a.PutStr("service.namespace", ns)
		}
	}
	if _, ok := a.Get("service.instance.id"); ok {
		return
	}
	pod, container := get("k8s.pod.name"), get("k8s.container.name")
	uid, cid, node := get("k8s.pod.uid"), get("container.id"), get("k8s.node.name")
	var inst string
	switch {
	case cid != "":
		inst = cid
	case uid != "" && container != "":
		inst = uid + "/" + container
	case uid != "":
		inst = uid
	case ns != "" && pod != "" && container != "":
		inst = ns + "/" + pod + "/" + container
	case ns != "" && pod != "":
		inst = ns + "/" + pod
	case node != "":
		inst = node
	}
	if inst != "" {
		a.PutStr("service.instance.id", inst)
	}
}

// PrefixInstance prepends prefix (+ "-") to service.instance.id so resources
// produced by an exporter that DESCRIBES other objects — cadvisor, or a
// kube-state-metrics splitter — get an instance distinct from those objects'
// own self-scraped metrics (which share the same service.name / namespace).
// Without it the two collide on (job, instance) with different resource
// attributes, flapping target_info. Mirrors cmb-alloy's instance_prefix. A
// resource with no service.instance.id is left alone — a bare prefix would
// stamp every such resource with the same meaningless instance. No-op for "".
func PrefixInstance(res pcommon.Resource, prefix string) {
	if prefix == "" {
		return
	}
	a := res.Attributes()
	if v, ok := a.Get("service.instance.id"); ok {
		a.PutStr("service.instance.id", prefix+"-"+v.AsString())
	}
}
