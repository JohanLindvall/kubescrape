// Package attrs maps kubescrape metadata onto OpenTelemetry resource
// attributes, following the k8s semantic conventions (and the
// k8sattributes-processor conventions for labels).
package attrs

import (
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/kubemeta"
)

// Pod sets the pod-level resource attributes.
func Pod(res pcommon.Resource, pod kubemeta.Pod) {
	a := res.Attributes()
	a.PutStr("k8s.namespace.name", pod.Namespace)
	a.PutStr("k8s.pod.name", pod.Name)
	a.PutStr("k8s.pod.uid", pod.UID)
	if pod.NodeName != "" {
		a.PutStr("k8s.node.name", pod.NodeName)
	}

	serviceName := pod.Name
	for _, o := range pod.Owners {
		switch o.Kind {
		case "ReplicaSet":
			a.PutStr("k8s.replicaset.name", o.Name)
		case "Deployment":
			a.PutStr("k8s.deployment.name", o.Name)
			serviceName = o.Name
		case "StatefulSet":
			a.PutStr("k8s.statefulset.name", o.Name)
			serviceName = o.Name
		case "DaemonSet":
			a.PutStr("k8s.daemonset.name", o.Name)
			serviceName = o.Name
		case "Job":
			a.PutStr("k8s.job.name", o.Name)
			serviceName = o.Name
		case "CronJob":
			a.PutStr("k8s.cronjob.name", o.Name)
			serviceName = o.Name
		}
	}
	a.PutStr("service.name", serviceName)

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
