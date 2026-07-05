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
