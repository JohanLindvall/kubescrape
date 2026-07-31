package main

// The metadata service's half of the self-metadata feature
// (internal/selfmeta): the service's own self-metrics are exported under a
// resource it builds unaided (service.name=kubescrape + hostname), which says
// nothing about the pod they came from.
//
// Unlike the agent it needs no HTTP call — every pod in the cluster is already
// in its store, including its own. It only needs its own identity, and that
// takes no downward-API env var either: the pod name is the hostname, and the
// namespace comes from $POD_NAMESPACE or the ServiceAccount projection this
// process already mounts for its in-cluster credentials.

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/selfmeta"
	"github.com/JohanLindvall/kubescrape/internal/server"
	"github.com/JohanLindvall/kubescrape/internal/store"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// selfResolver looks the service's own pod up in its own store, enriching it
// with the owner chain and namespace metadata exactly as the HTTP handlers do.
// It fails until the pod informer has delivered this pod (the lookup retries),
// and keeps failing — harmlessly, leaving the bare identity in place — when
// the process is not a pod whose name is its hostname.
func selfResolver(st *store.Store, resolver server.MetadataResolver) func(context.Context) (*kubemeta.Pod, error) {
	return func(context.Context) (*kubemeta.Pod, error) {
		ns := selfmeta.Namespace()
		if ns == "" {
			return nil, fmt.Errorf("no namespace ($POD_NAMESPACE or the ServiceAccount projection)")
		}
		host, err := os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("hostname: %w", err)
		}
		np, ok := st.GetPodByName(ns, host)
		if !ok {
			obs.SelfMetadataLookups.WithLabelValues("error").Inc()
			return nil, fmt.Errorf("pod %s/%s not in the store", ns, host)
		}
		obs.SelfMetadataLookups.WithLabelValues("by_name").Inc()
		pod := np.Pod
		pod.Owners = resolver.Resolve(pod.Namespace, np.OwnerRefs)
		pod.NamespaceMetadata = resolver.Namespace(pod.Namespace)
		return &pod, nil
	}
}

// selfBuild applies the built-in k8s attribute mapping. The service has no
// resourceAttributes config (that is the agent's surface), so this is the
// defaults plus the derived Mimir identity — and it is applied fill-if-absent,
// so service.name stays "kubescrape" and service.instance.id stays the
// hostname.
func selfBuild(res pcommon.Resource, pod *kubemeta.Pod) {
	// pod is nil until the store has this process's own pod (and forever if it
	// never does): the build must still run, and must not dereference it.
	if pod != nil {
		attrs.Pod(res, *pod)
	}
	attrs.Identity(res)
}
