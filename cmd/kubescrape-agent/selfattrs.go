package main

// The agent's half of the self-metadata feature (internal/selfmeta): how it
// finds the pod it runs in, and the attribute build applied to the metrics it
// generates about itself — its self-metrics and the span metrics derived from
// ingested traces. Both are built from agentSelfResource, which knows only
// what the process itself can see (service.name, version, node, namespace).

import (
	"context"
	"fmt"
	"os"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/selfmeta"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/metaclient"
)

// selfResolve resolves the pod THIS agent runs in.
//
// GET /v1/self first: the service attributes the caller by its connection's
// source address, so nothing has to be wired into the deployment. That address
// is not always the pod's own, though — a hostNetwork agent shares the node
// IP, a NAT hop or proxy replaces it, and on a dual-stack cluster the
// connection may use the family status.podIP does not carry — so a failure
// falls back to a lookup BY NAME, the way the metadata service resolves
// itself. The name comes from $POD_NAME or the hostname (Kubernetes sets it
// from the pod name), the namespace from $POD_NAMESPACE or the ServiceAccount
// projection; with neither available the original error stands.
func selfResolve(meta *metaclient.Client) func(context.Context) (*kubemeta.Pod, error) {
	return func(ctx context.Context) (*kubemeta.Pod, error) {
		pod, err := meta.Self(ctx)
		if err == nil {
			obs.SelfMetadataLookups.WithLabelValues(obs.SelfLookupSelf).Inc()
			return pod, nil
		}
		ns, name := selfmeta.Namespace(), selfPodName()
		if ns == "" || name == "" {
			obs.SelfMetadataLookups.WithLabelValues(obs.SelfLookupError).Inc()
			return nil, err
		}
		byName, nameErr := meta.PodByName(ctx, ns, name)
		if nameErr != nil {
			obs.SelfMetadataLookups.WithLabelValues(obs.SelfLookupError).Inc()
			return nil, fmt.Errorf("peer-address lookup: %w; by name %s/%s: %w", err, ns, name, nameErr)
		}
		obs.SelfMetadataLookups.WithLabelValues(obs.SelfLookupByName).Inc()
		return byName, nil
	}
}

// selfInstanceName is the pod name agentSelfResource uses as a singleton's /
// shard's service.instance.id, indirected through a var so a test can drive the
// empty-name case a hostNetwork pod without $POD_NAME produces (selfPodName
// reads os.Hostname, which is not env-forceable).
var selfInstanceName = selfPodName

// selfPodName is this pod's name: $POD_NAME when the deployment wires it, else
// the hostname.
//
// The hostname is only a usable fallback for a pod with its OWN UTS namespace,
// where Kubernetes sets it from the pod name. A hostNetwork pod shares the
// host's, so os.Hostname() is the NODE's name — and hostNetwork is precisely
// the case this by-name path exists to cover, since such a pod can never be
// attributed by peer address. $POD_NAME is therefore wired into the shipped
// manifests, and looking up a pod named after the node simply 404s (the
// resolver reports the failure rather than attributing anything).
func selfPodName() string {
	if n := strings.TrimSpace(os.Getenv("POD_NAME")); n != "" {
		return n
	}
	h, err := os.Hostname()
	if err != nil {
		return ""
	}
	return podHostname(h, *nodeName)
}

// podHostname returns the hostname unless it names the NODE, in which case
// this process shares the host's UTS namespace (hostNetwork) and the hostname
// is not a pod name at all — the caller gets "" and skips the by-name lookup.
//
// The node name is the evidence because it is already MANDATORY (-node-name /
// $NODE_NAME, checked in run()) and a hostNetwork pod's hostname IS the node's,
// which is the only case that matters here. The /proc comparison this replaced
// asked whether pid 1 lives in our network namespace, and every realistic way
// for the agent not to be pid 1 — shareProcessNamespace's pause container, an
// init wrapper like tini — puts pid 1 in the POD's namespace, so it answered
// "hostNetwork" for every pod and cost each one the fallback.
//
// A MISMATCH keeps the hostname, which is the safe direction: a wrong "no"
// (a kubelet whose --hostname-override differs from the registered node name)
// costs a lookup for a pod named after the node, which 404s and is reported,
// while a wrong "yes" would skip the by-name resolve outright and leave a
// singleton's service.instance.id to fall back to the node — colliding with
// that node's DaemonSet agent on one (job, instance).
func podHostname(hostname, node string) string {
	if node != "" && strings.EqualFold(strings.TrimSpace(hostname), strings.TrimSpace(node)) {
		return ""
	}
	return hostname
}

// selfBuild runs the configured `self` attribute pipeline over the agent's own
// pod. What it produces is applied fill-if-absent, so the pipeline can only
// ADD attributes — a template setting service.name or service.instance.id for
// these metrics is deliberately ineffective (see selfmeta.Wrap).
//
// pod is NIL until the lookup succeeds, and stays nil where it cannot: the
// build still runs, because the section's static attributes (a cluster name)
// are how an operator selects the very metrics that report the failure.
//
// Container is left nil: the pod's containers are known, but which of them is
// this process is not (a sidecar injector makes "the only one" a guess), and
// guessing would put a wrong k8s.container.name on every self-metric.
func selfBuild(b *attrs.Builder, node func() *attrs.NodeInfo) selfmeta.Build {
	return func(res pcommon.Resource, pod *kubemeta.Pod) {
		actx := attrs.Context{Pod: pod}
		if node != nil {
			actx.Node = node()
		}
		b.Build(res, actx) // nil-receiver safe: defaults, no filter
	}
}
