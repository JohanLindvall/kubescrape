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
// reads /proc and os.Hostname, neither env-forceable).
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
	if hostNetworkSelf() {
		return "" // the node's hostname is not this pod's name
	}
	h, err := os.Hostname()
	if err != nil {
		return ""
	}
	return h
}

// hostNetworkSelf reports whether this process shares the host's network
// namespace, in which case the hostname names the node rather than the pod.
// /proc/self/ns/net is compared with PID 1's, which is the HOST's init only
// under `hostPID: true` — no shipped manifest sets it — so the comparison is
// evidence of nothing unless /proc/1 is provably somebody else.
//
// An unprovable comparison is INCONCLUSIVE and keeps the hostname fallback,
// which is the safe direction: a wrong "yes" costs an ordinary pod the name
// Kubernetes put in its hostname (the by-name resolve is skipped and a
// singleton's service.instance.id falls back to the node, colliding with that
// node's DaemonSet agent), while a wrong "no" costs a lookup for a pod named
// after the node, which 404s and is reported.
func hostNetworkSelf() bool { return hostNetworkProc(os.Getpid(), os.Readlink) }

// hostNetworkProc is hostNetworkSelf over an injected pid and readlink.
func hostNetworkProc(pid int, readlink func(string) (string, error)) bool {
	if pid == 1 {
		// /proc/1 is THIS process — the shipped image runs the agent as the
		// container's entrypoint — so the comparison is self-vs-self and would
		// claim hostNetwork for every pod that can read the link.
		return false
	}
	self, err := readlink("/proc/self/ns/net")
	if err != nil {
		return false
	}
	host, err := readlink("/proc/1/ns/net")
	if err != nil {
		return false // cannot tell; keep the hostname fallback
	}
	return self == host
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
