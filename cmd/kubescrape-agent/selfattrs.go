package main

// The agent's half of the self-metadata feature (internal/selfmeta): resolving
// the pod THIS agent runs in through the metadata service, and the attribute
// build applied to the metrics it generates about itself — its self-metrics
// and the span metrics derived from ingested traces. Both are built from
// agentSelfResource, which knows only what the process itself can see
// (service.name, version, node).

import (
	"context"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/selfmeta"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/metaclient"
)

// selfResolver resolves the agent's own pod through the metadata service,
// which attributes the request by its connection's source address.
func selfResolver(meta *metaclient.Client) selfmeta.Resolve {
	return func(ctx context.Context) (*kubemeta.Pod, error) { return meta.Self(ctx) }
}

// selfBuild runs the configured `self` attribute pipeline over the agent's own
// pod. Container is deliberately left nil: the pod's containers are known, but
// which of them is this process is not (a sidecar injector makes "the only
// one" a guess), and guessing would put a wrong k8s.container.name on every
// self-metric.
func selfBuild(b *attrs.Builder, node func() *attrs.NodeInfo) selfmeta.Build {
	return func(res pcommon.Resource, pod *kubemeta.Pod) {
		actx := attrs.Context{Pod: pod}
		if node != nil {
			actx.Node = node()
		}
		b.Build(res, actx) // nil-receiver safe: defaults, no filter
	}
}
