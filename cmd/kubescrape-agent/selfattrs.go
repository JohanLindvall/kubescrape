package main

// The agent's half of the self-metadata feature (internal/selfmeta): the
// attribute build applied to the metrics it generates about itself — its
// self-metrics and the span metrics derived from ingested traces. Both are
// built from agentSelfResource, which knows only what the process itself can
// see (service.name, version, node). The pod comes from the metadata service's
// GET /v1/self, which attributes the request by its connection's source
// address; metaclient.Self is already a selfmeta resolve func, so there is no
// adapter here.

import (
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/selfmeta"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// selfBuild runs the configured `self` attribute pipeline over the agent's own
// pod. What it produces is applied fill-if-absent, so the pipeline can only
// ADD attributes — a template setting service.name or service.instance.id for
// these metrics is deliberately ineffective (see selfmeta.Wrap).
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
