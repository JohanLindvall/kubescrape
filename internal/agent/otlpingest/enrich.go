// Package otlpingest receives OTLP logs and metrics pushed by applications on
// the node and enriches each resource with Kubernetes metadata deduced from a
// container ID or pod UID already present on the data, then hands the result
// to the shared exporter. It closes the "apps push OTLP for enrichment" gap
// that otherwise requires a separate collector with the k8sattributes
// processor.
//
// Enrichment never overwrites an attribute the sender already set: the sender
// is authoritative for anything it chose to declare.
package otlpingest

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/logenrich"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// MetadataSource resolves pod/container metadata; implemented by
// metaclient.Client.
type MetadataSource interface {
	Container(ctx context.Context, id string, wait time.Duration) (*kubemeta.ContainerMetadata, error)
	PodByUID(ctx context.Context, uid string) (*kubemeta.Pod, error)
	PodByIP(ctx context.Context, ip string) (*kubemeta.Pod, error)
}

// MetricsMode selects how metric resources are enriched.
type MetricsMode string

const (
	// MetricsResource reads the ID from the incoming resource attributes and
	// enriches the resource in place (the OTLP norm: one resource per object).
	MetricsResource MetricsMode = "resource"
	// MetricsDatapoint reads the ID from each data point's attributes and
	// splits the points into one resource per distinct object.
	MetricsDatapoint MetricsMode = "datapoint"
	// MetricsAuto enriches from the resource attributes when an ID is present
	// there, otherwise falls back to per-data-point splitting.
	MetricsAuto MetricsMode = "auto"
)

// Config configures the enricher.
type Config struct {
	// ContainerIDKeys are the attribute keys inspected for a container ID
	// (checked first — a container ID resolves the exact incarnation).
	ContainerIDKeys []string
	// PodUIDKeys are the attribute keys inspected for a pod UID.
	PodUIDKeys []string
	// Wait is how long a metadata lookup may block for not-yet-known objects
	// (0 = never block; pushed telemetry normally lags pod creation).
	Wait time.Duration
	// MetricsMode selects resource-level vs data-point enrichment.
	MetricsMode MetricsMode
	// EnrichLines parses each pushed log record's body for a timestamp,
	// severity, trace/span IDs and structured fields (as -logs-enrich does),
	// filling only fields the sender left unset.
	EnrichLines bool
	// PeerIPFallback resolves the sending pod by the connection's peer IP
	// when the resource carries no container ID or pod UID, and merges its
	// k8s attributes (never overwriting sender values). Opt-in: peer IPs can
	// be rewritten by NAT, and hostNetwork senders share the node IP (those
	// never resolve — the metadata service only indexes pod-IP-owning pods).
	PeerIPFallback bool
	// Attrs builds the k8s resource attributes (nil = built-in defaults).
	Attrs *attrs.Builder
	// NodeInfo supplies the agent node's metadata for attribute templates.
	NodeInfo func() *attrs.NodeInfo

	Meta   MetadataSource
	Logger *slog.Logger
}

func (c Config) containerIDKeys() []string {
	if len(c.ContainerIDKeys) > 0 {
		return c.ContainerIDKeys
	}
	return []string{"container.id", "k8s.container.id"}
}

func (c Config) podUIDKeys() []string {
	if len(c.PodUIDKeys) > 0 {
		return c.PodUIDKeys
	}
	return []string{"k8s.pod.uid"}
}

func (c Config) metricsMode() MetricsMode {
	if c.MetricsMode == "" {
		return MetricsAuto
	}
	return c.MetricsMode
}

// Enricher attributes pushed telemetry.
type Enricher struct {
	cfg             Config
	containerIDKeys []string
	podUIDKeys      []string
	mode            MetricsMode
	log             *slog.Logger
}

// NewEnricher creates an Enricher.
func NewEnricher(cfg Config) *Enricher {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Enricher{
		cfg:             cfg,
		containerIDKeys: cfg.containerIDKeys(),
		podUIDKeys:      cfg.podUIDKeys(),
		mode:            cfg.metricsMode(),
		log:             log,
	}
}

// EnrichLogs enriches every resource in ld in place. When line enrichment is
// enabled, each record's body is additionally parsed for a timestamp,
// severity, trace/span IDs and structured fields (as the tailer does),
// without overwriting values the sender already set.
func (e *Enricher) EnrichLogs(ctx context.Context, ld plog.Logs) {
	// One lookup + attribute build per distinct ID across the request.
	cache := map[string]pcommon.Map{}
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		e.enrichResource(ctx, rl.Resource(), cache)
		if !e.cfg.EnrichLines {
			continue
		}
		sls := rl.ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			lrs := sls.At(j).LogRecords()
			for k := 0; k < lrs.Len(); k++ {
				lr := lrs.At(k)
				logenrich.ApplyBody(lr)
			}
		}
	}
}

// EnrichTraces enriches every resource in td in place (traces are otherwise a
// passthrough signal).
func (e *Enricher) EnrichTraces(ctx context.Context, td ptrace.Traces) {
	cache := map[string]pcommon.Map{}
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		e.enrichResource(ctx, rss.At(i).Resource(), cache)
	}
}

// EnrichMetrics enriches md according to the configured mode, returning the
// (possibly regrouped) metrics to export.
func (e *Enricher) EnrichMetrics(ctx context.Context, md pmetric.Metrics) pmetric.Metrics {
	switch e.mode {
	case MetricsDatapoint:
		return e.splitAndEnrich(ctx, md)
	case MetricsResource:
		e.enrichMetricResources(ctx, md)
		return md
	default: // auto
		if e.allResourcesHaveID(md) {
			e.enrichMetricResources(ctx, md)
			return md
		}
		return e.splitAndEnrich(ctx, md)
	}
}

// enrichMetricResources enriches each ResourceMetrics from its own resource
// attributes.
func (e *Enricher) enrichMetricResources(ctx context.Context, md pmetric.Metrics) {
	cache := map[string]pcommon.Map{}
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		e.enrichResource(ctx, rms.At(i).Resource(), cache)
	}
}

// allResourcesHaveID reports whether every ResourceMetrics carries an ID
// attribute at the resource level (so no data-point splitting is needed).
func (e *Enricher) allResourcesHaveID(md pmetric.Metrics) bool {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		if _, ok := e.findID(rms.At(i).Resource().Attributes()); !ok {
			return false
		}
	}
	return true
}

// enrichResource resolves the ID on res and merges the k8s attributes it maps
// to, without overwriting attributes the sender already set. cache memoizes
// the built attributes per ID token for the duration of one request.
func (e *Enricher) enrichResource(ctx context.Context, res pcommon.Resource, cache map[string]pcommon.Map) {
	e.applyMetadata(ctx, res.Attributes(), cache)
}

// applyMetadata looks up the ID in a and merges the derived k8s attributes
// into a, leaving existing keys untouched. It reports whether an ID resolved.
func (e *Enricher) applyMetadata(ctx context.Context, a pcommon.Map, cache map[string]pcommon.Map) bool {
	tok, ok := e.findID(a)
	if !ok {
		if pod := e.peerPod(ctx); pod != nil {
			obs.Ingested.WithLabelValues("peer_ip").Inc()
			e.build(pod, nil, a)
			return true
		}
		obs.Ingested.WithLabelValues("unresolved").Inc()
		return false
	}
	built := e.builtAttrs(ctx, cache, tok)
	if built.Len() == 0 {
		return false
	}
	mergeAttrs(built, a)
	return true
}

// builtAttrs returns the k8s attributes for a kind-tagged ID token, doing the
// metadata lookup and attribute build (and the enriched/unresolved counting)
// once per distinct token per cache. An empty map means the ID did not
// resolve.
func (e *Enricher) builtAttrs(ctx context.Context, cache map[string]pcommon.Map, token string) pcommon.Map {
	if built, ok := cache[token]; ok {
		return built
	}
	built := pcommon.NewMap()
	if pod, container := e.lookupByID(ctx, token); pod != nil {
		r := pcommon.NewResource()
		actx := attrs.Context{Pod: pod, Container: container}
		if e.cfg.NodeInfo != nil {
			actx.Node = e.cfg.NodeInfo()
		}
		e.cfg.Attrs.Build(r, actx)
		r.Attributes().CopyTo(built)
		obs.Ingested.WithLabelValues("enriched").Inc()
	} else {
		obs.Ingested.WithLabelValues("unresolved").Inc()
	}
	cache[token] = built
	return built
}

// mergeAttrs adds src's attributes to dst, never overwriting keys the sender
// already set.
func mergeAttrs(src, dst pcommon.Map) {
	src.Range(func(k string, v pcommon.Value) bool {
		if _, exists := dst.Get(k); !exists {
			v.CopyTo(dst.PutEmpty(k))
		}
		return true
	})
}

// build merges the k8s attributes for pod/container into a, never overwriting
// keys the sender already set.
func (e *Enricher) build(pod *kubemeta.Pod, container *kubemeta.Container, a pcommon.Map) {
	built := pcommon.NewResource()
	actx := attrs.Context{Pod: pod, Container: container}
	if e.cfg.NodeInfo != nil {
		actx.Node = e.cfg.NodeInfo()
	}
	e.cfg.Attrs.Build(built, actx)
	mergeAttrs(built.Attributes(), a)
}

// peerPod resolves the pushing pod from the connection's peer IP (nil when
// the fallback is disabled, the peer is unknown, or no live pod owns the IP).
func (e *Enricher) peerPod(ctx context.Context) *kubemeta.Pod {
	if !e.cfg.PeerIPFallback {
		return nil
	}
	ip := peerIP(ctx)
	if ip == "" {
		return nil
	}
	pod, err := e.cfg.Meta.PodByIP(ctx, ip)
	if err != nil {
		return nil
	}
	return pod
}

// idToken tags an ID value with its kind so a later lookup knows which
// endpoint to use, without re-scanning the key set.
const (
	tokContainer = "c\x00"
	tokPodUID    = "u\x00"
)

// findID reports the first container ID or pod UID found in a, as a kind-
// tagged token.
func (e *Enricher) findID(a pcommon.Map) (token string, ok bool) {
	for _, k := range e.containerIDKeys {
		if v, ok := a.Get(k); ok && v.Str() != "" {
			return tokContainer + v.Str(), true
		}
	}
	for _, k := range e.podUIDKeys {
		if v, ok := a.Get(k); ok && v.Str() != "" {
			return tokPodUID + v.Str(), true
		}
	}
	return "", false
}

// lookupByID resolves a kind-tagged ID token to metadata (nil pod on miss).
func (e *Enricher) lookupByID(ctx context.Context, token string) (*kubemeta.Pod, *kubemeta.Container) {
	switch {
	case len(token) >= 2 && token[:2] == tokContainer:
		id := token[2:]
		md, err := e.cfg.Meta.Container(ctx, id, e.cfg.Wait)
		if err != nil {
			e.log.Debug("ingest: container lookup failed", "id", id, "error", err)
			return nil, nil
		}
		return &md.Pod, &md.Container
	case len(token) >= 2 && token[:2] == tokPodUID:
		uid := token[2:]
		pod, err := e.cfg.Meta.PodByUID(ctx, uid)
		if err != nil {
			e.log.Debug("ingest: pod-uid lookup failed", "uid", uid, "error", err)
			return nil, nil
		}
		return pod, nil
	}
	return nil, nil
}
