package promscrape

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/promparse"
)

// MetaSource resolves pod and container metadata; implemented by
// metaclient.Client.
type MetaSource interface {
	PodByName(ctx context.Context, namespace, name string) (*kubemeta.Pod, error)
	Container(ctx context.Context, id string, wait time.Duration) (*kubemeta.ContainerMetadata, error)
}

// KubeletConfig configures scraping of the kubelet's metrics endpoints.
type KubeletConfig struct {
	// Endpoint is the kubelet base URL, e.g. "https://10.0.0.5:10250".
	// Empty disables both kubelet scrapes.
	Endpoint string
	// Cadvisor scrapes <Endpoint>/metrics/cadvisor: per-container cgroup
	// metrics, split into one OTLP resource per pod/container.
	Cadvisor bool
	// NodeMetrics scrapes <Endpoint>/metrics: the kubelet's own metrics,
	// exported under a node-level resource.
	NodeMetrics bool
	// TokenFile is read per scrape for the bearer token (the mounted
	// ServiceAccount token; it rotates). Empty sends no Authorization.
	TokenFile string
	// InsecureTLS skips certificate verification; kubelet serving
	// certificates are typically self-signed.
	InsecureTLS bool
	// DisableRollups drops the hierarchical cgroup aggregates: series for
	// cgroups above pod level (id "/", "/kubepods", QoS and system slices)
	// and pod-level rows of container-scoped families (the pod cgroup rolls
	// its containers up). Genuinely pod-scoped families
	// (container_network_*, which have no per-container breakdown),
	// container-level series and machine_* are kept.
	DisableRollups bool
	// Meta resolves the pod and container metadata referenced by cadvisor
	// series labels.
	Meta MetaSource
}

// chunker is a sink that also manages batch lifecycles.
type chunker interface {
	sink
	take() pmetric.Metrics
	count() int
	size() int // estimated encoded size of the accumulated batch
}

// defaultBatchBytes bounds one exported chunk well below the 4 MiB default
// gRPC receive limit of a collector (which applies to the decompressed
// message), leaving room for the size estimate's error margin.
const defaultBatchBytes = 3 << 20

// parseAndExport streams one scrape body through the series filter and the
// converter into cb, exporting a chunk whenever BatchPoints data points or
// BatchBytes estimated bytes accumulate. It returns the number of samples
// parsed.
//
// An aborted parse (sample limit, a truncated body, a read timeout mid-body)
// still exports what was converted before the abort: a partial scrape is worth
// far more than nothing, and every kind here is cumulative, so a missing series
// simply does not appear for that cycle.
func (s *Scraper) parseAndExport(ctx context.Context, body io.Reader, openMetrics, withExemplars bool, cb chunker, pipeline, what string) (int, error) {
	filter := s.cfg.Filters.filterFor(pipeline).session()
	exportFailed := false
	export := func() error {
		if err := s.cfg.Exporter.ExportMetrics(ctx, cb.take()); err != nil {
			exportFailed = true
			return err
		}
		return nil
	}
	full := func() bool {
		return cb.count() >= s.cfg.BatchPoints ||
			(s.cfg.BatchBytes > 0 && cb.size() >= s.cfg.BatchBytes)
	}
	conv := newConverter(cb, func() error {
		if full() {
			return export()
		}
		return nil
	})
	parser := promparse.Get(promparse.Options{MaxLineBytes: s.cfg.MaxLineBytes, OpenMetrics: openMetrics, Exemplars: withExemplars})
	defer promparse.Put(parser)
	samples := 0
	malformed, err := parser.Parse(body, func(sample Sample) error {
		samples++
		if s.cfg.MaxSamples > 0 && samples > s.cfg.MaxSamples {
			return ErrTooManySamples
		}
		if !filter.Keep(sample.Name, sample.Labels) {
			return nil
		}
		return conv.add(sample)
	})
	if err != nil {
		// Salvage the partially converted scrape. Pointless when the failure
		// WAS the export (the collector just rejected a chunk) or when the
		// context is gone (the send cannot succeed either).
		if !exportFailed && ctx.Err() == nil {
			if ferr := conv.finish(); ferr == nil && cb.count() > 0 {
				if eerr := export(); eerr != nil {
					s.log.Warn("exporting partial scrape", "target", what, "error", eerr)
				}
			}
		}
		return samples, err
	}
	if err := conv.finish(); err != nil {
		return samples, err
	}
	malformed += conv.malformed // component samples the converter rejected
	if cb.count() > 0 {
		if err := export(); err != nil {
			return samples, err
		}
	}
	if malformed > 0 {
		obs.ScrapeMalformed.WithLabelValues(pipeline).Add(float64(malformed))
		s.log.Warn("scrape had malformed lines", "target", what, "malformed", malformed, "samples", samples)
	}
	return samples, nil
}

// kubeletGet fetches a kubelet URL with bearer-token authentication. The
// caller must close the response body.
func (s *Scraper) kubeletGet(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/plain;version=0.0.4")
	if s.cfg.Kubelet.TokenFile != "" {
		token, err := os.ReadFile(s.cfg.Kubelet.TokenFile)
		if err != nil {
			return nil, fmt.Errorf("reading token: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	}
	resp, err := s.kubeletHTTP.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		drainClose(resp.Body)
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	return resp, nil
}

// drainClose reads a bounded remainder of an HTTP body before closing so the
// keep-alive connection can be reused, then closes it.
func drainClose(rc io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(rc, 1<<20))
	_ = rc.Close()
}

// scrapeCadvisor scrapes <kubelet>/metrics/cadvisor. cadvisor series carry
// the pod identity as labels (namespace/pod/container); they are routed into
// one OTLP resource per pod and container, with full metadata resolved
// through the metadata service.
func (s *Scraper) scrapeCadvisor(ctx context.Context) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	url := strings.TrimRight(s.cfg.Kubelet.Endpoint, "/") + "/metrics/cadvisor"
	resp, err := s.kubeletGet(ctx, url)
	if err != nil {
		return 0, err
	}
	defer drainClose(resp.Body)

	cb := newCadvisorBatcher(s, time.Now(), ctx)
	return s.parseAndExport(ctx, resp.Body, false, false, cb, pipelineCadvisor, url)
}

// scrapeNodeMetrics scrapes <kubelet>/metrics under a node-level resource.
func (s *Scraper) scrapeNodeMetrics(ctx context.Context) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	url := strings.TrimRight(s.cfg.Kubelet.Endpoint, "/") + "/metrics"
	resp, err := s.kubeletGet(ctx, url)
	if err != nil {
		return 0, err
	}
	defer drainClose(resp.Body)

	b := newBatcher(func(res pcommon.Resource) {
		a := res.Attributes()
		a.PutStr("service.name", "kubelet")
		a.PutStr("url.full", url)
		s.attrsFor(pipelineNode).Build(res, attrs.Context{Node: s.nodeInfo()})
	}, s.cfg.BatchPoints, s.cfg.StartTime, time.Now())
	return s.parseAndExport(ctx, resp.Body, false, false, b, pipelineNode, url)
}

// podMetaCacheTTL bounds how long resolved (or not-found) metadata is reused
// across cadvisor scrape cycles.
const podMetaCacheTTL = time.Minute

type podCacheEntry struct {
	pod       *kubemeta.Pod       // nil: lookup failed / unknown
	container *kubemeta.Container // set for container-ID entries
	fetched   time.Time
}

// cadvisorBatcher implements sink, routing each point into a ResourceMetrics
// chosen by the sample's namespace/pod/container labels. Those labels move
// into the resource attributes (enriched from the metadata service); the
// remaining labels stay on the data point.
type cadvisorBatcher struct {
	s        *Scraper
	ctx      context.Context
	startTS  pcommon.Timestamp
	scrapeTS pcommon.Timestamp

	md     pmetric.Metrics
	scopes map[string]pmetric.ScopeMetrics // resource key -> scope
	byKey  map[string]pmetric.Metric       // resource key + metric name
	points int
	bytes  int

	// Last-seen memos, mirroring the plain batcher's metricByName/remember:
	// cadvisor series arrive grouped by family and cgroup, so consecutive
	// samples often share the identity and metric — a struct compare replaces
	// the key-building allocations and map probes.
	lastIdent    cadvisorIdentity
	lastScope    pmetric.ScopeMetrics
	lastScopeOK  bool
	lastMetIdent cadvisorIdentity
	lastName     string
	lastMetric   pmetric.Metric
	lastPodScope bool
	lastOK       bool
	// keyBuf is the scratch buffer for identity/metric keys: probes use
	// map[string(keyBuf)] (no allocation); the string materializes only when a
	// new resource or metric is inserted.
	keyBuf []byte

	// cgroupMemo caches cgroupIdentity per raw "id" value: each container's
	// cgroup path recurs in every family of the scrape (~60×), and the parse
	// (plus the systemd layout's uid underscore rewrite) is the expensive part
	// of identityOf. The mapping is pure, so the memo survives reset() and
	// lives for the batcher's one scrape.
	cgroupMemo map[string]cgroupPair
}

type cgroupPair struct {
	podUID, containerID string
}

func newCadvisorBatcher(s *Scraper, scrape time.Time, ctx context.Context) *cadvisorBatcher {
	cb := &cadvisorBatcher{
		s:          s,
		ctx:        ctx,
		startTS:    pcommon.NewTimestampFromTime(s.cfg.StartTime),
		scrapeTS:   pcommon.NewTimestampFromTime(scrape),
		cgroupMemo: make(map[string]cgroupPair, 256),
	}
	cb.reset()
	return cb
}

func (cb *cadvisorBatcher) reset() {
	cb.md = pmetric.NewMetrics()
	if cb.scopes == nil {
		cb.scopes = make(map[string]pmetric.ScopeMetrics)
		cb.byKey = make(map[string]pmetric.Metric)
	} else {
		clear(cb.scopes)
		clear(cb.byKey)
	}
	cb.points = 0
	cb.bytes = 0
	// The memoized handles point into the previous batch's payload.
	cb.lastScopeOK = false
	cb.lastOK = false
}

func (cb *cadvisorBatcher) take() pmetric.Metrics {
	md := cb.md
	cb.reset()
	return md
}

func (cb *cadvisorBatcher) count() int { return cb.points }
func (cb *cadvisorBatcher) size() int  { return cb.bytes }

// cadvisorIdentity is the resource identity of one cadvisor sample: the
// namespace/pod/container labels plus the pod UID and container ID parsed
// from the cgroup path in the "id" label.
type cadvisorIdentity struct {
	namespace, pod, container string
	podUID, containerID       string
	image                     string // "image" label, container rows only
	hasCgroup                 bool   // an "id" label was present
}

func (cb *cadvisorBatcher) identityOf(labels []Label) cadvisorIdentity {
	var ident cadvisorIdentity
	sandbox := false
	for _, l := range labels {
		switch l.Name {
		case "namespace":
			ident.namespace = l.Value
		case "pod":
			ident.pod = l.Value
		case "container":
			if l.Value == "POD" {
				sandbox = true
			} else {
				ident.container = l.Value
			}
		case "image":
			ident.image = l.Value
		case "id":
			ident.hasCgroup = true
			pair, ok := cb.cgroupMemo[l.Value]
			if !ok {
				pair.podUID, pair.containerID = cgroupIdentity(l.Value)
				if len(cb.cgroupMemo) < maxTrackedFamilies {
					cb.cgroupMemo[l.Value] = pair
				}
			}
			ident.podUID, ident.containerID = pair.podUID, pair.containerID
		}
	}
	// Sandbox ("POD") rows are pod-level; with the systemd driver their
	// cgroup names the pause container, whose ID is not part of the pod's
	// container statuses — drop it so the row shares the pod resource. The
	// image label names the pause container too, never the workload.
	if sandbox {
		ident.containerID = ""
		ident.image = ""
	}
	return ident
}

// rollup reports whether the sample belongs to a cgroup above pod level.
func (id cadvisorIdentity) rollup() bool {
	return id.hasCgroup && id.pod == "" && id.podUID == "" && id.containerID == ""
}

// appendKey appends the resource identity key to b. The cgroup-derived
// identity is preferred: the pod UID disambiguates same-name pod recreations
// and the container ID distinguishes restarted container incarnations. It
// appends into a caller-owned scratch buffer so the per-sample map probes can
// use map[string(buf)] lookups without materializing a string (the parser's
// keyBuf discipline).
func (id cadvisorIdentity) appendKey(b []byte) []byte {
	// The cgroup-derived parts (uid, container id) are validated hex/UUID and
	// cannot contain the separator; namespace/pod/container come from exporter
	// LABELS, so they are length-prefixed to keep hostile values from aliasing
	// another identity.
	if id.podUID != "" {
		b = append(b, 'u', 0)
		b = append(b, id.podUID...)
		b = append(b, 0)
		b = append(b, id.containerID...)
		b = append(b, 0)
		return appendLP(b, id.container)
	}
	// containerID must participate: a non-pod cgroup with a parseable container
	// ID (a standalone, non-k8s container) has no namespace/pod/container labels,
	// and omitting the ID would merge every such container into one anonymous
	// resource with indistinguishable, conflicting series.
	b = append(b, 'n', 0)
	b = appendLP(b, id.namespace)
	b = appendLP(b, id.pod)
	b = appendLP(b, id.container)
	return append(b, id.containerID...)
}

// appendLP appends one length-prefixed label-derived key part (collision-proof
// join).
func appendLP(b []byte, v string) []byte {
	b = strconv.AppendInt(b, int64(len(v)), 10)
	b = append(b, ':')
	return append(b, v...)
}

// isIdentityLabel reports whether a label moved into the resource.
func isIdentityLabel(name string) bool {
	return name == "namespace" || name == "pod" || name == "container"
}

// redundantOnPodRow reports whether a label duplicates (or, for network rows'
// pause-container image/name, contradicts) the resolved resource identity of a
// pod- or container-identified row: the cgroup path in "id" is already parsed
// into pod uid + container.id, "name" is the runtime container name behind
// container.id, "image" lands on the resource. cmb-alloy deletes all three.
// Rollup rows keep "id" — there it is the only distinguisher between cgroups
// sharing the node-level resource.
func redundantOnPodRow(name string) bool {
	return name == "id" || name == "name" || name == "image"
}

// podScoped reports whether the sample resolved to a pod- or container-level
// resource (as opposed to a rollup cgroup or a machine_* row).
func (id cadvisorIdentity) podScoped() bool {
	return id.pod != "" || id.podUID != "" || id.containerID != "" || id.container != ""
}

// scope returns the ScopeMetrics for the resource identified by ident,
// creating it (with metadata-service enrichment) on first use per batch. The
// previous sample's identity is memoized: a repeat costs a struct compare
// instead of building the key.
func (cb *cadvisorBatcher) scope(ident cadvisorIdentity) pmetric.ScopeMetrics {
	if cb.lastScopeOK && ident == cb.lastIdent {
		return cb.lastScope
	}
	cb.keyBuf = ident.appendKey(cb.keyBuf[:0])
	sm, ok := cb.scopes[string(cb.keyBuf)] // no alloc: map read elides the copy
	if !ok {
		key := string(cb.keyBuf) // materialize once per new resource per batch
		rm := cb.md.ResourceMetrics().AppendEmpty()
		cb.fillResource(rm.Resource(), ident)
		sm = rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName(scopeNameCadvisor)
		cb.scopes[key] = sm
		// One resource per pod/container: its attributes count toward the chunk
		// size (see convert.go).
		cb.bytes += resourceBytes(rm.Resource(), scopeNameCadvisor)
	}
	cb.lastIdent, cb.lastScope, cb.lastScopeOK = ident, sm, true
	return sm
}

// fillResource builds the resource attributes for one identity, preferring
// the exact container incarnation (by container ID from the cgroup path),
// then the pod (by name, cross-checked against the cgroup pod UID), then the
// raw label identity.
func (cb *cadvisorBatcher) fillResource(res pcommon.Resource, ident cadvisorIdentity) {
	// Exact container incarnation via the cgroup container ID, else the pod.
	ctx, resolved := cb.s.resolveContext(cb.ctx, ident.containerID, ident.namespace, ident.pod, ident.podUID, ident.container, res)
	ctx.Node = cb.s.nodeInfo()

	if !resolved && (ident.pod != "" || ident.podUID != "" || ident.containerID != "") {
		// Metadata unavailable (or a same-name pod replaced this one, or a
		// standalone non-k8s container the metadata service cannot know): keep
		// the identity from the labels and the cgroup path — container.id is a
		// containerID-only row's ONLY distinguisher, since its id/name/image
		// labels are elided from the data points as pod-scoped-redundant.
		a := res.Attributes()
		if ident.namespace != "" {
			a.PutStr("k8s.namespace.name", ident.namespace)
		}
		if ident.pod != "" {
			a.PutStr("k8s.pod.name", ident.pod)
		}
		if ident.podUID != "" {
			a.PutStr("k8s.pod.uid", ident.podUID)
		}
		if ident.container != "" {
			a.PutStr("k8s.container.name", ident.container)
		}
		if ident.containerID != "" {
			a.PutStr("container.id", ident.containerID)
		}
		// The image label is elided from container-row data points as
		// resource-redundant; on an unresolved resource it is the only source.
		if ident.image != "" && (ident.container != "" || ident.containerID != "") {
			a.PutStr("container.image.name", ident.image)
		}
	}
	cb.s.attrsFor(pipelineCadvisor).Build(res, ctx)
}

// metric returns the (per-resource) metric for one sample's identity, plus
// whether the sample's row is pod/container-scoped (its id/name/image labels
// are then redundant with the resource and elided from the data points).
func (cb *cadvisorBatcher) metric(ident cadvisorIdentity, name string, shape func(pmetric.Metric)) (pmetric.Metric, bool) {
	// Last-seen fast path: consecutive samples of the same resource and family
	// skip the key building and the map probe entirely.
	if cb.lastOK && name == cb.lastName && ident == cb.lastMetIdent {
		return cb.lastMetric, cb.lastPodScope
	}
	cb.keyBuf = ident.appendKey(cb.keyBuf[:0])
	cb.keyBuf = append(cb.keyBuf, 0)
	cb.keyBuf = append(cb.keyBuf, name...)
	m, ok := cb.byKey[string(cb.keyBuf)] // no alloc: map read elides the copy
	if !ok {
		key := string(cb.keyBuf) // materialize once per new metric per batch
		// scope() reuses keyBuf, so the key must be materialized first.
		m = cb.scope(ident).Metrics().AppendEmpty()
		m.SetName(name)
		shape(m)
		cb.byKey[key] = m
		cb.bytes += len(name) + metricOverheadBytes // one descriptor per resource
	}
	cb.lastMetIdent, cb.lastName, cb.lastMetric, cb.lastPodScope, cb.lastOK = ident, name, m, ident.podScoped(), true
	return m, cb.lastPodScope
}

// drop applies the rollup filter (see KubeletConfig.DisableRollups) to one
// sample's identity.
func (cb *cadvisorBatcher) drop(name string, ident cadvisorIdentity) bool {
	if !cb.s.cfg.Kubelet.DisableRollups {
		return false
	}
	if ident.rollup() {
		return true // above pod level
	}
	// A pod-level row of a container-scoped family duplicates the sum of
	// its containers; only families without a per-container breakdown pass.
	if ident.hasCgroup && ident.container == "" && ident.containerID == "" &&
		(ident.pod != "" || ident.podUID != "") && !podScopedFamily(name) {
		return true
	}
	return false
}

// podScopedFamily reports whether a cadvisor metric only exists at pod
// level. Network counters are measured on the pod sandbox network
// namespace; there are no per-container rows to roll up.
func podScopedFamily(name string) bool {
	return strings.HasPrefix(name, "container_network_")
}

func (cb *cadvisorBatcher) addNumber(s Sample, monotonic bool) {
	ident := cb.identityOf(s.Labels) // computed once per sample
	if cb.drop(s.Name, ident) {
		return
	}
	m, podScoped := cb.metric(ident, s.Name, func(m pmetric.Metric) {
		if monotonic {
			sum := m.SetEmptySum()
			sum.SetIsMonotonic(true)
			sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		} else {
			m.SetEmptyGauge()
		}
	})

	dp, ok := numberDataPoint(m, cb.startTS)
	if !ok {
		return
	}
	dp.SetDoubleValue(s.Value)
	dp.SetTimestamp(pointTS(s.TimestampMs, cb.scrapeTS))
	cb.putFilteredLabels(dp.Attributes(), s.Labels, podScoped)
	cb.points++
	cb.bytes += numberBytes(s)
}

func (cb *cadvisorBatcher) addHistogram(family string, acc *histAcc) {
	ident := cb.identityOf(acc.labels)
	if cb.drop(family, ident) {
		return
	}
	m, podScoped := cb.metric(ident, family, func(m pmetric.Metric) {
		m.SetEmptyHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	})
	if m.Type() != pmetric.MetricTypeHistogram {
		obs.ScrapeCollisions.Inc()
		return
	}
	dp := m.Histogram().DataPoints().AppendEmpty()
	dp.SetStartTimestamp(cb.startTS)
	dp.SetTimestamp(pointTS(acc.ts, cb.scrapeTS))
	fillHistogramPoint(dp, acc)
	cb.putFilteredLabels(dp.Attributes(), acc.labels, podScoped)
	cb.points++
	cb.bytes += histBytes(acc)
}

func (cb *cadvisorBatcher) addSummary(family string, acc *summAcc) {
	ident := cb.identityOf(acc.labels)
	if cb.drop(family, ident) {
		return
	}
	m, podScoped := cb.metric(ident, family, func(m pmetric.Metric) {
		m.SetEmptySummary()
	})
	if m.Type() != pmetric.MetricTypeSummary {
		obs.ScrapeCollisions.Inc()
		return
	}
	dp := m.Summary().DataPoints().AppendEmpty()
	dp.SetStartTimestamp(cb.startTS)
	dp.SetTimestamp(pointTS(acc.ts, cb.scrapeTS))
	fillSummaryPoint(dp, acc)
	cb.putFilteredLabels(dp.Attributes(), acc.labels, podScoped)
	cb.points++
	cb.bytes += summBytes(acc)
}

func (cb *cadvisorBatcher) putFilteredLabels(attrs pcommon.Map, labels []Label, podScoped bool) {
	for _, l := range labels {
		if isIdentityLabel(l.Name) || (podScoped && redundantOnPodRow(l.Name)) {
			continue
		}
		attrs.PutStr(l.Name, l.Value)
	}
}

// podMeta resolves pod metadata by name with a small TTL cache; nil when
// unknown.
func (s *Scraper) podMeta(ctx context.Context, namespace, pod string) *kubemeta.Pod {
	key := "n\x00" + namespace + "/" + pod
	if e, ok := s.cacheGet(key); ok {
		return e.pod
	}
	meta, err := s.metaSource().PodByName(ctx, namespace, pod)
	if err != nil {
		meta = nil
		if ctx.Err() != nil {
			return nil // do not negative-cache cancellations
		}
	}
	s.cachePut(key, podCacheEntry{pod: meta, fetched: time.Now()})
	return meta
}

// containerMeta resolves the exact container incarnation by runtime ID; nil
// when unknown. The lookup is non-blocking (wait 0): the scraped series only
// reference containers that already exist.
func (s *Scraper) containerMeta(ctx context.Context, containerID string) *kubemeta.ContainerMetadata {
	key := "c\x00" + containerID
	if e, ok := s.cacheGet(key); ok {
		if e.pod == nil {
			return nil
		}
		return &kubemeta.ContainerMetadata{ContainerID: containerID, Container: *e.container, Pod: *e.pod}
	}
	md, err := s.metaSource().Container(ctx, containerID, 0)
	if err != nil {
		if ctx.Err() != nil {
			return nil // do not negative-cache cancellations
		}
		s.cachePut(key, podCacheEntry{fetched: time.Now()})
		return nil
	}
	s.cachePut(key, podCacheEntry{pod: &md.Pod, container: &md.Container, fetched: time.Now()})
	return md
}

// metaSource returns the metadata source shared by the kubelet scrapes and
// the splitters. Splitters require Kubelet.Meta to be set even when the
// kubelet scrapes are disabled.
func (s *Scraper) metaSource() MetaSource {
	return s.cfg.Kubelet.Meta
}

func (s *Scraper) cacheGet(key string) (podCacheEntry, bool) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	e, ok := s.podCache[key]
	if !ok || time.Since(e.fetched) >= podMetaCacheTTL {
		return podCacheEntry{}, false
	}
	return e, true
}

func (s *Scraper) cachePut(key string, e podCacheEntry) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	s.podCache[key] = e
	// Opportunistic pruning keeps the cache bounded by live pods/containers.
	if len(s.podCache) > 8192 {
		for k, old := range s.podCache {
			if time.Since(old.fetched) >= podMetaCacheTTL {
				delete(s.podCache, k)
			}
		}
	}
}

// newKubeletHTTPClient builds the TLS client for the kubelet.
func newKubeletHTTPClient(cfg KubeletConfig, timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			TLSClientConfig:     &tls.Config{InsecureSkipVerify: cfg.InsecureTLS},
			MaxIdleConnsPerHost: 1,
			IdleConnTimeout:     90 * time.Second,
		},
	}
}
