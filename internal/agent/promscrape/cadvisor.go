package promscrape

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/kubemeta"
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
}

// parseAndExport streams one scrape body through the converter into cb,
// exporting a chunk whenever BatchPoints accumulate.
func (s *Scraper) parseAndExport(ctx context.Context, body io.Reader, openMetrics, withExemplars bool, cb chunker, what string) error {
	conv := newConverter(cb)
	parser := NewParser(s.cfg.MaxLineBytes, openMetrics, withExemplars)
	samples := 0
	malformed, err := parser.Parse(body, func(sample Sample) error {
		samples++
		if s.cfg.MaxSamples > 0 && samples > s.cfg.MaxSamples {
			return ErrTooManySamples
		}
		conv.add(sample)
		if cb.count() >= s.cfg.BatchPoints {
			return s.cfg.Exporter.ExportMetrics(ctx, cb.take())
		}
		return nil
	})
	if err != nil {
		return err
	}
	conv.finish()
	if cb.count() > 0 {
		if err := s.cfg.Exporter.ExportMetrics(ctx, cb.take()); err != nil {
			return err
		}
	}
	if malformed > 0 {
		s.log.Warn("scrape had malformed lines", "target", what, "malformed", malformed, "samples", samples)
	}
	return nil
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
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		_ = resp.Body.Close()
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	return resp, nil
}

// scrapeCadvisor scrapes <kubelet>/metrics/cadvisor. cadvisor series carry
// the pod identity as labels (namespace/pod/container); they are routed into
// one OTLP resource per pod and container, with full metadata resolved
// through the metadata service.
func (s *Scraper) scrapeCadvisor(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	url := strings.TrimRight(s.cfg.Kubelet.Endpoint, "/") + "/metrics/cadvisor"
	resp, err := s.kubeletGet(ctx, url)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		_ = resp.Body.Close()
	}()

	cb := newCadvisorBatcher(s, time.Now(), ctx)
	return s.parseAndExport(ctx, resp.Body, false, false, cb, url)
}

// scrapeNodeMetrics scrapes <kubelet>/metrics under a node-level resource.
func (s *Scraper) scrapeNodeMetrics(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	url := strings.TrimRight(s.cfg.Kubelet.Endpoint, "/") + "/metrics"
	resp, err := s.kubeletGet(ctx, url)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		_ = resp.Body.Close()
	}()

	node := s.cfg.Node
	b := newBatcher(func(res pcommon.Resource) {
		a := res.Attributes()
		a.PutStr("k8s.node.name", node)
		a.PutStr("service.name", "kubelet")
		a.PutStr("url.full", url)
		s.cfg.AttrFilter.Apply(res)
	}, s.cfg.BatchPoints, s.cfg.StartTime, time.Now())
	return s.parseAndExport(ctx, resp.Body, false, false, b, url)
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
}

func newCadvisorBatcher(s *Scraper, scrape time.Time, ctx context.Context) *cadvisorBatcher {
	cb := &cadvisorBatcher{
		s:        s,
		ctx:      ctx,
		startTS:  pcommon.NewTimestampFromTime(s.cfg.StartTime),
		scrapeTS: pcommon.NewTimestampFromTime(scrape),
	}
	cb.reset()
	return cb
}

func (cb *cadvisorBatcher) reset() {
	cb.md = pmetric.NewMetrics()
	cb.scopes = make(map[string]pmetric.ScopeMetrics)
	cb.byKey = make(map[string]pmetric.Metric)
	cb.points = 0
}

func (cb *cadvisorBatcher) take() pmetric.Metrics {
	md := cb.md
	cb.reset()
	return md
}

func (cb *cadvisorBatcher) count() int { return cb.points }

// cadvisorIdentity is the resource identity of one cadvisor sample: the
// namespace/pod/container labels plus the pod UID and container ID parsed
// from the cgroup path in the "id" label.
type cadvisorIdentity struct {
	namespace, pod, container string
	podUID, containerID       string
	hasCgroup                 bool // an "id" label was present
}

func identityOf(labels []Label) cadvisorIdentity {
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
		case "id":
			ident.hasCgroup = true
			ident.podUID, ident.containerID = cgroupIdentity(l.Value)
		}
	}
	// Sandbox ("POD") rows are pod-level; with the systemd driver their
	// cgroup names the pause container, whose ID is not part of the pod's
	// container statuses — drop it so the row shares the pod resource.
	if sandbox {
		ident.containerID = ""
	}
	return ident
}

// rollup reports whether the sample belongs to a cgroup above pod level.
func (id cadvisorIdentity) rollup() bool {
	return id.hasCgroup && id.pod == "" && id.podUID == "" && id.containerID == ""
}

// key identifies the resource. The cgroup-derived identity is preferred: the
// pod UID disambiguates same-name pod recreations and the container ID
// distinguishes restarted container incarnations.
func (id cadvisorIdentity) key() string {
	if id.podUID != "" {
		return "u\x00" + id.podUID + "\x00" + id.containerID + "\x00" + id.container
	}
	return "n\x00" + id.namespace + "\x00" + id.pod + "\x00" + id.container
}

// isIdentityLabel reports whether a label moved into the resource.
func isIdentityLabel(name string) bool {
	return name == "namespace" || name == "pod" || name == "container"
}

// scope returns the ScopeMetrics for the resource identified by the labels,
// creating it (with metadata-service enrichment) on first use per batch.
func (cb *cadvisorBatcher) scope(labels []Label) (pmetric.ScopeMetrics, string) {
	ident := identityOf(labels)
	key := ident.key()
	if sm, ok := cb.scopes[key]; ok {
		return sm, key
	}

	rm := cb.md.ResourceMetrics().AppendEmpty()
	cb.fillResource(rm.Resource(), ident)
	cb.s.cfg.AttrFilter.Apply(rm.Resource())
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("github.com/JohanLindvall/kubescrape/agent/promscrape/cadvisor")
	cb.scopes[key] = sm
	return sm, key
}

// fillResource builds the resource attributes for one identity, preferring
// the exact container incarnation (by container ID from the cgroup path),
// then the pod (by name, cross-checked against the cgroup pod UID), then the
// raw label identity.
func (cb *cadvisorBatcher) fillResource(res pcommon.Resource, ident cadvisorIdentity) {
	res.Attributes().PutStr("k8s.node.name", cb.s.cfg.Node)

	// Exact container incarnation via the cgroup container ID.
	if ident.containerID != "" {
		if md := cb.s.containerMeta(cb.ctx, ident.containerID); md != nil {
			attrs.Pod(res, md.Pod)
			attrs.Container(res, md.Container)
			return
		}
	}

	if ident.pod == "" && ident.podUID == "" {
		return // node-level (machine_* or hierarchy cgroups)
	}

	if ident.pod != "" {
		if meta := cb.s.podMeta(cb.ctx, ident.namespace, ident.pod); meta != nil &&
			(ident.podUID == "" || meta.UID == ident.podUID) {
			attrs.Pod(res, *meta)
			if ident.container != "" {
				for _, c := range meta.Containers {
					if c.Name == ident.container {
						attrs.Container(res, c)
						return
					}
				}
				res.Attributes().PutStr("k8s.container.name", ident.container)
			}
			return
		}
	}

	// Metadata unavailable (or a same-name pod replaced this one): keep the
	// identity from the labels and the cgroup path.
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
}

func (cb *cadvisorBatcher) metric(labels []Label, name string, shape func(pmetric.Metric)) pmetric.Metric {
	sm, resKey := cb.scope(labels)
	key := resKey + "\x00" + name
	m, ok := cb.byKey[key]
	if !ok {
		m = sm.Metrics().AppendEmpty()
		m.SetName(name)
		shape(m)
		cb.byKey[key] = m
	}
	return m
}

// drop applies the rollup filter (see KubeletConfig.DisableRollups).
func (cb *cadvisorBatcher) drop(name string, labels []Label) bool {
	if !cb.s.cfg.Kubelet.DisableRollups {
		return false
	}
	ident := identityOf(labels)
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
	if cb.drop(s.Name, s.Labels) {
		return
	}
	m := cb.metric(s.Labels, s.Name, func(m pmetric.Metric) {
		if monotonic {
			sum := m.SetEmptySum()
			sum.SetIsMonotonic(true)
			sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		} else {
			m.SetEmptyGauge()
		}
	})

	var dp pmetric.NumberDataPoint
	switch m.Type() {
	case pmetric.MetricTypeSum:
		dp = m.Sum().DataPoints().AppendEmpty()
		dp.SetStartTimestamp(cb.startTS)
	case pmetric.MetricTypeGauge:
		dp = m.Gauge().DataPoints().AppendEmpty()
	default:
		return
	}
	dp.SetDoubleValue(s.Value)
	dp.SetTimestamp(cb.pointTS(s.TimestampMs))
	cb.putFilteredLabels(dp.Attributes(), s.Labels)
	cb.points++
}

func (cb *cadvisorBatcher) addHistogram(family string, acc *histAcc) {
	if cb.drop(family, acc.labels) {
		return
	}
	m := cb.metric(acc.labels, family, func(m pmetric.Metric) {
		m.SetEmptyHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	})
	if m.Type() != pmetric.MetricTypeHistogram {
		return
	}
	dp := m.Histogram().DataPoints().AppendEmpty()
	dp.SetStartTimestamp(cb.startTS)
	dp.SetTimestamp(cb.pointTS(acc.ts))
	fillHistogramPoint(dp, acc)
	cb.putFilteredLabels(dp.Attributes(), acc.labels)
	cb.points++
}

func (cb *cadvisorBatcher) addSummary(family string, acc *summAcc) {
	if cb.drop(family, acc.labels) {
		return
	}
	m := cb.metric(acc.labels, family, func(m pmetric.Metric) {
		m.SetEmptySummary()
	})
	if m.Type() != pmetric.MetricTypeSummary {
		return
	}
	dp := m.Summary().DataPoints().AppendEmpty()
	dp.SetStartTimestamp(cb.startTS)
	dp.SetTimestamp(cb.pointTS(acc.ts))
	fillSummaryPoint(dp, acc)
	cb.putFilteredLabels(dp.Attributes(), acc.labels)
	cb.points++
}

func (cb *cadvisorBatcher) pointTS(tsMs int64) pcommon.Timestamp {
	if tsMs != 0 {
		return pcommon.Timestamp(tsMs * int64(time.Millisecond))
	}
	return cb.scrapeTS
}

func (cb *cadvisorBatcher) putFilteredLabels(attrs pcommon.Map, labels []Label) {
	for _, l := range labels {
		if !isIdentityLabel(l.Name) {
			attrs.PutStr(l.Name, l.Value)
		}
	}
}

// podMeta resolves pod metadata by name with a small TTL cache; nil when
// unknown.
func (s *Scraper) podMeta(ctx context.Context, namespace, pod string) *kubemeta.Pod {
	key := "n\x00" + namespace + "/" + pod
	if e, ok := s.podCache[key]; ok && time.Since(e.fetched) < podMetaCacheTTL {
		return e.pod
	}
	meta, err := s.cfg.Kubelet.Meta.PodByName(ctx, namespace, pod)
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
// when unknown. The lookup is non-blocking (wait 0): cadvisor only reports
// containers that already exist.
func (s *Scraper) containerMeta(ctx context.Context, containerID string) *kubemeta.ContainerMetadata {
	key := "c\x00" + containerID
	if e, ok := s.podCache[key]; ok && time.Since(e.fetched) < podMetaCacheTTL {
		if e.pod == nil {
			return nil
		}
		return &kubemeta.ContainerMetadata{ContainerID: containerID, Container: *e.container, Pod: *e.pod}
	}
	md, err := s.cfg.Kubelet.Meta.Container(ctx, containerID, 0)
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

func (s *Scraper) cachePut(key string, e podCacheEntry) {
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
