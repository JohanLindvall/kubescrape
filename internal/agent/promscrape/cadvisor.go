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
	return s.parseAndExportFiltered(ctx, body, openMetrics, withExemplars, cb, pipeline, what, nil)
}

// parseAndExportFiltered additionally applies a per-target relabel session
// (monitor endpoints' metricRelabelings; nil = none).
func (s *Scraper) parseAndExportFiltered(ctx context.Context, body io.Reader, openMetrics, withExemplars bool, cb chunker, pipeline, what string, relabel *relabelFilter) (int, error) {
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
		if relabel != nil && !relabel.Keep(sample.Name, sample.Labels) {
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

// metaSource returns the metadata source shared by the kubelet scrapes and
// the splitters. Splitters require Kubelet.Meta to be set even when the
// kubelet scrapes are disabled.
func (s *Scraper) metaSource() MetaSource {
	return s.cfg.Kubelet.Meta
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
