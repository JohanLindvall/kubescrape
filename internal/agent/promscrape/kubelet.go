package promscrape

// The KUBELET: what all three kubelet scrapes share — the endpoint and the URLs
// derived from it, the bearer-authenticated GET and its refusal reporting, the
// scrape budget, the HTTP client and the node-level resource the kubelet's own
// series land on — and the two EXPOSITION endpoints: /metrics/cadvisor, whose
// rows cadvisorbatch.go attributes to pods and containers, and the kubelet's
// own /metrics. The third endpoint, /stats/summary, is JSON rather than
// exposition and lives in summary.go and summarybatch.go.

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
)

// KubeletConfig configures scraping of the kubelet's metrics endpoints.
type KubeletConfig struct {
	// Endpoint is the kubelet base URL, e.g. "https://10.0.0.5:10250".
	// Empty disables all three kubelet scrapes (cadvisor, /metrics and
	// /stats/summary).
	Endpoint string
	// Cadvisor scrapes <Endpoint>/metrics/cadvisor: per-container cgroup
	// metrics, split into one OTLP resource per pod/container.
	Cadvisor bool
	// NodeMetrics scrapes <Endpoint>/metrics: the kubelet's own metrics,
	// exported under a node-level resource.
	NodeMetrics bool
	// Summary scrapes <Endpoint>/stats/summary: the kubelet's JSON stats
	// document, converted into the per-pod, per-container, per-volume and
	// per-node filesystem/ephemeral-storage/process gauges cadvisor does not
	// report (summary.go).
	//
	// It authorizes against a DIFFERENT subresource from the other two — the
	// kubelet checks /stats/* against nodes/stats, not nodes/metrics — which is
	// why the flag behind it is off by default: with the binary rolled ahead of
	// the ClusterRole, an on-by-default scrape would 403 on every node in the
	// fleet every interval.
	Summary bool
	// TokenFile supplies the bearer token (the mounted ServiceAccount token;
	// it rotates). Empty sends no Authorization. Re-read at most once per
	// minute through internal/bearer, with the last good value kept across a
	// failed re-read — this used to be an os.ReadFile on EVERY kubelet
	// request, so the swap window kubelet opens when it rotates the projection
	// failed the scrape outright.
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

// The kubelet endpoints' paths, relative to Kubelet.Endpoint.
const (
	cadvisorPath    = "/metrics/cadvisor"
	nodeMetricsPath = "/metrics"
	summaryPath     = "/stats/summary"
)

// kubeletURLs are the three kubelet scrape URLs, derived ONCE, in New, from
// Kubelet.Endpoint. Each is read in two places that must agree: cycle() records
// it as the scrape's url — the health resource's url.full and the
// /debug/targets key — and the scrape function requests it and stamps it as
// url.full on the data it exports. `up` describes a pipeline's data only while
// the two are the same string, and they used to be two derivations, each
// trimming the endpoint and appending its own copy of the path.
type kubeletURLs struct{ cadvisor, node, summary string }

func newKubeletURLs(endpoint string) kubeletURLs {
	base := strings.TrimRight(endpoint, "/")
	return kubeletURLs{cadvisor: base + cadvisorPath, node: base + nodeMetricsPath, summary: base + summaryPath}
}

// statusError is a non-200 from a scrape — a kubelet endpoint or a discovered
// target — carrying the code so a caller can say something more useful about it
// than the number.
//
// The bare number is not diagnosable for the one status an operator will
// actually meet: the kubelet authorizes each of its endpoints against its own
// subresource, so a 403 on /stats/summary while /metrics/cadvisor succeeds
// means a missing RBAC RULE, not a broken credential — and the text says so
// where the summary scrape catches it. The message is unchanged from the
// fmt.Errorf it replaced, so nothing reading the log line moves.
//
// It is the TARGET path's status error too (it began as the kubelet's alone),
// so failureReason can split 401/403 out of every pipeline's non-200s without
// matching on the message text.
type statusError struct{ code int }

func (e *statusError) Error() string { return "status " + strconv.Itoa(e.code) }

// kubeletGet fetches a kubelet URL with bearer-token authentication, offering
// accept as the request's Accept header. The caller must close the response
// body.
func (s *Scraper) kubeletGet(ctx context.Context, url, accept string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", accept)
	if s.kubeletToken != nil {
		// A read error here fails only a scrape whose credential has NEVER been
		// readable; a transient failure mid-rotation serves the last good token
		// (internal/bearer).
		token, err := s.kubeletToken.Token()
		if err != nil {
			// `auth` rather than `unauthorized`: nothing was sent, so the
			// kubelet refused nothing — the projection at -kubelet-token-file
			// has never been readable by this process (internal/bearer keeps
			// the last good value across a transient failure, so reaching here
			// means there has never been one). The path, never the token.
			s.warnOnce("kubelettoken:"+s.cfg.Kubelet.TokenFile,
				"the kubelet bearer token could not be read; every kubelet scrape on this node will fail",
				"tokenFile", s.cfg.Kubelet.TokenFile, "error", err)
			return nil, classify(reasonAuth, fmt.Errorf("reading token: %w", err))
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := s.kubeletHTTP.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		drainClose(resp.Body)
		return nil, &statusError{code: resp.StatusCode}
	}
	return resp, nil
}

// reportKubeletRefusal names the remedy behind a kubelet 401 or 403, for all
// three kubelet pipelines. It is the ONE first-contact failure that takes a
// pipeline down on every node in the fleet at once, and "status 403" alone does
// not distinguish a missing ClusterRole rule from a token the kubelet would not
// accept — which is the whole diagnosis.
//
// The kubelet authorizes each endpoint against its own subresource
// (kubeletSubresource), which is why the 403 remedy is per pipeline:
// /metrics and /metrics/cadvisor check nodes/metrics, which is all the agent's
// ClusterRole has historically held, while /stats/summary checks nodes/stats —
// so the summary pipeline's first-contact failure is a 403 on every node while
// the other two kubelet scrapes keep working. A 401 is the token itself, on any
// of the three.
//
// Keyed on the endpoint, the pipeline and the code, so a 401 arriving after a
// 403 has been reported still says so; once per process, like every other
// complaint about something an operator has to go and edit.
func (s *Scraper) reportKubeletRefusal(pipeline, url string, err error) {
	var se *statusError
	if !errors.As(err, &se) || (se.code != http.StatusForbidden && se.code != http.StatusUnauthorized) {
		return
	}
	note := `the agent ClusterRole needs {apiGroups: [""], resources: ["` + kubeletSubresource(pipeline) + `"], verbs: ["get"]}, bound to this agent's ServiceAccount`
	if pipeline == pipelineSummary {
		note += "; the kubelet authorizes /stats/summary against nodes/stats, not the nodes/metrics the cadvisor and node scrapes use"
	}
	if se.code == http.StatusUnauthorized {
		note = "the kubelet did not accept the token at -kubelet-token-file at all: check that the ServiceAccount token is projected and that the kubelet runs with --authentication-token-webhook"
	}
	s.warnOnce("kubeletauth:"+s.cfg.Kubelet.Endpoint+":"+pipeline+":"+strconv.Itoa(se.code),
		"the kubelet refused the scrape",
		"pipeline", pipeline, "url", url, "status", se.code, "tokenFile", s.cfg.Kubelet.TokenFile, "note", note)
}

// kubeletSubresource is the nodes subresource the kubelet authorizes a kubelet
// pipeline's endpoint against: /stats/* checks nodes/stats, /metrics and
// /metrics/cadvisor check nodes/metrics.
func kubeletSubresource(pipeline string) string {
	if pipeline == pipelineSummary {
		return "nodes/stats"
	}
	return "nodes/metrics"
}

// drainClose reads a bounded remainder of an HTTP body before closing so the
// keep-alive connection can be reused, then closes it.
func drainClose(rc io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(rc, 1<<20))
	_ = rc.Close()
}

// kubeletTimeout is the per-scrape budget for the three kubelet endpoints,
// clamped to the scrape interval exactly as targetTimeout clamps every
// discovered target: cycle() waits for every scrape it started and Run only
// ticks after cycle returns, so a -scrape-timeout longer than -scrape-interval
// stretched the WHOLE NODE's cadence whenever a kubelet hung — the kubelet
// scrapes were the ones the clamp missed. The clamp lives in the request CONTEXT, which
// wins over the kubelet client's baked-in Timeout (the shorter of the two
// applies; the client's is only the backstop for a request without a
// deadline). Nothing in it is kubelet-specific, and exportHealth bounds the
// cycle's health export by it too.
func (s *Scraper) kubeletTimeout() time.Duration {
	if s.cfg.Interval > 0 {
		return min(s.cfg.Timeout, s.cfg.Interval)
	}
	return s.cfg.Timeout
}

// newKubeletHTTPClient builds the TLS client for the kubelet. It goes through
// newScrapeClient like every scrape client, which is what makes it refuse
// redirects: Go does not strip Authorization across a same-host https->http
// redirect (targetauth.go's noRedirect), and this is the client that attaches
// the node's ServiceAccount token — it was once the ONE scrape client that had
// not been updated.
func newKubeletHTTPClient(cfg KubeletConfig, timeout time.Duration) *http.Client {
	// One idle connection per kubelet scrape cycle() may run AT ONCE, derived
	// from the list of them: this transport speaks HTTP/1.1 (a custom
	// TLSClientConfig and no ForceAttemptHTTP2), so each concurrent request
	// needs its own connection, and a pool of one closed all but one of them
	// after every cycle — a fresh TCP+TLS handshake against the kubelet per
	// extra scrape per interval.
	c := newScrapeClient(&tls.Config{InsecureSkipVerify: cfg.InsecureTLS}, len(kubeletDueKeys))
	// A backstop only: the effective per-scrape budget is the request context's
	// kubeletTimeout() deadline, which may be SHORTER (clamped to
	// -scrape-interval) and always wins. This raw -scrape-timeout value merely
	// bounds a request that somehow arrives without a deadline.
	c.Timeout = timeout
	return c
}

// scrapeCadvisor scrapes <kubelet>/metrics/cadvisor. cadvisor series carry
// the pod identity as labels (namespace/pod/container); they are routed into
// one OTLP resource per pod and container, with full metadata resolved
// through the metadata service.
func (s *Scraper) scrapeCadvisor(ctx context.Context) (int, error) {
	ctx, cancel := s.scrapeContext(ctx, s.kubeletTimeout(), pipelineCadvisor)
	defer cancel()

	url := s.kubeletURLs.cadvisor
	resp, err := s.kubeletGet(ctx, url, acceptExposition)
	if err != nil {
		s.reportKubeletRefusal(pipelineCadvisor, url, err)
		return 0, err
	}
	defer drainClose(resp.Body)

	cb := newCadvisorBatcher(ctx, s, time.Now())
	return s.parseAndExport(ctx, resp.Body, false, false, cb, pipelineCadvisor, url)
}

// scrapeNodeMetrics scrapes <kubelet>/metrics under a node-level resource.
func (s *Scraper) scrapeNodeMetrics(ctx context.Context) (int, error) {
	// No metadata allowance is carved out here and none is needed: this is the
	// one kubelet pipeline that resolves nothing, which is exactly why it kept
	// succeeding throughout the outage metabudget.go quotes.
	ctx, cancel := context.WithTimeout(ctx, s.kubeletTimeout())
	defer cancel()

	url := s.kubeletURLs.node
	resp, err := s.kubeletGet(ctx, url, acceptExposition)
	if err != nil {
		s.reportKubeletRefusal(pipelineNode, url, err)
		return 0, err
	}
	defer drainClose(resp.Body)

	b := newBatcher(func(res pcommon.Resource) {
		s.fillKubeletResource(res, pipelineNode, url)
	}, s.cfg.StartTime, time.Now())
	return s.parseAndExport(ctx, resp.Body, false, false, b, pipelineNode, url)
}

// fillKubeletResource builds the node-level resource the kubelet's OWN series
// land on — the /metrics data, the /stats/summary node statistics, and every
// kubelet pipeline's health gauges: service.name=kubelet, url.full, then the
// pipeline's attribute builder over this agent's node.
//
// One body for all of them because they must AGREE: a kubelet pipeline's `up`
// (exportHealth) describes that pipeline's data only while the two resources
// derive the same (job, instance), and attrsFor's pipelineSummary case records
// what a disagreement costs. Three hand-written copies held that agreement by
// matching, and had already drifted in the order they put the two attributes.
func (s *Scraper) fillKubeletResource(res pcommon.Resource, pipeline, url string) {
	a := res.Attributes()
	a.PutStr("service.name", "kubelet")
	a.PutStr("url.full", url)
	s.attrsFor(pipeline).Build(res, attrs.Context{Node: s.nodeInfo()})
}
