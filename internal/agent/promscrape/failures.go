package promscrape

// Why a scrape failed, as a metric label value and as a log key.
//
// "Everything is up=0" is the first-live-run failure this package has to make
// diagnosable, and until now the only machine-readable thing it produced was
// kubescrape_scrapes_total{outcome="error"} — one bucket holding a refused
// connection, an expired token, a missing RBAC rule, a bad CA, a collector
// rejecting the payload and a monitor's uncompilable regex, which take five
// different remedies. The error TEXT distinguished them, but only in a log line
// nothing aggregated.
//
// So every failure is classified once, at the one place that already sees them
// all (Scraper.cycle's spawn), into kubescrape_scrape_failures_total{pipeline,
// reason} plus a throttled Warn carrying the URL — the one thing a counter
// cannot hold. The classification is by ERROR TYPE wherever the type exists
// (statusError, net.OpError, tls/x509, context) and by an explicit wrapper
// where only the site knows (auth, relabel, proto_refused, export, body), never
// by matching error strings: a message an upstream library rewords must not
// silently re-bucket a fleet's failures.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log/slog"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// Failure reasons. Enumerated in obs.ScrapeFailures' help text, which is the
// operator-facing definition of each — keep the two in step.
const (
	reasonDNS          = "dns"
	reasonConnect      = "connect"
	reasonTLS          = "tls"
	reasonTimeout      = "timeout"
	reasonCanceled     = "canceled"
	reasonUnauthorized = "unauthorized"
	reasonStatus       = "status"
	reasonAuth         = "auth"
	reasonRelabel      = "relabel"
	reasonProtoRefused = "proto_refused"
	reasonSampleLimit  = "sample_limit"
	reasonBody         = "body"
	reasonExport       = "export"
	reasonOther        = "other"
)

// classifiedError carries a reason the failing SITE knows and the classifier
// could not infer — a credential that would not resolve, a regex that would not
// compile, a collector that rejected the converted payload.
//
// It is TRANSPARENT: Error returns the cause verbatim and Unwrap exposes it, so
// wrapping changes no log line, no test comparing messages and no errors.Is
// beneath it. The classification is the only thing added.
type classifiedError struct {
	reason string
	err    error
}

func (e *classifiedError) Error() string { return e.err.Error() }
func (e *classifiedError) Unwrap() error { return e.err }

// classify tags err with a reason, leaving nil alone. An already-classified
// error keeps its ORIGINAL reason: the innermost site is the most specific one
// (an export failure inside a scrape is an export failure, not a parse).
func classify(reason string, err error) error {
	if err == nil {
		return nil
	}
	var c *classifiedError
	if errors.As(err, &c) {
		return err
	}
	return &classifiedError{reason: reason, err: err}
}

// failureReason classifies one scrape failure. Order matters: the explicit
// wrapper wins, then the transport-level types (a TLS failure arrives wrapped
// in a *url.Error and sometimes in a *net.OpError, so the TLS probes run
// first), then the context sentinels last — a deadline that fired during a dial
// is more usefully reported as a timeout than as a connect error, but a REFUSED
// dial that happens to race the deadline must not read as one, which is why
// net.Error.Timeout is consulted rather than the context alone.
func failureReason(err error) string {
	if err == nil {
		return ""
	}
	var c *classifiedError
	if errors.As(err, &c) {
		return c.reason
	}
	var se *statusError
	if errors.As(err, &se) {
		if se.code == 401 || se.code == 403 {
			return reasonUnauthorized
		}
		return reasonStatus
	}
	if errors.Is(err, ErrTooManySamples) {
		return reasonSampleLimit
	}
	// TLS before the net types: a handshake failure is reachable as a bare
	// verification error, as an x509 error, and (for a plaintext port answering
	// a TLS ClientHello) as a RecordHeaderError.
	var cve *tls.CertificateVerificationError
	var rhe tls.RecordHeaderError
	var uae x509.UnknownAuthorityError
	var hne x509.HostnameError
	var cie x509.CertificateInvalidError
	if errors.As(err, &cve) || errors.As(err, &rhe) ||
		errors.As(err, &uae) || errors.As(err, &hne) || errors.As(err, &cie) {
		return reasonTLS
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		// A DNS lookup that TIMED OUT is the metadata-service-style hang, not a
		// missing record: reported as a timeout so it lands beside the other
		// symptoms of a slow network rather than looking like a typo in a name.
		if dnsErr.IsTimeout {
			return reasonTimeout
		}
		return reasonDNS
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return reasonTimeout
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return reasonTimeout
	}
	if errors.Is(err, context.Canceled) {
		return reasonCanceled
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return reasonConnect
	}
	return reasonOther
}

// scrapeFailWarnEvery re-warns about a target that keeps failing at this
// cadence. It is not the "once per process" the configuration complaints use
// (warnOnce): a scrape failure is a condition an operator fixes out of band —
// a Secret is created, an RBAC rule is added, a pod starts listening — and the
// line has to come back if it is still true, both so a running incident stays
// visible and so its disappearance means something.
//
// Unthrottled, this was one Warn per failing target per cycle: fifty broken
// targets on each of two hundred nodes at -scrape-interval=30s is 20k lines a
// minute, all saying the same thing. The counter carries the rate.
const scrapeFailWarnEvery = 5 * time.Minute

// maxScrapeFailKeys bounds the failure-warning table. Keyed by CONFIGURATION
// (warnTarget) and reason rather than by URL, exactly as warnOnce is: a pod
// restart must not mint a key, or the noisiest cluster gets the most noise and
// the table grows for the life of the process.
const maxScrapeFailKeys = 1024

// reportScrapeFailure counts a failed scrape and names it, at most once per
// (target, reason) per scrapeFailWarnEvery. The REASON is part of the key so a
// failure that changes shape — a connection refused becoming a 403 once the pod
// starts listening — reports immediately instead of hiding behind the previous
// message's window.
// shuttingDown forces the reason to `canceled` whatever the error says: once
// the process context is done, a target's timeout or reset is collateral of the
// shutdown and reporting it as the target's fault would put one accusation per
// target into the last seconds of every rolling update.
func (s *Scraper) reportScrapeFailure(pipeline, url, warnKey string, err error, shuttingDown bool) {
	reason := failureReason(err)
	if shuttingDown {
		reason = reasonCanceled
	}
	obs.ScrapeFailures.WithLabelValues(pipeline, reason).Inc()
	if reason == reasonCanceled {
		// Shutdown, not a fault: the counter is the whole record. A line per
		// target here would arrive exactly when nobody can act on it.
		return
	}

	s.failMu.Lock()
	if s.failWarned == nil {
		s.failWarned = logdedupe.New(maxScrapeFailKeys, scrapeFailWarnEvery)
	}
	tab := s.failWarned
	s.failMu.Unlock()

	allow, saturated := tab.Allow(pipeline + "\x00" + warnKey + "\x00" + reason)
	if saturated {
		s.log.Warn("scrape failure warning table is full; further distinct failures are counted but not logged",
			"keys", maxScrapeFailKeys)
	}
	if !allow {
		return
	}
	args := []any{"pipeline", pipeline, "reason", reason, "url", url, "error", err}
	if note := failureNote(reason); note != "" {
		args = append(args, "note", note)
	}
	s.log.Warn("scrape failed", args...)
}

// failureNote is the remediation hint the message itself cannot carry, for the
// reasons whose first-run cause is specific enough to name. Deliberately empty
// for the rest: a hint that fits every case tells an operator nothing.
func failureNote(reason string) string {
	switch reason {
	case reasonUnauthorized:
		return "the target refused the credential: a monitor's bearerTokenSecret/basicAuth may be missing or wrong, and for the kubelet pipelines this is the nodes/metrics ClusterRole rule"
	case reasonAuth:
		return "this agent could not resolve the secret ref: the metadata service must run -scrape-auth-secrets and both sides must share -scrape-auth-token-file"
	case reasonProtoRefused:
		return "pass -scrape-native-histograms to accept the protobuf exposition, or fix the target to honour the Accept header"
	case reasonExport:
		return "the scrape itself succeeded; read kubescrape_export_requests_total and the collector's own logs"
	case reasonTLS:
		return "check the endpoint's scheme, the monitor's tlsConfig.ca and serverName, or set insecureSkipVerify deliberately"
	}
	return ""
}

// emptyTargetsWarnEvery re-states "this node has no scrape targets" at this
// cadence while it stays true. Longer than scrapeFailWarnEvery because it is a
// STANDING condition an operator diagnoses once, not an incident that changes.
const emptyTargetsWarnEvery = 30 * time.Minute

// reportTargetSet publishes the discovered-target count and says something when
// it is interesting: the set changing size (Debug), and the set being EMPTY
// (Warn on the transition, re-warned on a window, Info on recovery).
//
// The empty list is the most common first-run failure and the one that produces
// no other evidence at all — no scrape runs, so no scrape fails, so every
// counter in this package stays flat and /debug/targets is a blank page that
// looks exactly like a healthy agent whose targets happen to be elsewhere. The
// transition/re-warn/recovery shape is cmd/kubescrape's api-server watchdog's.
//
// Called only from a cycle that FETCHED the list: a failed fetch has its own
// Error line, and treating its empty slice as "no targets exist" would blame
// discovery for a metadata-service outage.
func (s *Scraper) reportTargetSet(targets []kubemeta.ScrapeTarget) {
	n := len(targets)
	obs.ScrapeTargets.Set(float64(n))

	if s.lastTargetsSet && n != s.lastTargets && s.log.Enabled(context.Background(), slog.LevelDebug) {
		// Guarded: the counts are field reads, but the by-source breakdown
		// walks the list, and this runs once per cycle on every node.
		s.log.Debug("scrape target set changed",
			"node", s.cfg.Node, "targets", n, "previous", s.lastTargets, "bySource", targetSources(targets))
	}
	s.lastTargets, s.lastTargetsSet = n, true

	switch {
	case n == 0:
		if !s.emptyTargets {
			s.emptyTargets = true
			s.emptyTargetWarn = logdedupe.Throttle{} // a fresh outage says so at once
		}
		if !s.emptyTargetWarn.Allow(emptyTargetsWarnEvery) {
			return
		}
		s.log.Warn("the metadata service returned NO scrape targets for this node, so nothing is being scraped from pods or Services here",
			"node", s.cfg.Node,
			"note", "annotate a pod or Service with prometheus.io/scrape=true, or check that the ServiceMonitor/PodMonitor selects a pod on THIS node and that the metadata service runs -servicemonitors; GET /v1/explain/{namespace}/{pod} on the metadata service says why one pod is not a target")
	case s.emptyTargets:
		s.emptyTargets = false
		s.log.Info("scrape targets discovered for this node", "node", s.cfg.Node, "targets", n)
	}
}

// targetSources is the by-source census the empty/changed report carries: pod
// and service annotations, ServiceMonitors and PodMonitors are discovered by
// three different mechanisms, and which of them produced nothing is most of the
// diagnosis. Built only under a Debug-enabled guard.
func targetSources(targets []kubemeta.ScrapeTarget) string {
	counts := map[string]int{}
	for i := range targets {
		src := targets[i].Source
		if src == "" {
			src = "unknown"
		}
		counts[src]++
	}
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(strconv.Itoa(counts[k]))
	}
	return b.String()
}

// reportNegotiation says, at Debug, when a target served a format other than
// the one this agent asked for. Nothing FAILS here — the parser reads whatever
// arrived — but the consequence is invisible otherwise: with -scrape-exemplars
// on, a target that answers text/plain simply produces no exemplars, forever,
// and the only evidence is an absence.
//
// One Content-Type header read per scrape, so no Enabled guard is needed; the
// call is skipped entirely once the levels agree.
//
// The header is the TARGET's bytes, bounded only by whatever the transport
// accepted, so it goes on the line through clipForLog — the same rule the
// protobuf refusal beside it follows.
func (s *Scraper) reportNegotiation(t kubemeta.ScrapeTarget, askedProto bool, contentType string, openMetrics bool) {
	if !s.log.Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	switch {
	case askedProto && !strings.Contains(contentType, protoContentType) && !openMetrics:
		s.log.Debug("target negotiated down from the protobuf exposition; native histograms will not be present",
			"url", t.URL, "monitor", t.Monitor, "contentType", clipForLog(contentType))
	case !askedProto && s.cfg.Exemplars && !openMetrics:
		s.log.Debug("target served classic text although OpenMetrics was offered; no exemplars will be scraped from it",
			"url", t.URL, "monitor", t.Monitor, "contentType", clipForLog(contentType))
	}
}

// reportCadvisorIdentity counts a cadvisor resource the metadata service did
// not place, and — at Debug — names it. The counter is the rate; the Debug line
// is the only thing that says WHICH container, which is the whole question when
// half a node's series lose their labels and the other half keep them.
//
// Not throttled, because it is Debug and because the count per scrape is
// bounded by the node's container count: this is not a per-item path (one call
// per resource per exported chunk) and an operator who turned Debug on during
// an incident wants every one of them.
func (s *Scraper) reportCadvisorIdentity(ident cadvisorIdentity) {
	level := "pod"
	if ident.containerID != "" || ident.container != "" {
		level = "container"
	}
	obs.CadvisorUnresolved.WithLabelValues(level).Inc()
	if !s.log.Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	// objectLevel, not "level": slog's own severity key IS "level", and a second
	// pair of that name on the line makes a logfmt reader resolve the record's
	// severity to "container". The line then reads as DEBUG to a human and as
	// level="container" to Loki, so a severity filter silently drops it. The
	// METRIC label stays "level" — a metric has no reserved key, and
	// METRICS.md documents it under that name.
	s.log.Debug("the metadata service did not place a cadvisor row; it is exported with the identity its own labels carried",
		"objectLevel", level, "namespace", ident.namespace, "pod", ident.pod,
		"container", ident.container, "id", ident.containerID, "uid", ident.podUID)
}

// debugSandboxFold reports, per pod per chunk, that a sandbox row was folded
// into the pod's resource. It answers the question the fold's own doc comment
// spends thirty lines on — "why does this pod have a resource carrying `pause`,
// or why does it NOT" — with the evidence for the pod actually in front of the
// operator. A row that declines the fold gets its own resource and shows up as
// an ordinary unresolved one above, so the two branches are both visible.
func (s *Scraper) debugSandboxFold(ident cadvisorIdentity) {
	if !s.log.Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	s.log.Debug("folded a pod sandbox row into the pod's resource",
		"namespace", ident.namespace, "pod", ident.pod, "uid", ident.podUID)
}

// cacheEvictWarnEvery throttles a bounded-cache eviction notice. Each cache
// carries its OWN gate (Scraper.tlsEvictWarn / relabelEvictWarn) — the caches
// thrash for different reasons and want different remedies, and a shared
// keyless throttle would let the first condition silence the second.
const cacheEvictWarnEvery = 30 * time.Minute

// warnCacheEviction reports a bounded per-target cache running at its cap.
// Nothing is LOST — the entry is rebuilt on demand — which is exactly why it
// needs saying: the symptom is a scrape that gets slower and a node that opens
// a connection per target per cycle, with every counter in this package flat.
//
// gate is the caller's per-cache throttle. Callers must invoke this OUTSIDE the
// lock guarding the cache: rendering and writing a slog record is I/O, and the
// scrape goroutines all contend for that lock.
func (s *Scraper) warnCacheEviction(gate *logdedupe.Throttle, what string, entries int, note string) {
	if !gate.Allow(cacheEvictWarnEvery) {
		return
	}
	s.log.Warn("a bounded scrape cache is at its cap and is evicting; the entries are rebuilt on demand, so this costs work rather than data",
		"cache", what, "entries", entries, "note", note)
}
