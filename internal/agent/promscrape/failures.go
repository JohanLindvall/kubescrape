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
// (statusError, net.OpError, tls/x509, context, io.ErrUnexpectedEOF) and by an
// explicit wrapper where only the site knows (auth, relabel, proto_refused,
// export, body), never by matching error strings: a message an upstream library
// rewords must not silently re-bucket a fleet's failures.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// Failure reasons. Each is DEFINED in obs.ScrapeFailures' help text, as
// `<reason> (<definition>)`, which is the operator-facing definition of it;
// TestFailureReasonsAreDocumented holds the two in step (the repo-wide
// help-enumeration guard cannot, since reportScrapeFailure passes a variable).
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
	if _, ok := errors.AsType[*classifiedError](err); ok {
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
	if c, ok := errors.AsType[*classifiedError](err); ok {
		return c.reason
	}
	if se, ok := errors.AsType[*statusError](err); ok {
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
	if dnsErr, ok := errors.AsType[*net.DNSError](err); ok {
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
	// A body that ENDED mid-stream — a Content-Length the target did not honour,
	// a truncated chunked or gzip stream, a protobuf varint or message cut by
	// EOF. Every reader in the chain reports it as io.ErrUnexpectedEOF, and it
	// is a body fault, not an unclassified one (the parser already names it,
	// MalformedDetail.TruncatedLines). After the context sentinels, so a cut the
	// scrape's own deadline caused still reads as a timeout; a connection RESET
	// is a *net.OpError and stays connect.
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return reasonBody
	}
	if opErr, ok := errors.AsType[*net.OpError](err); ok {
		// A TLS ALERT from the peer: crypto/tls surfaces it as
		// &net.OpError{Op: "remote error", Err: alert(n)}, and the alert type is
		// unexported (tls.AlertError is QUIC's), so the Op FIELD is the only
		// typed handle on it. It is how a target refuses this agent's CLIENT
		// CERTIFICATE — missing, untrusted or expired — which is the main
		// per-target mTLS failure and was counted `connect` with no note,
		// pointing the operator at networking. The field is a crypto/tls
		// constant rather than error text; were it ever renamed, the fallback
		// is the `connect` this always was.
		if opErr.Op == "remote error" {
			return reasonTLS
		}
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

	if !s.allowRepeatingWarn(pipeline + "\x00" + warnKey + "\x00" + reason) {
		return
	}
	args := []any{"pipeline", pipeline, "reason", reason, "url", url, "error", err}
	if note := failureNote(pipeline, reason); note != "" {
		args = append(args, "note", note)
	}
	s.log.Warn("scrape failed", args...)
}

// allowRepeatingWarn gates a per-target complaint about a condition that can
// clear out of band — a failing scrape, a failing export of one — at most once
// per scrapeFailWarnEvery per key, through the failWarned table. It is
// warnOnce's re-warning sibling; keys must not collide across callers, so each
// caller prefixes its own (a scrape failure's key starts with its pipeline).
func (s *Scraper) allowRepeatingWarn(key string) bool {
	allow, saturated := s.failWarned.Allow(key)
	if saturated {
		s.log.Warn("scrape failure warning table is full; further distinct failures are counted but not logged",
			"keys", maxScrapeFailKeys)
	}
	return allow
}

// failureNote is the remediation hint the message itself cannot carry, for the
// reasons whose first-run cause is specific enough to name. Deliberately empty
// for the rest: a hint that fits every case tells an operator nothing.
//
// The PIPELINE matters for `auth`: on a discovered target it is a secret ref
// the metadata service would not resolve, while on the three kubelet pipelines
// it can only be the agent's own ServiceAccount token at -kubelet-token-file
// (kubeletGet), which never goes near the metadata service — pointing that
// operator at -scrape-auth-secrets, every five minutes, was a wrong remedy.
// It matters for `unauthorized` for the same reason: a kubelet refused the
// agent's own token, and a 403 names the pipeline's OWN subresource
// (kubeletSubresource) — this note used to name nodes/metrics for every kubelet
// pipeline, which on /stats/summary is the rule the operator already has.
func failureNote(pipeline, reason string) string {
	switch reason {
	case reasonUnauthorized:
		if isKubeletPipeline(pipeline) {
			return "the kubelet refused this agent's ServiceAccount token: a 403 means the agent ClusterRole lacks " +
				kubeletSubresource(pipeline) + ", a 401 that the kubelet did not accept the token at -kubelet-token-file at all"
		}
		return "the target refused the credential: a monitor's bearerTokenSecret/basicAuth may be missing or wrong"
	case reasonAuth:
		if isKubeletPipeline(pipeline) {
			return "the kubelet bearer token at -kubelet-token-file has never been readable: check that the agent's pod mounts its ServiceAccount token (automountServiceAccountToken) or that the flag names the projected file"
		}
		return "this agent could not resolve the secret ref: the metadata service must run -scrape-auth-secrets and both sides must share -scrape-auth-token-file"
	case reasonProtoRefused:
		return "pass -scrape-native-histograms to accept the protobuf exposition, or fix the target to honour the Accept header"
	case reasonExport:
		return "the scrape itself succeeded; read kubescrape_export_requests_total and the collector's own logs"
	case reasonTLS:
		return "check the endpoint's scheme, the monitor's tlsConfig.ca and serverName, or set insecureSkipVerify deliberately; a \"remote error\" means the target refused this agent's client certificate (tlsConfig.cert/keySecret)"
	}
	return ""
}

// isKubeletPipeline reports whether pipeline scrapes the node's kubelet, with
// the agent's own ServiceAccount token, rather than a discovered target.
func isKubeletPipeline(pipeline string) bool {
	switch pipeline {
	case pipelineCadvisor, pipelineNode, pipelineSummary:
		return true
	}
	return false
}
