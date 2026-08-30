package otlpexport

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The destination-health report: ONE line when a destination starts refusing
// exports, a throttled re-warn while it keeps refusing, and one line when it
// starts accepting again.
//
// It exists because the wire outcome was, until now, counted and nothing else:
// kubescrape_export_requests_total{outcome="error"} moved and no line said
// WHICH endpoint, what the collector answered, or whether retrying could ever
// help. An operator's first live run is exactly the moment a collector is not
// up yet, an endpoint has the wrong port, or TLS disagrees at both ends — and
// each of those must say what is wrong once, not once per batch (a busy node
// exports several times a second, per signal).
//
// The shape is cmd/kubescrape/apiserver.go's, which is the repo's model for a
// repeatable CONDITION: transition warn, throttled re-warn, recovery info,
// beside a counter that carries the rate. What is added here over that model is
// the CLASS (permanent vs transient — whether the payload is coming back) and a
// remediation `note`, because "connection refused" and "x509: certificate
// signed by unknown authority" have different fixes and both arrive as an
// opaque error string on a path an operator cannot step through.
//
// THE CLASS ALSO DECIDES WHICH NARRATIVE THE FAILURE BELONGS TO, and getting
// that wrong is how a report meant to end a log flood becomes one. A PERMANENT
// rejection is a verdict on the PAYLOAD, not on the destination: the collector
// answered, and it will answer the next batch too. Feeding one into the
// destination state machine made a healthy collector that refuses a single
// poison payload — one over the receiver's limit, one shape its pipeline will
// not parse — flap "the destination is failing" / "the destination is accepting
// again" on every single export, forever, since the state re-arms on each
// success. That is a per-batch line on the path an operator watches during an
// incident, which is precisely what this file exists to prevent.
//
// So there are TWO reports, and only one of them is a state machine:
//
//   - the DESTINATION narrative (transition warn / throttled re-warn /
//     recovery Info) describes reachability, and only TRANSIENT outcomes move
//     it. It is a state because "the collector is down" is a state.
//   - a rejected PAYLOAD gets its own throttled line and touches no state. It
//     cannot flap, because it never recovers: there is nothing to recover from,
//     only a rate, which the line carries and
//     kubescrape_export_requests_total{outcome="permanent"} tracks.
//
// A destination that rejects EVERYTHING (an endpoint that is not an OTLP
// receiver at all, a collector with no pipeline for this signal) therefore
// reports through the second line, once per window, with the note that names
// the fix — not through a reachability narrative that would be describing
// something else.

// FailureReporter tracks one destination's per-signal send outcomes. The zero
// value is not usable; call NewFailureReporter. It is safe for concurrent use:
// three signals export from their own goroutines, and the router fans one
// payload out to several destinations at once.
type FailureReporter struct {
	log *slog.Logger
	// what names the destination in the message ("the OTLP collector", "a
	// routing destination"); attrs are the static key/values every line of this
	// destination carries (endpoint and protocol for a client, the route name
	// for a routing destination — a router destination has no endpoint to
	// print, which is what the startup summary's per-route lines are for).
	what  string
	attrs []any
	every time.Duration

	// failing is how many signals are currently in the failed state, read
	// WITHOUT the lock so the steady state costs one atomic load. Note is on
	// every export of every signal — including each of the ingest receiver's
	// forwards, which run on the concurrent receive goroutines — and a mutex
	// plus a clock read per successful push, to decide nothing, is a cost the
	// happy path should not carry.
	failing atomic.Int32

	mu sync.Mutex
	// state is the TRANSIENT (reachability) narrative, per signal; rejected is
	// the permanent-rejection throttle, per signal. Both are bounded by the
	// three signal names.
	state    map[string]*failState
	rejected map[string]*rejectState
	now      func() time.Time // injectable for tests
}

type failState struct {
	failures int64
	since    time.Time
	lastLog  time.Time
}

// rejectState throttles the per-payload rejection line. It is deliberately NOT
// a state machine: there is no "recovered" edge to report, because an accepted
// batch says nothing about the one the collector refused.
type rejectState struct {
	// rejections and windowStart accumulate between lines, so the line can say
	// how many payloads were dropped and over how long instead of implying the
	// one in front of it is the only one.
	rejections  int64
	windowStart time.Time
	lastLog     time.Time
}

// failWarnEvery paces the re-warn. A collector that is down is down for
// minutes, and the counter carries the rate meanwhile.
const failWarnEvery = 5 * time.Minute

// NewFailureReporter reports outcomes for one destination. A nil logger uses
// the process default, as the rest of this package's diagnostics do (the Client
// is built from Config alone and has no logger of its own).
func NewFailureReporter(log *slog.Logger, what string, attrs ...any) *FailureReporter {
	if log == nil {
		log = slog.Default()
	}
	return &FailureReporter{
		log: log, what: what, attrs: attrs, every: failWarnEvery,
		state: map[string]*failState{}, rejected: map[string]*rejectState{}, now: time.Now,
	}
}

// Note records one export outcome for signal and reports it if the destination
// changed state (or has been failing long enough to re-warn).
//
// A PERMANENT rejection never reaches that state machine: it is a verdict on
// the payload, so it goes to noteRejected instead. Routing it here made a
// collector that refuses one poison batch while accepting everything else flap
// warn/recovery on every export — see the split at the top of this file.
//
// A CANCELLED export is deliberately not a failure: shutdown cancels every
// in-flight send at once, and a "the collector is failing" line emitted while
// the process is on its way out is a false alarm on the one line an operator
// reads most carefully. It is still counted (the caller's counter runs first) —
// only the narrative skips it.
func (r *FailureReporter) Note(signal string, err error) {
	if err != nil && errors.Is(err, context.Canceled) {
		return
	}
	if err != nil && IsPermanent(err) {
		// A verdict on THIS payload. It is reported, and it is deliberately
		// kept out of the destination's health: see the split at the top of
		// this file.
		r.noteRejected(signal, err)
		return
	}
	if err == nil && r.failing.Load() == 0 {
		return // nothing to narrate: the steady state takes no lock and no clock
	}
	now := r.now()
	r.mu.Lock()
	st, ok := r.state[signal]
	if !ok {
		st = &failState{}
		r.state[signal] = st
	}
	var (
		recovered bool
		warn      bool
		failures  int64
		since     time.Time
	)
	if err == nil {
		if st.failures > 0 {
			recovered, failures, since = true, st.failures, st.since
			*st = failState{}
			r.failing.Add(-1)
		}
	} else {
		// Everything reaching here is TRANSIENT (permanent returned above), so
		// there is no class transition left to re-warn on: the shape of the
		// narrative is one condition, throttled.
		if st.failures == 0 {
			st.since = now
			warn = true
			r.failing.Add(1)
		} else if now.Sub(st.lastLog) >= r.every {
			warn = true
		}
		st.failures++
		if warn {
			st.lastLog = now
		}
		failures, since = st.failures, st.since
	}
	r.mu.Unlock()

	switch {
	case recovered:
		args := append([]any{"signal", signal, "failures", failures, "outage", now.Sub(since).Round(time.Second)}, r.attrs...)
		r.log.Info("exports to "+r.what+" are being accepted again", args...)
	case warn:
		// class is "transient" by construction here; it stays on the line so it
		// reads the same way as the rejection line below and as the
		// kubescrape_export_requests_total{outcome} label an alert selects on.
		args := []any{"signal", signal, "class", Class(err), "failures", failures}
		if failures > 1 {
			// Only on a re-warn: on the first line it is 0 by definition, and a
			// key whose value is always the same on the line an operator reads
			// first is noise.
			args = append(args, "failing", now.Sub(since).Round(time.Second))
		}
		args = append(args, "note", Diagnose(err), "error", err)
		args = append(args, r.attrs...)
		r.log.Warn("exports to "+r.what+" are failing; this telemetry is not reaching its destination", args...)
	}
}

// noteRejected reports a PERMANENTLY rejected payload: throttled per signal,
// with the count and span of what was dropped since the previous line, and
// touching no destination state.
//
// The throttle is the same window the destination narrative uses, and it is a
// plain last-log stamp rather than a transition: a rejection has no recovery
// edge to arm, so it cannot flap however the exports around it go.
func (r *FailureReporter) noteRejected(signal string, err error) {
	now := r.now()
	r.mu.Lock()
	st, ok := r.rejected[signal]
	if !ok {
		st = &rejectState{windowStart: now}
		r.rejected[signal] = st
	}
	st.rejections++
	warn := st.lastLog.IsZero() || now.Sub(st.lastLog) >= r.every
	var (
		count int64
		span  time.Duration
	)
	if warn {
		count, span = st.rejections, now.Sub(st.windowStart)
		st.lastLog, st.windowStart, st.rejections = now, now, 0
	}
	r.mu.Unlock()
	if !warn {
		return
	}
	args := []any{"signal", signal, "class", "permanent", "rejected", count}
	if count > 1 {
		// Only when it is a rate rather than a single event: on the first line
		// the span is zero by definition.
		args = append(args, "over", span.Round(time.Second))
	}
	args = append(args, "note", Diagnose(err), "error", err)
	args = append(args, r.attrs...)
	r.log.Warn(r.what+" rejected this telemetry outright; it is dropped rather than retried", args...)
}

// Class is how an export failure is classified for the payload's sake:
// "permanent" means the collector rejected THIS payload and retrying it cannot
// help (the producer drops it, or the disk buffer eventually does), "transient"
// means it is coming back. It is IsPermanent's answer, named — the same
// classification every producer's retry path already takes, made visible so an
// operator does not have to infer it from a drop counter.
func Class(err error) string {
	if err == nil {
		return "ok"
	}
	if IsPermanent(err) {
		return "permanent"
	}
	return "transient"
}

// Diagnose names the likeliest cause of an export failure, for the `note` key.
//
// It is a HINT and says so by being one: every arm below is a shape an operator
// meets on a first live run, where the raw error is technically complete and
// practically unreadable ("connection refused" does not say which flag names
// the host it refused). An unrecognised error gets no note rather than a
// guessed one — a wrong hint costs more than a missing one.
func Diagnose(err error) string {
	if err == nil {
		return ""
	}
	var he *HTTPStatusError
	if errors.As(err, &he) {
		switch {
		case he.Code == 401 || he.Code == 403:
			return "the receiver rejected the credentials; check the bearer token file this destination presents (-otlp-bearer-token-file on the default chain; a route, a per-signal export override and the trace tier's shard hop each name their own) and any static headers. The payload is retried, so fixing the credential recovers it"
		case he.Code == 404:
			return "the collector has no such path; check the endpoint's base URL (retried, in case a rollout is reprogramming its routes)"
		case he.Code == 413:
			return "the collector's body limit is smaller than what is being sent; lower -otlp-max-send-bytes or raise the receiver's limit"
		case he.Code == 415:
			return "the collector refused the media type; this client sends application/x-protobuf, so the endpoint is probably not an OTLP/HTTP receiver"
		case he.Code == 429 || he.Code == 503:
			return "the collector is back-pressuring or unavailable; the payload is retried"
		case he.Code == 400:
			return "the collector rejected the payload as malformed; it will not be retried"
		case he.Code >= 300 && he.Code < 400:
			return "the endpoint redirected and this client does not follow redirects (a replayed body could read reused memory); point the endpoint at the final URL — a redirect to https:// means the endpoint should be https://"
		}
		return ""
	}
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		switch st.Code() {
		case codes.Unimplemented:
			return "the collector does not serve this signal on this endpoint; check that the receiver has a pipeline for it"
		case codes.Unauthenticated, codes.PermissionDenied:
			return "the receiver rejected the credentials; check the bearer token file this destination presents (-otlp-bearer-token-file on the default chain; a route, a per-signal export override and the trace tier's shard hop each name their own) and any static headers"
		case codes.ResourceExhausted:
			return "the collector refused the payload for its size or its own back-pressure; lower -otlp-max-send-bytes if this persists"
		case codes.DeadlineExceeded:
			return "the export did not finish inside -otlp-timeout; the collector is slow, unreachable, or the payload is too large for the link"
		}
	}
	// Below the status layer: the transport never got an answer. These are the
	// first-run mistakes, and the error text they arrive as names the symptom
	// rather than the setting to change.
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
		return "the endpoint's host does not resolve; check the name and namespace of the collector Service"
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "connection refused"):
		return "nothing is listening at the endpoint; check the endpoint host and port and whether the collector is running"
	case strings.Contains(msg, "no such host"):
		return "the endpoint's host does not resolve; check the name and namespace of the collector Service"
	case strings.Contains(msg, "first record does not look like a TLS handshake"),
		strings.Contains(msg, "server gave HTTP response to HTTPS client"):
		return "the endpoint speaks plaintext but this client is using TLS; set -otlp-insecure for gRPC, or an http:// endpoint for OTLP/HTTP"
	case strings.Contains(msg, "x509:"), strings.Contains(msg, "tls:"), strings.Contains(msg, "certificate"):
		return "TLS could not be established; check -otlp-ca-file, the certificate's names, or -otlp-insecure-skip-verify to confirm that is the cause"
	case strings.Contains(msg, "i/o timeout"), strings.Contains(msg, "context deadline exceeded"):
		return "the endpoint did not answer inside -otlp-timeout; check network policy and that the port is the collector's OTLP port"
	}
	return ""
}
