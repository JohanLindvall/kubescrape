// Package tracesample drops ingested spans before they are forwarded: a
// consistent probabilistic sampler plus guard rails (always keep errors,
// always keep slow spans, cap total spans/second). It wraps the RAW trace
// exporter BELOW the spanmetrics tap, so RED metrics are still derived from
// 100% of the spans while only the sampled subset is shipped — the classic
// spanmetrics-plus-sampling arrangement.
//
// Decisions are deterministic per trace ID (an xxhash of the ID against the
// probability threshold), so all spans of a trace sample identically on this
// node AND on every other node running the same config — a node-local
// sampler still yields whole traces. A sender's retry of a failed payload
// re-samples identically, keeping the at-least-once path consistent.
package tracesample

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// Config is the agent config's traceSampling section.
type Config struct {
	// Probability keeps this fraction of traces (by trace ID). 1 (and the
	// zero value, treated as "unset") keeps everything.
	Probability float64 `json:"probability"`
	// KeepErrors keeps spans with status ERROR regardless of the probability
	// decision (default true).
	KeepErrors *bool `json:"keepErrors,omitempty"`
	// KeepSlowerThan keeps spans at least this slow regardless of the
	// probability decision (0 disables).
	KeepSlowerThan time.Duration `json:"keepSlowerThan,omitempty"`
	// MaxSpansPerSecond caps forwarded spans after sampling; excess spans are
	// dropped and counted (0 = uncapped). A hard safety valve, applied to
	// guard-rail keeps too — a cap that can be exceeded is not a cap.
	MaxSpansPerSecond float64 `json:"maxSpansPerSecond,omitempty"`
}

// Enabled reports whether the config asks for any sampling at all.
func (c Config) Enabled() bool {
	return (c.Probability > 0 && c.Probability < 1) || c.MaxSpansPerSecond > 0
}

// Exporter is the downstream trace exporter (otlpexport.Client and Buffered
// satisfy it).
type Exporter interface {
	ExportTraces(ctx context.Context, td ptrace.Traces) error
}

// Sampler filters spans and forwards the remainder.
type Sampler struct {
	next      Exporter
	threshold uint64 // keep when xxhash(traceID) < threshold
	keepErr   bool
	slow      time.Duration

	// Token bucket for MaxSpansPerSecond. Guarded: without the ingest
	// batcher, ExportTraces runs on concurrent ingest handlers.
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
	now    func() time.Time // injectable for tests
}

// New builds a Sampler in front of next.
func New(cfg Config, next Exporter) *Sampler {
	p := cfg.Probability
	if p <= 0 || p > 1 {
		p = 1
	}
	keepErr := cfg.KeepErrors == nil || *cfg.KeepErrors
	s := &Sampler{
		next:    next,
		keepErr: keepErr,
		slow:    cfg.KeepSlowerThan,
		rate:    cfg.MaxSpansPerSecond,
		burst:   cfg.MaxSpansPerSecond, // one second of headroom
		now:     time.Now,
	}
	if p == 1 {
		s.threshold = math.MaxUint64
	} else {
		s.threshold = uint64(p * float64(math.MaxUint64))
	}
	s.tokens = s.burst
	return s
}

// ExportTraces drops unsampled spans and forwards the remainder. A payload
// sampled down to nothing is acked without a send.
func (s *Sampler) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	dropped := 0
	rss := td.ResourceSpans()
	rss.RemoveIf(func(rs ptrace.ResourceSpans) bool {
		sss := rs.ScopeSpans()
		sss.RemoveIf(func(ss ptrace.ScopeSpans) bool {
			spans := ss.Spans()
			spans.RemoveIf(func(sp ptrace.Span) bool {
				if !s.keep(sp) {
					dropped++
					obs.TraceSpansDropped.WithLabelValues("probability").Inc()
					return true
				}
				if s.rate > 0 && !s.allow() {
					dropped++
					obs.TraceSpansDropped.WithLabelValues("rate").Inc()
					return true
				}
				return false
			})
			return spans.Len() == 0
		})
		return sss.Len() == 0
	})
	if td.ResourceSpans().Len() == 0 {
		return nil // everything sampled away: acked, nothing to send
	}
	return s.next.ExportTraces(ctx, td)
}

// keep is the per-span decision (rate cap excluded).
func (s *Sampler) keep(sp ptrace.Span) bool {
	if s.keepErr && sp.Status().Code() == ptrace.StatusCodeError {
		return true
	}
	if s.slow > 0 && sp.EndTimestamp() > sp.StartTimestamp() &&
		time.Duration(sp.EndTimestamp()-sp.StartTimestamp()) >= s.slow {
		return true
	}
	if s.threshold == math.MaxUint64 {
		return true
	}
	// Deterministic per trace: hashing (rather than trusting W3C trace-ID
	// randomness) stays uniform even for senders with non-random IDs.
	id := sp.TraceID()
	return xxhash.Sum64(id[:]) < s.threshold
}

// allow consumes one rate-cap token.
func (s *Sampler) allow() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if !s.last.IsZero() {
		s.tokens = min(s.burst, s.tokens+s.rate*now.Sub(s.last).Seconds())
	}
	s.last = now
	if s.tokens < 1 {
		return false
	}
	s.tokens--
	return true
}
