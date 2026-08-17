// Package tracesample drops ingested spans before they are forwarded: a
// consistent probabilistic sampler plus guard rails (always keep errors,
// always keep slow spans, cap total spans/second). It wraps the RAW trace
// exporter BELOW the spanmetrics tap, so RED metrics are still derived from
// 100% of the spans while only the sampled subset is shipped — the classic
// spanmetrics-plus-sampling arrangement.
//
// Decisions are deterministic per trace ID (tracehash: a rapidhash of the ID
// against the probability threshold), so all spans of a trace sample
// identically on this node AND on every other node running the same config —
// a node-local sampler still yields whole traces. A sender's retry of a failed
// payload re-samples identically, keeping the at-least-once path consistent.
package tracesample

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/tracehash"
	"github.com/JohanLindvall/kubescrape/internal/config"
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
	// probability decision (a Go duration such as "2s"; empty/0 disables).
	//
	// A STRING, not a time.Duration: the config is decoded through
	// sigs.k8s.io/yaml -> encoding/json, which cannot unmarshal "2s" into a
	// time.Duration and only accepts a raw nanosecond integer. Since the loader
	// is strict and a decode error is fatal, the documented spelling made the
	// agent refuse to start. Same treatment as logMetrics' maxAge.
	KeepSlowerThan string `json:"keepSlowerThan,omitempty"`
	// MaxSpansPerSecond caps forwarded spans after sampling; excess spans are
	// dropped and counted (0 = uncapped). A hard safety valve, applied to
	// guard-rail keeps too — a cap that can be exceeded is not a cap. NOTE:
	// when the ingest batcher retries a payload, the rate bucket is consumed
	// again for the re-sent spans, so the effective cap can be slightly
	// stricter than configured under sustained retries — acceptable for a
	// safety valve (the probability decision stays exact, being per-trace-ID).
	MaxSpansPerSecond float64 `json:"maxSpansPerSecond,omitempty"`
}

// Enabled reports whether the config asks for any sampling at all.
func (c Config) Enabled() bool {
	return (c.Probability > 0 && c.Probability < 1) || c.MaxSpansPerSecond > 0
}

// SlowerThan parses KeepSlowerThan. An empty value — and an explicit zero —
// disables the guard rail. Exported because the agent's configWarnings
// (cmd/kubescrape-agent) asks "is this guard rail armed?" and must read the
// field EXACTLY as the sampler does — it used to re-parse with bare
// time.ParseDuration, so a semantic change here would not have reached it.
func (c Config) SlowerThan() (time.Duration, error) {
	return config.Duration("traceSampling.keepSlowerThan", c.KeepSlowerThan, 0, config.ZeroDisables())
}

// Validate reports a malformed config, so a bad value fails startup with a
// clear message instead of silently disabling the guard rail.
func (c Config) Validate() error {
	// `probability: 50` is the percent-vs-fraction typo, and it does not fail
	// loudly: Enabled() reads it as out of the (0,1) sampling range, so the
	// whole sampler switches OFF — no "trace sampling enabled" line, a clean
	// -check-config, and 100% of spans shipped by a config asking for 50%.
	if c.Probability < 0 || c.Probability > 1 {
		return fmt.Errorf("probability %v is not a fraction in [0,1] (50%% is 0.5, not 50)", c.Probability)
	}
	if c.MaxSpansPerSecond < 0 {
		return fmt.Errorf("maxSpansPerSecond %v is negative", c.MaxSpansPerSecond)
	}
	_, err := c.SlowerThan()
	return err
}

// Exporter is the downstream trace exporter (otlpexport.Client and Buffered
// satisfy it).
type Exporter interface {
	ExportTraces(ctx context.Context, td ptrace.Traces) error
}

// Sampler filters spans and forwards the remainder.
type Sampler struct {
	next      Exporter
	threshold uint64 // tracehash.Threshold(probability); Keep compares the hashed id
	keepErr   bool
	slow      time.Duration

	// Token bucket for MaxSpansPerSecond (tracehash.Bucket carries the mutex:
	// without the ingest batcher, ExportTraces runs on concurrent ingest
	// handlers). rate is kept beside it for the is-a-cap-configured checks.
	rate   float64
	bucket *tracehash.Bucket
	now    func() time.Time // injectable for tests
}

// New builds a Sampler in front of next.
func New(cfg Config, next Exporter) *Sampler {
	slow, _ := cfg.SlowerThan() // validated at startup via Validate
	p := cfg.Probability
	if p <= 0 || p > 1 {
		p = 1
	}
	keepErr := cfg.KeepErrors == nil || *cfg.KeepErrors
	s := &Sampler{
		next:    next,
		keepErr: keepErr,
		slow:    slow,
		rate:    cfg.MaxSpansPerSecond,
		// Starts full, burst floored at one whole span — tracehash.NewBucket
		// carries the fractional-rate war story (a burst below one token
		// dropped 100% of spans forever).
		bucket: tracehash.NewBucket(cfg.MaxSpansPerSecond),
		now:    time.Now,
	}
	s.threshold = tracehash.Threshold(p)
	return s
}

// ExportTraces drops unsampled spans and forwards the remainder. A payload
// sampled down to nothing is acked without a send.
//
// The INPUT is never mutated: the spanmetrics tap sits above this sampler
// and aggregates from the payload AFTER a successful forward — pruning in
// place would feed RED metrics only the sampled subset (and nothing at all
// for a fully-sampled-away batch). Sampling therefore works on a copy; the
// all-kept fast path forwards the original without copying.
func (s *Sampler) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	if !s.wouldDrop(td) {
		return s.next.ExportTraces(ctx, td)
	}
	out := ptrace.NewTraces()
	td.CopyTo(out)
	// Tallied here, counted after the forward SUCCEEDS — the repo's
	// forward-first-act-on-success rule, which the spanmetrics tap above this
	// sampler follows for the same reason. A failed export is re-pushed by the
	// sender, the probability decision is deterministic, and the identical spans
	// would be reported dropped once per retry: an operator sizing sampling loss
	// during a back-pressure window would read it multiplied by the retry count.
	var byProb, byRate int
	rss := out.ResourceSpans()
	rss.RemoveIf(func(rs ptrace.ResourceSpans) bool {
		sss := rs.ScopeSpans()
		sss.RemoveIf(func(ss ptrace.ScopeSpans) bool {
			spans := ss.Spans()
			spans.RemoveIf(func(sp ptrace.Span) bool {
				if !s.keep(sp) {
					byProb++
					return true
				}
				if s.rate > 0 && !s.allow() {
					byRate++
					return true
				}
				return false
			})
			return spans.Len() == 0
		})
		return sss.Len() == 0
	})
	if out.ResourceSpans().Len() == 0 {
		// Everything sampled away: acked, nothing to send — so there is no
		// forward whose outcome the drops could wait on, and they are final.
		s.countDropped(byProb, byRate)
		return nil
	}
	if err := s.next.ExportTraces(ctx, out); err != nil {
		return err
	}
	s.countDropped(byProb, byRate)
	return nil
}

// countDropped publishes one payload's tallies. The rate arm's tally is the one
// this pass made: a retry re-bills the bucket, so its own drops may be a
// different subset, and only the pass that shipped is reported.
func (s *Sampler) countDropped(byProb, byRate int) {
	if byProb > 0 {
		obs.TraceSpansDropped.WithLabelValues("probability").Add(float64(byProb))
	}
	if byRate > 0 {
		obs.TraceSpansDropped.WithLabelValues("rate").Add(float64(byRate))
	}
}

// wouldDrop reports whether any span in td would be dropped — the decision
// pass that lets the common all-kept case skip the payload copy.
//
// The probability scan comes FIRST and touches no state. Only when every span
// survives it does the rate cap get consulted, and there it CONSUMES the whole
// payload's tokens: taking the fast path means these spans ship, so they must
// be paid for. Charging before the probability scan would over-charge for
// spans that never ship — the per-span path in ExportTraces bills those one at
// a time, after their own probability check, which is the same accounting.
func (s *Sampler) wouldDrop(td ptrace.Traces) bool {
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		sss := rss.At(i).ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				if !s.keep(spans.At(k)) {
					return true
				}
			}
		}
	}
	if s.rate > 0 && !s.consume(float64(td.SpanCount())) {
		// Not enough tokens for the whole payload: the copying path bills and
		// drops span by span, which is where a partial cap hit belongs.
		return true
	}
	return false
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
	// tracehash carries the nesting contract with tailsample's probabilistic
	// policy: same unsalted hash, same threshold arithmetic.
	return tracehash.Keep(sp.TraceID(), s.threshold)
}

// consume takes n tokens if the bucket holds them, and reports whether it did
// (tracehash.Bucket.TakeExact — all-or-nothing, so the fast path either pays
// for the whole payload or hands it to the per-span path). It passes s.now
// (the injectable clock), not time.Now: the two once disagreed, so a test
// driving the clock forward saw the bucket refill from wall time instead.
//
// The fast path MUST pay. It used to peek an availableTokens() that consumed
// nothing and then forward the payload untouched, so any batch smaller than
// the current bucket bypassed the cap entirely and the bucket refilled to
// full before the next one: 100 spans/s sailed through a 10/s cap with
// TraceSpansDropped{reason="rate"} sitting at 0. The cap only ever bound
// payloads that were ALSO losing spans to probability.
func (s *Sampler) consume(n float64) bool {
	return s.bucket.TakeExact(n, s.now())
}

// allow consumes one rate-cap token.
func (s *Sampler) allow() bool { return s.consume(1) }
