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
	"log/slog"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/tracehash"
	"github.com/JohanLindvall/kubescrape/internal/config"
	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
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

	// Logger reports what New decides for itself and what the rate cap costs at
	// runtime. json:"-" — wiring, not config (tailsample.Config.Script is the
	// same shape); nil means slog.Default(), which both binaries install as the
	// logfmt handler before anything runs.
	Logger *slog.Logger `json:"-"`
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

	log *slog.Logger
	// rateWarn throttles the spans/second-cap warning: at the cap every payload
	// takes that path.
	rateWarn logdedupe.Throttle
}

// New builds a Sampler in front of next.
func New(cfg Config, next Exporter) *Sampler {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	slow, err := cfg.SlowerThan() // validated at startup via Validate
	if err != nil {
		// Validate reports this and -check-config runs it, so reaching here
		// means a start that got past validation. Falling back is right —
		// refusing would take traces down for a typo — but silently disabling a
		// guard rail the operator wrote down is not.
		log.Warn("traceSampling.keepSlowerThan is unparseable; the keep-slow guard rail is disabled",
			"error", err, "keepSlowerThan", cfg.KeepSlowerThan)
	}
	p := cfg.Probability
	if p <= 0 || p > 1 {
		if cfg.Probability != 0 {
			// Config.Validate refuses anything outside [0,1] (the percent-vs-
			// fraction typo), so this is a should-not-happen branch — and what
			// it does is ship 100% of spans under a config that asked for a
			// fraction, which is the expensive direction to be silent about.
			log.Warn("traceSampling.probability is not a fraction in (0,1]; keeping every trace",
				"probability", cfg.Probability)
		}
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
		log:    log,
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
	// Tallied here, counted after the forward SUCCEEDS — the repo's
	// forward-first-act-on-success rule, which the spanmetrics tap above this
	// sampler follows for the same reason. A failed export is re-pushed by the
	// sender, the probability decision is deterministic, and the identical spans
	// would be reported dropped once per retry: an operator sizing sampling loss
	// during a back-pressure window would read it multiplied by the retry count.
	out, byProb, byRate := s.sampled(td)
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

// sampled builds the payload to forward by copying only the spans that SURVIVE,
// creating each destination resource and scope lazily on its first kept span.
// It returns the payload and the two drop tallies, and never touches td.
//
// The copy is the whole cost of this path — a span carries its attributes,
// events and links, and pdata copies each one — so it has to be paid for the
// spans that ship, not for the ones that do not. This used to deep-copy the
// WHOLE payload and then RemoveIf the drops out of it, which cost the same
// 24 allocations and ~1.3 kB per RECEIVED span whatever the probability was:
// at `probability: 0.01` ninety-nine hundredths of that copy was built and
// thrown away in the same call, on the trace tier's per-push path, for every
// span the cluster emits. Measured on a 200-span batch of realistically
// instrumented spans: 4822 allocs/op flat across every probability, against
// 2429 at 0.5, 266 at 0.1 and 2 at 0.01 here (the last keeps no trace at all,
// so the payload is acked without a send).
//
// The traversal order and the two checks are UNCHANGED from that RemoveIf,
// which matters for more than the tallies: the rate bucket is consumed per span
// in payload order, so a reordering would move which spans a partially-drained
// bucket admits.
func (s *Sampler) sampled(td ptrace.Traces) (ptrace.Traces, int, int) {
	out := ptrace.NewTraces()
	var byProb, byRate int
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		var dstRS ptrace.ResourceSpans
		haveRS := false
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			ss := sss.At(j)
			var dstSS ptrace.ScopeSpans
			haveSS := false
			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				sp := spans.At(k)
				if !s.keep(sp) {
					byProb++
					continue
				}
				if s.rate > 0 && !s.allow() {
					byRate++
					continue
				}
				if !haveRS {
					// A resource (and a scope) reaches the output only once one
					// of its spans does, which is what makes an all-dropped
					// group cost nothing — the RemoveIf shape copied it first
					// and deleted it afterwards.
					dstRS = out.ResourceSpans().AppendEmpty()
					rs.Resource().CopyTo(dstRS.Resource())
					dstRS.SetSchemaUrl(rs.SchemaUrl())
					haveRS = true
				}
				if !haveSS {
					dstSS = dstRS.ScopeSpans().AppendEmpty()
					ss.Scope().CopyTo(dstSS.Scope())
					dstSS.SetSchemaUrl(ss.SchemaUrl())
					haveSS = true
				}
				sp.CopyTo(dstSS.Spans().AppendEmpty())
			}
		}
	}
	return out, byProb, byRate
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
		// The two drop reasons are NOT alike and only one is worth a line.
		// Probability drops are the feature working, at the configured rate,
		// forever. The rate cap is a safety valve: when it bites, sampling is
		// no longer what the operator configured, whole traces are cut in half
		// (the cap is per span, not per trace), and the remedy is a number in
		// the config. Once per payload, throttled — never per span.
		if s.rateWarn.Allow(rateWarnEvery) {
			s.log.Warn("the traceSampling.maxSpansPerSecond cap is dropping spans, so traces are shipping incomplete; the cap is a per-span safety valve and does not respect trace boundaries",
				"dropped", byRate, "maxSpansPerSecond", s.rate)
		}
	}
}

// rateWarnEvery re-warns while the spans/second cap keeps biting.
const rateWarnEvery = time.Minute

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
