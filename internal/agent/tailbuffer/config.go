package tailbuffer

import (
	"errors"
	"fmt"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/tailsample"
	"github.com/JohanLindvall/kubescrape/internal/config"
)

// Defaults. The memory ones are sized for a shard with a ~1 GiB limit: a
// buffered span costs ~365 B for a minimal one and ~1 KiB for a realistic one
// (bench_test.go measures both), plus ~470 B once per (pushed payload, trace)
// group for the ResourceSpans/ScopeSpans wrapper and its copy of the resource
// attributes — the part that surprises people. maxSpans 200k is therefore
// ~100-300 MiB of spans.
//
// It is not left at that number blindly: memory.go checks it against the
// container's actual memory limit at startup, lowers the DEFAULT when the limit
// cannot afford it, and refuses an explicit setting that could only end in an
// OOM. The sizing rule in one line is
//
//	maxSpans x 1 KiB must fit in a quarter of the pod's memory limit.
//
// The arithmetic an operator has to do on top of that: a shard receiving R
// spans/second holds up to R * (decisionWait + tick) spans in the steady state
// — a trace waits for the first sweep AFTER its window closes, and the tick is
// a quarter of the window clamped to 100ms-1s (tickFor) — plus R * the decision
// loop's export time whenever the collector is slow, because the loop decides
// nothing while its own send is in flight (see the TIME bound in the package
// doc). At 50k spans/s and a 5s window that is 300k — above the default, so the
// maxSpans bound would bind and decide the oldest traces early. Raising maxSpans
// costs memory LINEARLY and raises the odds of the OOM that loses the whole
// buffer, so the answer above a pod's budget is more shards (the ring divides R
// by the shard count), not a bigger number; the early counter says which is
// happening.
const (
	defaultDecisionWait     = 5 * time.Second
	defaultMaxTraces        = 100_000
	defaultMaxSpansPerTrace = 1_000
	defaultMaxSpans         = 200_000
	defaultCacheSize        = 100_000
	defaultCacheTTL         = time.Minute
)

// Config is the agent config's tailSampling section: the policy list
// (agent/tailsample, embedded so the section reads as one thing) plus this
// layer's own memory and timing knobs.
//
// Every duration is a STRING parsed with time.ParseDuration, never a
// time.Duration: the agent config decodes through sigs.k8s.io/yaml ->
// encoding/json, which accepts only a raw nanosecond integer for a
// time.Duration, and because the file is UnmarshalStrict'ed one such field
// rejects the WHOLE config and the workload does not start. Same treatment as
// tailsample's own thresholds, servicegraph.Config.Wait and
// tracesample.Config.KeepSlowerThan.
type Config struct {
	// Config is the policy list — `policies`, evaluated in order, first match
	// wins. Embedded, so the section is one flat object and the policy syntax is
	// documented in exactly one place (agent/tailsample's package doc).
	tailsample.Config

	// DecisionWait is how long a trace is held from the moment its FIRST span
	// arrives before it is judged ("5s" by default). It is the assembly budget:
	// long enough to cover the slowest span's journey from its process to this
	// shard, short enough that the buffer's contents — which a hard kill loses —
	// stay small. It is not a latency budget for the trace itself; a trace whose
	// root span is still open when the window closes is judged on what arrived.
	DecisionWait string `json:"decisionWait,omitempty"`

	// MaxTraces caps distinct traces held at once (100000 default). At the cap
	// the oldest is decided early rather than evicted.
	MaxTraces int `json:"maxTraces,omitempty"`
	// MaxSpansPerTrace caps one trace's buffered spans (1000 default). At the cap
	// that trace is decided on the spans present and its remainder follows the
	// decision through the cache — which is what keeps one pathological trace
	// (a retry storm under a single id, an instrumented loop) from being the
	// whole buffer.
	MaxSpansPerTrace int `json:"maxSpansPerTrace,omitempty"`
	// MaxSpans caps the buffer's TOTAL spans (200000 default) — the bound that
	// actually determines the process's memory, since the other two multiply.
	//
	// Budget ~1 KiB per span and keep the product under a QUARTER of the pod's
	// memory limit (memory.go explains the share). Left unset, the default is
	// lowered at startup to whatever the limit affords; set explicitly it is
	// honoured, warned about above that budget, and refused outright when the
	// spans alone would need the whole limit — an OOM here loses every buffered
	// span at once, which is strictly worse than the early decisions a smaller
	// ceiling causes.
	MaxSpans int `json:"maxSpans,omitempty"`

	// DecisionCacheSize bounds the verdict cache used for spans arriving after
	// their trace decided (100000 default). At the cap the oldest verdict is
	// evicted and a later span for it starts a FRESH window — see put.
	DecisionCacheSize int `json:"decisionCacheSize,omitempty"`
	// DecisionCacheTTL is how long a verdict is remembered ("1m" default). It
	// bounds how late a straggler can still follow its trace's decision;
	// stragglers later than this are indistinguishable from a new trace.
	DecisionCacheTTL string `json:"decisionCacheTTL,omitempty"`
}

// Enabled reports whether tail sampling is configured. The knobs alone do not
// enable it: a section with bounds but no policies would drop every trace.
func (c *Config) Enabled() bool { return c != nil && c.Config.Enabled() }

// Validate reports a malformed section. Shape-only — no clock, no filesystem, no
// network — so -check-config runs it, and it validates the policy list by
// COMPILING it (tailsample.Config.Validate), so the dry run cannot drift from
// what a real start accepts.
func (c *Config) Validate() error {
	if c == nil {
		return nil
	}
	if !c.Enabled() {
		// Bounds without policies: the section reads as configured and samples
		// nothing, which is indistinguishable from the feature being off. The
		// cache knobs count too — `decisionCacheSize: 50000` with no policies
		// is the same misconfiguration as `maxTraces: 5` with none, and only
		// the latter used to be caught.
		if c.DecisionWait != "" || c.MaxTraces != 0 || c.MaxSpans != 0 || c.MaxSpansPerTrace != 0 ||
			c.DecisionCacheSize != 0 || c.DecisionCacheTTL != "" {
			return errors.New("tailSampling has buffer settings but no policies (an evaluator with no policies drops every trace, so the settings would silently sample nothing)")
		}
		return nil
	}
	if err := c.Config.Validate(); err != nil {
		return err
	}
	_, err := c.settings()
	return err
}

// settings resolves the config to its effective values, defaults applied. It is
// the ONE place a default or a bound check lives, shared by Validate and New so
// a dry run and a start cannot disagree.
func (c *Config) settings() (settings, error) {
	s := settings{
		wait:             defaultDecisionWait,
		maxTraces:        defaultMaxTraces,
		maxSpansPerTrace: defaultMaxSpansPerTrace,
		maxSpans:         defaultMaxSpans,
		cacheSize:        defaultCacheSize,
		cacheTTL:         defaultCacheTTL,
	}
	var err error
	// config.Duration is the one optional-duration reader (empty takes the
	// default, a negative value is an error naming the field and the value);
	// Positive folds in the bound check these two fields need, so the
	// explanation stays attached to the error rather than living in a separate
	// if below it.
	if s.wait, err = config.Duration("tailSampling.decisionWait", c.DecisionWait, defaultDecisionWait,
		config.Positive("a zero window decides every trace on its first span")); err != nil {
		return s, err
	}
	if s.cacheTTL, err = config.Duration("tailSampling.decisionCacheTTL", c.DecisionCacheTTL, defaultCacheTTL,
		config.Positive("with no cache every late span re-decides its trace")); err != nil {
		return s, err
	}
	for _, b := range []struct {
		field string
		val   int
		dst   *int
	}{
		{"maxTraces", c.MaxTraces, &s.maxTraces},
		{"maxSpansPerTrace", c.MaxSpansPerTrace, &s.maxSpansPerTrace},
		{"maxSpans", c.MaxSpans, &s.maxSpans},
		{"decisionCacheSize", c.DecisionCacheSize, &s.cacheSize},
	} {
		if b.val < 0 {
			return s, fmt.Errorf("tailSampling.%s %d is negative", b.field, b.val)
		}
		if b.val > 0 {
			*b.dst = b.val
		}
	}
	// A trace that cannot fit under the total ceiling could never be decided
	// normally: the per-trace bound would never be reached, and the ceiling would
	// early-decide it (and everything else) on every payload.
	if s.maxSpansPerTrace > s.maxSpans {
		return s, fmt.Errorf("tailSampling.maxSpansPerTrace %d is above maxSpans %d (one trace could never fit in the buffer)", s.maxSpansPerTrace, s.maxSpans)
	}
	return s, nil
}

// settings is the resolved config.
type settings struct {
	wait             time.Duration
	maxTraces        int
	maxSpansPerTrace int
	maxSpans         int
	cacheSize        int
	cacheTTL         time.Duration
}
