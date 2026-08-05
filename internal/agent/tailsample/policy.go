package tailsample

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/tracehash"
)

const (
	// defaultRegexCacheSize bounds a stringAttribute regex cache when the
	// config does not. Big enough that a per-route or per-status-code
	// attribute never misses in practice, small enough to be uninteresting.
	defaultRegexCacheSize = 1000
	// maxRegexCacheKeyBytes: values longer than this are matched but never
	// cached. The keys come off the wire, and a cache bounded only by ENTRY
	// COUNT is not bounded in bytes — one instrumented service logging a
	// megabyte-long attribute would pin a megabyte per distinct value.
	maxRegexCacheKeyBytes = 256
)

// verdict is one policy's opinion.
type verdict uint8

const (
	// verdictAbstain: no opinion, the next policy decides. This is what "does
	// not match" means, and also what an inconclusive evaluation means (a
	// missing attribute, a wrongly-typed value, a trace with no timestamps) —
	// see the package doc on why there is no error verdict.
	verdictAbstain verdict = iota
	// verdictSample: keep the trace, stop evaluating.
	verdictSample
	// verdictVeto: drop the trace, stop evaluating. Only an inverted policy
	// produces one, directly or by propagation through and/composite.
	verdictVeto
)

// policy is one compiled entry of the list.
type policy interface {
	// eval returns its verdict and, when a COMPOSING policy delegated the
	// decision, the precomputed qualified name of the sub-policy that made it
	// ("" means "attribute this to my own configured name").
	//
	// now is the single clock reading of this decision, zero when no policy in
	// the whole list needs one.
	eval(t Trace, now time.Time) (verdict, string)
}

// namedPolicy pairs a compiled policy with the name a Decision reports. The
// name lives out here rather than in every leaf struct so that a leaf carries
// only what it evaluates.
type namedPolicy struct {
	name string
	p    policy
}

// names lists every name this entry can put in a Decision — its own, or a
// composite's qualified sub-names.
func (n namedPolicy) names() []string {
	if s, ok := n.p.(interface{ subNames() []string }); ok {
		return s.subNames()
	}
	return []string{n.name}
}

// compilePolicies compiles a list and reports whether any policy in it needs a
// clock. sub marks a nested list (and/composite), where and/composite are
// themselves refused.
func compilePolicies(list []PolicyConfig, sub bool) ([]namedPolicy, bool, error) {
	out := make([]namedPolicy, 0, len(list))
	seen := make(map[string]struct{}, len(list))
	needClock := false
	for i, pc := range list {
		where := policyWhere(sub, i, pc.Name)
		if pc.Name == "" {
			return nil, false, errPolicy(where, "name is required (it is what a Decision and its metric label report)")
		}
		if _, dup := seen[pc.Name]; dup {
			return nil, false, errPolicy(where, "duplicate policy name %q", pc.Name)
		}
		seen[pc.Name] = struct{}{}
		p, clock, err := compilePolicy(pc, where, sub)
		if err != nil {
			return nil, false, err
		}
		needClock = needClock || clock
		out = append(out, namedPolicy{name: pc.Name, p: p})
	}
	return out, needClock, nil
}

// policyWhere locates a policy for an error message.
func policyWhere(sub bool, i int, name string) string {
	kind := "policies"
	if sub {
		kind = "sub-policies"
	}
	if name == "" {
		return fmt.Sprintf("%s[%d]", kind, i)
	}
	return fmt.Sprintf("%s[%d] %q", kind, i, name)
}

// compilePolicy builds one policy and checks that exactly the body matching its
// type is present.
func compilePolicy(pc PolicyConfig, where string, sub bool) (policy, bool, error) {
	if pc.Type == "" {
		return nil, false, errPolicy(where, "type is required (one of %s)", strings.Join(allTypes(), ", "))
	}
	if !knownType(pc.Type) {
		// Checked before the body, so a typo'd type reports the typo rather
		// than "no <typo> body is set".
		return nil, false, errPolicy(where, "unknown type %q (want one of %s)", pc.Type, strings.Join(allTypes(), ", "))
	}
	if err := checkBody(pc, where); err != nil {
		return nil, false, err
	}
	switch pc.Type {
	case TypeAlwaysSample:
		return alwaysPolicy{}, false, nil
	case TypeLatency:
		p, err := compileLatency(where, pc.Latency)
		return p, false, err
	case TypeStatusCode:
		p, err := compileStatusCode(where, pc.StatusCode)
		return p, false, err
	case TypeStringAttribute:
		p, err := compileStringAttribute(where, pc.StringAttribute)
		return p, false, err
	case TypeNumericAttribute:
		p, err := compileNumericAttribute(where, pc.NumericAttribute)
		return p, false, err
	case TypeBooleanAttribute:
		p, err := compileBooleanAttribute(where, pc.BooleanAttribute)
		return p, false, err
	case TypeProbabilistic:
		p, err := compileProbabilistic(where, pc.Probabilistic)
		return p, false, err
	case TypeRateLimiting:
		p, err := compileRateLimiting(where, pc.RateLimiting)
		return p, true, err
	case TypeAnd:
		if sub {
			return nil, false, errPolicy(where, "%s may not be nested inside and/composite (sub-policies must be leaf types)", TypeAnd)
		}
		return compileAnd(where, pc.And)
	case TypeComposite:
		if sub {
			return nil, false, errPolicy(where, "%s may not be nested inside and/composite (sub-policies must be leaf types)", TypeComposite)
		}
		p, err := compileComposite(pc.Name, where, pc.Composite)
		return p, true, err
	default:
		// Unreachable: knownType gated the switch. Kept so that adding a type
		// constant without a case here fails loudly rather than silently.
		return nil, false, errPolicy(where, "type %q has no compiler", pc.Type)
	}
}

func allTypes() []string {
	return []string{
		TypeAlwaysSample, TypeLatency, TypeStatusCode, TypeStringAttribute,
		TypeNumericAttribute, TypeBooleanAttribute, TypeProbabilistic,
		TypeRateLimiting, TypeAnd, TypeComposite,
	}
}

func knownType(t string) bool {
	for _, k := range allTypes() {
		if k == t {
			return true
		}
	}
	return false
}

// checkBody requires the body matching pc.Type and refuses any other. A body
// belonging to a different type is never harmless config: it is a policy
// somebody edited halfway, and reading past it silently (as the Collector does)
// applies a rule nobody wrote.
func checkBody(pc PolicyConfig, where string) error {
	bodies := [...]struct {
		typ     string
		present bool
	}{
		{TypeLatency, pc.Latency != nil},
		{TypeStatusCode, pc.StatusCode != nil},
		{TypeStringAttribute, pc.StringAttribute != nil},
		{TypeNumericAttribute, pc.NumericAttribute != nil},
		{TypeBooleanAttribute, pc.BooleanAttribute != nil},
		{TypeProbabilistic, pc.Probabilistic != nil},
		{TypeRateLimiting, pc.RateLimiting != nil},
		{TypeAnd, pc.And != nil},
		{TypeComposite, pc.Composite != nil},
	}
	own := false
	for _, b := range bodies {
		if b.typ == pc.Type {
			own = b.present
			continue
		}
		if b.present {
			return errPolicy(where, "type is %q but a %q body is also set", pc.Type, b.typ)
		}
	}
	if pc.Type == TypeAlwaysSample {
		return nil // alwaysSample has no body to require
	}
	if !own {
		return errPolicy(where, "type is %q but no %q body is set", pc.Type, pc.Type)
	}
	return nil
}

// --- alwaysSample -----------------------------------------------------------

type alwaysPolicy struct{}

func (alwaysPolicy) eval(Trace, time.Time) (verdict, string) { return verdictSample, "" }

// --- latency ----------------------------------------------------------------

// latencyPolicy keeps traces whose duration falls in [min,max].
type latencyPolicy struct{ min, max time.Duration }

// compileLatency parses both bounds. This is the ONLY place the duration
// strings are read, so Validate (which compiles and discards) and New cannot
// disagree about what "500ms" means or about which spellings are rejected.
func compileLatency(where string, cfg *LatencyConfig) (policy, error) {
	lower, err := cfg.threshold()
	if err != nil {
		return nil, errPolicy(where, "%w", err)
	}
	upper, err := cfg.upper()
	if err != nil {
		return nil, errPolicy(where, "%w", err)
	}
	if upper > 0 && upper < lower {
		return nil, errPolicy(where, "latency.upper %q is below threshold %q (the window is empty)", cfg.Upper, cfg.Threshold)
	}
	p := &latencyPolicy{min: lower, max: math.MaxInt64}
	if upper > 0 {
		p.max = upper
	}
	return p, nil
}

// eval measures earliest start to latest end over the spans PRESENT — a lower
// bound on the real duration whenever assembly is incomplete (see the package
// doc). A trace with no usable timestamp abstains rather than counting as
// zero-length: "we cannot tell" is not "it was fast".
func (p *latencyPolicy) eval(t Trace, _ time.Time) (verdict, string) {
	d, ok := traceDuration(t)
	if !ok {
		return verdictAbstain, ""
	}
	if d >= p.min && d <= p.max {
		return verdictSample, ""
	}
	return verdictAbstain, ""
}

// --- statusCode -------------------------------------------------------------

// statusPolicy keeps traces where any span carries one of the wanted codes,
// held as a bitmask over the three-valued ptrace.StatusCode so the check is one
// shift and one AND per span.
type statusPolicy struct{ mask uint8 }

func compileStatusCode(where string, cfg *StatusCodeConfig) (policy, error) {
	if len(cfg.StatusCodes) == 0 {
		return nil, errPolicy(where, "statusCode.statusCodes is empty")
	}
	var mask uint8
	for _, s := range cfg.StatusCodes {
		code, err := parseStatusCode(s)
		if err != nil {
			return nil, errPolicy(where, "statusCode.statusCodes: %w", err)
		}
		mask |= 1 << uint(code)
	}
	return &statusPolicy{mask: mask}, nil
}

func parseStatusCode(s string) (ptrace.StatusCode, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "UNSET":
		return ptrace.StatusCodeUnset, nil
	case "OK":
		return ptrace.StatusCodeOk, nil
	case "ERROR":
		return ptrace.StatusCodeError, nil
	}
	return 0, fmt.Errorf("unknown status code %q (want ERROR, OK or UNSET)", s)
}

func (p *statusPolicy) eval(t Trace, _ time.Time) (verdict, string) {
	for i := range t.Spans {
		if p.mask&(1<<uint(t.Spans[i].Span.Status().Code())) != 0 {
			return verdictSample, ""
		}
	}
	return verdictAbstain, ""
}

// --- stringAttribute --------------------------------------------------------

// stringAttrPolicy matches a string attribute on any span (span attributes
// first, resource attributes second — see StringAttributeConfig.Key).
type stringAttrPolicy struct {
	key string
	// exact is the non-regex value set. A map rather than a slice scan because
	// the set is operator-sized but the LOOKUP is per span per trace, and a map
	// probe with a string key allocates nothing.
	exact map[string]struct{}
	res   []*regexp.Regexp
	cache *regexCache
	// invert turns a match into a veto. Note the asymmetry with the Collector,
	// explained in the package doc: no match ABSTAINS here rather than sampling
	// everything the rule does not name.
	invert bool
}

func compileStringAttribute(where string, cfg *StringAttributeConfig) (policy, error) {
	if cfg.Key == "" {
		return nil, errPolicy(where, "stringAttribute.key is required")
	}
	if len(cfg.Values) == 0 {
		return nil, errPolicy(where, "stringAttribute.values is empty")
	}
	if cfg.CacheMaxSize < 0 {
		return nil, errPolicy(where, "stringAttribute.cacheMaxSize %d is negative", cfg.CacheMaxSize)
	}
	if !cfg.EnabledRegexMatching && cfg.CacheMaxSize > 0 {
		return nil, errPolicy(where, "stringAttribute.cacheMaxSize is set without enabledRegexMatching (exact matching has nothing to cache)")
	}
	p := &stringAttrPolicy{key: cfg.Key, invert: cfg.InvertMatch}
	if cfg.EnabledRegexMatching {
		p.res = make([]*regexp.Regexp, 0, len(cfg.Values))
		for i, v := range cfg.Values {
			re, err := regexp.Compile(v)
			if err != nil {
				return nil, errPolicy(where, "stringAttribute.values[%d] %q: %w", i, v, err)
			}
			p.res = append(p.res, re)
		}
		size := cfg.CacheMaxSize
		if size == 0 {
			size = defaultRegexCacheSize
		}
		p.cache = newRegexCache(size)
		return p, nil
	}
	p.exact = make(map[string]struct{}, len(cfg.Values))
	for _, v := range cfg.Values {
		p.exact[v] = struct{}{}
	}
	return p, nil
}

func (p *stringAttrPolicy) eval(t Trace, _ time.Time) (verdict, string) {
	for i := range t.Spans {
		v, ok := lookup(t.Spans[i], p.key)
		if !ok || v.Type() != pcommon.ValueTypeStr {
			// A non-string value is not a match, not an error: see
			// StringAttributeConfig.Values for why no coercion happens.
			continue
		}
		if p.match(v.Str()) {
			if p.invert {
				return verdictVeto, ""
			}
			return verdictSample, ""
		}
	}
	return verdictAbstain, ""
}

func (p *stringAttrPolicy) match(s string) bool {
	if p.exact != nil {
		_, ok := p.exact[s]
		return ok
	}
	if v, ok := p.cache.get(s); ok {
		return v
	}
	m := false
	for _, re := range p.res {
		if re.MatchString(s) {
			m = true
			break
		}
	}
	p.cache.put(s, m)
	return m
}

// regexCache memoizes regex results by attribute value.
//
// Bounded by a generational CLEAR rather than an LRU: an LRU costs a list node
// and two pointer writes per hit to maintain recency, all under the same lock,
// to buy a marginally better hit rate on a workload (a handful of hot route
// names) where any policy is warm after a few hundred traces. A full clear at
// the cap costs one cold period per cap-worth of distinct values and bounds the
// map absolutely, which is the property that matters when the keys come off the
// wire.
type regexCache struct {
	mu  sync.Mutex
	max int
	m   map[string]bool
}

func newRegexCache(max int) *regexCache {
	return &regexCache{max: max, m: make(map[string]bool, min(max, 64))}
}

func (c *regexCache) get(s string) (bool, bool) {
	c.mu.Lock()
	v, ok := c.m[s]
	c.mu.Unlock()
	return v, ok
}

func (c *regexCache) put(s string, v bool) {
	if len(s) > maxRegexCacheKeyBytes {
		return
	}
	c.mu.Lock()
	if len(c.m) >= c.max {
		clear(c.m) // keeps the allocated buckets, drops the entries
	}
	c.m[s] = v
	c.mu.Unlock()
}

// --- numericAttribute -------------------------------------------------------

// numericPolicy matches an int or double attribute within inclusive bounds. An
// unset bound is the type's extreme, so the comparison is branchless.
type numericPolicy struct {
	key      string
	min, max int64
}

func compileNumericAttribute(where string, cfg *NumericAttributeConfig) (policy, error) {
	if cfg.Key == "" {
		return nil, errPolicy(where, "numericAttribute.key is required")
	}
	if cfg.MinValue == nil && cfg.MaxValue == nil {
		return nil, errPolicy(where, "numericAttribute needs minValue and/or maxValue")
	}
	p := &numericPolicy{key: cfg.Key, min: math.MinInt64, max: math.MaxInt64}
	if cfg.MinValue != nil {
		p.min = *cfg.MinValue
	}
	if cfg.MaxValue != nil {
		p.max = *cfg.MaxValue
	}
	if p.min > p.max {
		return nil, errPolicy(where, "numericAttribute.minValue %d is above maxValue %d (the range is empty)", p.min, p.max)
	}
	return p, nil
}

// eval accepts Int and Double values. The Collector reads only Int; a double is
// accepted here because an SDK recording a duration or a size as a float would
// otherwise never match a policy that looks correct, and the widening
// comparison is exact for every magnitude either side can express to within the
// float's own precision.
func (p *numericPolicy) eval(t Trace, _ time.Time) (verdict, string) {
	for i := range t.Spans {
		v, ok := lookup(t.Spans[i], p.key)
		if !ok {
			continue
		}
		switch v.Type() {
		case pcommon.ValueTypeInt:
			if n := v.Int(); n >= p.min && n <= p.max {
				return verdictSample, ""
			}
		case pcommon.ValueTypeDouble:
			if d := v.Double(); d >= float64(p.min) && d <= float64(p.max) {
				return verdictSample, ""
			}
		}
	}
	return verdictAbstain, ""
}

// --- booleanAttribute -------------------------------------------------------

type boolPolicy struct {
	key  string
	want bool
}

func compileBooleanAttribute(where string, cfg *BooleanAttributeConfig) (policy, error) {
	if cfg.Key == "" {
		return nil, errPolicy(where, "booleanAttribute.key is required")
	}
	if cfg.Value == nil {
		return nil, errPolicy(where, "booleanAttribute.value is required (omitting it would silently mean false)")
	}
	return &boolPolicy{key: cfg.Key, want: *cfg.Value}, nil
}

func (p *boolPolicy) eval(t Trace, _ time.Time) (verdict, string) {
	for i := range t.Spans {
		v, ok := lookup(t.Spans[i], p.key)
		if ok && v.Type() == pcommon.ValueTypeBool && v.Bool() == p.want {
			return verdictSample, ""
		}
	}
	return verdictAbstain, ""
}

// --- probabilistic ----------------------------------------------------------

// probPolicy keeps a fraction of traces, chosen by hashing the trace id.
type probPolicy struct{ threshold uint64 }

func compileProbabilistic(where string, cfg *ProbabilisticConfig) (policy, error) {
	pct := cfg.SamplingPercentage
	if pct < 0 || pct > 100 {
		return nil, errPolicy(where, "probabilistic.samplingPercentage %v is not in [0,100] (this is a percentage: half is 50, not 0.5)", pct)
	}
	// The FRACTION is passed (pct/100, divided here) so tracehash computes the
	// threshold's float rounding identically for this policy and for
	// tracesample's probability — that bit-identity is half the nesting
	// contract (tracehash's package doc carries the whole of it).
	return &probPolicy{threshold: tracehash.Threshold(pct / 100)}, nil
}

// eval hashes the trace id against the threshold — tracehash.Keep, SHARED with
// agent/tracesample, and sharing it is the point rather than a coincidence:
// the same trace decides identically wherever and whenever it is judged (so a
// re-decision after a retry, a restart or late spans reaches the same answer),
// and the unsalted hash makes this stage NEST with the head sampler rather
// than compound with it. See tracehash's package doc for the contract.
func (p *probPolicy) eval(t Trace, _ time.Time) (verdict, string) {
	if tracehash.Keep(t.TraceID, p.threshold) {
		return verdictSample, ""
	}
	return verdictAbstain, ""
}

// --- rateLimiting -----------------------------------------------------------

type ratePolicy struct{ b *tracehash.Bucket }

func compileRateLimiting(where string, cfg *RateLimitingConfig) (policy, error) {
	if cfg.SpansPerSecond <= 0 {
		return nil, errPolicy(where, "rateLimiting.spansPerSecond must be > 0 (got %v, which would sample nothing)", cfg.SpansPerSecond)
	}
	return &ratePolicy{b: tracehash.NewBucket(cfg.SpansPerSecond)}, nil
}

// eval charges the whole trace, all or nothing: admitting the prefix of a trace
// that happens to fit the remaining budget produces a trace that is worse than
// either outcome.
//
// The charge happens only when this policy is REACHED, so an earlier policy
// that samples spends nothing here — which is what makes "keep all errors, then
// rate-limit the rest" mean what it reads like.
func (p *ratePolicy) eval(t Trace, now time.Time) (verdict, string) {
	if charge(p.b, float64(len(t.Spans)), now, t.Charged) {
		return verdictSample, ""
	}
	return verdictAbstain, ""
}

// charge spends n against b — or, when the trace was already charged by an
// earlier decision (Trace.Charged), merely checks that it would fit. A
// re-decision must not bill the same trace twice: the budget is a rate of spans
// leaving, and these spans were counted the first time.
//
// The bucket itself is tracehash.Bucket, shared with agent/tracesample's rate
// cap; THIS package's semantics are AdmitDebt (admission needs min(n, burst),
// the charge is the full n, so an over-sized trace goes into debt rather than
// being shut out forever) and Peek (the state-untouched re-decision) — the
// method docs carry the why.
func charge(b *tracehash.Bucket, n float64, now time.Time, already bool) bool {
	if already {
		return b.Peek(n, now)
	}
	return b.AdmitDebt(n, now)
}

// --- and --------------------------------------------------------------------

// andPolicy samples only when every sub-policy does.
type andPolicy struct{ subs []policy }

func compileAnd(where string, cfg *AndConfig) (policy, bool, error) {
	if len(cfg.SubPolicies) == 0 {
		return nil, false, errPolicy(where, "and.andSubPolicy is empty")
	}
	subs, needClock, err := compilePolicies(cfg.SubPolicies, true)
	if err != nil {
		return nil, false, err
	}
	ps := make([]policy, len(subs))
	for i := range subs {
		ps[i] = subs[i].p
	}
	return &andPolicy{subs: ps}, needClock, nil
}

// eval requires every sub-policy to sample. A veto from any sub-policy
// propagates as the AND's verdict rather than merely failing the conjunction:
// an exclusion nested in an AND is still an exclusion, and letting a later
// policy in the outer list resurrect the trace would make `and` a place where
// vetoes go to die.
//
// Sub-policies are evaluated in order and the evaluation stops at the first one
// that does not sample, so an operator can put the cheap discriminating test
// first — and so a rateLimiting sub-policy placed last is charged only for
// traces the rest of the conjunction already accepted.
func (p *andPolicy) eval(t Trace, now time.Time) (verdict, string) {
	for _, sub := range p.subs {
		switch v, _ := sub.eval(t, now); v {
		case verdictVeto:
			return verdictVeto, ""
		case verdictAbstain:
			return verdictAbstain, ""
		}
	}
	return verdictSample, ""
}

// --- composite --------------------------------------------------------------

// compositePolicy runs ordered sub-policies, each against its own slice of a
// shared spans/second budget: the first sub-policy that both MATCHES and has
// budget left decides. It is how an operator says "spend the first 50% of the
// budget on errors, the next 25% on slow traces, whatever is left on the rest"
// without those three rules competing for one bucket.
type compositePolicy struct{ subs []compositeSub }

type compositeSub struct {
	// name is the qualified "<composite>/<sub>" string, built at compile time:
	// naming the author of a decision must not concatenate on the hot path.
	name string
	p    policy
	b    *tracehash.Bucket
}

func compileComposite(name, where string, cfg *CompositeConfig) (policy, error) {
	if cfg.MaxTotalSpansPerSecond <= 0 {
		return nil, errPolicy(where, "composite.maxTotalSpansPerSecond must be > 0 (got %v, which allocates nothing to any sub-policy)", cfg.MaxTotalSpansPerSecond)
	}
	if len(cfg.SubPolicies) == 0 {
		return nil, errPolicy(where, "composite.compositeSubPolicy is empty")
	}
	subs, _, err := compilePolicies(cfg.SubPolicies, true)
	if err != nil {
		return nil, err
	}
	order, err := compositeOrder(where, cfg, subs)
	if err != nil {
		return nil, err
	}
	shares, err := compositeShares(where, cfg, subs)
	if err != nil {
		return nil, err
	}

	out := &compositePolicy{subs: make([]compositeSub, 0, len(subs))}
	for _, idx := range order {
		s := subs[idx]
		rate := cfg.MaxTotalSpansPerSecond * shares[s.name] / 100
		out.subs = append(out.subs, compositeSub{
			name: name + "/" + s.name,
			p:    s.p,
			b:    tracehash.NewBucket(rate),
		})
	}
	return out, nil
}

// compositeOrder resolves policyOrder to indices into subs. An explicit order
// must be a PERMUTATION of the sub-policy names: a partial list would leave
// "and the rest, in some order" to be invented here, and a name that matches
// nothing is a typo that would otherwise cost the operator a policy silently.
func compositeOrder(where string, cfg *CompositeConfig, subs []namedPolicy) ([]int, error) {
	if len(cfg.PolicyOrder) == 0 {
		order := make([]int, len(subs))
		for i := range order {
			order[i] = i
		}
		return order, nil
	}
	byName := make(map[string]int, len(subs))
	for i, s := range subs {
		byName[s.name] = i
	}
	order := make([]int, 0, len(cfg.PolicyOrder))
	used := make(map[string]struct{}, len(cfg.PolicyOrder))
	for _, n := range cfg.PolicyOrder {
		i, ok := byName[n]
		if !ok {
			return nil, errPolicy(where, "composite.policyOrder names %q, which is not a compositeSubPolicy", n)
		}
		if _, dup := used[n]; dup {
			return nil, errPolicy(where, "composite.policyOrder names %q twice", n)
		}
		used[n] = struct{}{}
		order = append(order, i)
	}
	if len(order) != len(subs) {
		var missing []string
		for _, s := range subs {
			if _, ok := used[s.name]; !ok {
				missing = append(missing, s.name)
			}
		}
		sort.Strings(missing)
		return nil, errPolicy(where, "composite.policyOrder omits %s (it must list every compositeSubPolicy)", strings.Join(missing, ", "))
	}
	return order, nil
}

// compositeShares resolves rateAllocation to a percentage per sub-policy.
// Sub-policies with no entry split the leftover evenly (which is what makes the
// Collector's own partial-allocation examples work); a leftover of zero with
// unallocated policies left is an error, because the alternative is a
// sub-policy that is configured, evaluated and can never sample.
func compositeShares(where string, cfg *CompositeConfig, subs []namedPolicy) (map[string]float64, error) {
	shares := make(map[string]float64, len(subs))
	known := make(map[string]struct{}, len(subs))
	for _, s := range subs {
		known[s.name] = struct{}{}
	}
	total := 0.0
	for _, ra := range cfg.RateAllocation {
		if _, ok := known[ra.Policy]; !ok {
			return nil, errPolicy(where, "composite.rateAllocation names %q, which is not a compositeSubPolicy", ra.Policy)
		}
		if _, dup := shares[ra.Policy]; dup {
			return nil, errPolicy(where, "composite.rateAllocation names %q twice", ra.Policy)
		}
		if ra.Percent <= 0 || ra.Percent > 100 {
			return nil, errPolicy(where, "composite.rateAllocation[%q].percent %v is not in (0,100]", ra.Policy, ra.Percent)
		}
		shares[ra.Policy] = ra.Percent
		total += ra.Percent
	}
	// A tiny epsilon over 100 is float summation noise, not an over-allocation:
	// three shares an operator wrote to total 100 (e.g. 33.33+33.33+33.34) sum
	// to 100.00000000000001 in float64, and refusing that at startup is a
	// confusing rejection of an arithmetically-correct config.
	if total > 100+1e-9 {
		return nil, errPolicy(where, "composite.rateAllocation sums to %v%% (more than the whole budget)", total)
	}
	rest := len(subs) - len(shares)
	if rest == 0 {
		return shares, nil
	}
	leftover := 100 - total
	if leftover <= 0 {
		return nil, errPolicy(where, "composite.rateAllocation leaves 0%% for %d unallocated sub-policies (they could never sample)", rest)
	}
	each := leftover / float64(rest)
	for _, s := range subs {
		if _, ok := shares[s.name]; !ok {
			shares[s.name] = each
		}
	}
	return shares, nil
}

// eval walks the sub-policies in order; the first that matches AND fits its own
// budget decides, and is named in the Decision. A sub-policy that matches but is
// over budget does NOT end the evaluation: the next sub-policy gets its chance,
// which is what turns the allocation into "this class of trace gets at most this
// much" instead of "this class of trace blocks the ones below it".
//
// A veto propagates immediately, for the reason andPolicy.eval gives.
func (p *compositePolicy) eval(t Trace, now time.Time) (verdict, string) {
	n := float64(len(t.Spans))
	for i := range p.subs {
		sub := &p.subs[i]
		v, _ := sub.p.eval(t, now)
		switch v {
		case verdictVeto:
			return verdictVeto, sub.name
		case verdictSample:
			if charge(sub.b, n, now, t.Charged) {
				return verdictSample, sub.name
			}
		}
	}
	return verdictAbstain, ""
}

// subNames reports the qualified names a composite can put in a Decision. Its
// own configured name never appears in one — the sub-policy is what decided.
func (p *compositePolicy) subNames() []string {
	out := make([]string, 0, len(p.subs))
	for i := range p.subs {
		out = append(out, p.subs[i].name)
	}
	return out
}
