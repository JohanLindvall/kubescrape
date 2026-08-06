package metrics

// Compilation of Dynamic specs into metricRules: type/action/bucket
// validation, the label-template DSL parser, and the line-field key index.

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/JohanLindvall/kubescrape/internal/config"
	"github.com/JohanLindvall/kubescrape/internal/logline"
)

// kind resolves the metric type name to a seriesKind.
func (d *Dynamic) kind() (seriesKind, error) {
	switch strings.ToLower(d.Type) {
	case "", CounterType:
		return kindCounter, nil
	case GaugeType:
		return kindGauge, nil
	case HistogramType:
		return kindHistogram, nil
	case SummaryType:
		return kindSummary, nil
	default:
		return 0, fmt.Errorf("invalid metric type: %s", d.Type)
	}
}

// maxAge parses and clamps the expiration duration. A zero or negative
// expiration would mark every sample idle on every export, silently turning
// counters into per-interval deltas — so this is one of the fields where zero
// is NOT a legal value, which config.Positive says in the error itself.
func (d *Dynamic) maxAge() (time.Duration, error) {
	age, err := config.Duration("maxAge", d.MaxAge, defaultMaxAge,
		config.Positive("every sample would be idle on every export, turning counters into per-interval deltas"))
	if err != nil {
		return 0, err
	}
	return min(age, maxMaxAge), nil
}

// validateBuckets checks that histogram bounds increase and that non-histograms
// declare none.
func (d *Dynamic) validateBuckets(kind seriesKind) error {
	if kind != kindHistogram {
		if len(d.Buckets) > 0 {
			return errors.New("buckets can only be set for histogram metrics")
		}
		return nil
	}
	for i := 1; i < len(d.Buckets); i++ {
		if d.Buckets[i] <= d.Buckets[i-1] {
			return fmt.Errorf("buckets must be increasing: %v", d.Buckets)
		}
	}
	return nil
}

// compileRule builds one rule, reusing (or creating) the named series in shared.
func compileRule(d *Dynamic, cfg *setConfig, shared map[string]*series) (*metricRule, error) {
	kind, err := d.kind()
	if err != nil {
		return nil, err
	}
	action, err := d.gaugeAction(kind)
	if err != nil {
		return nil, err
	}
	if err := d.validateBuckets(kind); err != nil {
		return nil, err
	}
	age, err := d.maxAge()
	if err != nil {
		return nil, err
	}

	rule := &metricRule{value: d.Value}
	if d.ValueRegexp != "" {
		if d.Value != "" {
			return nil, errors.New("value and valueRegexp are mutually exclusive")
		}
		if rule.valueRe, err = regexp.Compile(d.ValueRegexp); err != nil {
			return nil, fmt.Errorf("invalid valueRegexp: %w", err)
		}
	}
	if rule.match, err = logline.ParseSelectors(d.Match, d.MatchRegexp); err != nil {
		return nil, err
	}
	if rule.labels, err = parseLabelTemplates(d.Labels, d.LabelPrefix); err != nil {
		return nil, err
	}
	if rule.resLabels, err = parseLabelTemplates(d.ResourceLabels, d.LabelPrefix); err != nil {
		return nil, err
	}
	if err := rejectUnresolvableKeys(d, rule); err != nil {
		return nil, err
	}

	// The check is on d.Name, not the prefixed result: a non-empty prefix made
	// the concatenation pass for a rule with no name at all, which compiled
	// into a metric literally named the prefix — and two such rules silently
	// shared one series.
	if d.Name == "" {
		return nil, fmt.Errorf("metric rule has no name")
	}
	name := cfg.namePrefix + d.Name
	if kind == kindHistogram {
		// maxCardinality counts LABEL COMBINATIONS, and a histogram's label set
		// costs one live sample per bucket — so the memory the cap is defending
		// is the PRODUCT. Refuse a combination that could outgrow the store's
		// stream budget rather than silently admitting fewer label sets than
		// configured (which is exactly the bug this unit fix removed). The
		// effective bucket count is what matters: an empty Buckets is replaced
		// by defaultBuckets in newSeries.
		streams := len(d.Buckets) + 1
		if len(d.Buckets) == 0 {
			streams = len(defaultBuckets) + 1
		}
		if cap := cardinalityCap(d.MaxCardinality); cap*streams > maxStreamCap {
			return nil, fmt.Errorf("metric %q: maxCardinality %d x %d buckets = %d live samples, above the %d-sample budget — lower maxCardinality or use fewer buckets",
				d.Name, cap, streams, cap*streams, maxStreamCap)
		}
	}
	if existing, ok := shared[name]; ok {
		if existing.kind != kind || existing.action != action {
			return nil, fmt.Errorf("metric %q declared with conflicting type/action", d.Name)
		}
		// Rules sharing a name share the FIRST one's series, so every shape
		// field a later rule declares is unenforceable — and silently so. A
		// second rule feeding an existing metric with a deliberately tight
		// maxCardinality (its label set is the riskier one) started cleanly and
		// admitted label sets up to the first rule's cap instead, which is the
		// memory blow-up the field was set to prevent. Refuse a DIFFERING
		// declaration; leaving a field unset still means "whatever the name
		// already has".
		if kind == kindHistogram && len(d.Buckets) > 0 && !slices.Equal(existing.buckets[:len(existing.buckets)-1], d.Buckets) {
			return nil, fmt.Errorf("metric %q declared with conflicting buckets", d.Name)
		}
		if cap := cardinalityCap(d.MaxCardinality); d.MaxCardinality != 0 && cap != existing.maxSize {
			return nil, fmt.Errorf("metric %q declared with conflicting maxCardinality (%d, already %d)", d.Name, cap, existing.maxSize)
		}
		if secs := expirationSeconds(age); d.MaxAge != "" && secs != existing.expiration {
			return nil, fmt.Errorf("metric %q declared with conflicting maxAge (%ds, already %ds)", d.Name, secs, existing.expiration)
		}
		if d.Description != "" && d.Description != existing.desc {
			return nil, fmt.Errorf("metric %q declared with conflicting description (%q, already %q)", d.Name, d.Description, existing.desc)
		}
		rule.series = existing
	} else {
		rule.series = newSeries(seriesSpec{
			name:       name,
			desc:       d.Description,
			kind:       kind,
			action:     action,
			maxSize:    cardinalityCap(d.MaxCardinality),
			expiration: age,
			buckets:    d.Buckets,
			log:        cfg.log,
			drops:      cfg.drops,
			now:        cfg.now,
		})
		shared[name] = rule.series
	}
	// A rule that must read a value but names no value source (no `value`, no
	// `valueRegexp`) resolves the empty key to nothing on every line and records
	// zero data — a silent misconfiguration. Reject it at compile time. Gauge
	// inc/dec/count tally lines and legitimately need no value.
	if rule.needsValue() && rule.value == "" && rule.valueRe == nil {
		return nil, fmt.Errorf("metric %q: type %q needs a value source — set `value` or `valueRegexp`", d.Name, d.Type)
	}
	return rule, nil
}

// rejectUnresolvableKeys refuses the two keys this engine's resolution tiers
// cannot answer, both of which compile cleanly and then do nothing forever.
//
//   - logline.SeverityKey is the RULES tier's synthetic key: only
//     logchain.Resolver.RuleFn has an arm for it, while the label and value
//     lookups a metric resolves through see record attributes, resource
//     attributes and line fields — none of which is the enriched severity. The
//     selector language, the config file and the documented example are shared
//     with logs.rules, which is exactly what makes writing it under a metric's
//     match: the natural mistake, and it yields a permanently absent metric.
//   - logline.LineKey as the observed VALUE: the label tier resolves it (through
//     logline.ResolveKey) but the value tier does not, and buildKeyIndex skips
//     it, so the rule records nothing. The "needs a value source" check below
//     passes because the string is non-empty, which is what let it through.
//
// Both refusals name the alternative: a level FIELD for the first, valueRegexp
// for the second.
func rejectUnresolvableKeys(d *Dynamic, rule *metricRule) error {
	reject := func(where string) error {
		return fmt.Errorf("metric %q: %s reads %s, which only logs.rules resolves — a log-metric sees record/resource attributes and line fields, so it would match nothing; select on the line's own level field instead",
			d.Name, where, logline.SeverityKey)
	}
	for _, key := range rule.match.LabelKeys() {
		if key == logline.SeverityKey {
			return reject("match")
		}
	}
	for _, lt := range rule.labels {
		if lt.getKey == logline.SeverityKey {
			return reject("label " + lt.setKey)
		}
	}
	for _, lt := range rule.resLabels {
		if lt.getKey == logline.SeverityKey {
			return reject("resourceLabel " + lt.setKey)
		}
	}
	if d.Value == logline.SeverityKey {
		return reject("value")
	}
	if d.Value == logline.LineKey {
		return fmt.Errorf("metric %q: value %s is the whole raw line, which the value tier does not resolve (and is never a number) — use valueRegexp to capture a number off the line",
			d.Name, logline.LineKey)
	}
	return nil
}

// cardinalityCap resolves the configured MaxCardinality: unset (or negative)
// means the documented default of maxCardinalityCap — never unlimited, since
// the cap is the defense against a high-cardinality label (request id, user
// id) exhausting the node agent's memory.
func cardinalityCap(configured int) int {
	if configured <= 0 {
		return maxCardinalityCap
	}
	return min(configured, maxCardinalityCap)
}

// parseLabelTemplates compiles a list of label specs.
func parseLabelTemplates(specs []string, setPrefix string) ([]labelTemplate, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	out := make([]labelTemplate, 0, len(specs))
	for _, spec := range specs {
		lt, err := parseLabelTemplate(spec, setPrefix)
		if err != nil {
			return nil, err
		}
		out = append(out, lt)
	}
	return out, nil
}

// labelTemplateRe captures the four label forms: an optional `set=`, a `$get`
// or literal value, and an optional `(pattern)` or `/regex/`.
var labelTemplateRe = regexp.MustCompile(`^((?P<set>[a-zA-Z_:][a-zA-Z0-9_:.]*)=)?(?P<get>\$?[a-zA-Z_:][a-zA-Z0-9_:.]*)(\((?P<pat>[^)]+)\)|(?P<re>/.+))?$`)

// parseLabelTemplate compiles one `set=$get` label spec. setPrefix, when set,
// is prepended to the label's name.
func parseLabelTemplate(spec, setPrefix string) (labelTemplate, error) {
	match := labelTemplateRe.FindStringSubmatch(spec)
	if match == nil {
		return labelTemplate{}, fmt.Errorf("invalid label: %s", spec)
	}
	var lt labelTemplate
	getKey, value, pattern, reSpec := "", "", "", ""
	for i, name := range labelTemplateRe.SubexpNames() {
		if match[i] == "" {
			continue
		}
		switch name {
		case "set":
			lt.setKey = match[i]
		case "get":
			if strings.HasPrefix(match[i], "$") {
				getKey = match[i][1:]
			} else {
				value = match[i]
			}
		case "pat":
			pattern = match[i]
		case "re":
			reSpec = match[i]
		}
	}
	// Bare `key`: sets and reads itself.
	if value != "" && getKey == "" && lt.setKey == "" {
		getKey, lt.setKey, value = value, value, ""
	}
	if setPrefix != "" && lt.setKey != "" {
		lt.setKey = setPrefix + lt.setKey
	}
	if lt.setKey == "" || (value == "" && getKey == "") {
		return labelTemplate{}, fmt.Errorf("invalid label: %s", spec)
	}

	get, err := labelGetter(getKey, value, pattern, reSpec, spec)
	if err != nil {
		return labelTemplate{}, err
	}
	lt.get = get
	lt.getKey = getKey
	return lt, nil
}

// labelGetter builds the value-producing closure for a label template from
// whichever of the four forms is present.
func labelGetter(getKey, value, pattern, reSpec, spec string) (func(func(string) string) string, error) {
	// A LITERAL value with a mask or a regex transform is a contradiction: the
	// transform has nothing to read, and the constant arm below would silently
	// win — `class=status(_xx)` compiled to the constant label class="status",
	// one merged series, wrong forever, with no error and no counter. The
	// legitimate spelling is a `$`-prefixed key (`class=$status(_xx)`), which is
	// also what the bare `status(_xx)` form promotes to before it gets here.
	if value != "" && (pattern != "" || reSpec != "") {
		return nil, fmt.Errorf("invalid label %q: the literal value %q cannot carry a mask or a regex transform — write $%s to read the field",
			spec, value, value)
	}
	switch {
	case value != "":
		return func(func(string) string) string { return value }, nil
	case pattern != "":
		return func(lookup func(string) string) string {
			return maskPattern(lookup(getKey), pattern)
		}, nil
	case reSpec != "":
		pat, repl, err := parseRegexpReplace(reSpec)
		if err != nil {
			return nil, fmt.Errorf("invalid label re %q: %w", spec, err)
		}
		re, err := regexp.Compile(pat)
		if err != nil {
			return nil, fmt.Errorf("invalid label re %q: %w", spec, err)
		}
		return func(lookup func(string) string) string {
			return re.ReplaceAllString(lookup(getKey), repl)
		}, nil
	default:
		return func(lookup func(string) string) string { return lookup(getKey) }, nil
	}
}

// maskPattern overlays value onto pattern: each '_' in pattern is replaced by
// the corresponding CHARACTER of value, other characters are kept. So value
// "503" with pattern "_xx" yields "5xx".
//
// The step is a rune, not a byte. Byte indexing split a multi-byte rune
// straddling a mask boundary — maskPattern("é50", "_xx") produced "\xc3xx" —
// and that value is hashed, retained and finally written to a pcommon.Map, i.e.
// an invalid-UTF-8 string field in the marshaled OTLP payload, which a strict
// protobuf receiver can reject PERMANENTLY: the whole chunk, every metric
// sharing that resource, dropped every interval the series stays live. A byte
// of value that is ALREADY invalid re-encodes as U+FFFD rather than being
// copied through, since the rune loop decodes it as RuneError.
//
// The rule this states is narrow, and deliberately so: the MASK must not
// MANUFACTURE invalid output from a valid line. It is not a payload-level
// guarantee, because this package does not have one — the plain passthrough
// label returns lookup(getKey) verbatim, which on the logfmt path is an unsafe
// view straight into the raw line, and truncLabelCut keeps invalid bytes on
// purpose when a value has no rune boundary to back off to (dropping the label
// would silently merge two series, which is worse). A validity check at
// labels.set, the one door every label value comes through, would close only
// this package's doors — a body, an enriched record attribute and a logattrs
// resource attribute reach the same payload without passing it — and it costs
// ~13ns per label per line (utf8.ValidString over a value already cut to
// maxLabelValueBytes; over the UNCUT value it would be the __line__-sized scan
// asciiOverlay exists to avoid). Inventing invalidity is what a transform can
// be held to; passing a line's own bytes through is not.
func maskPattern(value, pattern string) string {
	if pattern == "" {
		return ""
	}
	if value == "" {
		// A missing source field must not fabricate a label from the mask's
		// literal characters ("_xx" buckets for lines that lack the field);
		// an empty value drops the label, matching the plain passthrough.
		return ""
	}
	if asciiOverlay(value, pattern) {
		// Byte and rune step are the same thing here, and this is the log-line
		// hot path: a status code masked to its class is what the DSL is for.
		out := make([]byte, len(pattern))
		for i := 0; i < len(pattern); i++ {
			if pattern[i] == '_' && i < len(value) {
				out[i] = value[i]
			} else {
				out[i] = pattern[i]
			}
		}
		return string(out)
	}
	out := make([]byte, 0, len(pattern)+len(value))
	rest := value
	for _, pr := range pattern {
		// Every mask position consumes one rune of value, masked or not: the
		// two are overlaid position by position.
		vr, size := utf8.DecodeRuneInString(rest)
		rest = rest[size:]
		if pr == '_' && size > 0 {
			out = utf8.AppendRune(out, vr)
			continue
		}
		out = utf8.AppendRune(out, pr)
	}
	return string(out)
}

// asciiOverlay reports whether the byte-stepped overlay is also the rune-stepped
// one: pattern entirely single-byte, and value single-byte as far as the overlay
// READS it.
//
// The probe over value is bounded to len(pattern) bytes because that is every
// byte the loop can touch. An unbounded probe is work proportional to the whole
// VALUE on a per-line path where the value is routinely a whole log line — a
// label may read the synthetic __line__ key, and any JSON or logfmt field can be
// long — so `class=$__line__(_xx)` over a 1 MiB multiline entry cost ~300us of
// the single sweep goroutine that serves every log file on the node, against
// ~13ns for the overlay itself. A prefix cut inside a multi-byte rune ends on a
// byte >= RuneSelf, which routes to the rune loop, so the bound cannot admit a
// value the byte step would mis-slice.
func asciiOverlay(value, pattern string) bool {
	return isASCII(pattern) && isASCII(value[:min(len(value), len(pattern))])
}

// isASCII reports whether s is entirely single-byte, i.e. whether a byte index
// into it is also a rune index.
func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

// parseRegexpReplace splits a `/pattern/replacement/` spec, honouring
// backslash escapes.
func parseRegexpReplace(in string) (pattern, replacement string, err error) {
	if !strings.HasPrefix(in, "/") {
		return "", "", errors.New("must start with '/'")
	}
	var parts []string
	var buf strings.Builder
	var escaped bool
	for _, ch := range in[1:] {
		switch {
		case escaped:
			// Only the DSL's own delimiters are consumed by the escape: `\/`
			// is a literal slash, `\\` a literal backslash. Any OTHER escape
			// keeps its backslash so regex classes reach the compiler intact —
			// consuming it silently turned `error (\d+)` into `error (d+)`,
			// a pattern that never matches what the user wrote.
			if ch != '/' && ch != '\\' {
				buf.WriteRune('\\')
			}
			buf.WriteRune(ch)
			escaped = false
		case ch == '\\':
			escaped = true
		case ch == '/':
			parts = append(parts, buf.String())
			buf.Reset()
		default:
			buf.WriteRune(ch)
		}
	}
	if escaped {
		return "", "", errors.New("dangling backslash at end")
	}
	if buf.Len() > 0 || len(parts) < 2 {
		parts = append(parts, buf.String())
	}
	if len(parts) != 2 {
		return "", "", errors.New("must be in the form /pattern/replacement/")
	}
	return parts[0], parts[1], nil
}

// buildKeyIndex collects the distinct field keys referenced across rules:
// label getters, the observed value, and selector labels.
func buildKeyIndex(rules []*metricRule) logline.KeyIndex {
	ki := logline.NewKeyIndex()
	for _, r := range rules {
		// "1" is the constant-one VALUE literal (readValue short-circuits it),
		// not a field reference — the classification lives here, at the value
		// call site, so a selector or label legitimately reading a line field
		// named "1" still registers it.
		if r.value != "1" {
			ki.Add(r.value)
		}
		for _, lt := range r.labels {
			ki.Add(lt.getKey)
		}
		for _, lt := range r.resLabels {
			ki.Add(lt.getKey)
		}
		for _, key := range r.match.LabelKeys() {
			ki.Add(key)
		}
	}
	return ki
}
