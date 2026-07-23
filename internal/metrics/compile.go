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

// maxAge parses and clamps the expiration duration.
func (d *Dynamic) maxAge() (time.Duration, error) {
	if d.MaxAge == "" {
		return defaultMaxAge, nil
	}
	age, err := time.ParseDuration(d.MaxAge)
	if err != nil {
		return 0, err
	}
	if age <= 0 {
		// A zero/negative expiration would mark every sample idle on every
		// export, silently turning counters into per-interval deltas.
		return 0, fmt.Errorf("maxAge must be positive: %s", d.MaxAge)
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

	name := cfg.namePrefix + d.Name
	if name == "" {
		return nil, fmt.Errorf("metric rule has no name")
	}
	if kind == kindHistogram {
		// Guard against the EFFECTIVE bucket count: an empty Buckets is replaced by
		// defaultBuckets in newSeries, so checking len(d.Buckets)+1 (==1 when empty)
		// would pass a maxCardinality below the real stream count and then admit
		// nothing at all (the all-or-nothing histogram pre-pass rejects every
		// observation) — silent total data loss instead of this compile-time error.
		streams := len(d.Buckets) + 1
		if len(d.Buckets) == 0 {
			streams = len(defaultBuckets) + 1
		}
		if cap := cardinalityCap(d.MaxCardinality); cap < streams {
			return nil, fmt.Errorf("metric %q: maxCardinality %d is below the histogram's %d bucket streams — nothing could ever be admitted", d.Name, cap, streams)
		}
	}
	if existing, ok := shared[name]; ok {
		if existing.kind != kind || existing.action != action {
			return nil, fmt.Errorf("metric %q declared with conflicting type/action", d.Name)
		}
		// A second rule's buckets/maxCardinality/maxAge would be silently
		// ignored (the first rule's series wins) — reject a conflicting
		// histogram bucket declaration like a conflicting type.
		if kind == kindHistogram && len(d.Buckets) > 0 && !slices.Equal(existing.buckets[:len(existing.buckets)-1], d.Buckets) {
			return nil, fmt.Errorf("metric %q declared with conflicting buckets", d.Name)
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
// the corresponding character of value, other characters are kept. So value
// "503" with pattern "_xx" yields "5xx".
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
		ki.Add(r.value)
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
