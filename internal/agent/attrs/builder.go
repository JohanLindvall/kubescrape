package attrs

import (
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"regexp/syntax"
	"slices"
	"sort"
	"strings"
	"text/template"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// NodeInfo is the metadata of the node the agent runs on.
type NodeInfo struct {
	Name        string
	Labels      map[string]string
	Annotations map[string]string
}

// Context carries the metadata available when one resource is built. Fields
// are nil/empty when not applicable (e.g. no Container on a pod-level
// resource).
type Context struct {
	Node      *NodeInfo
	Pod       *kubemeta.Pod
	Container *kubemeta.Container
	Service   *kubemeta.Service
}

// Pipeline names accepted under Config.Pipelines. "self" is the agent's own
// generated metrics (self-metrics and span metrics), whose Pod context is the
// pod the agent itself runs in.
//
// "summary" governs the NODE-level resource of the kubelet's /stats/summary
// scrape and nothing else: the pod, container and volume resources that scrape
// builds are the CADVISOR pipeline's, because a summary series is only worth
// anything while it joins the cadvisor series for the same object, and two
// resources join only while they are byte-identical. A separate attribute
// surface over them would be a supported way to break that (the coupling
// internal/agent/cgroupstats documents for the same reason).
var pipelineNames = []string{"logs", "targets", "cadvisor", "node", "summary", "journal", "ingest", "self"}

// Config declares how resource attributes are built. It is the
// `resourceAttributes` section of the agent config:
//
//	defaults: true            # include the built-in k8s.* mapping
//	static:                   # fixed attributes on every resource
//	  cluster: prod-eu
//	attributes:               # Go templates evaluated against Context
//	  team: '{{ index .Pod.Labels "team" }}'
//	  service.name: '{{ coalesce (index .Pod.Labels "gp/service-name") (index .Pod.Labels "app.kubernetes.io/name") .Pod.Name }}'
//	  k8s.node.zone: '{{ with .Node }}{{ index .Labels "topology.kubernetes.io/zone" }}{{ end }}'
//	pipelines:                # per-pipeline overrides (logs|targets|cadvisor|node|summary|journal|ingest|self)
//	  node:
//	    attributes:
//	      service.name: aks-node
//
// Template attributes that evaluate to "" (or fail, e.g. a nil .Pod) are
// omitted. Available functions besides the text/template built-ins:
// env, coalesce (first non-empty), default, regexMatch, regexReplace.
type Config struct {
	// Defaults includes the built-in mapping (k8s.pod.name, owners, labels,
	// ...); absent means true.
	Defaults *bool `json:"defaults,omitempty"`
	// Static attributes are set verbatim on every resource.
	Static map[string]string `json:"static,omitempty"`
	// Attributes maps attribute keys to Go templates over Context.
	Attributes map[string]string `json:"attributes,omitempty"`
	// InstancePrefix is prepended to the derived service.instance.id (see
	// PrefixInstance). It defaults per pipeline (cadvisor -> "cadvisor") so
	// describing exporters do not collide with self-scraped metrics; set it to
	// "" to disable, or to any string to override.
	InstancePrefix *string `json:"instancePrefix,omitempty"`
	// Enable keeps only resource attributes whose key matches one of these
	// anchored regexes (empty keeps all); Disable drops matching keys. Both are
	// applied last, after defaults, static values and templates.
	//
	// Lists, not comma-separated strings: a pattern may therefore contain a
	// comma (`.{1,3}`), which the removed -resource-attrs-enable/-disable flags
	// could not express. Top-level only — a pipeline section setting them is
	// rejected, since the filter is shared by every pipeline.
	Enable  []string `json:"enable,omitempty"`
	Disable []string `json:"disable,omitempty"`
	// Pipelines overrides/extends the top-level settings for one pipeline;
	// static and attribute maps merge with the pipeline entry winning.
	Pipelines map[string]*Config `json:"pipelines,omitempty"`
}

// defaultInstancePrefix is the built-in service.instance.id prefix per pipeline
// (empty for pipelines whose resources are the exporter's own identity). It
// applies unless the config sets InstancePrefix explicitly.
//
// "summary" is here for the same collision the cadvisor entry prevents, one
// level up: the kubelet's /stats/summary node resource carries service.name
// "kubelet" and the node's name exactly as the /metrics scrape's does, so
// without a prefix the two would share (job, instance) while differing on
// url.full — a target_info flapping between two attribute sets every cycle, and
// two conflicting `up` values in one payload from exportHealth.
var defaultInstancePrefix = map[string]string{"cadvisor": "cadvisor", "summary": "summary"}

// validatePipelines rejects unknown pipeline names (strict YAML parsing cannot
// catch bad map keys) and nested pipeline sections. It runs in NewBuilders —
// the one production entry point: the unified agent config unmarshals the
// section itself, so this is what rejects a typo'd pipeline instead of
// silently ignoring it. (The standalone LoadConfig lives on only in
// builder_test.go.)
func (c *Config) validatePipelines() error {
	if c == nil {
		return nil
	}
	for name, sub := range c.Pipelines {
		if !slices.Contains(pipelineNames, name) {
			return fmt.Errorf("unknown pipeline %q (want one of %s)", name, strings.Join(pipelineNames, ", "))
		}
		if sub != nil && len(sub.Pipelines) > 0 {
			return fmt.Errorf("pipeline %q must not nest pipelines", name)
		}
		// The attribute filter is shared by every pipeline, so honouring these
		// per pipeline is not possible; rejecting beats silently ignoring them.
		if sub != nil && (len(sub.Enable) > 0 || len(sub.Disable) > 0) {
			return fmt.Errorf("pipeline %q must not set enable/disable: the attribute filter is global — set them at the top level of resourceAttributes", name)
		}
	}
	return nil
}

// Builders holds one compiled Builder per pipeline. A nil *Builders (or nil
// field) means built-in defaults.
type Builders struct {
	Logs     *Builder
	Targets  *Builder
	Cadvisor *Builder
	Node     *Builder
	Summary  *Builder
	Journal  *Builder
	Ingest   *Builder
	Self     *Builder
}

// NewBuilders compiles one builder per pipeline from cfg (nil = defaults
// everywhere) and one shared filter. Each pipeline merges the base config with
// its own section and picks up its default instance prefix.
func NewBuilders(cfg *Config, filter *Filter) (*Builders, error) {
	if err := cfg.validatePipelines(); err != nil {
		return nil, err
	}
	b := &Builders{}
	assign := map[string]**Builder{
		"logs": &b.Logs, "targets": &b.Targets, "cadvisor": &b.Cadvisor,
		"node": &b.Node, "summary": &b.Summary, "journal": &b.Journal,
		"ingest": &b.Ingest, "self": &b.Self,
	}
	for _, name := range pipelineNames {
		var sub *Config
		if cfg != nil {
			sub = cfg.Pipelines[name]
		}
		merged := mergeConfig(cfg, sub)
		// Instance-prefix precedence: an explicit pipeline section wins;
		// otherwise the built-in pipeline default (e.g. cadvisor) beats the
		// top-level base, so a global prefix can't silently strip cadvisor's
		// collision protection. mergeConfig already left the base value in place.
		//
		// KSM split resources LAYER rather than choose: the splitter applies its
		// rule/default prefix (promscrape SplitRule.InstancePrefix) after Build
		// has already applied the prefix resolved here, so a top-level
		// instancePrefix yields "<ruleprefix>-<baseprefix>-<instance>" on split
		// resources. The instances stay distinct (no collision) — just doubly
		// prefixed.
		if sub == nil || sub.InstancePrefix == nil {
			if p, ok := defaultInstancePrefix[name]; ok {
				merged.InstancePrefix = &p
			}
		}
		pb, err := NewBuilder(merged, filter)
		if err != nil {
			return nil, fmt.Errorf("pipeline %q: %w", name, err)
		}
		*assign[name] = pb
	}
	return b, nil
}

// mergeConfig overlays a pipeline config onto the base (nil base allowed); maps
// merge with the pipeline winning.
func mergeConfig(base, over *Config) *Config {
	out := &Config{}
	if base != nil {
		out.Defaults = base.Defaults
		out.InstancePrefix = base.InstancePrefix
	}
	if over != nil {
		if over.Defaults != nil {
			out.Defaults = over.Defaults
		}
		if over.InstancePrefix != nil {
			out.InstancePrefix = over.InstancePrefix
		}
	}
	out.Static = mergeMaps(overMap(base, func(c *Config) map[string]string { return c.Static }),
		overMap(over, func(c *Config) map[string]string { return c.Static }))
	out.Attributes = mergeMaps(overMap(base, func(c *Config) map[string]string { return c.Attributes }),
		overMap(over, func(c *Config) map[string]string { return c.Attributes }))
	return out
}

func overMap(c *Config, get func(*Config) map[string]string) map[string]string {
	if c == nil {
		return nil
	}
	return get(c)
}

func mergeMaps(base, over map[string]string) map[string]string {
	if len(base) == 0 && len(over) == 0 {
		return nil
	}
	out := make(map[string]string, len(base)+len(over))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range over {
		out[k] = v
	}
	return out
}

// Builder produces the resource attributes for every exported resource:
// the built-in mapping (unless disabled), static attributes, template
// attributes, and finally the enable/disable filter.
type Builder struct {
	defaults       bool
	static         map[string]string
	dynamic        []dynamicAttr
	instancePrefix string
	filter         *Filter
}

type dynamicAttr struct {
	key  string
	tmpl *template.Template
}

// regexCache backs the template regex functions.
//
// The patterns USUALLY come from a fixed config, which is what would make an
// unbounded cache reasonable — but regexMatch/regexReplace take the pattern as
// a template ARGUMENT, so a template may compose one from per-object data
// (`regexMatch .Pod.Name ...`, a label value, an annotation). Every distinct
// value then pins a compiled *regexp.Regexp for the agent's life, on a path
// that runs per resource built. The cap bounds that — by EVICTING, never by
// refusing to cache: a working set larger than the cap has to keep its hot
// patterns or every call past the cap is a fresh compile, which is three orders
// of magnitude dearer than the lookup and lands on precisely the data-derived
// input this bound exists for (see genCache).
const maxRegexKeys = 1024

var regexCache = newGenCache[any](maxRegexKeys) // pattern -> *regexp.Regexp | error

func cachedRegexp(pattern string) (*regexp.Regexp, error) {
	if v, ok := regexCache.load(pattern); ok {
		if re, ok := v.(*regexp.Regexp); ok {
			return re, nil
		}
		return nil, v.(error)
	}
	re, err := regexp.Compile(pattern)
	regexCache.store(pattern, cacheValue(re, err))
	return re, err
}

// cacheValue is what gets stored: the compiled regex, or the compile error.
func cacheValue(re *regexp.Regexp, err error) any {
	if err != nil {
		return err
	}
	return re
}

// templateFuncs are available in attribute templates.
var templateFuncs = template.FuncMap{
	"env": os.Getenv,
	"coalesce": func(vals ...string) string {
		for _, v := range vals {
			if v != "" {
				return v
			}
		}
		return ""
	},
	"default": func(def, s string) string {
		if s == "" {
			return def
		}
		return s
	},
	"regexMatch": func(pattern, s string) (bool, error) {
		re, err := cachedRegexp(pattern)
		if err != nil {
			return false, err
		}
		return re.MatchString(s), nil
	},
	"regexReplace": func(pattern, replacement, s string) (string, error) {
		re, err := cachedRegexp(pattern)
		if err != nil {
			return "", err
		}
		return re.ReplaceAllString(s, replacement), nil
	},
}

// NewBuilder compiles a Builder; a nil cfg means defaults only. Pipeline
// sections of cfg are ignored here (see NewBuilders). Templates are both
// parsed AND dry-run (validateTemplate): the execution errors that are config
// errors — a typo'd field, a bad literal regex — surface here, where
// -check-config sees them, not as a silent per-resource skip at Build time.
func NewBuilder(cfg *Config, filter *Filter) (*Builder, error) {
	b := &Builder{defaults: true, filter: filter}
	if cfg == nil {
		return b, nil
	}
	if cfg.Defaults != nil {
		b.defaults = *cfg.Defaults
	}
	if cfg.InstancePrefix != nil {
		b.instancePrefix = *cfg.InstancePrefix
	}
	b.static = cfg.Static

	keys := make([]string, 0, len(cfg.Attributes))
	for key := range cfg.Attributes {
		keys = append(keys, key)
	}
	sort.Strings(keys) // deterministic evaluation order
	for _, key := range keys {
		tmpl, err := template.New(key).Funcs(templateFuncs).Option("missingkey=zero").Parse(cfg.Attributes[key])
		if err != nil {
			return nil, fmt.Errorf("attribute %q: %w", key, err)
		}
		if err := validateTemplate(tmpl); err != nil {
			return nil, fmt.Errorf("attribute %q: %w", key, err)
		}
		b.dynamic = append(b.dynamic, dynamicAttr{key: key, tmpl: tmpl})
	}
	return b, nil
}

// validationContext is the synthetic Context every template is dry-run against
// at construction: every pointer populated with a zero-value struct (including
// the Pod's NamespaceMetadata), so any field a template names is resolved
// against the real types. The VALUES are deliberately left zero — a non-empty
// dummy would leak into regex patterns composed from context data
// (`regexMatch (index .Pod.Labels "pat") ...` renders the pattern "", which
// always compiles) and could manufacture errors no production value produces.
func validationContext() Context {
	return Context{
		Node:      &NodeInfo{},
		Pod:       &kubemeta.Pod{NamespaceMetadata: &kubemeta.ObjectMeta{}},
		Container: &kubemeta.Container{},
		Service:   &kubemeta.Service{},
	}
}

// validateTemplate dry-runs tmpl against validationContext and returns the
// execution error when it is a config error — one no production Context could
// avoid. Build's skip-on-error stays the not-applicable escape (a nil .Pod on
// a node-level resource); this is what keeps that skip from also swallowing
// typos forever.
func validateTemplate(tmpl *template.Template) error {
	err := tmpl.Execute(io.Discard, validationContext())
	if err == nil || !templateConfigError(err) {
		return nil
	}
	return err
}

// templateConfigError reports whether a dry-run execution error is
// value-independent — broken against every Context, not just the synthetic
// one. Two classes provably are:
//
//   - a field miss ("can't evaluate field X in type Y"): Context's types are
//     static and carry no interface fields, so no production value makes the
//     field appear. text/template has no typed error for it; the message is
//     exec.errorf's, stable since Go 1, and pinned by TestTemplateValidation.
//   - a regex that does not compile, from regexMatch/regexReplace: template
//     function errors are wrapped (%w), so errors.As reaches the
//     *syntax.Error. Only a literal (or env-derived — equally config-owned)
//     pattern can fail here; a data-derived one renders "" against the
//     zero-value context, and "" compiles.
//
// Everything else stays a runtime skip, because value-dependent errors are
// legitimate against a zero-value context: {{ (index .Pod.Owners 0).Name }}
// fails on the synthetic nil slice yet works on every owned pod, and
// {{ slice .Pod.Name 0 5 }} fails on the empty name. Refusing any error would
// reject configs that work in production.
func templateConfigError(err error) bool {
	var syn *syntax.Error
	if errors.As(err, &syn) {
		return true
	}
	return strings.Contains(err.Error(), "can't evaluate field")
}

// Build fills res from ctx. Safe on a nil receiver (defaults only, no
// filter).
func (b *Builder) Build(res pcommon.Resource, ctx Context) {
	// Size the attribute slice once instead of growing it through five
	// reallocations: a realistic pod (2 owners, 7 labels, a container) puts ~22
	// attributes, and the growth alone was half the bytes this call allocates.
	// EnsureCapacity is a no-op when the capacity is already there, so a
	// pre-populated resource (the KSM splitter's) keeps what it has.
	res.Attributes().EnsureCapacity(res.Attributes().Len() + b.attrCount(ctx))
	if b == nil || b.defaults {
		if ctx.Node != nil && ctx.Node.Name != "" {
			res.Attributes().PutStr("k8s.node.name", ctx.Node.Name)
		}
		if ctx.Pod != nil {
			Pod(res, *ctx.Pod)
		}
		if ctx.Container != nil {
			Container(res, *ctx.Container)
		}
		if ctx.Service != nil {
			Service(res, ctx.Service)
		}
	}
	if b != nil {
		for key, value := range b.static {
			res.Attributes().PutStr(key, value)
		}
		var sb strings.Builder
		for _, d := range b.dynamic {
			sb.Reset()
			// Execution errors (e.g. a nil .Pod on a node-level resource) and
			// empty results mean "attribute not applicable here". The silent
			// skip is safe because construction dry-ran every template against
			// a fully-populated Context (validateTemplate): the
			// value-independent config errors — a typo'd field, a bad literal
			// regex — were refused at startup, so only not-applicable contexts
			// and value-dependent misses reach this branch.
			if err := d.tmpl.Execute(&sb, ctx); err == nil && sb.Len() > 0 {
				res.Attributes().PutStr(d.key, sb.String())
			}
		}
	}
	// Derive service.namespace / service.instance.id (Mimir job/instance) from
	// whatever identity attributes ended up on the resource — including
	// pre-populated and template/static-set ones, and regardless of the
	// defaults toggle. Fill-if-absent, so a template-set value still wins.
	Identity(res)
	if b == nil {
		return
	}
	// Prefix the (possibly template-overridden) instance last, before filtering.
	PrefixInstance(res, b.instancePrefix)
	b.filter.Apply(res)
}

// attrCount is an UPPER BOUND on the attributes Build is about to put, for
// pre-sizing only: an over-estimate costs a few unused slots in one allocation,
// while an under-estimate costs the reallocation the sizing exists to avoid.
// Nil-receiver safe, like Build (nil = defaults, no static or template
// attributes).
func (b *Builder) attrCount(ctx Context) int {
	n := 2 // Identity: service.namespace + service.instance.id
	if b == nil || b.defaults {
		if ctx.Node != nil {
			n++ // k8s.node.name
		}
		if p := ctx.Pod; p != nil {
			// namespace, name, uid, node, ip, service.name.
			n += 6 + len(p.Owners) + len(p.Labels)
			if p.NamespaceMetadata != nil {
				n += len(p.NamespaceMetadata.Labels)
			}
		}
		if ctx.Container != nil {
			n += 4 // name, id, image, restart_count
		}
		if ctx.Service != nil {
			n += 2 // name, uid
		}
	}
	if b != nil {
		n += len(b.static) + len(b.dynamic)
	}
	return n
}

// FilterResource applies the builder's global attribute filter to an
// already-built resource. It exists for callers that stamp attributes AFTER
// Build (the tailer's plain-source statics, which are applied post-Build so
// they beat templates and defaults): the operator's enable/disable lists must
// still get the last word over the final attribute set, exactly as they do
// inside Build. Nil-safe.
func (b *Builder) FilterResource(res pcommon.Resource) {
	if b == nil {
		return
	}
	b.filter.Apply(res)
}
