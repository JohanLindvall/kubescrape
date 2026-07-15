package attrs

import (
	"fmt"
	"os"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"text/template"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"sigs.k8s.io/yaml"

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

// Pipeline names accepted under Config.Pipelines.
var pipelineNames = []string{"logs", "targets", "cadvisor", "node", "journal", "ingest"}

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
//	pipelines:                # per-pipeline overrides (logs|targets|cadvisor|node|journal)
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
	// Pipelines overrides/extends the top-level settings for one pipeline;
	// static and attribute maps merge with the pipeline entry winning.
	Pipelines map[string]*Config `json:"pipelines,omitempty"`
}

// defaultInstancePrefix is the built-in service.instance.id prefix per pipeline
// (empty for pipelines whose resources are the exporter's own identity). It
// applies unless the config sets InstancePrefix explicitly.
var defaultInstancePrefix = map[string]string{"cadvisor": "cadvisor"}

// LoadConfig reads a Config from a YAML file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.UnmarshalStrict(data, &cfg); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if err := cfg.validatePipelines(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &cfg, nil
}

// validatePipelines rejects unknown pipeline names (strict YAML parsing cannot
// catch bad map keys) and nested pipeline sections. It runs in LoadConfig and
// again in NewBuilders, so the unified agent config path — which unmarshals
// the section itself — gets the same errors instead of silently ignoring a
// typo'd pipeline.
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
	Journal  *Builder
	Ingest   *Builder
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
		"node": &b.Node, "journal": &b.Journal, "ingest": &b.Ingest,
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

// regexCache backs the template regex functions; patterns come from a fixed
// config so the cache stays tiny.
var regexCache sync.Map // pattern -> *regexp.Regexp | error

func cachedRegexp(pattern string) (*regexp.Regexp, error) {
	if v, ok := regexCache.Load(pattern); ok {
		if re, ok := v.(*regexp.Regexp); ok {
			return re, nil
		}
		return nil, v.(error)
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		regexCache.Store(pattern, err)
		return nil, err
	}
	regexCache.Store(pattern, re)
	return re, nil
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
// sections of cfg are ignored here (see NewBuilders).
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
		b.dynamic = append(b.dynamic, dynamicAttr{key: key, tmpl: tmpl})
	}
	return b, nil
}

// Build fills res from ctx. Safe on a nil receiver (defaults only, no
// filter).
func (b *Builder) Build(res pcommon.Resource, ctx Context) {
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
			// empty results mean "attribute not applicable here".
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

// ParseStatic parses a "key=value,key=value" flag into a static attribute
// map (merged over any config-file statics by the caller).
func ParseStatic(s string) (map[string]string, error) {
	out := map[string]string{}
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part == "" {
			continue
		}
		key, value, ok := strings.Cut(part, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("static attribute %q: want key=value", part)
		}
		out[key] = value
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}
