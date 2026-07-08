package attrs

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"text/template"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"sigs.k8s.io/yaml"

	"github.com/JohanLindvall/kubescrape/internal/kubemeta"
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

// Config declares how resource attributes are built. It is loaded from YAML
// (-resource-attrs-config):
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
	// Pipelines overrides/extends the top-level settings for one pipeline;
	// static and attribute maps merge with the pipeline entry winning.
	Pipelines map[string]*Config `json:"pipelines,omitempty"`
}

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
	for name, sub := range cfg.Pipelines {
		if !slicesContains(pipelineNames, name) {
			return nil, fmt.Errorf("%s: unknown pipeline %q (want one of %s)", path, name, strings.Join(pipelineNames, ", "))
		}
		if sub != nil && len(sub.Pipelines) > 0 {
			return nil, fmt.Errorf("%s: pipeline %q must not nest pipelines", path, name)
		}
	}
	return &cfg, nil
}

func slicesContains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
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

// NewBuilders compiles the per-pipeline builders from cfg (nil = defaults
// everywhere) and one shared filter.
func NewBuilders(cfg *Config, filter *Filter) (*Builders, error) {
	base, err := NewBuilder(cfg, filter)
	if err != nil {
		return nil, err
	}
	b := &Builders{Logs: base, Targets: base, Cadvisor: base, Node: base, Journal: base, Ingest: base}
	if cfg == nil {
		return b, nil
	}
	for name, sub := range cfg.Pipelines {
		merged := mergeConfig(cfg, sub)
		pb, err := NewBuilder(merged, filter)
		if err != nil {
			return nil, fmt.Errorf("pipeline %q: %w", name, err)
		}
		switch name {
		case "logs":
			b.Logs = pb
		case "targets":
			b.Targets = pb
		case "cadvisor":
			b.Cadvisor = pb
		case "node":
			b.Node = pb
		case "journal":
			b.Journal = pb
		case "ingest":
			b.Ingest = pb
		}
	}
	return b, nil
}

// mergeConfig overlays a pipeline config onto the base; maps merge with the
// pipeline winning.
func mergeConfig(base, over *Config) *Config {
	out := &Config{Defaults: base.Defaults}
	if over != nil && over.Defaults != nil {
		out.Defaults = over.Defaults
	}
	out.Static = mergeMaps(base.Static, overMap(over, func(c *Config) map[string]string { return c.Static }))
	out.Attributes = mergeMaps(base.Attributes, overMap(over, func(c *Config) map[string]string { return c.Attributes }))
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
	defaults bool
	static   map[string]string
	dynamic  []dynamicAttr
	filter   *Filter
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
	defaults := b == nil || b.defaults
	if defaults {
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
	if b == nil {
		return
	}
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
