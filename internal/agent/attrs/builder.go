package attrs

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"text/template"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"sigs.k8s.io/yaml"

	"github.com/JohanLindvall/kubescrape/internal/kubemeta"
)

// Context carries the metadata available when one resource is built. Fields
// are nil/empty when not applicable (e.g. no Container on a pod-level
// resource).
type Context struct {
	Node      string
	Pod       *kubemeta.Pod
	Container *kubemeta.Container
	Service   *kubemeta.Service
}

// Config declares how resource attributes are built. It is loaded from YAML
// (-resource-attrs-config):
//
//	defaults: true            # include the built-in k8s.* mapping
//	static:                   # fixed attributes on every resource
//	  cluster: prod-eu
//	attributes:               # Go templates evaluated against Context
//	  team: '{{ index .Pod.Labels "team" }}'
//	  image: '{{ with .Container }}{{ .Image }}{{ end }}'
//
// Template attributes that evaluate to "" (or fail, e.g. a nil .Pod) are
// omitted.
type Config struct {
	// Defaults includes the built-in mapping (k8s.pod.name, owners, labels,
	// ...); absent means true.
	Defaults *bool `json:"defaults,omitempty"`
	// Static attributes are set verbatim on every resource.
	Static map[string]string `json:"static,omitempty"`
	// Attributes maps attribute keys to Go templates over Context.
	Attributes map[string]string `json:"attributes,omitempty"`
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
	return &cfg, nil
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

// NewBuilder compiles a Builder; a nil cfg means defaults only.
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
		tmpl, err := template.New(key).Option("missingkey=zero").Parse(cfg.Attributes[key])
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
		if ctx.Node != "" {
			res.Attributes().PutStr("k8s.node.name", ctx.Node)
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
