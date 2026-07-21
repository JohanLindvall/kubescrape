package attrs

import (
	"os"
	"path/filepath"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"fmt"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"sigs.k8s.io/yaml"
)

// LoadConfig loads a standalone config file. Production config arrives solely
// through the unified agent config (cmd/kubescrape-agent -config); this
// loader survives only for the strict-YAML parse/validate tests here.
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

func testCtx() Context {
	return Context{
		Node: &NodeInfo{
			Name:   "node1",
			Labels: map[string]string{"topology.kubernetes.io/zone": "eu-1", "agentpool": "system"},
		},
		Pod: &kubemeta.Pod{
			Name: "pod1", Namespace: "ns1", UID: "u1",
			Labels: map[string]string{"team": "core"},
			Owners: []kubemeta.Owner{{Kind: "Deployment", Name: "dep1"}},
		},
		Container: &kubemeta.Container{Name: "app", ID: "cid", Image: "img:1"},
	}
}

func build(t *testing.T, cfg *Config, filter *Filter, ctx Context) map[string]any {
	t.Helper()
	b, err := NewBuilder(cfg, filter)
	if err != nil {
		t.Fatal(err)
	}
	res := pcommon.NewResource()
	b.Build(res, ctx)
	return res.Attributes().AsRaw()
}

func TestBuilderNilIsDefaults(t *testing.T) {
	var b *Builder
	res := pcommon.NewResource()
	b.Build(res, testCtx())
	got := res.Attributes().AsRaw()
	if got["k8s.pod.name"] != "pod1" || got["k8s.node.name"] != "node1" || got["container.id"] != "cid" {
		t.Fatalf("attrs = %v", got)
	}
}

func TestBuilderStaticAndTemplates(t *testing.T) {
	got := build(t, &Config{
		Static: map[string]string{"cluster": "prod"},
		Attributes: map[string]string{
			"team":      `{{ index .Pod.Labels "team" }}`,
			"image":     `{{ with .Container }}{{ .Image }}{{ end }}`,
			"missing":   `{{ index .Pod.Labels "nope" }}`, // empty -> omitted
			"nil-deref": `{{ .Service.Name }}`,            // exec error -> omitted
		},
	}, nil, testCtx())

	if got["cluster"] != "prod" || got["team"] != "core" || got["image"] != "img:1" {
		t.Fatalf("attrs = %v", got)
	}
	for _, absent := range []string{"missing", "nil-deref"} {
		if _, ok := got[absent]; ok {
			t.Errorf("attribute %q should be omitted: %v", absent, got)
		}
	}
	if got["k8s.deployment.name"] != "dep1" {
		t.Fatalf("defaults missing: %v", got)
	}
}

func TestBuilderDefaultsOff(t *testing.T) {
	off := false
	got := build(t, &Config{
		Defaults:   &off,
		Attributes: map[string]string{"pod": `{{ .Pod.Name }}`},
	}, nil, testCtx())
	if len(got) != 1 || got["pod"] != "pod1" {
		t.Fatalf("attrs = %v (defaults must be off)", got)
	}
}

func TestBuilderFilterRunsLast(t *testing.T) {
	filter, err := NewFilter("", "cluster")
	if err != nil {
		t.Fatal(err)
	}
	got := build(t, &Config{Static: map[string]string{"cluster": "prod"}}, filter, testCtx())
	if _, ok := got["cluster"]; ok {
		t.Fatalf("filter must apply to injected attributes: %v", got)
	}
}

func TestBuilderBadTemplate(t *testing.T) {
	if _, err := NewBuilder(&Config{Attributes: map[string]string{"x": "{{"}}, nil); err == nil {
		t.Fatal("invalid template must error")
	}
}

// instanceOf builds one pipeline's resource and returns its service.instance.id.
func instanceOf(t *testing.T, b *Builder) string {
	t.Helper()
	res := pcommon.NewResource()
	b.Build(res, testCtx())
	v, _ := res.Attributes().Get("service.instance.id")
	return v.Str()
}

func TestInstancePrefixDefaults(t *testing.T) {
	// testCtx has container.id "cid" -> derived instance "cid".
	bs, err := NewBuilders(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := instanceOf(t, bs.Cadvisor); got != "cadvisor-cid" {
		t.Errorf("cadvisor instance = %q, want cadvisor-cid (default prefix)", got)
	}
	for name, b := range map[string]*Builder{"targets": bs.Targets, "logs": bs.Logs, "node": bs.Node, "ingest": bs.Ingest} {
		if got := instanceOf(t, b); got != "cid" {
			t.Errorf("%s instance = %q, want cid (no prefix)", name, got)
		}
	}
}

func TestInstancePrefixConfig(t *testing.T) {
	empty, custom := "", "ksm"
	bs, err := NewBuilders(&Config{
		Pipelines: map[string]*Config{
			"cadvisor": {InstancePrefix: &empty},  // opt out of the default
			"targets":  {InstancePrefix: &custom}, // opt in
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := instanceOf(t, bs.Cadvisor); got != "cid" {
		t.Errorf("cadvisor instance = %q, want cid (default cleared)", got)
	}
	if got := instanceOf(t, bs.Targets); got != "ksm-cid" {
		t.Errorf("targets instance = %q, want ksm-cid", got)
	}
}

func TestInstancePrefixTopLevel(t *testing.T) {
	prefix := "cluster7"
	bs, err := NewBuilders(&Config{InstancePrefix: &prefix}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Top-level applies to pipelines without their own default/override...
	if got := instanceOf(t, bs.Targets); got != "cluster7-cid" {
		t.Errorf("targets instance = %q, want cluster7-cid", got)
	}
	// ...but the cadvisor pipeline's built-in default still wins over the base
	// (a pipeline default is only overridden by an explicit pipeline setting).
	if got := instanceOf(t, bs.Cadvisor); got != "cadvisor-cid" {
		t.Errorf("cadvisor instance = %q, want cadvisor-cid", got)
	}
}

// Identity derivation is independent of the defaults toggle and runs after
// templates: a defaults:false pipeline whose resource carries (pre-populated
// or template-set) identity attributes still gets service.instance.id.
func TestIdentityWithDefaultsOff(t *testing.T) {
	off := false
	b, err := NewBuilder(&Config{
		Defaults:   &off,
		Attributes: map[string]string{"k8s.namespace.name": `{{ .Pod.Namespace }}`, "k8s.pod.name": `{{ .Pod.Name }}`},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	res := pcommon.NewResource()
	b.Build(res, testCtx())
	got := res.Attributes().AsRaw()
	if got["service.instance.id"] != "ns1/pod1" || got["service.namespace"] != "ns1" {
		t.Fatalf("identity not derived with defaults off: %v", got)
	}

	// A template-set instance still wins over the derived one.
	b2, err := NewBuilder(&Config{
		Defaults:   &off,
		Attributes: map[string]string{"k8s.pod.name": `{{ .Pod.Name }}`, "service.instance.id": "tmpl-wins"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	res = pcommon.NewResource()
	b2.Build(res, testCtx())
	if v, _ := res.Attributes().Get("service.instance.id"); v.Str() != "tmpl-wins" {
		t.Fatalf("template instance overwritten: %q", v.Str())
	}
}

// A prefix must never be stamped on its own: with no derived instance there is
// nothing to disambiguate, and a shared bare "cadvisor" instance would be
// worse than none.
func TestPrefixInstanceSkipsWithoutInstance(t *testing.T) {
	off := false
	prefix := "cadvisor"
	b, err := NewBuilder(&Config{Defaults: &off, InstancePrefix: &prefix}, nil)
	if err != nil {
		t.Fatal(err)
	}
	res := pcommon.NewResource()
	b.Build(res, testCtx())
	if v, ok := res.Attributes().Get("service.instance.id"); ok {
		t.Fatalf("bare prefix stamped as instance: %q", v.Str())
	}
}

func TestLoadConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "attrs.yaml")
	if err := os.WriteFile(path, []byte(`
defaults: false
static:
  cluster: kind
attributes:
  team: '{{ index .Pod.Labels "team" }}'
`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Defaults == nil || *cfg.Defaults || cfg.Static["cluster"] != "kind" || len(cfg.Attributes) != 1 {
		t.Fatalf("cfg = %+v", cfg)
	}

	if err := os.WriteFile(path, []byte("nonsense: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("unknown fields must error")
	}
}

func TestBuilderTemplateFuncs(t *testing.T) {
	t.Setenv("TEST_CLUSTER", "prod-eu")
	got := build(t, &Config{
		Attributes: map[string]string{
			"cluster":       `{{ env "TEST_CLUSTER" }}`,
			"svc":           `{{ coalesce (index .Pod.Labels "gp/service-name") (index .Pod.Labels "team") .Pod.Name }}`,
			"fallback":      `{{ default "unknown" (index .Pod.Labels "nope") }}`,
			"zone":          `{{ with .Node }}{{ index .Labels "topology.kubernetes.io/zone" }}{{ end }}`,
			"infra":         `{{ if regexMatch "^tigera-operator$|-system$" .Pod.Namespace }}gp-infrastructure{{ end }}`,
			"trimmed-image": `{{ with .Container }}{{ regexReplace ":.*$" "" .Image }}{{ end }}`,
		},
	}, nil, testCtx())

	want := map[string]string{
		"cluster":       "prod-eu",
		"svc":           "core", // team label wins over pod name
		"fallback":      "unknown",
		"zone":          "eu-1",
		"trimmed-image": "img",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v, want %q (all: %v)", k, got[k], v, got)
		}
	}
	if _, ok := got["infra"]; ok {
		t.Errorf("infra should be omitted for namespace ns1: %v", got)
	}
}

func TestBuildersPipelines(t *testing.T) {
	off := false
	cfg := &Config{
		Static:     map[string]string{"cluster": "prod"},
		Attributes: map[string]string{"team": `{{ index .Pod.Labels "team" }}`},
		Pipelines: map[string]*Config{
			"node": {
				Defaults:   &off,
				Static:     map[string]string{"scope": "node"},
				Attributes: map[string]string{"team": "infra"},
			},
		},
	}
	bs, err := NewBuilders(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	res := pcommon.NewResource()
	bs.Targets.Build(res, testCtx())
	got := res.Attributes().AsRaw()
	if got["cluster"] != "prod" || got["team"] != "core" || got["k8s.pod.name"] != "pod1" {
		t.Fatalf("targets attrs = %v", got)
	}

	res = pcommon.NewResource()
	bs.Node.Build(res, testCtx())
	got = res.Attributes().AsRaw()
	// Pipeline override: defaults off, statics merged, attribute replaced.
	if got["cluster"] != "prod" || got["scope"] != "node" || got["team"] != "infra" {
		t.Fatalf("node attrs = %v", got)
	}
	if _, ok := got["k8s.pod.name"]; ok {
		t.Fatalf("node pipeline must have defaults off: %v", got)
	}
}

// NewBuilders must reject bad pipeline sections itself: the unified agent
// config unmarshals the resourceAttributes section without going through
// LoadConfig, so a typo'd pipeline name (a map key strict YAML parsing cannot
// catch) or a nested pipelines section must not be silently ignored.
func TestNewBuildersPipelineValidation(t *testing.T) {
	if _, err := NewBuilders(&Config{
		Pipelines: map[string]*Config{"bogus": {Static: map[string]string{"a": "b"}}},
	}, nil); err == nil {
		t.Error("unknown pipeline name must error")
	}
	if _, err := NewBuilders(&Config{
		Pipelines: map[string]*Config{"logs": {
			Pipelines: map[string]*Config{"node": {Static: map[string]string{"a": "b"}}},
		}},
	}, nil); err == nil {
		t.Error("nested pipelines must error")
	}
}

func TestLoadConfigPipelineValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "attrs.yaml")
	if err := os.WriteFile(path, []byte("pipelines:\n  bogus:\n    static: {a: b}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("unknown pipeline name must error")
	}
}

func TestParseStatic(t *testing.T) {
	got, err := ParseStatic("a=1, b=x=y ,")
	if err != nil {
		t.Fatal(err)
	}
	if got["a"] != "1" || got["b"] != "x=y" {
		t.Fatalf("got %v", got)
	}
	if m, err := ParseStatic(""); err != nil || m != nil {
		t.Fatalf("empty input: %v %v", m, err)
	}
	if _, err := ParseStatic("novalue"); err == nil {
		t.Fatal("missing '=' must error")
	}
}
