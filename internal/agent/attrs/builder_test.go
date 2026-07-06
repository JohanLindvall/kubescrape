package attrs

import (
	"os"
	"path/filepath"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/kubemeta"
)

func testCtx() Context {
	return Context{
		Node: "node1",
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
