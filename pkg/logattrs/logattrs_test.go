package logattrs

import (
	"os"
	"path/filepath"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

func mustExtractor(t *testing.T, rules ...Rule) *Extractor {
	t.Helper()
	e, err := New(&Config{Rules: rules})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestExtractJSON(t *testing.T) {
	e := mustExtractor(t,
		Rule{Key: "user.id", Attribute: "enduser.id", Target: TargetResource},
		Rule{Key: "region", Target: TargetScope},
		Rule{Key: "level", Target: TargetLog},
		Rule{Key: "status"},                 // defaults: attribute=key, target=log
		Rule{Key: "cached"},                 // bool
		Rule{Key: "ratio"},                  // non-integer float
		Rule{Key: "absent"},                 // missing
		Rule{Key: "obj", Target: TargetLog}, // object → skipped
	)
	r := e.Extract(`{"user":{"id":"u-42"},"region":"eu","level":"warn","status":503,"cached":true,"ratio":0.5,"obj":{"a":1}}`)

	if len(r.Resource) != 1 || r.Resource[0].Key != "enduser.id" || r.Resource[0].Val != "u-42" {
		t.Errorf("resource = %+v", r.Resource)
	}
	if len(r.Scope) != 1 || r.Scope[0].Val != "eu" {
		t.Errorf("scope = %+v", r.Scope)
	}
	got := map[string]any{}
	for _, a := range r.Log {
		got[a.Key] = a.Val
	}
	if got["level"] != "warn" || got["status"] != float64(503) || got["cached"] != true || got["ratio"] != 0.5 {
		t.Errorf("log = %+v", got)
	}
	if _, ok := got["absent"]; ok {
		t.Error("absent key extracted")
	}
	if _, ok := got["obj"]; ok {
		t.Error("object value extracted")
	}
}

func TestExtractLogfmt(t *testing.T) {
	e := mustExtractor(t,
		Rule{Key: "level", Target: TargetLog},
		Rule{Key: "tenant", Target: TargetResource},
		Rule{Key: "absent"},
	)
	r := e.Extract(`ts=2026-01-02T03:04:05Z level=error tenant=acme msg="boom"`)
	if len(r.Log) != 1 || r.Log[0].Val != "error" {
		t.Errorf("log = %+v", r.Log)
	}
	if len(r.Resource) != 1 || r.Resource[0].Val != "acme" {
		t.Errorf("resource = %+v", r.Resource)
	}

	// A duplicated key keeps its last value.
	r = e.Extract(`level=info level=warn`)
	if len(r.Log) != 1 || r.Log[0].Val != "warn" {
		t.Errorf("duplicate key = %+v, want last value", r.Log)
	}
}

func TestExtractNonStructured(t *testing.T) {
	e := mustExtractor(t, Rule{Key: "level"})
	if r := e.Extract("a plain line with no = or json"); !r.Empty() {
		t.Errorf("plain line extracted %+v", r)
	}
	if r := e.Extract(`{"not json`); !r.Empty() {
		t.Errorf("broken json extracted %+v", r)
	}
}

func TestNilExtractor(t *testing.T) {
	var e *Extractor
	if r := e.Extract(`{"level":"warn"}`); !r.Empty() {
		t.Errorf("nil extractor returned %+v", r)
	}
	if got, err := New(&Config{}); err != nil || got != nil {
		t.Errorf("empty config: extractor=%v err=%v", got, err)
	}
}

func TestNewErrors(t *testing.T) {
	if _, err := New(&Config{Rules: []Rule{{Key: ""}}}); err == nil {
		t.Error("empty key: want error")
	}
	if _, err := New(&Config{Rules: []Rule{{Key: "x", Target: "bogus"}}}); err == nil {
		t.Error("bad target: want error")
	}
}

func TestPutTypes(t *testing.T) {
	m := pcommon.NewMap()
	Put(m, []Attr{
		{Key: "s", Val: "str"},
		{Key: "b", Val: true},
		{Key: "i", Val: float64(42)},
		{Key: "f", Val: 1.5},
	})
	if v, _ := m.Get("s"); v.Type() != pcommon.ValueTypeStr || v.Str() != "str" {
		t.Errorf("s = %v", v.AsRaw())
	}
	if v, _ := m.Get("b"); v.Type() != pcommon.ValueTypeBool {
		t.Errorf("b = %v", v.AsRaw())
	}
	if v, _ := m.Get("i"); v.Type() != pcommon.ValueTypeInt || v.Int() != 42 {
		t.Errorf("i = %v", v.AsRaw())
	}
	if v, _ := m.Get("f"); v.Type() != pcommon.ValueTypeDouble {
		t.Errorf("f = %v", v.AsRaw())
	}
}

func TestKeyStability(t *testing.T) {
	a := []Attr{{Key: "x", Val: "1"}, {Key: "y", Val: float64(2)}}
	b := []Attr{{Key: "x", Val: "1"}, {Key: "y", Val: float64(2)}}
	if Key(a) != Key(b) {
		t.Error("Key not stable")
	}
	if Key(nil) != "" {
		t.Error("empty key not empty")
	}
	if Key(a) == Key([]Attr{{Key: "x", Val: "2"}, {Key: "y", Val: float64(2)}}) {
		t.Error("distinct attrs share a key")
	}
}

func TestLoadConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "la.yaml")
	_ = os.WriteFile(path, []byte("rules:\n  - key: user.id\n    attribute: enduser.id\n    target: resource\n  - key: level\n"), 0o644)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Rules) != 2 || cfg.Rules[0].Attribute != "enduser.id" || cfg.Rules[0].Target != TargetResource {
		t.Errorf("cfg = %+v", cfg.Rules)
	}
	if _, err := LoadConfig(filepath.Join(dir, "nope.yaml")); err == nil {
		t.Error("missing file: want error")
	}
}
