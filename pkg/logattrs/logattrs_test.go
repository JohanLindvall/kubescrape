package logattrs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	ljson "github.com/JohanLindvall/lightning/pkg/json"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"sigs.k8s.io/yaml"
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
	t.Parallel()
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
	// status is an INTEGRAL JSON number, so it decodes as int64 (float64 cannot
	// hold a 64-bit id exactly); ratio keeps float64. Either way apply.Put
	// stores a whole number with PutInt, so the exported attribute is the same.
	if got["level"] != "warn" || got["status"] != int64(503) || got["cached"] != true || got["ratio"] != 0.5 {
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
	t.Parallel()
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
	t.Parallel()
	e := mustExtractor(t, Rule{Key: "level"})
	if r := e.Extract("a plain line with no = or json"); !r.Empty() {
		t.Errorf("plain line extracted %+v", r)
	}
	if r := e.Extract(`{"not json`); !r.Empty() {
		t.Errorf("broken json extracted %+v", r)
	}
}

func TestNilExtractor(t *testing.T) {
	t.Parallel()
	var e *Extractor
	if r := e.Extract(`{"level":"warn"}`); !r.Empty() {
		t.Errorf("nil extractor returned %+v", r)
	}
	if got, err := New(&Config{}); err != nil || got != nil {
		t.Errorf("empty config: extractor=%v err=%v", got, err)
	}
}

func TestNewErrors(t *testing.T) {
	t.Parallel()
	if _, err := New(&Config{Rules: []Rule{{Key: ""}}}); err == nil {
		t.Error("empty key: want error")
	}
	if _, err := New(&Config{Rules: []Rule{{Key: "x", Target: "bogus"}}}); err == nil {
		t.Error("bad target: want error")
	}
}

func TestPutTypes(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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

// LoadConfig loads a standalone config file. Production config arrives solely
// through the unified agent config (cmd/kubescrape-agent -config, the
// logAttributes section); this loader survives only for the strict-YAML
// parse tests here — it has no place in a public package, where it dragged os
// and sigs.k8s.io/yaml into every consumer's dependency graph for a function
// only this file called.
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

func TestLoadConfig(t *testing.T) {
	t.Parallel()
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

// The pooled scratch must hold no reference into the line once Extract has
// returned. The JSON arm's raws are read-only VIEWS into it, and the GC scans a
// slice's whole backing array — so truncating the length released nothing and
// one pooled holder pinned the last line (up to MaxEntryBytes, 1 MiB) until its
// next use. The twin holder in internal/logline was fixed for exactly this.
func TestExtractDoesNotPinTheLine(t *testing.T) {
	t.Parallel()
	e := mustExtractor(t, Rule{Key: "msg"}, Rule{Key: "level"})
	line := `{"level":"info","msg":"` + strings.Repeat("x", 1<<20) + `"}`

	// A scratch straight off the JSON arm, then released: every slot must be
	// nil, not merely out of the slice's LENGTH.
	sc := &scratch{vals: make([]string, len(e.rules)), found: make([]bool, len(e.rules))}
	raws, err := ljson.GetPaths([]byte(line), e.paths, sc.raws[:0])
	if err != nil {
		t.Fatal(err)
	}
	sc.raws = raws[:0]
	if cap(sc.raws) < len(e.paths) {
		t.Fatalf("GetPaths filled %d slots, want %d", cap(sc.raws), len(e.paths))
	}
	e.release(sc)
	for i, raw := range sc.raws[:cap(sc.raws)] {
		if raw != nil {
			t.Fatalf("slot %d still aliases %d bytes of the line after release", i, len(raw))
		}
	}

	// And Extract must go through release. The pooled object is usually the one
	// this goroutine just returned; when the pool hands back something else
	// there is nothing to assert, so the check is on what it does hand back —
	// a false failure here would be worse than the occasional skipped round.
	if r := e.Extract(line); len(r.Log) != 2 {
		t.Fatalf("extracted %d attributes, want 2", len(r.Log))
	}
	if used, _ := e.scratch.Get().(*scratch); used != nil {
		for i, raw := range used.raws[:cap(used.raws)] {
			if raw != nil {
				t.Fatalf("pooled scratch slot %d still aliases %d bytes: Extract does not release", i, len(raw))
			}
		}
	}
}

func BenchmarkExtractJSON(b *testing.B) {
	e, err := New(&Config{Rules: []Rule{
		{Key: "trace_id", Attribute: "trace.id", Target: "log"},
		{Key: "tenant", Attribute: "tenant.id", Target: "resource"},
		{Key: "component", Attribute: "component", Target: "scope"},
	}})
	if err != nil {
		b.Fatal(err)
	}
	line := `{"level":"info","tenant":"acme","component":"api","trace_id":"abc123","msg":"served request","dur_ms":12.5}`
	b.ReportAllocs()
	for b.Loop() {
		r := e.Extract(line)
		if len(r.Log) != 1 || len(r.Resource) != 1 || len(r.Scope) != 1 {
			b.Fatal("bad extract")
		}
	}
}

func BenchmarkExtractLogfmt(b *testing.B) {
	e, err := New(&Config{Rules: []Rule{
		{Key: "trace_id", Attribute: "trace.id", Target: "log"},
		{Key: "tenant", Attribute: "tenant.id", Target: "resource"},
	}})
	if err != nil {
		b.Fatal(err)
	}
	line := `level=info tenant=acme trace_id=abc123 msg="served request" dur_ms=12.5`
	b.ReportAllocs()
	for b.Loop() {
		r := e.Extract(line)
		if len(r.Log) != 1 || len(r.Resource) != 1 {
			b.Fatal("bad extract")
		}
	}
}

func BenchmarkExtractNoMatchJSON(b *testing.B) {
	e, err := New(&Config{Rules: []Rule{{Key: "trace_id", Attribute: "trace.id", Target: "log"}}})
	if err != nil {
		b.Fatal(err)
	}
	line := `{"level":"info","msg":"served request","dur_ms":12.5}`
	b.ReportAllocs()
	for b.Loop() {
		if r := e.Extract(line); len(r.Log) != 0 {
			b.Fatal("bad extract")
		}
	}
}

// Key must honor the full Attr.Val contract, int64 included: a producer
// following the contract must not have its values silently dropped from the
// grouping key (two sets differing only in an int64 value would merge into
// one mis-attributed resource/scope).
func TestKeyDistinguishesInt64Values(t *testing.T) {
	t.Parallel()
	a := Key([]Attr{{Key: "pid", Val: int64(1)}})
	b := Key([]Attr{{Key: "pid", Val: int64(2)}})
	if a == b {
		t.Fatalf("int64 values dropped from the grouping key: %q == %q", a, b)
	}
	// int64 must not alias the STRING form of the same digits — but a WHOLE
	// float64 must key identically to the int64, because Put stores both with
	// PutInt: key identity follows stored identity, or {"shard":2} and
	// {"shard":2.0} split into two ResourceLogs whose exported resources are
	// byte-identical (a duplicate resource per payload for an emitter mixing
	// spellings).
	f := Key([]Attr{{Key: "pid", Val: float64(1)}})
	if a != f {
		t.Fatalf("whole float64 keys differently from the int64 it is stored as: i=%q f=%q", a, f)
	}
	if frac := Key([]Attr{{Key: "pid", Val: 1.5}}); frac == a {
		t.Fatalf("fractional float64 aliases int64: %q", frac)
	}
	s := Key([]Attr{{Key: "pid", Val: "1"}})
	if a == s {
		t.Fatalf("int64 aliases the string form: i=%q s=%q", a, s)
	}
}

// A logfmt value's escapes must be decoded, exactly as the JSON arm decodes
// them: the same logical value has to read the same whichever format the line
// used. In the tailer a record attribute lifted here is consulted BEFORE the
// raw line, so a divergence here silently changes what a logMetrics label or a
// logs.rules selector matches.
func TestLogfmtValuesAreUnescaped(t *testing.T) {
	t.Parallel()
	e, err := New(&Config{Rules: []Rule{{Key: "msg", Attribute: "msg"}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ line, want string }{
		{`level=error msg="connect failed: \"db-1\" retrying"`, `connect failed: "db-1" retrying`},
		{`msg="a\tb"`, "a\tb"},
		{`msg="plain"`, "plain"}, // the no-escape fast path
	} {
		got := e.Extract(tc.line)
		var have string
		for _, a := range got.Log {
			if a.Key == "msg" {
				have, _ = a.Val.(string)
			}
		}
		if have != tc.want {
			t.Errorf("%s\n  got  %q\n  want %q", tc.line, have, tc.want)
		}
	}
}

// A BARE logfmt key yields the sentinel "true", which is prose, not a field:
// `weight=10 disk error` must not lift error="true" onto every record carrying
// that sentence. Whether the words are scanned at all depends on an unrelated
// '=' elsewhere on the line (the no-'=' fast path), so admitting them makes an
// attribute appear and disappear with the rest of the sentence.
func TestBareLogfmtKeysLiftNothing(t *testing.T) {
	t.Parallel()
	e := mustExtractor(t, Rule{Key: "error"}, Rule{Key: "disk"}, Rule{Key: "weight"})
	for _, line := range []string{`weight=10 disk error`, `disk error`} {
		r := e.Extract(line)
		for _, a := range r.Log {
			if a.Key != "weight" {
				t.Errorf("line %q lifted %s=%v from a bare word", line, a.Key, a.Val)
			}
		}
	}
	r := e.Extract(`weight=10 disk error`)
	if len(r.Log) != 1 || r.Log[0].Key != "weight" || r.Log[0].Val != "10" {
		t.Errorf("real pairs on the same line must still lift: %+v", r.Log)
	}
}

// Duplicate keys resolve FIRST-wins in JSON (lightning's GetPaths contract) and
// LAST-wins in logfmt (the scan overwrites the slot). Both inputs are
// malformed and neither reader dictates an answer, so the asymmetry is
// documented rather than papered over — this pins what the documentation says,
// here and in the twin extractor in internal/logline.
func TestDuplicateKeyResolutionIsAsDocumented(t *testing.T) {
	t.Parallel()
	e := mustExtractor(t, Rule{Key: "level"})
	for line, want := range map[string]string{
		`{"level":"info","level":"warn"}`: "info",
		`level=info level=warn`:           "warn",
	} {
		r := e.Extract(line)
		if len(r.Log) != 1 || r.Log[0].Val != want {
			t.Errorf("line %q lifted %+v, want level=%q", line, r.Log, want)
		}
	}
}

// FloatString is the ONE float rendering shared with the metric-label and
// record-attribute paths: pcommon's ES6 algorithm, not FormatFloat('f').
func TestFloatStringMatchesPcommon(t *testing.T) {
	t.Parallel()
	for _, f := range []float64{5e-7, 1e21, 2.5e22, 1e-6, 42.5, 0, -3e21, -0.125} {
		if got, want := FloatString(f), pcommon.NewValueDouble(f).AsString(); got != want {
			t.Errorf("FloatString(%v) = %q, pcommon says %q", f, got, want)
		}
	}
}
