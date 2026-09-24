package docscheck

import (
	"encoding/json"
	"net/netip"
	"os"
	"slices"
	"strings"
	"testing"
)

// A recursive shape of both kinds the real config has: a MAP of self (the
// agent's resourceAttributes.pipelines) and a SLICE of self inside a nested
// struct (tailSampling's and/composite sub-policies).
type recPolicy struct {
	Name string    `json:"name,omitempty"`
	And  recAndArm `json:"and"`
}

type recAndArm struct {
	Sub []recPolicy `json:"andSubPolicy,omitempty"`
}

type recAttrs struct {
	Prefix    string               `json:"instancePrefix,omitempty"`
	Pipelines map[string]*recAttrs `json:"pipelines,omitempty"`
}

type recConfig struct {
	Attrs    recAttrs    `json:"resourceAttributes"`
	Policies []recPolicy `json:"policies,omitempty"`
}

// The generator's whole claim is parity with UnmarshalStrict, and a cycle used
// to be answered with an unconstrained {} — which in draft-07 accepts ANY
// instance. That made exactly the deepest, most hand-written nodes
// (per-pipeline resourceAttributes overrides, and/composite sub-policies) the
// three places a typo validated clean and then killed every agent on startup.
// A recursive type must render once under definitions and by $ref thereafter,
// so those subtrees keep additionalProperties: false like everything else.
func TestRecursiveTypesRefTheirDefinitionRatherThanAcceptingAnything(t *testing.T) {
	root := schemaOf(t, recConfig{})

	defs, _ := root["definitions"].(map[string]any)
	if len(defs) == 0 {
		t.Fatal("no definitions emitted for a recursive config tree")
	}

	// The map-of-self door.
	attrs := prop(t, root, "resourceAttributes")
	mustBeStrictObject(t, attrs, "resourceAttributes")
	pipelines := prop(t, attrs, "pipelines")
	target := refTarget(t, root, pipelines["additionalProperties"], "resourceAttributes.pipelines value")
	mustBeStrictObject(t, target, "the pipelines definition")
	if _, ok := propsOf(t, target)["instancePrefix"]; !ok {
		t.Error("the pipelines definition does not describe the type's own fields")
	}
	// ...and it recurses through the definition, not through a fresh expansion.
	inner := prop(t, target, "pipelines")
	refTarget(t, root, inner["additionalProperties"], "the definition's own pipelines value")

	// The slice-of-self door, one struct deeper.
	policies := prop(t, root, "policies")
	item, ok := policies["items"].(map[string]any)
	if !ok {
		t.Fatalf("policies items is %T, want a schema", policies["items"])
	}
	mustBeStrictObject(t, item, "policies items")
	and := prop(t, item, "and")
	mustBeStrictObject(t, and, "policies[].and")
	sub := prop(t, and, "andSubPolicy")
	subItem := refTarget(t, root, sub["items"], "policies[].and.andSubPolicy items")
	mustBeStrictObject(t, subItem, "the sub-policy definition")
	if _, ok := propsOf(t, subItem)["name"]; !ok {
		t.Error("the sub-policy definition does not describe the type's own fields")
	}

	// Nothing anywhere may be an empty schema: an empty schema is the "anything
	// goes" this test exists to keep out, and the only loosenings that produce
	// one (a custom UnmarshalJSON, an interface field) are absent from this
	// fixture. The null arm is not one of them: it adds {type: null} beside a
	// schema, it never replaces one.
	if paths := emptySchemas(root, "#"); len(paths) > 0 {
		t.Errorf("unconstrained subschemas at %v — every one of them accepts a typo the strict decoder refuses",
			paths)
	}
}

// The definitions registry is shared across the walk, so a type reached from
// two branches must not be defined twice under one name with different bodies,
// and a $ref must always resolve.
func TestEveryRefResolvesToADefinition(t *testing.T) {
	root := schemaOf(t, recConfig{})
	defs, _ := root["definitions"].(map[string]any)
	var walk func(n any, path string)
	walk = func(n any, path string) {
		if arr, ok := n.([]any); ok {
			// The null arm wraps every $ref below the root in an anyOf, so a
			// walk that stopped at arrays would check none of them.
			for i, v := range arr {
				walk(v, path+"/"+itoa(i))
			}
			return
		}
		m, ok := n.(map[string]any)
		if !ok {
			return
		}
		if r, ok := m["$ref"].(string); ok {
			name := strings.TrimPrefix(r, "#/definitions/")
			if name == r {
				t.Errorf("%s: $ref %q does not point into #/definitions", path, r)
				return
			}
			if _, ok := defs[name]; !ok {
				t.Errorf("%s: $ref %q has no definition (a dangling ref validates nothing)", path, r)
			}
			return
		}
		for k, v := range m {
			walk(v, path+"/"+k)
		}
	}
	walk(root, "#")
}

// The three nodes the fix is about, asserted on the SHIPPED artifact rather
// than on a fixture: these are the exact paths a Draft7Validator accepted any
// object at (a typo'd per-pipeline override, a typo'd and/composite sub-policy
// field), while -check-config refused the same document. The generator test in
// cmd/kubescrape-agent keeps the file CURRENT; this keeps it CONSTRAINED.
func TestShippedConfigSchemaConstrainsItsRecursiveNodes(t *testing.T) {
	b, err := os.ReadFile("../../docs/agent-config.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var root map[string]any
	if err := json.Unmarshal(b, &root); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		what string
		path []string
	}{
		{"resourceAttributes.pipelines.<name>",
			[]string{"properties", "resourceAttributes", "properties", "pipelines", "additionalProperties"}},
		{"tailSampling.policies[].and.andSubPolicy[]",
			[]string{"properties", "tailSampling", "properties", "policies", "items", "properties", "and", "properties", "andSubPolicy", "items"}},
		{"tailSampling.policies[].composite.compositeSubPolicy[]",
			[]string{"properties", "tailSampling", "properties", "policies", "items", "properties", "composite", "properties", "compositeSubPolicy", "items"}},
	} {
		node := any(root)
		for _, step := range c.path {
			m, ok := node.(map[string]any)
			if !ok {
				t.Fatalf("%s: %q is not an object on the way down", c.what, step)
			}
			if node, ok = m[step]; !ok {
				t.Fatalf("%s: no %q — the schema's shape changed; re-point this test", c.what, step)
			}
		}
		mustBeStrictObject(t, refTarget(t, root, node, c.what), c.what+" definition")
	}
}

// Every kind of value the config tree holds, each reachable as a property, a
// map value and a list item.
type nullLeaf struct {
	S string `json:"s"`
}

type nullConfig struct {
	Str    string              `json:"str"`
	Num    int                 `json:"num"`
	Flag   bool                `json:"flag"`
	Ratio  float64             `json:"ratio"`
	Ptr    *nullLeaf           `json:"ptr"`
	Leaf   nullLeaf            `json:"leaf"`
	List   []string            `json:"list"`
	Leaves []nullLeaf          `json:"leaves"`
	ByName map[string]nullLeaf `json:"byName"`
	Rec    recAttrs            `json:"rec"`
}

// YAML spells null as a key with nothing after it — which is what a section
// whose children are all commented out looks like — and encoding/json accepts
// null for ANY field. A schema that refused it made an editor flag `logs:` as
// the wrong type on a file the agent loads without a word, which is the
// optimistic direction the package doc says the schema never takes.
func TestNullIsAcceptedWhereverTheDecoderAcceptsIt(t *testing.T) {
	// The decoder half, so the schema is held to what the binary DOES: every
	// field, map value and list item null, under the strict decode.
	doc := `{"str":null,"num":null,"flag":null,"ratio":null,"ptr":null,"leaf":null,"list":[null],` +
		`"leaves":[null],"byName":{"x":null},"rec":{"pipelines":{"logs":null}}}`
	dec := json.NewDecoder(strings.NewReader(doc))
	dec.DisallowUnknownFields()
	var v nullConfig
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("fixture: the strict decoder refused nulls: %v", err)
	}

	root := schemaOf(t, nullConfig{})
	for name, raw := range propsOf(t, root) {
		p, _ := raw.(map[string]any)
		if !admitsNull(p) {
			t.Errorf("property %q = %v refuses null, which the decoder accepts", name, p)
		}
	}
	for _, c := range []struct {
		what string
		node any
	}{
		{"list item", prop(t, root, "list")["items"]},
		{"struct list item", prop(t, root, "leaves")["items"]},
		{"map value", prop(t, root, "byName")["additionalProperties"]},
		{"recursive map value", prop(t, prop(t, root, "rec"), "pipelines")["additionalProperties"]},
	} {
		if m, _ := c.node.(map[string]any); !admitsNull(m) {
			t.Errorf("%s = %v refuses null, which the decoder accepts", c.what, c.node)
		}
	}
	// And null is an ADDITION, never a replacement: a nested section is still
	// the strict object it was.
	mustBeStrictObject(t, prop(t, root, "leaf"), "leaf")
}

// A config type the generator does not describe structurally, reached the way
// encoding/json reaches it.
type embeddedInner struct {
	Promoted string `json:"promoted"`
}

type embeddedPtrInner struct {
	Unreachable string `json:"unreachable"`
}

type textAndEmbedConfig struct {
	embeddedInner
	*embeddedPtrInner
	Addr netip.Addr `json:"addr"`
}

// Two encoding/json rules that are not about tags, each of which made the
// schema refuse a document the binary accepts: a TextUnmarshaler decodes from a
// JSON STRING (describing netip.Addr's struct fields refused "10.0.0.1"), and an
// UNEXPORTED embedded struct's exported fields are promoted (skipping the field
// as unexported dropped them, and additionalProperties: false then refused
// them).
func TestTextUnmarshalersAndUnexportedEmbeddingFollowTheDecoder(t *testing.T) {
	dec := json.NewDecoder(strings.NewReader(`{"promoted":"x","addr":"10.0.0.1"}`))
	dec.DisallowUnknownFields()
	var v textAndEmbedConfig
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("fixture: the strict decoder refused the document: %v", err)
	}
	// ...while a pointer to an unexported embedded struct cannot be allocated,
	// so the decoder refuses a key reaching it.
	dec = json.NewDecoder(strings.NewReader(`{"unreachable":"x"}`))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&textAndEmbedConfig{}); err == nil {
		t.Fatal("fixture: the decoder accepted a key behind an embedded pointer to an unexported struct")
	}

	root := schemaOf(t, textAndEmbedConfig{})
	props := propsOf(t, root)
	if typ, _ := typesOf(prop(t, root, "addr")); !typ["string"] || typ["object"] {
		t.Errorf("addr = %v, want a string: a TextUnmarshaler decodes from text, not from its fields", props["addr"])
	}
	if _, ok := props["promoted"]; !ok {
		t.Errorf("the unexported embedded struct's exported field is missing, so the schema refuses a key the decoder accepts: %v", props)
	}
	if _, ok := props["unreachable"]; ok {
		t.Errorf("a field behind an embedded POINTER to an unexported struct is described, but the decoder refuses it: %v", props)
	}
}

func schemaOf(t *testing.T, v any) map[string]any {
	t.Helper()
	b, err := ConfigSchema(v, "t", "d")
	if err != nil {
		t.Fatal(err)
	}
	var root map[string]any
	if err := json.Unmarshal(b, &root); err != nil {
		t.Fatal(err)
	}
	return root
}

func propsOf(t *testing.T, schema map[string]any) map[string]any {
	t.Helper()
	p, ok := schema["properties"].(map[string]any)
	if !ok {
		t.Fatalf("schema has no properties: %v", schema)
	}
	return p
}

func prop(t *testing.T, schema map[string]any, name string) map[string]any {
	t.Helper()
	v, ok := propsOf(t, schema)[name]
	if !ok {
		t.Fatalf("no property %q", name)
	}
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("property %q is %T, want a schema", name, v)
	}
	return m
}

// refTarget resolves a $ref subschema to its definition, failing if the node is
// not a ref at all — which is the regression: an inline {} in its place. A ref
// below the root arrives wrapped in its null arm (anyOf [$ref, null]; see
// nullable), which is unwrapped first.
func refTarget(t *testing.T, root map[string]any, node any, what string) map[string]any {
	t.Helper()
	m, ok := node.(map[string]any)
	if !ok {
		t.Fatalf("%s is %T, want a schema", what, node)
	}
	if arms, ok := m["anyOf"].([]any); ok {
		if len(arms) != 2 || !isNullSchema(arms[1]) {
			t.Fatalf("%s is anyOf %v, want exactly [$ref, {type: null}]", what, arms)
		}
		if m, ok = arms[0].(map[string]any); !ok {
			t.Fatalf("%s: anyOf's first arm is %T, want a schema", what, arms[0])
		}
	}
	r, ok := m["$ref"].(string)
	if !ok {
		t.Fatalf("%s is %v, want a $ref into #/definitions (an inline empty schema accepts anything)", what, m)
	}
	defs, _ := root["definitions"].(map[string]any)
	target, ok := defs[strings.TrimPrefix(r, "#/definitions/")].(map[string]any)
	if !ok {
		t.Fatalf("%s: $ref %q resolves to nothing", what, r)
	}
	return target
}

func mustBeStrictObject(t *testing.T, schema map[string]any, what string) {
	t.Helper()
	if typ, _ := typesOf(schema); !typ["object"] {
		t.Errorf("%s type = %v, want object", what, schema["type"])
	}
	if ap, ok := schema["additionalProperties"].(bool); !ok || ap {
		t.Errorf("%s additionalProperties = %v, want false (UnmarshalStrict's behaviour)", what, schema["additionalProperties"])
	}
}

// typesOf reads a schema's "type" keyword in either spelling (one name, or a
// list of them), reporting whether it had one at all.
func typesOf(schema map[string]any) (map[string]bool, bool) {
	out := map[string]bool{}
	switch v := schema["type"].(type) {
	case string:
		out[v] = true
	case []any:
		for _, e := range v {
			if name, ok := e.(string); ok {
				out[name] = true
			}
		}
	default:
		return out, false
	}
	return out, true
}

func isNullSchema(n any) bool {
	m, ok := n.(map[string]any)
	return ok && len(m) == 1 && m["type"] == "null"
}

// admitsNull reports whether a (non-root) subschema accepts a JSON null in the
// three shapes the generator emits: an unconstrained {}, a type list naming
// null, or the anyOf wrapper around a $ref.
func admitsNull(schema map[string]any) bool {
	if len(schema) == 0 {
		return true
	}
	if typ, ok := typesOf(schema); ok {
		return typ["null"]
	}
	if arms, ok := schema["anyOf"].([]any); ok {
		if slices.ContainsFunc(arms, isNullSchema) {
			return true
		}
	}
	return false
}

// emptySchemas reports the paths of every {} subschema in the document.
func emptySchemas(n any, path string) []string {
	m, ok := n.(map[string]any)
	if !ok {
		if arr, ok := n.([]any); ok {
			var out []string
			for i, v := range arr {
				out = append(out, emptySchemas(v, path+"/"+itoa(i))...)
			}
			return out
		}
		return nil
	}
	if len(m) == 0 {
		return []string{path}
	}
	var out []string
	for k, v := range m {
		out = append(out, emptySchemas(v, path+"/"+k)...)
	}
	return out
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for ; i > 0; i /= 10 {
		b = append([]byte{byte('0' + i%10)}, b...)
	}
	return string(b)
}
