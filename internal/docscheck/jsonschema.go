package docscheck

// JSON-Schema generation for the agent's -config YAML, by reflection over the
// very structs the file decodes into. The config decodes through
// sigs.k8s.io/yaml → encoding/json with UnmarshalStrict, so the schema's
// additionalProperties: false is not a stylistic choice — it is what the
// binary actually enforces, and an editor validating against the generated
// schema (yaml-language-server) rejects exactly the typo the binary would.
//
// Two deliberate loosenings keep the schema honest rather than optimistic:
// a type with its own UnmarshalJSON decodes by rules reflection cannot see,
// so it renders as an unconstrained schema; and every field is optional,
// because required-ness in this config is semantic (validated by
// -check-config) rather than structural.
//
// There is NOT a third. A RECURSIVE type — attrs.Config, whose `pipelines` map
// holds more attrs.Config; tailsample.PolicyConfig, whose `and`/`composite`
// arms hold more PolicyConfig — renders once into "definitions" and by "$ref"
// on re-entry, so the recursive subtrees carry the same
// additionalProperties: false as everything else. Emitting an unconstrained {}
// there instead (the obvious way to stop an infinite descent, and what this
// generator did) quietly made the schema accept ANY object at exactly three
// nodes: per-pipeline resourceAttributes overrides and both flavours of
// tail-sampling sub-policy — hand-written surfaces, deeply nested, and so
// among the likeliest places to typo a key. A typo there passed CI and then
// killed every agent on the strict decoder. If a future change needs a bail-out
// for a shape $ref cannot express, say so here and delete the parity claim
// above with it; do not leave the claim standing over a hole.

import (
	"encoding/json"
	"reflect"
	"strings"
)

// ConfigSchema renders a draft-07 JSON Schema for v's type.
func ConfigSchema(v any, title, description string) ([]byte, error) {
	c := &schemaCtx{
		seen:  map[reflect.Type]bool{},
		names: map[reflect.Type]string{},
		defs:  map[string]any{},
	}
	root := c.schemaFor(reflect.TypeOf(v))
	root["$schema"] = "http://json-schema.org/draft-07/schema#"
	root["title"] = title
	root["description"] = description
	if len(c.defs) > 0 {
		// "definitions" rather than "$defs": the document declares draft-07,
		// and $defs is draft 2019-09's spelling of the same thing.
		root["definitions"] = c.defs
	}
	return json.MarshalIndent(root, "", "  ")
}

var jsonUnmarshaler = reflect.TypeOf((*json.Unmarshaler)(nil)).Elem()

// schemaCtx carries the walk's state: seen is the ANCESTOR path (a type is on
// it only while its own subtree is being rendered), names/defs are the shared
// registry of recursive types, which outlives any single branch.
type schemaCtx struct {
	seen  map[reflect.Type]bool
	names map[reflect.Type]string
	defs  map[string]any
}

func (c *schemaCtx) schemaFor(t reflect.Type) map[string]any {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if c.seen[t] {
		// A cycle. Render the type ONCE under definitions and point at it, so
		// the recursive subtree keeps additionalProperties: false; returning an
		// empty schema here would accept any object at this node (see the
		// package doc). Only a struct can be on the path — every other kind
		// returns before seen is written — so define() always has fields.
		return c.define(t)
	}
	// A custom unmarshaler decodes by its own rules; describing its FIELDS
	// would reject inputs the binary accepts.
	if t.Implements(jsonUnmarshaler) || reflect.PointerTo(t).Implements(jsonUnmarshaler) {
		return map[string]any{}
	}
	switch t.Kind() {
	case reflect.Struct:
		c.seen[t] = true
		defer delete(c.seen, t)
		return c.structSchema(t)
	case reflect.Map:
		return map[string]any{
			"type":                 "object",
			"additionalProperties": c.schemaFor(t.Elem()),
		}
	case reflect.Slice, reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			return map[string]any{"type": "string"} // []byte: base64 text
		}
		return map[string]any{"type": "array", "items": c.schemaFor(t.Elem())}
	case reflect.String:
		return map[string]any{"type": "string"}
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"type": "integer"}
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}
	default:
		return map[string]any{} // interface{} and anything exotic: any
	}
}

// structSchema renders t's fields as an object schema. The caller has already
// put t on the ancestor path.
func (c *schemaCtx) structSchema(t reflect.Type) map[string]any {
	props := map[string]any{}
	c.collectStructProps(t, props)
	return map[string]any{
		"type":                 "object",
		"properties":           props,
		"additionalProperties": false, // UnmarshalStrict's behavior
	}
}

// define registers t under definitions (once) and returns a $ref to it.
//
// The body is rendered with t ALONE on the path, so the inner re-entry lands
// back here, finds the name already reserved and emits a $ref instead of
// descending — which is what terminates. The reservation must happen BEFORE
// the body is built, or that inner call would recurse forever.
func (c *schemaCtx) define(t reflect.Type) map[string]any {
	name, ok := c.names[t]
	if !ok {
		name = defName(t)
		for other, taken := range c.names {
			if taken == name && other != t {
				// Two identically named types from different packages. Rare
				// enough that the config tree has none today, but a silent
				// merge would describe one type with the other's fields.
				name = t.PkgPath() + "." + t.Name()
				break
			}
		}
		c.names[t] = name
		c.defs[name] = nil // reserve first: the render below re-enters t
		outer := c.seen
		c.seen = map[reflect.Type]bool{t: true}
		c.defs[name] = c.structSchema(t)
		c.seen = outer
	}
	return map[string]any{"$ref": "#/definitions/" + jsonPointerEscape(name)}
}

// defName is the definition key for t: "pkg.Type", the same string Go prints.
func defName(t reflect.Type) string {
	if t.Name() == "" {
		return "anonymous" // unreachable for a named config type; kept total
	}
	return t.String()
}

// jsonPointerEscape escapes the two characters a JSON-pointer token may not
// carry literally (RFC 6901). A Go package path can contain "/", which the
// duplicate-name fallback above puts into a key.
func jsonPointerEscape(s string) string {
	s = strings.ReplaceAll(s, "~", "~0")
	return strings.ReplaceAll(s, "/", "~1")
}

// collectStructProps flattens a struct's exported fields into props,
// descending into untagged embedded structs the way encoding/json does.
func (c *schemaCtx) collectStructProps(t reflect.Type, props map[string]any) {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		tag := strings.Split(f.Tag.Get("json"), ",")[0]
		if tag == "-" {
			continue
		}
		if tag == "" {
			if f.Anonymous {
				ft := f.Type
				for ft.Kind() == reflect.Pointer {
					ft = ft.Elem()
				}
				if ft.Kind() == reflect.Struct {
					c.collectStructProps(ft, props)
					continue
				}
			}
			tag = f.Name
		}
		props[tag] = c.schemaFor(f.Type)
	}
}
