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

import (
	"encoding/json"
	"reflect"
	"strings"
)

// ConfigSchema renders a draft-07 JSON Schema for v's type.
func ConfigSchema(v any, title, description string) ([]byte, error) {
	root := schemaFor(reflect.TypeOf(v), map[reflect.Type]bool{})
	root["$schema"] = "http://json-schema.org/draft-07/schema#"
	root["title"] = title
	root["description"] = description
	return json.MarshalIndent(root, "", "  ")
}

var jsonUnmarshaler = reflect.TypeOf((*json.Unmarshaler)(nil)).Elem()

func schemaFor(t reflect.Type, seen map[reflect.Type]bool) map[string]any {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if seen[t] {
		return map[string]any{} // cycle: anything goes rather than infinite descent
	}
	// A custom unmarshaler decodes by its own rules; describing its FIELDS
	// would reject inputs the binary accepts.
	if t.Implements(jsonUnmarshaler) || reflect.PointerTo(t).Implements(jsonUnmarshaler) {
		return map[string]any{}
	}
	switch t.Kind() {
	case reflect.Struct:
		seen[t] = true
		defer delete(seen, t)
		props := map[string]any{}
		collectStructProps(t, seen, props)
		return map[string]any{
			"type":                 "object",
			"properties":           props,
			"additionalProperties": false, // UnmarshalStrict's behavior
		}
	case reflect.Map:
		return map[string]any{
			"type":                 "object",
			"additionalProperties": schemaFor(t.Elem(), seen),
		}
	case reflect.Slice, reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			return map[string]any{"type": "string"} // []byte: base64 text
		}
		return map[string]any{"type": "array", "items": schemaFor(t.Elem(), seen)}
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

// collectStructProps flattens a struct's exported fields into props,
// descending into untagged embedded structs the way encoding/json does.
func collectStructProps(t reflect.Type, seen map[reflect.Type]bool, props map[string]any) {
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
					collectStructProps(ft, seen, props)
					continue
				}
			}
			tag = f.Name
		}
		props[tag] = schemaFor(f.Type, seen)
	}
}
