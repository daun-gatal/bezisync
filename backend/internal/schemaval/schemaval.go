// Package schemaval validates data against a Pydantic model's JSON Schema with Pydantic's
// semantics (lax coercion, defaults, ignored extras, identical errors) and returns the
// model_dump(mode="json") of the result.
//
// Connector, secret-store and runtime models are flat Pydantic models; their schemas were
// captured from the Python implementation and are the single source of truth for both the
// UI forms and validation.
package schemaval

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// Schema is a JSON Schema object (decoded with key order kept: *om.Map).
type Schema = *om.Map

func get(s Schema, key string) any {
	v, _ := s.Get(key)
	return v
}

func str(s Schema, key string) string {
	v, _ := get(s, key).(string)
	return v
}

func num(s Schema, key string) *float64 {
	v, ok := s.Get(key)
	if !ok {
		return nil
	}
	f, ok := toFloat(v)
	if !ok {
		return nil
	}
	return &f
}

// Properties returns the model's field names in validation order: Pydantic validates
// fields in declaration order, and the SSH mixin's fields (shown last in the schema)
// are declared first by the base class.
func Properties(model Schema) []string {
	props, _ := get(model, "properties").(*om.Map)
	if props == nil {
		return nil
	}
	var ssh, rest []string
	for _, k := range props.Keys() {
		if strings.HasPrefix(k, "ssh_") {
			ssh = append(ssh, k)
		} else {
			rest = append(rest, k)
		}
	}
	return append(ssh, rest...)
}

// Field returns one property's schema.
func Field(model Schema, name string) Schema {
	props, _ := get(model, "properties").(*om.Map)
	if props == nil {
		return nil
	}
	f, _ := props.Get(name)
	m, _ := f.(*om.Map)
	return m
}

func required(model Schema) map[string]bool {
	out := map[string]bool{}
	if l, ok := get(model, "required").([]any); ok {
		for _, x := range l {
			if s, ok := x.(string); ok {
				out[s] = true
			}
		}
	}
	return out
}

// Model validates raw against a model schema; the result is model_dump(mode="json")
// (every field, defaults filled, values coerced; unknown keys ignored).
func Model(v *val.V, loc val.Loc, model Schema, raw any) (map[string]any, bool) {
	before := v.Count()
	title := str(model, "title")
	o := v.Model(loc, raw, title)
	out := map[string]any{}
	if o.Failed {
		return out, false
	}
	req := required(model)
	for _, name := range Properties(model) {
		fs := Field(model, name)
		f := o.Get(name)
		if !f.Present {
			if req[name] {
				f.Required()
				continue
			}
			def, hasDefault := fs.Get("default")
			switch {
			case hasDefault:
				out[name] = om.Plain(def)
			case str(fs, "type") == "array":
				// default_factory=list: Pydantic's schema shows no default, the model gives [].
				out[name] = []any{}
			default:
				out[name] = nil
			}
			continue
		}
		if value, ok := Value(v, f.Loc, fs, f.Raw); ok {
			out[name] = value
		}
	}
	return out, v.Count() == before
}

// Value validates one value against a field schema.
func Value(v *val.V, loc val.Loc, s Schema, raw any) (any, bool) {
	if anyOf, ok := get(s, "anyOf").([]any); ok {
		var nonNull []Schema
		nullable := false
		for _, alt := range anyOf {
			m, _ := alt.(*om.Map)
			if m == nil {
				continue
			}
			if str(m, "type") == "null" {
				nullable = true
				continue
			}
			nonNull = append(nonNull, m)
		}
		if raw == nil && nullable {
			return nil, true
		}
		if len(nonNull) == 1 {
			return Value(v, loc, nonNull[0], raw)
		}
		// Not used by any model today; validate against the first alternative.
		if len(nonNull) > 0 {
			return Value(v, loc, nonNull[0], raw)
		}
		return raw, true
	}
	if enum, ok := get(s, "enum").([]any); ok {
		allowed := make([]string, 0, len(enum))
		for _, e := range enum {
			if es, ok := e.(string); ok {
				allowed = append(allowed, es)
			}
		}
		return v.Literal(loc, raw, allowed...)
	}
	switch str(s, "type") {
	case "string":
		if str(s, "format") == "email" {
			return v.Email(loc, raw)
		}
		r := val.StrRules{Pattern: str(s, "pattern")}
		if n := num(s, "minLength"); n != nil {
			r.Min = int(*n)
		}
		if n := num(s, "maxLength"); n != nil {
			r.Max = int(*n)
		}
		return v.Constrained(loc, raw, r)
	case "integer":
		return v.IntIn(loc, raw, bounds(s))
	case "number":
		return v.FloatIn(loc, raw, bounds(s))
	case "boolean":
		return v.Bool(loc, raw)
	case "array":
		items, ok := v.List(loc, raw)
		if !ok {
			return nil, false
		}
		itemSchema, _ := get(s, "items").(*om.Map)
		out := make([]any, 0, len(items))
		good := true
		for i, item := range items {
			if itemSchema == nil {
				out = append(out, item)
				continue
			}
			x, ok := Value(v, loc.Add(i), itemSchema, item)
			good = good && ok
			out = append(out, x)
		}
		if !good {
			return out, false
		}
		minItems, maxItems := 0, 0
		if n := num(s, "minItems"); n != nil {
			minItems = int(*n)
		}
		if n := num(s, "maxItems"); n != nil {
			maxItems = int(*n)
		}
		return out, v.ListLen(loc, len(out), minItems, maxItems)
	case "object":
		if _, hasProps := s.Get("properties"); hasProps {
			return Model(v, loc, s, raw)
		}
		m, ok := v.Dict(loc, raw)
		if !ok {
			return nil, false
		}
		ap, _ := get(s, "additionalProperties").(*om.Map)
		if ap == nil {
			return m, true
		}
		out := make(map[string]any, len(m))
		good := true
		for _, k := range sortedKeys(m) {
			x, ok := Value(v, loc.Add(k), ap, m[k])
			good = good && ok
			out[k] = x
		}
		return out, good
	}
	return raw, true
}

func bounds(s Schema) val.Num {
	return val.Num{GE: num(s, "minimum"), LE: num(s, "maximum"), GT: num(s, "exclusiveMinimum"), LT: num(s, "exclusiveMaximum")}
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	case float64:
		return x, true
	case int:
		return float64(x), true
	}
	return 0, false
}

func sortedKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
