package pyjson

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// DumpsIndent is json.dumps(value, indent=n, sort_keys=True) (ensure_ascii, separators
// "," and ": ") for generic JSON values (maps, slices, json.Number, scalars). Other values
// go through their JSON form first.
func DumpsIndent(v any, n int) (string, error) {
	switch v.(type) {
	case nil, bool, string, json.Number, map[string]any, []any:
	default:
		g, err := ToGeneric(v)
		if err != nil {
			return "", err
		}
		v = g
	}
	var b strings.Builder
	if err := indent(&b, v, n, 0); err != nil {
		return "", err
	}
	return b.String(), nil
}

func indent(b *strings.Builder, v any, n, depth int) error {
	pad := func(d int) string { return "\n" + strings.Repeat(" ", n*d) }
	switch x := v.(type) {
	case map[string]any:
		if len(x) == 0 {
			b.WriteString("{}")
			return nil
		}
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(pad(depth + 1))
			writeString(b, k, true)
			b.WriteString(": ")
			if err := indent(b, x[k], n, depth+1); err != nil {
				return err
			}
		}
		b.WriteString(pad(depth))
		b.WriteByte('}')
	case []any:
		if len(x) == 0 {
			b.WriteString("[]")
			return nil
		}
		b.WriteByte('[')
		for i, item := range x {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(pad(depth + 1))
			if err := indent(b, item, n, depth+1); err != nil {
				return err
			}
		}
		b.WriteString(pad(depth))
		b.WriteByte(']')
	case nil, bool, string, json.Number:
		s, err := Encode(x, Options{EnsureASCII: true})
		if err != nil {
			return err
		}
		b.WriteString(s)
	default:
		return fmt.Errorf("pyjson: unsupported type %T", v)
	}
	return nil
}
