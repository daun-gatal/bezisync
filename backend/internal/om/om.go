// Package om is an insertion-ordered JSON object, for documents whose key order is
// visible to users (YAML manifests, Python dict order in responses).
package om

import (
	"bytes"
	"encoding/json"
	"sort"
)

// Map keeps keys in insertion order.
type Map struct {
	keys   []string
	values map[string]any
}

// New creates an empty map.
func New() *Map { return &Map{values: map[string]any{}} }

// Set adds or replaces a key (new keys go last).
func (m *Map) Set(k string, v any) *Map {
	if _, ok := m.values[k]; !ok {
		m.keys = append(m.keys, k)
	}
	m.values[k] = v
	return m
}

// SetFirst adds a key at the front (replacing any existing one).
func (m *Map) SetFirst(k string, v any) *Map {
	m.Delete(k)
	m.keys = append([]string{k}, m.keys...)
	m.values[k] = v
	return m
}

// Get returns a value.
func (m *Map) Get(k string) (any, bool) {
	v, ok := m.values[k]
	return v, ok
}

// Delete removes a key.
func (m *Map) Delete(k string) {
	if _, ok := m.values[k]; !ok {
		return
	}
	delete(m.values, k)
	for i, key := range m.keys {
		if key == k {
			m.keys = append(m.keys[:i], m.keys[i+1:]...)
			break
		}
	}
}

// Pop removes and returns a value.
func (m *Map) Pop(k string) (any, bool) {
	v, ok := m.values[k]
	m.Delete(k)
	return v, ok
}

// Keys in order.
func (m *Map) Keys() []string { return append([]string{}, m.keys...) }

// Len is the number of keys.
func (m *Map) Len() int { return len(m.keys) }

// Plain converts to a regular map (recursively).
func (m *Map) Plain() map[string]any {
	out := make(map[string]any, len(m.keys))
	for _, k := range m.keys {
		out[k] = Plain(m.values[k])
	}
	return out
}

// Plain converts ordered maps inside v to regular maps.
func Plain(v any) any {
	switch x := v.(type) {
	case *Map:
		return x.Plain()
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = Plain(e)
		}
		return out
	}
	return v
}

// MarshalJSON writes keys in order.
func (m *Map) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, k := range m.keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		kb, _ := json.Marshal(k)
		buf.Write(kb)
		buf.WriteByte(':')
		vb, err := json.Marshal(m.values[k])
		if err != nil {
			return nil, err
		}
		buf.Write(vb)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// JSONBKeys sorts keys the way PostgreSQL stores JSONB objects (shorter keys first, then
// bytewise), which is the order Python saw for every dict loaded from the database.
func JSONBKeys(keys []string) []string {
	out := append([]string{}, keys...)
	sort.SliceStable(out, func(i, j int) bool {
		if len(out[i]) != len(out[j]) {
			return len(out[i]) < len(out[j])
		}
		return out[i] < out[j]
	})
	return out
}

// FromJSONB builds an ordered map from a plain map in JSONB key order (recursively).
func FromJSONB(v any) any {
	switch x := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		m := New()
		for _, k := range JSONBKeys(keys) {
			m.Set(k, FromJSONB(x[k]))
		}
		return m
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = FromJSONB(e)
		}
		return out
	}
	return v
}

// DecodeOrdered decodes JSON keeping object key order (objects become *Map, numbers
// json.Number).
func DecodeOrdered(raw []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	return decodeValue(dec)
}

func decodeValue(dec *json.Decoder) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			m := New()
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return nil, err
				}
				v, err := decodeValue(dec)
				if err != nil {
					return nil, err
				}
				m.Set(kt.(string), v)
			}
			if _, err := dec.Token(); err != nil {
				return nil, err
			}
			return m, nil
		case '[':
			out := []any{}
			for dec.More() {
				v, err := decodeValue(dec)
				if err != nil {
					return nil, err
				}
				out = append(out, v)
			}
			if _, err := dec.Token(); err != nil {
				return nil, err
			}
			return out, nil
		}
	}
	return tok, nil
}
