package services

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

func jsonMarshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// ToMap converts a struct to a db.Map through JSON (numbers kept exact).
func ToMap(v any) (map[string]any, error) {
	raw, err := jsonMarshal(v)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	var dm = (*mapScanner)(&m)
	return m, dm.scan(raw)
}

type mapScanner map[string]any

func (m *mapScanner) scan(raw []byte) error {
	var out map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&out); err != nil {
		return err
	}
	*m = out
	return nil
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
