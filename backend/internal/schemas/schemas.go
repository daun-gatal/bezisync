// Package schemas holds the JSON Schemas of secret-store, runtime and alert-channel models,
// captured from the Python implementation (the UI renders forms from them and schemaval
// validates against them).
package schemas

import (
	_ "embed"
	"fmt"

	"github.com/daun-gatal/bezisync/backend/internal/om"
)

// Schema is a JSON Schema with key order kept.
type Schema = *om.Map

// Kind is one model family entry.
type Kind struct {
	Kind, Title, Description   string
	ConfigSchema, SecretSchema Schema
}

//go:embed model_schemas.json
var raw []byte

var stores, runtimes, channels []Kind

func init() {
	doc, err := om.DecodeOrdered(raw)
	if err != nil {
		panic(fmt.Sprintf("schemas: %v", err))
	}
	m := doc.(*om.Map)
	load := func(key string) []Kind {
		v, _ := m.Get(key)
		var out []Kind
		for _, e := range v.([]any) {
			em := e.(*om.Map)
			k := Kind{}
			if s, ok := em.Get("kind"); ok {
				k.Kind = s.(string)
			}
			if s, ok := em.Get("title"); ok {
				k.Title, _ = s.(string)
			}
			if s, ok := em.Get("description"); ok {
				k.Description, _ = s.(string)
			}
			c, _ := em.Get("config_schema")
			sc, _ := em.Get("secret_schema")
			k.ConfigSchema, k.SecretSchema = c.(*om.Map), sc.(*om.Map)
			out = append(out, k)
		}
		return out
	}
	stores, runtimes, channels = load("stores"), load("runtimes"), load("channels")
}

func find(list []Kind, kind string) (Kind, bool) {
	for _, k := range list {
		if k.Kind == kind {
			return k, true
		}
	}
	return Kind{}, false
}

// Stores lists secret-store kinds in display order.
func Stores() []Kind { return stores }

// Store returns one secret-store kind.
func Store(kind string) (Kind, bool) { return find(stores, kind) }

// Runtimes lists runtime kinds.
func Runtimes() []Kind { return runtimes }

// Runtime returns one runtime kind.
func Runtime(kind string) (Kind, bool) { return find(runtimes, kind) }

// Channels lists alert-channel kinds.
func Channels() []Kind { return channels }

// Channel returns one alert-channel kind.
func Channel(kind string) (Kind, bool) { return find(channels, kind) }
