package compiler_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/compiler"
	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/contract"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

type conn [4]json.RawMessage // type, provider, config, secrets

type compileFixture struct {
	PipelineID string                     `json:"pipeline_id"`
	Sources    map[string]conn            `json:"sources"`
	Sinks      map[string]conn            `json:"sinks"`
	Specs      map[string]json.RawMessage `json:"specs"`
	Cases      []struct {
		Source, Sink, Spec string
		Properties         map[string]string `json:"properties"`
		Env                map[string]string `json:"env"`
		Image              string            `json:"image"`
		Resources          map[string]any    `json:"resources"`
		Destinations       map[string]string `json:"destinations"`
		Files              map[string]string `json:"files"`
		Tunnel             json.RawMessage   `json:"tunnel"`
		Rendered           string            `json:"rendered"`
		ConfigHash         string            `json:"config_hash"`
		Problem            *struct {
			Status int              `json:"status"`
			Code   string           `json:"code"`
			Detail string           `json:"detail"`
			Errors []map[string]any `json:"errors"`
		} `json:"problem"`
	} `json:"cases"`
}

func resolved(t *testing.T, c conn) connectors.Resolved {
	t.Helper()
	var typ, provider string
	_ = json.Unmarshal(c[0], &typ)
	_ = json.Unmarshal(c[1], &provider)
	cfg, err := pyjson.DecodeGeneric(c[2])
	if err != nil {
		t.Fatal(err)
	}
	sec, err := pyjson.DecodeGeneric(c[3])
	if err != nil {
		t.Fatal(err)
	}
	return connectors.Resolved{Type: typ, Provider: provider, Config: cfg.(map[string]any), Secrets: sec.(map[string]any)}
}

func TestCompileMatchesPython(t *testing.T) {
	var fx compileFixture
	contract.Load(t, "compile", &fx)
	s, err := config.Load(func(k string) (string, bool) {
		switch k {
		case "BEZISYNC_STATE_DATABASE_URL":
			return "postgresql+psycopg://state_user:p%40ss%3Aw@state-db.internal:5433/bzstate", true
		case "BEZISYNC_AGENT_IMAGE":
			return "ghcr.io/example/bezisync-agent:9.9.9", true
		}
		return "", false
	})
	if err != nil {
		t.Fatal(err)
	}
	config.Set(s)
	t.Cleanup(config.Reset)
	for _, c := range fx.Cases {
		name := c.Source + "/" + c.Sink + "/" + c.Spec
		raw, _ := pyjson.DecodeGeneric(fx.Specs[c.Spec])
		v := &val.V{}
		spec, ok := domain.ParseSpec(v, nil, raw)
		if !ok {
			t.Fatalf("%s: %v", name, v.Errors)
		}
		ctx := connectors.CompileContext{PipelineID: fx.PipelineID, ShortID: "3f2c1d4e5b6a", Name: "orders-cdc", Spec: spec}
		image := ""
		if spec.DebeziumImage != nil {
			image = *spec.DebeziumImage
		}
		got, err := compiler.Compile(ctx, resolved(t, fx.Sources[c.Source]), resolved(t, fx.Sinks[c.Sink]), image)
		if c.Problem != nil {
			var p *problem.Problem
			if !errors.As(err, &p) || p.Status != c.Problem.Status || p.Code != c.Problem.Code || p.Detail != c.Problem.Detail {
				t.Errorf("%s: got %v, want problem %+v", name, err, c.Problem)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if !reflect.DeepEqual(got.Properties, c.Properties) {
			for k, want := range c.Properties {
				if got.Properties[k] != want {
					t.Errorf("%s: property %s = %q, want %q", name, k, got.Properties[k], want)
				}
			}
			for k := range got.Properties {
				if _, ok := c.Properties[k]; !ok {
					t.Errorf("%s: extra property %s", name, k)
				}
			}
		}
		if !reflect.DeepEqual(got.Env, c.Env) {
			t.Errorf("%s: env\n got %v\nwant %v", name, got.Env, c.Env)
		}
		if got.Image != c.Image || !reflect.DeepEqual(got.Destinations, c.Destinations) {
			t.Errorf("%s: image/destinations %q %v, want %q %v", name, got.Image, got.Destinations, c.Image, c.Destinations)
		}
		if len(got.Files) != len(c.Files) {
			t.Errorf("%s: files %v, want %v", name, got.Files, c.Files)
		}
		if got.Render() != c.Rendered {
			t.Errorf("%s: rendered properties differ", name)
		}
		if h := got.ConfigHash(); h != c.ConfigHash {
			t.Errorf("%s: config_hash %s, want %s", name, h, c.ConfigHash)
		}
	}
}
