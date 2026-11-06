package compiler_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/compiler"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// The compile matrix pins what every source × destination × spec shape compiles to today, as a
// config hash (or the refusal). The controller recompiles running pipelines and replaces any whose
// hash changed, so a change here restarts real pipelines on upgrade: accept one with
// UPDATE_GOLDEN=1 only when the restart is intended (and say so in the changelog).

// matrixValues fills required fields the schemas give no default for.
var matrixValues = map[string]any{
	"host": "db.internal", "hosts": "m1:27017,m2:27017", "database": "app", "user": "cdc", "keyspace": "commerce",
	"project_id": "proj-1", "instance_id": "inst-1", "database_id": "db-1", "coordination_kafka": "k1:9092",
	"bootstrap_servers": "k1:9092,k2:9092", "url": "https://hooks.example.com/cdc", "region": "eu-west-1",
	"warehouse": "s3://lake/warehouse", "queue_url": "https://sqs.eu-west-1.amazonaws.com/123456789012/cdc",
	"account_id": "123456789012", "hub_name": "cdc", "service_url": "pulsar://pulsar:6650", "admin_url": "http://pulsar:8080",
}

// matrixConn is a connection of typ built from its descriptor: schema defaults, the first
// preset's defaults, the values above for required fields, and every secret set.
func matrixConn(t *testing.T, d *connectors.Descriptor) connectors.Resolved {
	t.Helper()
	cfg := map[string]any{}
	props := schemaProps(d.ConfigSchema)
	for _, k := range props.Keys() {
		p, _ := props.Get(k)
		if pm, ok := p.(*om.Map); ok {
			if def, ok := pm.Get("default"); ok && def != nil {
				cfg[k] = def
			}
		}
	}
	provider := ""
	if len(d.Presets) > 0 {
		if pm, ok := d.Presets[0].(*om.Map); ok {
			if id, ok := pm.Get("id"); ok {
				provider = fmt.Sprint(id)
			}
			if defs, ok := pm.Get("defaults"); ok {
				if dm, ok := defs.(*om.Map); ok {
					for _, k := range dm.Keys() {
						v, _ := dm.Get(k)
						cfg[k] = v
					}
				}
			}
		}
	}
	if req, ok := d.ConfigSchema.Get("required"); ok {
		for _, r := range req.([]any) {
			k := fmt.Sprint(r)
			if _, set := cfg[k]; !set {
				v, ok := matrixValues[k]
				if !ok {
					t.Fatalf("%s/%s: no matrix value for required %q", d.Kind, d.Type, k)
				}
				cfg[k] = v
			}
		}
	}
	sec := map[string]any{}
	if d.SecretSchema != nil {
		sp := schemaProps(d.SecretSchema)
		for _, k := range sp.Keys() {
			sec[k] = "s-" + k
		}
	}
	for k, v := range matrixTypeValues[d.Type] {
		cfg[k] = v
	}
	r := connectors.Resolved{Type: d.Type, Provider: provider, Config: cfg, Secrets: sec}
	for field, kindType := range d.Links {
		kind, typ, _ := strings.Cut(kindType, "/")
		ld, err := connectors.Describe(kind, typ)
		if err != nil {
			t.Fatal(err)
		}
		r.Linked = map[string]connectors.Resolved{field: matrixConn(t, ld)}
	}
	return r
}

// matrixTypeValues are per-connector values the generic ones above don't satisfy.
var matrixTypeValues = map[string]map[string]any{
	"nats":    {"url": "nats://nats:4222"},
	"iceberg": {"uri": "https://catalog.example.com/api/catalog"},
}

func schemaProps(m *om.Map) *om.Map {
	if m == nil {
		return om.New()
	}
	p, ok := m.Get("properties")
	if !ok {
		return om.New()
	}
	pm, _ := p.(*om.Map)
	if pm == nil {
		return om.New()
	}
	return pm
}

// Spec shapes: the defaults, everything Debezium-side a spec can hold, and a bigger size.
var debeziumMatrixSpecs = map[string]map[string]any{
	"basic": {"source": map[string]any{"tables": map[string]any{"include": []any{"public.orders", "public.customers"}}}},
	"rich": {
		"source": map[string]any{"tables": map[string]any{"include": []any{"public.users", "public.orders"}},
			"columns": map[string]any{"public.users": map[string]any{"mask": []any{"email"}, "exclude": []any{"ssn"}}}},
		"snapshot":   map[string]any{"mode": "initial", "chunk_size": 4096, "fetch_size": 500, "delay_seconds": 30},
		"transforms": []any{map[string]any{"type": "filter", "operations": []any{"insert", "update"}}, map[string]any{"type": "unwrap", "delete_handling": "rewrite", "add_fields": []any{"op"}}},
		"policies":   map[string]any{"heartbeat_seconds": 30, "resources": "large", "errors": map[string]any{"retries": 3, "on_poison_event": "fail"}, "delivery_canary": true},
		"advanced":   map[string]any{"debezium.source.poll.interval.ms": "250"},
	},
	"medium": {"source": map[string]any{"tables": map[string]any{"include": []any{"public.orders"}}}, "policies": map[string]any{"resources": "medium"}},
}

var flinkMatrixSpecs = map[string]map[string]any{
	"basic": {"source": map[string]any{"tables": map[string]any{"include": []any{"shop.orders", "shop.customers"}}}},
	"large": {"source": map[string]any{"tables": map[string]any{"include": []any{"shop.orders"}}},
		"policies": map[string]any{"resources": "large"},
		"flink_cdc": map[string]any{"schema_changes": "ignore", "new_columns": "hold", "commit_seconds": 300,
			"flink_conf": map[string]any{"execution.checkpointing.timeout": "15 min"}}},
}

// matrixFlinkInput gives Flink CDC the tables' columns, as a deploy does.
var matrixFlinkInput = func() compiler.FlinkInput {
	in := secrets
	in.Columns = map[string][]string{"shop.orders": {"id", "total", "created_at"}, "shop.customers": {"id", "email"}}
	in.Defaults = map[string][]string{}
	return in
}()

// flinkOnly are destinations only Flink CDC writes to.
var flinkOnly = map[string]bool{"starrocks": true, "doris": true, "fluss": true}

func withNaming(raw map[string]any, naming string) map[string]any {
	out := map[string]any{}
	for k, v := range raw {
		out[k] = v
	}
	sink := map[string]any{"naming": naming}
	if s, ok := raw["sink"].(map[string]any); ok {
		for k, v := range s {
			sink[k] = v
		}
		sink["naming"] = naming
	}
	out["sink"] = sink
	return out
}

func matrixSpec(t *testing.T, raw map[string]any) (domain.PipelineSpec, error) {
	t.Helper()
	b, _ := json.Marshal(raw)
	generic, err := pyjson.DecodeGeneric(b) // ints stay ints, as the API decodes them
	if err != nil {
		t.Fatal(err)
	}
	v := &val.V{}
	spec, ok := domain.ParseSpec(v, nil, generic)
	if !ok {
		return spec, fmt.Errorf("spec: %s", specErrors(v))
	}
	return spec, nil
}

// specErrors is a spec's validation errors without the pointers %v would print.
func specErrors(v *val.V) string {
	msgs := make([]string, 0, len(v.Errors))
	for _, e := range v.Errors {
		msgs = append(msgs, strings.Join(e.Loc, ".")+": "+e.Message)
	}
	return strings.Join(msgs, "; ")
}

func outcome(c *compiler.Compiled, err error) string {
	if err != nil {
		var p *problem.Problem
		if errors.As(err, &p) {
			return fmt.Sprintf("refused %d %s: %s", p.Status, p.Code, p.Detail)
		}
		return "error: " + err.Error()
	}
	return c.ConfigHash()
}

func TestCompileMatrixUnchanged(t *testing.T) {
	flinkConfig(t)
	var sources, sinks []*connectors.Descriptor
	for _, d := range connectors.Descriptors() {
		if d.Type == "flinktest" {
			continue
		}
		if d.Kind == "source" {
			sources = append(sources, d)
		} else {
			sinks = append(sinks, d)
		}
	}
	got := map[string]string{}
	ctx := func(spec domain.PipelineSpec) connectors.CompileContext {
		return connectors.CompileContext{PipelineID: "3f2c1d4e-5b6a-4789-8abc-0123456789ab", ShortID: "3f2c1d4e5b6a", Name: "orders-cdc", Spec: spec}
	}
	for _, src := range sources {
		for _, snk := range sinks {
			for name, raw := range debeziumMatrixSpecs {
				if flinkOnly[snk.Type] {
					break
				}
				if snk.Type == "sns" { // SNS topic names allow no dots
					raw = withNaming(raw, "{schema}_{table}")
				}
				key := "debezium/" + src.Type + "/" + snk.Type + "/" + name
				spec, err := matrixSpec(t, raw)
				if err != nil {
					got[key] = err.Error()
					continue
				}
				got[key] = outcome(compiler.Compile(ctx(spec), matrixConn(t, src), matrixConn(t, snk), ""))
			}
			if !slices.Contains([]string{"mysql", "postgresql", "sqlserver"}, src.Type) || !slices.Contains([]string{"iceberg", "starrocks", "doris", "fluss"}, snk.Type) {
				continue
			}
			for name, raw := range flinkMatrixSpecs {
				key := "flink-cdc/" + src.Type + "/" + snk.Type + "/" + name
				spec, err := matrixSpec(t, raw)
				if err != nil {
					got[key] = err.Error()
					continue
				}
				f, err := compiler.CompileFlink(ctx(spec), matrixConn(t, src), matrixConn(t, snk), "flink-cdc:3.7.0-2.2.1", matrixFlinkInput)
				if err != nil {
					got[key] = outcome(nil, err)
					continue
				}
				got[key] = outcome(compiler.FromFlink(f), nil)
			}
		}
	}
	path := filepath.Join("..", "..", "testdata", "compile_matrix.json") // outside the image (.dockerignore)
	encoded, _ := json.MarshalIndent(got, "", " ")
	encoded = append(encoded, '\n')
	if os.Getenv("UPDATE_GOLDEN") != "" {
		if err := os.WriteFile(path, encoded, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile(path) //nolint:gosec // test data
	if err != nil {
		t.Fatal(err)
	}
	var want map[string]string
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	for k, w := range want {
		if g, ok := got[k]; !ok {
			t.Errorf("%s: no longer compiled", k)
		} else if g != w {
			t.Errorf("%s: %s, want %s (a running pipeline like this would be replaced on upgrade)", k, g, w)
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Errorf("%s: new case, not in testdata/compile_matrix.json (UPDATE_GOLDEN=1 adds it)", k)
		}
	}
}
