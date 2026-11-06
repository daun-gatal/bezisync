package services

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
)

const adoptFile = `debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector
debezium.source.database.hostname=db.internal
debezium.source.database.dbname=shop
debezium.source.database.user=cdc
debezium.source.database.password=plain
debezium.source.table.include.list=public.orders,public.customers
debezium.source.column.exclude.list=public.orders.notes,public.customers.secret_.*
debezium.source.column.mask.with.8.chars=public.customers.email
debezium.source.poll.interval.ms=250
debezium.source.snapshot.max.threads=99
debezium.source.max.queue.size=4096
debezium.source.status.update.interval.ms=5000
debezium.source.some.expert.option=1
debezium.source.converters=geo
debezium.sink.type=kafka
debezium.sink.kafka.producer.bootstrap.servers=k:9092
debezium.sink.kafka.producer.linger.ms=20
quarkus.log.level=DEBUG
`

func keysOf(items []ImportKey) []string {
	out := make([]string, len(items))
	for i, x := range items {
		out[i] = x.Key
	}
	return out
}

func TestPlanImport(t *testing.T) {
	report := Adopt(adoptFile)
	plan := PlanImport(adoptFile, report)
	has := func(items []ImportKey, k string) *ImportKey {
		for i := range items {
			if items[i].Key == k {
				return &items[i]
			}
		}
		return nil
	}
	// Typed settings move into tuning, canonical.
	tuning, _ := plan.Spec["tuning"].(map[string]any)
	if tuning["debezium.source.poll.interval.ms"] != "250" || tuning["debezium.source.status.update.interval.ms"] != "5000" ||
		tuning["debezium.sink.kafka.producer.linger.ms"] != "20" {
		t.Fatal(tuning)
	}
	// C22: out of bounds is never clamped: listed with the bound, left out.
	threads := has(plan.Tunable, "debezium.source.snapshot.max.threads")
	if threads == nil || !strings.Contains(threads.Problem, "1 to 16") || tuning["debezium.source.snapshot.max.threads"] != nil {
		t.Fatal(threads, tuning)
	}
	if lvl := has(plan.Tunable, "quarkus.log.level"); lvl == nil || !strings.Contains(lvl.Problem, "INFO, WARN, ERROR") {
		t.Fatal(lvl)
	}
	// C23: a key a wizard choice (the size) models stays modelled.
	if has(plan.Modelled, "debezium.source.max.queue.size") == nil || has(plan.Tunable, "debezium.source.max.queue.size") != nil {
		t.Fatal(keysOf(plan.Modelled), keysOf(plan.Tunable))
	}
	if has(plan.Owned, "debezium.source.database.password") == nil || has(plan.Expert, "debezium.source.some.expert.option") == nil ||
		has(plan.Forbidden, "debezium.source.converters") == nil {
		t.Fatal(plan)
	}
	// Advanced keeps only what isn't a setting.
	adv, _ := plan.Spec["advanced"].(map[string]any)
	if adv["debezium.source.poll.interval.ms"] != nil || adv["debezium.source.some.expert.option"] != "1" {
		t.Fatal(adv)
	}
	// Excluded and masked columns are mapped (patterns can't be, and say so).
	cols := plan.Spec["source"].(map[string]any)["columns"].(map[string]any)
	orders, customers := cols["public.orders"].(map[string]any), cols["public.customers"].(map[string]any)
	if orders["exclude"].([]any)[0] != "notes" || customers["mask"].([]any)[0] != "email" || len(customers["exclude"].([]any)) != 0 {
		t.Fatal(cols)
	}
	if len(plan.Warnings) != 1 || !strings.Contains(plan.Warnings[0], "public.customers.secret_.*") {
		t.Fatal(plan.Warnings)
	}
	// The spec it makes is one BeziSync accepts.
	if _, err := ParseSpec(plan.Spec); err != nil {
		t.Fatal(err)
	}
	// Adopt's own report is unchanged (the recorded contract): poll interval stays in its spec.
	if !strings.Contains(string(mustJSON(t, report.Spec)), "debezium.source.poll.interval.ms") {
		t.Fatal("Adopt changed")
	}
}

func TestVersionEffects(t *testing.T) {
	saved := engine.DebeziumSettings
	t.Cleanup(func() { engine.DebeziumSettings = saved })
	engine.DebeziumSettings = append(append([]engine.Setting{}, saved...),
		engine.Setting{Engine: engine.Debezium, Scope: engine.ScopeSource, Stage: engine.StageStreaming, Impact: engine.ImpactRestart, Key: "debezium.source.old.name",
			Title: "Old", Type: "int", Default: "1", Min: "0", Max: "9", Until: "3.5", Renamed: "debezium.source.new.name"},
		engine.Setting{Engine: engine.Debezium, Scope: engine.ScopeSource, Stage: engine.StageStreaming, Impact: engine.ImpactRestart, Key: "debezium.source.gone",
			Title: "Gone", Type: "int", Default: "1", Min: "0", Max: "9", Until: "3.6"})
	spec := domain.DefaultSpec()
	spec.Tuning = map[string]string{"debezium.source.old.name": "3", "debezium.source.gone": "1", "debezium.source.skip.messages.without.change": "true", "debezium.source.poll.interval.ms": "250"}
	spec.Advanced = map[string]string{"debezium.source.raw.thing": "x"}
	by := func(effects []SettingEffect) map[string]SettingEffect {
		m := map[string]SettingEffect{}
		for _, e := range effects {
			m[e.Key] = e
		}
		return m
	}
	e := by(VersionEffects(spec, "quay.io/debezium/server:3.7.0.Final"))
	if e["debezium.source.old.name"].Effect != "renamed" || e["debezium.source.old.name"].To != "debezium.source.new.name" || e["debezium.source.old.name"].Blocking {
		t.Fatal(e)
	}
	if !e["debezium.source.gone"].Blocking || e["debezium.source.gone"].Effect != "removed" {
		t.Fatal(e)
	}
	// C24: raw properties are not checked, and say so.
	if e["debezium.source.raw.thing"].Effect != "unchecked" || len(e) != 3 {
		t.Fatal(e)
	}
	// An older release: skip unchanged isn't there yet; the others still are.
	old := by(VersionEffects(spec, "quay.io/debezium/server:1.9.7.Final"))
	if old["debezium.source.skip.messages.without.change"].Effect != "missing" || old["debezium.source.gone"].Effect != "" {
		t.Fatal(old)
	}
	// An image BeziSync can't place: unknown, never fine.
	if u := by(VersionEffects(spec, "registry.example/ours:1")); u["debezium.source.gone"].Effect != "unknown" {
		t.Fatal(u)
	}
	applyRenames(&spec, VersionEffects(spec, "quay.io/debezium/server:3.7.0.Final"))
	if spec.Tuning["debezium.source.new.name"] != "3" || spec.Tuning["debezium.source.old.name"] != "" {
		t.Fatal(spec.Tuning)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// T7: a raw truncation becomes the Truncate step (modelled); T5: only text is cut.
func TestTruncateImportAndChecks(t *testing.T) {
	file := "debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector\n" +
		"debezium.source.table.include.list=public.orders\n" +
		"debezium.source.column.truncate.to.20.chars=public.orders.notes,public.orders.x.*\n"
	plan := PlanImport(file, Adopt(file))
	steps, _ := plan.Spec["transforms"].([]any)
	last, _ := steps[len(steps)-1].(map[string]any)
	if last["type"] != "truncate" || last["columns"].([]any)[0].(map[string]any)["column"] != "notes" || len(plan.Warnings) != 1 {
		t.Fatal(steps, plan.Warnings)
	}
	modelled := false
	for _, k := range plan.Modelled {
		modelled = modelled || k.Key == "debezium.source.column.truncate.to.20.chars"
	}
	if !modelled {
		t.Fatal(plan.Modelled, plan.Forbidden)
	}
	if _, err := ParseSpec(plan.Spec); err != nil {
		t.Fatal(err)
	}
	spec := domain.DefaultSpec()
	spec.Transforms = []domain.Transform{{Type: "truncate", Truncate: []domain.Truncation{{Table: "public.orders", Column: "notes", Length: 5}, {Table: "public.orders", Column: "total", Length: 5}}}}
	found := connectors.Discovery{Tables: []connectors.TableInfo{{Schema: "public", Name: "orders", Columns: []connectors.Column{{Name: "notes", Type: "text"}, {Name: "total", Type: "integer"}}}}}
	if c := truncateChecks(found, spec, false); len(c) != 1 || c[0].Status != "warn" || !strings.Contains(c[0].Message, "total (integer)") {
		t.Fatal(c)
	}
	if c := truncateChecks(found, spec, true); c[0].Status != "fail" || !c[0].Blocking {
		t.Fatal(c)
	}
}
