package api_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/daun-gatal/bezisync/backend/internal/compiler"
	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/db"
)

func seedOffsets(t *testing.T, shortID string) {
	t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, db.NormalizeURL(config.Get().EffectiveStateDatabaseURL()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(ctx) }()
	for _, sql := range []string{
		"CREATE SCHEMA IF NOT EXISTS " + compiler.StateSchema,
		fmt.Sprintf(`CREATE TABLE %s."offsets_%s" (id text, offset_key text, offset_val text, record_insert_ts timestamp, record_insert_seq int)`, compiler.StateSchema, shortID),
		fmt.Sprintf(`INSERT INTO %s."offsets_%s" VALUES ('1', '["kafka",{"server":"x"}]', '{"lsn": 100, "ts_usec": 1793003413680478}', now(), 1)`, compiler.StateSchema, shortID),
	} {
		if _, err := c.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		c, err := pgx.Connect(ctx, db.NormalizeURL(config.Get().EffectiveStateDatabaseURL()))
		if err == nil {
			_, _ = c.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s."offsets_%s"`, compiler.StateSchema, shortID))
			_ = c.Close(ctx)
		}
	})
}

func TestOffsetsReadAndResetFlow(t *testing.T) {
	e := newPipelineEnv(t)
	pipe := createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	u := "/api/v1/projects/" + e.pid + "/pipelines/" + pipe.Str("id")
	reset := func(mode, confirm string) *resp {
		return e.admin.post(u+"/offsets/reset", map[string]any{"mode": mode, "confirm": confirm})
	}
	if c := reset("resnapshot", "orders-cdc").Code(); c != "not_deployed" {
		t.Fatal(c)
	}
	e.admin.post(u+"/deploy", map[string]any{}).expect(200)
	e.tick()
	short := strings.ReplaceAll(pipe.Str("id"), "-", "")[:12]
	if l := e.admin.get(u + "/offsets").expect(200).List("offsets"); len(l) != 0 {
		t.Fatal(l)
	}
	seedOffsets(t, short)
	read := e.admin.get(u + "/offsets").expect(200)
	if read.Str("offsets", 0, "position") != "LSN 0/64" {
		t.Fatal(string(read.Raw))
	}
	if c := reset("resnapshot", "orders-cdc").Code(); c != "not_paused" {
		t.Fatal(c)
	}
	e.admin.post(u+"/pause", nil).expect(200)
	e.tick()
	reset("skip_to_now", "wrong").expect(422)
	reset("rewind", "orders-cdc").expect(422)
	res := reset("skip_to_now", "orders-cdc").expect(200)
	kinds := map[any]bool{}
	for _, r := range res.List("results") {
		kinds[at(t, r, "kind")] = true
	}
	if len(kinds) != 2 || !kinds["offsets"] || !kinds["replication_slot"] {
		t.Fatal(string(res.Raw))
	}
	after := e.admin.get(u + "/offsets").expect(200)
	if len(after.List("offsets")) != 0 || after.Str("snapshot_override") != "no_data" {
		t.Fatal(string(after.Raw))
	}
	e.admin.post(u+"/resume", nil).expect(200)
	e.tick()
	workload := func() string {
		for _, w := range e.driver.Workloads {
			return w.Properties
		}
		return ""
	}
	if !strings.Contains(workload(), "debezium.source.snapshot.mode=no_data") {
		t.Fatal(workload())
	}
	e.admin.post(u+"/deploy", map[string]any{}).expect(200)
	e.tick()
	if !strings.Contains(workload(), "debezium.source.snapshot.mode=initial") {
		t.Fatal(workload())
	}
}

func TestVersionDiff(t *testing.T) {
	e := newPipelineEnv(t)
	pipe := createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	u := "/api/v1/projects/" + e.pid + "/pipelines/" + pipe.Str("id")
	// Deploying seals v1, so the edit is v2.
	e.admin.post(u+"/deploy", map[string]any{}).expect(200)
	e.admin.patch(u, map[string]any{"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders", "shop.refunds"}}}}},
		ifMatch(e.admin.get(u).At("version"))...).expect(200)
	d := e.admin.get(u + "/diff?from=1&to=2").expect(200)
	if !strings.HasPrefix(string(d.Raw), `{"from":1,"to":2,`) {
		t.Fatal(string(d.Raw))
	}
	found := false
	for _, l := range d.List("lines") {
		found = found || l == "+    - shop.refunds"
	}
	if !found || d.List("change", "tables_added")[0] != "shop.refunds" {
		t.Fatal(string(d.Raw))
	}
	e.admin.get(u + "/diff?from=1&to=9").expect(404)
	e.admin.get(u + "/diff?from=1").expect(422)
}
