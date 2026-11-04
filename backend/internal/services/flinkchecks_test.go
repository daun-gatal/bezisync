package services

import (
	"testing"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/models"
)

func TestServerIDCollision(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	lo, hi := idRange(connectors.MyServerIDRange(a.String(), 4))
	same := models.Pipeline{ID: a, Name: "twin", Engine: "flink-cdc"}
	if o := serverIDCollision(lo, hi, []models.Pipeline{same}); o == nil || o.Name != "twin" {
		t.Fatal("a pipeline with the same block collides")
	}
	other := models.Pipeline{ID: b, Name: "other", Engine: "flink-cdc"}
	olo, _ := idRange(connectors.MyServerIDRange(b.String(), 8))
	if (olo == lo) != (serverIDCollision(lo, hi, []models.Pipeline{other}) != nil) {
		t.Fatal("blocks collide only when they are the same block")
	}
	// A Debezium pipeline whose one id falls inside the block.
	deb := models.Pipeline{ID: b, Name: "debezium", Engine: "debezium"}
	id := connectors.MyServerID(b.String())
	if (id >= lo && id <= hi) != (serverIDCollision(lo, hi, []models.Pipeline{deb}) != nil) {
		t.Fatal("a Debezium id collides only inside the block")
	}
	if serverIDCollision(id, id, []models.Pipeline{deb}) == nil {
		t.Fatal("the same id")
	}
}

func TestFlinkAgentAndServicesChecks(t *testing.T) {
	direct := &models.Runtime{Name: "rt", Mode: "direct", Kind: "docker"}
	if flinkAgentCheck(direct) != nil {
		t.Fatal("no agent, no check")
	}
	never := &models.Runtime{Name: "rt", Mode: "agent", Kind: "docker", AgentState: db.Map{}}
	if flinkAgentCheck(never) != nil {
		t.Fatal("a never-connected agent is the runtime check's job")
	}
	old := &models.Runtime{Name: "edge", Mode: "agent", Kind: "kubernetes", AgentState: db.Map{"version": "0.2.0", "features": []any{"health_port"},
		"local_test": map[string]any{"ok": true, "flink_cdc": false}}}
	if c := flinkAgentCheck(old); c == nil || c.Code != "flink_agent_version" || !c.Blocking {
		t.Fatalf("%+v", c)
	}
	if c := flinkServicesCheck(t.Context(), nil, old); c == nil || c.Code != "flink_kubernetes_services" || c.Fix.Kind != "yaml" {
		t.Fatalf("%+v", c)
	}
	current := &models.Runtime{Name: "edge", Mode: "agent", Kind: "kubernetes", AgentState: db.Map{"version": "0.3.0", "features": []any{"health_port", "flink_cdc"},
		"local_test": map[string]any{"ok": true, "flink_cdc": true}}}
	if flinkAgentCheck(current) != nil || flinkServicesCheck(t.Context(), nil, current) != nil {
		t.Fatal("an agent that runs Flink CDC with Services allowed")
	}
}

func TestFlinkTableChecks(t *testing.T) {
	full, def := "full", "default"
	found := connectors.Discovery{Tables: []connectors.TableInfo{
		{Schema: "public", Name: "orders", PrimaryKey: []string{"id"}, ReplicaIdentity: &full},
		{Schema: "public", Name: "events", ReplicaIdentity: &def},
	}}
	spec := domain.DefaultSpec()
	spec.Source.Tables.Include = []string{"public.orders", "public.events"}
	got := flinkTableChecks("postgresql", spec, found)
	if len(got) != 2 || got[0].Code != "flink_primary_key" || !got[0].Blocking || got[1].Code != "flink_replica_identity" ||
		got[1].Fix.Code != `ALTER TABLE "public"."events" REPLICA IDENTITY FULL;` {
		t.Fatalf("%+v", got)
	}
	// A picked key is a key; MySQL logs whole rows anyway.
	spec.Keys = map[string][]string{"public.events": {"event_id"}}
	got = flinkTableChecks("mysql", spec, found)
	if len(got) != 1 || got[0].Status != "pass" {
		t.Fatalf("%+v", got)
	}
}
