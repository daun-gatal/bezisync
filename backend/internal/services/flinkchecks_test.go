package services

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
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

func TestFlinkSlotsCheck(t *testing.T) {
	report := func(used, maxSlots, senders, maxSenders int) checks.Report {
		msg := fmt.Sprintf("Replication slots %d of %d used · WAL senders %d of %d.", used, maxSlots, senders, maxSenders)
		return checks.Report{Checks: []checks.Check{{Code: "pg_capacity", Status: checks.Pass, Message: msg}}}
	}
	spec := func(size, mode string) domain.PipelineSpec {
		s := domain.DefaultSpec()
		s.Policies.Resources, s.Snapshot.Mode = size, mode
		return s
	}
	draft := &models.Pipeline{}
	deployed := &models.Pipeline{DeployedVersionID: &uuid.UUID{}}
	// Medium copies with 4 readers: 1 + 4 slots and senders.
	if c := flinkSlotsCheck(draft, spec("medium", "initial"), report(3, 8, 3, 8)); c == nil || c.Status != checks.Pass || !strings.Contains(c.Message, "5 replication slots") {
		t.Fatal(c)
	}
	c := flinkSlotsCheck(draft, spec("medium", "initial"), report(4, 8, 0, 20))
	if c == nil || c.Status != checks.Fail || !c.Blocking || !strings.Contains(c.Message, "only 4 slots") || !strings.Contains(c.Fix.Code, "max_replication_slots = 13") {
		t.Fatal(c)
	}
	// WAL senders run out first.
	if c := flinkSlotsCheck(draft, spec("large", "initial"), report(0, 30, 27, 30)); c == nil || c.Status != checks.Fail || !strings.Contains(c.Message, "3 WAL senders are free") {
		t.Fatal(c)
	}
	// Streaming from now: only its own slot.
	if c := flinkSlotsCheck(draft, spec("large", "no_data"), report(9, 10, 0, 10)); c == nil || c.Status != checks.Pass || !strings.Contains(c.Message, "1 replication slot and") {
		t.Fatal(c)
	}
	// A redeploy neither creates the slot nor copies.
	if c := flinkSlotsCheck(deployed, spec("large", "initial"), report(10, 10, 10, 10)); c != nil {
		t.Fatal(c)
	}
	// No pg_capacity (it couldn't run): nothing to say here.
	if c := flinkSlotsCheck(draft, spec("small", "initial"), checks.Report{}); c != nil {
		t.Fatal(c)
	}
	// A failing pg_capacity carries its numbers in Technical; negative free counts show as 0.
	failed := checks.Report{Checks: []checks.Check{{Code: "pg_capacity", Status: checks.Fail, Message: "No free replication slot or WAL sender for a new pipeline.",
		Technical: "Replication slots 12 of 10 used · WAL senders 1 of 10."}}}
	if c := flinkSlotsCheck(draft, spec("small", "initial"), failed); c == nil || !strings.Contains(c.Message, "only 0 slots") {
		t.Fatal(c)
	}
}
