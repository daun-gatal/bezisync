package api_test

import (
	"fmt"
	"testing"
)

// A Spanner source names a Kafka connection of its project (its coordination Kafka): given by
// name or id, it is stored by id, and that Kafka connection cannot be deleted while it is linked.
func TestLinkedConnections(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	base := "/api/v1/projects/" + defaultProject(t, admin)
	kafka := admin.post(base+"/connections", map[string]any{"kind": "sink", "type": "kafka", "name": "coord-kafka",
		"config": map[string]any{"bootstrap_servers": "kafka.internal:9092"}}).expect(201)
	kafkaID := kafka.Str("id")
	pg := admin.post(base+"/connections", sourceBody(t, "not-kafka")).expect(201)

	spanner := func(name, link string) map[string]any {
		return map[string]any{"kind": "source", "type": "spanner", "name": name, "provider": "gcp",
			"config": map[string]any{"project_id": "p", "instance_id": "i", "database_id": "d", "coordination_kafka": link, "emulator_host": "spanner-emulator:9010"}}
	}
	byName := admin.post(base+"/connections", spanner("orders-spanner", "coord-kafka")).expect(201)
	if got := byName.Str("config", "coordination_kafka"); got != kafkaID {
		t.Fatalf("stored link %q, want the Kafka connection's id %s", got, kafkaID)
	}
	byID := admin.post(base+"/connections", spanner("orders-spanner-2", kafkaID)).expect(201)
	if got := byID.Str("config", "coordination_kafka"); got != kafkaID {
		t.Fatalf("stored link %q", got)
	}

	// Only a Kafka connection of this project will do.
	for _, link := range []string{"not-kafka", pg.Str("id"), "nope", "00000000-0000-0000-0000-000000000000"} {
		r := admin.post(base+"/connections", spanner("bad-"+randHex(3), link)).expect(422)
		if r.Code() != "linked_connection_invalid" {
			t.Fatalf("%s: %s", link, r.Code())
		}
	}

	// Editing to a link that is not a Kafka connection of the project is refused.
	id := byName.Str("id")
	version := byName.At("version")
	r := admin.patch(base+"/connections/"+id, map[string]any{"config": map[string]any{"project_id": "p", "instance_id": "i", "database_id": "d",
		"coordination_kafka": "not-kafka", "emulator_host": "spanner-emulator:9010"}}, "If-Match", fmt.Sprintf(`"%v"`, version)).expect(422)
	if r.Code() != "linked_connection_invalid" {
		t.Fatal(r.Code())
	}

	// A linked Kafka connection cannot be deleted.
	r = admin.del(base + "/connections/" + kafkaID).expect(409)
	if r.Code() != "connection_in_use" || len(r.List("connections")) != 2 {
		t.Fatalf("delete: %v", r.JSON())
	}
	admin.del(base + "/connections/" + id).expect(204)
	admin.del(base + "/connections/" + byID.Str("id")).expect(204)
	admin.del(base + "/connections/" + kafkaID).expect(204)

	// The connector types describe the link, and Spanner's capture.
	for _, ct := range admin.get(base + "/connector-types").expect(200).List() {
		m := ct.(map[string]any)
		if m["type"] != "spanner" {
			continue
		}
		props := m["config_schema"].(map[string]any)["properties"].(map[string]any)
		if props["coordination_kafka"].(map[string]any)["ui"].(map[string]any)["connection"] != "sink/kafka" {
			t.Fatal("coordination_kafka is not a linked field")
		}
		capture := m["capture"].(map[string]any)
		if len(capture["snapshot_modes"].([]any)) != 0 || capture["incremental"] != false || capture["canary"] != false {
			t.Fatalf("capture: %v", capture)
		}
	}
}

// Deleted pipelines keep their rows, which still name their connections: deleting such a
// connection is a clear conflict, not a database error.
func TestConnectionOfDeletedPipelineCannotBeDeleted(t *testing.T) {
	e := newPipelineEnv(t)
	pipe := createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	exec(t, fmt.Sprintf(`UPDATE pipelines SET deleted_at = now() WHERE id = '%s'`, pipe.Str("id")))
	r := e.admin.del("/api/v1/projects/" + e.pid + "/connections/" + e.ids.Source).expect(409)
	if r.Code() != "connection_in_history" || r.At("deleted_pipelines") != float64(1) {
		t.Fatalf("%v", r.JSON())
	}
}
