package services_test

import (
	"encoding/json"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/services"
)

// Agents get a connection's links in the check payload ("linked") and rebuild them.
func TestLinkedFromPayload(t *testing.T) {
	var payload any
	if err := json.Unmarshal([]byte(`{"coordination_kafka": {"type": "kafka", "provider": "self_hosted",
		"config": {"bootstrap_servers": "kafka:9092"}, "secrets": {"password": "pw"}}, "broken": "x"}`), &payload); err != nil {
		t.Fatal(err)
	}
	linked := services.LinkedFromPayload(payload)
	k, ok := linked["coordination_kafka"]
	if !ok || k.Type != "kafka" || k.Config["bootstrap_servers"] != "kafka:9092" || k.Secrets["password"] != "pw" {
		t.Fatalf("%+v", linked)
	}
	if _, ok := linked["broken"]; ok || len(linked) != 1 {
		t.Fatalf("%+v", linked)
	}
	if services.LinkedFromPayload(nil) != nil || services.LinkedFromPayload(map[string]any{}) != nil {
		t.Fatal("no links from an old control plane")
	}
	var noSecrets any
	_ = json.Unmarshal([]byte(`{"f": {"type": "kafka", "config": {}}}`), &noSecrets)
	if s := services.LinkedFromPayload(noSecrets)["f"].Secrets; s == nil {
		t.Fatal("secrets must be a map")
	}
}
