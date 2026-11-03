package compiler_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

func TestCompileEventHubs(t *testing.T) {
	conn := "Endpoint=sb://shop.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=K3Y="
	sink := connectors.Resolved{Type: "eventhubs", Provider: "azure", Config: map[string]any{"hub_name": "orders-cdc"},
		Secrets: map[string]any{"connection_string": conn}}
	got, err := compilePipeline(t, "pg", sink, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.Properties["debezium.sink.eventhubs.connectionstring"] != "${BZ_EVENTHUBS_CONNECTION}" || got.Properties["debezium.sink.eventhubs.hubname"] != "orders-cdc" ||
		got.Env["BZ_EVENTHUBS_CONNECTION"] != conn || got.Destinations["shop.orders"] != "orders-cdc" {
		t.Errorf("props %v env %v destinations %v", got.Properties, got.Env, got.Destinations)
	}
	for _, v := range got.Properties {
		if strings.Contains(v, "K3Y") {
			t.Fatal("a secret reached application.properties")
		}
	}
	sink.Secrets = map[string]any{"connection_string": conn + ";EntityPath=orders-cdc"}
	_, err = compilePipeline(t, "pg", sink, nil)
	var p *problem.Problem
	if !errors.As(err, &p) || p.Code != "eventhubs_config" {
		t.Fatalf("EntityPath: %v", err)
	}
}
