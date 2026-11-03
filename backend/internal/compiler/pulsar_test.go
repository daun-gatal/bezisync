package compiler_test

import (
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/connectors"
)

func TestCompilePulsar(t *testing.T) {
	sink := connectors.Resolved{Type: "pulsar", Provider: "self_hosted", Config: map[string]any{"service_url": "pulsar+ssl://broker:6651",
		"admin_url": "https://broker:8443", "tenant": "shop", "namespace": "cdc"}, Secrets: map[string]any{"token": "eyJ.x.y", "ca_cert": "-----BEGIN CERTIFICATE-----"}}
	got, err := compilePipeline(t, "pg", sink, nil)
	if err != nil {
		t.Fatal(err)
	}
	p := got.Properties
	for k, v := range map[string]string{
		"debezium.sink.type":                                "pulsar",
		"debezium.sink.pulsar.client.serviceUrl":            "pulsar+ssl://broker:6651",
		"debezium.sink.pulsar.tenant":                       "shop",
		"debezium.sink.pulsar.namespace":                    "cdc",
		"debezium.sink.pulsar.client.authParams":            "token:${BZ_PULSAR_TOKEN}",
		"debezium.sink.pulsar.client.tlsTrustCertsFilePath": connectors.SecretDir + "/pulsar-ca.pem",
		"debezium.sink.pulsar.producer.blockIfQueueFull":    "true",
	} {
		if p[k] != v {
			t.Errorf("%s = %q, want %q", k, p[k], v)
		}
	}
	if got.Env["BZ_PULSAR_TOKEN"] != "eyJ.x.y" || got.Files["pulsar-ca.pem"] == "" || got.Destinations["shop.orders"] != "shop_orders" {
		t.Errorf("env %v files %v destinations %v", got.Env, got.Files, got.Destinations)
	}
}
