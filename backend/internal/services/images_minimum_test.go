package services_test

import (
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/services"
)

func TestMinimumDebeziumCheck(t *testing.T) {
	crdb, err := connectors.Describe("source", "cockroachdb")
	if err != nil || crdb.Capture.MinDebezium != "3.7.0" {
		t.Fatalf("%v %+v", err, crdb)
	}
	cases := map[string]string{
		"quay.io/debezium/server:3.7.0.Final":   "",
		"quay.io/debezium/server:3.8.1.Final":   "",
		"quay.io/debezium/server:4.0.0.Alpha1":  "",
		"debezium/server:3.7.0.Final":           "",
		"quay.io/debezium/server:3.6.3.Final":   checks.Fail,
		"quay.io/debezium/server:3.7.0.CR1":     checks.Fail,
		"quay.io/debezium/server:2.7.4.Final":   checks.Fail,
		"registry.example.com/dbz:custom":       checks.Warn,
		"quay.io/debezium/server@sha256:abc123": checks.Warn,
	}
	for image, want := range cases {
		got := services.MinimumDebeziumCheck(image, crdb)
		switch {
		case want == "" && got != nil:
			t.Errorf("%s: %+v", image, got)
		case want != "" && (got == nil || got.Status != want || got.Blocking != (want == checks.Fail)):
			t.Errorf("%s: %+v, want %s", image, got, want)
		}
	}
	pg, _ := connectors.Describe("source", "postgresql")
	if c := services.MinimumDebeziumCheck("quay.io/debezium/server:3.0.0.Final", pg); c != nil {
		t.Errorf("a source without a minimum: %+v", c)
	}
}
