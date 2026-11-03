package compiler_test

import (
	"errors"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

func TestCompileSQS(t *testing.T) {
	sink := connectors.Resolved{Type: "sqs", Provider: "aws", Config: map[string]any{"region": "eu-west-1",
		"queue_url": "https://sqs.eu-west-1.amazonaws.com/123456789012/orders-cdc", "access_key_id": "AKIA"},
		Secrets: map[string]any{"secret_access_key": "SECRET"}}
	got, err := compilePipeline(t, "pg", sink, func(s *domain.PipelineSpec) { s.Sink.Naming = "cdc.{schema}.{table}" })
	if err != nil {
		t.Fatal(err)
	}
	p := got.Properties
	for k, v := range map[string]string{
		"debezium.sink.type":                                  "sqs",
		"debezium.sink.sqs.queue.url":                         "https://sqs.eu-west-1.amazonaws.com/123456789012/orders-cdc",
		"debezium.source.tombstones.on.delete":                "false",
		"debezium.format.value":                               "json",
		`quarkus.log.category."io.debezium.server.sqs".level`: "WARN",
	} {
		if p[k] != v {
			t.Errorf("%s = %q, want %q", k, p[k], v)
		}
	}
	// One queue for every table.
	if got.Destinations["shop.orders"] != "orders-cdc" || got.Destinations["shop.events"] != "orders-cdc" {
		t.Errorf("destinations: %v", got.Destinations)
	}
	if got.Env["AWS_SECRET_ACCESS_KEY"] != "SECRET" {
		t.Errorf("env: %v", got.Env)
	}
	// Empty values fail on SQS: no tombstone deletes, no binary formats.
	for name, mutate := range map[string]func(*domain.PipelineSpec){
		"tombstone deletes": func(s *domain.PipelineSpec) {
			s.Transforms = []domain.Transform{{Type: "unwrap", DeleteHandling: "tombstone", AddFields: []string{"op"}}}
		},
		"avro": func(s *domain.PipelineSpec) {
			reg := "http://registry:8080"
			s.Sink.Format = domain.FormatSpec{Type: "avro", RegistryURL: &reg, RegistryKind: "apicurio"}
		},
	} {
		_, err := compilePipeline(t, "pg", sink, mutate)
		var pr *problem.Problem
		if !errors.As(err, &pr) || (pr.Code != "transform_unsupported" && pr.Code != "format_unsupported") {
			t.Errorf("%s: got %v", name, err)
		}
	}
}

func TestCompileSNS(t *testing.T) {
	sink := connectors.Resolved{Type: "sns", Provider: "aws", Config: map[string]any{"region": "cn-north-1", "account_id": "123456789012"}, Secrets: map[string]any{}}
	got, err := compilePipeline(t, "pg", sink, func(s *domain.PipelineSpec) { s.Sink.Naming = "cdc_{schema}_{table}.fifo" })
	if err != nil {
		t.Fatal(err)
	}
	if got.Properties["debezium.sink.sns.topic.arn.prefix"] != "arn:aws-cn:sns:cn-north-1:123456789012:" || got.Destinations["shop.orders"] != "cdc_shop_orders.fifo" {
		t.Errorf("props %v destinations %v", got.Properties, got.Destinations)
	}
	if _, ok := got.Env["AWS_ACCESS_KEY_ID"]; ok {
		t.Error("no keys: the runtime's IAM role signs")
	}
	_, err = compilePipeline(t, "pg", sink, func(s *domain.PipelineSpec) { s.Sink.Naming = "cdc.{schema}.{table}" })
	var pr *problem.Problem
	if !errors.As(err, &pr) || pr.Code != "naming_invalid" {
		t.Errorf("dots in topic names: %v", err)
	}
}
