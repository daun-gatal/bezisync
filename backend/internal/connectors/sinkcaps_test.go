package connectors

import (
	"errors"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

func problemCode(err error) string {
	var p *problem.Problem
	if errors.As(err, &p) {
		return p.Code
	}
	if err != nil {
		return err.Error()
	}
	return ""
}

func capsFrom(t *testing.T, raw string) SinkCaps {
	t.Helper()
	v, err := om.DecodeOrdered([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	return parseSinkCaps(v.(*om.Map))
}

func TestSinkCapsDefaults(t *testing.T) {
	d := om.New().Set("kind", "sink")
	c := parseSinkCaps(d)
	if c.Naming.Default != DefaultNaming || c.Naming.Explicit || len(c.Formats) != 4 || !c.Outbox || c.Unwrap != nil {
		t.Fatalf("defaults: %+v", c)
	}
	if _, ok := d.Get("sink"); !ok {
		t.Fatal("connector-types must show the sink capabilities")
	}
	// The first sinks keep their behaviour: only the template is checked.
	if err := c.CheckDestinations("Kafka", map[string]string{"shop.Order Items": "cdc.shop.Order Items"}); err != nil {
		t.Fatal(err)
	}
}

func TestSinkCapsNaming(t *testing.T) {
	sns := capsFrom(t, `{"sink": {"naming": {"default": "cdc_{schema}_{table}", "pattern": "^[A-Za-z0-9_-]+$", "max_length": 20,
		"message": "Use letters, digits, '_' and '-'."}}}`)
	if code := problemCode(sns.CheckNaming("SNS", "cdc.{schema}.{table}")); code != "naming_invalid" {
		t.Errorf("dots: %q", code)
	}
	if err := sns.CheckNaming("SNS", "cdc_{schema}_{table}"); err != nil {
		t.Error(err)
	}
	if code := problemCode(sns.CheckDestinations("SNS", map[string]string{"shop.orders": "cdc_shop_orders_with_a_long_name"})); code != "naming_invalid" {
		t.Errorf("length: %q", code)
	}
	if code := problemCode(sns.CheckDestinations("SNS", map[string]string{"shop.order$": "cdc_shop_order$"})); code != "naming_invalid" {
		t.Errorf("rendered characters: %q", code)
	}
	iceberg := capsFrom(t, `{"sink": {"naming": {"default": "{schema}_{table}", "pattern": "^[a-z0-9_]+$", "flatten_dots": true}}}`)
	if err := iceberg.CheckNaming("Iceberg", "{schema}.{table}"); err != nil {
		t.Errorf("flattened template: %v", err)
	}
	if err := iceberg.CheckDestinations("Iceberg", map[string]string{"shop.orders": "shop.orders"}); err != nil {
		t.Errorf("flattened dots: %v", err)
	}
	single := capsFrom(t, `{"sink": {"naming": {"single": true, "pattern": "^x$"}}}`)
	if err := single.CheckNaming("SQS", "anything.{table}"); err != nil {
		t.Errorf("single destination ignores naming: %v", err)
	}
}

func TestSinkCapsTransformsAndFormats(t *testing.T) {
	c := capsFrom(t, `{"sink": {"formats": ["json", "cloudevents"], "outbox": false,
		"unwrap": {"required": false, "add_fields": [], "delete_handling": ["rewrite", "drop"]}}}`)
	if code := problemCode(c.CheckTransforms("SQS", []domain.Transform{{Type: "outbox"}})); code != "transform_unsupported" {
		t.Errorf("outbox: %q", code)
	}
	if code := problemCode(c.CheckTransforms("SQS", []domain.Transform{{Type: "unwrap", DeleteHandling: "tombstone"}})); code != "transform_unsupported" {
		t.Errorf("tombstone deletes: %q", code)
	}
	if err := c.CheckTransforms("SQS", []domain.Transform{{Type: "unwrap", DeleteHandling: "drop"}, {Type: "filter"}}); err != nil {
		t.Error(err)
	}
	if code := problemCode(func() error { _, err := c.EventFormat("SQS", "avro"); return err }()); code != "format_unsupported" {
		t.Errorf("avro: %q", code)
	}
	forced := capsFrom(t, `{"sink": {"formats": ["connect"]}}`)
	if f, err := forced.EventFormat("Iceberg", "json"); err != nil || f != "connect" {
		t.Errorf("forced: %q %v", f, err)
	}
}

func TestSinkCapsOptions(t *testing.T) {
	c := capsFrom(t, `{"sink": {"options_schema": {"properties": {
		"mode": {"default": "upsert", "enum": ["upsert", "append"], "title": "Mode", "type": "string"}}, "title": "Options", "type": "object"}}}`)
	got, err := c.Options(nil)
	if err != nil || got["mode"] != "upsert" {
		t.Fatalf("defaults: %v %v", got, err)
	}
	if code := problemCode(func() error { _, err := c.Options(map[string]any{"mode": "merge"}); return err }()); code != "validation_failed" {
		t.Errorf("bad value: %q", code)
	}
	none := capsFrom(t, `{}`)
	if got, err := none.Options(map[string]any{"legacy": true}); err != nil || len(got) != 0 {
		t.Errorf("sinks without options ignore old keys: %v %v", got, err)
	}
}

func TestIcebergRustFSTakesChangeLogsOnly(t *testing.T) {
	sink := &icebergSink{}
	// No catalog URL: the check stops before contacting a catalog.
	conn := Resolved{Type: "iceberg", Provider: "rustfs", Config: map[string]any{"catalog_type": "rest", "warehouse": "lake"}}
	found := func(opts map[string]any) bool {
		for _, c := range sink.CheckPipeline(t.Context(), conn, CompileContext{}, PipelineFacts{Options: opts}) {
			if c.Code == "iceberg_rustfs_upsert" && c.Blocking {
				return true
			}
		}
		return false
	}
	if !found(map[string]any{"write_mode": "upsert_keep_deletes"}) || found(map[string]any{"write_mode": "append"}) {
		t.Fatal("RustFS: upserts blocked, change logs allowed")
	}
}
