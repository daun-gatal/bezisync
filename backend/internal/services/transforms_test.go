package services_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/services"
)

func TestTransformDriftCondition(t *testing.T) {
	v := uuid.New()
	p := &models.Pipeline{Name: "orders", DeployedVersionID: &v, Status: db.Map{"transform_drift": []any{
		map[string]any{"code": "transform_column_missing", "fail": false, "message": "public.orders, step 3 (rename): No field cust_id at this point: nothing to rename."},
		map[string]any{"code": "transform_field_conflict", "fail": true, "message": "boom."},
	}}}
	c := services.Evaluate("transform_drift", p, map[string]any{}, db.Now())
	if !c.Active || c.Severity != "critical" || c.Title != "Transform needs attention on orders" ||
		c.Message != "public.orders, step 3 (rename): No field cust_id at this point: nothing to rename. (and 1 more)" {
		t.Fatalf("%+v", c)
	}
	p.Status = db.Map{"transform_drift": []any{map[string]any{"message": "x", "fail": false}}}
	if c := services.Evaluate("transform_drift", p, map[string]any{}, db.Now()); !c.Active || c.Severity != "" {
		t.Fatalf("%+v", c)
	}
	for _, status := range []db.Map{{}, {"transform_drift": []any{}}} {
		p.Status = status
		if services.Evaluate("transform_drift", p, map[string]any{}, db.Now()).Active {
			t.Fatalf("active for %v", status)
		}
	}
	p.Status, p.DeployedVersionID = db.Map{"transform_drift": []any{map[string]any{"message": "x"}}}, nil
	if services.Evaluate("transform_drift", p, map[string]any{}, db.Now()).Active {
		t.Fatal("active for an undeployed pipeline")
	}
}

const adoptBase = `debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector
debezium.source.topic.prefix=shop
debezium.source.table.include.list=public.orders,public.audit_log
debezium.sink.type=kafka
`

func adopt(t *testing.T, extra string) (string, []string) {
	t.Helper()
	r := services.Adopt(adoptBase + extra)
	spec, err := json.Marshal(r.Spec)
	if err != nil {
		t.Fatal(err)
	}
	// The mapped spec must be a valid spec.
	raw, _ := pyjson.DecodeGeneric(spec)
	if _, err := services.ParseSpec(raw); err != nil {
		t.Fatalf("adopted spec is invalid: %v\n%s", err, spec)
	}
	return string(spec), r.Warnings
}

func TestAdoptMapsKnownTransforms(t *testing.T) {
	spec, warnings := adopt(t, `debezium.transforms=filter,unwrap,ren,add,flat,route,smt
debezium.transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState
debezium.transforms.unwrap.add.fields=op, source.ts_ms
debezium.transforms.ren.type=org.apache.kafka.connect.transforms.ReplaceField$Value
debezium.transforms.ren.renames=cust_id:customer_id,a:b
debezium.transforms.add.type=org.apache.kafka.connect.transforms.InsertField$Value
debezium.transforms.add.static.field=env
debezium.transforms.add.static.value=prod
debezium.transforms.flat.type=org.apache.kafka.connect.transforms.Flatten$Value
debezium.transforms.filter.type=io.debezium.transforms.Filter
debezium.transforms.filter.language=jsr223.groovy
debezium.transforms.filter.condition=value.op == 'c'
debezium.transforms.route.type=org.apache.kafka.connect.transforms.RegexRouter
debezium.transforms.smt.type=com.acme.Smt
debezium.transforms.smt.field=total
debezium.transforms.smt.predicate=isOrders
debezium.transforms.smt.secret=${DB_PASSWORD}
debezium.source.decimal.handling.mode=double
debezium.source.time.precision.mode=adaptive_time_microseconds
debezium.source.message.key.columns=public\.audit_log:event_id
`)
	want := `"transforms":[{"type":"filter","condition":"value.op == 'c'"},{"type":"unwrap","delete_handling":"rewrite","add_fields":["op","source.ts_ms"]},` +
		`{"type":"rename","renames":[{"from":"cust_id","to":"customer_id"},{"from":"a","to":"b"}]},{"type":"add_field","name":"env","value":{"kind":"static","text":"prod"}},` +
		`{"type":"flatten","delimiter":"."},{"type":"custom","class":"com.acme.Smt","properties":{"field":"total"}}]`
	if !strings.Contains(spec, want) {
		t.Errorf("steps:\n%s", spec)
	}
	if !strings.Contains(spec, `"values":{"decimals":"double"}`) || !strings.Contains(spec, `"keys":{"public.audit_log":["event_id"]}`) ||
		!strings.Contains(spec, `"advanced":{"debezium.source.time.precision.mode":"adaptive_time_microseconds"}`) {
		t.Errorf("options:\n%s", spec)
	}
	joined := strings.Join(warnings, "\n")
	for _, w := range []string{
		"Transform filter is a filter script; deploying it needs the project policy “Allow scripted transforms”.",
		"Transform smt (com.acme.Smt) is kept as a custom step; deploying it needs the project policy “Allow custom transforms”. Not kept: predicate, secret",
		"A RegexRouter renamed topics",
	} {
		if !strings.Contains(joined, w) {
			t.Errorf("missing warning %q in\n%s", w, joined)
		}
	}
	if strings.Contains(joined, "No unwrap transform") || strings.Contains(joined, "reordered") {
		t.Errorf("unexpected warnings:\n%s", joined)
	}
}

func TestAdoptKeepsTheChainValid(t *testing.T) {
	// Field changes before the shape are reordered; without a shape they cannot be kept.
	_, warnings := adopt(t, `debezium.transforms=ren,unwrap
debezium.transforms.ren.type=org.apache.kafka.connect.transforms.ReplaceField$Value
debezium.transforms.ren.renames=a:b
debezium.transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState
`)
	if !strings.Contains(strings.Join(warnings, " "), "the steps were reordered") {
		t.Errorf("%v", warnings)
	}
	spec, warnings := adopt(t, `debezium.transforms=ren,smt
debezium.transforms.ren.type=org.apache.kafka.connect.transforms.ReplaceField$Value
debezium.transforms.ren.renames=a:b
debezium.transforms.smt.type=com.acme.Smt
`)
	if strings.Contains(spec, `"rename"`) || strings.Contains(spec, `"custom"`) || !strings.Contains(strings.Join(warnings, " "), "The rename step needs the row-after-change event shape") {
		t.Errorf("%s %v", spec, warnings)
	}
	// Renames that are not plain names, and unsafe static values, become custom steps / are refused.
	spec, _ = adopt(t, `debezium.transforms=unwrap,ren,add
debezium.transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState
debezium.transforms.ren.type=org.apache.kafka.connect.transforms.ReplaceField$Value
debezium.transforms.ren.renames=a:b c
debezium.transforms.add.type=org.apache.kafka.connect.transforms.InsertField$Value
debezium.transforms.add.static.field=x
debezium.transforms.add.static.value=${HOME}
`)
	if !strings.Contains(spec, `{"type":"custom","class":"org.apache.kafka.connect.transforms.ReplaceField$Value","properties":{"renames":"a:b c"}}`) ||
		strings.Contains(spec, "HOME") {
		t.Errorf("%s", spec)
	}
	// Key columns of tables that are not captured stay as advanced properties.
	spec, _ = adopt(t, "debezium.source.message.key.columns=other.t:id\n")
	if strings.Contains(spec, `"keys"`) || !strings.Contains(spec, `"debezium.source.message.key.columns":"other.t:id"`) {
		t.Errorf("%s", spec)
	}
}
