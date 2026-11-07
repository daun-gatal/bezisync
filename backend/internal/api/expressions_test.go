package api_test

import (
	"strings"
	"testing"
)

// ADR 0008 E6: the expression check is pure, viewers may use it, other projects see nothing, and
// a refusal carries its position in UTF-16 code units.
func TestExpressionCheckEndpoint(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	base := "/api/v1/projects/" + e.pid
	pipe := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	u := base + "/pipelines/" + pipe.Str("id") + "/expressions:check"
	cols := []any{map[string]any{"name": "id", "type": "bigint"}, map[string]any{"name": "status", "type": "text"}}

	ok := admin.post(u, map[string]any{"use": "column", "table": "shop.orders", "expression": "upper(status) || '-'", "columns": cols}).expect(200)
	if ok.Str("canonical") != "UPPER(`status`) || '-'" || ok.Str("kind") != "text" || ok.At("deterministic") != true || ok.At("error") != nil {
		t.Fatal(string(ok.Raw))
	}
	// Without columns every column is accepted (preflight checks them).
	if r := admin.post(u, map[string]any{"use": "column", "table": "shop.orders", "expression": "upper(anything)"}).expect(200); r.At("error") != nil {
		t.Fatal(string(r.Raw))
	}
	// A refusal: message, and the offset and length as the browser counts them ("日本" is 2 code units).
	bad := admin.post(u, map[string]any{"use": "filter", "table": "shop.orders", "expression": "'日本' = status and nope(id)", "columns": cols}).expect(200)
	if !strings.Contains(string(bad.Raw), `"offset":18`) || !strings.Contains(string(bad.Raw), `"length":4`) || !strings.Contains(string(bad.Raw), "isn't a function") {
		t.Fatal(string(bad.Raw))
	}
	if r := admin.post(u, map[string]any{"use": "filter", "table": "shop.orders", "expression": "now() is not null", "columns": cols}).expect(200); !strings.Contains(string(r.Raw), "can't use NOW") {
		t.Fatal(string(r.Raw))
	}
	admin.post(u, map[string]any{"use": "nope", "table": "shop.orders", "expression": "1"}).expect(422)
	admin.post(u, map[string]any{"use": "column", "table": "shop.orders", "expression": "1", "extra": true}).expect(422)
	admin.post(u, map[string]any{"use": "column", "table": "shop.orders", "expression": strings.Repeat("a", 4001)}).expect(422)

	viewer := member(t, e.srv, admin, e.pid, "viewer")
	viewer.post(u, map[string]any{"use": "column", "table": "shop.orders", "expression": "id"}).expect(200)
	other := admin.post("/api/v1/projects", map[string]any{"slug": "other-expr", "name": "Other"}).expect(201).Str("id")
	admin.post("/api/v1/projects/"+other+"/pipelines/"+pipe.Str("id")+"/expressions:check", map[string]any{"use": "column", "table": "shop.orders", "expression": "id"}).expect(404)
}
