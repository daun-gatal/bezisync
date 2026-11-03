package api_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/testdb"
)

func TestTemplateFromPipelineThenUsedByAnEditor(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	base := "/api/v1/projects/" + e.pid
	pipe := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	admin.patch(base+"/pipelines/"+pipe.Str("id"), map[string]any{"spec": map[string]any{
		"source":   map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}, "columns": map[string]any{"shop.orders": map[string]any{"mask": []string{"total"}}}},
		"sink":     map[string]any{"naming": "team.{table}", "format": map[string]any{"type": "cloudevents"}},
		"policies": map[string]any{"resources": "medium"},
	}}, ifMatch(pipe.At("version"))...).expect(200)
	editor := member(t, e.srv, admin, e.pid, "editor")
	body := map[string]any{"name": "orders-standard", "title": "Orders standard", "from_pipeline_id": pipe.Str("id")}
	editor.post(base+"/templates", body).expect(403)
	tpl := admin.post(base+"/templates", body).expect(201)
	if len(tpl.List("spec", "source", "tables", "include")) != 0 || len(tpl.At("spec", "source", "columns").(map[string]any)) != 0 ||
		tpl.Str("spec", "sink", "format", "type") != "cloudevents" || tpl.Str("sink", "id") != e.ids.Sink {
		t.Fatal(string(tpl.Raw))
	}
	admin.post(base+"/templates", body).expect(409)
	listed := editor.get(base + "/templates").expect(200).List()
	if len(listed) != 1 {
		t.Fatal(listed)
	}
	p := editor.post(base+"/pipelines", map[string]any{"name": "refunds-cdc", "source_id": e.ids.Source, "template_id": at(t, listed[0], "id"),
		"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.refunds"}}}}}).expect(201)
	if p.Str("sink", "id") != e.ids.Sink || p.Str("runtime", "id") != e.ids.Runtime || p.Str("labels", "template") != "orders-standard" ||
		p.List("spec", "source", "tables", "include")[0] != "shop.refunds" || p.Str("spec", "sink", "naming") != "team.{table}" ||
		p.Str("spec", "policies", "resources") != "medium" {
		t.Fatal(string(p.Raw))
	}
	u := base + "/templates/" + at(t, listed[0], "id").(string)
	if r := admin.patch(u, map[string]any{"title": "Orders v2"}, ifMatch(1)...).expect(200); r.Str("title") != "Orders v2" || r.At("version") != float64(2) {
		t.Fatal(string(r.Raw))
	}
	admin.patch(u, map[string]any{"title": "stale"}, ifMatch(1)...).expect(412)
	admin.put(base+"/policies", map[string]any{"allowed_sink_types": []string{"http"}}).expect(200)
	blocked := admin.post(base+"/templates", map[string]any{"name": "other", "title": "Other", "sink_id": e.ids.Sink}).expect(422)
	if blocked.Code() != "policy_violation" {
		t.Fatal(string(blocked.Raw))
	}
	admin.del(u).expect(204)
}

func TestSandboxBootstrap(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	ctx := context.Background()
	ensure := func() bool {
		var created bool
		if err := db.Tx(ctx, func(tx pgx.Tx) error {
			var err error
			created, err = services.EnsureSandbox(ctx, tx)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return created
	}
	if ensure() {
		t.Fatal("created without configuration")
	}
	s := *config.Get()
	s.SandboxSourceHost, s.SandboxSourcePassword, s.SandboxKafkaBootstrap, s.SandboxDockerNetwork = "demo-shop", "demo-pw", "kafka:9092", "bezisync-demo"
	config.Set(&s)
	t.Cleanup(config.Reset)
	if !ensure() || ensure() {
		t.Fatal("not created exactly once")
	}
	q := testdb.Pool(t)
	conns, err := db.Select[struct {
		Name       string  `db:"name"`
		Managed    bool    `db:"managed"`
		SecretsEnc *string `db:"secrets_enc"`
	}](ctx, q, `SELECT c.name, c.managed, c.secrets_enc FROM connections c JOIN projects p ON p.id = c.project_id WHERE p.slug = 'sandbox' ORDER BY c.name`)
	if err != nil || len(conns) != 2 || conns[0].Name != "demo-kafka" || conns[1].Name != "demo-shop" || !conns[0].Managed || !conns[1].Managed {
		t.Fatal(conns, err)
	}
	if conns[1].SecretsEnc == nil || strings.Contains(*conns[1].SecretsEnc, "demo-pw") {
		t.Fatal("secret not encrypted")
	}
	network, err := db.Scalar[string](ctx, q, `SELECT r.config->>'network' FROM runtimes r JOIN projects p ON p.id = r.project_id WHERE p.slug = 'sandbox' AND r.kind = 'docker'`)
	if err != nil || network != "bezisync-demo" {
		t.Fatal(network, err)
	}
	sinkName, err := db.Scalar[string](ctx, q, `SELECT c.name FROM pipeline_templates t JOIN connections c ON c.id = t.sink_id`)
	if err != nil || sinkName != "demo-kafka" {
		t.Fatal(sinkName, err)
	}
	found := false
	for _, p := range admin.get("/api/v1/auth/me").List("projects") {
		found = found || (at(t, p, "slug") == "sandbox" && at(t, p, "is_sandbox") == true)
	}
	if !found {
		t.Fatal("sandbox not visible")
	}
}
