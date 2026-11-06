package migrate_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/daun-gatal/bezisync/backend/internal/migrate"
)

// 00008: projects whose pipelines already use advanced properties (working or deployed version)
// get allow_advanced_properties and an audit event; others, and deleted pipelines, don't count.
// Down removes the key, which older releases would refuse.
func TestMigrationAllowsAdvancedWhereUsed(t *testing.T) {
	url := freshDB(t)
	ctx := context.Background()
	if err := migrate.GooseTo(ctx, url, 7, false); err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	type pipe struct {
		project, spec         string
		deleted, deployedOnly bool
	}
	projects := map[string]string{
		"uses":         "11111111-1111-1111-1111-111111111111",
		"deployedonly": "22222222-2222-2222-2222-222222222222",
		"empty":        "33333333-3333-3333-3333-333333333333",
		"deleted":      "44444444-4444-4444-4444-444444444444",
		"none":         "55555555-5555-5555-5555-555555555555",
	}
	for slug, id := range projects {
		exec(`INSERT INTO projects (id, slug, name, description, is_sandbox, policies, created_at, updated_at) VALUES ($1, $2, $2, '', false, '{"require_approval": true}', now(), now())`, id, slug)
	}
	src := "aaaaaaaa-0000-0000-0000-000000000001"
	exec(`INSERT INTO connections (id, project_id, kind, type, name, description, provider, config, managed, version, created_at, updated_at)
	      VALUES ($1, $2, 'source', 'postgresql', 'pg', '', 'self_hosted', '{}', false, 1, now(), now())`, src, projects["uses"])
	for i, p := range []pipe{
		{project: "uses", spec: `{"advanced": {"debezium.source.poll.interval.ms": "250"}}`},
		{project: "deployedonly", spec: `{"advanced": {"debezium.source.poll.interval.ms": "250"}}`, deployedOnly: true},
		{project: "empty", spec: `{"advanced": {}}`},
		{project: "deleted", spec: `{"advanced": {"debezium.source.poll.interval.ms": "250"}}`, deleted: true},
		{project: "none", spec: `{"source": {"tables": {"include": ["a.b"]}}}`},
	} {
		pid := "bbbbbbbb-0000-0000-0000-00000000000" + string(rune('1'+i))
		vid := "cccccccc-0000-0000-0000-00000000000" + string(rune('1'+i))
		other := "dddddddd-0000-0000-0000-00000000000" + string(rune('1'+i))
		exec(`INSERT INTO pipelines (id, project_id, name, description, owner, labels, source_id, desired_state, status, lifecycle, health, artifacts, version, created_at, updated_at, deleted_at)
		      VALUES ($1, $2, $3, '', '', '{}', $4, 'paused', '{}', 'draft', 'unknown', '{}', 1, now(), now(), CASE WHEN $5 THEN now() END)`,
			pid, projects[p.project], "p"+p.project, src, p.deleted)
		exec(`INSERT INTO pipeline_versions (id, pipeline_id, number, spec, spec_hash, note, created_at, created_by_label) VALUES ($1, $2, 1, $3, 'h', '', now(), 'x')`, vid, pid, p.spec)
		exec(`INSERT INTO pipeline_versions (id, pipeline_id, number, spec, spec_hash, note, created_at, created_by_label) VALUES ($1, $2, 2, '{}', 'h2', '', now(), 'x')`, other, pid)
		if p.deployedOnly {
			exec(`UPDATE pipelines SET current_version_id = $1, deployed_version_id = $2 WHERE id = $3`, other, vid, pid)
		} else {
			exec(`UPDATE pipelines SET current_version_id = $1 WHERE id = $2`, vid, pid)
		}
	}
	if err := migrate.GooseTo(ctx, url, 8, false); err != nil {
		t.Fatal(err)
	}
	allowed := func(slug string) (bool, bool) {
		var on *bool
		var approval bool
		if err := conn.QueryRow(ctx, `SELECT (policies->>'allow_advanced_properties')::boolean, (policies->>'require_approval')::boolean FROM projects WHERE slug = $1`, slug).Scan(&on, &approval); err != nil {
			t.Fatal(err)
		}
		return on != nil && *on, approval
	}
	for slug, want := range map[string]bool{"uses": true, "deployedonly": true, "empty": false, "deleted": false, "none": false} {
		got, approval := allowed(slug)
		if got != want || !approval {
			t.Errorf("%s: allow_advanced_properties %v (want %v), require_approval kept %v", slug, got, want, approval)
		}
	}
	var events int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM events WHERE kind = 'project.policies_updated' AND audit AND actor_id IS NULL`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 2 {
		t.Fatalf("%d audit events, want 2", events)
	}
	if err := migrate.GooseTo(ctx, url, 7, true); err != nil {
		t.Fatal(err)
	}
	for slug := range projects {
		if got, approval := allowed(slug); got || !approval {
			t.Errorf("after down %s: %v %v", slug, got, approval)
		}
	}
}
