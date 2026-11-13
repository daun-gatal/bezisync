package services

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/logging"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/security"
)

var bootstrapLog = logging.Logger("bezisync.services.bootstrap")

// Bootstrap creates the first admin and a "default" project when the instance has no
// users. Idempotent: it does nothing once any user exists; true when it created data.
func Bootstrap(ctx context.Context, q db.Querier) (bool, error) {
	s := config.Get()
	n, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM users`)
	if err != nil || n > 0 {
		return false, err
	}
	if s.BootstrapAdminPassword == "" {
		bootstrapLog.Warn("no users exist and BEZISYNC_BOOTSTRAP_ADMIN_PASSWORD is not set; skipping bootstrap")
		return false, nil
	}
	hash, err := security.HashPassword(s.BootstrapAdminPassword)
	if err != nil {
		return false, err
	}
	now := db.Now()
	admin := &models.User{ID: db.NewID(), Email: strings.ToLower(s.BootstrapAdminEmail), Name: "Administrator", PasswordHash: &hash,
		IsSuperuser: true, IsActive: true, UIMode: "guided", MutedNotifications: db.List{}, CreatedAt: now, UpdatedAt: now}
	project := &models.Project{ID: db.NewID(), Slug: "default", Name: "Default", Description: "Created on first start.", Policies: db.Map{},
		CreatedAt: now, UpdatedAt: now}
	if err := db.Insert(ctx, q, "users", admin); err != nil {
		return false, err
	}
	if err := db.Insert(ctx, q, "projects", project); err != nil {
		return false, err
	}
	if _, err := Record(ctx, q, AuditEvent{Kind: "project.created", Summary: "Created project " + project.Name, ProjectID: &project.ID,
		Principal: &auth.Principal{User: admin}, ResourceType: "project", ResourceID: &project.ID}); err != nil {
		return false, err
	}
	bootstrapLog.Info("bootstrapped administrator " + admin.Email + " and project 'default'")
	return true, nil
}

// EnsureSandbox creates the sandbox project of the demo stack once (idempotent; true
// when it created it).
func EnsureSandbox(ctx context.Context, q db.Querier) (bool, error) {
	s := config.Get()
	if s.SandboxSourceHost == "" || s.SandboxKafkaBootstrap == "" {
		return false, nil
	}
	n, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM projects WHERE is_sandbox IS TRUE`)
	if err != nil || n > 0 {
		return false, err
	}
	admins, err := db.Select[models.User](ctx, q, `SELECT * FROM users WHERE is_superuser IS TRUE`)
	if err != nil || len(admins) == 0 {
		return false, err
	}
	now := db.Now()
	project := &models.Project{ID: db.NewID(), Slug: "sandbox", Name: "Sandbox",
		Description: "Sample shop database and Kafka. Safe to experiment: nothing here is production.", IsSandbox: true, Policies: db.Map{},
		CreatedAt: now, UpdatedAt: now}
	if err := db.Insert(ctx, q, "projects", project); err != nil {
		return false, err
	}
	sourceSecrets := map[string]any{}
	if s.SandboxSourcePassword != "" {
		sourceSecrets["password"] = s.SandboxSourcePassword
	}
	cfg, sec, err := ValidateConnection("source", "postgresql", map[string]any{"host": s.SandboxSourceHost, "port": s.SandboxSourcePort,
		"database": s.SandboxSourceDatabase, "user": s.SandboxSourceUser, "ssl_mode": "disable"}, sourceSecrets)
	if err != nil {
		return false, err
	}
	enc, err := EncryptOrNil(sec)
	if err != nil {
		return false, err
	}
	source := &models.Connection{ID: db.NewID(), ProjectID: project.ID, Kind: "source", Type: "postgresql", Name: "demo-shop", Config: db.Map(cfg),
		SecretsEnc: enc, Description: "Sample shop database with live orders.", Provider: "self_hosted", Managed: true, Version: 1, CreatedAt: now, UpdatedAt: now}
	sinkCfg, _, err := ValidateConnection("sink", "kafka", map[string]any{"bootstrap_servers": s.SandboxKafkaBootstrap}, map[string]any{})
	if err != nil {
		return false, err
	}
	sink := &models.Connection{ID: db.NewID(), ProjectID: project.ID, Kind: "sink", Type: "kafka", Name: "demo-kafka", Config: db.Map(sinkCfg),
		Provider: "self_hosted", Managed: true, Version: 1, CreatedAt: now, UpdatedAt: now}
	rt := &models.Runtime{ID: db.NewID(), ProjectID: project.ID, Name: "local-docker", Kind: "docker", Mode: "direct",
		Config: db.Map{"host": "", "tls_verify": false, "network": s.SandboxDockerNetwork}, Enabled: true, Version: 1, AgentState: db.Map{},
		CreatedAt: now, UpdatedAt: now}
	if err := db.Insert(ctx, q, "connections", source); err != nil {
		return false, err
	}
	if err := db.Insert(ctx, q, "connections", sink); err != nil {
		return false, err
	}
	if err := db.Insert(ctx, q, "runtimes", rt); err != nil {
		return false, err
	}
	spec := domain.MustSpec(map[string]any{"snapshot": map[string]any{"mode": "initial"}, "sink": map[string]any{"naming": "demo.{schema}.{table}"}})
	specMap, err := SpecMap(spec)
	if err != nil {
		return false, err
	}
	tpl := &models.PipelineTemplate{ID: db.NewID(), ProjectID: project.ID, Name: "demo-shop-to-kafka", Title: "Shop → Kafka (demo)",
		Description: "Row-level JSON events per table, personal data masked, small size.", Spec: specMap, SinkID: &sink.ID, RuntimeID: &rt.ID,
		Version: 1, CreatedAt: now, UpdatedAt: now}
	if err := db.Insert(ctx, q, "pipeline_templates", tpl); err != nil {
		return false, err
	}
	if _, err := Record(ctx, q, AuditEvent{Kind: "project.created", Summary: "Created the sandbox project", ProjectID: &project.ID,
		Principal: &auth.Principal{User: &admins[0]}, ResourceType: "project", ResourceID: &project.ID}); err != nil {
		return false, err
	}
	// The extras are created with the project and never after: a Sandbox that exists is left
	// alone, so a deleted piece stays deleted (ADR 0018).
	if err := addSandboxExtras(ctx, q, s, project, rt, &admins[0]); err != nil {
		return false, err
	}
	bootstrapLog.Info("created the sandbox project for the demo stack")
	return true, nil
}

// sandboxConnection is one optional connection of the Sandbox, with its template when it is a sink.
type sandboxConnection struct {
	kind, typ, provider, name, description string
	config                                 map[string]any
	secrets                                map[string]any
	template                               *sandboxTemplate
}

type sandboxTemplate struct{ name, title, description string }

// SandboxTeam are the teammates the Sandbox adds when BEZISYNC_SANDBOX_TEAM_PASSWORD is set: one
// per project role, so a visitor can sign in as each and see what it may do.
var SandboxTeam = []struct{ Email, Name, Role string }{
	{"editor@bezisync.local", "Erin Editor", models.RoleEditor},
	{"operator@bezisync.local", "Oscar Operator", models.RoleOperator},
	{"viewer@bezisync.local", "Vera Viewer", models.RoleViewer},
}

// SandboxTeamPasswordProblem says why a team password can't be used ("" when it can). The
// teammates are ordinary users, so it follows the rule for an initial password (12 characters
// or more) and refuses the same weak values as the bootstrap admin's.
func SandboxTeamPasswordProblem(pw string) string {
	switch {
	case pw == "admin" || pw == "password":
		return "BEZISYNC_SANDBOX_TEAM_PASSWORD is too weak"
	case len(pw) < 12:
		return "BEZISYNC_SANDBOX_TEAM_PASSWORD must be at least 12 characters"
	case len(pw) > 1024:
		return "BEZISYNC_SANDBOX_TEAM_PASSWORD must be at most 1024 characters"
	}
	return ""
}

func sandboxExtras(s *config.Settings) []sandboxConnection {
	secret := func(pw string) map[string]any {
		if pw == "" {
			return map[string]any{}
		}
		return map[string]any{"password": pw}
	}
	var out []sandboxConnection
	if s.SandboxMySQLHost != "" {
		out = append(out, sandboxConnection{kind: "source", typ: "mysql", provider: "self_hosted", name: "demo-mysql",
			description: "Sample shop database (MySQL with GTIDs) with live orders.",
			config:      map[string]any{"host": s.SandboxMySQLHost, "port": s.SandboxMySQLPort, "user": s.SandboxMySQLUser, "ssl_mode": "disabled"},
			secrets:     secret(s.SandboxMySQLPassword)})
	}
	if s.SandboxLegacyHost != "" {
		out = append(out, sandboxConnection{kind: "source", typ: "postgresql", provider: "self_hosted", name: "legacy-postgres",
			description: "Not ready for change data capture on purpose (wal_level=replica): run preflight to see what to fix.",
			config: map[string]any{"host": s.SandboxLegacyHost, "port": s.SandboxLegacyPort, "database": s.SandboxLegacyDatabase,
				"user": s.SandboxLegacyUser, "ssl_mode": "disable"},
			secrets: secret(s.SandboxLegacyPassword)})
	}
	if s.SandboxWarehouseHost != "" {
		out = append(out, sandboxConnection{kind: "sink", typ: "jdbc", provider: "postgresql", name: "demo-warehouse",
			description: "PostgreSQL warehouse: pipelines create their tables in public.",
			config: map[string]any{"database_type": "postgresql", "host": s.SandboxWarehouseHost, "port": s.SandboxWarehousePort,
				"database": s.SandboxWarehouseDatabase, "schema": "public", "user": s.SandboxWarehouseUser, "tls": "disable"},
			secrets: secret(s.SandboxWarehousePassword),
			template: &sandboxTemplate{name: "demo-shop-to-warehouse", title: "Shop → Warehouse (demo)",
				description: "Keeps a copy of each chosen shop table in the warehouse, one table each."}})
	}
	if s.SandboxHTTPURL != "" {
		out = append(out, sandboxConnection{kind: "sink", typ: "http", provider: "self_hosted", name: "demo-http",
			description: "HTTP endpoint that accepts every event.",
			config:      map[string]any{"url": s.SandboxHTTPURL},
			secrets:     map[string]any{},
			template: &sandboxTemplate{name: "demo-shop-to-http", title: "Shop → HTTP (demo)",
				description: "Posts each change as JSON to an HTTP endpoint."}})
	}
	return out
}

// addSandboxExtras adds the optional connections, templates and teammates to a new Sandbox.
func addSandboxExtras(ctx context.Context, q db.Querier, s *config.Settings, project *models.Project, rt *models.Runtime, admin *models.User) error {
	now := db.Now()
	for _, x := range sandboxExtras(s) {
		cfg, sec, err := ValidateConnection(x.kind, x.typ, x.config, x.secrets)
		if err != nil {
			return fmt.Errorf("sandbox connection %s: %w", x.name, err)
		}
		enc, err := EncryptOrNil(sec)
		if err != nil {
			return err
		}
		conn := &models.Connection{ID: db.NewID(), ProjectID: project.ID, Kind: x.kind, Type: x.typ, Name: x.name, Config: db.Map(cfg),
			SecretsEnc: enc, Description: x.description, Provider: x.provider, Managed: true, Version: 1, CreatedAt: now, UpdatedAt: now}
		if err := db.Insert(ctx, q, "connections", conn); err != nil {
			return err
		}
		if x.template == nil {
			continue
		}
		spec := domain.MustSpec(map[string]any{"snapshot": map[string]any{"mode": "initial"},
			"sink": map[string]any{"naming": "demo.{schema}.{table}"}})
		specMap, err := SpecMap(spec)
		if err != nil {
			return err
		}
		tpl := &models.PipelineTemplate{ID: db.NewID(), ProjectID: project.ID, Name: x.template.name, Title: x.template.title,
			Description: x.template.description, Spec: specMap, SinkID: &conn.ID, RuntimeID: &rt.ID, Version: 1, CreatedAt: now, UpdatedAt: now}
		if err := db.Insert(ctx, q, "pipeline_templates", tpl); err != nil {
			return err
		}
	}
	return addSandboxTeam(ctx, q, s.SandboxTeamPassword, project, admin)
}

// addSandboxTeam creates the teammates whose email is free and makes each a Sandbox member with
// its role. A user that already exists is left untouched and gets no membership.
func addSandboxTeam(ctx context.Context, q db.Querier, password string, project *models.Project, admin *models.User) error {
	if password == "" {
		return nil
	}
	if problem := SandboxTeamPasswordProblem(password); problem != "" {
		return errors.New(problem)
	}
	hash, err := security.HashPassword(password)
	if err != nil {
		return err
	}
	now := db.Now()
	for _, member := range SandboxTeam {
		taken, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM users WHERE lower(email) = $1`, member.Email)
		if err != nil {
			return err
		}
		if taken > 0 {
			bootstrapLog.Warn("sandbox teammate " + member.Email + " already exists; left as it is")
			continue
		}
		u := &models.User{ID: db.NewID(), Email: member.Email, Name: member.Name, PasswordHash: &hash, IsActive: true, UIMode: "guided",
			MutedNotifications: db.List{}, CreatedAt: now, UpdatedAt: now}
		if err := db.Insert(ctx, q, "users", u); err != nil {
			return err
		}
		m := &models.Membership{ID: db.NewID(), ProjectID: project.ID, UserID: u.ID, Role: member.Role, CreatedAt: now, UpdatedAt: now}
		if err := db.Insert(ctx, q, "memberships", m); err != nil {
			return err
		}
		if _, err := Record(ctx, q, AuditEvent{Kind: "member.upserted", Summary: fmt.Sprintf("Set %s as %s", u.Email, member.Role),
			ProjectID: &project.ID, Principal: &auth.Principal{User: admin}, ResourceType: "user", ResourceID: &u.ID,
			Details: map[string]any{"role": member.Role, "can_approve": false}}); err != nil {
			return err
		}
	}
	return nil
}
