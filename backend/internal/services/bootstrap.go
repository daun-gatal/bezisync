package services

import (
	"context"
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
	bootstrapLog.Info("created the sandbox project for the demo stack")
	return true, nil
}
