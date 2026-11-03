// Package services holds the business logic behind the API and the controller.
package services

import (
	"context"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
)

// AuditEvent is one activity/audit event.
type AuditEvent struct {
	Kind, Summary string
	ProjectID     *uuid.UUID
	Principal     *auth.Principal
	ResourceType  string
	ResourceID    *uuid.UUID
	Details       map[string]any
	// NotAudit marks an operational (non-audit) event; audit is the default.
	NotAudit bool
}

// Record writes an event (every mutation records one).
func Record(ctx context.Context, q db.Querier, e AuditEvent) (uuid.UUID, error) {
	id := db.NewID()
	var actorID *uuid.UUID
	label := "system"
	if e.Principal != nil {
		actorID = &e.Principal.User.ID
		label = e.Principal.Label()
	}
	details := db.Map(e.Details)
	if details == nil {
		details = db.Map{}
	}
	var rid *string
	if r := httpapi.RequestID(ctx); r != "" {
		rid = &r
	}
	_, err := q.Exec(ctx, `INSERT INTO events (id, occurred_at, project_id, actor_id, actor_label, kind, resource_type, resource_id, summary, details, audit, request_id)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
		id, db.Now(), e.ProjectID, actorID, label, e.Kind, e.ResourceType, e.ResourceID, e.Summary, details, !e.NotAudit, rid)
	return id, err
}

// UUIDPtr is a helper for optional ids.
func UUIDPtr(id uuid.UUID) *uuid.UUID { return &id }
