package services

import (
	"context"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/models"
)

// InboxKinds are the notification kinds users can mute, with their labels.
var InboxKinds = []struct{ Kind, Label string }{
	{"alert_fired", "An alert fires"},
	{"alert_resolved", "An alert resolves"},
	{"approval_requested", "A deploy waits for my approval"},
	{"approval_decided", "My deploy request is approved or rejected"},
	{"feedback_received", "Someone sends feedback (Admins only)"},
}

// ProjectAudience is who hears about a project: its members (or only its approvers) and
// every active Admin.
func ProjectAudience(ctx context.Context, q db.Querier, projectID uuid.UUID, approversOnly bool) ([]uuid.UUID, error) {
	sql := `SELECT user_id FROM memberships WHERE project_id = $1`
	if approversOnly {
		sql += ` AND can_approve IS TRUE`
	}
	sql += ` UNION SELECT id FROM users WHERE is_superuser IS TRUE AND is_active IS TRUE`
	rows, err := db.Select[struct {
		ID uuid.UUID `db:"user_id"`
	}](ctx, q, sql, projectID)
	if err != nil {
		return nil, err
	}
	out := make([]uuid.UUID, len(rows))
	for i, r := range rows {
		out[i] = r.ID
	}
	return out, nil
}

// Notification is one inbox entry to add.
type Notification struct {
	ProjectID               uuid.UUID
	Kind, Title, Body, Link string
}

// Notify adds an inbox entry for every user who has not muted the kind; returns how many.
func Notify(ctx context.Context, q db.Querier, userIDs []uuid.UUID, n Notification) (int, error) {
	if len(userIDs) == 0 {
		return 0, nil
	}
	users, err := db.Select[models.User](ctx, q, `SELECT * FROM users WHERE id = ANY($1)`, userIDs)
	if err != nil {
		return 0, err
	}
	added := 0
	for _, u := range users {
		muted := false
		for _, k := range u.Muted() {
			if k == n.Kind {
				muted = true
			}
		}
		if muted {
			continue
		}
		if _, err := q.Exec(ctx, `INSERT INTO notifications (id, user_id, project_id, kind, title, body, link, created_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, db.NewID(), u.ID, n.ProjectID, n.Kind, n.Title, n.Body, n.Link, db.Now()); err != nil {
			return added, err
		}
		added++
	}
	return added, nil
}
