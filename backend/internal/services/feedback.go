package services

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/ratelimit"
)

// Feedback from the app: anyone signed in sends it, Admins read and resolve it.

// FeedbackLabels name the feedback kinds.
var FeedbackLabels = map[string]string{"bug": "Something is broken", "security": "Security issue", "idea": "Idea", "other": "Other"}

// SubmitFeedback stores feedback and tells every Admin (limited per person).
func SubmitFeedback(ctx context.Context, q db.Querier, p *auth.Principal, project *models.Project, kind, message, page, userAgent string) (*models.Feedback, error) {
	if err := ratelimit.Feedback.Check(p.User.ID.String()); err != nil {
		return nil, err
	}
	item := &models.Feedback{ID: db.NewID(), ProjectID: project.ID, UserID: &p.User.ID, Email: p.User.Email, Kind: kind, Message: message,
		Page: page, UserAgent: domain.Truncate(userAgent, 300), CreatedAt: db.Now()}
	if err := db.Insert(ctx, q, "feedback", item); err != nil {
		return nil, err
	}
	// The message stays out of the audit log: it is free text and may hold anything.
	if _, err := Record(ctx, q, AuditEvent{Kind: "feedback.sent", Summary: fmt.Sprintf("Sent feedback (%s)", strings.ToLower(FeedbackLabels[kind])),
		ProjectID: &project.ID, Principal: p, ResourceType: "feedback", ResourceID: &item.ID, Details: map[string]any{"kind": kind}}); err != nil {
		return nil, err
	}
	admins, err := db.Select[struct {
		ID uuid.UUID `db:"id"`
	}](ctx, q, `SELECT id FROM users WHERE is_superuser IS TRUE AND is_active IS TRUE`)
	if err != nil {
		return nil, err
	}
	var ids []uuid.UUID
	for _, a := range admins {
		if a.ID != p.User.ID {
			ids = append(ids, a.ID)
		}
	}
	title := "New feedback: " + strings.ToLower(FeedbackLabels[kind])
	if kind == "security" {
		title = "New security report from " + item.Email
	}
	_, err = Notify(ctx, q, ids, Notification{ProjectID: project.ID, Kind: "feedback_received", Title: title,
		Body: domain.Truncate(message, 200), Link: fmt.Sprintf("/p/%s/settings/feedback", project.Slug)})
	return item, err
}

// FeedbackPage is a page of feedback with project names.
func FeedbackPage(ctx context.Context, q db.Querier, status string, kind *string, cursor string, limit int) ([]models.Feedback, *string, map[uuid.UUID]string, error) {
	base := `SELECT * FROM feedback`
	var conds []string
	var args []any
	switch status {
	case "open":
		conds = append(conds, "resolved_at IS NULL")
	case "resolved":
		conds = append(conds, "resolved_at IS NOT NULL")
	}
	if kind != nil {
		args = append(args, *kind)
		conds = append(conds, fmt.Sprintf("kind = $%d", len(args)))
	}
	if len(conds) > 0 {
		base += " WHERE " + strings.Join(conds, " AND ")
	}
	sql, args, err := PageSQL(base, args, "created_at", "id", cursor, limit, false)
	if err != nil {
		return nil, nil, nil, err
	}
	rows, err := db.Select[models.Feedback](ctx, q, sql, args...)
	if err != nil {
		return nil, nil, nil, err
	}
	var next *string
	if l := ClampLimit(limit); len(rows) > l {
		rows = rows[:l]
		last := rows[len(rows)-1]
		c := EncodeCursor(last.CreatedAt, last.ID)
		next = &c
	}
	names := map[uuid.UUID]string{}
	if len(rows) > 0 {
		var ids []uuid.UUID
		for _, r := range rows {
			ids = append(ids, r.ProjectID)
		}
		projects, err := db.Select[struct {
			ID   uuid.UUID `db:"id"`
			Name string    `db:"name"`
		}](ctx, q, `SELECT id, name FROM projects WHERE id = ANY($1)`, ids)
		if err != nil {
			return nil, nil, nil, err
		}
		for _, p := range projects {
			names[p.ID] = p.Name
		}
	}
	return rows, next, names, nil
}

// FeedbackCounts is how many reports are open and resolved.
func FeedbackCounts(ctx context.Context, q db.Querier) (int64, int64, error) {
	open, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM feedback WHERE resolved_at IS NULL`)
	if err != nil {
		return 0, 0, err
	}
	resolved, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM feedback WHERE resolved_at IS NOT NULL`)
	return open, resolved, err
}

// SetFeedbackResolved resolves or reopens a report.
func SetFeedbackResolved(ctx context.Context, q db.Querier, p *auth.Principal, id uuid.UUID, resolved bool) (*models.Feedback, error) {
	item, err := db.Find[models.Feedback](ctx, q, `SELECT * FROM feedback WHERE id = $1`, id)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return nil, problem.NotFound("Feedback")
	}
	switch {
	case resolved && item.ResolvedAt == nil:
		now := db.Now()
		item.ResolvedAt, item.ResolvedBy = &now, &p.User.Email
	case !resolved && item.ResolvedAt != nil:
		item.ResolvedAt, item.ResolvedBy = nil, nil
	default:
		return item, nil
	}
	if err := db.Update(ctx, q, "feedback", item); err != nil {
		return nil, err
	}
	kind, verb := "feedback.reopened", "Reopened"
	if resolved {
		kind, verb = "feedback.resolved", "Resolved"
	}
	_, err = Record(ctx, q, AuditEvent{Kind: kind, Summary: fmt.Sprintf("%s feedback from %s", verb, item.Email), ProjectID: &item.ProjectID,
		Principal: p, ResourceType: "feedback", ResourceID: &item.ID})
	return item, err
}
