package services

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

// Change requests: deploys that need a second person's approval.

// ApprovalRequired reports the project's require_approval policy.
func ApprovalRequired(ctx context.Context, q db.Querier, projectID uuid.UUID) (bool, error) {
	p, err := PoliciesOf(ctx, q, projectID)
	return p.RequireApproval, err
}

// PendingFor is the pipeline's pending change request.
func PendingFor(ctx context.Context, q db.Querier, pipelineID uuid.UUID) (*models.ChangeRequest, error) {
	return db.Find[models.ChangeRequest](ctx, q, `SELECT * FROM change_requests WHERE pipeline_id = $1 AND state = 'pending' LIMIT 1`, pipelineID)
}

func pipelineLink(a *auth.ProjectAccess, p *models.Pipeline) string {
	return fmt.Sprintf("/p/%s/pipelines/%s", a.Project.Slug, p.ID)
}

func without(ids []uuid.UUID, drop uuid.UUID) []uuid.UUID {
	var out []uuid.UUID
	for _, id := range ids {
		if id != drop {
			out = append(out, id)
		}
	}
	return out
}

// RequestApproval records a deploy as a pending change request.
func RequestApproval(ctx context.Context, q db.Querier, a *auth.ProjectAccess, p *models.Pipeline, versionNumber *int, forceReason *string, note string) (*models.ChangeRequest, error) {
	if err := a.Require(auth.PipelineDeploy); err != nil {
		return nil, err
	}
	v, report, _, err := CheckDeployable(ctx, q, p, versionNumber, forceReason)
	if err != nil {
		return nil, err
	}
	// The approver reviews exactly what was sent: later saves start a new version.
	if err := SealVersion(ctx, q, v); err != nil {
		return nil, err
	}
	previous, err := PendingFor(ctx, q, p.ID)
	if err != nil {
		return nil, err
	}
	if previous != nil {
		if previous.VersionID == v.ID && previous.RequestedByID != nil && *previous.RequestedByID == a.Principal.User.ID {
			return previous, nil
		}
		now := db.Now()
		previous.State, previous.DecidedAt = "superseded", &now
		if err := db.Update(ctx, q, "change_requests", previous); err != nil {
			return nil, err
		}
	}
	pre, err := ToMap(report)
	if err != nil {
		return nil, err
	}
	cr := &models.ChangeRequest{ID: db.NewID(), ProjectID: a.Project.ID, PipelineID: p.ID, VersionID: v.ID, State: "pending",
		RequestedByID: &a.Principal.User.ID, ForceReason: forceReason, Note: note, Preflight: pre, CreatedAt: db.Now()}
	if err := db.Insert(ctx, q, "change_requests", cr); err != nil {
		return nil, err
	}
	forced := forceReason != nil && *forceReason != ""
	if _, err := Record(ctx, q, AuditEvent{Kind: "pipeline.deploy_requested", Summary: fmt.Sprintf("Requested approval to deploy %s v%d", p.Name, v.Number),
		ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "pipeline", ResourceID: &p.ID,
		Details: map[string]any{"change_request": cr.ID.String(), "version": v.Number, "forced": forced}}); err != nil {
		return nil, err
	}
	approvers, err := ProjectAudience(ctx, q, a.Project.ID, true)
	if err != nil {
		return nil, err
	}
	body := "Requested by " + a.Principal.User.Email + "."
	if forced {
		body += " Skips failed checks: " + *forceReason
	}
	_, err = Notify(ctx, q, without(approvers, a.Principal.User.ID), Notification{ProjectID: a.Project.ID, Kind: "approval_requested",
		Title: fmt.Sprintf("Approve deploying %s v%d?", p.Name, v.Number), Body: body, Link: pipelineLink(a, p)})
	return cr, err
}

func getChangeRequest(ctx context.Context, q db.Querier, a *auth.ProjectAccess, id uuid.UUID) (*models.ChangeRequest, *models.Pipeline, error) {
	cr, err := db.Find[models.ChangeRequest](ctx, q, `SELECT * FROM change_requests WHERE id = $1`, id)
	if err != nil {
		return nil, nil, err
	}
	if cr == nil || cr.ProjectID != a.Project.ID {
		return nil, nil, problem.NotFound("Change request")
	}
	p, err := GetPipeline(ctx, q, a, cr.PipelineID, false)
	if err != nil {
		return nil, nil, err
	}
	return cr, p, nil
}

func decide(ctx context.Context, q db.Querier, a *auth.ProjectAccess, cr *models.ChangeRequest, state, comment string) error {
	if cr.State != "pending" {
		return problem.Conflict("change_request_closed", fmt.Sprintf("This request is already %s.", cr.State), nil)
	}
	now := db.Now()
	cr.State, cr.DecidedByID, cr.DecidedAt, cr.Comment = state, &a.Principal.User.ID, &now, comment
	return db.Update(ctx, q, "change_requests", cr)
}

// Approve approves and deploys a change request (re-running preflight).
func Approve(ctx context.Context, q db.Querier, a *auth.ProjectAccess, id uuid.UUID, comment string) (*models.ChangeRequest, error) {
	if err := a.Require(auth.PipelineApprove); err != nil {
		return nil, err
	}
	cr, p, err := getChangeRequest(ctx, q, a, id)
	if err != nil {
		return nil, err
	}
	if cr.RequestedByID != nil && *cr.RequestedByID == a.Principal.User.ID {
		return nil, problem.New(403, "own_change_request", "Someone else must approve your own request.")
	}
	v, err := VersionOf(ctx, q, &cr.VersionID)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, problem.NotFound("Version")
	}
	if err := decide(ctx, q, a, cr, "approved", comment); err != nil {
		return nil, err
	}
	number := v.Number
	if _, _, err := Deploy(ctx, q, a, p, &number, cr.ForceReason); err != nil {
		return nil, err
	}
	if _, err := Record(ctx, q, AuditEvent{Kind: "pipeline.deploy_approved", Summary: fmt.Sprintf("Approved deploying %s v%d", p.Name, v.Number),
		ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "pipeline", ResourceID: &p.ID,
		Details: map[string]any{"change_request": cr.ID.String(), "comment": comment}}); err != nil {
		return nil, err
	}
	if cr.RequestedByID != nil {
		if _, err := Notify(ctx, q, []uuid.UUID{*cr.RequestedByID}, Notification{ProjectID: a.Project.ID, Kind: "approval_decided",
			Title: fmt.Sprintf("Approved: %s v%d", p.Name, v.Number),
			Body:  strings.TrimSpace(fmt.Sprintf("%s approved and deployed it. %s", a.Principal.User.Email, comment)), Link: pipelineLink(a, p)}); err != nil {
			return nil, err
		}
	}
	return cr, nil
}

// Reject rejects a change request (a comment is required).
func Reject(ctx context.Context, q db.Querier, a *auth.ProjectAccess, id uuid.UUID, comment string) (*models.ChangeRequest, error) {
	if err := a.Require(auth.PipelineApprove); err != nil {
		return nil, err
	}
	cr, p, err := getChangeRequest(ctx, q, a, id)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(comment) == "" {
		return nil, problem.New(422, "comment_required", "Say why, so the requester knows what to change.")
	}
	if err := decide(ctx, q, a, cr, "rejected", comment); err != nil {
		return nil, err
	}
	if _, err := Record(ctx, q, AuditEvent{Kind: "pipeline.deploy_rejected", Summary: fmt.Sprintf("Rejected deploying %s: %s", p.Name, comment),
		ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "pipeline", ResourceID: &p.ID,
		Details: map[string]any{"change_request": cr.ID.String()}}); err != nil {
		return nil, err
	}
	if cr.RequestedByID != nil {
		if _, err := Notify(ctx, q, []uuid.UUID{*cr.RequestedByID}, Notification{ProjectID: a.Project.ID, Kind: "approval_decided",
			Title: "Rejected: " + p.Name, Body: a.Principal.User.Email + ": " + comment, Link: pipelineLink(a, p)}); err != nil {
			return nil, err
		}
	}
	return cr, nil
}

// CancelChangeRequest cancels a request (requester or approver).
func CancelChangeRequest(ctx context.Context, q db.Querier, a *auth.ProjectAccess, id uuid.UUID) (*models.ChangeRequest, error) {
	cr, p, err := getChangeRequest(ctx, q, a, id)
	if err != nil {
		return nil, err
	}
	if (cr.RequestedByID == nil || *cr.RequestedByID != a.Principal.User.ID) && !a.Can(auth.PipelineApprove) {
		return nil, problem.New(403, "forbidden", "Only the requester or an approver can cancel this request.")
	}
	if err := decide(ctx, q, a, cr, "cancelled", ""); err != nil {
		return nil, err
	}
	_, err = Record(ctx, q, AuditEvent{Kind: "pipeline.deploy_request_cancelled", Summary: "Cancelled the deploy request for " + p.Name,
		ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "pipeline", ResourceID: &p.ID})
	return cr, err
}

// EmailOf is a user's email (nil for unknown ids).
func EmailOf(ctx context.Context, q db.Querier, id *uuid.UUID) (*string, error) {
	if id == nil {
		return nil, nil
	}
	u, err := db.Find[models.User](ctx, q, `SELECT * FROM users WHERE id = $1`, *id)
	if err != nil || u == nil {
		return nil, err
	}
	return &u.Email, nil
}
