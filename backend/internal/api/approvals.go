package api

import (
	"context"
	"net/http"
	"reflect"
	"sort"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// ChangeRequestRead is a deploy waiting for (or decided by) a second person.
type ChangeRequestRead struct {
	ID               uuid.UUID    `json:"id"`
	PipelineID       uuid.UUID    `json:"pipeline_id"`
	PipelineName     string       `json:"pipeline_name"`
	Version          int          `json:"version"`
	State            string       `json:"state"`
	RequestedBy      *string      `json:"requested_by"`
	ForceReason      *string      `json:"force_reason"`
	Note             string       `json:"note"`
	PreflightOK      *bool        `json:"preflight_ok"`
	PreflightSummary string       `json:"preflight_summary"`
	DecidedBy        *string      `json:"decided_by"`
	DecidedAt        *pytime.Time `json:"decided_at"`
	Comment          string       `json:"comment"`
	CreatedAt        pytime.Time  `json:"created_at"`
}

func changeRead(ctx context.Context, q db.Querier, cr *models.ChangeRequest) (*ChangeRequestRead, error) {
	name, err := db.FindScalar[string](ctx, q, `SELECT name FROM pipelines WHERE id = $1`, cr.PipelineID)
	if err != nil {
		return nil, err
	}
	number, err := db.FindScalar[int](ctx, q, `SELECT number FROM pipeline_versions WHERE id = $1`, cr.VersionID)
	if err != nil {
		return nil, err
	}
	requested, err := services.EmailOf(ctx, q, cr.RequestedByID)
	if err != nil {
		return nil, err
	}
	decided, err := services.EmailOf(ctx, q, cr.DecidedByID)
	if err != nil {
		return nil, err
	}
	out := &ChangeRequestRead{ID: cr.ID, PipelineID: cr.PipelineID, PipelineName: "?", State: cr.State, RequestedBy: requested,
		ForceReason: cr.ForceReason, Note: cr.Note, DecidedBy: decided, DecidedAt: pytime.P(cr.DecidedAt), Comment: cr.Comment,
		CreatedAt: pytime.T(cr.CreatedAt)}
	if name != nil {
		out.PipelineName = *name
	}
	if number != nil {
		out.Version = *number
	}
	if ok, isBool := cr.Preflight["ok"].(bool); isBool {
		out.PreflightOK = &ok
	}
	if s, ok := cr.Preflight["summary"]; ok {
		out.PreflightSummary = pyStr(s)
	}
	return out, nil
}

func approvalRoutes(r chi.Router) {
	r.Get("/projects/{project_id}/change-requests", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		qs := httpapi.Q(r)
		state := qs.StrRules("state", val.StrRules{Pattern: `^(pending|approved|rejected|cancelled|superseded)$`})
		if err := qs.Err(); err != nil {
			return err
		}
		if err := a.Require(auth.PipelineView); err != nil {
			return err
		}
		ctx := r.Context()
		q := pool(ctx)
		sql, args := `SELECT * FROM change_requests WHERE project_id = $1`, []any{a.Project.ID}
		if state != nil && *state != "" {
			sql, args = sql+` AND state = $2`, append(args, *state)
		}
		rows, err := db.Select[models.ChangeRequest](ctx, q, sql+` ORDER BY created_at DESC LIMIT 200`, args...)
		if err != nil {
			return err
		}
		out := make([]*ChangeRequestRead, len(rows))
		for i := range rows {
			if out[i], err = changeRead(ctx, q, &rows[i]); err != nil {
				return err
			}
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	decide := func(fn func(ctx context.Context, q db.Querier, a *auth.ProjectAccess, id uuid.UUID, comment string) (*models.ChangeRequest, error), withBody bool) http.HandlerFunc {
		return httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
			id, err := pathID(r, "request_id")
			if err != nil {
				return err
			}
			a, err := project(r)
			if err != nil {
				return err
			}
			comment := ""
			if withBody {
				if comment, err = body(r, func(v *val.V, o *val.Object) string {
					return str(v, o, "comment", "", val.StrRules{Max: 2000}, false)
				}); err != nil {
					return err
				}
			}
			ctx := r.Context()
			var out *ChangeRequestRead
			err = tx(ctx, func(q db.Querier) error {
				cr, err := fn(ctx, q, a, id, comment)
				if err != nil {
					return err
				}
				out, err = changeRead(ctx, q, cr)
				return err
			})
			if err != nil {
				return err
			}
			httpapi.JSON(w, http.StatusOK, out)
			return nil
		})
	}
	r.Post("/projects/{project_id}/change-requests/{request_id}/approve", decide(services.Approve, true))
	r.Post("/projects/{project_id}/change-requests/{request_id}/reject", decide(services.Reject, true))
	r.Post("/projects/{project_id}/change-requests/{request_id}/cancel", decide(
		func(ctx context.Context, q db.Querier, a *auth.ProjectAccess, id uuid.UUID, _ string) (*models.ChangeRequest, error) {
			return services.CancelChangeRequest(ctx, q, a, id)
		}, false))
	r.Get("/projects/{project_id}/policies", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		if err := a.Require(auth.ProjectView); err != nil {
			return err
		}
		ctx := r.Context()
		p, err := services.PoliciesOf(ctx, pool(ctx), a.Project.ID)
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, p)
		return nil
	}))
	// How many pipelines proposed setting limits would refuse (counted before an Admin saves).
	r.Post("/projects/{project_id}/policies/setting-limits:impact", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		raw, p := httpapi.Body(r, true)
		if p != nil {
			return p
		}
		v := &val.V{}
		policies, ok := domain.ParsePolicies(v, val.Loc{"body"}, raw)
		if !ok {
			return v.Problem()
		}
		if err := a.Require(auth.ProjectManage); err != nil {
			return err
		}
		if err := services.ValidateSettingLimits(policies.SettingLimits); err != nil {
			return err
		}
		ctx := r.Context()
		out, err := services.SettingLimitImpact(ctx, pool(ctx), a.Project.ID, policies.SettingLimits)
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, map[string]any{"items": out})
		return nil
	}))
	r.Put("/projects/{project_id}/policies", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		raw, p := httpapi.Body(r, true)
		if p != nil {
			return p
		}
		v := &val.V{}
		policies, ok := domain.ParsePolicies(v, val.Loc{"body"}, raw)
		if !ok {
			return v.Problem()
		}
		if err := a.Require(auth.ProjectManage); err != nil {
			return err
		}
		ctx := r.Context()
		err = tx(ctx, func(q db.Querier) error {
			current, err := services.PoliciesOf(ctx, q, a.Project.ID)
			if err != nil {
				return err
			}
			// New or changed limits must fit their settings; stored ones a later registry dropped
			// are kept as they are (shown obsolete, ignored).
			fresh := map[string]domain.SettingLimit{}
			for k, l := range policies.SettingLimits {
				if old, kept := current.SettingLimits[k]; !kept || !reflect.DeepEqual(old, l) {
					fresh[k] = l
				}
			}
			if err := services.ValidateSettingLimits(fresh); err != nil {
				return err
			}
			if err != nil {
				return err
			}
			before, after := current.Dump(), policies.Dump()
			var changed []string
			// Policies added after the Python releases are dumped only when set, so a key
			// can disappear: compare both sides.
			keys := after.Keys()
			for _, k := range before.Keys() {
				if _, ok := after.Get(k); !ok {
					keys = append(keys, k)
				}
			}
			for _, k := range keys {
				x, _ := after.Get(k)
				y, _ := before.Get(k)
				xs, _ := pyjson.Dumps(x)
				ys, _ := pyjson.Dumps(y)
				if xs != ys {
					changed = append(changed, k)
				}
			}
			sort.Strings(changed)
			plain, _ := om.Plain(after).(map[string]any)
			if _, err := q.Exec(ctx, `UPDATE projects SET policies = $1, updated_at = $2 WHERE id = $3`, db.Map(plain), db.Now(), a.Project.ID); err != nil {
				return err
			}
			summary := strings.Join(changed, ", ")
			if summary == "" {
				summary = "no change"
			}
			_, err = services.Record(ctx, q, services.AuditEvent{Kind: "project.policies_updated", Summary: "Changed project policies: " + summary,
				ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "project", ResourceID: &a.Project.ID,
				Details: map[string]any{"before": before, "after": after}})
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, policies)
		return nil
	}))
}
