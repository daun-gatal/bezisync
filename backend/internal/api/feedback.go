package api

import (
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

var feedbackKinds = []string{"bug", "security", "idea", "other"}

// FeedbackRead is one piece of feedback.
type FeedbackRead struct {
	ID          uuid.UUID    `json:"id"`
	Kind        string       `json:"kind"`
	Message     string       `json:"message"`
	Page        string       `json:"page"`
	UserAgent   string       `json:"user_agent"`
	Email       string       `json:"email"`
	ProjectID   uuid.UUID    `json:"project_id"`
	ProjectName string       `json:"project_name"`
	CreatedAt   pytime.Time  `json:"created_at"`
	ResolvedAt  *pytime.Time `json:"resolved_at"`
	ResolvedBy  *string      `json:"resolved_by"`
}

func feedbackRead(f *models.Feedback, projectName string) FeedbackRead {
	return FeedbackRead{ID: f.ID, Kind: f.Kind, Message: f.Message, Page: f.Page, UserAgent: f.UserAgent, Email: f.Email, ProjectID: f.ProjectID,
		ProjectName: projectName, CreatedAt: pytime.T(f.CreatedAt), ResolvedAt: pytime.P(f.ResolvedAt), ResolvedBy: f.ResolvedBy}
}

func feedbackRoutes(r chi.Router) {
	r.Post("/projects/{project_id}/feedback", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		type create struct{ Kind, Message, Page string }
		b, err := body(r, func(v *val.V, o *val.Object) create {
			c := create{}
			if f := o.Get("kind"); f.Required() {
				c.Kind, _ = v.Literal(f.Loc, f.Raw, feedbackKinds...)
			}
			if f := o.Get("message"); f.Required() {
				if s, ok := v.Constrained(f.Loc, f.Raw, val.StrRules{Max: 5000}); ok {
					if c.Message = domain.PyStrip(s); c.Message == "" {
						v.Fail(f.Loc, "value_error", "Value error, Write a message.")
					}
				}
			}
			// The app path the sender was on; anything else is dropped.
			c.Page = str(v, o, "page", "", val.StrRules{Max: 500}, false)
			if !strings.HasPrefix(c.Page, "/") || strings.HasPrefix(c.Page, "//") {
				c.Page = ""
			}
			return c
		})
		if err != nil {
			return err
		}
		if a.Principal.APIToken != nil {
			return problem.New(403, "session_required", "Send feedback from the app, not with an API token.")
		}
		ctx := r.Context()
		var out FeedbackRead
		err = tx(ctx, func(q db.Querier) error {
			item, err := services.SubmitFeedback(ctx, q, a.Principal, a.Project, b.Kind, b.Message, b.Page, r.Header.Get("User-Agent"))
			if err != nil {
				return err
			}
			out = feedbackRead(item, a.Project.Name)
			return nil
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusCreated, out)
		return nil
	}))
	r.Get("/feedback", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		qs := httpapi.Q(r)
		status := qs.Literal("status", "open", "open", "resolved", "all")
		var kind *string
		if k := qs.OptStr("kind"); k != nil {
			s := qs.Literal("kind", "", feedbackKinds...)
			kind = &s
		}
		cursor := qs.Str("cursor")
		limit := qs.Int("limit", 50, val.Num{})
		if err := qs.Err(); err != nil {
			return err
		}
		if _, err := superuser(r); err != nil {
			return err
		}
		ctx := r.Context()
		q := pool(ctx)
		rows, next, names, err := services.FeedbackPage(ctx, q, status, kind, cursor, limit)
		if err != nil {
			return err
		}
		open, resolved, err := services.FeedbackCounts(ctx, q)
		if err != nil {
			return err
		}
		items := make([]FeedbackRead, len(rows))
		for i := range rows {
			items[i] = feedbackRead(&rows[i], names[rows[i].ProjectID])
		}
		httpapi.JSON(w, http.StatusOK, obj("items", items, "next_cursor", next, "open", open, "resolved", resolved))
		return nil
	}))
	r.Patch("/feedback/{feedback_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "feedback_id")
		if err != nil {
			return err
		}
		p, err := superuser(r)
		if err != nil {
			return err
		}
		resolved, err := body(r, func(v *val.V, o *val.Object) bool {
			f := o.Get("resolved")
			if !f.Required() {
				return false
			}
			b, _ := v.Bool(f.Loc, f.Raw)
			return b
		})
		if err != nil {
			return err
		}
		ctx := r.Context()
		var out FeedbackRead
		err = tx(ctx, func(q db.Querier) error {
			item, err := services.SetFeedbackResolved(ctx, q, p, id, resolved)
			if err != nil {
				return err
			}
			name, err := db.FindScalar[string](ctx, q, `SELECT name FROM projects WHERE id = $1`, item.ProjectID)
			if err != nil {
				return err
			}
			pn := ""
			if name != nil {
				pn = *name
			}
			out = feedbackRead(item, pn)
			return nil
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
}
