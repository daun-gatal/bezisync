package api

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/security"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// SlugRe is the slug syntax of projects, connections, runtimes and pipelines.
var SlugRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

const slugMessage = "use lowercase letters, digits and dashes (max 63)"

// slug validates a slug field (Value error when malformed).
func slug(v *val.V, o *val.Object, key string, required bool) *string {
	f := o.Get(key)
	if !f.Present {
		if required {
			f.Required()
		}
		return nil
	}
	if !required && f.IsNull() {
		return nil
	}
	s, ok := v.Str(f.Loc, f.Raw)
	if !ok {
		return nil
	}
	if !SlugRe.MatchString(s) {
		v.Fail(f.Loc, "value_error", "Value error, "+slugMessage)
		return nil
	}
	return &s
}

// ProjectRead is a project.
type ProjectRead struct {
	ID          uuid.UUID   `json:"id"`
	Slug        string      `json:"slug"`
	Name        string      `json:"name"`
	Description string      `json:"description"`
	IsSandbox   bool        `json:"is_sandbox"`
	CreatedAt   pytime.Time `json:"created_at"`
	Members     *int64      `json:"members"`
	Pipelines   *int64      `json:"pipelines"`
}

func projectRead(p *models.Project) ProjectRead {
	return ProjectRead{ID: p.ID, Slug: p.Slug, Name: p.Name, Description: p.Description, IsSandbox: p.IsSandbox, CreatedAt: pytime.T(p.CreatedAt)}
}

// MemberRead is a project member.
type MemberRead struct {
	UserID       uuid.UUID `json:"user_id"`
	Email        string    `json:"email"`
	Name         string    `json:"name"`
	Role         string    `json:"role"`
	CanApprove   bool      `json:"can_approve"`
	GrantedBySSO bool      `json:"granted_by_sso"`
}

// EventRead is an activity/audit event.
type EventRead struct {
	ID           uuid.UUID   `json:"id"`
	OccurredAt   pytime.Time `json:"occurred_at"`
	ActorLabel   string      `json:"actor_label"`
	Kind         string      `json:"kind"`
	ResourceType string      `json:"resource_type"`
	ResourceID   *uuid.UUID  `json:"resource_id"`
	Summary      string      `json:"summary"`
	Details      db.Map      `json:"details"`
	Audit        bool        `json:"audit"`
}

func eventRead(e *models.Event) EventRead {
	d := e.Details
	if d == nil {
		d = db.Map{}
	}
	return EventRead{ID: e.ID, OccurredAt: pytime.T(e.OccurredAt), ActorLabel: e.ActorLabel, Kind: e.Kind, ResourceType: e.ResourceType,
		ResourceID: e.ResourceID, Summary: e.Summary, Details: d, Audit: e.Audit}
}

// Page is a cursor page.
type Page[T any] struct {
	Items      []T     `json:"items"`
	NextCursor *string `json:"next_cursor"`
}

func roleOf(v *val.V, o *val.Object, key string) string {
	f := o.Get(key)
	if !f.Required() {
		return ""
	}
	s, _ := v.Enum(f.Loc, f.Raw, models.RoleEditor, models.RoleOperator, models.RoleViewer)
	return s
}

func projectRoutes(r chi.Router) {
	r.Post("/projects", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		p, err := superuser(r)
		if err != nil {
			return err
		}
		type create struct{ Slug, Name, Description string }
		b, err := body(r, func(v *val.V, o *val.Object) create {
			c := create{Name: str(v, o, "name", "", val.StrRules{Min: 1, Max: 200}, true), Description: str(v, o, "description", "", val.StrRules{Max: 2000}, false)}
			if s := slug(v, o, "slug", true); s != nil {
				c.Slug = *s
			}
			return c
		})
		if err != nil {
			return err
		}
		ctx := r.Context()
		var out ProjectRead
		err = tx(ctx, func(q db.Querier) error {
			n, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM projects WHERE slug = $1`, b.Slug)
			if err != nil {
				return err
			}
			if n > 0 {
				return problem.Conflict("project_exists", fmt.Sprintf("A project with slug '%s' already exists.", b.Slug), nil)
			}
			now := db.Now()
			pr := &models.Project{ID: db.NewID(), Slug: b.Slug, Name: b.Name, Description: b.Description, Policies: db.Map{}, CreatedAt: now, UpdatedAt: now}
			if err := db.Insert(ctx, q, "projects", pr); err != nil {
				return err
			}
			if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "project.created", Summary: "Created project " + pr.Name, ProjectID: &pr.ID,
				Principal: p, ResourceType: "project", ResourceID: &pr.ID}); err != nil {
				return err
			}
			out = projectRead(pr)
			return nil
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusCreated, out)
		return nil
	}))
	r.Get("/projects", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		p, err := principal(r)
		if err != nil {
			return err
		}
		ctx := r.Context()
		q := pool(ctx)
		var rows []models.Project
		if p.User.IsSuperuser {
			rows, err = db.Select[models.Project](ctx, q, `SELECT * FROM projects ORDER BY name`)
		} else {
			rows, err = db.Select[models.Project](ctx, q, `SELECT p.* FROM projects p JOIN memberships m ON m.project_id = p.id WHERE m.user_id = $1 ORDER BY p.name`, p.User.ID)
		}
		if err != nil {
			return err
		}
		counts := func(sql string) (map[uuid.UUID]int64, error) {
			cs, err := db.Select[struct {
				ID uuid.UUID `db:"project_id"`
				N  int64     `db:"n"`
			}](ctx, q, sql)
			out := map[uuid.UUID]int64{}
			for _, c := range cs {
				out[c.ID] = c.N
			}
			return out, err
		}
		members, err := counts(`SELECT project_id, count(*) AS n FROM memberships GROUP BY project_id`)
		if err != nil {
			return err
		}
		pipelines, err := counts(`SELECT project_id, count(*) AS n FROM pipelines WHERE deleted_at IS NULL GROUP BY project_id`)
		if err != nil {
			return err
		}
		out := make([]ProjectRead, len(rows))
		for i := range rows {
			pr := projectRead(&rows[i])
			m, pl := members[rows[i].ID], pipelines[rows[i].ID]
			pr.Members, pr.Pipelines = &m, &pl
			out[i] = pr
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Get("/projects/{project_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, projectRead(a.Project))
		return nil
	}))
	r.Patch("/projects/{project_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		type update struct{ Name, Description *string }
		b, err := body(r, func(v *val.V, o *val.Object) update {
			return update{Name: optStr(v, o, "name", val.StrRules{Min: 1, Max: 200}), Description: optStr(v, o, "description", val.StrRules{Max: 2000})}
		})
		if err != nil {
			return err
		}
		if err := a.Require(auth.ProjectManage); err != nil {
			return err
		}
		ctx := r.Context()
		err = tx(ctx, func(q db.Querier) error {
			if b.Name != nil {
				a.Project.Name = *b.Name
			}
			if b.Description != nil {
				a.Project.Description = *b.Description
			}
			a.Project.UpdatedAt = db.Now()
			if err := db.Update(ctx, q, "projects", a.Project); err != nil {
				return err
			}
			_, err := services.Record(ctx, q, services.AuditEvent{Kind: "project.updated", Summary: "Updated project settings", ProjectID: &a.Project.ID,
				Principal: a.Principal, ResourceType: "project", ResourceID: &a.Project.ID})
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, projectRead(a.Project))
		return nil
	}))
	r.Delete("/projects/{project_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "project_id")
		if err != nil {
			return err
		}
		p, err := superuser(r)
		if err != nil {
			return err
		}
		ctx := r.Context()
		err = tx(ctx, func(q db.Querier) error {
			pr, err := db.Find[models.Project](ctx, q, `SELECT * FROM projects WHERE id = $1`, id)
			if err != nil {
				return err
			}
			if pr == nil {
				return problem.NotFound("Project")
			}
			live, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM pipelines WHERE project_id = $1 AND deleted_at IS NULL`, pr.ID)
			if err != nil {
				return err
			}
			if live > 0 {
				return problem.Conflict("project_not_empty", fmt.Sprintf("Delete the project's %d pipeline(s) first, so their connectors and source objects are cleaned up.", live), nil)
			}
			busy, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM jobs WHERE project_id = $1 AND kind = 'cleanup' AND state IN ('queued', 'running')`, pr.ID)
			if err != nil {
				return err
			}
			if busy > 0 {
				return problem.Conflict("project_busy", "Deleted pipelines are still being cleaned up. Try again once that finishes.", nil)
			}
			if err := db.Delete(ctx, q, "projects", pr.ID); err != nil {
				return err
			}
			// Recorded without the project: its own audit log goes with it.
			_, err = services.Record(ctx, q, services.AuditEvent{Kind: "project.deleted", Summary: fmt.Sprintf("Deleted project %s (%s)", pr.Name, pr.Slug),
				Principal: p, ResourceType: "project", ResourceID: &id})
			return err
		})
		if err != nil {
			return err
		}
		httpapi.NoContent(w)
		return nil
	}))
	r.Get("/projects/{project_id}/members", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		ctx := r.Context()
		rows, err := db.Select[struct {
			UserID       uuid.UUID `db:"user_id"`
			Email        string    `db:"email"`
			Name         string    `db:"name"`
			Role         string    `db:"role"`
			CanApprove   bool      `db:"can_approve"`
			GrantedBySSO bool      `db:"granted_by_sso"`
		}](ctx, pool(ctx), `SELECT m.user_id, u.email, u.name, m.role, m.can_approve, m.granted_by_sso FROM memberships m JOIN users u ON u.id = m.user_id
			WHERE m.project_id = $1 ORDER BY u.email`, a.Project.ID)
		if err != nil {
			return err
		}
		out := make([]MemberRead, len(rows))
		for i, m := range rows {
			out[i] = MemberRead{UserID: m.UserID, Email: m.Email, Name: m.Name, Role: m.Role, CanApprove: m.CanApprove, GrantedBySSO: m.GrantedBySSO}
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Put("/projects/{project_id}/members", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		type upsert struct {
			Email, Role     string
			CanApprove      bool
			InitialPassword *string
		}
		b, err := body(r, func(v *val.V, o *val.Object) upsert {
			u := upsert{}
			if f := o.Get("email"); f.Required() {
				u.Email, _ = v.Email(f.Loc, f.Raw)
			}
			u.Role = roleOf(v, o, "role")
			u.CanApprove = boolean(v, o, "can_approve", false)
			u.InitialPassword = optStr(v, o, "initial_password", val.StrRules{Min: 12, Max: 1024})
			return u
		})
		if err != nil {
			return err
		}
		if err := a.Require(auth.MemberManage); err != nil {
			return err
		}
		ctx := r.Context()
		var out MemberRead
		err = tx(ctx, func(q db.Querier) error {
			u, err := db.Find[models.User](ctx, q, `SELECT * FROM users WHERE lower(email) = $1`, strings.ToLower(b.Email))
			if err != nil {
				return err
			}
			if u == nil {
				pw := security.TokenURLSafe(24)
				if b.InitialPassword != nil && *b.InitialPassword != "" {
					pw = *b.InitialPassword
				}
				hash, err := security.HashPassword(pw)
				if err != nil {
					return err
				}
				now := db.Now()
				u = &models.User{ID: db.NewID(), Email: strings.ToLower(b.Email), PasswordHash: &hash, IsActive: true, UIMode: "guided",
					MutedNotifications: db.List{}, CreatedAt: now, UpdatedAt: now}
				if err := db.Insert(ctx, q, "users", u); err != nil {
					return err
				}
			}
			m, err := upsertMembership(ctx, q, a.Project.ID, u.ID, b.Role, b.CanApprove)
			if err != nil {
				return err
			}
			if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "member.upserted", Summary: fmt.Sprintf("Set %s as %s", u.Email, b.Role),
				ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "user", ResourceID: &u.ID,
				Details: map[string]any{"role": b.Role, "can_approve": b.CanApprove}}); err != nil {
				return err
			}
			out = MemberRead{UserID: u.ID, Email: u.Email, Name: u.Name, Role: m.Role, CanApprove: m.CanApprove, GrantedBySSO: false}
			return nil
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Delete("/projects/{project_id}/members/{user_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		uid, err := pathID(r, "user_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		if err := a.Require(auth.MemberManage); err != nil {
			return err
		}
		ctx := r.Context()
		err = tx(ctx, func(q db.Querier) error {
			m, err := db.Find[models.Membership](ctx, q, `SELECT * FROM memberships WHERE project_id = $1 AND user_id = $2`, a.Project.ID, uid)
			if err != nil {
				return err
			}
			if m == nil {
				return problem.NotFound("Member")
			}
			if err := db.Delete(ctx, q, "memberships", m.ID); err != nil {
				return err
			}
			_, err = services.Record(ctx, q, services.AuditEvent{Kind: "member.removed", Summary: "Removed a member", ProjectID: &a.Project.ID,
				Principal: a.Principal, ResourceType: "user", ResourceID: &uid})
			return err
		})
		if err != nil {
			return err
		}
		httpapi.NoContent(w)
		return nil
	}))
	r.Get("/projects/{project_id}/events", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		q := httpapi.Q(r)
		resourceID := q.UUID("resource_id")
		auditOnly := q.Bool("audit_only", false)
		cursor := q.Str("cursor")
		limit := q.Int("limit", 50, val.Num{GE: val.F(1), LE: val.F(200)})
		if err := q.Err(); err != nil {
			return err
		}
		if auditOnly {
			if err := a.Require(auth.AuditView); err != nil {
				return err
			}
		}
		base := `SELECT * FROM events WHERE project_id = $1`
		args := []any{a.Project.ID}
		if resourceID != nil {
			args = append(args, *resourceID)
			base += fmt.Sprintf(" AND resource_id = $%d", len(args))
		}
		if auditOnly {
			base += " AND audit IS TRUE"
		}
		ctx := r.Context()
		rows, next, err := paginate(ctx, pool(ctx), base, args, "occurred_at", "id", cursor, limit, false,
			func(e *models.Event) (time.Time, uuid.UUID) { return e.OccurredAt, e.ID })
		if err != nil {
			return err
		}
		page := Page[EventRead]{Items: make([]EventRead, len(rows)), NextCursor: next}
		for i := range rows {
			page.Items[i] = eventRead(&rows[i])
		}
		httpapi.JSON(w, http.StatusOK, page)
		return nil
	}))
	r.Get("/projects/{project_id}/audit:export", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		q := httpapi.Q(r)
		format := q.Literal("format", "csv", "csv", "jsonl")
		since, until := q.OptStr("since"), q.OptStr("until")
		if err := q.Err(); err != nil {
			return err
		}
		vv := &val.V{}
		var sinceT, untilT *time.Time
		for _, p := range []struct {
			name string
			raw  *string
			out  **time.Time
		}{{"since", since, &sinceT}, {"until", until, &untilT}} {
			if p.raw != nil {
				if t, ok := vv.Datetime(val.Loc{"query", p.name}, *p.raw); ok {
					*p.out = &t
				}
			}
		}
		if p := vv.Problem(); p != nil {
			return p
		}
		if err := a.Require(auth.AuditView); err != nil {
			return err
		}
		sql := `SELECT * FROM events WHERE project_id = $1 AND audit IS TRUE`
		args := []any{a.Project.ID}
		if sinceT != nil {
			args = append(args, *sinceT)
			sql += fmt.Sprintf(" AND occurred_at >= $%d", len(args))
		}
		if untilT != nil {
			args = append(args, *untilT)
			sql += fmt.Sprintf(" AND occurred_at < $%d", len(args))
		}
		ctx := r.Context()
		rows, err := db.Select[models.Event](ctx, pool(ctx), sql+" ORDER BY occurred_at", args...)
		if err != nil {
			return err
		}
		var buf strings.Builder
		if format == "jsonl" {
			for i := range rows {
				line, err := pyjson.Encode(eventRow(&rows[i]), pyjson.Options{Compact: true, EnsureASCII: true})
				if err != nil {
					return err
				}
				buf.WriteString(line + "\n")
			}
		} else {
			columns := []string{"occurred_at", "actor_label", "kind", "resource_type", "resource_id", "summary", "details", "id"}
			writeCSVRow(&buf, columns)
			for i := range rows {
				e := eventRow(&rows[i])
				var cells []string
				for _, c := range columns {
					v, _ := e.Get(c)
					if c == "details" {
						s, _ := pyjson.Encode(v, pyjson.Options{EnsureASCII: true})
						cells = append(cells, s)
						continue
					}
					if v == nil {
						cells = append(cells, "")
					} else {
						cells = append(cells, fmt.Sprint(v))
					}
				}
				writeCSVRow(&buf, cells)
			}
		}
		media := "text/csv; charset=utf-8"
		if format == "jsonl" {
			media = "application/x-ndjson"
		}
		w.Header().Set("Content-Type", media)
		w.Header().Set("content-disposition", fmt.Sprintf(`attachment; filename="bezisync-audit-%s.%s"`, a.Project.Slug, format))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(buf.String()))
		return nil
	}))
}

// eventRow is EventRead.model_dump(mode="json") in field order.
func eventRow(e *models.Event) *om.Map {
	var rid any
	if e.ResourceID != nil {
		rid = e.ResourceID.String()
	}
	details := om.FromJSONB(map[string]any(e.Details))
	if e.Details == nil {
		details = om.New()
	}
	return om.New().Set("id", e.ID.String()).Set("occurred_at", pytime.Pydantic(e.OccurredAt)).Set("actor_label", e.ActorLabel).
		Set("kind", e.Kind).Set("resource_type", e.ResourceType).Set("resource_id", rid).Set("summary", e.Summary).
		Set("details", details).Set("audit", e.Audit)
}

// writeCSVRow writes one row like Python's csv.writer (QUOTE_MINIMAL, \r\n).
func writeCSVRow(b *strings.Builder, cells []string) {
	for i, c := range cells {
		if i > 0 {
			b.WriteByte(',')
		}
		if strings.ContainsAny(c, ",\"\r\n") {
			b.WriteByte('"')
			b.WriteString(strings.ReplaceAll(c, `"`, `""`))
			b.WriteByte('"')
		} else {
			b.WriteString(c)
		}
	}
	b.WriteString("\r\n")
}
