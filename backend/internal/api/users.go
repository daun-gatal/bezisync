package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/security"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// UserProjectAccess is one project role of a user.
type UserProjectAccess struct {
	ProjectID    uuid.UUID `json:"project_id" db:"project_id"`
	Slug         string    `json:"slug" db:"slug"`
	Name         string    `json:"name" db:"name"`
	Role         string    `json:"role" db:"role"`
	CanApprove   bool      `json:"can_approve" db:"can_approve"`
	GrantedBySSO bool      `json:"granted_by_sso" db:"granted_by_sso"`
}

// UserAdminRead is an account as Admins see it.
type UserAdminRead struct {
	ID          uuid.UUID           `json:"id"`
	Email       string              `json:"email"`
	Name        string              `json:"name"`
	IsSuperuser bool                `json:"is_superuser"`
	IsActive    bool                `json:"is_active"`
	SSO         bool                `json:"sso"`
	HasPassword bool                `json:"has_password"`
	LastLoginAt *pytime.Time        `json:"last_login_at"`
	CreatedAt   pytime.Time         `json:"created_at"`
	Projects    []UserProjectAccess `json:"projects"`
}

func userAdminRead(ctx context.Context, q db.Querier, u *models.User) (UserAdminRead, error) {
	rows, err := db.Select[UserProjectAccess](ctx, q, `SELECT p.id AS project_id, p.slug, p.name, m.role, m.can_approve, m.granted_by_sso
		FROM memberships m JOIN projects p ON p.id = m.project_id WHERE m.user_id = $1 ORDER BY p.name`, u.ID)
	if err != nil {
		return UserAdminRead{}, err
	}
	return UserAdminRead{ID: u.ID, Email: u.Email, Name: u.Name, IsSuperuser: u.IsSuperuser, IsActive: u.IsActive, SSO: u.SSOSubject != nil,
		HasPassword: u.PasswordHash != nil, LastLoginAt: pytime.P(u.LastLoginAt), CreatedAt: pytime.T(u.CreatedAt), Projects: rows}, nil
}

func getUser(ctx context.Context, q db.Querier, id uuid.UUID) (*models.User, error) {
	u, err := db.Find[models.User](ctx, q, `SELECT * FROM users WHERE id = $1`, id)
	if err == nil && u == nil {
		err = problem.NotFound("User")
	}
	return u, err
}

func ensureOtherSuperuser(ctx context.Context, q db.Querier, u *models.User) error {
	n, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM users WHERE is_superuser IS TRUE AND is_active IS TRUE AND id != $1`, u.ID)
	if err != nil {
		return err
	}
	if n == 0 {
		return problem.New(409, "last_superuser", "BeziSync needs at least one active Admin.")
	}
	return nil
}

// upsertMembership sets a hand-made role (SSO group sync leaves it alone afterwards).
func upsertMembership(ctx context.Context, q db.Querier, projectID, userID uuid.UUID, role string, canApprove bool) (*models.Membership, error) {
	m, err := db.Find[models.Membership](ctx, q, `SELECT * FROM memberships WHERE project_id = $1 AND user_id = $2`, projectID, userID)
	if err != nil {
		return nil, err
	}
	now := db.Now()
	isNew := m == nil
	if isNew {
		m = &models.Membership{ID: db.NewID(), ProjectID: projectID, UserID: userID, CreatedAt: now}
	}
	m.Role, m.CanApprove, m.GrantedBySSO, m.UpdatedAt = role, canApprove, false, now
	if isNew {
		err = db.Insert(ctx, q, "memberships", m)
	} else {
		err = db.Update(ctx, q, "memberships", m)
	}
	return m, err
}

func userRoutes(r chi.Router) {
	r.Get("/users", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		qs := httpapi.Q(r)
		search := qs.StrRules("q", val.StrRules{Max: 200})
		projectID := qs.UUID("project_id")
		cursor := qs.Str("cursor")
		limit := qs.Int("limit", 50, val.Num{GE: val.F(1), LE: val.F(200)})
		if err := qs.Err(); err != nil {
			return err
		}
		if _, err := superuser(r); err != nil {
			return err
		}
		base := `SELECT * FROM users WHERE TRUE`
		var args []any
		if search != nil && *search != "" {
			args = append(args, "%"+strings.ToLower(*search)+"%")
			base += fmt.Sprintf(" AND (lower(email) LIKE $%d OR lower(name) LIKE $%d)", len(args), len(args))
		}
		if projectID != nil {
			args = append(args, *projectID)
			base += fmt.Sprintf(" AND (is_superuser IS TRUE OR id IN (SELECT user_id FROM memberships WHERE project_id = $%d))", len(args))
		}
		ctx := r.Context()
		q := pool(ctx)
		rows, next, err := paginate(ctx, q, base, args, "created_at", "id", cursor, limit, false,
			func(u *models.User) (time.Time, uuid.UUID) { return u.CreatedAt, u.ID })
		if err != nil {
			return err
		}
		page := Page[UserAdminRead]{Items: make([]UserAdminRead, len(rows)), NextCursor: next}
		for i := range rows {
			if page.Items[i], err = userAdminRead(ctx, q, &rows[i]); err != nil {
				return err
			}
		}
		httpapi.JSON(w, http.StatusOK, page)
		return nil
	}))
	r.Post("/users", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		p, err := superuser(r)
		if err != nil {
			return err
		}
		type create struct {
			Email, Name     string
			InitialPassword *string
			IsSuperuser     bool
		}
		b, err := body(r, func(v *val.V, o *val.Object) create {
			c := create{}
			if f := o.Get("email"); f.Required() {
				c.Email, _ = v.Email(f.Loc, f.Raw)
			}
			c.Name = str(v, o, "name", "", val.StrRules{Max: 200}, false)
			c.InitialPassword = optStr(v, o, "initial_password", val.StrRules{Min: 12, Max: 1024})
			c.IsSuperuser = boolean(v, o, "is_superuser", false)
			return c
		})
		if err != nil {
			return err
		}
		ctx := r.Context()
		var out UserAdminRead
		err = tx(ctx, func(q db.Querier) error {
			email := strings.ToLower(b.Email)
			n, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM users WHERE lower(email) = $1`, email)
			if err != nil {
				return err
			}
			if n > 0 {
				return problem.Conflict("user_exists", email+" already has an account.", nil)
			}
			// Without a password the account is for single sign-on: it binds by email at the first sign-in.
			var hash *string
			if b.InitialPassword != nil && *b.InitialPassword != "" {
				h, err := security.HashPassword(*b.InitialPassword)
				if err != nil {
					return err
				}
				hash = &h
			}
			now := db.Now()
			u := &models.User{ID: db.NewID(), Email: email, Name: b.Name, PasswordHash: hash, IsSuperuser: b.IsSuperuser, IsActive: true,
				UIMode: "guided", MutedNotifications: db.List{}, CreatedAt: now, UpdatedAt: now}
			if err := db.Insert(ctx, q, "users", u); err != nil {
				return err
			}
			summary := "Created account " + email
			if b.IsSuperuser {
				summary += " as Admin"
			}
			if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "user.created", Summary: summary, Principal: p,
				ResourceType: "user", ResourceID: &u.ID}); err != nil {
				return err
			}
			out, err = userAdminRead(ctx, q, u)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusCreated, out)
		return nil
	}))
	r.Get("/users/{user_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "user_id")
		if err != nil {
			return err
		}
		if _, err := superuser(r); err != nil {
			return err
		}
		ctx := r.Context()
		u, err := getUser(ctx, pool(ctx), id)
		if err != nil {
			return err
		}
		out, err := userAdminRead(ctx, pool(ctx), u)
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Patch("/users/{user_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "user_id")
		if err != nil {
			return err
		}
		p, err := superuser(r)
		if err != nil {
			return err
		}
		type update struct {
			Name                  *string
			IsSuperuser, IsActive *bool
		}
		b, err := body(r, func(v *val.V, o *val.Object) update {
			return update{Name: optStr(v, o, "name", val.StrRules{Max: 200}), IsSuperuser: optBool(v, o, "is_superuser"), IsActive: optBool(v, o, "is_active")}
		})
		if err != nil {
			return err
		}
		ctx := r.Context()
		var out UserAdminRead
		err = tx(ctx, func(q db.Querier) error {
			u, err := getUser(ctx, q, id)
			if err != nil {
				return err
			}
			var changes []string
			if b.Name != nil && *b.Name != u.Name {
				u.Name = *b.Name
				changes = append(changes, "name")
			}
			if b.IsSuperuser != nil && *b.IsSuperuser != u.IsSuperuser {
				if !*b.IsSuperuser {
					if u.ID == p.User.ID {
						return problem.New(409, "cannot_demote_self", "You cannot remove your own Admin role.")
					}
					if err := ensureOtherSuperuser(ctx, q, u); err != nil {
						return err
					}
				}
				u.IsSuperuser = *b.IsSuperuser
				changes = append(changes, map[bool]string{true: "made Admin", false: "Admin removed"}[*b.IsSuperuser])
			}
			if b.IsActive != nil && *b.IsActive != u.IsActive {
				if !*b.IsActive {
					if u.ID == p.User.ID {
						return problem.New(409, "cannot_deactivate_self", "You cannot deactivate your own account.")
					}
					if u.IsSuperuser {
						if err := ensureOtherSuperuser(ctx, q, u); err != nil {
							return err
						}
					}
					// Signed out everywhere now; API tokens stop working because the account is inactive.
					if _, err := q.Exec(ctx, `DELETE FROM user_sessions WHERE user_id = $1`, u.ID); err != nil {
						return err
					}
				}
				u.IsActive = *b.IsActive
				changes = append(changes, map[bool]string{true: "activated", false: "deactivated"}[*b.IsActive])
			}
			if len(changes) > 0 {
				u.UpdatedAt = db.Now()
				if err := db.Update(ctx, q, "users", u); err != nil {
					return err
				}
				anyChanges := make([]any, len(changes))
				for i, c := range changes {
					anyChanges[i] = c
				}
				if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "user.updated", Summary: fmt.Sprintf("Updated %s: %s", u.Email, strings.Join(changes, ", ")),
					Principal: p, ResourceType: "user", ResourceID: &u.ID, Details: map[string]any{"changes": anyChanges}}); err != nil {
					return err
				}
			}
			out, err = userAdminRead(ctx, q, u)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Put("/users/{user_id}/projects/{project_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		uid, err := pathID(r, "user_id")
		if err != nil {
			return err
		}
		pid, err := pathID(r, "project_id")
		if err != nil {
			return err
		}
		p, err := superuser(r)
		if err != nil {
			return err
		}
		type access struct {
			Role       string
			CanApprove bool
		}
		b, err := body(r, func(v *val.V, o *val.Object) access {
			return access{Role: roleOf(v, o, "role"), CanApprove: boolean(v, o, "can_approve", false)}
		})
		if err != nil {
			return err
		}
		ctx := r.Context()
		var out UserAdminRead
		err = tx(ctx, func(q db.Querier) error {
			u, err := getUser(ctx, q, uid)
			if err != nil {
				return err
			}
			pr, err := db.Find[models.Project](ctx, q, `SELECT * FROM projects WHERE id = $1`, pid)
			if err != nil {
				return err
			}
			if pr == nil {
				return problem.NotFound("Project")
			}
			if _, err := upsertMembership(ctx, q, pr.ID, u.ID, b.Role, b.CanApprove); err != nil {
				return err
			}
			if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "member.upserted", Summary: fmt.Sprintf("Set %s as %s", u.Email, b.Role),
				ProjectID: &pr.ID, Principal: p, ResourceType: "user", ResourceID: &u.ID,
				Details: map[string]any{"role": b.Role, "can_approve": b.CanApprove}}); err != nil {
				return err
			}
			out, err = userAdminRead(ctx, q, u)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Delete("/users/{user_id}/projects/{project_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		uid, err := pathID(r, "user_id")
		if err != nil {
			return err
		}
		pid, err := pathID(r, "project_id")
		if err != nil {
			return err
		}
		p, err := superuser(r)
		if err != nil {
			return err
		}
		ctx := r.Context()
		var out UserAdminRead
		err = tx(ctx, func(q db.Querier) error {
			u, err := getUser(ctx, q, uid)
			if err != nil {
				return err
			}
			m, err := db.Find[models.Membership](ctx, q, `SELECT * FROM memberships WHERE project_id = $1 AND user_id = $2`, pid, u.ID)
			if err != nil {
				return err
			}
			if m == nil {
				return problem.NotFound("Membership")
			}
			if err := db.Delete(ctx, q, "memberships", m.ID); err != nil {
				return err
			}
			if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "member.removed", Summary: "Removed " + u.Email, ProjectID: &pid,
				Principal: p, ResourceType: "user", ResourceID: &u.ID}); err != nil {
				return err
			}
			out, err = userAdminRead(ctx, q, u)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
}
