package api

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/ratelimit"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// ConnectionRead is a connection; secrets are never returned, only which fields are set.
type ConnectionRead struct {
	ID              uuid.UUID         `json:"id"`
	Kind            string            `json:"kind"`
	Type            string            `json:"type"`
	Name            string            `json:"name"`
	Description     string            `json:"description"`
	Provider        string            `json:"provider"`
	Config          db.Map            `json:"config"`
	SecretFieldsSet []string          `json:"secret_fields_set"`
	SecretRefs      map[string]string `json:"secret_refs"`
	// Required secrets nobody has entered yet (a copy from another project): it can't be used until they are.
	MissingSecrets []string       `json:"missing_secrets"`
	Managed        bool           `json:"managed"`
	LastCheck      *checks.Report `json:"last_check"`
	LastCheckedAt  *pytime.Time   `json:"last_checked_at"`
	Version        int            `json:"version"`
	UsedBy         int            `json:"used_by"`
	CreatedAt      pytime.Time    `json:"created_at"`
	UpdatedAt      pytime.Time    `json:"updated_at"`
}

func connectionRead(ctx context.Context, q db.Querier, c *models.Connection) (ConnectionRead, error) {
	fields, err := services.SecretFieldsSet(c)
	if err != nil {
		return ConnectionRead{}, err
	}
	refs, err := services.SecretRefsOf(c)
	if err != nil {
		return ConnectionRead{}, err
	}
	used, err := services.ConnectionUsage(ctx, q, c.ID)
	if err != nil {
		return ConnectionRead{}, err
	}
	missing, err := services.ConnectionMissingSecrets(c)
	if err != nil {
		return ConnectionRead{}, err
	}
	var last *checks.Report
	if len(c.LastCheck) > 0 {
		r, err := services.ParseReport(c.LastCheck)
		if err != nil {
			return ConnectionRead{}, err
		}
		last = &r
	}
	cfg := c.Config
	if cfg == nil {
		cfg = db.Map{}
	}
	if refs == nil {
		refs = map[string]string{}
	}
	return ConnectionRead{ID: c.ID, Kind: c.Kind, Type: c.Type, Name: c.Name, Description: c.Description, Provider: c.Provider, Config: cfg,
		SecretFieldsSet: fields, SecretRefs: refs, MissingSecrets: missing, Managed: c.Managed, LastCheck: last, LastCheckedAt: pytime.P(c.LastCheckedAt),
		Version: c.Version, UsedBy: len(used), CreatedAt: pytime.T(c.CreatedAt), UpdatedAt: pytime.T(c.UpdatedAt)}, nil
}

// projectConnection loads a connection of the project (others look missing).
func projectConnection(ctx context.Context, q db.Querier, a *auth.ProjectAccess, id uuid.UUID) (*models.Connection, error) {
	c, err := services.GetConnection(ctx, q, id)
	if err != nil {
		return nil, err
	}
	if c == nil || c.ProjectID != a.Project.ID {
		return nil, problem.NotFound("Connection")
	}
	return c, nil
}

// parseIfMatch reads the edited version from If-Match (a bare or quoted integer).
func parseIfMatch(r *http.Request) (int, error) {
	h := r.Header.Get("If-Match")
	if h == "" {
		return 0, problem.New(428, "precondition_required", "Send If-Match with the version you edited.")
	}
	s := strings.Trim(strings.TrimPrefix(strings.TrimSpace(h), "W/"), `"`)
	n, err := pyInt(s)
	if err != nil {
		return 0, problem.New(400, "bad_if_match", "If-Match must be the resource version.")
	}
	return n, nil
}

// pyInt parses like Python's int(str): surrounding whitespace, a sign and single
// underscores between digits are allowed.
func pyInt(s string) (int, error) {
	s = strings.TrimSpace(s)
	body := strings.TrimLeft(s, "+-")
	if len(s)-len(body) > 1 || body == "" || body[0] == '_' || body[len(body)-1] == '_' || strings.Contains(body, "__") {
		return 0, strconv.ErrSyntax
	}
	return strconv.Atoi(strings.ReplaceAll(strings.TrimPrefix(s, "+"), "_", ""))
}

func requireManage(a *auth.ProjectAccess, c *models.Connection, managed *bool) error {
	if err := a.Require(auth.ConnectionManage); err != nil {
		return err
	}
	if ((c != nil && c.Managed) || (managed != nil && *managed)) && !a.Can(auth.ProjectManage) {
		return problem.New(403, "managed_connection", "Only Admins can change managed connections.")
	}
	return nil
}

func kindOf(v *val.V, o *val.Object, key string) string {
	f := o.Get(key)
	if !f.Required() {
		return ""
	}
	s, _ := v.Literal(f.Loc, f.Raw, "source", "sink")
	return s
}

func sortedKeys(m map[string]any) []any {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]any, len(keys))
	for i, k := range keys {
		out[i] = k
	}
	return out
}

func connectionRoutes(r chi.Router) {
	r.Get("/projects/{project_id}/connector-types", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		if _, err := project(r); err != nil {
			return err
		}
		ds := connectors.Descriptors()
		out := make([]any, len(ds))
		for i, d := range ds {
			out[i] = d.Descriptor
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Get("/projects/{project_id}/connections", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		qs := httpapi.Q(r)
		kind := qs.Literal("kind", "", "source", "sink")
		if err := qs.Err(); err != nil {
			return err
		}
		if err := a.Require(auth.ConnectionView); err != nil {
			return err
		}
		ctx := r.Context()
		q := pool(ctx)
		sql, args := `SELECT * FROM connections WHERE project_id = $1`, []any{a.Project.ID}
		if kind != "" {
			sql, args = sql+` AND kind = $2`, append(args, kind)
		}
		rows, err := db.Select[models.Connection](ctx, q, sql+` ORDER BY name`, args...)
		if err != nil {
			return err
		}
		out := make([]ConnectionRead, len(rows))
		for i := range rows {
			if out[i], err = connectionRead(ctx, q, &rows[i]); err != nil {
				return err
			}
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Post("/projects/{project_id}/connections", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		type create struct {
			Kind, Type, Name, Description, Provider string
			Config, Secrets                         map[string]any
			Managed                                 bool
		}
		b, err := body(r, func(v *val.V, o *val.Object) create {
			c := create{Kind: kindOf(v, o, "kind"), Type: str(v, o, "type", "", val.StrRules{}, true)}
			if s := slug(v, o, "name", true); s != nil {
				c.Name = *s
			}
			c.Description = str(v, o, "description", "", val.StrRules{Max: 2000}, false)
			c.Provider = str(v, o, "provider", "self_hosted", val.StrRules{}, false)
			c.Config = dict(v, o, "config", true)
			c.Secrets = dict(v, o, "secrets", false)
			if c.Secrets == nil {
				c.Secrets = map[string]any{}
			}
			c.Managed = boolean(v, o, "managed", false)
			return c
		})
		if err != nil {
			return err
		}
		if err := requireManage(a, nil, &b.Managed); err != nil {
			return err
		}
		config, secretValues, err := services.ValidateConnection(b.Kind, b.Type, b.Config, b.Secrets)
		if err != nil {
			return err
		}
		ctx := r.Context()
		var out ConnectionRead
		err = tx(ctx, func(q db.Querier) error {
			if err := services.NormalizeLinks(ctx, q, a.Project.ID, b.Kind, b.Type, config); err != nil {
				return err
			}
			if err := services.ValidateRefs(ctx, q, a.Project.ID, secretValues); err != nil {
				return err
			}
			enc, err := services.EncryptOrNil(secretValues)
			if err != nil {
				return err
			}
			now := db.Now()
			c := &models.Connection{ID: db.NewID(), ProjectID: a.Project.ID, Kind: b.Kind, Type: b.Type, Name: b.Name, Description: b.Description,
				Provider: b.Provider, Config: db.Map(config), SecretsEnc: enc, Managed: b.Managed, Version: 1, CreatedByID: &a.Principal.User.ID,
				CreatedAt: now, UpdatedAt: now}
			if err := db.Insert(ctx, q, "connections", c); err != nil {
				if db.IsUniqueViolation(err) {
					return problem.Conflict("connection_exists", fmt.Sprintf("A connection named '%s' already exists.", b.Name), nil)
				}
				return err
			}
			if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "connection.created", Summary: fmt.Sprintf("Added %s %s (%s)", b.Kind, b.Name, b.Type),
				ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "connection", ResourceID: &c.ID,
				Details: map[string]any{"secret_fields": sortedKeys(secretValues)}}); err != nil {
				return err
			}
			out, err = connectionRead(ctx, q, c)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusCreated, out)
		return nil
	}))
	r.Get("/projects/{project_id}/connections/{connection_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "connection_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		if err := a.Require(auth.ConnectionView); err != nil {
			return err
		}
		ctx := r.Context()
		c, err := projectConnection(ctx, pool(ctx), a, id)
		if err != nil {
			return err
		}
		out, err := connectionRead(ctx, pool(ctx), c)
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Patch("/projects/{project_id}/connections/{connection_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "connection_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		type update struct {
			Description, Provider *string
			Config, Secrets       map[string]any
			HasConfig, HasSecrets bool
			Managed               *bool
		}
		b, err := body(r, func(v *val.V, o *val.Object) update {
			u := update{Description: optStr(v, o, "description", val.StrRules{Max: 2000}), Provider: optStr(v, o, "provider", val.StrRules{})}
			u.Config, u.HasConfig = optDict(v, o, "config")
			u.Secrets, u.HasSecrets = optDict(v, o, "secrets")
			u.Managed = optBool(v, o, "managed")
			return u
		})
		if err != nil {
			return err
		}
		ctx := r.Context()
		var out ConnectionRead
		err = tx(ctx, func(q db.Querier) error {
			c, err := projectConnection(ctx, q, a, id)
			if err != nil {
				return err
			}
			if err := requireManage(a, c, b.Managed); err != nil {
				return err
			}
			edited, err := parseIfMatch(r)
			if err != nil {
				return err
			}
			if edited != c.Version {
				return problem.New(412, "version_conflict", "Someone else changed this connection. Reload and try again.").
					WithExtra(map[string]any{"current_version": c.Version})
			}
			stored, err := services.StoredSecrets(c)
			if err != nil {
				return err
			}
			merged := services.MergeSecrets(stored, b.Secrets)
			cfg := map[string]any(c.Config)
			if b.HasConfig {
				cfg = b.Config
			}
			config, secretValues, err := services.ValidateConnection(c.Kind, c.Type, cfg, merged)
			if err != nil {
				return err
			}
			if err := services.NormalizeLinks(ctx, q, a.Project.ID, c.Kind, c.Type, config); err != nil {
				return err
			}
			if err := services.ValidateRefs(ctx, q, a.Project.ID, secretValues); err != nil {
				return err
			}
			enc, err := services.EncryptOrNil(secretValues)
			if err != nil {
				return err
			}
			c.Config, c.SecretsEnc = db.Map(config), enc
			if b.Description != nil {
				c.Description = *b.Description
			}
			if b.Provider != nil {
				c.Provider = *b.Provider
			}
			if b.Managed != nil {
				c.Managed = *b.Managed
			}
			c.Version++
			c.LastCheck, c.LastCheckOK = nil, nil
			c.UpdatedAt = db.Now()
			if err := db.Update(ctx, q, "connections", c); err != nil {
				return err
			}
			if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "connection.updated", Summary: fmt.Sprintf("Updated %s %s", c.Kind, c.Name),
				ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "connection", ResourceID: &c.ID,
				Details: map[string]any{"secrets_changed": sortedKeys(b.Secrets), "config_changed": b.HasConfig}}); err != nil {
				return err
			}
			out, err = connectionRead(ctx, q, c)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Delete("/projects/{project_id}/connections/{connection_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "connection_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		ctx := r.Context()
		err = tx(ctx, func(q db.Querier) error {
			c, err := projectConnection(ctx, q, a, id)
			if err != nil {
				return err
			}
			if err := requireManage(a, c, nil); err != nil {
				return err
			}
			users, err := services.ConnectionUsage(ctx, q, c.ID)
			if err != nil {
				return err
			}
			if len(users) > 0 {
				shown := users
				if len(shown) > 5 {
					shown = shown[:5]
				}
				return problem.Conflict("connection_in_use", fmt.Sprintf("Used by %d pipeline(s): %s.", len(users), strings.Join(shown, ", ")),
					map[string]any{"pipelines": users})
			}
			// Deleted pipelines keep their rows (history, audit), which still name the connection.
			history, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM pipelines WHERE (source_id = $1 OR sink_id = $1) AND deleted_at IS NOT NULL`, c.ID)
			if err != nil {
				return err
			}
			if history > 0 {
				return problem.Conflict("connection_in_history", fmt.Sprintf("%d deleted pipeline(s) keep their history with this connection, so it can't be deleted.", history),
					map[string]any{"deleted_pipelines": history})
			}
			linkedBy, err := services.LinkedBy(ctx, q, c)
			if err != nil {
				return err
			}
			if len(linkedBy) > 0 {
				return problem.Conflict("connection_in_use", fmt.Sprintf("Used by the connection(s) %s.", strings.Join(linkedBy, ", ")),
					map[string]any{"connections": linkedBy})
			}
			if err := db.Delete(ctx, q, "connections", c.ID); err != nil {
				return err
			}
			_, err = services.Record(ctx, q, services.AuditEvent{Kind: "connection.deleted", Summary: fmt.Sprintf("Deleted %s %s", c.Kind, c.Name),
				ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "connection", ResourceID: &c.ID})
			return err
		})
		if err != nil {
			return err
		}
		httpapi.NoContent(w)
		return nil
	}))
	r.Post("/projects/{project_id}/connections/{connection_id}/check", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "connection_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		tables, _, err := optBody(r, func(v *val.V, o *val.Object) []string { return optStrList(v, o, "tables", 5000) })
		if err != nil {
			return err
		}
		if err := a.Require(auth.ConnectionView); err != nil {
			return err
		}
		if err := ratelimit.Check.Check("check:" + a.Principal.User.ID.String()); err != nil {
			return err
		}
		ctx := r.Context()
		var report checks.Report
		err = tx(ctx, func(q db.Querier) error {
			c, err := projectConnection(ctx, q, a, id)
			if err != nil {
				return err
			}
			report, err = services.CheckAndStore(ctx, q, c, tables)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, report)
		return nil
	}))
	r.Post("/projects/{project_id}/connections:check", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		type draft struct {
			Kind, Type, Provider string
			Config, Secrets      map[string]any
			ConnectionID         *uuid.UUID
			Tables               []string
		}
		b, err := body(r, func(v *val.V, o *val.Object) draft {
			d := draft{Kind: kindOf(v, o, "kind"), Type: str(v, o, "type", "", val.StrRules{}, true),
				Provider: str(v, o, "provider", "self_hosted", val.StrRules{}, false), Config: dict(v, o, "config", true)}
			d.Secrets = dict(v, o, "secrets", false)
			d.ConnectionID = optUUID(v, o, "connection_id")
			d.Tables = optStrList(v, o, "tables", 0)
			return d
		})
		if err != nil {
			return err
		}
		if err := a.Require(auth.ConnectionManage); err != nil {
			return err
		}
		if err := ratelimit.Check.Check("check:" + a.Principal.User.ID.String()); err != nil {
			return err
		}
		ctx := r.Context()
		q := pool(ctx)
		current := map[string]any{}
		if b.ConnectionID != nil {
			c, err := projectConnection(ctx, q, a, *b.ConnectionID)
			if err != nil {
				return err
			}
			if current, err = services.StoredSecrets(c); err != nil {
				return err
			}
		}
		config, secretValues, err := services.ValidateConnection(b.Kind, b.Type, b.Config, services.MergeSecrets(current, b.Secrets))
		if err != nil {
			return err
		}
		if err := services.NormalizeLinks(ctx, q, a.Project.ID, b.Kind, b.Type, config); err != nil {
			return err
		}
		if err := services.ValidateRefs(ctx, q, a.Project.ID, secretValues); err != nil {
			return err
		}
		resolved, err := services.ResolveRefs(ctx, q, a.Project.ID, secretValues)
		if p, ok := services.IsUnresolved(err); ok {
			httpapi.JSON(w, http.StatusOK, services.UnresolvedReport(p))
			return nil
		} else if err != nil {
			return err
		}
		checked := connectors.Resolved{Type: b.Type, Provider: b.Provider, Config: config, Secrets: resolved}
		if err := services.ResolveLinks(ctx, q, a.Project.ID, b.Kind, &checked); err != nil {
			return err
		}
		report, err := services.RunCheck(ctx, checked, b.Kind, b.Tables)
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, report)
		return nil
	}))
	r.Post("/projects/{project_id}/connections/{connection_id}/discover", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "connection_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		if err := a.Require(auth.ConnectionView); err != nil {
			return err
		}
		ctx := r.Context()
		c, err := projectConnection(ctx, pool(ctx), a, id)
		if err != nil {
			return err
		}
		d, err := services.Discover(ctx, pool(ctx), c)
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, d)
		return nil
	}))
	r.Get("/projects/{project_id}/connections/{connection_id}/usage", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "connection_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		if err := a.Require(auth.ConnectionView); err != nil {
			return err
		}
		ctx := r.Context()
		c, err := projectConnection(ctx, pool(ctx), a, id)
		if err != nil {
			return err
		}
		users, err := services.ConnectionUsage(ctx, pool(ctx), c.ID)
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, users)
		return nil
	}))
}
