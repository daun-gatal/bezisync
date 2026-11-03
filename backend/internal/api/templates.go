package api

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// ConnectionRef names a connection or runtime.
type ConnectionRef struct {
	ID   uuid.UUID `json:"id"`
	Name string    `json:"name"`
	Type string    `json:"type"`
}

// TemplateRead is a pipeline template.
type TemplateRead struct {
	ID          uuid.UUID           `json:"id"`
	Name        string              `json:"name"`
	Title       string              `json:"title"`
	Description string              `json:"description"`
	Spec        domain.PipelineSpec `json:"spec"`
	Sink        *ConnectionRef      `json:"sink"`
	Runtime     *ConnectionRef      `json:"runtime"`
	CreatedAt   pytime.Time         `json:"created_at"`
	Version     int                 `json:"version"`
}

func connectionRef(ctx context.Context, q db.Querier, id *uuid.UUID) (*ConnectionRef, error) {
	if id == nil {
		return nil, nil
	}
	c, err := services.GetConnection(ctx, q, *id)
	if err != nil || c == nil {
		return nil, err
	}
	return &ConnectionRef{ID: c.ID, Name: c.Name, Type: c.Type}, nil
}

func runtimeRef(ctx context.Context, q db.Querier, id *uuid.UUID) (*ConnectionRef, error) {
	if id == nil {
		return nil, nil
	}
	rt, err := services.GetRuntime(ctx, q, *id)
	if err != nil || rt == nil {
		return nil, err
	}
	return &ConnectionRef{ID: rt.ID, Name: rt.Name, Type: rt.Kind}, nil
}

// stripTables drops tables and everything tied to them (column rules, transform steps limited
// to some tables, message key columns): those belong to each pipeline.
func stripTables(spec domain.PipelineSpec) domain.PipelineSpec {
	out := spec.Clone()
	out.Source.Tables = domain.Tables{Include: []string{}}
	out.Source.Columns = map[string]domain.ColumnRules{}
	steps := out.Transforms[:0]
	for _, t := range out.Transforms {
		if t.Tables == nil {
			steps = append(steps, t)
		}
	}
	out.Transforms = steps
	out.Keys = nil
	return out
}

func templateRead(ctx context.Context, q db.Querier, t *models.PipelineTemplate) (TemplateRead, error) {
	sink, err := connectionRef(ctx, q, t.SinkID)
	if err != nil {
		return TemplateRead{}, err
	}
	rt, err := runtimeRef(ctx, q, t.RuntimeID)
	if err != nil {
		return TemplateRead{}, err
	}
	return TemplateRead{ID: t.ID, Name: t.Name, Title: t.Title, Description: t.Description, Spec: domain.MustSpec(map[string]any(t.Spec)),
		Sink: sink, Runtime: rt, CreatedAt: pytime.T(t.CreatedAt), Version: t.Version}, nil
}

func getTemplate(ctx context.Context, q db.Querier, a *auth.ProjectAccess, id uuid.UUID) (*models.PipelineTemplate, error) {
	t, err := db.Find[models.PipelineTemplate](ctx, q, `SELECT * FROM pipeline_templates WHERE id = $1`, id)
	if err != nil {
		return nil, err
	}
	if t == nil || t.ProjectID != a.Project.ID {
		return nil, problem.NotFound("Template")
	}
	return t, nil
}

// optSpec reads an optional PipelineSpec field.
func optSpec(v *val.V, o *val.Object, key string) *domain.PipelineSpec {
	f := o.Get(key)
	if !f.Present || f.IsNull() {
		return nil
	}
	s, ok := domain.ParseSpec(v, f.Loc, f.Raw)
	if !ok {
		return nil
	}
	return &s
}

func templateRoutes(r chi.Router) {
	r.Get("/projects/{project_id}/templates", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		if err := a.Require(auth.PipelineView); err != nil {
			return err
		}
		ctx := r.Context()
		q := pool(ctx)
		rows, err := db.Select[models.PipelineTemplate](ctx, q, `SELECT * FROM pipeline_templates WHERE project_id = $1 ORDER BY title`, a.Project.ID)
		if err != nil {
			return err
		}
		out := make([]TemplateRead, len(rows))
		for i := range rows {
			if out[i], err = templateRead(ctx, q, &rows[i]); err != nil {
				return err
			}
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Post("/projects/{project_id}/templates", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		type create struct {
			Name, Title, Description string
			FromPipeline             *uuid.UUID
			Spec                     *domain.PipelineSpec
			SinkID, RuntimeID        *uuid.UUID
		}
		b, err := body(r, func(v *val.V, o *val.Object) create {
			c := create{}
			if s := slug(v, o, "name", true); s != nil {
				c.Name = *s
			}
			c.Title = str(v, o, "title", "", val.StrRules{Min: 1, Max: 200}, true)
			c.Description = str(v, o, "description", "", val.StrRules{Max: 2000}, false)
			c.FromPipeline = optUUID(v, o, "from_pipeline_id")
			c.Spec = optSpec(v, o, "spec")
			c.SinkID, c.RuntimeID = optUUID(v, o, "sink_id"), optUUID(v, o, "runtime_id")
			return c
		})
		if err != nil {
			return err
		}
		if err := a.Require(auth.ProjectManage); err != nil {
			return err
		}
		ctx := r.Context()
		var out TemplateRead
		err = tx(ctx, func(q db.Querier) error {
			spec := domain.DefaultSpec()
			if b.Spec != nil {
				spec = *b.Spec
			}
			sinkID, runtimeID := b.SinkID, b.RuntimeID
			if b.FromPipeline != nil {
				src, err := services.GetPipeline(ctx, q, a, *b.FromPipeline, false)
				if err != nil {
					return err
				}
				current, err := services.SpecOf(ctx, q, src.CurrentVersionID)
				if err != nil {
					return err
				}
				if current == nil {
					return problem.New(409, "no_version", "That pipeline has no saved version yet.")
				}
				spec = *current
				if sinkID == nil {
					sinkID = src.SinkID
				}
				if runtimeID == nil {
					runtimeID = src.RuntimeID
				}
			}
			if sinkID != nil {
				sink, err := services.GetConnection(ctx, q, *sinkID)
				if err != nil {
					return err
				}
				if sink == nil || sink.ProjectID != a.Project.ID || sink.Kind != "sink" {
					return problem.NotFound("Destination")
				}
			}
			if runtimeID != nil {
				rt, err := services.GetRuntime(ctx, q, *runtimeID)
				if err != nil {
					return err
				}
				if rt == nil || rt.ProjectID != a.Project.ID {
					return problem.NotFound("Runtime")
				}
			}
			if err := services.EnforceSavePolicies(ctx, q, a.Project.ID, runtimeID, nil, sinkID); err != nil {
				return err
			}
			specMap, err := services.SpecMap(stripTables(spec))
			if err != nil {
				return err
			}
			now := db.Now()
			t := &models.PipelineTemplate{ID: db.NewID(), ProjectID: a.Project.ID, Name: b.Name, Title: b.Title, Description: b.Description,
				Spec: specMap, SinkID: sinkID, RuntimeID: runtimeID, CreatedByID: &a.Principal.User.ID, Version: 1, CreatedAt: now, UpdatedAt: now}
			if err := db.Insert(ctx, q, "pipeline_templates", t); err != nil {
				if db.IsUniqueViolation(err) {
					return problem.Conflict("template_exists", fmt.Sprintf("A template named '%s' already exists.", b.Name), nil)
				}
				return err
			}
			if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "template.created", Summary: "Published template " + b.Title,
				ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "template", ResourceID: &t.ID}); err != nil {
				return err
			}
			out, err = templateRead(ctx, q, t)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusCreated, out)
		return nil
	}))
	r.Patch("/projects/{project_id}/templates/{template_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "template_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		type update struct{ Title, Description *string }
		b, err := body(r, func(v *val.V, o *val.Object) update {
			return update{Title: optStr(v, o, "title", val.StrRules{Min: 1, Max: 200}), Description: optStr(v, o, "description", val.StrRules{Max: 2000})}
		})
		if err != nil {
			return err
		}
		if err := a.Require(auth.ProjectManage); err != nil {
			return err
		}
		ctx := r.Context()
		var out TemplateRead
		err = tx(ctx, func(q db.Querier) error {
			t, err := getTemplate(ctx, q, a, id)
			if err != nil {
				return err
			}
			edited, err := parseIfMatch(r)
			if err != nil {
				return err
			}
			if edited != t.Version {
				return problem.New(412, "version_conflict", "Someone else changed this template. Reload and try again.")
			}
			if b.Title != nil {
				t.Title = *b.Title
			}
			if b.Description != nil {
				t.Description = *b.Description
			}
			t.Version++
			t.UpdatedAt = db.Now()
			if err := db.Update(ctx, q, "pipeline_templates", t); err != nil {
				return err
			}
			out, err = templateRead(ctx, q, t)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Delete("/projects/{project_id}/templates/{template_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "template_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		if err := a.Require(auth.ProjectManage); err != nil {
			return err
		}
		ctx := r.Context()
		err = tx(ctx, func(q db.Querier) error {
			t, err := getTemplate(ctx, q, a, id)
			if err != nil {
				return err
			}
			if err := db.Delete(ctx, q, "pipeline_templates", t.ID); err != nil {
				return err
			}
			_, err = services.Record(ctx, q, services.AuditEvent{Kind: "template.deleted", Summary: "Deleted template " + t.Title,
				ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "template", ResourceID: &t.ID})
			return err
		})
		if err != nil {
			return err
		}
		httpapi.NoContent(w)
		return nil
	}))
}
