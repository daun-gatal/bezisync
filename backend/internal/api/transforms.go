package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

const maxPreviewColumns = 2000

type previewRequest struct {
	Spec            *domain.PipelineSpec
	Table           string
	Operation       string
	Columns         []domain.PreviewColumn
	PrimaryKey      []string
	ReplicaIdentity *string
}

func parsePreviewRequest(v *val.V, o *val.Object) previewRequest {
	req := previewRequest{Spec: optSpec(v, o, "spec"), Operation: "insert"}
	if f := o.Get("table"); f.Required() {
		req.Table, _ = v.Constrained(f.Loc, f.Raw, val.StrRules{Max: 300, Pattern: `^[A-Za-z0-9_$\-]+\.[A-Za-z0-9_$\-]+$`})
	}
	if f := o.Get("operation"); f.Present {
		req.Operation, _ = v.Literal(f.Loc, f.Raw, "insert", "update", "delete", "snapshot")
	}
	if f := o.Get("columns"); f.Present && !f.IsNull() {
		if items, ok := v.List(f.Loc, f.Raw); ok && v.ListLen(f.Loc, len(items), 0, maxPreviewColumns) {
			for i, item := range items {
				co := v.Model(f.Loc.Add(i), item, "PreviewColumn")
				if co.Failed {
					continue
				}
				var c domain.PreviewColumn
				if g := co.Get("name"); g.Required() {
					c.Name, _ = v.Constrained(g.Loc, g.Raw, val.StrRules{Min: 1, Max: 300})
				}
				if g := co.Get("type"); g.Present {
					c.Type, _ = v.Constrained(g.Loc, g.Raw, val.StrRules{Max: 200})
				}
				if g := co.Get("nullable"); g.Present {
					c.Nullable, _ = v.Bool(g.Loc, g.Raw)
				}
				if g := co.Get("pii"); g.Present {
					c.PII, _ = v.Bool(g.Loc, g.Raw)
				}
				co.Forbid()
				req.Columns = append(req.Columns, c)
			}
		}
	}
	if f := o.Get("primary_key"); f.Present && !f.IsNull() {
		req.PrimaryKey, _ = v.StrList(f.Loc, f.Raw)
	}
	if f := o.Get("replica_identity"); f.Present && !f.IsNull() {
		if s, ok := v.Constrained(f.Loc, f.Raw, val.StrRules{Max: 20}); ok {
			req.ReplicaIdentity = &s
		}
	}
	o.Forbid()
	return req
}

func transformRoutes(r chi.Router) {
	expressionRoutes(r)
	// A pure computation over a sample row built from the columns the client already read
	// through discovery: it never touches the source or the destination.
	r.Post("/projects/{project_id}/pipelines/{pipeline_id}/transforms:preview", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		req, err := body(r, parsePreviewRequest)
		if err != nil {
			if _, aerr := project(r); aerr != nil {
				return aerr
			}
			return err
		}
		_, p, err := viewPipeline(r, auth.PipelineView, false)
		if err != nil {
			return err
		}
		ctx := r.Context()
		q := pool(ctx)
		spec := req.Spec
		if spec == nil {
			s, err := services.SpecOf(ctx, q, p.CurrentVersionID)
			if err != nil {
				return err
			}
			if s == nil {
				return problem.NotFound("Version")
			}
			spec = s
		}
		if !contains(spec.Source.Tables.Include, req.Table) {
			return problem.New(422, "table_not_captured", req.Table+" is not captured by this pipeline.")
		}
		source, err := services.GetConnection(ctx, q, p.SourceID)
		if err != nil {
			return err
		}
		res := domain.Preview(domain.PreviewInput{
			SourceType: source.Type, Pipeline: p.Name, Prefix: "bz_" + p.ShortID(), Spec: *spec,
			Table: req.Table, Operation: req.Operation, Columns: req.Columns, PrimaryKey: req.PrimaryKey,
			ReplicaIdentity: req.ReplicaIdentity,
		})
		httpapi.JSON(w, http.StatusOK, res)
		return nil
	}))
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
