package api

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// ManifestRead is exported manifests as YAML and as documents.
type ManifestRead struct {
	YAML      string `json:"yaml"`
	Documents []any  `json:"documents"`
}

func manifestProblem(err error) error {
	var me *domain.ManifestError
	if errors.As(err, &me) {
		p := problem.New(422, "invalid_manifest", me.Error())
		if len(me.Errors) > 0 {
			p = p.WithErrors(me.Errors)
		}
		return p
	}
	return err
}

func applyResult(r *services.ApplyResult) *services.ApplyResult {
	if r.Diff == nil {
		r.Diff = []string{}
	}
	if r.Notes == nil {
		r.Notes = []string{}
	}
	return r
}

func manifestRoutes(r chi.Router) {
	r.Get("/projects/{project_id}/pipelines/{pipeline_id}/manifest", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "pipeline_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		if err := a.Require(auth.PipelineView); err != nil {
			return err
		}
		ctx := r.Context()
		q := pool(ctx)
		p, err := services.GetPipeline(ctx, q, a, id, false)
		if err != nil {
			return err
		}
		doc, err := services.ExportPipeline(ctx, q, a, p)
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, ManifestRead{YAML: domain.ToYAML([]any{doc}), Documents: []any{doc}})
		return nil
	}))
	r.Get("/projects/{project_id}/pipelines:export", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		if err := a.Require(auth.PipelineView); err != nil {
			return err
		}
		ctx := r.Context()
		q := pool(ctx)
		rows, err := db.Select[models.Pipeline](ctx, q, `SELECT * FROM pipelines WHERE project_id = $1 AND deleted_at IS NULL ORDER BY name`, a.Project.ID)
		if err != nil {
			return err
		}
		docs := []any{}
		for i := range rows {
			if rows[i].CurrentVersionID == nil {
				continue
			}
			doc, err := services.ExportPipeline(ctx, q, a, &rows[i])
			if err != nil {
				return err
			}
			docs = append(docs, doc)
		}
		out := ManifestRead{Documents: docs}
		if len(docs) > 0 {
			out.YAML = domain.ToYAML(docs)
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Post("/projects/{project_id}/pipelines:apply", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		type apply struct {
			Manifest       string
			DryRun, Deploy bool
		}
		b, err := body(r, func(v *val.V, o *val.Object) apply {
			return apply{Manifest: str(v, o, "manifest", "", val.StrRules{Min: 1, Max: 1_000_000}, true),
				DryRun: boolean(v, o, "dry_run", false), Deploy: boolean(v, o, "deploy", false)}
		})
		if err != nil {
			return err
		}
		docs, err := domain.ParseDocuments(b.Manifest)
		if err != nil {
			return manifestProblem(err)
		}
		parsed := make([]*domain.ParsedPipeline, 0, len(docs))
		seen := map[string]bool{}
		for _, d := range docs {
			p, err := domain.FromDocument(d)
			if err != nil {
				return manifestProblem(err)
			}
			parsed = append(parsed, p)
		}
		for _, p := range parsed {
			if seen[p.Name] {
				return problem.New(422, "invalid_manifest", "Each pipeline may appear only once per apply.")
			}
			seen[p.Name] = true
		}
		ctx := r.Context()
		results := []*services.ApplyResult{}
		// Every document, or none: any error rolls the whole request back.
		err = dryTx(ctx, b.DryRun, func(q db.Querier) error {
			for _, p := range parsed {
				res, err := services.ApplyManifest(ctx, q, a, p, services.ApplyOptions{DryRun: b.DryRun, Deploy: b.Deploy})
				if err != nil {
					return err
				}
				results = append(results, applyResult(res))
			}
			if b.DryRun {
				return nil
			}
			var parts []string
			changed := false
			for _, res := range results {
				parts = append(parts, fmt.Sprintf("%s (%s)", res.Name, res.Action))
				changed = changed || res.Action == "created" || res.Action == "updated"
			}
			if !changed {
				return nil
			}
			_, err := services.Record(ctx, q, services.AuditEvent{Kind: "pipelines.applied", Summary: "Applied manifest: " + strings.Join(parts, ", "),
				ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "project", ResourceID: &a.Project.ID})
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, results)
		return nil
	}))
	r.Post("/projects/{project_id}/pipelines:adopt", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		props, err := body(r, func(v *val.V, o *val.Object) string {
			return str(v, o, "properties", "", val.StrRules{Min: 1, Max: 500_000}, true)
		})
		if err != nil {
			return err
		}
		if err := a.Require(auth.PipelineEdit); err != nil {
			return err
		}
		// Adopt's report as recorded, and the five-bucket plan the draft is created from.
		report := services.Adopt(props)
		out, err := services.ToMap(report)
		if err != nil {
			return err
		}
		out["plan"] = services.PlanImport(props, report)
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Post("/projects/{project_id}/pipelines/{pipeline_id}/promote", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "pipeline_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		type promote struct {
			Target, Source uuid.UUID
			Sink, Runtime  *uuid.UUID
			Name           *string
			DryRun, Deploy bool
		}
		b, err := body(r, func(v *val.V, o *val.Object) promote {
			return promote{Target: reqUUID(v, o, "target_project_id"), Source: reqUUID(v, o, "source_id"), Sink: optUUID(v, o, "sink_id"),
				Runtime: optUUID(v, o, "runtime_id"), Name: optStr(v, o, "name", val.StrRules{Pattern: SlugRe.String()}),
				DryRun: boolean(v, o, "dry_run", false), Deploy: boolean(v, o, "deploy", false)}
		})
		if err != nil {
			return err
		}
		if err := a.Require(auth.PipelineView); err != nil {
			return err
		}
		if b.Target == a.Project.ID {
			return problem.New(422, "same_project", "Pick a different project to promote to.")
		}
		ctx := r.Context()
		var result *services.ApplyResult
		err = dryTx(ctx, b.DryRun, func(q db.Querier) error {
			p, err := services.GetPipeline(ctx, q, a, id, false)
			if err != nil {
				return err
			}
			target, err := auth.Project(ctx, q, a.Principal, b.Target)
			if err != nil {
				return err
			}
			doc, err := services.ExportPipeline(ctx, q, a, p)
			if err != nil {
				return err
			}
			name := p.Name
			if b.Name != nil && *b.Name != "" {
				name = *b.Name
			}
			if meta, ok := doc.Get("metadata"); ok {
				if m, ok := meta.(*om.Map); ok {
					m.Set("name", name)
				}
			}
			// Through JSON, like the API documents a client would send.
			generic, err := pyjson.ToGeneric(doc)
			if err != nil {
				return err
			}
			plain, _ := generic.(map[string]any)
			parsed, err := domain.FromDocument(plain)
			if err != nil {
				return manifestProblem(err)
			}
			pipelineSource, err := services.GetConnection(ctx, q, p.SourceID)
			if err != nil {
				return err
			}
			source, err := services.GetConnectionOfKind(ctx, q, target.Project.ID, b.Source, "source")
			if err != nil {
				return err
			}
			if pipelineSource != nil && source.Type != pipelineSource.Type {
				return problem.New(422, "type_mismatch", fmt.Sprintf("The target source must be a %s connection.", pipelineSource.Type))
			}
			if b.Sink != nil {
				sink, err := services.GetConnectionOfKind(ctx, q, target.Project.ID, *b.Sink, "sink")
				if err != nil {
					return err
				}
				if p.SinkID != nil {
					pipelineSink, err := services.GetConnection(ctx, q, *p.SinkID)
					if err != nil {
						return err
					}
					if pipelineSink != nil && sink.Type != pipelineSink.Type {
						return problem.New(422, "type_mismatch", fmt.Sprintf("The target destination must be a %s connection.", pipelineSink.Type))
					}
				}
			}
			// Unmapped destination / runtime: use the same name in the target if it exists,
			// otherwise leave it unset so the promoted pipeline is a draft to finish there.
			if b.Sink == nil && parsed.SinkConnection != nil && *parsed.SinkConnection != "" {
				c, err := services.FindConnectionByName(ctx, q, target.Project.ID, *parsed.SinkConnection, "sink")
				if err != nil {
					return err
				}
				if c == nil {
					parsed.SinkConnection = nil
				}
			}
			if b.Runtime == nil && parsed.Runtime != nil && *parsed.Runtime != "" {
				rt, err := services.FindRuntimeByName(ctx, q, target.Project.ID, *parsed.Runtime)
				if err != nil {
					return err
				}
				if rt == nil {
					parsed.Runtime = nil
				}
			}
			src := b.Source
			result, err = services.ApplyManifest(ctx, q, target, parsed, services.ApplyOptions{DryRun: b.DryRun, Deploy: b.Deploy,
				SourceID: &src, SinkID: b.Sink, RuntimeID: b.Runtime, Note: fmt.Sprintf("Promoted from %s/%s", a.Project.Slug, p.Name)})
			if err != nil {
				return err
			}
			applyResult(result)
			if b.DryRun {
				return nil
			}
			_, err = services.Record(ctx, q, services.AuditEvent{Kind: "pipeline.promoted",
				Summary:   fmt.Sprintf("Promoted %s to %s (%s)", p.Name, target.Project.Slug, result.Action),
				ProjectID: &target.Project.ID, Principal: a.Principal, ResourceType: "pipeline", ResourceID: result.PipelineID,
				Details: map[string]any{"from_project": a.Project.Slug, "from_pipeline": p.ID.String()}})
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, result)
		return nil
	}))
}
