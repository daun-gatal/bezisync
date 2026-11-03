package api

import (
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// uuidList reads list[uuid.UUID] with length bounds.
func uuidList(v *val.V, o *val.Object, key string, min, max int) []uuid.UUID {
	f := o.Get(key)
	if !f.Required() {
		return nil
	}
	items, ok := v.List(f.Loc, f.Raw)
	if !ok {
		return nil
	}
	out := make([]uuid.UUID, 0, len(items))
	for i, item := range items {
		if id, ok := v.UUID(f.Loc.Add(i), item); ok {
			out = append(out, id)
		}
	}
	if len(out) != len(items) || !v.ListLen(f.Loc, len(out), min, max) {
		return nil
	}
	return out
}

func imageRoutes(r chi.Router) {
	r.Get("/projects/{project_id}/debezium-images", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		if err := a.Require(auth.PipelineView); err != nil {
			return err
		}
		ctx := r.Context()
		q := pool(ctx)
		def, err := services.DefaultImage(ctx, q)
		if err != nil {
			return err
		}
		usage, err := services.Usage(ctx, q, a.Project.ID)
		if err != nil {
			return err
		}
		if usage == nil {
			usage = []services.ImageUsage{}
		}
		for i := range usage {
			if usage[i].Pipelines == nil {
				usage[i].Pipelines = []services.PipelineImage{}
			}
		}
		httpapi.JSON(w, http.StatusOK, obj("default_image", def, "default_release_notes_url", services.ReleaseNotesURL(def), "images", usage))
		return nil
	}))
	r.Post("/projects/{project_id}/debezium-images:apply", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		type change struct {
			IDs    []uuid.UUID
			Image  *string
			Deploy bool
		}
		b, err := body(r, func(v *val.V, o *val.Object) change {
			return change{IDs: uuidList(v, o, "pipeline_ids", 1, 200), Image: optStr(v, o, "image", val.StrRules{Max: 300, Pattern: `^[\w.\-/:@]+$`}),
				Deploy: boolean(v, o, "deploy", true)}
		})
		if err != nil {
			return err
		}
		if err := a.Require(auth.PipelineEdit); err != nil {
			return err
		}
		if b.Deploy {
			if err := a.Require(auth.PipelineDeploy); err != nil {
				return err
			}
		}
		// Unique, in the order given.
		seen := map[uuid.UUID]bool{}
		var ids []uuid.UUID
		for _, id := range b.IDs {
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
		ctx := r.Context()
		var results []services.UpgradeResult
		err = tx(ctx, func(q db.Querier) error {
			var err error
			if results, err = services.ChangeImage(ctx, q, a, ids, b.Image, b.Deploy); err != nil {
				return err
			}
			var changed []string
			for _, res := range results {
				if res.OK && res.Version != nil {
					changed = append(changed, res.Name)
				}
			}
			if len(changed) == 0 {
				return nil
			}
			target := "the runtime default image"
			if b.Image != nil && *b.Image != "" {
				target = *b.Image
			}
			_, err = services.Record(ctx, q, services.AuditEvent{Kind: "pipeline.image_changed", Summary: "Moved " + strings.Join(changed, ", ") + " to " + target,
				ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "project", ResourceID: &a.Project.ID})
			return err
		})
		if err != nil {
			return err
		}
		if results == nil {
			results = []services.UpgradeResult{}
		}
		httpapi.JSON(w, http.StatusOK, results)
		return nil
	}))
}
