package api

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

const imagePattern = `^[\w.\-/:@]+$`

type catalogProjectUsage struct {
	ProjectID uuid.UUID `json:"project_id" db:"project_id"`
	Slug      string    `json:"slug" db:"slug"`
	Name      string    `json:"name" db:"name"`
	Pipelines int64     `json:"pipelines" db:"n"`
}

type catalogEntryRead struct {
	ID              *uuid.UUID             `json:"id"`
	Image           string                 `json:"image"`
	Label           string                 `json:"label"`
	Notes           string                 `json:"notes"`
	IsDefault       bool                   `json:"is_default"`
	Deprecated      bool                   `json:"deprecated"`
	Builtin         bool                   `json:"builtin"`
	Official        bool                   `json:"official"`
	ReleaseNotesURL *string                `json:"release_notes_url"`
	Pipelines       *int64                 `json:"pipelines"`
	Projects        *[]catalogProjectUsage `json:"projects"`
}

// readCatalog is an engine's install-wide image catalog; Admins also see where each image runs.
func readCatalog(ctx context.Context, q db.Querier, p *auth.Principal, id engine.ID) (*om.Map, error) {
	admin := p.User.IsSuperuser
	usage := map[string][]catalogProjectUsage{}
	if admin {
		rows, err := db.Select[struct {
			catalogProjectUsage
			Image *string `db:"deployed_image"`
		}](ctx, q, `SELECT p.deployed_image, pr.id AS project_id, pr.slug, pr.name, count(*) AS n FROM pipelines p
			JOIN projects pr ON pr.id = p.project_id WHERE p.deleted_at IS NULL AND p.deployed_image IS NOT NULL
			GROUP BY p.deployed_image, pr.id, pr.slug, pr.name ORDER BY pr.name`)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			if r.Image != nil && *r.Image != "" {
				usage[*r.Image] = append(usage[*r.Image], r.catalogProjectUsage)
			}
		}
	}
	def, err := services.DefaultImageFor(ctx, q, id)
	if err != nil {
		return nil, err
	}
	entries, err := services.CatalogEntriesFor(ctx, q, id)
	if err != nil {
		return nil, err
	}
	out := make([]catalogEntryRead, len(entries))
	for i, e := range entries {
		repo, _ := services.SplitImage(e.Image)
		er := catalogEntryRead{Image: e.Image, Label: e.Label, Notes: e.Notes, IsDefault: e.IsDefault, Deprecated: e.Deprecated, Builtin: e.Builtin,
			Official: repo == services.OfficialRepository, ReleaseNotesURL: services.ReleaseNotesURL(e.Image)}
		if e.Row != nil {
			id := e.Row.ID
			er.ID = &id
		}
		if admin {
			list := usage[e.Image]
			if list == nil {
				list = []catalogProjectUsage{}
			}
			var total int64
			for _, u := range list {
				total += u.Pipelines
			}
			er.Pipelines, er.Projects = &total, &list
		}
		out[i] = er
	}
	var defImage any
	if def != "" {
		defImage = def
	}
	return obj("engine", string(id), "default_image", defImage, "entries", out), nil
}

// engineRead is an engine and whether this install can run it.
type engineRead struct {
	ID           string  `json:"id"`
	Title        string  `json:"title"`
	Available    bool    `json:"available"`
	Reason       *string `json:"reason"`
	DefaultImage *string `json:"default_image"`
	// Sources and Sinks it runs (nil: every one, except ExceptSinks).
	Sources     []string `json:"sources"`
	Sinks       []string `json:"sinks"`
	ExceptSinks []string `json:"except_sinks"`
	// Flink CDC: the settings a pipeline may choose and the size presets.
	Settings []engine.FlinkSetting `json:"settings,omitempty"`
	Sizes    map[string]flinkSize  `json:"sizes,omitempty"`
}

type flinkSize struct {
	JobManagerCPU     string `json:"jobmanager_cpu"`
	JobManagerMemory  string `json:"jobmanager_memory"`
	TaskManagers      int    `json:"taskmanagers"`
	TaskManagerCPU    string `json:"taskmanager_cpu"`
	TaskManagerMemory string `json:"taskmanager_memory"`
	Parallelism       int    `json:"parallelism"`
	TotalMemoryMiB    int    `json:"total_memory_mib"`
}

// catalogEngine reads the ?engine= of a catalog request (Debezium when absent).
func catalogEngine(r *http.Request) (engine.ID, error) {
	raw := r.URL.Query().Get("engine")
	if raw == "" {
		return engine.Debezium, nil
	}
	if id := engine.ID(raw); id.Valid() {
		return id, nil
	}
	return "", problem.New(422, "unknown_engine", "Unknown engine '"+raw+"'.")
}

func makeDefault(ctx context.Context, q db.Querier, row *models.DebeziumImage) error {
	if _, err := q.Exec(ctx, `UPDATE debezium_images SET is_default = FALSE WHERE id != $1 AND engine = $2`, row.ID, row.Engine); err != nil {
		return err
	}
	row.IsDefault, row.Deprecated = true, false
	return nil
}

func getImage(ctx context.Context, q db.Querier, id uuid.UUID) (*models.DebeziumImage, error) {
	row, err := db.Find[models.DebeziumImage](ctx, q, `SELECT * FROM debezium_images WHERE id = $1`, id)
	if err == nil && row == nil {
		err = problem.NotFound("Image")
	}
	return row, err
}

func catalogRoutes(r chi.Router) {
	r.Get("/engines", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		if _, err := principal(r); err != nil {
			return err
		}
		ctx := r.Context()
		q := pool(ctx)
		out := make([]engineRead, 0, len(engine.All))
		for _, id := range engine.All {
			a, err := services.EngineAvailable(ctx, q, id)
			if err != nil {
				return err
			}
			def, err := services.DefaultImageFor(ctx, q, id)
			if err != nil {
				return err
			}
			e := engineRead{ID: string(id), Title: id.Title(), Available: a.Available}
			e.Sources, e.Sinks, e.ExceptSinks = engine.Runs(id)
			if id == engine.FlinkCDC {
				e.Settings, e.Sizes = engine.FlinkSettings, map[string]flinkSize{}
				for name, s := range engine.FlinkSizes {
					e.Sizes[name] = flinkSize{JobManagerCPU: s.JobManagerCPU, JobManagerMemory: s.JobManagerMemory, TaskManagers: s.TaskManagers,
						TaskManagerCPU: s.TaskManagerCPU, TaskManagerMemory: s.TaskManagerMemory, Parallelism: s.Parallelism(), TotalMemoryMiB: s.TotalMemoryMiB()}
				}
			}
			if a.Reason != "" {
				e.Reason = &a.Reason
			}
			if def != "" {
				e.DefaultImage = &def
			}
			out = append(out, e)
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Get("/debezium-catalog", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		p, err := principal(r)
		if err != nil {
			return err
		}
		id, err := catalogEngine(r)
		if err != nil {
			return err
		}
		ctx := r.Context()
		out, err := readCatalog(ctx, pool(ctx), p, id)
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Post("/debezium-catalog", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		p, err := superuser(r)
		if err != nil {
			return err
		}
		type create struct {
			Image, Label, Notes string
			IsDefault           bool
			Engine              engine.ID
		}
		b, err := body(r, func(v *val.V, o *val.Object) create {
			c := create{Image: str(v, o, "image", "", val.StrRules{Max: 300, Pattern: imagePattern}, true),
				Label: str(v, o, "label", "", val.StrRules{Max: 100}, false), Notes: str(v, o, "notes", "", val.StrRules{Max: 2000}, false),
				IsDefault: boolean(v, o, "is_default", false), Engine: engine.Debezium}
			if e := optEngine(v, o); e != nil {
				c.Engine = engine.ID(*e)
			}
			return c
		})
		if err != nil {
			return err
		}
		ctx := r.Context()
		var out *om.Map
		err = tx(ctx, func(q db.Querier) error {
			n, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM debezium_images WHERE image = $1`, b.Image)
			if err != nil {
				return err
			}
			if n > 0 {
				return problem.Conflict("image_exists", b.Image+" is already in the catalog.", nil)
			}
			now := db.Now()
			row := &models.DebeziumImage{ID: db.NewID(), Image: b.Image, Label: b.Label, Notes: b.Notes, Engine: string(b.Engine),
				CreatedByID: &p.User.ID, CreatedAt: now, UpdatedAt: now}
			if b.IsDefault {
				if err := makeDefault(ctx, q, row); err != nil {
					return err
				}
			}
			if err := db.Insert(ctx, q, "debezium_images", row); err != nil {
				return err
			}
			summary := "Added " + b.Engine.Title() + " image " + b.Image
			if b.IsDefault {
				summary += " as the default"
			}
			if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "debezium_image.added", Summary: summary, Principal: p,
				ResourceType: "debezium_image", ResourceID: &row.ID}); err != nil {
				return err
			}
			out, err = readCatalog(ctx, q, p, b.Engine)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusCreated, out)
		return nil
	}))
	r.Patch("/debezium-catalog/{image_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "image_id")
		if err != nil {
			return err
		}
		p, err := superuser(r)
		if err != nil {
			return err
		}
		type update struct {
			Label, Notes          *string
			IsDefault, Deprecated *bool
		}
		b, err := body(r, func(v *val.V, o *val.Object) update {
			return update{Label: optStr(v, o, "label", val.StrRules{Max: 100}), Notes: optStr(v, o, "notes", val.StrRules{Max: 2000}),
				IsDefault: optBool(v, o, "is_default"), Deprecated: optBool(v, o, "deprecated")}
		})
		if err != nil {
			return err
		}
		ctx := r.Context()
		var out *om.Map
		err = tx(ctx, func(q db.Querier) error {
			row, err := getImage(ctx, q, id)
			if err != nil {
				return err
			}
			if b.Label != nil {
				row.Label = *b.Label
			}
			if b.Notes != nil {
				row.Notes = *b.Notes
			}
			if b.IsDefault != nil {
				if *b.IsDefault {
					if err := makeDefault(ctx, q, row); err != nil {
						return err
					}
				} else {
					row.IsDefault = false // the install default applies again
				}
			}
			if b.Deprecated != nil {
				if *b.Deprecated && row.IsDefault {
					return problem.New(409, "catalog_default", "Choose another default before deprecating this image.")
				}
				row.Deprecated = *b.Deprecated
			}
			row.UpdatedAt = db.Now()
			if err := db.Update(ctx, q, "debezium_images", row); err != nil {
				return err
			}
			summary := "Updated " + engine.Of(row.Engine).Title() + " image " + row.Image
			if b.IsDefault != nil && *b.IsDefault {
				summary += " (now the default)"
			}
			if b.Deprecated != nil && *b.Deprecated {
				summary += " (deprecated)"
			}
			if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "debezium_image.updated", Summary: summary, Principal: p,
				ResourceType: "debezium_image", ResourceID: &row.ID}); err != nil {
				return err
			}
			out, err = readCatalog(ctx, q, p, engine.Of(row.Engine))
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Delete("/debezium-catalog/{image_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "image_id")
		if err != nil {
			return err
		}
		p, err := superuser(r)
		if err != nil {
			return err
		}
		ctx := r.Context()
		var out *om.Map
		err = tx(ctx, func(q db.Querier) error {
			row, err := getImage(ctx, q, id)
			if err != nil {
				return err
			}
			if row.IsDefault {
				return problem.New(409, "catalog_default", "Choose another default before removing this image.")
			}
			if err := db.Delete(ctx, q, "debezium_images", row.ID); err != nil {
				return err
			}
			if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "debezium_image.removed", Summary: "Removed " + engine.Of(row.Engine).Title() + " image " + row.Image + " from the catalog",
				Principal: p, ResourceType: "debezium_image", ResourceID: &row.ID}); err != nil {
				return err
			}
			out, err = readCatalog(ctx, q, p, engine.Of(row.Engine))
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
}
