package api

import (
	"net/http"
	"slices"

	"github.com/go-chi/chi/v5"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/compiler"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// maxClassify is how many keys one classify request may ask about.
const maxClassify = 200

// classifyItem is one key the wizard asks about (its value matters: ${…} and line breaks are refused).
type classifyItem struct{ Key, Value string }

// classifyRead is what BeziSync makes of a raw property. Values are never echoed.
type classifyRead struct {
	Key     string `json:"key"`
	Class   string `json:"class"`
	Message string `json:"message"`
	// Fix is where the value is set instead ("tables", "sink", "transform", "review", "connection").
	Fix string `json:"fix"`
	// Setting is the registry setting with this key, when there is one: the wizard offers it typed.
	Setting *engine.Setting `json:"setting"`
}

func settingsRoutes(r chi.Router) {
	// Pure: what BeziSync makes of raw properties for this pipeline, so the wizard never copies
	// the classifier. Keys BeziSync compiles for the pipeline are refused at preflight.
	r.Post("/projects/{project_id}/pipelines/{pipeline_id}/settings:classify", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		// The pipeline first: someone who can't see it gets a 404 whatever the body holds.
		_, p, err := viewPipeline(r, auth.PipelineView, false)
		if err != nil {
			return err
		}
		items, err := body(r, func(v *val.V, o *val.Object) []classifyItem {
			f := o.Get("items")
			if !f.Present {
				v.Fail(f.Loc, "missing", "Field required")
				return nil
			}
			list, ok := v.List(f.Loc, f.Raw)
			if !ok {
				return nil
			}
			if len(list) > maxClassify {
				v.Fail(f.Loc, "too_long", "List should have at most 200 items after validation, not more")
				return nil
			}
			out := make([]classifyItem, 0, len(list))
			for i, raw := range list {
				item := v.Model(f.Loc.Add(i), raw, "ClassifyItem")
				if item.Failed {
					continue
				}
				out = append(out, classifyItem{Key: str(v, item, "key", "", val.StrRules{Max: 300}, true), Value: str(v, item, "value", "", val.StrRules{Max: 5000}, false)})
				item.Forbid()
			}
			o.Forbid()
			return out
		})
		if err != nil {
			return err
		}
		id := services.EngineOf(p)
		sourceType := ""
		if src, err := services.GetConnection(r.Context(), db.MustPool(r.Context()), p.SourceID); err == nil && src != nil {
			sourceType = src.Type
		}
		out := make([]classifyRead, 0, len(items))
		for _, it := range items {
			res := classifyRead{Key: it.Key}
			if s := engine.SettingOf(id, it.Key); s != nil && slices.Contains(engine.TuningScopes(id), s.Scope) {
				res.Setting = s
			}
			if id == engine.FlinkCDC {
				v := compiler.ClassifyFlinkAdvanced(it.Key, it.Value, nil)
				res.Class, res.Message, res.Fix = string(v.Class), v.Message, v.Fix
			} else if why := services.DriverPropertyRefusal(sourceType, it.Key); why != "" {
				res.Class, res.Message = string(compiler.AdvancedInvalid), why
			} else {
				v := compiler.ClassifyAdvanced(it.Key, it.Value, nil)
				res.Class, res.Message, res.Fix = string(v.Class), v.Message, v.Fix
			}
			out = append(out, res)
		}
		httpapi.JSON(w, http.StatusOK, map[string]any{"items": out})
		return nil
	}))

	// Pure: how a custom Flink CDC cluster would be packed on the pipeline's runtime now, so the
	// wizard never copies the packing. The deploy packs again and keeps that shape.
	r.Post("/projects/{project_id}/pipelines/{pipeline_id}/cluster:pack", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		_, p, err := viewPipeline(r, auth.PipelineView, false)
		if err != nil {
			return err
		}
		if services.EngineOf(p) != engine.FlinkCDC {
			return problem.New(422, "engine_settings", "A cluster is packed only for pipelines that run on Flink CDC.")
		}
		cluster, err := body(r, func(v *val.V, o *val.Object) *domain.FlinkCluster {
			f := o.Get("cluster")
			if !f.Present {
				v.Fail(f.Loc, "missing", "Field required")
				return nil
			}
			c := domain.ParseCluster(v, f.Loc, f.Raw)
			o.Forbid()
			return c
		})
		if err != nil {
			return err
		}
		pv, err := services.PreviewCluster(r.Context(), db.MustPool(r.Context()), p, cluster)
		if err != nil {
			return err
		}
		s := pv.Size
		var node any
		if pv.Node.Known() {
			node = map[string]any{"cpu": float64(pv.Node.CPUMilli) / 1000, "memory_mib": pv.Node.MemoryMiB}
		}
		var streaming any
		if ss := pv.Streaming; ss != nil {
			n := services.FlinkNeed(ss.Size)
			streaming = map[string]any{"task_managers": ss.Size.TaskManagers, "slots": ss.Size.SlotsPerTaskManager(), "readers": ss.Size.Parallelism(),
				"task_manager_cpu": ss.Size.TaskManagerCPU, "task_manager_memory": ss.Size.TaskManagerMemory,
				"job_manager_cpu": ss.Size.JobManagerCPU, "job_manager_memory": ss.Size.JobManagerMemory, "why": ss.Why,
				"need": map[string]any{"cpu": n.CPU, "memory_mib": n.MiB}}
		}
		httpapi.JSON(w, http.StatusOK, map[string]any{
			"streaming":     streaming,
			"task_managers": s.TaskManagers, "slots": s.SlotsPerTaskManager(), "readers": s.Parallelism(),
			"task_manager_cpu": s.TaskManagerCPU, "task_manager_memory": s.TaskManagerMemory,
			"job_manager_cpu": s.JobManagerCPU, "job_manager_memory": s.JobManagerMemory,
			"why": pv.Why, "need": map[string]any{"cpu": pv.Need.CPU, "memory_mib": pv.Need.MiB}, "node": node, "check": pv.Check})
		return nil
	}))
}
