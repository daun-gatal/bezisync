package controller

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/models"
)

// PlanItem is one pipeline `bezisync uninstall` would remove.
type PlanItem struct {
	Project, Pipeline string
	Runtime           *string
	Deployed          bool
	// Artifacts recorded when the pipeline was prepared (slot, publication, tables, topics).
	Artifacts []string
}

// UninstallEntry is the outcome for one pipeline.
type UninstallEntry struct {
	Pipeline string
	Results  []map[string]any
	OK       bool
}

const livePipelines = `SELECT * FROM pipelines WHERE lifecycle NOT IN ('deleted') ORDER BY project_id, name`

// Plan lists what uninstall removes; it changes nothing.
func Plan(ctx context.Context, q db.Querier) ([]PlanItem, error) {
	rows, err := db.Select[models.Pipeline](ctx, q, livePipelines)
	if err != nil {
		return nil, err
	}
	items := make([]PlanItem, 0, len(rows))
	for i := range rows {
		p := &rows[i]
		project, err := db.FindScalar[string](ctx, q, `SELECT slug FROM projects WHERE id = $1`, p.ProjectID)
		if err != nil {
			return nil, err
		}
		var rt *string
		if p.RuntimeID != nil {
			if rt, err = db.FindScalar[string](ctx, q, `SELECT name FROM runtimes WHERE id = $1`, *p.RuntimeID); err != nil {
				return nil, err
			}
		}
		artifacts := []string{"workload bz-" + p.ShortID()}
		recorded, _ := p.Artifacts["source"].([]any)
		for _, a := range recorded {
			if m, ok := a.(map[string]any); ok {
				artifacts = append(artifacts, fmt.Sprintf("%s %s", pyStrOrNone(m["kind"]), pyStrOrNone(m["name"])))
			}
		}
		if p.DeployedVersionID != nil {
			artifacts = append(artifacts, "offsets offsets_"+p.ShortID())
		}
		slug := "?"
		if project != nil {
			slug = *project
		}
		items = append(items, PlanItem{Project: slug, Pipeline: p.Name, Runtime: rt, Deployed: p.DeployedVersionID != nil, Artifacts: artifacts})
	}
	return items, nil
}

// pyStrOrNone is str() of a value that may be missing (Python prints None).
func pyStrOrNone(v any) string {
	if v == nil {
		return "None"
	}
	return strOf(v)
}

// Uninstall removes every pipeline's connector, source artifacts and offsets (and with
// dropDestinations its topics/streams), one pipeline per transaction, reusing the
// idempotent cleanup job.
func Uninstall(ctx context.Context, dropDestinations bool) ([]UninstallEntry, error) {
	rows, err := db.Select[models.Pipeline](ctx, db.MustPool(ctx), livePipelines)
	if err != nil {
		return nil, err
	}
	var report []UninstallEntry
	for i := range rows {
		p := &rows[i]
		entry := UninstallEntry{Pipeline: p.Name, OK: true, Results: []map[string]any{}}
		err := db.Tx(ctx, func(tx pgx.Tx) error {
			now := db.Now()
			if p.RuntimeID == nil {
				_, err := tx.Exec(ctx, `UPDATE pipelines SET lifecycle = 'deleted', deleted_at = COALESCE(deleted_at, $1) WHERE id = $2`, now, p.ID)
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE pipelines SET desired_state = 'paused', lifecycle = 'deleting', deleted_at = COALESCE(deleted_at, $1) WHERE id = $2`,
				now, p.ID); err != nil {
				return err
			}
			pid := p.ID
			job := &models.Job{ID: db.NewID(), Kind: "cleanup", ProjectID: &p.ProjectID, ResourceID: &pid,
				Payload: db.Map{"drop_destinations": dropDestinations}, State: "running", CreatedAt: now}
			out, jerr := runSavepoint(ctx, tx, job)
			if jerr != nil {
				entry.Results = []map[string]any{{"kind": "pipeline", "name": p.Name, "outcome": "failed", "detail": domain.Truncate(errMsg(jerr), 300)}}
				entry.OK = false
				return nil
			}
			list, _ := out["results"].([]any)
			for _, r := range list {
				m, _ := r.(map[string]any)
				entry.Results = append(entry.Results, m)
				if m["outcome"] == "failed" {
					entry.OK = false
				}
			}
			return nil
		})
		if err != nil {
			return report, err
		}
		report = append(report, entry)
	}
	return report, nil
}

// runSavepoint runs a job handler, undoing its writes when it fails.
func runSavepoint(ctx context.Context, tx pgx.Tx, j *models.Job) (map[string]any, error) {
	sp, err := tx.Begin(ctx)
	if err != nil {
		return nil, err
	}
	out, err := RunJob(ctx, sp, j)
	if err != nil {
		_ = sp.Rollback(ctx)
		return nil, err
	}
	return out, sp.Commit(ctx)
}
