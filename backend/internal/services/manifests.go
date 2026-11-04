package services

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

// Apply and export pipeline manifests (config-as-code), and promote between projects.

// ApplyResult is the outcome of applying one manifest document.
type ApplyResult struct {
	Name       string     `json:"name"`
	Action     string     `json:"action"`
	PipelineID *uuid.UUID `json:"pipeline_id"`
	Version    *int       `json:"version"`
	Diff       []string   `json:"diff"`
	Notes      []string   `json:"notes"`
	Deploy     *string    `json:"deploy"`
}

// ExportPipeline renders a pipeline as a manifest document.
func ExportPipeline(ctx context.Context, q db.Querier, a *auth.ProjectAccess, p *models.Pipeline) (*om.Map, error) {
	spec, err := SpecOf(ctx, q, p.CurrentVersionID)
	if err != nil {
		return nil, err
	}
	if spec == nil {
		return nil, problem.New(409, "no_version", "This pipeline has no saved version yet.")
	}
	source, err := GetConnection(ctx, q, p.SourceID)
	if err != nil {
		return nil, err
	}
	var sinkName, rtName *string
	if p.SinkID != nil {
		if s, err := GetConnection(ctx, q, *p.SinkID); err != nil {
			return nil, err
		} else if s != nil {
			sinkName = &s.Name
		}
	}
	if p.RuntimeID != nil {
		if r, err := GetRuntime(ctx, q, *p.RuntimeID); err != nil {
			return nil, err
		} else if r != nil {
			rtName = &r.Name
		}
	}
	return domain.ToDocument(p.Name, a.Project.Slug, p.Description, p.Owner, p.LabelMap(), source.Name, sinkName, rtName, *spec, p.Engine), nil
}

// FindConnectionByName finds a project connection by name and kind.
func FindConnectionByName(ctx context.Context, q db.Querier, projectID uuid.UUID, name, kind string) (*models.Connection, error) {
	return db.Find[models.Connection](ctx, q, `SELECT * FROM connections WHERE project_id = $1 AND name = $2 AND kind = $3`, projectID, name, kind)
}

// FindRuntimeByName finds a project runtime by name.
func FindRuntimeByName(ctx context.Context, q db.Querier, projectID uuid.UUID, name string) (*models.Runtime, error) {
	return db.Find[models.Runtime](ctx, q, `SELECT * FROM runtimes WHERE project_id = $1 AND name = $2`, projectID, name)
}

func manifestConnection(ctx context.Context, q db.Querier, projectID uuid.UUID, name, kind string) (*models.Connection, error) {
	c, err := FindConnectionByName(ctx, q, projectID, name, kind)
	if err != nil {
		return nil, err
	}
	if c == nil {
		what, field := "destination", "sink"
		if kind == "source" {
			what, field = "source", "source"
		}
		return nil, problem.New(422, "unknown_connection", fmt.Sprintf("This project has no %s connection named '%s'.", what, name)).
			WithErrors([]map[string]any{{"loc": []string{"spec", field, "connection"}, "message": fmt.Sprintf("unknown %s '%s'", what, name)}})
	}
	return c, nil
}

// ApplyOptions modify ApplyManifest.
type ApplyOptions struct {
	DryRun, Deploy              bool
	SourceID, SinkID, RuntimeID *uuid.UUID
	Note                        string
}

func labelsEqual(m db.Map, l map[string]string) bool {
	if len(m) != len(l) {
		return false
	}
	for k, v := range l {
		if s, ok := m[k].(string); !ok || s != v {
			return false
		}
	}
	return true
}

func uuidChanged(have *uuid.UUID, want *uuid.UUID) bool {
	if want == nil {
		return false
	}
	return have == nil || *have != *want
}

// ApplyManifest creates or updates the pipeline named in the manifest so it matches it.
func ApplyManifest(ctx context.Context, q db.Querier, a *auth.ProjectAccess, parsed *domain.ParsedPipeline, o ApplyOptions) (*ApplyResult, error) {
	if o.Note == "" {
		o.Note = "Applied from a manifest"
	}
	if err := a.Require(auth.PipelineEdit); err != nil {
		return nil, err
	}
	if o.Deploy {
		if err := a.Require(auth.PipelineDeploy); err != nil {
			return nil, err
		}
	}
	pid := a.Project.ID
	sourceID := o.SourceID
	if sourceID == nil {
		c, err := manifestConnection(ctx, q, pid, parsed.SourceConnection, "source")
		if err != nil {
			return nil, err
		}
		sourceID = &c.ID
	}
	sinkID := o.SinkID
	if sinkID == nil && parsed.SinkConnection != nil && *parsed.SinkConnection != "" {
		c, err := manifestConnection(ctx, q, pid, *parsed.SinkConnection, "sink")
		if err != nil {
			return nil, err
		}
		sinkID = &c.ID
	}
	runtimeID := o.RuntimeID
	if runtimeID == nil && parsed.Runtime != nil && *parsed.Runtime != "" {
		rt, err := FindRuntimeByName(ctx, q, pid, *parsed.Runtime)
		if err != nil {
			return nil, err
		}
		if rt == nil {
			return nil, problem.New(422, "unknown_runtime", fmt.Sprintf("This project has no runtime named '%s'.", *parsed.Runtime)).
				WithErrors([]map[string]any{{"loc": []string{"spec", "runtime"}, "message": fmt.Sprintf("unknown runtime '%s'", *parsed.Runtime)}})
		}
		runtimeID = &rt.ID
	}
	existing, err := db.Find[models.Pipeline](ctx, q, `SELECT * FROM pipelines WHERE project_id = $1 AND name = $2 AND deleted_at IS NULL`, pid, parsed.Name)
	if err != nil {
		return nil, err
	}
	var current *domain.PipelineSpec
	if existing != nil {
		if current, err = SpecOf(ctx, q, existing.CurrentVersionID); err != nil {
			return nil, err
		}
	}
	from := "(new)"
	if existing != nil {
		from = "current"
	}
	diff := domain.SpecDiff(current, parsed.Spec, from, "manifest")
	notes := []string{}
	if existing != nil {
		notes = domain.SummarizeChange(current, parsed.Spec).Notes
	}
	if existing != nil && existing.SourceID != *sourceID {
		return nil, problem.Conflict("source_changed", fmt.Sprintf("Pipeline %s reads from another source; the source cannot change.", parsed.Name), nil)
	}
	engineChanged := existing != nil && parsed.Engine != "" && parsed.Engine != existing.Engine
	metaChanged := existing != nil && (existing.Description != parsed.Description || existing.Owner != parsed.Owner ||
		!labelsEqual(existing.Labels, parsed.Labels) || uuidChanged(existing.SinkID, sinkID) || uuidChanged(existing.RuntimeID, runtimeID) || engineChanged)
	if o.DryRun {
		action := "would_create"
		var id *uuid.UUID
		if existing != nil {
			id = &existing.ID
			action = "unchanged"
			if len(diff) > 0 || metaChanged {
				action = "would_update"
			}
		}
		return &ApplyResult{Name: parsed.Name, Action: action, PipelineID: id, Diff: diff, Notes: notes}, nil
	}
	var p *models.Pipeline
	action := "unchanged"
	if existing == nil {
		if p, err = CreatePipeline(ctx, q, a, NewPipeline{Name: parsed.Name, Description: parsed.Description, Owner: parsed.Owner,
			Labels: parsed.Labels, SourceID: *sourceID, SinkID: sinkID, RuntimeID: runtimeID, Spec: parsed.Spec, Engine: parsed.Engine}); err != nil {
			return nil, err
		}
		action = "created"
	} else {
		p = existing
		if len(diff) > 0 || metaChanged {
			spec := parsed.Spec
			desc, owner := parsed.Description, parsed.Owner
			edit := PipelineEdit{ExpectedVersion: p.Version, Spec: &spec, Description: &desc, Owner: &owner,
				Labels: parsed.Labels, LabelsSet: true, RuntimeID: runtimeID, SinkID: sinkID, Note: o.Note}
			if engineChanged {
				edit.Engine = &parsed.Engine
			}
			if _, err := UpdatePipeline(ctx, q, a, p, edit); err != nil {
				return nil, err
			}
			action = "updated"
		}
	}
	v, err := VersionOf(ctx, q, p.CurrentVersionID)
	if err != nil {
		return nil, err
	}
	var number *int
	if v != nil {
		n := v.Number
		number = &n
	}
	result := &ApplyResult{Name: parsed.Name, Action: action, PipelineID: &p.ID, Version: number, Diff: diff, Notes: notes}
	differs := p.CurrentVersionID == nil || p.DeployedVersionID == nil || *p.CurrentVersionID != *p.DeployedVersionID
	if o.Deploy && differs {
		required, err := ApprovalRequired(ctx, q, pid)
		if err != nil {
			return nil, err
		}
		if required {
			if _, err := RequestApproval(ctx, q, a, p, nil, nil, o.Note); err != nil {
				return nil, err
			}
			s := "waiting_for_approval"
			result.Deploy = &s
		} else {
			if _, _, err := Deploy(ctx, q, a, p, nil, nil); err != nil {
				return nil, err
			}
			s := "deploying"
			result.Deploy = &s
		}
	}
	return result, nil
}
