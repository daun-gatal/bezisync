package services

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/compiler"
	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/logging"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/runtime"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// Pipeline lifecycle: create, version, preflight, deploy, pause/resume/restart, delete.
// The API writes desired state only; the controller converges the runtime.

// PreflightSection is one part of a preflight report.
type PreflightSection struct {
	Key    string        `json:"key"`
	Title  string        `json:"title"`
	Report checks.Report `json:"report"`
}

// PreflightReport is what deploying a spec would do and whether it may.
type PreflightReport struct {
	OK           bool                 `json:"ok"`
	Summary      string               `json:"summary"`
	Sections     []PreflightSection   `json:"sections"`
	Properties   *string              `json:"properties"`
	Destinations map[string]string    `json:"destinations"`
	Change       domain.ChangeSummary `json:"change"`
	// SettingsChange is what the deploy changes in the pipeline's settings (omitted when nothing).
	SettingsChange *domain.SettingsChange `json:"settings_change,omitempty"`
	// Recommendations are setting values worth offering, from this run's evidence (omitted: none).
	Recommendations []Recommendation `json:"recommendations,omitempty"`
}

// ParseSpec validates a spec from a request (422 invalid_spec).
func ParseSpec(raw any) (domain.PipelineSpec, error) {
	v := &val.V{}
	spec, ok := domain.ParseSpec(v, nil, raw)
	if !ok {
		errs := make([]map[string]any, len(v.Errors))
		for i, e := range v.Errors {
			errs[i] = map[string]any{"loc": append([]string{"spec"}, e.Loc...), "message": e.Message, "type": *e.Type}
		}
		return spec, problem.New(422, "invalid_spec", "The pipeline spec is invalid.").WithErrors(errs)
	}
	return spec, nil
}

// GetPipeline loads a project's pipeline (404 when missing, deleted or elsewhere).
func GetPipeline(ctx context.Context, q db.Querier, a *auth.ProjectAccess, id uuid.UUID, includeDeleted bool) (*models.Pipeline, error) {
	p, err := db.Find[models.Pipeline](ctx, q, `SELECT * FROM pipelines WHERE id = $1`, id)
	if err != nil {
		return nil, err
	}
	if p == nil || p.ProjectID != a.Project.ID || (p.DeletedAt != nil && !includeDeleted) {
		return nil, problem.NotFound("Pipeline")
	}
	return p, nil
}

// GetConnectionOfKind loads a project's source or sink (422 when not found).
func GetConnectionOfKind(ctx context.Context, q db.Querier, projectID, id uuid.UUID, kind string) (*models.Connection, error) {
	c, err := db.Find[models.Connection](ctx, q, `SELECT * FROM connections WHERE id = $1`, id)
	if err != nil {
		return nil, err
	}
	if c == nil || c.ProjectID != projectID || c.Kind != kind {
		what := "destination"
		if kind == "source" {
			what = "source"
		}
		return nil, problem.New(422, "invalid_"+kind, fmt.Sprintf("The %s was not found in this project.", what))
	}
	return c, nil
}

// GetConnection loads any connection by id.
func GetConnection(ctx context.Context, q db.Querier, id uuid.UUID) (*models.Connection, error) {
	return db.Find[models.Connection](ctx, q, `SELECT * FROM connections WHERE id = $1`, id)
}

// GetRuntime loads a runtime by id.
func GetRuntime(ctx context.Context, q db.Querier, id uuid.UUID) (*models.Runtime, error) {
	return db.Find[models.Runtime](ctx, q, `SELECT * FROM runtimes WHERE id = $1`, id)
}

func projectRuntime(ctx context.Context, q db.Querier, projectID, id uuid.UUID) (*models.Runtime, error) {
	rt, err := GetRuntime(ctx, q, id)
	if err != nil {
		return nil, err
	}
	if rt == nil || rt.ProjectID != projectID {
		return nil, problem.New(422, "invalid_runtime", "The runtime was not found in this project.")
	}
	return rt, RequireEnabled(rt)
}

// RequireEnabled refuses cordoned runtimes.
func RequireEnabled(rt *models.Runtime) error {
	if !rt.Enabled {
		return problem.Conflict("runtime_disabled", fmt.Sprintf("Runtime %s is disabled: nothing new can be placed or deployed on it. "+
			"Choose another runtime or ask an admin to enable it.", rt.Name), nil)
	}
	return nil
}

// ResolveImage is the image a deploy would use now: the pin, the runtime's, the default.
func ResolveImage(ctx context.Context, q db.Querier, spec domain.PipelineSpec, rt *models.Runtime, sinkType string) (string, error) {
	// A sink the official images lack (Iceberg) runs its own build: runtime and catalog defaults
	// are official images, so only a pin to another tag of that build applies.
	if family := connectors.SinkImageFor(sinkType); family != "" {
		if spec.DebeziumImage != nil && *spec.DebeziumImage != "" {
			if !connectors.InSinkImageFamily(sinkType, *spec.DebeziumImage) {
				d, _ := connectors.Describe("sink", sinkType)
				return "", problem.New(422, "image_family", fmt.Sprintf("%s pipelines run the %s image (%s); pin another tag of it or follow the default.",
					d.Title, d.Title, connectors.ImageRepository(family)))
			}
			return *spec.DebeziumImage, nil
		}
		return family, nil
	}
	if spec.DebeziumImage != nil && *spec.DebeziumImage != "" {
		return *spec.DebeziumImage, nil
	}
	if rt != nil && rt.DefaultImage != nil && *rt.DefaultImage != "" {
		return *rt.DefaultImage, nil
	}
	return DefaultImage(ctx, q)
}

// ResolvePipelineImage is the image a pipeline runs on its engine.
func ResolvePipelineImage(ctx context.Context, q db.Querier, p *models.Pipeline, spec domain.PipelineSpec, rt *models.Runtime, sinkType string) (string, error) {
	if EngineOf(p) == engine.FlinkCDC {
		return ResolveFlinkImage(ctx, q, spec)
	}
	return ResolveImage(ctx, q, spec, rt, sinkType)
}

// SinkTypeOf is the type of a pipeline's destination ("" when it has none).
func SinkTypeOf(ctx context.Context, q db.Querier, p *models.Pipeline) (string, error) {
	if p.SinkID == nil {
		return "", nil
	}
	c, err := GetConnection(ctx, q, *p.SinkID)
	if err != nil {
		return "", err
	}
	return c.Type, nil
}

// VersionOf loads a version (nil id → nil).
func VersionOf(ctx context.Context, q db.Querier, id *uuid.UUID) (*models.PipelineVersion, error) {
	if id == nil {
		return nil, nil
	}
	return db.Find[models.PipelineVersion](ctx, q, `SELECT * FROM pipeline_versions WHERE id = $1`, *id)
}

// SpecOf loads the spec of a version.
func SpecOf(ctx context.Context, q db.Querier, id *uuid.UUID) (*domain.PipelineSpec, error) {
	v, err := VersionOf(ctx, q, id)
	if err != nil || v == nil {
		return nil, err
	}
	spec := domain.MustSpec(map[string]any(v.Spec))
	return &spec, nil
}

// SpecMap is spec.model_dump(mode="json") for storage.
func SpecMap(spec domain.PipelineSpec) (db.Map, error) {
	raw, err := jsonMarshal(spec)
	if err != nil {
		return nil, err
	}
	var m db.Map
	return m, m.Scan(raw)
}

func newVersion(ctx context.Context, q db.Querier, p *models.Pipeline, spec domain.PipelineSpec, a *auth.ProjectAccess, note string) (*models.PipelineVersion, error) {
	maxN, err := db.Scalar[*int](ctx, q, `SELECT max(number) FROM pipeline_versions WHERE pipeline_id = $1`, p.ID)
	if err != nil {
		return nil, err
	}
	number := 1
	if maxN != nil {
		number = *maxN + 1
	}
	specMap, err := SpecMap(spec)
	if err != nil {
		return nil, err
	}
	v := &models.PipelineVersion{ID: db.NewID(), PipelineID: p.ID, Number: number, Spec: specMap, SpecHash: domain.SpecHash(spec),
		Note: note, CreatedAt: db.Now(), CreatedByID: &a.Principal.User.ID, CreatedByLabel: a.Principal.Label()}
	if err := db.Insert(ctx, q, "pipeline_versions", v); err != nil {
		return nil, err
	}
	p.CurrentVersionID = &v.ID
	return v, nil
}

// isDraftOf says whether a version is still its author's working draft: nothing depends on it
// (not deployed, not sent for approval) and the caller made it.
func isDraftOf(v *models.PipelineVersion, a *auth.ProjectAccess) bool {
	return v.SealedAt == nil && v.CreatedByID != nil && *v.CreatedByID == a.Principal.User.ID
}

// amendVersion saves a spec into the working draft, so one change is one version however many
// saves it takes. The draft keeps its note unless the edit names one. It reports false when the
// version was sealed meanwhile (a deploy or approval request in another transaction): the caller
// then starts a new version instead.
func amendVersion(ctx context.Context, q db.Querier, v *models.PipelineVersion, spec domain.PipelineSpec, note string) (bool, error) {
	specMap, err := SpecMap(spec)
	if err != nil {
		return false, err
	}
	if note == "" {
		note = v.Note
	}
	now, hash := db.Now(), domain.SpecHash(spec)
	tag, err := q.Exec(ctx, `UPDATE pipeline_versions SET spec = $1, spec_hash = $2, note = $3, created_at = $4 WHERE id = $5 AND sealed_at IS NULL`,
		specMap, hash, note, now, v.ID)
	if err != nil || tag.RowsAffected() == 0 {
		return false, err
	}
	v.Spec, v.SpecHash, v.Note, v.CreatedAt = specMap, hash, note, now
	return true, nil
}

// SealVersion marks a version as depended on (deployed, sent for approval): it never changes
// again. It refuses (409) when the draft was saved again after its checks ran, so what is deployed
// or reviewed is always exactly what was checked.
func SealVersion(ctx context.Context, q db.Querier, v *models.PipelineVersion) error {
	if v.SealedAt != nil {
		return nil
	}
	now := db.Now()
	tag, err := q.Exec(ctx, `UPDATE pipeline_versions SET sealed_at = $1 WHERE id = $2 AND (sealed_at IS NOT NULL OR spec_hash = $3)`, now, v.ID, v.SpecHash)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return problem.Conflict("version_changed", fmt.Sprintf("v%d was saved again while it was being checked. Run the checks again.", v.Number), nil)
	}
	v.SealedAt = &now
	return nil
}

// PoliciesOf loads a project's policies.
func PoliciesOf(ctx context.Context, q db.Querier, projectID uuid.UUID) (domain.ProjectPolicies, error) {
	proj, err := db.Find[models.Project](ctx, q, `SELECT * FROM projects WHERE id = $1`, projectID)
	if err != nil {
		return domain.ProjectPolicies{}, err
	}
	if proj == nil || len(proj.Policies) == 0 {
		return domain.DefaultPolicies(), nil
	}
	p := domain.LoadPolicies(proj.Policies)
	if p.Unreadable {
		warnUnreadable(projectID, len(p.Problems))
	}
	return p, nil
}

var (
	policyLog      = logging.Logger("bezisync.services.policies")
	policyWarnedAt sync.Map // project id → time of the last warning
)

// warnUnreadable logs unreadable policies at most every 10 minutes per project (the controller
// reads policies on every tick).
func warnUnreadable(projectID uuid.UUID, problems int) {
	now := time.Now()
	if last, ok := policyWarnedAt.Load(projectID); ok && now.Sub(last.(time.Time)) < 10*time.Minute {
		return
	}
	policyWarnedAt.Store(projectID, now)
	policyLog.Warn("project policies can't be read: changes are blocked until an Admin saves them",
		"project_id", projectID.String(), "problems", problems)
}

// UnreadablePolicies is the refusal of a change in a project whose policies can't be read.
func UnreadablePolicies() error {
	c := domain.UnreadableCheck()
	return problem.New(409, c.Code, c.Message).WithErrors([]map[string]any{{"loc": []string{c.Code}, "message": c.Message}})
}

// EnforceSavePolicies refuses building blocks the project does not allow.
func EnforceSavePolicies(ctx context.Context, q db.Querier, projectID uuid.UUID, runtimeID, sourceID, sinkID *uuid.UUID) error {
	var sourceType, sinkType string
	if sourceID != nil {
		c, err := GetConnection(ctx, q, *sourceID)
		if err != nil {
			return err
		}
		if c != nil {
			sourceType = c.Type
		}
	}
	if sinkID != nil {
		c, err := GetConnection(ctx, q, *sinkID)
		if err != nil {
			return err
		}
		if c != nil {
			sinkType = c.Type
		}
	}
	pol, err := PoliciesOf(ctx, q, projectID)
	if err != nil {
		return err
	}
	if pol.Unreadable {
		return UnreadablePolicies()
	}
	violations := domain.SaveViolations(pol, runtimeID, sourceType, sinkType)
	if len(violations) == 0 {
		return nil
	}
	msgs := make([]string, len(violations))
	errs := make([]map[string]any, len(violations))
	for i, v := range violations {
		msgs[i] = v.Message
		errs[i] = map[string]any{"loc": []string{v.Code}, "message": v.Message}
	}
	return problem.New(422, "policy_violation", strings.Join(msgs, " ")).WithErrors(errs)
}

// NewPipeline are the fields of a pipeline to create.
type NewPipeline struct {
	Name, Description, Owner string
	Labels                   map[string]string
	SourceID                 uuid.UUID
	SinkID, RuntimeID        *uuid.UUID
	Spec                     domain.PipelineSpec
	// Engine is the requested engine ("" = the default for the source and destination).
	Engine string
}

// CreatePipeline creates a draft pipeline with version 1.
func CreatePipeline(ctx context.Context, q db.Querier, a *auth.ProjectAccess, n NewPipeline) (*models.Pipeline, error) {
	source, err := GetConnectionOfKind(ctx, q, a.Project.ID, n.SourceID, "source")
	if err != nil {
		return nil, err
	}
	sinkType, err := connectionType(ctx, q, n.SinkID)
	if err != nil {
		return nil, err
	}
	eng, err := ChooseEngine(ctx, q, a.Project.ID, n.Engine, "", source.Type, sinkType)
	if err != nil {
		return nil, err
	}
	if err := ValidateSinkSpec(ctx, q, a.Project.ID, n.SinkID, n.Spec, eng); err != nil {
		return nil, err
	}
	if err := ValidateEngineSpec(eng, n.Spec); err != nil {
		return nil, err
	}
	if err := RefuseNewAdvanced(eng, nil, n.Spec); err != nil {
		return nil, err
	}
	if err := refuseDriverProps(ctx, q, n.SourceID, nil, n.Spec); err != nil {
		return nil, err
	}
	if err := ValidateTuning(eng, source.Type, sinkType, n.Spec); err != nil {
		return nil, err
	}
	if rules, err := PoliciesOf(ctx, q, a.Project.ID); err != nil {
		return nil, err
	} else if err := RefuseSettingLimits(rules, eng, n.Spec, nil); err != nil {
		return nil, err
	}
	if err := enforceEnginePolicy(ctx, q, a.Project.ID, eng); err != nil {
		return nil, err
	}
	if n.RuntimeID != nil {
		if _, err := projectRuntime(ctx, q, a.Project.ID, *n.RuntimeID); err != nil {
			return nil, err
		}
	}
	if err := ensureNameFree(ctx, q, a.Project.ID, n.Name); err != nil {
		return nil, err
	}
	if err := EnforceSavePolicies(ctx, q, a.Project.ID, n.RuntimeID, &n.SourceID, n.SinkID); err != nil {
		return nil, err
	}
	labels := db.Map{}
	for k, v := range n.Labels {
		labels[k] = v
	}
	now := db.Now()
	p := &models.Pipeline{ID: db.NewID(), ProjectID: a.Project.ID, Name: n.Name, Description: n.Description, Owner: n.Owner,
		Labels: labels, SourceID: n.SourceID, SinkID: n.SinkID, RuntimeID: n.RuntimeID, DesiredState: "paused", Engine: string(eng),
		Status: db.Map{"lifecycle": "draft", "health": "unknown", "summary": "Not deployed yet."}, Lifecycle: "draft",
		Health: "unknown", Artifacts: db.Map{}, Version: 1, CreatedByID: &a.Principal.User.ID, CreatedAt: now, UpdatedAt: now}
	if err := db.Insert(ctx, q, "pipelines", p); err != nil {
		return nil, err
	}
	v, err := newVersion(ctx, q, p, n.Spec, a, "Created")
	if err != nil {
		return nil, err
	}
	if err := SavePipeline(ctx, q, p); err != nil {
		return nil, err
	}
	_, err = Record(ctx, q, AuditEvent{Kind: "pipeline.created", Summary: fmt.Sprintf("Created pipeline %s (v%d)", n.Name, v.Number),
		ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "pipeline", ResourceID: &p.ID})
	return p, err
}

// SavePipeline writes every column of a pipeline (updated_at refreshed).
func SavePipeline(ctx context.Context, q db.Querier, p *models.Pipeline) error {
	p.UpdatedAt = db.Now()
	return db.Update(ctx, q, "pipelines", p)
}

func ensureNameFree(ctx context.Context, q db.Querier, projectID uuid.UUID, name string) error {
	n, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM pipelines WHERE project_id = $1 AND name = $2 AND deleted_at IS NULL`, projectID, name)
	if err != nil {
		return err
	}
	if n > 0 {
		return problem.Conflict("pipeline_exists", fmt.Sprintf("A pipeline named '%s' already exists.", name), nil)
	}
	return nil
}

// RequireComplete returns the sink and runtime of a deployable pipeline.
func RequireComplete(ctx context.Context, q db.Querier, p *models.Pipeline) (*models.Connection, *models.Runtime, error) {
	var sink *models.Connection
	var rt *models.Runtime
	var err error
	if p.SinkID != nil {
		if sink, err = GetConnection(ctx, q, *p.SinkID); err != nil {
			return nil, nil, err
		}
	}
	if p.RuntimeID != nil {
		if rt, err = GetRuntime(ctx, q, *p.RuntimeID); err != nil {
			return nil, nil, err
		}
	}
	if sink == nil || rt == nil {
		var missing []string
		if sink == nil {
			missing = append(missing, "destination")
		}
		if rt == nil {
			missing = append(missing, "runtime")
		}
		return nil, nil, problem.New(409, "incomplete_draft", fmt.Sprintf("Choose a %s first.", strings.Join(missing, " and a ")))
	}
	return sink, rt, nil
}

// PipelineEdit are the optional changes of an update.
type PipelineEdit struct {
	ExpectedVersion          int
	Spec                     *domain.PipelineSpec
	Description, Owner, Name *string
	Labels                   map[string]string
	LabelsSet                bool
	RuntimeID, SinkID        *uuid.UUID
	// Engine is a requested engine; nil keeps the current one while it still runs the pipeline.
	Engine *string
	Note   string
	// Separate always starts a new version (a change that must be its own, like a Debezium image
	// move), instead of updating the caller's working draft.
	Separate bool
}

// nextEngine is the engine a pipeline has after an edit: the requested one, or the current one
// while it still runs the (new) destination, else the pair's default. A deployed pipeline keeps
// its engine; the spec must hold only settings of that engine.
func nextEngine(ctx context.Context, q db.Querier, a *auth.ProjectAccess, p *models.Pipeline, e PipelineEdit) (engine.ID, error) {
	current := EngineOf(p)
	sinkID := p.SinkID
	if e.SinkID != nil {
		sinkID = e.SinkID
	}
	requested := ""
	if e.Engine != nil {
		requested = *e.Engine
	}
	id := current
	sinkChanged := e.SinkID != nil && (p.SinkID == nil || *e.SinkID != *p.SinkID)
	if requested != "" || sinkChanged {
		sourceType, err := connectionType(ctx, q, &p.SourceID)
		if err != nil {
			return "", err
		}
		sinkType, err := connectionType(ctx, q, sinkID)
		if err != nil {
			return "", err
		}
		if id, err = ChooseEngine(ctx, q, a.Project.ID, requested, p.Engine, sourceType, sinkType); err != nil {
			return "", err
		}
	}
	if id != current {
		if p.DeployedVersionID != nil {
			return "", problem.Conflict("engine_after_deploy", "The engine of a deployed pipeline can't change. Create a new pipeline.", nil)
		}
		if err := enforceEnginePolicy(ctx, q, a.Project.ID, id); err != nil {
			return "", err
		}
	}
	spec := e.Spec
	if spec == nil && id != current {
		if spec, err := SpecOf(ctx, q, p.CurrentVersionID); err != nil {
			return "", err
		} else if spec != nil {
			if err := ValidateEngineSpec(id, *spec); err != nil {
				return "", err
			}
			// The stored format must suit the new engine too (Flink CDC writes Kafka only as JSON).
			return id, ValidateSinkSpec(ctx, q, a.Project.ID, sinkID, *spec, id)
		}
	}
	if spec != nil {
		if err := ValidateEngineSpec(id, *spec); err != nil {
			return "", err
		}
	}
	return id, nil
}

// UpdatePipeline applies an edit; returns the version holding the spec when it changed: the
// caller's working draft, updated in place, or a new version when the current one is sealed or
// someone else's.
func UpdatePipeline(ctx context.Context, q db.Querier, a *auth.ProjectAccess, p *models.Pipeline, e PipelineEdit) (*models.PipelineVersion, error) {
	if e.ExpectedVersion != p.Version {
		return nil, problem.New(412, "version_conflict", "Someone else changed this pipeline. Reload and try again.").
			WithExtra(map[string]any{"current_version": p.Version})
	}
	eng, err := nextEngine(ctx, q, a, p, e)
	if err != nil {
		return nil, err
	}
	var created *models.PipelineVersion
	if e.Spec != nil {
		sinkID := p.SinkID
		if e.SinkID != nil {
			sinkID = e.SinkID
		}
		if err := ValidateSinkSpec(ctx, q, a.Project.ID, sinkID, *e.Spec, eng); err != nil {
			return nil, err
		}
		current, err := VersionOf(ctx, q, p.CurrentVersionID)
		if err != nil {
			return nil, err
		}
		var prev *domain.PipelineSpec
		if current != nil {
			s := domain.MustSpec(map[string]any(current.Spec))
			prev = &s
		}
		if err := RefuseNewAdvanced(eng, prev, *e.Spec); err != nil {
			return nil, err
		}
		if err := refuseDriverProps(ctx, q, p.SourceID, prev, *e.Spec); err != nil {
			return nil, err
		}
		// A changed spec in a project whose policies can't be read is refused (ADR 0002); saving
		// the same spec again (an autosave) is not a change.
		if current == nil || current.SpecHash != domain.SpecHash(*e.Spec) {
			rules, err := PoliciesOf(ctx, q, p.ProjectID)
			if err != nil {
				return nil, err
			}
			if rules.Unreadable {
				return nil, UnreadablePolicies()
			}
		}
		if len(e.Spec.Tuning) > 0 {
			src, err := GetConnection(ctx, q, p.SourceID)
			if err != nil {
				return nil, err
			}
			sinkType, err := connectionType(ctx, q, sinkID)
			if err != nil {
				return nil, err
			}
			if err := ValidateTuning(eng, src.Type, sinkType, *e.Spec); err != nil {
				return nil, err
			}
			rules, err := PoliciesOf(ctx, q, p.ProjectID)
			if err != nil {
				return nil, err
			}
			if err := RefuseSettingLimits(rules, eng, *e.Spec, prev); err != nil {
				return nil, err
			}
		}
		switch {
		case current != nil && current.SpecHash == domain.SpecHash(*e.Spec):
		case current != nil && !e.Separate && isDraftOf(current, a):
			amended, err := amendVersion(ctx, q, current, *e.Spec, e.Note)
			if err != nil {
				return nil, err
			}
			if amended {
				created = current
				break
			}
			fallthrough
		default:
			note := e.Note
			if note == "" {
				note = "Edited"
			}
			if created, err = newVersion(ctx, q, p, *e.Spec, a, note); err != nil {
				return nil, err
			}
		}
	}
	if e.Description != nil {
		p.Description = *e.Description
	}
	if e.Owner != nil {
		p.Owner = *e.Owner
	}
	if e.LabelsSet {
		labels := db.Map{}
		same := len(e.Labels) == len(p.Labels)
		for k, v := range e.Labels {
			labels[k] = v
			same = same && p.Labels[k] == v
		}
		// Changed labels follow the create-time rule; unchanged ones (even an older pipeline's
		// that break it) save as they are.
		if !same && !domain.LabelsValid(e.Labels) {
			v := &val.V{}
			v.Fail(val.Loc{"body", "labels"}, "value_error", domain.LabelsMessage)
			return nil, v.Problem()
		}
		p.Labels = labels
	}
	if e.Name != nil && *e.Name != p.Name {
		if p.DeployedVersionID != nil {
			return nil, problem.Conflict("rename_after_deploy", "A deployed pipeline cannot be renamed.", nil)
		}
		if err := ensureNameFree(ctx, q, a.Project.ID, *e.Name); err != nil {
			return nil, err
		}
		p.Name = *e.Name
	}
	if e.SinkID != nil && (p.SinkID == nil || *e.SinkID != *p.SinkID) {
		if p.DeployedVersionID != nil {
			return nil, problem.Conflict("sink_after_deploy", "The destination of a deployed pipeline cannot change. Create a new pipeline.", nil)
		}
		if _, err := GetConnectionOfKind(ctx, q, a.Project.ID, *e.SinkID, "sink"); err != nil {
			return nil, err
		}
		if err := EnforceSavePolicies(ctx, q, a.Project.ID, nil, nil, e.SinkID); err != nil {
			return nil, err
		}
		p.SinkID = e.SinkID
	}
	if e.RuntimeID != nil && (p.RuntimeID == nil || *e.RuntimeID != *p.RuntimeID) {
		if p.DeployedVersionID != nil && p.DesiredState == "running" {
			return nil, problem.Conflict("pipeline_running", "Pause the pipeline before moving it to another runtime.", nil)
		}
		if _, err := projectRuntime(ctx, q, a.Project.ID, *e.RuntimeID); err != nil {
			return nil, err
		}
		if err := EnforceSavePolicies(ctx, q, a.Project.ID, e.RuntimeID, nil, nil); err != nil {
			return nil, err
		}
		p.RuntimeID = e.RuntimeID
	}
	p.Engine = string(eng)
	p.Version++
	if err := SavePipeline(ctx, q, p); err != nil {
		return nil, err
	}
	summary := "Edited pipeline " + p.Name
	if created != nil {
		summary += fmt.Sprintf(" → v%d", created.Number)
	}
	_, err = Record(ctx, q, AuditEvent{Kind: "pipeline.updated", Summary: summary, ProjectID: &a.Project.ID, Principal: a.Principal,
		ResourceType: "pipeline", ResourceID: &p.ID})
	return created, err
}

// RefuseNewAdvanced refuses advanced properties a save adds or changes that BeziSync can't pass
// on (ClassifyAdvanced). Ones the stored spec already holds with the same value still save, so an
// older pipeline can be edited elsewhere: preflight blocks deploying them until they are removed.
func RefuseNewAdvanced(eng engine.ID, prev *domain.PipelineSpec, next domain.PipelineSpec) error {
	keys := make([]string, 0, len(next.Advanced))
	for k := range next.Advanced {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var errs []map[string]any
	for _, k := range keys {
		value := next.Advanced[k]
		if prev != nil {
			if old, ok := prev.Advanced[k]; ok && old == value {
				continue
			}
		}
		v := compiler.ClassifyAdvanced(k, value, nil)
		if eng == engine.FlinkCDC {
			v = compiler.ClassifyFlinkAdvanced(k, value, nil)
		}
		if v.Refused() {
			errs = append(errs, map[string]any{"loc": []string{"advanced", k}, "message": v.Message})
		}
	}
	if len(errs) > 0 {
		return problem.New(422, "managed_property", "Some advanced properties cannot be set.").WithErrors(errs)
	}
	return nil
}

// NoJDBC are sources without a JDBC driver: debezium.source.driver.* means nothing to them (C21).
var NoJDBC = []string{"mongodb", "spanner"}

// DriverPropertyRefusal is why a raw key can't be set for a source type ("" when it can).
func DriverPropertyRefusal(sourceType, key string) string {
	if strings.HasPrefix(key, "debezium.source.driver.") && slices.Contains(NoJDBC, sourceType) {
		return "this source has no JDBC driver, so driver.* properties do nothing"
	}
	if option, ok := strings.CutPrefix(key, "source."); ok {
		return connectors.FlinkSourceIgnores(sourceType, option)
	}
	return ""
}

// refuseDriverProps refuses new driver.* advanced properties on a source without JDBC.
func refuseDriverProps(ctx context.Context, q db.Querier, sourceID uuid.UUID, prev *domain.PipelineSpec, next domain.PipelineSpec) error {
	if len(next.Advanced) == 0 {
		return nil
	}
	src, err := GetConnection(ctx, q, sourceID)
	if err != nil || src == nil {
		return err
	}
	var errs []map[string]any
	for _, k := range tuningKeys(next.Advanced) {
		if prev != nil && prev.Advanced[k] == next.Advanced[k] {
			continue
		}
		if why := DriverPropertyRefusal(src.Type, k); why != "" {
			errs = append(errs, map[string]any{"loc": []string{"advanced", k}, "message": why})
		}
	}
	if len(errs) > 0 {
		return problem.New(422, "managed_property", "Some advanced properties cannot be set.").WithErrors(errs)
	}
	return nil
}

// advancedCheck blocks a deploy whose advanced properties BeziSync can't pass on, including ones
// that would change what it compiled for this pipeline. It is a configuration check, so a forced
// deploy can't skip it.
func advancedCheck(spec domain.PipelineSpec, managed map[string]bool, flink bool, sourceType string) *checks.Check {
	refused := compiler.AdvancedRefusals(spec.Advanced, managed)
	if flink {
		refused = compiler.FlinkAdvancedRefusals(spec.Advanced, managed)
		for _, k := range sortedStrings(spec.Advanced) {
			if block, option, _ := strings.Cut(k, "."); block == "source" && !slices.ContainsFunc(refused, func(v compiler.AdvancedVerdict) bool { return v.Key == k }) {
				if why := connectors.FlinkSourceIgnores(sourceType, option); why != "" {
					refused = append(refused, compiler.AdvancedVerdict{Key: k, Class: compiler.AdvancedInvalid, Message: why})
				}
			}
		}
	}
	if len(refused) == 0 {
		return nil
	}
	keys := make([]string, len(refused))
	why := make([]string, len(refused))
	for i, r := range refused {
		keys[i] = r.Key
		why[i] = r.Key + ": " + r.Message
	}
	what, them := "property", "it"
	if len(refused) > 1 {
		what, them = "properties", "them"
	}
	return &checks.Check{Code: "advanced_properties", Title: "Advanced properties", Category: checks.CatDestination, Status: checks.Fail,
		Blocking: true, Message: fmt.Sprintf("%d advanced %s can't be set: %s. Remove %s in Review.", len(refused), what, strings.Join(keys, ", "), them),
		Technical: strings.Join(why, " ")}
}

// RenderForPeople is the compiled configuration as people see it (preflight, Config): what runs,
// except that advanced properties that look like credentials show as hidden. Pipelines saved
// before those were refused may still hold one; the workload keeps the real value.
func RenderForPeople(c *compiler.Compiled, spec domain.PipelineSpec) string {
	if c.Flink != nil {
		return c.Render()
	}
	props := make(map[string]string, len(c.Properties))
	for k, v := range c.Properties {
		props[k] = v
	}
	for k := range spec.Advanced {
		if _, ok := props[k]; ok && compiler.ClassifyAdvanced(k, "", nil).Class == compiler.AdvancedCredential {
			props[k] = "(hidden)"
		}
	}
	return compiler.RenderProperties(props)
}

// redactSettings hides the values of advanced properties that look like credentials: pipelines
// saved before those were refused may still hold one, and a change summary never repeats it.
func redactSettings(sc *domain.SettingsChange) *domain.SettingsChange {
	if sc == nil {
		return nil
	}
	hidden := "(hidden)"
	for i, c := range sc.Settings {
		if c.Unverified && compiler.ClassifyAdvanced(c.Key, "", nil).Class == compiler.AdvancedCredential {
			if c.From != nil {
				sc.Settings[i].From = &hidden
			}
			if c.To != nil {
				sc.Settings[i].To = &hidden
			}
		}
	}
	return sc
}

// unverifiedCheck says which advanced properties are passed through as typed: BeziSync can't
// check what they do, so they are Unknown (grey), never a pass.
func unverifiedCheck(spec domain.PipelineSpec) *checks.Check {
	if len(spec.Advanced) == 0 {
		return nil
	}
	keys := make([]string, 0, len(spec.Advanced))
	for k := range spec.Advanced {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return &checks.Check{Code: "advanced_unverified", Title: "Advanced properties", Category: checks.CatDestination, Status: checks.Unknown,
		Message: fmt.Sprintf("Passed on as typed, not verified by BeziSync: %s.", strings.Join(keys, ", "))}
}

// experimentalCheck warns about tuned settings the engine marks experimental (F7).
func experimentalCheck(id engine.ID, spec domain.PipelineSpec) *checks.Check {
	var keys []string
	for _, k := range tuningKeys(spec.Tuning) {
		if s := engine.SettingOf(id, k); s != nil && s.Experimental {
			keys = append(keys, s.Title+" ("+k+")")
		}
	}
	if len(keys) == 0 {
		return nil
	}
	return &checks.Check{Code: "setting_experimental", Title: "Experimental settings", Category: checks.CatDestination, Status: checks.Warn,
		Message: "The engine marks these experimental: they may change or go in a later release: " + strings.Join(keys, ", ") + "."}
}

// CompileFor compiles a pipeline with a spec ("" image = resolve it).
func CompileFor(ctx context.Context, q db.Querier, p *models.Pipeline, spec domain.PipelineSpec, image string) (*compiler.Compiled, error) {
	cctx := connectors.CompileContext{PipelineID: p.ID.String(), ShortID: p.ShortID(), Name: p.Name, Spec: spec}
	sink, rt, err := RequireComplete(ctx, q, p)
	if err != nil {
		return nil, err
	}
	flink := EngineOf(p) == engine.FlinkCDC
	if image == "" {
		if flink {
			image, err = ResolveFlinkImage(ctx, q, spec)
		} else {
			image, err = ResolveImage(ctx, q, spec, rt, sink.Type)
		}
		if err != nil {
			return nil, err
		}
	}
	source, err := GetConnection(ctx, q, p.SourceID)
	if err != nil {
		return nil, err
	}
	src, err := Resolve(ctx, q, source)
	if err != nil {
		return nil, err
	}
	snk, err := Resolve(ctx, q, sink)
	if err != nil {
		return nil, err
	}
	if flink {
		in, err := flinkInput(ctx, q, p, spec, rt)
		if err != nil {
			return nil, err
		}
		f, err := compiler.CompileFlink(cctx, src, snk, image, *in)
		if err != nil {
			return nil, err
		}
		c := compiler.FromFlink(f)
		c.Placement = PlacementOf(p, spec, rt)
		return c, nil
	}
	c, err := compiler.Compile(cctx, src, snk, image)
	if err != nil {
		return nil, err
	}
	c.Placement = PlacementOf(p, spec, rt)
	return c, nil
}

// WorkloadFor is the desired workload of a deployed pipeline (nil when never deployed).
func WorkloadFor(ctx context.Context, q db.Querier, p *models.Pipeline) (*runtime.Workload, error) {
	spec, err := SpecOf(ctx, q, p.DeployedVersionID)
	if err != nil || spec == nil {
		return nil, err
	}
	if override, ok := p.Artifacts["snapshot_override"].(string); ok && override != "" {
		// Set by "skip to now": start without copying existing rows until the next deploy.
		s := spec.Clone()
		s.Snapshot.Mode = override
		spec = &s
	}
	image := ""
	if p.DeployedImage != nil {
		image = *p.DeployedImage
	}
	c, err := CompileFor(ctx, q, p, *spec, image)
	if err != nil {
		return nil, err
	}
	var rtConfig map[string]any
	if p.RuntimeID != nil {
		rt, err := GetRuntime(ctx, q, *p.RuntimeID)
		if err != nil {
			return nil, err
		}
		if rt != nil {
			rtConfig = rt.Config
		}
	}
	pull := []string{}
	if l, ok := rtConfig["image_pull_secrets"].([]any); ok {
		for _, s := range l {
			pull = append(pull, fmt.Sprint(s))
		}
	}
	sa := ""
	if v, ok := rtConfig["service_account"]; ok && v != nil {
		sa = fmt.Sprint(v)
	}
	files := c.Files
	if files == nil {
		files = map[string]string{}
	}
	if c.Flink != nil {
		return &runtime.Workload{PipelineID: p.ID.String(), ShortID: p.ShortID(), Name: p.Name, Image: c.Image, Env: c.Env, Files: files,
			ConfigHash: c.ConfigHash(), Running: p.DesiredState == "running", ImagePullSecrets: pull, ServiceAccount: sa, Tunnel: c.Tunnel,
			HealthPort: c.HealthPort, Engine: string(engine.FlinkCDC), Flink: c.Flink.Topology(config.Get().AgentImage),
			StopGraceSeconds: compiler.FlinkStopGrace, Placement: c.Placement}, nil
	}
	return &runtime.Workload{PipelineID: p.ID.String(), ShortID: p.ShortID(), Name: p.Name, Image: c.Image, Properties: c.Render(),
		Env: c.Env, Files: files, ConfigHash: c.ConfigHash(), CPU: fmt.Sprint(c.Resources["cpu"]), Memory: fmt.Sprint(c.Resources["memory"]),
		Heap: fmt.Sprint(c.Resources["heap"]), Running: p.DesiredState == "running", ImagePullSecrets: pull, ServiceAccount: sa, Tunnel: c.Tunnel,
		HealthPort: c.HealthPort, Placement: c.Placement}, nil
}

func cpuOf(v string) float64 { return runtime.QuantityCPU(v) }

func toFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case interface{ Float64() (float64, error) }:
		f, _ := x.Float64()
		return f
	case string:
		f, _ := strconv.ParseFloat(x, 64)
		return f
	}
	return 0
}

// CapacityCheck warns when a runtime has no room for the pipeline's size.
func CapacityCheck(capacity map[string]any, policies domain.Policies) *checks.Check {
	return capacityCheck(capacity, DebeziumNeed(policies), Need{}, "")
}

// Need is what a pipeline asks the runtime for: CPUs and MiB.
type Need struct {
	CPU float64
	MiB int
}

// DebeziumNeed is what a Debezium pipeline's container asks for.
func DebeziumNeed(policies domain.Policies) Need {
	r := compiler.ResourceFor(policies)
	return Need{CPU: cpuOf(r.CPU), MiB: int(runtime.QuantityMem(r.Memory) >> 20)}
}

// capacityCheck compares what a pipeline asks for (beyond what it already runs, when it is
// redeployed) with the runtime's capacity as just measured. Unknown, never a pass, without a
// measurement. Over the namespace's ResourceQuota blocks: Kubernetes would refuse the pods. Over
// the free CPU or memory warns: the pods may wait for room.
func capacityCheck(capacity map[string]any, need, running Need, what string) *checks.Check {
	return capacityCheckWith(capacity, need, running, what, pending{})
}

// pending is what other pipelines on the runtime will ask for once their pods exist: deployed to
// run, but not yet counted in the runtime's measured capacity (B3).
type pending struct {
	Need
	names []string
}

// capacityCheckWith is capacityCheck counting pipelines still starting on the runtime: two
// deploys seconds apart both passed while the first one's pods didn't exist yet.
func capacityCheckWith(capacity map[string]any, need, running Need, what string, others pending) *checks.Check {
	title := "Runtime capacity"
	if len(capacity) == 0 || toFloat(capacity["cpu_allocatable"]) == 0 {
		return &checks.Check{Code: "runtime_capacity", Title: title, Category: checks.CatCapacity, Status: checks.Unknown,
			Message: "The runtime didn't report its free CPU and memory just now, so BeziSync can't tell whether this pipeline fits."}
	}
	extra := Need{CPU: max(need.CPU-running.CPU, 0), MiB: max(need.MiB-running.MiB, 0)}
	asks := fmt.Sprintf("this pipeline asks for %s CPU and %d MiB%s", strconv.FormatFloat(need.CPU, 'g', -1, 64), need.MiB, what)
	if running != (Need{}) {
		asks += fmt.Sprintf(", %s CPU and %d MiB more than it runs now", strconv.FormatFloat(extra.CPU, 'g', -1, 64), extra.MiB)
	}
	if len(others.names) > 0 {
		verb := "starts"
		if len(others.names) > 1 {
			verb = "start"
		}
		asks += fmt.Sprintf(", and %s %s here without pods yet, asking for %s CPU and %d MiB", strings.Join(others.names, ", "), verb,
			strconv.FormatFloat(others.CPU, 'g', -1, 64), others.MiB)
		extra.CPU += others.CPU
		extra.MiB += others.MiB
	}
	if q, _ := capacity["quota"].(bool); q {
		cpuFree, hasCPU := capacity["quota_cpu_free"]
		memFree, hasMem := capacity["quota_memory_free"]
		freeMiB := int(toFloat(memFree) / (1 << 20))
		if (hasCPU && extra.CPU > toFloat(cpuFree)+1e-9) || (hasMem && extra.MiB > freeMiB) {
			left := []string{}
			if hasCPU {
				left = append(left, domain.FormatFixed(toFloat(cpuFree), 2)+" CPU")
			}
			if hasMem {
				left = append(left, strconv.Itoa(freeMiB)+" MiB")
			}
			return &checks.Check{Code: "runtime_capacity", Title: title, Category: checks.CatCapacity, Status: checks.Fail, Blocking: true,
				Message: fmt.Sprintf("The namespace's ResourceQuota leaves %s; %s. Kubernetes would refuse its pods.", strings.Join(left, " and "), asks),
				Fix:     &checks.Fix{Kind: "text", Who: "you", Code: quotaFix(others)}}
		}
	}
	freeCPU := toFloat(capacity["cpu_allocatable"]) - toFloat(capacity["cpu_requested"])
	freeMiB := -1
	if m := toFloat(capacity["memory_allocatable"]); m > 0 {
		freeMiB = max(int((m-toFloat(capacity["memory_requested"]))/(1<<20)), 0)
	}
	free := domain.FormatFixed(max(freeCPU, 0), 1) + " CPU"
	if freeMiB >= 0 {
		free += fmt.Sprintf(" and %d MiB", freeMiB)
	}
	if freeCPU < extra.CPU || (freeMiB >= 0 && freeMiB < extra.MiB) {
		return &checks.Check{Code: "runtime_capacity", Title: title, Category: checks.CatCapacity, Status: checks.Warn,
			Message: fmt.Sprintf("The runtime has %s free; %s.", free, asks),
			Why:     "The connector may stay pending until resources free up."}
	}
	msg := fmt.Sprintf("%s free; %s.", free, asks)
	switch q, known := capacity["quota"].(bool); {
	case known && !q:
		msg += " The namespace has no ResourceQuota."
	case known:
		msg += " Within the namespace's ResourceQuota."
	}
	return &checks.Check{Code: "runtime_capacity", Title: title, Category: checks.CatCapacity, Status: checks.Pass, Message: msg}
}

func quotaFix(others pending) string {
	if len(others.names) > 0 {
		return "Wait until " + strings.Join(others.names, ", ") + " run (or pause one), choose a smaller size, or ask an admin to raise the namespace's ResourceQuota."
	}
	return "Choose a smaller size, or ask an admin to raise the namespace's ResourceQuota."
}

// pendingNeed sums what the other pipelines on this runtime ask for that should run but have no
// pods yet (the controller hasn't created them, or Kubernetes refused them): the measured capacity
// doesn't count them.
func pendingNeed(ctx context.Context, q db.Querier, p *models.Pipeline, rt *models.Runtime) (pending, error) {
	others, err := db.Select[models.Pipeline](ctx, q, `SELECT * FROM pipelines WHERE runtime_id = $1 AND id <> $2 AND deleted_at IS NULL
		AND desired_state = 'running' AND deployed_version_id IS NOT NULL ORDER BY name`, rt.ID, p.ID)
	if err != nil {
		return pending{}, err
	}
	var out pending
	for i := range others {
		o := &others[i]
		if !withoutPods(o) {
			continue
		}
		n, err := runningNeed(ctx, q, o, rt)
		if err != nil {
			return pending{}, err
		}
		out.CPU += n.CPU
		out.MiB += n.MiB
		out.names = append(out.names, o.Name)
	}
	return out, nil
}

// withoutPods reports a pipeline whose last observation found no pod: none yet, or one Kubernetes
// won't create.
func withoutPods(p *models.Pipeline) bool {
	w, _ := p.Status["workload"].(map[string]any)
	phase, _ := w["phase"].(string)
	reason, _ := w["reason"].(string)
	switch phase {
	case "", runtime.PhaseMissing:
		return true
	case runtime.PhasePending:
		return reason == runtime.ReasonNoPod || strings.HasPrefix(reason, runtime.ReasonRefusedPrefix)
	}
	return false
}

// runtimeReport tests the runtime, and returns the capacity it reported just now (nil when the
// test failed or reported none).
func runtimeReport(ctx context.Context, q db.Querier, rt *models.Runtime) (checks.Report, map[string]any) {
	started := time.Now()
	if !rt.Enabled {
		return checks.Summarize([]checks.Check{{Code: "runtime_disabled", Title: "Runtime " + rt.Name, Category: checks.CatDestination,
			Status: checks.Fail, Blocking: true, Message: fmt.Sprintf("Runtime %s is disabled, so nothing can be deployed on it.", rt.Name),
			Why: "An admin disabled (cordoned) this runtime. Pipelines already running there keep running."}}, started), nil
	}
	var c checks.Check
	var fresh map[string]any
	d, err := DriverFor(ctx, q, rt)
	if err == nil {
		res := d.Test(ctx)
		if res.OK && toFloat(res.Details["cpu_allocatable"]) > 0 {
			fresh = res.Details
		}
		status := checks.Fail
		if res.OK {
			status = checks.Pass
		}
		c = checks.Check{Code: "runtime_ready", Title: "Runtime " + rt.Name, Category: checks.CatDestination, Status: status, Blocking: !res.OK, Message: res.Message}
	} else {
		c = checks.Check{Code: "runtime_ready", Title: "Runtime " + rt.Name, Category: checks.CatDestination, Status: checks.Fail, Blocking: true, Message: err.Error()}
	}
	return checks.Summarize([]checks.Check{c}, started), fresh
}

// useFreshCapacity makes rt's capacity the one measured just now (none when it couldn't be): the
// checks, the Flink CDC shape and the compile then all see the same numbers. A direct runtime's
// is stored too (an agent stores its own with each report), so the deploy that follows packs
// with it.
//
// The write is its own statement on the pool, never part of the caller's transaction: a deploy
// holds its pipeline's row while the controller holds the runtime's (last_seen_at) and then wants
// a pipeline's, so a runtime write inside the deploy's transaction could deadlock with it. Alone
// it holds no other lock; a deploy that reads the runtime afterwards sees it committed.
func useFreshCapacity(ctx context.Context, _ db.Querier, rt *models.Runtime, fresh map[string]any) error {
	rt.Capacity = fresh
	if fresh == nil || rt.Mode == "agent" {
		return nil
	}
	_, err := db.MustPool(ctx).Exec(ctx, `UPDATE runtimes SET capacity = $1 WHERE id = $2 AND capacity IS DISTINCT FROM $1`, db.Map(fresh), rt.ID)
	return err
}

// runningNeed is what p runs on rt now (nothing when it isn't running there): a redeploy needs
// only what it adds.
func runningNeed(ctx context.Context, q db.Querier, p *models.Pipeline, rt *models.Runtime) (Need, error) {
	if p.DeployedVersionID == nil || p.DesiredState != "running" || p.RuntimeID == nil || *p.RuntimeID != rt.ID {
		return Need{}, nil
	}
	deployed, err := SpecOf(ctx, q, p.DeployedVersionID)
	if err != nil {
		return Need{}, err
	}
	if EngineOf(p) == engine.FlinkCDC {
		return FlinkNeed(FlinkSizeOf(p, *deployed, RuntimeCapacityFor(p, *deployed, rt)).Size), nil
	}
	return DebeziumNeed(deployed.Policies), nil
}

// checkWhere runs a connection check where the connector will run.
func checkWhere(ctx context.Context, rt *models.Runtime, conn connectors.Resolved, kind string, tables []string) (checks.Report, bool, error) {
	if rt.Mode == "agent" {
		// ADR 0016 D7: an agent that predates read-only destination checks would create test
		// tables; running the check from here instead would fail destinations only it can reach.
		if features, _ := rt.AgentState["features"].([]any); GatedForAgent(kind, conn.Type, features) {
			return OlderAgentReport(rt.Name, rt.AgentState["version"]), false, nil
		}
		raw, err := AgentRunCheck(ctx, rt.ID, kind, conn, tables)
		if err == nil {
			report, perr := ParseReport(raw)
			if perr == nil {
				return report, true, nil
			}
			err = perr
		}
		var re *runtime.Error
		if !errors.As(err, &re) {
			return checks.Report{}, false, err
		}
		report, rerr := RunCheck(ctx, conn, kind, tables)
		if rerr != nil {
			return checks.Report{}, false, rerr
		}
		note := checks.Check{Code: "checked_from_control_plane", Title: "Checked from BeziSync", Status: checks.Warn, Category: checks.CatNetwork,
			Message: fmt.Sprintf("The agent could not run this check (%s); BeziSync ran it instead, so ", re.Msg) +
				"results may differ inside the runtime's network.", Doc: checks.DocURL("checked_from_control_plane")}
		report.Checks = append([]checks.Check{note}, report.Checks...)
		return report, false, nil
	}
	report, err := RunCheck(ctx, conn, kind, tables)
	return report, false, err
}

// ParseReport reads a stored or remote check report.
func ParseReport(raw map[string]any) (checks.Report, error) {
	b, err := jsonMarshal(raw)
	if err != nil {
		return checks.Report{}, err
	}
	var r checks.Report
	if err := jsonUnmarshal(b, &r); err != nil {
		return checks.Report{}, err
	}
	if r.Checks == nil {
		r.Checks = []checks.Check{}
	}
	return r, nil
}

// Preflight checks a spec against the source, destination, runtime, config and policies.
func Preflight(ctx context.Context, q db.Querier, p *models.Pipeline, spec domain.PipelineSpec) (*PreflightReport, error) {
	started := time.Now()
	sinkConn, rt, err := RequireComplete(ctx, q, p)
	if err != nil {
		return nil, err
	}
	sourceConn, err := GetConnection(ctx, q, p.SourceID)
	if err != nil {
		return nil, err
	}
	// What would deploy: the spec with the project's pinned settings (written at deploy, P7).
	if rules, err := PoliciesOf(ctx, q, p.ProjectID); err != nil {
		return nil, err
	} else {
		spec = withPins(spec, Pins(rules, EngineOf(p), sourceConn.Type, sinkConn.Type, spec))
	}
	source, err := Resolve(ctx, q, sourceConn)
	if err != nil {
		return nil, err
	}
	sink, err := Resolve(ctx, q, sinkConn)
	if err != nil {
		return nil, err
	}
	tables := spec.Source.Tables.Include
	var wg sync.WaitGroup
	var srcReport, sinkReport, rtReport checks.Report
	var srcAgent, sinkAgent bool
	var srcErr, sinkErr error
	wg.Add(3)
	go func() { defer wg.Done(); srcReport, srcAgent, srcErr = checkWhere(ctx, rt, source, "source", tables) }()
	go func() { defer wg.Done(); sinkReport, sinkAgent, sinkErr = checkWhere(ctx, rt, sink, "sink", nil) }()
	var freshCapacity map[string]any
	go func() { defer wg.Done(); rtReport, freshCapacity = runtimeReport(ctx, db.MustPool(ctx), rt) }()
	wg.Wait()
	if err := useFreshCapacity(ctx, q, rt, freshCapacity); err != nil {
		return nil, err
	}
	if srcErr != nil {
		return nil, srcErr
	}
	if drop := connectors.FlinkDebeziumOnlyChecks(sourceConn.Type); len(drop) > 0 && EngineOf(p) == engine.FlinkCDC {
		kept := slices.DeleteFunc(slices.Clone(srcReport.Checks), func(c checks.Check) bool { return slices.Contains(drop, c.Code) })
		if len(kept) != len(srcReport.Checks) {
			srcReport = checks.Summarize(kept, started)
		}
	}
	if sinkErr != nil {
		return nil, sinkErr
	}
	agentNote := func(b bool) string {
		if b {
			return " (checked by the agent)"
		}
		return ""
	}
	sections := []PreflightSection{
		{Key: "source", Title: "Source · " + sourceConn.Name + agentNote(srcAgent), Report: srcReport},
		{Key: "sink", Title: "Destination · " + sinkConn.Name + agentNote(sinkAgent), Report: sinkReport},
		{Key: "runtime", Title: "Runtime · " + rt.Name, Report: rtReport},
	}
	running, err := runningNeed(ctx, q, p, rt)
	if err != nil {
		return nil, err
	}
	others, err := pendingNeed(ctx, q, p, rt)
	if err != nil {
		return nil, err
	}
	capacity := capacityCheckWith(rt.Capacity, DebeziumNeed(spec.Policies), running, "", others)
	var fit *checks.Check
	if EngineOf(p) == engine.FlinkCDC {
		placed := RuntimeCapacityFor(p, spec, rt)
		packed := FlinkSizeOf(p, spec, placed)
		capacity = capacityCheckWith(rt.Capacity, FlinkNeed(packed.Size), running, flinkWhat(packed.Size), others)
		fit = flinkClusterFitCheck(spec, packed, placed)
	}
	placement := placementCheck(p, spec, rt)
	if c := capacity; c != nil {
		// Appended after summarizing, like the Python report (no doc link, summary unchanged).
		sections[2].Report.Checks = append(sections[2].Report.Checks, *c)
	}
	sinkDescriptor, _ := connectors.Describe("sink", sinkConn.Type)
	for _, c := range []*checks.Check{PlatformCheck(rt.Capacity, sinkDescriptor), AgentHealthPortCheck(rt, sinkDescriptor), placement, fit} {
		if c != nil {
			sections[2].Report = checks.Summarize(append(sections[2].Report.Checks, *c), started)
		}
	}
	var properties *string
	destinations := map[string]string{}
	var cfgCheck checks.Check
	compiled, err := CompileFor(ctx, q, p, spec, "")
	var prob *problem.Problem
	switch {
	case err == nil:
		s := RenderForPeople(compiled, spec)
		properties = &s
		destinations = compiled.Destinations
		cfgCheck = checks.Check{Code: "config_valid", Title: "Configuration", Category: checks.CatDestination, Status: checks.Pass,
			Message: fmt.Sprintf("Generated %d Debezium Server properties.", len(compiled.Properties))}
		if compiled.Flink != nil {
			cfgCheck.Message = fmt.Sprintf("Generated the Flink CDC pipeline: %d tables on %d readers.", len(compiled.Destinations), compiled.Flink.Size.Parallelism())
		}
	case errors.As(err, &prob):
		var msgs []string
		for _, e := range prob.Errors {
			m, _ := e["message"].(string)
			msgs = append(msgs, m)
		}
		cfgCheck = checks.Check{Code: "config_valid", Title: "Configuration", Category: checks.CatDestination, Status: checks.Fail,
			Blocking: true, Message: prob.Detail, Technical: strings.Join(msgs, "; ")}
		if prob.Code == "managed_property" {
			// The same refusal, naming every property at once: what BeziSync compiles without them
			// tells which of the others it sets itself.
			clean := spec.Clone()
			clean.Advanced = map[string]string{}
			var managed map[string]bool
			if c, err := CompileFor(ctx, q, p, clean, ""); err == nil {
				managed = c.Managed
			}
			if c := advancedCheck(spec, managed, EngineOf(p) == engine.FlinkCDC, sourceConn.Type); c != nil {
				cfgCheck = *c
			}
		}
	default:
		return nil, err
	}
	configChecks := []checks.Check{cfgCheck}
	if compiled != nil {
		if c := advancedCheck(spec, compiled.Managed, compiled.Flink != nil, sourceConn.Type); c != nil {
			configChecks = append(configChecks, *c)
		} else if c := unverifiedCheck(spec); c != nil {
			configChecks = append(configChecks, *c)
		}
		if c := experimentalCheck(EngineOf(p), spec); c != nil {
			configChecks = append(configChecks, *c)
		}
	}
	flink := EngineOf(p) == engine.FlinkCDC
	var image string
	if flink {
		image, err = ResolveFlinkImage(ctx, q, spec)
	} else {
		image, err = ResolveImage(ctx, q, spec, rt, sinkConn.Type)
	}
	if err != nil {
		return nil, err
	}
	catalog := "Debezium"
	if flink {
		catalog = "Flink CDC"
	}
	standing, err := StandingFor(ctx, q, EngineOf(p), image)
	if connectors.InSinkImageFamily(sinkConn.Type, image) {
		// The sink's own build is never in the catalog of official images.
		standing = "offered"
	}
	if err != nil {
		return nil, err
	}
	switch standing {
	case "deprecated":
		configChecks = append(configChecks, checks.Check{Code: "debezium_image_deprecated", Title: catalog + " version", Category: checks.CatDestination,
			Status: checks.Warn, Message: image + " is deprecated in the " + catalog + " version catalog.",
			Why: "Your Admins marked this image deprecated; move to one they offer."})
	case "unlisted":
		configChecks = append(configChecks, checks.Check{Code: "debezium_image_unlisted", Title: catalog + " version", Category: checks.CatDestination,
			Status: checks.Warn, Message: image + " is not in the " + catalog + " version catalog.",
			Why: "Your Admins keep a catalog of tested " + catalog + " images; this one is not among them."})
	}
	srcDesc, _ := connectors.Describe("source", sourceConn.Type)
	sinkDesc, _ := connectors.Describe("sink", sinkConn.Type)
	if c := MinimumDebeziumCheck(image, srcDesc); c != nil && !flink {
		configChecks = append(configChecks, *c)
	}
	if spec.Policies.DeliveryCanary && !CanaryRuns(EngineOf(p), srcDesc, sinkDesc) {
		msg := "The delivery canary needs a source BeziSync can write a canary row to (" +
			connectors.CanarySources() + ") and a destination where BeziSync can find it again (not SQS or HTTP); it stays off for this pipeline."
		if flink {
			msg = "Flink CDC pipelines have no delivery canary: they capture only the pipeline's tables, so a canary row would never arrive. It stays off; health comes from the job and its saved positions."
		}
		configChecks = append(configChecks, checks.Check{Code: "delivery_canary_unavailable", Title: "Delivery canary", Status: checks.Warn,
			Category: checks.CatResilience, Message: msg})
	}
	if srcDesc != nil && !srcDesc.Capture.Incremental && p.DeployedVersionID != nil {
		deployed, err := SpecOf(ctx, q, p.DeployedVersionID)
		if err != nil {
			return nil, err
		}
		if added := domain.SummarizeChange(deployed, spec).TablesAdded; len(added) > 0 {
			configChecks = append(configChecks, checks.Check{Code: "tables_added_not_copied", Title: "Added tables", Status: checks.Warn,
				Category: checks.CatTables, Message: fmt.Sprintf("%s can't copy the rows %s already has: the pipeline streams only their new changes.",
					srcDesc.Title, strings.Join(added, ", ")),
				Why: "Copying existing rows of tables added to a running pipeline needs incremental snapshots, which this source doesn't offer."})
		}
	}
	if flink {
		c, err := flinkReadersCheck(ctx, q, p, spec, RuntimeCapacityFor(p, spec, rt))
		if err != nil {
			return nil, err
		}
		if c != nil {
			configChecks = append(configChecks, *c)
		}
		if c, err = flinkTablesAddedCheck(ctx, q, p, spec, sourceConn.Type); err != nil {
			return nil, err
		} else if c != nil {
			configChecks = append(configChecks, *c)
		}
		layoutChecks, err := flinkLayoutChecks(ctx, q, p, spec, source, sink)
		if err != nil {
			return nil, err
		}
		configChecks = append(configChecks, layoutChecks...)
	}
	discover := &discoverOnce{src: source, typ: sourceConn.Type}
	var recommended []Recommendation
	if !flink {
		if c := settingVersionCheck(spec, image); c != nil {
			configChecks = append(configChecks, *c)
		}
		limit, used, known := connectionsOf(ctx, source, sourceConn.Type, rt)
		if c := threadsCheck(p, spec, limit, used, known); c != nil {
			configChecks = append(configChecks, *c)
		}
		found, derr := discover.get(ctx)
		var avgRow, rows int64
		if derr == nil {
			avgRow, rows = averageRow(found, spec.Source.Tables.Include)
		}
		configChecks = append(configChecks, memoryChecks(spec, compiled, avgRow)...)
		recommended = recommendations(p, spec, sourceConn.Type, rows, len(spec.Source.Tables.Include), limit, used, known, time.Now())
	}
	if slices.ContainsFunc(spec.Transforms, func(t domain.Transform) bool { return t.Type == "truncate" }) {
		if found, err := discover.get(ctx); err == nil {
			configChecks = append(configChecks, truncateChecks(found, spec, flink)...)
		}
	}
	if slices.ContainsFunc(spec.Transforms, func(t domain.Transform) bool { return t.Type == "route" }) {
		if found, err := discover.get(ctx); err == nil {
			configChecks = append(configChecks, routeChecks(found, spec)...)
		}
	}
	configChecks = append(configChecks, transformChecks(ctx, sourceConn, discover, p.Engine, p.Name, spec)...)
	configChecks = append(configChecks, personalDataChecks(ctx, srcDesc, discover, spec)...)
	if sinkDesc != nil && compiled != nil {
		cctx := connectors.CompileContext{PipelineID: p.ID.String(), ShortID: p.ShortID(), Name: p.Name, Spec: spec}
		configChecks = append(configChecks, sinkChecks(ctx, sink, sinkDesc, cctx, compiled.Destinations, discover, flink)...)
	}
	var engineChecks []checks.Check
	if flink {
		if engineChecks, err = FlinkChecks(ctx, q, p, spec, rt, sourceConn, discover, srcReport); err != nil {
			return nil, err
		}
		// A check whose failure the job can't survive goes where a forced deploy can't skip it.
		for i := 0; i < len(engineChecks); {
			if slices.Contains(flinkConfigChecks, engineChecks[i].Code) {
				configChecks = append(configChecks, engineChecks[i])
				engineChecks = slices.Delete(engineChecks, i, i+1)
				continue
			}
			i++
		}
	}
	sections = append(sections, PreflightSection{Key: "config", Title: "Configuration", Report: checks.Summarize(configChecks, started)})
	if flink {
		sections = append(sections, PreflightSection{Key: "engine", Title: "Flink CDC", Report: checks.Summarize(engineChecks, started)})
	}
	policy, err := PolicyChecks(ctx, q, p, spec)
	if err != nil {
		return nil, err
	}
	sections = append(sections, PreflightSection{Key: "policy", Title: "Project policies", Report: checks.Summarize(policy, started)})
	var all []checks.Check
	for _, s := range sections {
		all = append(all, s.Report.Checks...)
	}
	overall := checks.Summarize(all, started)
	old, err := SpecOf(ctx, q, p.DeployedVersionID)
	if err != nil {
		return nil, err
	}
	return &PreflightReport{OK: overall.OK, Summary: overall.Summary, Sections: sections, Properties: properties,
		Destinations: destinations, Change: domain.SummarizeChange(old, spec), SettingsChange: redactSettings(domain.SummarizeSettings(old, spec)),
		Recommendations: recommended}, nil
}

// ReportExtra is report.model_dump(mode="json") for problem bodies.
func ReportExtra(r *PreflightReport) map[string]any {
	m, _ := ToMap(r)
	return m
}

// CheckDeployable runs preflight for a deploy and applies the skip rules.
func CheckDeployable(ctx context.Context, q db.Querier, p *models.Pipeline, versionNumber *int, forceReason *string) (*models.PipelineVersion, *PreflightReport, bool, error) {
	var v *models.PipelineVersion
	var err error
	if versionNumber != nil {
		v, err = db.Find[models.PipelineVersion](ctx, q, `SELECT * FROM pipeline_versions WHERE pipeline_id = $1 AND number = $2`, p.ID, *versionNumber)
	} else {
		v, err = VersionOf(ctx, q, p.CurrentVersionID)
	}
	if err != nil {
		return nil, nil, false, err
	}
	if v == nil {
		return nil, nil, false, problem.NotFound("Version")
	}
	if err := writePins(ctx, q, p, v); err != nil {
		return nil, nil, false, err
	}
	report, err := Preflight(ctx, q, p, domain.MustSpec(map[string]any(v.Spec)))
	if err != nil {
		return nil, nil, false, err
	}
	forced := false
	if !report.OK {
		if forceReason == nil || *forceReason == "" {
			return nil, nil, false, problem.New(409, "preflight_failed", fmt.Sprintf("Deploy blocked: %s.", report.Summary)).
				WithExtra(map[string]any{"preflight": ReportExtra(report)})
		}
		pol, err := PoliciesOf(ctx, q, p.ProjectID)
		if err != nil {
			return nil, nil, false, err
		}
		if !pol.AllowForceDeploy {
			return nil, nil, false, problem.New(403, "force_not_allowed", "This project does not allow deploying with failed checks. Fix them first.").
				WithExtra(map[string]any{"preflight": ReportExtra(report)})
		}
		for _, s := range report.Sections {
			if s.Key != "config" && s.Key != "runtime" && s.Key != "policy" {
				continue
			}
			for _, c := range s.Report.Checks {
				if c.Status == checks.Fail {
					return nil, nil, false, problem.New(409, "preflight_failed", "Configuration, runtime or policy problems cannot be skipped.").
						WithExtra(map[string]any{"preflight": ReportExtra(report)})
				}
			}
		}
		forced = true
	}
	return v, report, forced, nil
}

// Deploy points the pipeline at a version and asks the controller to run it.
func Deploy(ctx context.Context, q db.Querier, a *auth.ProjectAccess, p *models.Pipeline, versionNumber *int, forceReason *string) (*models.PipelineVersion, *PreflightReport, error) {
	v, report, forced, err := CheckDeployable(ctx, q, p, versionNumber, forceReason)
	if err != nil {
		return nil, nil, err
	}
	if err := SealVersion(ctx, q, v); err != nil {
		return nil, nil, err
	}
	previous := p.DeployedVersionID
	var rt *models.Runtime
	if p.RuntimeID != nil {
		if rt, err = GetRuntime(ctx, q, *p.RuntimeID); err != nil {
			return nil, nil, err
		}
	}
	sinkType, err := SinkTypeOf(ctx, q, p)
	if err != nil {
		return nil, nil, err
	}
	image, err := ResolvePipelineImage(ctx, q, p, domain.MustSpec(map[string]any(v.Spec)), rt, sinkType)
	if err != nil {
		return nil, nil, err
	}
	p.DeployedImage = &image
	artifacts := db.Map{}
	for k, val := range p.Artifacts {
		if k != "snapshot_override" {
			artifacts[k] = val
		}
	}
	// ADR 0013: the version whose columns the saved positions' job writes, recorded once; every
	// planned start records the deployed one after.
	if _, ok := artifacts[ArtifactWritten]; !ok && previous != nil && EngineOf(p) == engine.FlinkCDC {
		artifacts[ArtifactWritten] = previous.String()
	}
	p.Artifacts = artifacts
	applyPlacement(p, domain.MustSpec(map[string]any(v.Spec)), rt)
	if EngineOf(p) == engine.FlinkCDC {
		capacity := RuntimeCapacityFor(p, domain.MustSpec(map[string]any(v.Spec)), rt)
		// Tables added to a running pipeline are copied while it streams: it grows back for that.
		applyFlinkShape(p, domain.MustSpec(map[string]any(v.Spec)), capacity, previous != nil && len(report.Change.TablesAdded) == 0)
	}
	p.DeployedVersionID = &v.ID
	p.DesiredState = "running"
	now := db.Now()
	p.DeployedAt = &now
	p.Lifecycle = "deploying"
	status := db.Map{}
	for k, val := range p.Status {
		status[k] = val
	}
	status["lifecycle"], status["health"], status["summary"] = "deploying", "unknown", "Deploy requested…"
	status["as_of"], status["forced"] = pytime.ISOFormat(time.Now()), forced
	p.Status = status
	if err := SavePipeline(ctx, q, p); err != nil {
		return nil, nil, err
	}
	var failed []any
	for _, s := range report.Sections {
		for _, c := range s.Report.Checks {
			if c.Status == checks.Fail {
				failed = append(failed, c.Code)
			}
		}
	}
	kind, summary := "pipeline.deployed", fmt.Sprintf("Deployed %s v%d", p.Name, v.Number)
	skipped := []any{}
	if forced {
		kind = "pipeline.deployed_forced"
		summary = fmt.Sprintf("Deployed %s v%d despite failed checks: %s", p.Name, v.Number, *forceReason)
		skipped = failed
	}
	var prev any
	if previous != nil {
		prev = previous.String()
	}
	var reason any
	if forceReason != nil {
		reason = *forceReason
	}
	change, _ := ToMap(report.Change)
	if _, err := Record(ctx, q, AuditEvent{Kind: kind, Summary: summary, ProjectID: &a.Project.ID, Principal: a.Principal,
		ResourceType: "pipeline", ResourceID: &p.ID, Details: map[string]any{"version": v.Number, "previous_version_id": prev,
			"forced": forced, "skipped_checks": skipped, "reason": reason, "change": change}}); err != nil {
		return nil, nil, err
	}
	capture, _, err := SourceCapture(ctx, q, p)
	if err != nil {
		return nil, nil, err
	}
	// Tables added to a running pipeline are copied while it streams, where the source can. Only
	// Debezium needs a snapshot signal for that; Flink CDC copies added tables itself where its
	// reader can (AddTables). Where it can't (Oracle), preflight let this deploy through only with
	// nothing left to restore; the positions saved before it must never be restored (ADR 0006).
	if previous != nil && *previous != v.ID && len(report.Change.TablesAdded) > 0 {
		src, err := GetConnection(ctx, q, p.SourceID)
		if err != nil {
			return nil, nil, err
		}
		if err := recordTablesFloor(ctx, q, p, src.Type, report.Change.TablesAdded); err != nil {
			return nil, nil, err
		}
	}
	if previous != nil && *previous != v.ID && len(report.Change.TablesAdded) > 0 && capture.Incremental &&
		engine.CapsFor(EngineOf(p), "").RecopyTable {
		tables := make([]any, len(report.Change.TablesAdded))
		for i, t := range report.Change.TablesAdded {
			tables[i] = t
		}
		if _, err := Enqueue(ctx, q, "snapshot", p, map[string]any{"tables": tables, "reason": "tables added"}, 20*time.Second); err != nil {
			return nil, nil, err
		}
	}
	return v, report, nil
}

// SetDesired pauses or resumes a deployed pipeline.
func SetDesired(ctx context.Context, q db.Querier, a *auth.ProjectAccess, p *models.Pipeline, state string) error {
	if p.DeployedVersionID == nil {
		return problem.Conflict("not_deployed", "Deploy the pipeline first.", nil)
	}
	if p.DesiredState == state {
		return nil
	}
	p.DesiredState = state
	if err := SavePipeline(ctx, q, p); err != nil {
		return err
	}
	kind, verb := "pipeline.paused", "Paused"
	if state == "running" {
		kind, verb = "pipeline.resumed", "Resumed"
	}
	_, err := Record(ctx, q, AuditEvent{Kind: kind, Summary: verb + " " + p.Name, ProjectID: &a.Project.ID, Principal: a.Principal,
		ResourceType: "pipeline", ResourceID: &p.ID})
	return err
}

// Restart queues a restart of a running pipeline.
func Restart(ctx context.Context, q db.Querier, a *auth.ProjectAccess, p *models.Pipeline) (*models.Job, error) {
	if p.DeployedVersionID == nil || p.DesiredState != "running" {
		return nil, problem.Conflict("not_running", "Only a running pipeline can be restarted.", nil)
	}
	if _, err := Record(ctx, q, AuditEvent{Kind: "pipeline.restarted", Summary: "Restarted " + p.Name, ProjectID: &a.Project.ID,
		Principal: a.Principal, ResourceType: "pipeline", ResourceID: &p.ID}); err != nil {
		return nil, err
	}
	return Enqueue(ctx, q, "restart", p, map[string]any{}, 0)
}

// DeletePipeline soft-deletes a pipeline and queues its cleanup.
func DeletePipeline(ctx context.Context, q db.Querier, a *auth.ProjectAccess, p *models.Pipeline, dropDestinations bool) (*models.Job, error) {
	now := db.Now()
	p.DeletedAt = &now
	p.DesiredState = "paused"
	p.Lifecycle = "deleting"
	status := db.Map{}
	for k, v := range p.Status {
		status[k] = v
	}
	status["lifecycle"], status["summary"] = "deleting", "Removing connector and source artifacts…"
	p.Status = status
	if err := SavePipeline(ctx, q, p); err != nil {
		return nil, err
	}
	if _, err := Record(ctx, q, AuditEvent{Kind: "pipeline.deleted", Summary: "Deleted pipeline " + p.Name, ProjectID: &a.Project.ID,
		Principal: a.Principal, ResourceType: "pipeline", ResourceID: &p.ID, Details: map[string]any{"drop_destinations": dropDestinations}}); err != nil {
		return nil, err
	}
	return Enqueue(ctx, q, "cleanup", p, map[string]any{"drop_destinations": dropDestinations}, 0)
}

// Enqueue adds a controller job for a pipeline.
func Enqueue(ctx context.Context, q db.Querier, kind string, p *models.Pipeline, payload map[string]any, delay time.Duration) (*models.Job, error) {
	now := db.Now()
	pid := p.ProjectID
	rid := p.ID
	j := &models.Job{ID: db.NewID(), Kind: kind, ProjectID: &pid, ResourceID: &rid, Payload: payload, State: "queued",
		MaxAttempts: 5, RunAfter: now.Add(delay), CreatedAt: now}
	return j, db.Insert(ctx, q, "jobs", j)
}

// ResetOffsets resets the stored position of a paused pipeline. fromMS is the start of mode
// "from_time" (Flink CDC on MySQL or SQL Server).
func ResetOffsets(ctx context.Context, q db.Querier, a *auth.ProjectAccess, p *models.Pipeline, mode string, fromMS int64) (map[string]any, error) {
	if EngineOf(p) == engine.FlinkCDC {
		if err := requireStoppedFlink(ctx, q, p); err != nil {
			return nil, err
		}
		return resetFlink(ctx, q, a, p, mode, fromMS)
	}
	if mode == "from_time" {
		caps := engine.CapsFor(engine.Debezium, "")
		return nil, problem.New(422, "engine_unsupported", caps.Why["start_from_time"])
	}
	if p.DeployedVersionID == nil {
		return nil, problem.Conflict("not_deployed", "The pipeline has never run; there is no position to reset.", nil)
	}
	if p.DesiredState != "paused" {
		return nil, problem.Conflict("not_paused", "Pause the pipeline first.", nil)
	}
	_, rt, err := RequireComplete(ctx, q, p)
	if err != nil {
		return nil, err
	}
	d, err := DriverFor(ctx, q, rt)
	if err != nil {
		return nil, err
	}
	obs, err := d.Observe(ctx, p.ShortID())
	if err != nil {
		return nil, err
	}
	if obs.Phase == runtime.PhaseRunning || obs.Phase == runtime.PhaseRestarting || obs.Phase == runtime.PhasePending {
		return nil, problem.Conflict("still_running", "The connector is still stopping. Try again in a few seconds.", nil)
	}
	if mode == "resnapshot" {
		capture, title, err := SourceCapture(ctx, q, p)
		if err != nil {
			return nil, err
		}
		if len(capture.SnapshotModes) == 0 {
			return nil, problem.New(422, "snapshot_unsupported", title+" cannot copy existing rows: reset to stream from now instead.")
		}
	}
	spec, err := SpecOf(ctx, q, p.DeployedVersionID)
	if err != nil {
		return nil, err
	}
	results := []any{}
	if err := ClearOffsets(ctx, p.ShortID()); err != nil {
		return nil, err
	}
	results = append(results, map[string]any{"kind": "offsets", "name": "offsets_" + p.ShortID(), "outcome": "removed"})
	artifacts := db.Map{}
	for k, v := range p.Artifacts {
		artifacts[k] = v
	}
	if mode == "skip_to_now" {
		cctx := connectors.CompileContext{PipelineID: p.ID.String(), ShortID: p.ShortID(), Name: p.Name, Spec: *spec}
		source, err := GetConnection(ctx, q, p.SourceID)
		if err != nil {
			return nil, err
		}
		src, err := connectors.GetSource(source.Type)
		if err != nil {
			return nil, err
		}
		resolved, err := Resolve(ctx, q, source)
		if err != nil {
			return nil, err
		}
		forgot, err := src.ForgetPosition(ctx, resolved, cctx)
		if err != nil {
			return nil, err
		}
		for _, r := range forgot {
			m, _ := ToMap(r)
			results = append(results, m)
		}
		artifacts["snapshot_override"] = "no_data"
	} else {
		delete(artifacts, "snapshot_override")
	}
	p.Artifacts = artifacts
	// A reset legitimately moves the position; forget the last seen one.
	status := db.Map{}
	for k, v := range p.Status {
		if k != "last_lsn" && k != "position_regressed" {
			status[k] = v
		}
	}
	p.Status = status
	if err := SavePipeline(ctx, q, p); err != nil {
		return nil, err
	}
	label := "skip to now"
	if mode == "resnapshot" {
		label = "re-snapshot"
	}
	if _, err := Record(ctx, q, AuditEvent{Kind: "pipeline.offsets_reset", Summary: fmt.Sprintf("Reset the position of %s (%s)", p.Name, label),
		ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "pipeline", ResourceID: &p.ID,
		Details: map[string]any{"mode": mode, "results": results}}); err != nil {
		return nil, err
	}
	return map[string]any{"mode": mode, "results": results}, nil
}

// DiffVersions diffs two versions of a pipeline.
func DiffVersions(ctx context.Context, q db.Querier, p *models.Pipeline, a, b int) (map[string]any, error) {
	rows, err := db.Select[models.PipelineVersion](ctx, q, `SELECT * FROM pipeline_versions WHERE pipeline_id = $1 AND number = ANY($2)`, p.ID, []int{a, b})
	if err != nil {
		return nil, err
	}
	byNumber := map[int]models.PipelineVersion{}
	for _, r := range rows {
		byNumber[r.Number] = r
	}
	ra, okA := byNumber[a]
	rb, okB := byNumber[b]
	if !okA || !okB {
		return nil, problem.NotFound("Version")
	}
	old, new := domain.MustSpec(map[string]any(ra.Spec)), domain.MustSpec(map[string]any(rb.Spec))
	change, _ := ToMap(domain.SummarizeChange(&old, new))
	lines := domain.SpecDiff(&old, new, fmt.Sprintf("v%d", a), fmt.Sprintf("v%d", b))
	out := map[string]any{"from": a, "to": b, "lines": lines, "change": change}
	if sc := redactSettings(domain.SummarizeSettings(&old, new)); sc != nil {
		out["settings_change"] = sc
	}
	return out, nil
}

// PolicyChecks is the policy section of preflight.
func PolicyChecks(ctx context.Context, q db.Querier, p *models.Pipeline, spec domain.PipelineSpec) ([]checks.Check, error) {
	rules, err := PoliciesOf(ctx, q, p.ProjectID)
	if err != nil {
		return nil, err
	}
	sourceConn, err := GetConnection(ctx, q, p.SourceID)
	if err != nil {
		return nil, err
	}
	others, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM pipelines WHERE source_id = $1 AND id <> $2 AND deleted_at IS NULL AND deployed_version_id IS NOT NULL`,
		p.SourceID, p.ID)
	if err != nil {
		return nil, err
	}
	sinkType := ""
	if p.SinkID != nil {
		if sink, err := GetConnection(ctx, q, *p.SinkID); err == nil && sink != nil {
			sinkType = sink.Type
		}
	}
	out := domain.EvaluatePolicies(rules, spec, p.RuntimeID, sourceConn.Type, sinkType, int(others))
	if rules.Unreadable {
		return out, nil // the one check that says why; the strict values would only add noise
	}
	if c := settingLimitCheck(rules, EngineOf(p), spec); c != nil {
		kept := out[:0]
		for _, x := range out {
			if x.Code != "policy_ok" {
				kept = append(kept, x)
			}
		}
		out = append(kept, *c)
	}
	var capacity map[string]any
	if p.RuntimeID != nil {
		if rt, err := GetRuntime(ctx, q, *p.RuntimeID); err == nil && rt != nil {
			capacity = RuntimeCapacityFor(p, spec, rt)
		}
	}
	if extra := resourcePolicyChecks(p, rules, spec, FootprintOf(p, spec, capacity)); len(extra) > 0 {
		// policy_ok means nothing failed: it goes when a limit does.
		kept := out[:0]
		for _, c := range out {
			if c.Code != "policy_ok" {
				kept = append(kept, c)
			}
		}
		out = append(kept, extra...)
	}
	// A source that copies no existing rows (Spanner) has no snapshot mode a policy could restrict.
	if d, err := connectors.Describe("source", sourceConn.Type); err == nil && len(d.Capture.SnapshotModes) == 0 {
		kept := out[:0]
		for _, c := range out {
			if c.Code != "policy_snapshot_mode" {
				kept = append(kept, c)
			}
		}
		out = kept
	}
	return out, nil
}

var _ = sort.Strings

// writePins writes the project's pinned settings into the version about to deploy (P7, I2): an
// unsealed one takes them in place; a sealed one (deployed before, sent for approval) can't change,
// so its deploy is refused until a new version is saved.
func writePins(ctx context.Context, q db.Querier, p *models.Pipeline, v *models.PipelineVersion) error {
	rules, err := PoliciesOf(ctx, q, p.ProjectID)
	if err != nil {
		return err
	}
	src, err := GetConnection(ctx, q, p.SourceID)
	if err != nil || src == nil {
		return err
	}
	sinkType, err := SinkTypeOf(ctx, q, p)
	if err != nil {
		return err
	}
	spec := domain.MustSpec(map[string]any(v.Spec))
	pins := Pins(rules, EngineOf(p), src.Type, sinkType, spec)
	if len(pins) == 0 {
		return nil
	}
	keys := make([]string, 0, len(pins))
	for k := range pins {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if v.SealedAt != nil {
		return problem.Conflict("pinned_setting", fmt.Sprintf("This project now sets %s; v%d was saved before. Save a new version (any edit) to deploy with the pinned values.",
			strings.Join(keys, ", "), v.Number), nil)
	}
	pinned := withPins(spec, pins)
	m, err := ToMap(pinned)
	if err != nil {
		return err
	}
	v.Spec, v.SpecHash = db.Map(m), domain.SpecHash(pinned)
	_, err = q.Exec(ctx, `UPDATE pipeline_versions SET spec = $1, spec_hash = $2 WHERE id = $3`, v.Spec, v.SpecHash, v.ID)
	return err
}

// flinkConfigChecks are Flink CDC checks a forced deploy can't skip: the job could only fail.
var flinkConfigChecks = []string{"flink_oracle_version"}

// CanaryRuns reports whether the delivery canary can run for a pipeline: a source BeziSync writes a
// canary row to, a destination it reads it back from, and Debezium (Flink CDC captures only the
// spec's tables, so the canary row never reaches the destination).
func CanaryRuns(id engine.ID, srcDesc, sinkDesc *connectors.Descriptor) bool {
	return id != engine.FlinkCDC && srcDesc != nil && srcDesc.SupportsCanary && sinkDesc != nil && sinkDesc.Canary
}
