package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/runtime"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// PipelineRead is a pipeline with its versions, state and pending approval.
type PipelineRead struct {
	ID                   uuid.UUID            `json:"id"`
	Name                 string               `json:"name"`
	Description          string               `json:"description"`
	Owner                string               `json:"owner"`
	Labels               map[string]string    `json:"labels"`
	Source               ConnectionRef        `json:"source"`
	Sink                 *ConnectionRef       `json:"sink"`
	Runtime              *ConnectionRef       `json:"runtime"`
	DesiredState         string               `json:"desired_state"`
	Lifecycle            string               `json:"lifecycle"`
	Health               string               `json:"health"`
	Status               db.Map               `json:"status"`
	CurrentVersion       *int                 `json:"current_version"`
	DeployedVersion      *int                 `json:"deployed_version"`
	HasUndeployedChanges bool                 `json:"has_undeployed_changes"`
	Spec                 *domain.PipelineSpec `json:"spec"`
	Artifacts            db.Map               `json:"artifacts"`
	DeployedAt           *pytime.Time         `json:"deployed_at"`
	CreatedAt            pytime.Time          `json:"created_at"`
	UpdatedAt            pytime.Time          `json:"updated_at"`
	Version              int                  `json:"version"`
	ApprovalRequired     bool                 `json:"approval_required"`
	PendingChange        *ChangeRequestRead   `json:"pending_change"`
	// Engine runs the pipeline; Caps is what it can do (actions it can't do carry a reason).
	Engine string      `json:"engine"`
	Caps   engine.Caps `json:"caps"`
}

func pipelineRead(ctx context.Context, q db.Querier, p *models.Pipeline, withSpec bool) (*PipelineRead, error) {
	current, err := services.VersionOf(ctx, q, p.CurrentVersionID)
	if err != nil {
		return nil, err
	}
	deployed, err := services.VersionOf(ctx, q, p.DeployedVersionID)
	if err != nil {
		return nil, err
	}
	source, err := connectionRef(ctx, q, &p.SourceID)
	if err != nil {
		return nil, err
	}
	sink, err := connectionRef(ctx, q, p.SinkID)
	if err != nil {
		return nil, err
	}
	rt, err := runtimeRef(ctx, q, p.RuntimeID)
	if err != nil {
		return nil, err
	}
	required, err := services.ApprovalRequired(ctx, q, p.ProjectID)
	if err != nil {
		return nil, err
	}
	eng := services.EngineOf(p)
	sourceType := ""
	if source != nil {
		sourceType = source.Type
	}
	out := &PipelineRead{ID: p.ID, Name: p.Name, Description: p.Description, Owner: p.Owner, Labels: p.LabelMap(),
		Engine: string(eng), Caps: engine.CapsFor(eng, sourceType),
		DesiredState: p.DesiredState, Lifecycle: p.Lifecycle, Health: p.Health, Status: p.Status, Artifacts: p.Artifacts,
		DeployedAt: pytime.P(p.DeployedAt), CreatedAt: pytime.T(p.CreatedAt), UpdatedAt: pytime.T(p.UpdatedAt), Version: p.Version,
		ApprovalRequired: required}
	if out.Labels == nil {
		out.Labels = map[string]string{}
	}
	if out.Status == nil {
		out.Status = db.Map{}
	}
	if out.Artifacts == nil {
		out.Artifacts = db.Map{}
	}
	if source != nil {
		out.Source = *source
	}
	out.Sink, out.Runtime = sink, rt
	if current != nil {
		out.CurrentVersion = &current.Number
		out.HasUndeployedChanges = deployed == nil || current.ID != deployed.ID
		if withSpec {
			spec := domain.MustSpec(map[string]any(current.Spec))
			out.Spec = &spec
		}
	}
	if deployed != nil {
		out.DeployedVersion = &deployed.Number
	}
	pending, err := services.PendingFor(ctx, q, p.ID)
	if err != nil {
		return nil, err
	}
	if pending != nil {
		if out.PendingChange, err = changeRead(ctx, q, pending); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// optEngine reads an optional engine id ("debezium", "flink-cdc").
func optEngine(v *val.V, o *val.Object) *string {
	f := o.Get("engine")
	if !f.Present || f.IsNull() {
		return nil
	}
	ids := make([]string, len(engine.All))
	for i, id := range engine.All {
		ids[i] = string(id)
	}
	s, ok := v.Literal(f.Loc, f.Raw, ids...)
	if !ok {
		return nil
	}
	return &s
}

// labelsOf reads labels: dict[str, str] (validate adds the create-time rule).
func labelsOf(v *val.V, o *val.Object, key string, validate bool) (map[string]string, bool) {
	f := o.Get(key)
	if !f.Present || f.IsNull() {
		return nil, false
	}
	m, ok := v.StrDict(f.Loc, f.Raw)
	if !ok {
		return nil, false
	}
	if validate {
		bad := len(m) > 20
		for k, value := range m {
			if !SlugRe.MatchString(k) || len([]rune(value)) > 63 {
				bad = true
			}
		}
		if bad {
			v.Fail(f.Loc, "value_error", "Value error, up to 20 labels; keys are slugs, values at most 63 characters")
			return nil, false
		}
	}
	if m == nil {
		m = map[string]string{}
	}
	return m, true
}

// pipelineAction is the shape of most pipeline endpoints: resolve the project, require a
// permission, load the pipeline, run fn in a transaction and return the pipeline.
func pipelineAction(perm auth.Perm, status int, fn func(ctx context.Context, q db.Querier, a *auth.ProjectAccess, p *models.Pipeline, r *http.Request) (int, error)) http.HandlerFunc {
	return httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "pipeline_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		if err := a.Require(perm); err != nil {
			return err
		}
		ctx := r.Context()
		var out *PipelineRead
		code := status
		err = tx(ctx, func(q db.Querier) error {
			p, err := services.GetPipeline(ctx, q, a, id, false)
			if err != nil {
				return err
			}
			if c, err := fn(ctx, q, a, p, r); err != nil {
				return err
			} else if c != 0 {
				code = c
			}
			out, err = pipelineRead(ctx, q, p, true)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, code, out)
		return nil
	})
}

// viewPipeline resolves {pipeline_id} for read-only endpoints.
func viewPipeline(r *http.Request, perm auth.Perm, includeDeleted bool) (*auth.ProjectAccess, *models.Pipeline, error) {
	id, err := pathID(r, "pipeline_id")
	if err != nil {
		return nil, nil, err
	}
	a, err := project(r)
	if err != nil {
		return nil, nil, err
	}
	if err := a.Require(perm); err != nil {
		return nil, nil, err
	}
	ctx := r.Context()
	p, err := services.GetPipeline(ctx, pool(ctx), a, id, includeDeleted)
	return a, p, err
}

func versionByNumber(ctx context.Context, q db.Querier, pipelineID uuid.UUID, number int) (*models.PipelineVersion, error) {
	return db.Find[models.PipelineVersion](ctx, q, `SELECT * FROM pipeline_versions WHERE pipeline_id = $1 AND number = $2`, pipelineID, number)
}

func optInt(v *val.V, o *val.Object, key string) *int {
	f := o.Get(key)
	if !f.Present || f.IsNull() {
		return nil
	}
	n, ok := v.Int(f.Loc, f.Raw)
	if !ok {
		return nil
	}
	return &n
}

// SchemaColumn is one column of a captured table.
type SchemaColumn struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// SchemaChangeRead is one detected schema change.
type SchemaChangeRead struct {
	ID          uuid.UUID    `json:"id"`
	Table       string       `json:"table"`
	Kind        string       `json:"kind"`
	Column      *string      `json:"column"`
	Before      *string      `json:"before"`
	After       *string      `json:"after"`
	Breaking    bool         `json:"breaking"`
	Description string       `json:"description"`
	DetectedAt  pytime.Time  `json:"detected_at"`
	ReviewedAt  *pytime.Time `json:"reviewed_at"`
	// PersonalData: an added column that looks like personal data and has no masking choice
	// in the pipeline's latest version, so it is sent as it is.
	PersonalData bool `json:"personal_data"`
}

// SchemaRead is the captured tables' columns and their history.
type SchemaRead struct {
	Tracked         bool               `json:"tracked"`
	CheckedAt       *pytime.Time       `json:"checked_at"`
	Error           *string            `json:"error"`
	Tables          *om.Map            `json:"tables"`
	Changes         []SchemaChangeRead `json:"changes"`
	PendingBreaking int                `json:"pending_breaking"`
}

func schemaRead(ctx context.Context, q db.Querier, p *models.Pipeline) (*SchemaRead, error) {
	state, _ := p.Artifacts["schema"].(map[string]any)
	rows, err := db.Select[models.SchemaChange](ctx, q, `SELECT * FROM schema_changes WHERE pipeline_id = $1 ORDER BY detected_at DESC LIMIT 200`, p.ID)
	if err != nil {
		return nil, err
	}
	source, err := services.GetConnection(ctx, q, p.SourceID)
	if err != nil {
		return nil, err
	}
	out := &SchemaRead{Tables: om.New(), Changes: make([]SchemaChangeRead, len(rows))}
	if source != nil {
		if d, err := connectors.Describe("source", source.Type); err == nil {
			out.Tracked = d.TracksSchema
		}
	}
	if s, ok := state["checked_at"].(string); ok && s != "" {
		t, err := pytime.Parse(s)
		if err != nil {
			return nil, err
		}
		out.CheckedAt = pytime.P(&t)
	}
	if e, ok := state["error"].(string); ok {
		out.Error = &e
	}
	tables, _ := state["tables"].(map[string]any)
	names := make([]string, 0, len(tables))
	for t := range tables {
		names = append(names, t)
	}
	sort.Strings(names)
	for _, t := range names {
		cols, _ := tables[t].(map[string]any)
		keys := make([]string, 0, len(cols))
		for c := range cols {
			keys = append(keys, c)
		}
		list := []SchemaColumn{}
		// JSONB keeps object keys in its own order (shorter first); Python lists them so.
		for _, c := range om.JSONBKeys(keys) {
			info, _ := cols[c].(map[string]any)
			list = append(list, SchemaColumn{Name: c, Type: pyStr(info["type"]), Nullable: truthy(info["nullable"])})
		}
		out.Tables.Set(t, list)
	}
	latest, err := db.Find[models.PipelineVersion](ctx, q, `SELECT * FROM pipeline_versions WHERE pipeline_id = $1 ORDER BY number DESC LIMIT 1`, p.ID)
	if err != nil {
		return nil, err
	}
	var columns map[string]domain.ColumnRules
	if latest != nil {
		columns = domain.MustSpec(map[string]any(latest.Spec)).Source.Columns
	}
	decided := func(table, column string) bool {
		rules := columns[table]
		return slices.Contains(rules.Mask, column) || slices.Contains(rules.Exclude, column) || slices.Contains(rules.Keep, column)
	}
	for i, r := range rows {
		personal := r.Kind == "column_added" && r.Column != nil && connectors.LooksLikePII(*r.Column) && !decided(r.Table, *r.Column)
		out.Changes[i] = SchemaChangeRead{PersonalData: personal, ID: r.ID, Table: r.Table, Kind: r.Kind, Column: r.Column, Before: r.Before, After: r.After,
			Breaking: r.Breaking, Description: domain.Change{Table: r.Table, Kind: r.Kind, Column: r.Column, Before: r.Before, After: r.After,
				Breaking: r.Breaking}.Describe(), DetectedAt: pytime.T(r.DetectedAt), ReviewedAt: pytime.P(r.ReviewedAt)}
		if r.Breaking && r.ReviewedAt == nil {
			out.PendingBreaking++
		}
	}
	return out, nil
}

func pipelineRoutes(r chi.Router) {
	base := "/projects/{project_id}/pipelines"
	r.Get(base, httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		qs := httpapi.Q(r)
		search := qs.StrRules("q", val.StrRules{Max: 200})
		includeDeleted := qs.Bool("include_deleted", false)
		if err := qs.Err(); err != nil {
			return err
		}
		if err := a.Require(auth.PipelineView); err != nil {
			return err
		}
		sql, args := `SELECT * FROM pipelines WHERE project_id = $1`, []any{a.Project.ID}
		if !includeDeleted {
			sql += ` AND deleted_at IS NULL`
		}
		if search != nil && *search != "" {
			args = append(args, "%"+strings.ReplaceAll(*search, "%", "")+"%")
			sql += fmt.Sprintf(` AND name ILIKE $%d`, len(args))
		}
		ctx := r.Context()
		q := pool(ctx)
		rows, err := db.Select[models.Pipeline](ctx, q, sql+` ORDER BY name LIMIT 500`, args...)
		if err != nil {
			return err
		}
		out := make([]*PipelineRead, len(rows))
		for i := range rows {
			if out[i], err = pipelineRead(ctx, q, &rows[i], false); err != nil {
				return err
			}
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Post(base, httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		type create struct {
			services.NewPipeline
			TemplateID *uuid.UUID
		}
		b, err := body(r, func(v *val.V, o *val.Object) create {
			c := create{}
			if s := slug(v, o, "name", true); s != nil {
				c.Name = *s
			}
			c.Description = str(v, o, "description", "", val.StrRules{Max: 4000}, false)
			c.Owner = str(v, o, "owner", "", val.StrRules{Max: 200}, false)
			c.Labels, _ = labelsOf(v, o, "labels", true)
			if c.Labels == nil {
				c.Labels = map[string]string{}
			}
			c.SourceID = reqUUID(v, o, "source_id")
			c.SinkID, c.RuntimeID = optUUID(v, o, "sink_id"), optUUID(v, o, "runtime_id")
			c.Spec = domain.DefaultSpec()
			if f := o.Get("spec"); f.Present {
				c.Spec, _ = domain.ParseSpec(v, f.Loc, f.Raw)
			}
			c.TemplateID = optUUID(v, o, "template_id")
			if e := optEngine(v, o); e != nil {
				c.Engine = *e
			}
			return c
		})
		if err != nil {
			return err
		}
		if err := a.Require(auth.PipelineEdit); err != nil {
			return err
		}
		ctx := r.Context()
		var out *PipelineRead
		err = tx(ctx, func(q db.Querier) error {
			n := b.NewPipeline
			if b.TemplateID != nil {
				t, err := getTemplate(ctx, q, a, *b.TemplateID)
				if err != nil {
					return err
				}
				spec := domain.MustSpec(map[string]any(t.Spec))
				spec.Source.Tables, spec.Source.Columns = n.Spec.Source.Tables, n.Spec.Source.Columns
				n.Spec = spec
				if n.SinkID == nil {
					n.SinkID = t.SinkID
				}
				if n.RuntimeID == nil {
					n.RuntimeID = t.RuntimeID
				}
				if _, ok := n.Labels["template"]; !ok {
					n.Labels["template"] = t.Name
				}
			}
			p, err := services.CreatePipeline(ctx, q, a, n)
			if err != nil {
				return err
			}
			out, err = pipelineRead(ctx, q, p, true)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusCreated, out)
		return nil
	}))
	r.Get(base+"/{pipeline_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		_, p, err := viewPipeline(r, auth.PipelineView, true)
		if err != nil {
			return err
		}
		ctx := r.Context()
		out, err := pipelineRead(ctx, pool(ctx), p, true)
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Patch(base+"/{pipeline_id}", func(w http.ResponseWriter, r *http.Request) {
		var e services.PipelineEdit
		b, err := body(r, func(v *val.V, o *val.Object) services.PipelineEdit {
			e := services.PipelineEdit{Name: slug(v, o, "name", false), SinkID: optUUID(v, o, "sink_id"), Spec: optSpec(v, o, "spec"),
				Description: optStr(v, o, "description", val.StrRules{Max: 4000}), Owner: optStr(v, o, "owner", val.StrRules{Max: 200}),
				RuntimeID: optUUID(v, o, "runtime_id"), Note: str(v, o, "note", "", val.StrRules{Max: 500}, false), Engine: optEngine(v, o)}
			e.Labels, e.LabelsSet = labelsOf(v, o, "labels", false)
			return e
		})
		if err != nil {
			// Authentication still comes first.
			if _, aerr := project(r); aerr != nil {
				httpapi.Fail(w, r, aerr)
				return
			}
			httpapi.Fail(w, r, err)
			return
		}
		e = b
		pipelineAction(auth.PipelineEdit, http.StatusOK, func(ctx context.Context, q db.Querier, a *auth.ProjectAccess, p *models.Pipeline, r *http.Request) (int, error) {
			expected, err := parseIfMatch(r)
			if err != nil {
				return 0, err
			}
			e.ExpectedVersion = expected
			_, err = services.UpdatePipeline(ctx, q, a, p, e)
			return 0, err
		})(w, r)
	})
	r.Get(base+"/{pipeline_id}/versions", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		_, p, err := viewPipeline(r, auth.PipelineView, true)
		if err != nil {
			return err
		}
		ctx := r.Context()
		rows, err := db.Select[models.PipelineVersion](ctx, pool(ctx), `SELECT * FROM pipeline_versions WHERE pipeline_id = $1 ORDER BY number DESC`, p.ID)
		if err != nil {
			return err
		}
		type versionRead struct {
			Number    int                 `json:"number"`
			Spec      domain.PipelineSpec `json:"spec"`
			SpecHash  string              `json:"spec_hash"`
			Note      string              `json:"note"`
			CreatedAt pytime.Time         `json:"created_at"`
			CreatedBy string              `json:"created_by"`
			Deployed  bool                `json:"deployed"`
			Draft     bool                `json:"draft"`
		}
		out := make([]versionRead, len(rows))
		for i, v := range rows {
			out[i] = versionRead{Number: v.Number, Spec: domain.MustSpec(map[string]any(v.Spec)), SpecHash: v.SpecHash, Note: v.Note,
				CreatedAt: pytime.T(v.CreatedAt), CreatedBy: v.CreatedByLabel, Deployed: p.DeployedVersionID != nil && v.ID == *p.DeployedVersionID,
				Draft: v.SealedAt == nil}
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Post(base+"/{pipeline_id}/preflight", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		type request struct {
			Version *int
			Spec    *domain.PipelineSpec
		}
		b, _, err := optBody(r, func(v *val.V, o *val.Object) request {
			return request{Version: optInt(v, o, "version"), Spec: optSpec(v, o, "spec")}
		})
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
		spec := b.Spec
		if spec == nil {
			var version *models.PipelineVersion
			if b.Version != nil && *b.Version != 0 {
				version, err = versionByNumber(ctx, q, p.ID, *b.Version)
			} else {
				version, err = services.VersionOf(ctx, q, p.CurrentVersionID)
			}
			if err != nil {
				return err
			}
			if version == nil {
				return problem.NotFound("Version")
			}
			s := domain.MustSpec(map[string]any(version.Spec))
			spec = &s
		}
		report, err := services.Preflight(ctx, q, p, *spec)
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, report)
		return nil
	}))
	r.Post(base+"/{pipeline_id}/deploy", func(w http.ResponseWriter, r *http.Request) {
		type request struct {
			Version     *int
			ForceReason *string
		}
		b, err := body(r, func(v *val.V, o *val.Object) request {
			return request{Version: optInt(v, o, "version"), ForceReason: optStr(v, o, "force_reason", val.StrRules{Min: 10, Max: 1000})}
		})
		if err != nil {
			if _, aerr := project(r); aerr != nil {
				httpapi.Fail(w, r, aerr)
				return
			}
			httpapi.Fail(w, r, err)
			return
		}
		pipelineAction(auth.PipelineDeploy, http.StatusOK, func(ctx context.Context, q db.Querier, a *auth.ProjectAccess, p *models.Pipeline, _ *http.Request) (int, error) {
			required, err := services.ApprovalRequired(ctx, q, a.Project.ID)
			if err != nil {
				return 0, err
			}
			if required {
				_, err := services.RequestApproval(ctx, q, a, p, b.Version, b.ForceReason, "")
				return http.StatusAccepted, err
			}
			_, _, err = services.Deploy(ctx, q, a, p, b.Version, b.ForceReason)
			return 0, err
		})(w, r)
	})
	desired := func(state string) http.HandlerFunc {
		return pipelineAction(auth.PipelineOperate, http.StatusOK, func(ctx context.Context, q db.Querier, a *auth.ProjectAccess, p *models.Pipeline, _ *http.Request) (int, error) {
			return 0, services.SetDesired(ctx, q, a, p, state)
		})
	}
	r.Post(base+"/{pipeline_id}/pause", desired("paused"))
	r.Post(base+"/{pipeline_id}/resume", desired("running"))
	r.Post(base+"/{pipeline_id}/restart", pipelineAction(auth.PipelineOperate, http.StatusOK,
		func(ctx context.Context, q db.Querier, a *auth.ProjectAccess, p *models.Pipeline, _ *http.Request) (int, error) {
			_, err := services.Restart(ctx, q, a, p)
			return 0, err
		}))
	r.Delete(base+"/{pipeline_id}", func(w http.ResponseWriter, r *http.Request) {
		qs := httpapi.Q(r)
		drop := qs.Bool("drop_destinations", false)
		if err := qs.Err(); err != nil {
			httpapi.Fail(w, r, err)
			return
		}
		pipelineAction(auth.PipelineDeploy, http.StatusOK, func(ctx context.Context, q db.Querier, a *auth.ProjectAccess, p *models.Pipeline, _ *http.Request) (int, error) {
			_, err := services.DeletePipeline(ctx, q, a, p, drop)
			return 0, err
		})(w, r)
	})
	r.Get(base+"/{pipeline_id}/config", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		qs := httpapi.Q(r)
		version := qs.Int("version", 0, val.Num{})
		if err := qs.Err(); err != nil {
			return err
		}
		_, p, err := viewPipeline(r, auth.PipelineView, true)
		if err != nil {
			return err
		}
		ctx := r.Context()
		q := pool(ctx)
		var row *models.PipelineVersion
		if version != 0 {
			row, err = versionByNumber(ctx, q, p.ID, version)
		} else {
			id := p.DeployedVersionID
			if id == nil {
				id = p.CurrentVersionID
			}
			row, err = services.VersionOf(ctx, q, id)
		}
		if err != nil {
			return err
		}
		if row == nil {
			return problem.NotFound("Version")
		}
		compiled, err := services.CompileFor(ctx, q, p, domain.MustSpec(map[string]any(row.Spec)), "")
		if err != nil {
			return err
		}
		env := make([]string, 0, len(compiled.Env))
		for k := range compiled.Env {
			env = append(env, k)
		}
		sort.Strings(env)
		dest := compiled.Destinations
		if dest == nil {
			dest = map[string]string{}
		}
		httpapi.JSON(w, http.StatusOK, obj("version", row.Number, "properties", compiled.Render(), "secret_env", env, "destinations", dest, "image", compiled.Image))
		return nil
	}))
	r.Get(base+"/{pipeline_id}/logs", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		qs := httpapi.Q(r)
		tail := qs.Int("tail", 300, val.Num{GE: val.F(1), LE: val.F(5000)})
		if err := qs.Err(); err != nil {
			return err
		}
		_, p, err := viewPipeline(r, auth.PipelineView, true)
		if err != nil {
			return err
		}
		ctx := r.Context()
		q := pool(ctx)
		sink, rt, err := services.RequireComplete(ctx, q, p)
		if err != nil {
			return err
		}
		text, err := func() (string, error) {
			d, err := services.DriverFor(ctx, q, rt)
			if err != nil {
				return "", err
			}
			return d.Logs(ctx, p.ShortID(), tail, false)
		}()
		if err != nil {
			var re *runtime.Error
			if errors.As(err, &re) {
				return problem.New(503, "runtime_unavailable", "Logs are unavailable: "+re.Error())
			}
			return err
		}
		source, err := services.GetConnection(ctx, q, p.SourceID)
		if err != nil {
			return err
		}
		values := map[string]any{}
		if source != nil {
			s, err := services.StoredSecrets(source)
			if err != nil {
				return err
			}
			for k, v := range s {
				values[k] = v
			}
		}
		s, err := services.StoredSecrets(sink)
		if err != nil {
			return err
		}
		for k, v := range s {
			values["s_"+k] = v
		}
		text = services.Redact(text, values)
		lines := domain.SplitLines(text)
		if len(lines) > tail {
			lines = lines[len(lines)-tail:]
		}
		if lines == nil {
			lines = []string{}
		}
		httpapi.JSON(w, http.StatusOK, obj("lines", lines, "fetched_at", pytime.T(db.Now()), "available", text != ""))
		return nil
	}))
	r.Get(base+"/{pipeline_id}/events", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		qs := httpapi.Q(r)
		table := qs.Str("table")
		limit := qs.Int("limit", 50, val.Num{GE: val.F(1), LE: val.F(500)})
		mask := qs.StrRules("mask", val.StrRules{Max: 2000})
		if err := qs.Err(); err != nil {
			return err
		}
		_, p, err := viewPipeline(r, auth.PipelinePreviewData, false)
		if err != nil {
			return err
		}
		ctx := r.Context()
		q := pool(ctx)
		id := p.DeployedVersionID
		if id == nil {
			id = p.CurrentVersionID
		}
		spec, err := services.SpecOf(ctx, q, id)
		if err != nil {
			return err
		}
		if spec == nil || len(spec.Source.Tables.Include) == 0 {
			return problem.New(409, "no_tables", "The pipeline has no tables yet.")
		}
		chosen := table
		if chosen == "" {
			chosen = spec.Source.Tables.Include[0]
		}
		found := false
		for _, t := range spec.Source.Tables.Include {
			found = found || t == chosen
		}
		if !found {
			return problem.New(422, "unknown_table", chosen+" is not captured by this pipeline.")
		}
		compiled, err := services.CompileFor(ctx, q, p, *spec, "")
		if err != nil {
			return err
		}
		destination := compiled.Destinations[chosen]
		sinkConn, _, err := services.RequireComplete(ctx, q, p)
		if err != nil {
			return err
		}
		d, err := connectors.Describe("sink", sinkConn.Type)
		if err != nil {
			return err
		}
		now := pytime.T(db.Now())
		if !d.Readable {
			httpapi.JSON(w, http.StatusOK, obj("table", chosen, "destination", destination, "readable", false, "records", []any{}, "masked_fields", []string{}, "fetched_at", now))
			return nil
		}
		sink, err := connectors.GetSink(sinkConn.Type)
		if err != nil {
			return err
		}
		records, err := func() ([]connectors.TailRecord, error) {
			resolved, err := services.Resolve(ctx, q, sinkConn)
			if err != nil {
				return nil, err
			}
			return sink.Tail(ctx, resolved, destination, limit, 8*time.Second)
		}()
		if err != nil {
			return problem.New(502, "preview_failed", "Could not read from the destination: "+err.Error())
		}
		extra := map[string]bool{}
		if mask != nil {
			for _, m := range strings.Split(*mask, ",") {
				if m = strings.TrimSpace(m); m != "" {
					extra[m] = true
				}
			}
		}
		for _, rules := range spec.Source.Columns {
			for _, m := range rules.Mask {
				extra[m] = true
			}
		}
		masked, fields := services.MaskRecords(records, extra)
		if masked == nil {
			masked = []connectors.TailRecord{}
		}
		if fields == nil {
			fields = []string{}
		}
		httpapi.JSON(w, http.StatusOK, obj("table", chosen, "destination", destination, "readable", true, "records", masked, "masked_fields", fields, "fetched_at", pytime.T(db.Now())))
		return nil
	}))
	r.Post(base+"/{pipeline_id}/snapshots", func(w http.ResponseWriter, r *http.Request) {
		type request struct {
			Tables       []string
			IgnoreWindow bool
		}
		b, err := body(r, func(v *val.V, o *val.Object) request {
			req := request{IgnoreWindow: boolean(v, o, "ignore_window", false)}
			if f := o.Get("tables"); f.Required() {
				if l, ok := v.StrList(f.Loc, f.Raw); ok && v.ListLen(f.Loc, len(l), 1, 1000) {
					req.Tables = l
				}
			}
			return req
		})
		if err != nil {
			if _, aerr := project(r); aerr != nil {
				httpapi.Fail(w, r, aerr)
				return
			}
			httpapi.Fail(w, r, err)
			return
		}
		pipelineAction(auth.PipelineOperate, http.StatusAccepted, func(ctx context.Context, q db.Querier, a *auth.ProjectAccess, p *models.Pipeline, _ *http.Request) (int, error) {
			spec, err := services.SpecOf(ctx, q, p.DeployedVersionID)
			if err != nil {
				return 0, err
			}
			if spec == nil || p.DesiredState != "running" {
				return 0, problem.New(409, "not_running", "Snapshots need a running pipeline.")
			}
			capture, title, err := services.SourceCapture(ctx, q, p)
			if err != nil {
				return 0, err
			}
			if !capture.Incremental {
				return 0, problem.New(422, "snapshot_unsupported", title+" cannot copy tables again while streaming.")
			}
			captured := map[string]bool{}
			for _, t := range spec.Source.Tables.Include {
				captured[t] = true
			}
			var unknown []string
			for _, t := range b.Tables {
				if !captured[t] {
					unknown = append(unknown, t)
				}
			}
			if len(unknown) > 0 {
				return 0, problem.New(422, "unknown_table", fmt.Sprintf("Not captured by this pipeline: %s.", strings.Join(unknown, ", ")))
			}
			tables := make([]any, len(b.Tables))
			for i, t := range b.Tables {
				tables[i] = t
			}
			if _, err := services.Enqueue(ctx, q, "snapshot", p, map[string]any{"tables": tables, "reason": "requested", "ignore_window": b.IgnoreWindow}, 0); err != nil {
				return 0, err
			}
			_, err = services.Record(ctx, q, services.AuditEvent{Kind: "pipeline.snapshot_requested", Summary: "Requested a snapshot of " + strings.Join(b.Tables, ", "),
				ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "pipeline", ResourceID: &p.ID})
			return 0, err
		})(w, r)
	})
	r.Get(base+"/{pipeline_id}/metrics", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		qs := httpapi.Q(r)
		rng := "6h"
		if s := qs.StrRules("range", val.StrRules{Pattern: `^(1h|6h|24h|7d|30d)$`}); s != nil {
			rng = *s
		}
		if err := qs.Err(); err != nil {
			return err
		}
		_, p, err := viewPipeline(r, auth.PipelineView, true)
		if err != nil {
			return err
		}
		ctx := r.Context()
		points, err := services.MetricSeries(ctx, pool(ctx), p.ID, rng)
		if err != nil {
			return err
		}
		if points == nil {
			points = []services.MetricPoint{}
		}
		httpapi.JSON(w, http.StatusOK, obj("range", rng, "points", points))
		return nil
	}))
	r.Get(base+"/{pipeline_id}/offsets", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		_, p, err := viewPipeline(r, auth.PipelineView, false)
		if err != nil {
			return err
		}
		rows, err := services.ReadOffsets(r.Context(), p.ShortID())
		if err != nil {
			return problem.New(503, "state_db_unavailable", "The state database is not reachable: "+err.Error())
		}
		type offsetRead struct {
			Key                string         `json:"key"`
			Position           string         `json:"position"`
			SourceTime         *string        `json:"source_time"`
			StoredAt           *string        `json:"stored_at"`
			SnapshotInProgress bool           `json:"snapshot_in_progress"`
			Raw                map[string]any `json:"raw"`
		}
		out := make([]offsetRead, len(rows))
		for i, row := range rows {
			o := offsetRead{Key: pyStr(row["key"]), Position: pyStr(row["position"])}
			if s, ok := row["source_time"].(string); ok {
				o.SourceTime = &s
			}
			if s, ok := row["stored_at"].(string); ok {
				o.StoredAt = &s
			}
			o.SnapshotInProgress, _ = row["snapshot_in_progress"].(bool)
			if o.Raw, _ = row["raw"].(map[string]any); o.Raw == nil {
				o.Raw = map[string]any{}
			}
			out[i] = o
		}
		var override *string
		if s, ok := p.Artifacts["snapshot_override"].(string); ok {
			override = &s
		}
		httpapi.JSON(w, http.StatusOK, obj("offsets", out, "snapshot_override", override))
		return nil
	}))
	r.Post(base+"/{pipeline_id}/offsets/reset", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "pipeline_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		type request struct {
			Mode, Confirm string
			FromMS        int64
		}
		b, err := body(r, func(v *val.V, o *val.Object) request {
			req := request{}
			if f := o.Get("mode"); f.Required() {
				req.Mode, _ = v.Literal(f.Loc, f.Raw, "resnapshot", "skip_to_now", "from_time")
			}
			req.Confirm = str(v, o, "confirm", "", val.StrRules{}, true)
			if f := o.Get("from_time"); f.Present && !f.IsNull() {
				if t, ok := v.Datetime(f.Loc, f.Raw); ok {
					req.FromMS = t.UnixMilli()
				}
			} else if req.Mode == "from_time" {
				v.Fail(f.Loc, "missing", "Field required")
			}
			return req
		})
		if err != nil {
			return err
		}
		if err := a.Require(auth.OffsetsReset); err != nil {
			return err
		}
		ctx := r.Context()
		var out map[string]any
		err = tx(ctx, func(q db.Querier) error {
			p, err := services.GetPipeline(ctx, q, a, id, false)
			if err != nil {
				return err
			}
			if b.Confirm != p.Name {
				return problem.New(422, "confirmation_mismatch", "Type the pipeline name to confirm.")
			}
			out, err = services.ResetOffsets(ctx, q, a, p, b.Mode, b.FromMS)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Get(base+"/{pipeline_id}/positions", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		_, p, err := viewPipeline(r, auth.PipelineView, false)
		if err != nil {
			return err
		}
		if services.EngineOf(p) != engine.FlinkCDC {
			httpapi.JSON(w, http.StatusOK, obj("positions", []services.SavedPosition{}))
			return nil
		}
		positions, err := services.SavedPositions(r.Context(), p)
		if err != nil {
			return problem.New(503, "state_db_unavailable", "The state database is not reachable: "+err.Error())
		}
		httpapi.JSON(w, http.StatusOK, obj("positions", positions))
		return nil
	}))
	r.Post(base+"/{pipeline_id}/positions/{position_id}/restore", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "pipeline_id")
		if err != nil {
			return err
		}
		position, err := strconv.ParseInt(chi.URLParam(r, "position_id"), 10, 64)
		if err != nil || position <= 0 {
			return problem.NotFound("Saved position")
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		b, err := body(r, func(v *val.V, o *val.Object) string { return str(v, o, "confirm", "", val.StrRules{}, true) })
		if err != nil {
			return err
		}
		if err := a.Require(auth.OffsetsReset); err != nil {
			return err
		}
		ctx := r.Context()
		var out *services.SavedPosition
		err = tx(ctx, func(q db.Querier) error {
			p, err := services.GetPipeline(ctx, q, a, id, false)
			if err != nil {
				return err
			}
			if b != p.Name {
				return problem.New(422, "confirmation_mismatch", "Type the pipeline name to confirm.")
			}
			out, err = services.RestoreSavedPosition(ctx, q, a, p, position)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, obj("position", out))
		return nil
	}))
	r.Get(base+"/{pipeline_id}/diff", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		qs := httpapi.Q(r)
		from := qs.RequiredInt("from")
		to := qs.RequiredInt("to")
		if err := qs.Err(); err != nil {
			return err
		}
		_, p, err := viewPipeline(r, auth.PipelineView, true)
		if err != nil {
			return err
		}
		ctx := r.Context()
		out, err := services.DiffVersions(ctx, pool(ctx), p, from, to)
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, om.New().Set("from", out["from"]).Set("to", out["to"]).Set("lines", out["lines"]).Set("change", out["change"]))
		return nil
	}))
	r.Get(base+"/{pipeline_id}/schema", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		_, p, err := viewPipeline(r, auth.PipelineView, false)
		if err != nil {
			return err
		}
		ctx := r.Context()
		out, err := schemaRead(ctx, pool(ctx), p)
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Post(base+"/{pipeline_id}/schema/review", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "pipeline_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		if err := a.Require(auth.PipelineOperate); err != nil {
			return err
		}
		ctx := r.Context()
		var out *SchemaRead
		err = tx(ctx, func(q db.Querier) error {
			p, err := services.GetPipeline(ctx, q, a, id, false)
			if err != nil {
				return err
			}
			now := db.Now()
			if _, err := q.Exec(ctx, `UPDATE schema_changes SET reviewed_at = $1 WHERE pipeline_id = $2 AND reviewed_at IS NULL`, now, p.ID); err != nil {
				return err
			}
			if p.Status == nil {
				p.Status = db.Map{}
			}
			p.Status["schema_pending_breaking"] = 0
			if _, err := q.Exec(ctx, `UPDATE pipelines SET status = $1 WHERE id = $2`, p.Status, p.ID); err != nil {
				return err
			}
			if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "pipeline.schema_reviewed", Summary: "Reviewed schema changes of " + p.Name,
				ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "pipeline", ResourceID: &p.ID}); err != nil {
				return err
			}
			out, err = schemaRead(ctx, q, p)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
}
