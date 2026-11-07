package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/runtime"
	"github.com/daun-gatal/bezisync/backend/internal/schemas"
	"github.com/daun-gatal/bezisync/backend/internal/schemaval"
	"github.com/daun-gatal/bezisync/backend/internal/security"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// RuntimeTest is the result of a runtime connectivity test.
type RuntimeTest struct {
	OK       bool           `json:"ok"`
	Message  string         `json:"message"`
	Details  map[string]any `json:"details"`
	TestedAt pytime.Time    `json:"tested_at"`
}

// RuntimeRead is a runtime; secrets are never returned.
type RuntimeRead struct {
	ID              uuid.UUID      `json:"id"`
	Name            string         `json:"name"`
	Kind            string         `json:"kind"`
	Mode            string         `json:"mode"`
	AgentVersion    *string        `json:"agent_version"`
	AgentToken      *string        `json:"agent_token"`
	AgentInstall    *string        `json:"agent_install"`
	Config          db.Map         `json:"config"`
	SecretFieldsSet []string       `json:"secret_fields_set"`
	DefaultImage    *string        `json:"default_image"`
	EffectiveImage  string         `json:"effective_image"`
	Enabled         bool           `json:"enabled"`
	LastTest        *RuntimeTest   `json:"last_test"`
	LastSeenAt      *pytime.Time   `json:"last_seen_at"`
	Capacity        map[string]any `json:"capacity"`
	Pipelines       int64          `json:"pipelines"`
	Version         int            `json:"version"`
	// PlacementProfiles are where this Kubernetes runtime's pipelines may run, by name.
	PlacementProfiles []runtime.Placement `json:"placement_profiles"`
}

// placementProfiles reads an optional placement_profiles list (strictly: unknown keys refused).
func placementProfiles(v *val.V, o *val.Object) ([]runtime.Placement, bool) {
	f := o.Get("placement_profiles")
	if !f.Present {
		return nil, false
	}
	raw, err := json.Marshal(f.Raw)
	if err != nil {
		v.Fail(f.Loc, "value_error", "Value error, not a list of placement profiles")
		return nil, true
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	out := []runtime.Placement{}
	if f.IsNull() {
		return out, true
	}
	if err := dec.Decode(&out); err != nil {
		v.Fail(f.Loc, "value_error", "Value error, not a list of placement profiles: "+err.Error())
	}
	return out, true
}

func runtimeSecrets(rt *models.Runtime) (map[string]any, error) {
	if rt.SecretsEnc == nil || *rt.SecretsEnc == "" {
		return map[string]any{}, nil
	}
	return security.DecryptSecret(*rt.SecretsEnc)
}

func runtimeTestOf(m db.Map) (*RuntimeTest, error) {
	if len(m) == 0 {
		return nil, nil
	}
	var t struct {
		OK       bool           `json:"ok"`
		Message  string         `json:"message"`
		Details  map[string]any `json:"details"`
		TestedAt string         `json:"tested_at"`
	}
	raw, err := pyjson.DumpsCompact(map[string]any(m))
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(raw), &t); err != nil {
		return nil, err
	}
	at, err := pytime.Parse(t.TestedAt)
	if err != nil {
		return nil, err
	}
	if t.Details == nil {
		t.Details = map[string]any{}
	}
	return &RuntimeTest{OK: t.OK, Message: t.Message, Details: t.Details, TestedAt: pytime.T(at)}, nil
}

func runtimeRead(ctx context.Context, q db.Querier, rt *models.Runtime) (RuntimeRead, error) {
	sec, err := runtimeSecrets(rt)
	if err != nil {
		return RuntimeRead{}, err
	}
	fields := make([]string, 0, len(sec))
	for k := range sec {
		fields = append(fields, k)
	}
	sort.Strings(fields)
	count, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM pipelines WHERE runtime_id = $1 AND deleted_at IS NULL`, rt.ID)
	if err != nil {
		return RuntimeRead{}, err
	}
	effective := ""
	if rt.DefaultImage != nil && *rt.DefaultImage != "" {
		effective = *rt.DefaultImage
	} else if effective, err = services.DefaultImage(ctx, q); err != nil {
		return RuntimeRead{}, err
	}
	last, err := runtimeTestOf(rt.LastTest)
	if err != nil {
		return RuntimeRead{}, err
	}
	cfg := rt.Config
	if cfg == nil {
		cfg = db.Map{}
	}
	var capacity map[string]any
	if rt.Capacity != nil {
		capacity = rt.Capacity
	}
	return RuntimeRead{ID: rt.ID, Name: rt.Name, Kind: rt.Kind, Mode: rt.Mode, AgentVersion: rt.AgentVersion, Config: cfg, SecretFieldsSet: fields,
		DefaultImage: rt.DefaultImage, EffectiveImage: effective, Enabled: rt.Enabled, LastTest: last, LastSeenAt: pytime.P(rt.LastSeenAt),
		Capacity: capacity, Pipelines: count, Version: rt.Version, PlacementProfiles: nonNil(services.ProfilesOf(rt))}, nil
}

func projectRuntime(ctx context.Context, q db.Querier, a *auth.ProjectAccess, id uuid.UUID) (*models.Runtime, error) {
	rt, err := services.GetRuntime(ctx, q, id)
	if err != nil {
		return nil, err
	}
	if rt == nil || rt.ProjectID != a.Project.ID {
		return nil, problem.NotFound("Runtime")
	}
	return rt, nil
}

// validateRuntime validates a runtime kind's config and secrets (empty secrets dropped).
func validateRuntime(kind string, cfg, sec map[string]any) (map[string]any, map[string]any, error) {
	k, ok := schemas.Runtime(kind)
	if !ok {
		return nil, nil, problem.New(422, "validation_failed", "Some fields are invalid.")
	}
	return validateKind(k, cfg, sec)
}

// validateKind validates config and secrets against a model family entry; empty secret
// values are dropped.
func validateKind(k schemas.Kind, cfg, sec map[string]any) (map[string]any, map[string]any, error) {
	fail := func(v *val.V) error {
		errs := make([]map[string]any, len(v.Errors))
		for i, e := range v.Errors {
			errs[i] = map[string]any{"loc": e.Loc, "message": e.Message}
		}
		return problem.New(422, "validation_failed", "Some fields are invalid.").WithErrors(errs)
	}
	v := &val.V{}
	config, ok := schemaval.Model(v, nil, k.ConfigSchema, cfg)
	if !ok {
		return nil, nil, fail(v)
	}
	secretValues, ok := schemaval.Model(v, nil, k.SecretSchema, sec)
	if !ok {
		return nil, nil, fail(v)
	}
	out := map[string]any{}
	for key, value := range secretValues {
		if truthy(value) {
			out[key] = value
		}
	}
	return config, out, nil
}

// truthy is Python's bool() for JSON values.
func truthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case string:
		return x != ""
	case []any:
		return len(x) > 0
	case map[string]any:
		return len(x) > 0
	}
	if f, ok := number(v); ok {
		return f != 0
	}
	return true
}

func newRuntimeTest(r runtime.TestResult) RuntimeTest {
	d := r.Details
	if d == nil {
		d = map[string]any{}
	}
	return RuntimeTest{OK: r.OK, Message: r.Message, Details: d, TestedAt: pytime.T(db.Now())}
}

func (t RuntimeTest) dump() db.Map {
	return db.Map{"ok": t.OK, "message": t.Message, "details": t.Details, "tested_at": pytime.Pydantic(t.TestedAt.Time)}
}

// testDriver builds a driver and tests it; construction errors are a failed test.
func testDriver(ctx context.Context, build func() (runtime.Driver, error)) runtime.TestResult {
	d, err := build()
	if err != nil {
		var re *runtime.Error
		if errors.As(err, &re) {
			return runtime.TestResult{Message: re.Error()}
		}
		return runtime.TestResult{Message: err.Error()}
	}
	return d.Test(ctx)
}

// installCommand is the one-time agent install command for a runtime.
func installCommand(rt *models.Runtime, token string) string {
	s := config.Get()
	url := s.AgentURL
	if url == "" {
		url = s.PublicURL
	}
	repo, tag := "", s.AgentImage
	if i := strings.LastIndex(s.AgentImage, ":"); i >= 0 {
		repo, tag = s.AgentImage[:i], s.AgentImage[i+1:]
	}
	if rt.Kind == "kubernetes" {
		ns, _ := rt.Config["namespace"].(string)
		if ns == "" {
			ns = "default"
		}
		chart := s.AgentChart
		ver := ""
		// The chart sets the agent chart version it was released with; without one Helm installs
		// the newest chart (an app version is no chart version).
		if strings.HasPrefix(chart, "oci://") && s.AgentChartVersion != "" {
			ver = " --version " + s.AgentChartVersion
		}
		var pulls strings.Builder
		names, _ := rt.Config["image_pull_secrets"].([]any)
		for i, n := range names {
			fmt.Fprintf(&pulls, " \\\n  --set 'imagePullSecrets[%d].name=%s'", i, pyStr(n))
		}
		// The chart re-pulls `latest` (pullPolicy Always); a pinned tag must also work for
		// mirrored, air-gapped or locally loaded images.
		policy := ""
		if tag != "" && tag != "latest" {
			policy = " --set image.pullPolicy=IfNotPresent"
		}
		return fmt.Sprintf("helm install bezisync-agent %s%s --namespace %s --create-namespace \\\n"+
			"  --set controlPlane.url=%s --set token=%s \\\n"+
			"  --set image.repository=%s --set image.tag=%s%s%s", chart, ver, ns, url, token, repo, tag, policy, pulls.String())
	}
	network := ""
	if n, ok := rt.Config["network"].(string); ok {
		network = n
	}
	out := "docker run -d --name bezisync-agent --restart unless-stopped \\\n" +
		"  -v /var/run/docker.sock:/var/run/docker.sock \\\n" +
		fmt.Sprintf("  -e BEZISYNC_AGENT_URL=%s -e BEZISYNC_AGENT_TOKEN=%s -e BEZISYNC_AGENT_KIND=docker \\\n", url, token)
	if network != "" {
		out += fmt.Sprintf("  -e BEZISYNC_AGENT_DOCKER_NETWORK=%s \\\n", network)
	}
	if t, ok := rt.Config["image_pull_timeout"].(string); ok && t != "" && t != "30m" {
		out += fmt.Sprintf("  -e BEZISYNC_AGENT_DOCKER_IMAGE_PULL_TIMEOUT=%s \\\n", t)
	}
	return out + "  " + s.AgentImage + " agent"
}

func runtimeKind(v *val.V, o *val.Object) string {
	f := o.Get("kind")
	if !f.Required() {
		return ""
	}
	s, _ := v.Literal(f.Loc, f.Raw, "kubernetes", "docker")
	return s
}

// sameJSON compares two JSON documents by their canonical form.
func sameJSON(a, b map[string]any) bool {
	x, err1 := pyjson.Dumps(a)
	y, err2 := pyjson.Dumps(b)
	return err1 == nil && err2 == nil && x == y
}

func runtimeRoutes(r chi.Router) {
	r.Get("/projects/{project_id}/runtime-kinds", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		if _, err := project(r); err != nil {
			return err
		}
		var out []any
		for _, k := range schemas.Runtimes() {
			out = append(out, obj("kind", k.Kind, "title", k.Title, "description", k.Description, "config_schema", k.ConfigSchema, "secret_schema", k.SecretSchema))
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Get("/projects/{project_id}/runtimes", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		if err := a.Require(auth.ProjectView); err != nil {
			return err
		}
		ctx := r.Context()
		q := pool(ctx)
		rows, err := db.Select[models.Runtime](ctx, q, `SELECT * FROM runtimes WHERE project_id = $1 ORDER BY name`, a.Project.ID)
		if err != nil {
			return err
		}
		out := make([]RuntimeRead, len(rows))
		for i := range rows {
			if out[i], err = runtimeRead(ctx, q, &rows[i]); err != nil {
				return err
			}
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Post("/projects/{project_id}/runtimes", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		type create struct {
			Name, Kind, Mode string
			Config, Secrets  map[string]any
			DefaultImage     *string
			Profiles         []runtime.Placement
		}
		b, err := body(r, func(v *val.V, o *val.Object) create {
			c := create{}
			if s := slug(v, o, "name", true); s != nil {
				c.Name = *s
			}
			c.Kind = runtimeKind(v, o)
			c.Mode = "direct"
			if f := o.Get("mode"); f.Present {
				c.Mode, _ = v.Literal(f.Loc, f.Raw, "direct", "agent")
			}
			c.Config = dict(v, o, "config", true)
			if c.Secrets = dict(v, o, "secrets", false); c.Secrets == nil {
				c.Secrets = map[string]any{}
			}
			c.DefaultImage = optStr(v, o, "default_image", val.StrRules{Max: 300})
			c.Profiles, _ = placementProfiles(v, o)
			return c
		})
		if err != nil {
			return err
		}
		if err := a.Require(auth.RuntimeManage); err != nil {
			return err
		}
		cfg, sec, err := validateRuntime(b.Kind, b.Config, b.Secrets)
		if err != nil {
			return err
		}
		ctx := r.Context()
		if err := services.ValidateProfiles(ctx, nil, nil, b.Kind, b.Profiles); err != nil {
			return err
		}
		var out RuntimeRead
		var token string
		err = tx(ctx, func(q db.Querier) error {
			enc, err := services.EncryptOrNil(sec)
			if err != nil {
				return err
			}
			now := db.Now()
			rt := &models.Runtime{ID: db.NewID(), ProjectID: a.Project.ID, Name: b.Name, Kind: b.Kind, Mode: b.Mode, Config: db.Map(cfg), SecretsEnc: enc,
				DefaultImage: b.DefaultImage, Enabled: true, Version: 1, AgentState: db.Map{}, CreatedAt: now, UpdatedAt: now, PlacementProfiles: profileList(b.Profiles)}
			var result runtime.TestResult
			if b.Mode == "agent" {
				token = security.NewToken("bza")
				h := security.TokenHash(token)
				rt.AgentTokenHash = &h
				result = runtime.TestResult{Message: "Waiting for the agent to connect."}
			} else {
				result = testDriver(ctx, func() (runtime.Driver, error) { return services.BuildDriver(ctx, b.Kind, cfg, sec) })
			}
			rt.LastTest = newRuntimeTest(result).dump()
			if len(result.Details) > 0 {
				rt.Capacity = result.Details
			}
			if err := db.Insert(ctx, q, "runtimes", rt); err != nil {
				if db.IsUniqueViolation(err) {
					return problem.Conflict("runtime_exists", fmt.Sprintf("A runtime named '%s' already exists.", b.Name), nil)
				}
				return err
			}
			if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "runtime.created", Summary: fmt.Sprintf("Added %s runtime %s", b.Kind, b.Name),
				ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "runtime", ResourceID: &rt.ID}); err != nil {
				return err
			}
			if out, err = runtimeRead(ctx, q, rt); err != nil {
				return err
			}
			if token != "" {
				install := installCommand(rt, token)
				out.AgentToken, out.AgentInstall = &token, &install
			}
			return nil
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusCreated, out)
		return nil
	}))
	r.Post("/projects/{project_id}/runtimes/{runtime_id}/agent-token", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "runtime_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		if err := a.Require(auth.RuntimeManage); err != nil {
			return err
		}
		ctx := r.Context()
		var out RuntimeRead
		err = tx(ctx, func(q db.Querier) error {
			rt, err := projectRuntime(ctx, q, a, id)
			if err != nil {
				return err
			}
			if rt.Mode != "agent" {
				return problem.New(409, "not_agent_runtime", "This runtime does not use an agent.")
			}
			token := security.NewToken("bza")
			h := security.TokenHash(token)
			rt.AgentTokenHash, rt.UpdatedAt = &h, db.Now()
			if err := db.Update(ctx, q, "runtimes", rt); err != nil {
				return err
			}
			if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "runtime.agent_token_rotated", Summary: "Rotated the agent token of " + rt.Name,
				ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "runtime", ResourceID: &rt.ID}); err != nil {
				return err
			}
			if out, err = runtimeRead(ctx, q, rt); err != nil {
				return err
			}
			install := installCommand(rt, token)
			out.AgentToken, out.AgentInstall = &token, &install
			return nil
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Get("/projects/{project_id}/runtimes/{runtime_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "runtime_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		ctx := r.Context()
		rt, err := projectRuntime(ctx, pool(ctx), a, id)
		if err != nil {
			return err
		}
		out, err := runtimeRead(ctx, pool(ctx), rt)
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Patch("/projects/{project_id}/runtimes/{runtime_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "runtime_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		type update struct {
			Name, DefaultImage *string
			Config, Secrets    map[string]any
			HasConfig          bool
			HasDefaultImage    bool
			Enabled            *bool
			OnlyEnabled        bool
			Profiles           []runtime.Placement
			HasProfiles        bool
		}
		b, err := body(r, func(v *val.V, o *val.Object) update {
			u := update{Name: slug(v, o, "name", false)}
			u.Config, u.HasConfig = optDict(v, o, "config")
			u.Secrets, _ = optDict(v, o, "secrets")
			u.DefaultImage = optStr(v, o, "default_image", val.StrRules{Max: 300})
			u.HasDefaultImage = o.Has("default_image")
			u.Enabled = optBool(v, o, "enabled")
			u.Profiles, u.HasProfiles = placementProfiles(v, o)
			u.OnlyEnabled = !o.Has("name") && !o.Has("config") && !o.Has("secrets") && !o.Has("default_image") && !o.Has("placement_profiles")
			return u
		})
		if err != nil {
			return err
		}
		if err := a.Require(auth.RuntimeManage); err != nil {
			return err
		}
		ctx := r.Context()
		var out RuntimeRead
		err = tx(ctx, func(q db.Querier) error {
			rt, err := projectRuntime(ctx, q, a, id)
			if err != nil {
				return err
			}
			edited, err := parseIfMatch(r)
			if err != nil {
				return err
			}
			if edited != rt.Version {
				return problem.New(412, "version_conflict", "Someone else changed this runtime. Reload and try again.")
			}
			current, err := runtimeSecrets(rt)
			if err != nil {
				return err
			}
			cfgIn := map[string]any(rt.Config)
			if b.HasConfig {
				cfgIn = b.Config
			}
			cfg, sec, err := validateRuntime(rt.Kind, cfgIn, services.MergeSecrets(current, b.Secrets))
			if err != nil {
				return err
			}
			if !sameJSON(cfg, rt.Config) {
				running, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM pipelines WHERE runtime_id = $1 AND deleted_at IS NULL AND desired_state = 'running'`, rt.ID)
				if err != nil {
					return err
				}
				if running > 0 {
					return problem.Conflict("runtime_busy", fmt.Sprintf("Pause the %d running pipeline(s) on this runtime before changing where it points.", running), nil)
				}
			}
			enc, err := services.EncryptOrNil(sec)
			if err != nil {
				return err
			}
			rt.Config, rt.SecretsEnc = db.Map(cfg), enc
			// Profiles change without pausing anything: a pipeline takes its new one at its next deploy.
			if b.HasProfiles {
				if err := services.ValidateProfiles(ctx, q, rt, rt.Kind, b.Profiles); err != nil {
					return err
				}
				rt.PlacementProfiles = profileList(b.Profiles)
			}
			if b.HasDefaultImage {
				rt.DefaultImage = b.DefaultImage
			}
			renamed := b.Name != nil && *b.Name != rt.Name
			previous := rt.Name
			if renamed {
				n, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM runtimes WHERE project_id = $1 AND name = $2`, rt.ProjectID, *b.Name)
				if err != nil {
					return err
				}
				if n > 0 {
					return problem.Conflict("runtime_exists", fmt.Sprintf("A runtime named '%s' already exists.", *b.Name), nil)
				}
				rt.Name = *b.Name
			}
			toggled := b.Enabled != nil && *b.Enabled != rt.Enabled
			if toggled {
				rt.Enabled = *b.Enabled
			}
			rt.Version++
			rt.UpdatedAt = db.Now()
			if err := db.Update(ctx, q, "runtimes", rt); err != nil {
				return err
			}
			if toggled {
				kind, verb := "runtime.disabled", "Disabled"
				if rt.Enabled {
					kind, verb = "runtime.enabled", "Enabled"
				}
				if _, err := services.Record(ctx, q, services.AuditEvent{Kind: kind, Summary: verb + " runtime " + rt.Name,
					ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "runtime", ResourceID: &rt.ID}); err != nil {
					return err
				}
			}
			if !(toggled && b.OnlyEnabled) {
				summary := "Updated runtime " + rt.Name
				if renamed {
					summary += " (renamed from " + previous + ")"
				}
				if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "runtime.updated", Summary: summary,
					ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "runtime", ResourceID: &rt.ID}); err != nil {
					return err
				}
			}
			out, err = runtimeRead(ctx, q, rt)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Delete("/projects/{project_id}/runtimes/{runtime_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "runtime_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		if err := a.Require(auth.RuntimeManage); err != nil {
			return err
		}
		ctx := r.Context()
		err = tx(ctx, func(q db.Querier) error {
			rt, err := projectRuntime(ctx, q, a, id)
			if err != nil {
				return err
			}
			live, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM pipelines WHERE runtime_id = $1 AND deleted_at IS NULL`, rt.ID)
			if err != nil {
				return err
			}
			if live > 0 {
				return problem.Conflict("runtime_in_use", "Move or delete the pipelines on this runtime first.", nil)
			}
			pending, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM jobs WHERE state IN ('queued', 'running')
				AND resource_id IN (SELECT id FROM pipelines WHERE runtime_id = $1)`, rt.ID)
			if err != nil {
				return err
			}
			if pending > 0 {
				return problem.Conflict("runtime_cleanup_pending", "Deleted pipelines are still being cleaned up on this runtime. Try again once that finishes.", nil)
			}
			// Deleted pipelines keep their history; they just no longer point at a runtime.
			if _, err := q.Exec(ctx, `UPDATE pipelines SET runtime_id = NULL WHERE runtime_id = $1`, rt.ID); err != nil {
				return err
			}
			if err := db.Delete(ctx, q, "runtimes", rt.ID); err != nil {
				return err
			}
			_, err = services.Record(ctx, q, services.AuditEvent{Kind: "runtime.deleted", Summary: "Deleted runtime " + rt.Name,
				ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "runtime", ResourceID: &rt.ID})
			return err
		})
		if err != nil {
			return err
		}
		httpapi.NoContent(w)
		return nil
	}))
	r.Post("/projects/{project_id}/runtimes/{runtime_id}/test", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "runtime_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		ctx := r.Context()
		if _, err := projectRuntime(ctx, pool(ctx), a, id); err != nil {
			return err
		}
		// A test calls the runtime and rewrites its last result and capacity.
		if err := a.Require(auth.RuntimeManage); err != nil {
			return err
		}
		var out RuntimeTest
		err = tx(ctx, func(q db.Querier) error {
			rt, err := projectRuntime(ctx, q, a, id)
			if err != nil {
				return err
			}
			result := testDriver(ctx, func() (runtime.Driver, error) { return services.DriverFor(ctx, q, rt) })
			out = newRuntimeTest(result)
			dump := out.dump()
			capacity := rt.Capacity
			if len(result.Details) > 0 {
				capacity = result.Details
			}
			_, err = q.Exec(ctx, `UPDATE runtimes SET last_test = $1, capacity = $2, updated_at = $3 WHERE id = $4`, dump, capacity, db.Now(), rt.ID)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Post("/projects/{project_id}/runtimes:test", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		type draft struct {
			Kind            string
			Config, Secrets map[string]any
		}
		b, err := body(r, func(v *val.V, o *val.Object) draft {
			d := draft{Kind: runtimeKind(v, o), Config: dict(v, o, "config", true)}
			if d.Secrets = dict(v, o, "secrets", false); d.Secrets == nil {
				d.Secrets = map[string]any{}
			}
			return d
		})
		if err != nil {
			return err
		}
		if err := a.Require(auth.RuntimeManage); err != nil {
			return err
		}
		cfg, sec, err := validateRuntime(b.Kind, b.Config, b.Secrets)
		if err != nil {
			return err
		}
		ctx := r.Context()
		result := testDriver(ctx, func() (runtime.Driver, error) { return services.BuildDriver(ctx, b.Kind, cfg, sec) })
		httpapi.JSON(w, http.StatusOK, newRuntimeTest(result))
		return nil
	}))
}

func profileList(profiles []runtime.Placement) db.List {
	out := db.List{}
	raw, _ := json.Marshal(profiles)
	var items []any
	_ = json.Unmarshal(raw, &items)
	return append(out, items...)
}

func nonNil(p []runtime.Placement) []runtime.Placement {
	if p == nil {
		return []runtime.Placement{}
	}
	return p
}
