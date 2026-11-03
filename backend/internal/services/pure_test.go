package services_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/contract"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/services"
)

type pureFixture struct {
	Adopt []struct {
		Text   string          `json:"text"`
		Report json.RawMessage `json:"report"`
	} `json:"adopt"`
	Offsets []struct {
		Position  json.RawMessage `json:"position"`
		Described json.RawMessage `json:"described"`
	} `json:"offsets"`
	Conditions []struct {
		Kind     string `json:"kind"`
		Pipeline struct {
			Name              string          `json:"name"`
			Status            json.RawMessage `json:"status"`
			DesiredState      string          `json:"desired_state"`
			DeployedVersionID *string         `json:"deployed_version_id"`
			Lifecycle         string          `json:"lifecycle"`
			Health            string          `json:"health"`
		} `json:"pipeline"`
		Params json.RawMessage `json:"params"`
		Cond   struct {
			Active   bool    `json:"active"`
			Title    string  `json:"title"`
			Message  string  `json:"message"`
			Fix      string  `json:"fix"`
			Severity *string `json:"severity"`
		} `json:"cond"`
	} `json:"conditions"`
	SSO struct {
		Rules    []map[string]any `json:"rules"`
		Projects []string         `json:"projects"`
		Cases    []struct {
			Groups          []string         `json:"groups"`
			DefaultRole     string           `json:"default_role"`
			DefaultProjects []string         `json:"default_projects"`
			Access          map[string][]any `json:"access"`
			Superuser       bool             `json:"superuser"`
		} `json:"cases"`
	} `json:"sso"`
}

func gen(t *testing.T, raw []byte) any {
	t.Helper()
	g, err := pyjson.DecodeGeneric(raw)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func canonical(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	s, err := pyjson.Dumps(gen(t, raw))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestPureServicesMatchPython(t *testing.T) {
	var fx pureFixture
	contract.Load(t, "services_pure", &fx)
	for _, c := range fx.Adopt {
		got := canonical(t, services.Adopt(c.Text))
		want := canonical(t, gen(t, c.Report))
		if got != want {
			t.Errorf("adopt(%q)\n got %s\nwant %s", c.Text, got, want)
		}
	}
	for _, c := range fx.Offsets {
		pos := gen(t, c.Position).(map[string]any)
		if got, want := canonical(t, services.DescribeOffset(pos)), canonical(t, gen(t, c.Described)); got != want {
			t.Errorf("describe(%s)\n got %s\nwant %s", c.Position, got, want)
		}
	}
	now, _ := pytime.Parse("2026-01-01T12:00:00+00:00")
	for i, c := range fx.Conditions {
		st := gen(t, c.Pipeline.Status).(map[string]any)
		p := &models.Pipeline{Name: c.Pipeline.Name, Status: db.Map(st), DesiredState: c.Pipeline.DesiredState,
			Lifecycle: c.Pipeline.Lifecycle, Health: c.Pipeline.Health}
		if c.Pipeline.DeployedVersionID != nil {
			id := uuid.New()
			p.DeployedVersionID = &id
		}
		params, _ := gen(t, c.Params).(map[string]any)
		got := services.Evaluate(c.Kind, p, params, now)
		want := c.Cond
		sev := ""
		if want.Severity != nil {
			sev = *want.Severity
		}
		if got.Active != want.Active || got.Title != want.Title || got.Message != want.Message || got.Fix != want.Fix || got.Severity != sev {
			t.Errorf("condition %d (%s): got %+v, want %+v", i, c.Kind, got, want)
		}
	}
	projects := map[string]*models.Project{}
	for _, s := range fx.SSO.Projects {
		projects[s] = &models.Project{Slug: s}
	}
	for _, c := range fx.SSO.Cases {
		access, _, super := services.DesiredAccess(fx.SSO.Rules, c.Groups, projects, fx.SSO.Projects, c.DefaultRole, c.DefaultProjects)
		got := map[string][]any{}
		for k, a := range access {
			got[k] = []any{a.Role, a.Approver}
		}
		if !reflect.DeepEqual(got, c.Access) || super != c.Superuser {
			t.Errorf("desired_access(%v, %q): got %v %v, want %v %v", c.Groups, c.DefaultRole, got, super, c.Access, c.Superuser)
		}
	}
}
