package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/schemas"
	"github.com/daun-gatal/bezisync/backend/internal/secrets"
	"github.com/daun-gatal/bezisync/backend/internal/security"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// StoreTest is the result of a secret store test or reference check.
type StoreTest struct {
	OK       bool        `json:"ok"`
	Message  string      `json:"message"`
	TestedAt pytime.Time `json:"tested_at"`
}

func (t StoreTest) dump() db.Map {
	return db.Map{"ok": t.OK, "message": t.Message, "tested_at": pytime.Pydantic(t.TestedAt.Time)}
}

// StoreRead is a secret store; its credentials are never returned.
type StoreRead struct {
	ID              uuid.UUID `json:"id"`
	Name            string    `json:"name"`
	Kind            string    `json:"kind"`
	Config          db.Map    `json:"config"`
	SecretFieldsSet []string  `json:"secret_fields_set"`
	// When the credentials were last set or replaced (null: none saved yet).
	SecretsUpdatedAt *pytime.Time `json:"secrets_updated_at"`
	LastTest         *StoreTest   `json:"last_test"`
	UsedBy           []string     `json:"used_by"`
	Version          int          `json:"version"`
}

// StoreSecrets are the paths of a store's secrets (never their values).
type StoreSecrets struct {
	OK      bool     `json:"ok"`
	Message string   `json:"message"`
	Secrets []string `json:"secrets"`
	// More secrets exist than are listed: type the path instead.
	Truncated bool `json:"truncated"`
}

// SecretKeys are the key names of one secret (never their values).
type SecretKeys struct {
	OK      bool     `json:"ok"`
	Message string   `json:"message"`
	Keys    []string `json:"keys"`
	// The value is not a JSON object: a reference uses all of it (no #key).
	WholeValue bool `json:"whole_value"`
}

func storeUsedBy(ctx context.Context, q db.Querier, s *models.SecretStore) ([]string, error) {
	conns, err := db.Select[models.Connection](ctx, q, `SELECT * FROM connections WHERE project_id = $1`, s.ProjectID)
	if err != nil {
		return nil, err
	}
	names := []string{}
	for i := range conns {
		stored, err := services.StoredSecrets(&conns[i])
		if err != nil {
			return nil, err
		}
		refs, err := services.RefsOf(stored)
		if err != nil {
			return nil, err
		}
		for _, r := range refs {
			if r.Store == s.Name {
				names = append(names, conns[i].Name)
				break
			}
		}
	}
	sort.Strings(names)
	return names, nil
}

func storeRead(ctx context.Context, q db.Querier, s *models.SecretStore) (StoreRead, error) {
	fields := []string{}
	if s.SecretsEnc != nil && *s.SecretsEnc != "" {
		sec, err := security.DecryptSecret(*s.SecretsEnc)
		if err != nil {
			return StoreRead{}, err
		}
		for k := range sec {
			fields = append(fields, k)
		}
		sort.Strings(fields)
	}
	used, err := storeUsedBy(ctx, q, s)
	if err != nil {
		return StoreRead{}, err
	}
	out := StoreRead{ID: s.ID, Name: s.Name, Kind: s.Kind, Config: s.Config, SecretFieldsSet: fields, SecretsUpdatedAt: pytime.P(s.SecretsUpdatedAt), UsedBy: used, Version: s.Version}
	if out.Config == nil {
		out.Config = db.Map{}
	}
	if len(s.LastTest) > 0 {
		ok, _ := s.LastTest["ok"].(bool)
		msg, _ := s.LastTest["message"].(string)
		at, _ := s.LastTest["tested_at"].(string)
		t, err := pytime.Parse(at)
		if err != nil {
			return StoreRead{}, err
		}
		out.LastTest = &StoreTest{OK: ok, Message: msg, TestedAt: pytime.T(t)}
	}
	return out, nil
}

func projectStore(ctx context.Context, q db.Querier, a *auth.ProjectAccess, id uuid.UUID) (*models.SecretStore, error) {
	s, err := db.Find[models.SecretStore](ctx, q, `SELECT * FROM secret_stores WHERE id = $1`, id)
	if err != nil {
		return nil, err
	}
	if s == nil || s.ProjectID != a.Project.ID {
		return nil, problem.NotFound("Secret store")
	}
	return s, nil
}

// storeErrorMessage is the user-facing text of a store client error.
func storeErrorMessage(err error) string {
	var se *secrets.Error
	if errors.As(err, &se) {
		return se.Error()
	}
	// Client libraries fail in all kinds of ways.
	msg := err.Error()
	if utf8.RuneCountInString(msg) > 300 {
		msg = string([]rune(msg)[:300])
	}
	return msg
}

func testStore(ctx context.Context, s *models.SecretStore) StoreTest {
	now := pytime.T(db.Now())
	client, err := services.StoreClient(ctx, s)
	if err != nil {
		return StoreTest{Message: storeErrorMessage(err), TestedAt: now}
	}
	msg, err := client.Test(ctx)
	if err != nil {
		return StoreTest{Message: storeErrorMessage(err), TestedAt: now}
	}
	return StoreTest{OK: true, Message: msg, TestedAt: now}
}

func validateStore(kind string, cfg, sec map[string]any) (map[string]any, map[string]any, error) {
	k, ok := schemas.Store(kind)
	if !ok {
		return nil, nil, problem.New(422, "validation_failed", "Some fields are invalid.")
	}
	return validateKind(k, cfg, sec)
}

func secretStoreRoutes(r chi.Router) {
	r.Get("/projects/{project_id}/secret-store-kinds", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		if _, err := project(r); err != nil {
			return err
		}
		out := []any{}
		for _, k := range schemas.Stores() {
			out = append(out, obj("kind", k.Kind, "title", k.Title, "description", k.Description, "config_schema", k.ConfigSchema, "secret_schema", k.SecretSchema))
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Get("/projects/{project_id}/secret-stores", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		if err := a.Require(auth.ConnectionView); err != nil {
			return err
		}
		ctx := r.Context()
		q := pool(ctx)
		rows, err := db.Select[models.SecretStore](ctx, q, `SELECT * FROM secret_stores WHERE project_id = $1 ORDER BY name`, a.Project.ID)
		if err != nil {
			return err
		}
		out := make([]StoreRead, len(rows))
		for i := range rows {
			if out[i], err = storeRead(ctx, q, &rows[i]); err != nil {
				return err
			}
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Post("/projects/{project_id}/secret-stores", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		type create struct {
			Name, Kind      string
			Config, Secrets map[string]any
			// Test the settings first and save nothing when the test fails.
			TestFirst bool
		}
		b, err := body(r, func(v *val.V, o *val.Object) create {
			c := create{}
			if s := slug(v, o, "name", true); s != nil {
				c.Name = *s
			}
			if f := o.Get("kind"); f.Required() {
				c.Kind, _ = v.Literal(f.Loc, f.Raw, "vault", "aws", "gcp", "kubernetes")
			}
			c.Config = dict(v, o, "config", true)
			if c.Secrets = dict(v, o, "secrets", false); c.Secrets == nil {
				c.Secrets = map[string]any{}
			}
			c.TestFirst = boolean(v, o, "test_first", false)
			return c
		})
		if err != nil {
			return err
		}
		if err := a.Require(auth.ProjectManage); err != nil {
			return err
		}
		cfg, sec, err := validateStore(b.Kind, b.Config, b.Secrets)
		if err != nil {
			return err
		}
		ctx := r.Context()
		var out StoreRead
		err = tx(ctx, func(q db.Querier) error {
			enc, err := services.EncryptOrNil(sec)
			if err != nil {
				return err
			}
			now := db.Now()
			s := &models.SecretStore{ID: db.NewID(), ProjectID: a.Project.ID, Name: b.Name, Kind: b.Kind, Config: db.Map(cfg), SecretsEnc: enc,
				Version: 1, CreatedAt: now, UpdatedAt: now}
			if enc != nil {
				s.SecretsUpdatedAt = &now
			}
			result := testStore(ctx, s)
			if b.TestFirst && !result.OK {
				return problem.New(422, "store_test_failed", "Couldn't connect with these settings, so nothing was saved: "+result.Message)
			}
			s.LastTest = result.dump()
			if err := db.Insert(ctx, q, "secret_stores", s); err != nil {
				if db.IsUniqueViolation(err) {
					return problem.Conflict("secret_store_exists", fmt.Sprintf("A secret store named '%s' already exists.", b.Name), nil)
				}
				return err
			}
			if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "secret_store.created", Summary: fmt.Sprintf("Added %s secret store %s", b.Kind, b.Name),
				ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "secret_store", ResourceID: &s.ID}); err != nil {
				return err
			}
			out, err = storeRead(ctx, q, s)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusCreated, out)
		return nil
	}))
	r.Patch("/projects/{project_id}/secret-stores/{store_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "store_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		type update struct {
			Config, Secrets map[string]any
			HasConfig       bool
			// Test the new settings first and save nothing when the test fails.
			TestFirst bool
		}
		b, err := body(r, func(v *val.V, o *val.Object) update {
			u := update{}
			u.Config, u.HasConfig = optDict(v, o, "config")
			u.Secrets, _ = optDict(v, o, "secrets")
			u.TestFirst = boolean(v, o, "test_first", false)
			return u
		})
		if err != nil {
			return err
		}
		if err := a.Require(auth.ProjectManage); err != nil {
			return err
		}
		ctx := r.Context()
		var out StoreRead
		err = tx(ctx, func(q db.Querier) error {
			s, err := projectStore(ctx, q, a, id)
			if err != nil {
				return err
			}
			edited, err := parseIfMatch(r)
			if err != nil {
				return err
			}
			if edited != s.Version {
				return problem.New(412, "version_conflict", "Someone else changed this secret store. Reload and try again.")
			}
			current := map[string]any{}
			if s.SecretsEnc != nil && *s.SecretsEnc != "" {
				if current, err = security.DecryptSecret(*s.SecretsEnc); err != nil {
					return err
				}
			}
			cfgIn := map[string]any(s.Config)
			if b.HasConfig {
				cfgIn = b.Config
			}
			cfg, sec, err := validateStore(s.Kind, cfgIn, services.MergeSecrets(current, b.Secrets))
			if err != nil {
				return err
			}
			enc, err := services.EncryptOrNil(sec)
			if err != nil {
				return err
			}
			s.Config, s.SecretsEnc = db.Map(cfg), enc
			if len(b.Secrets) > 0 {
				now := db.Now()
				s.SecretsUpdatedAt = &now
				if enc == nil {
					s.SecretsUpdatedAt = nil
				}
			}
			s.Version++
			services.ForgetStore(s.ID)
			result := testStore(ctx, s)
			if b.TestFirst && !result.OK {
				return problem.New(422, "store_test_failed", "Couldn't connect with the new settings, so nothing was saved: "+result.Message)
			}
			s.LastTest = result.dump()
			s.UpdatedAt = db.Now()
			if err := db.Update(ctx, q, "secret_stores", s); err != nil {
				return err
			}
			if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "secret_store.updated", Summary: "Updated secret store " + s.Name,
				ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "secret_store", ResourceID: &s.ID}); err != nil {
				return err
			}
			out, err = storeRead(ctx, q, s)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Delete("/projects/{project_id}/secret-stores/{store_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "store_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		if err := a.Require(auth.ProjectManage); err != nil {
			return err
		}
		ctx := r.Context()
		err = tx(ctx, func(q db.Querier) error {
			s, err := projectStore(ctx, q, a, id)
			if err != nil {
				return err
			}
			users, err := storeUsedBy(ctx, q, s)
			if err != nil {
				return err
			}
			if len(users) > 0 {
				return problem.Conflict("secret_store_in_use", fmt.Sprintf("Connections still read secrets from it: %s.", strings.Join(users, ", ")), nil)
			}
			if err := db.Delete(ctx, q, "secret_stores", s.ID); err != nil {
				return err
			}
			services.ForgetStore(s.ID)
			_, err = services.Record(ctx, q, services.AuditEvent{Kind: "secret_store.deleted", Summary: "Deleted secret store " + s.Name,
				ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "secret_store", ResourceID: &s.ID})
			return err
		})
		if err != nil {
			return err
		}
		httpapi.NoContent(w)
		return nil
	}))
	r.Post("/projects/{project_id}/secret-stores/{store_id}/test", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "store_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		if err := a.Require(auth.ConnectionManage); err != nil {
			return err
		}
		ctx := r.Context()
		var out StoreTest
		err = tx(ctx, func(q db.Querier) error {
			s, err := projectStore(ctx, q, a, id)
			if err != nil {
				return err
			}
			out = testStore(ctx, s)
			_, err = q.Exec(ctx, `UPDATE secret_stores SET last_test = $1 WHERE id = $2`, out.dump(), s.ID)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	// What a picker offers: the store's secret paths and, for one secret, its key names. Values
	// never leave the server; a store that can't list answers ok=false with the reason.
	r.Get("/projects/{project_id}/secret-stores/{store_id}/secrets", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "store_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		if err := a.Require(auth.ConnectionManage); err != nil {
			return err
		}
		ctx := r.Context()
		s, err := projectStore(ctx, pool(ctx), a, id)
		if err != nil {
			return err
		}
		paths, truncated, err := func() ([]string, bool, error) {
			client, err := services.StoreClient(ctx, s)
			if err != nil {
				return nil, false, err
			}
			return client.List(ctx, secrets.ListLimit)
		}()
		if err != nil {
			httpapi.JSON(w, http.StatusOK, StoreSecrets{Message: storeErrorMessage(err), Secrets: []string{}})
			return nil
		}
		if paths == nil {
			paths = []string{}
		}
		httpapi.JSON(w, http.StatusOK, StoreSecrets{OK: true, Secrets: paths, Truncated: truncated})
		return nil
	}))
	r.Post("/projects/{project_id}/secret-stores/{store_id}/keys", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "store_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		path, err := body(r, func(v *val.V, o *val.Object) string {
			return str(v, o, "path", "", val.StrRules{Min: 1, Max: 500}, true)
		})
		if err != nil {
			return err
		}
		if err := a.Require(auth.ConnectionManage); err != nil {
			return err
		}
		ctx := r.Context()
		s, err := projectStore(ctx, pool(ctx), a, id)
		if err != nil {
			return err
		}
		keys, err := func() ([]string, error) {
			client, err := services.StoreClient(ctx, s)
			if err != nil {
				return nil, err
			}
			return secrets.Keys(ctx, client, path)
		}()
		if err != nil {
			httpapi.JSON(w, http.StatusOK, SecretKeys{Message: storeErrorMessage(err), Keys: []string{}})
			return nil
		}
		httpapi.JSON(w, http.StatusOK, SecretKeys{OK: true, Keys: append([]string{}, keys...), WholeValue: keys == nil})
		return nil
	}))
	r.Post("/projects/{project_id}/secret-stores/{store_id}/check-ref", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "store_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		type check struct {
			Path string
			Key  *string
		}
		b, err := body(r, func(v *val.V, o *val.Object) check {
			return check{Path: str(v, o, "path", "", val.StrRules{Min: 1, Max: 500}, true), Key: optStr(v, o, "key", val.StrRules{Max: 200})}
		})
		if err != nil {
			return err
		}
		if err := a.Require(auth.ConnectionManage); err != nil {
			return err
		}
		ctx := r.Context()
		s, err := projectStore(ctx, pool(ctx), a, id)
		if err != nil {
			return err
		}
		// The value itself is never returned.
		value, err := func() (string, error) {
			client, err := services.StoreClient(ctx, s)
			if err != nil {
				return "", err
			}
			return secrets.Get(ctx, client, b.Path, b.Key)
		}()
		now := pytime.T(db.Now())
		if err != nil {
			var se *secrets.Error
			if errors.As(err, &se) {
				httpapi.JSON(w, http.StatusOK, StoreTest{Message: se.Error(), TestedAt: now})
				return nil
			}
			return err
		}
		ref := fmt.Sprintf("ref+%s://%s", s.Name, b.Path)
		if b.Key != nil && *b.Key != "" {
			ref += "#" + *b.Key
		}
		httpapi.JSON(w, http.StatusOK, StoreTest{OK: true, Message: fmt.Sprintf("Found a value (%d characters). Use %s", utf8.RuneCountInString(value), ref), TestedAt: now})
		return nil
	}))
}
