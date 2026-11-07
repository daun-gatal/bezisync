package services

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/schemas"
	"github.com/daun-gatal/bezisync/backend/internal/schemaval"
	"github.com/daun-gatal/bezisync/backend/internal/secrets"
	"github.com/daun-gatal/bezisync/backend/internal/security"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// Connection lifecycle: validation, write-only secret handling, checks and discovery.

func validationProblem(errs []problemFieldError, where string) *problem.Problem {
	out := make([]map[string]any, len(errs))
	for i, e := range errs {
		out[i] = map[string]any{"loc": append([]string{where}, e.Loc...), "message": e.Message, "type": e.Type}
	}
	return problem.New(422, "validation_failed", "Some fields are invalid.").WithErrors(out)
}

type problemFieldError struct {
	Loc     []string
	Message string
	Type    string
}

func fieldErrors(v *val.V) []problemFieldError {
	out := make([]problemFieldError, len(v.Errors))
	for i, e := range v.Errors {
		t := ""
		if e.Type != nil {
			t = *e.Type
		}
		out[i] = problemFieldError{Loc: e.Loc, Message: e.Message, Type: t}
	}
	return out
}

// ValidateModel validates config + secrets against models; secret references are only
// syntax-checked (the value lives in the store), so "missing" errors for referenced
// fields are acceptable. Returns the normalized config and stored secrets.
func ValidateModel(configSchema, secretSchema schemas.Schema, config, secretValues map[string]any) (map[string]any, map[string]any, error) {
	if config == nil {
		config = map[string]any{}
	}
	if secretValues == nil {
		secretValues = map[string]any{}
	}
	cv := &val.V{}
	cfg, ok := schemaval.Model(cv, nil, configSchema, config)
	if !ok {
		return nil, nil, validationProblem(fieldErrors(cv), "config")
	}
	// Settings never hold "${": Debezium Server and the Flink CDC helper would read it as a
	// variable, which can name another secret (connectors.ConfigExpressions).
	if locs := connectors.ConfigExpressions(cfg); len(locs) > 0 {
		errs := make([]problemFieldError, len(locs))
		for i, l := range locs {
			errs[i] = problemFieldError{Loc: l, Message: "Value error, must not contain \"${\": it would be read as a variable", Type: "value_error"}
		}
		return nil, nil, validationProblem(errs, "config")
	}
	refs, err := RefsOf(secretValues)
	if err != nil {
		return nil, nil, err
	}
	plain := map[string]any{}
	for k, v := range secretValues {
		if _, isRef := refs[k]; !isRef {
			plain[k] = v
		}
	}
	sv := &val.V{}
	sec, ok := schemaval.Model(sv, nil, secretSchema, plain)
	if !ok {
		var real []problemFieldError
		for _, e := range fieldErrors(sv) {
			if len(e.Loc) > 0 {
				if _, isRef := refs[e.Loc[0]]; isRef {
					continue
				}
			}
			real = append(real, e)
		}
		if len(real) > 0 {
			return nil, nil, validationProblem(real, "secrets")
		}
		sec = plain // model_construct: the given values only
	}
	values := map[string]any{}
	for k, v := range sec {
		if _, isRef := refs[k]; isRef || v == nil || v == "" {
			continue
		}
		values[k] = v
	}
	for k, r := range refs {
		values[k] = r.String()
	}
	return cfg, values, nil
}

// ValidateConnection validates a connection's config and secrets.
func ValidateConnection(kind, typ string, config, secretValues map[string]any) (map[string]any, map[string]any, error) {
	d, err := connectors.Describe(kind, typ)
	if err != nil {
		return nil, nil, err
	}
	return ValidateModel(d.ConfigSchema, d.SecretSchema, config, secretValues)
}

// StoredSecrets decrypts a connection's secrets.
func StoredSecrets(c *models.Connection) (map[string]any, error) {
	if c.SecretsEnc == nil || *c.SecretsEnc == "" {
		return map[string]any{}, nil
	}
	return security.DecryptSecret(*c.SecretsEnc)
}

// MergeSecrets is a write-only merge: given keys replace, null or "" removes, omitted stay.
func MergeSecrets(current, patch map[string]any) map[string]any {
	merged := make(map[string]any, len(current))
	for k, v := range current {
		merged[k] = v
	}
	for k, v := range patch {
		if v == nil || v == "" {
			delete(merged, k)
		} else {
			merged[k] = v
		}
	}
	return merged
}

// EncryptOrNil encrypts non-empty secrets (nil when empty).
func EncryptOrNil(values map[string]any) (*string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	s, err := security.EncryptSecret(values)
	if err != nil {
		return nil, err
	}
	return &s, nil
}

// SecretFieldsSet lists the names of stored secret fields.
func SecretFieldsSet(c *models.Connection) ([]string, error) {
	s, err := StoredSecrets(c)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(s))
	for k := range s {
		out = append(out, k)
	}
	sort.Strings(out)
	return out, nil
}

// SecretRefsOf lists fields that reference an external store.
func SecretRefsOf(c *models.Connection) (map[string]string, error) {
	s, err := StoredSecrets(c)
	if err != nil {
		return nil, err
	}
	refs, err := RefsOf(s)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for k, r := range refs {
		out[k] = r.String()
	}
	return out, nil
}

// RefsOf parses every reference in a secrets dict (422 for malformed ones).
func RefsOf(values map[string]any) (map[string]secrets.Ref, error) {
	out := map[string]secrets.Ref{}
	for _, field := range sortedKeys(values) {
		r, err := secrets.ParseRef(values[field])
		if err != nil {
			return nil, problem.New(422, "validation_failed", "Some fields are invalid.").WithErrors([]map[string]any{
				{"loc": []string{"secrets", field}, "message": err.Error(), "type": "secret_ref"},
			})
		}
		if r != nil {
			out[field] = *r
		}
	}
	return out, nil
}

func sortedKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// --- secret stores ---

const storeCacheTTL = 60 * time.Second

type storeKey struct {
	id      uuid.UUID
	version int
	path    string
	key     string
}

var (
	storeCacheMu sync.Mutex
	storeCache   = map[storeKey]struct {
		at    time.Time
		value string
	}{}
)

// UnresolvedSecret means a referenced secret could not be read (422).
func UnresolvedSecret(detail string) *problem.Problem {
	return problem.New(422, "secret_ref_unresolved", detail)
}

// IsUnresolved reports an UnresolvedSecret or MissingSecret problem: a secret the connection
// needs and does not have.
func IsUnresolved(err error) (*problem.Problem, bool) {
	var p *problem.Problem
	if errors.As(err, &p) && (p.Code == "secret_ref_unresolved" || p.Code == "secret_missing") {
		return p, true
	}
	return nil, false
}

func findStore(ctx context.Context, q db.Querier, projectID uuid.UUID, name string) (*models.SecretStore, error) {
	return db.Find[models.SecretStore](ctx, q, `SELECT * FROM secret_stores WHERE project_id = $1 AND name = $2`, projectID, name)
}

// StoreClient builds the client of a stored secret store.
func StoreClient(ctx context.Context, s *models.SecretStore) (secrets.Client, error) {
	sec := map[string]any{}
	if s.SecretsEnc != nil && *s.SecretsEnc != "" {
		var err error
		if sec, err = security.DecryptSecret(*s.SecretsEnc); err != nil {
			return nil, err
		}
	}
	cfg, err := normalizeStoreConfig(s.Kind, s.Config)
	if err != nil {
		return nil, err
	}
	return secrets.Build(ctx, s.Kind, cfg, sec)
}

func normalizeStoreConfig(kind string, raw map[string]any) (map[string]any, error) {
	k, ok := schemas.Store(kind)
	if !ok {
		return nil, &secrets.Error{Msg: fmt.Sprintf("Unknown secret store kind '%s'.", kind)}
	}
	v := &val.V{}
	cfg, okk := schemaval.Model(v, nil, k.ConfigSchema, raw)
	if !okk {
		return nil, fmt.Errorf("stored secret store config is invalid: %v", v.Errors)
	}
	return cfg, nil
}

// ValidateRefs requires every reference to name a store of the project.
func ValidateRefs(ctx context.Context, q db.Querier, projectID uuid.UUID, values map[string]any) error {
	refs, err := RefsOf(values)
	if err != nil {
		return err
	}
	var errs []map[string]any
	for _, field := range sortedKeys(values) {
		r, ok := refs[field]
		if !ok {
			continue
		}
		s, err := findStore(ctx, q, projectID, r.Store)
		if err != nil {
			return err
		}
		if s == nil {
			errs = append(errs, map[string]any{"loc": []string{"secrets", field}, "message": fmt.Sprintf("There is no secret store named '%s'.", r.Store), "type": "secret_ref"})
		}
	}
	if len(errs) > 0 {
		return problem.New(422, "validation_failed", "Some fields are invalid.").WithErrors(errs)
	}
	return nil
}

// ResolveRefs replaces references with their values (60 s cache per store version).
func ResolveRefs(ctx context.Context, q db.Querier, projectID uuid.UUID, values map[string]any) (map[string]any, error) {
	refs, err := RefsOf(values)
	if err != nil {
		return nil, err
	}
	if len(refs) == 0 {
		return values, nil
	}
	out := make(map[string]any, len(values))
	for k, v := range values {
		out[k] = v
	}
	clients := map[string]secrets.Client{}
	for _, field := range sortedKeys(values) {
		r, ok := refs[field]
		if !ok {
			continue
		}
		store, err := findStore(ctx, q, projectID, r.Store)
		if err != nil {
			return nil, err
		}
		if store == nil {
			return nil, UnresolvedSecret(fmt.Sprintf("Secret '%s' refers to the secret store '%s', which does not exist.", field, r.Store))
		}
		key := storeKey{store.ID, store.Version, r.Path, ""}
		if r.Key != nil {
			key.key = "#" + *r.Key
		}
		now := time.Now()
		storeCacheMu.Lock()
		hit, cached := storeCache[key]
		storeCacheMu.Unlock()
		if cached && now.Sub(hit.at) < storeCacheTTL {
			out[field] = hit.value
			continue
		}
		client, ok := clients[store.Name]
		if !ok {
			client, err = StoreClient(ctx, store)
			if err != nil {
				var se *secrets.Error
				if errors.As(err, &se) {
					return nil, UnresolvedSecret(fmt.Sprintf("Secret '%s' could not be read from '%s' (%s): %s", field, r.Store, r, se.Msg))
				}
				return nil, err
			}
			clients[store.Name] = client
		}
		value, err := secrets.Get(ctx, client, r.Path, r.Key)
		if err != nil {
			var se *secrets.Error
			if errors.As(err, &se) {
				return nil, UnresolvedSecret(fmt.Sprintf("Secret '%s' could not be read from '%s' (%s): %s", field, r.Store, r, se.Msg))
			}
			return nil, err
		}
		storeCacheMu.Lock()
		storeCache[key] = struct {
			at    time.Time
			value string
		}{now, value}
		storeCacheMu.Unlock()
		out[field] = value
	}
	return out, nil
}

// ForgetStore drops cached values of a store.
func ForgetStore(storeID uuid.UUID) {
	storeCacheMu.Lock()
	defer storeCacheMu.Unlock()
	for k := range storeCache {
		if k.id == storeID {
			delete(storeCache, k)
		}
	}
}

// UnresolvedReport is the check report for a secret that could not be read or was never entered.
func UnresolvedReport(p *problem.Problem) checks.Report {
	if p.Code == "secret_missing" {
		return checks.Summarize([]checks.Check{{
			Code: "secret_missing", Title: "Saved secrets", Status: checks.Fail, Category: checks.CatAuth,
			Blocking: true, Message: p.Detail, Why: "The connection was copied from another project without its secrets, and nobody has entered them here yet.",
		}}, time.Now())
	}
	return checks.Summarize([]checks.Check{{
		Code: "secret_ref_unresolved", Title: "Secrets from the secret store", Status: checks.Fail, Category: checks.CatAuth,
		Blocking: true, Message: p.Detail, Why: "The connection takes a secret from an external store, and reading it failed.",
	}}, time.Now())
}

// Resolve decrypts the stored secrets, reads referenced ones from their stores and attaches the
// connections it links to.
func Resolve(ctx context.Context, q db.Querier, c *models.Connection) (connectors.Resolved, error) {
	r, err := resolveOwn(ctx, q, c)
	if err != nil {
		return connectors.Resolved{}, err
	}
	if err := ResolveLinks(ctx, q, c.ProjectID, c.Kind, &r); err != nil {
		return connectors.Resolved{}, err
	}
	return r, nil
}

// resolveOwn resolves a connection without its links.
func resolveOwn(ctx context.Context, q db.Querier, c *models.Connection) (connectors.Resolved, error) {
	sec, err := StoredSecrets(c)
	if err != nil {
		return connectors.Resolved{}, err
	}
	if d, err := connectors.Describe(c.Kind, c.Type); err == nil {
		if missing := MissingSecrets(d.SecretSchema, sec); len(missing) > 0 {
			return connectors.Resolved{}, MissingSecret(c, missing)
		}
	}
	refs, err := RefsOf(sec)
	if err != nil {
		return connectors.Resolved{}, err
	}
	if len(refs) > 0 {
		if sec, err = ResolveRefs(ctx, q, c.ProjectID, sec); err != nil {
			return connectors.Resolved{}, err
		}
	}
	cfg := map[string]any(c.Config)
	if cfg == nil {
		cfg = map[string]any{}
	}
	return connectors.Resolved{Type: c.Type, Provider: c.Provider, Config: cfg, Secrets: sec, Name: c.Name}, nil
}

// RunCheck runs a connector's checks and scrubs secret values from the report.
func RunCheck(ctx context.Context, conn connectors.Resolved, kind string, tables []string) (checks.Report, error) {
	c, err := connectors.Get(kind, conn.Type)
	if err != nil {
		return checks.Report{}, err
	}
	report, err := c.Check(ctx, conn, tables)
	if err != nil {
		return checks.Report{}, err
	}
	return ScrubReport(report, secretValuesOf(conn)), nil
}

// secretValuesOf is every secret a check of the connection may have seen, its links' included.
func secretValuesOf(conn connectors.Resolved) map[string]any {
	values := map[string]any{}
	for k, v := range conn.Secrets {
		values[k] = v
	}
	for field, linked := range conn.Linked {
		for k, v := range linked.Secrets {
			values[field+"."+k] = v
		}
	}
	return values
}

// CheckAndStore runs the checks of a stored connection and records the result.
func CheckAndStore(ctx context.Context, q db.Querier, c *models.Connection, tables []string) (checks.Report, error) {
	var report checks.Report
	resolved, err := Resolve(ctx, q, c)
	if p, ok := IsUnresolved(err); ok {
		report = UnresolvedReport(p)
	} else if err != nil {
		return checks.Report{}, err
	} else if report, err = RunCheck(ctx, resolved, c.Kind, tables); err != nil {
		return checks.Report{}, err
	}
	dump, err := ReportMap(report)
	if err != nil {
		return checks.Report{}, err
	}
	now := db.Now()
	c.LastCheck, c.LastCheckOK, c.LastCheckedAt = dump, &report.OK, &now
	_, err = q.Exec(ctx, `UPDATE connections SET last_check = $1, last_check_ok = $2, last_checked_at = $3 WHERE id = $4`, dump, report.OK, now, c.ID)
	return report, err
}

// Discover lists a source's tables.
func Discover(ctx context.Context, q db.Querier, c *models.Connection) (connectors.Discovery, error) {
	if c.Kind != "source" {
		return connectors.Discovery{}, problem.New(400, "not_a_source", "Only sources have tables to discover.")
	}
	src, err := connectors.GetSource(c.Type)
	if err != nil {
		return connectors.Discovery{}, wrapDiscovery(err)
	}
	resolved, err := Resolve(ctx, q, c)
	if err != nil {
		return connectors.Discovery{}, wrapDiscovery(err)
	}
	d, err := src.Discover(ctx, resolved)
	if err != nil {
		return connectors.Discovery{}, wrapDiscovery(err)
	}
	return d, nil
}

func wrapDiscovery(err error) error {
	return problem.New(502, "discovery_failed", strings.TrimSpace("Could not read the table list: "+pyErrString(err)))
}

// pyErrString is str(exc) of the Python implementation: problems render their detail.
func pyErrString(err error) string {
	var p *problem.Problem
	if errors.As(err, &p) {
		return p.Detail
	}
	return err.Error()
}

// Redact removes every known secret value from text that may reach a user or a log.
func Redact(text string, values map[string]any) string {
	for _, k := range sortedKeys(values) {
		if s, ok := values[k].(string); ok && len([]rune(s)) >= 4 {
			text = strings.ReplaceAll(text, s, "•••")
		}
	}
	return text
}

// ScrubReport redacts secret values from check messages.
func ScrubReport(r checks.Report, values map[string]any) checks.Report {
	for i := range r.Checks {
		r.Checks[i].Message = Redact(r.Checks[i].Message, values)
		r.Checks[i].Technical = Redact(r.Checks[i].Technical, values)
	}
	return r
}

// ReportMap is report.model_dump(mode="json") for storage.
func ReportMap(r checks.Report) (db.Map, error) {
	var m db.Map
	raw, err := jsonMarshal(r)
	if err != nil {
		return nil, err
	}
	if err := m.Scan(raw); err != nil {
		return nil, err
	}
	return m, nil
}
