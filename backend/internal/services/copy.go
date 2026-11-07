package services

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/security"
)

// Copying connections and secret stores to another project. A copy never moves anything: the
// original stays and keeps working. Saved secrets stay behind unless an Admin chooses to copy
// them (server side, still encrypted, never returned); references to a secret store keep
// pointing at a store of the same name, which may be copied along or swapped for one of the
// target's. Both projects' audit logs record the copy.

// CopySecrets says what happens to saved secrets.
const (
	SecretsLeave = "leave" // the target's people enter them (default)
	SecretsCopy  = "copy"  // Admins only
)

// CopyItem is one connection to copy.
type CopyItem struct {
	ID uuid.UUID
	// Name in the target project ("" keeps the name).
	Name string
	// Update replaces the settings of the target's connection of that name instead of creating one.
	Update bool
}

// StoreChoice is what to do about a secret store the target project does not have.
type StoreChoice struct {
	// "copy": copy the store too; "use": point the references at Use, a store of the target.
	Action string
	Use    string
}

// CopyRequest copies connections into Target.
type CopyRequest struct {
	Items   []CopyItem
	Secrets string
	Stores  map[string]StoreChoice
}

// FieldChange is one setting an update changes.
type FieldChange struct {
	Field string `json:"field"`
	From  any    `json:"from"`
	To    any    `json:"to"`
}

// CopyPlanItem is what happens to one connection (the result after a real run).
type CopyPlanItem struct {
	SourceID   uuid.UUID `json:"source_id"`
	SourceName string    `json:"source_name"`
	Kind       string    `json:"kind"`
	Type       string    `json:"type"`
	Name       string    `json:"name"`
	// "create" or "update".
	Action string     `json:"action"`
	ID     *uuid.UUID `json:"id"`
	// A connection with this name already exists in the target.
	Exists bool `json:"exists"`
	// A free name to copy under when it exists.
	SuggestedName string `json:"suggested_name"`
	// Required secrets the copy will lack (the target's people enter them).
	MissingSecrets []string `json:"missing_secrets"`
	// Settings an update changes (secret values are never listed).
	Changes []FieldChange `json:"changes"`
	// Pipelines in the target that use the connection being updated.
	UsedBy []string `json:"used_by"`
	// Why the copy can't go ahead, in plain sentences.
	Problems []string `json:"problems"`
}

// CopyStoreNeed is a secret store a copied connection refers to and the target lacks.
type CopyStoreNeed struct {
	Name   string   `json:"name"`
	UsedBy []string `json:"used_by"`
	// The choice made ("" when none yet), and the target store for "use".
	Action string `json:"action"`
	Use    string `json:"use"`
}

// CopyPlan is the dry run's answer and the real run's result.
type CopyPlan struct {
	TargetProjectID uuid.UUID       `json:"target_project_id"`
	TargetProject   string          `json:"target_project"`
	Secrets         string          `json:"secrets"`
	Items           []CopyPlanItem  `json:"items"`
	Stores          []CopyStoreNeed `json:"stores"`
	// The target's secret stores (to point references at instead).
	TargetStores []string `json:"target_stores"`
	// Nothing in the plan stops the copy.
	Ready bool `json:"ready"`
}

// CanCopySecrets: only Admins copy saved secrets between projects.
func CanCopySecrets(p *auth.Principal) bool {
	return p != nil && p.User != nil && p.User.IsSuperuser
}

func freeName(base string, taken func(string) bool) string {
	for i := 2; i < 1000; i++ {
		suffix := fmt.Sprintf("-%d", i)
		name := base
		if len(name)+len(suffix) > 63 {
			name = strings.TrimRight(name[:63-len(suffix)], "-")
		}
		if !taken(name + suffix) {
			return name + suffix
		}
	}
	return ""
}

func typeAllowed(allowed []string, typ string) bool {
	return len(allowed) == 0 || slices.Contains(allowed, typ)
}

func canonical(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func configChanges(from, to map[string]any) []FieldChange {
	keys := map[string]bool{}
	for k := range from {
		keys[k] = true
	}
	for k := range to {
		keys[k] = true
	}
	names := make([]string, 0, len(keys))
	for k := range keys {
		names = append(names, k)
	}
	sort.Strings(names)
	out := []FieldChange{}
	for _, k := range names {
		if canonical(from[k]) != canonical(to[k]) {
			out = append(out, FieldChange{Field: k, From: from[k], To: to[k]})
		}
	}
	return out
}

// CopyConnections plans (and, unless the caller rolls the transaction back, performs) a copy of
// connections from src into dst. The caller has checked connection.manage in both projects, and
// that only Admins copy secrets or stores.
func CopyConnections(ctx context.Context, q db.Querier, src, dst *auth.ProjectAccess, req CopyRequest) (CopyPlan, error) {
	if req.Secrets == "" {
		req.Secrets = SecretsLeave
	}
	plan := CopyPlan{TargetProjectID: dst.Project.ID, TargetProject: dst.Project.Name, Secrets: req.Secrets, Items: []CopyPlanItem{}, Stores: []CopyStoreNeed{}, TargetStores: []string{}}

	targetConns, err := db.Select[models.Connection](ctx, q, `SELECT * FROM connections WHERE project_id = $1`, dst.Project.ID)
	if err != nil {
		return plan, err
	}
	byName := map[string]*models.Connection{}
	for i := range targetConns {
		byName[targetConns[i].Name] = &targetConns[i]
	}
	stores, err := db.Select[models.SecretStore](ctx, q, `SELECT * FROM secret_stores WHERE project_id = $1 ORDER BY name`, dst.Project.ID)
	if err != nil {
		return plan, err
	}
	targetStores := map[string]bool{}
	for _, s := range stores {
		targetStores[s.Name] = true
		plan.TargetStores = append(plan.TargetStores, s.Name)
	}
	pol, err := PoliciesOf(ctx, q, dst.Project.ID)
	if err != nil {
		return plan, err
	}

	type work struct {
		src     *models.Connection
		desc    *connectors.Descriptor
		config  map[string]any
		secrets map[string]any
		target  *models.Connection
		links   map[string]string // field → linked connection's name in the target
	}
	seen := map[uuid.UUID]bool{}
	planned := map[string]bool{} // names the copy will take in the target
	var jobs []*work
	sourceIDs := map[uuid.UUID]string{} // copied source id → its name in the target
	needs := map[string]*CopyStoreNeed{}
	for _, it := range req.Items {
		if seen[it.ID] {
			continue
		}
		seen[it.ID] = true
		c, err := db.Find[models.Connection](ctx, q, `SELECT * FROM connections WHERE project_id = $1 AND id = $2`, src.Project.ID, it.ID)
		if err != nil {
			return plan, err
		}
		if c == nil {
			return plan, problem.NotFound("connection")
		}
		d, err := connectors.Describe(c.Kind, c.Type)
		if err != nil {
			return plan, err
		}
		name := it.Name
		if name == "" {
			name = c.Name
		}
		item := CopyPlanItem{SourceID: c.ID, SourceName: c.Name, Kind: c.Kind, Type: c.Type, Name: name, Action: "create",
			MissingSecrets: []string{}, Changes: []FieldChange{}, UsedBy: []string{}, Problems: []string{}}
		existing := byName[name]
		if existing != nil || planned[name] {
			item.Exists = true
			item.SuggestedName = freeName(name, func(n string) bool { return byName[n] != nil || planned[n] })
		}
		if planned[name] {
			item.Problems = append(item.Problems, fmt.Sprintf("Two of the copies would be named '%s'.", name))
		}
		planned[name] = true
		if existing != nil {
			switch {
			case !it.Update:
				item.Problems = append(item.Problems, fmt.Sprintf("%s already has a connection named '%s'. Copy it under another name, or update the existing one.", dst.Project.Name, name))
			case existing.Kind != c.Kind || existing.Type != c.Type:
				item.Problems = append(item.Problems, fmt.Sprintf("'%s' in %s is a %s %s, so it can't take the settings of a %s %s.", name, dst.Project.Name, existing.Type, existing.Kind, c.Type, c.Kind))
			case existing.Managed:
				item.Problems = append(item.Problems, fmt.Sprintf("'%s' in %s is managed by BeziSync and can't be overwritten.", name, dst.Project.Name))
			default:
				item.Action = "update"
				id := existing.ID
				item.ID = &id
				if item.UsedBy, err = ConnectionUsage(ctx, q, existing.ID); err != nil {
					return plan, err
				}
			}
		}
		allowed := pol.AllowedSourceTypes
		if c.Kind == "sink" {
			allowed = pol.AllowedSinkTypes
		}
		if pol.Unreadable {
			item.Problems = append(item.Problems, fmt.Sprintf("%s's policies can't be read; an Admin must fix them in Settings → Policies first.", dst.Project.Name))
		} else if !typeAllowed(allowed, c.Type) {
			what := "sources"
			if c.Kind == "sink" {
				what = "destinations"
			}
			item.Problems = append(item.Problems, fmt.Sprintf("%s's policies don't allow %s %s (Settings → Policies).", dst.Project.Name, c.Type, what))
		}

		stored, err := StoredSecrets(c)
		if err != nil {
			return plan, err
		}
		refs, err := RefsOf(stored)
		if err != nil {
			return plan, err
		}
		secrets := map[string]any{}
		if item.Action == "update" && req.Secrets == SecretsLeave {
			// Updating keeps the target's own secrets; references follow the copied settings.
			if secrets, err = StoredSecrets(existing); err != nil {
				return plan, err
			}
			for k := range secrets {
				if _, isRef := refs[k]; isRef {
					delete(secrets, k)
				}
			}
		}
		for k, v := range stored {
			r, isRef := refs[k]
			if !isRef {
				if req.Secrets == SecretsCopy {
					secrets[k] = v
				}
				continue
			}
			if targetStores[r.Store] {
				secrets[k] = r.String()
				continue
			}
			need := needs[r.Store]
			if need == nil {
				ch := req.Stores[r.Store]
				need = &CopyStoreNeed{Name: r.Store, UsedBy: []string{}, Action: ch.Action, Use: ch.Use}
				needs[r.Store] = need
			}
			if !slices.Contains(need.UsedBy, c.Name) {
				need.UsedBy = append(need.UsedBy, c.Name)
			}
			switch need.Action {
			case "copy":
				secrets[k] = r.String()
			case "use":
				if !targetStores[need.Use] {
					item.Problems = append(item.Problems, fmt.Sprintf("%s has no secret store named '%s'.", dst.Project.Name, need.Use))
					continue
				}
				moved := r
				moved.Store = need.Use
				secrets[k] = moved.String()
			default:
				item.Problems = append(item.Problems, fmt.Sprintf("Its %s comes from the secret store '%s', which %s doesn't have: copy the store too, or use one of %s's.", k, r.Store, dst.Project.Name, dst.Project.Name))
			}
		}
		item.MissingSecrets = MissingSecrets(d.SecretSchema, secrets)

		config := map[string]any{}
		for k, v := range c.Config {
			config[k] = v
		}
		w := &work{src: c, desc: d, config: config, secrets: secrets, target: existing, links: map[string]string{}}
		for _, field := range sortedLinkFields(d) {
			value := strings.TrimSpace(connectors.Str(config, field))
			if value == "" {
				continue
			}
			linked, err := findLinked(ctx, q, src.Project.ID, value)
			if err != nil {
				return plan, err
			}
			if linked == nil {
				item.Problems = append(item.Problems, fmt.Sprintf("Its linked connection in field '%s' no longer exists.", field))
				continue
			}
			w.links[field] = linked.Name // replaced with the copy's name below, if it is copied too
		}
		if item.Action == "update" {
			item.Changes = configChanges(existing.Config, config)
		}
		sourceIDs[c.ID] = name
		plan.Items = append(plan.Items, item)
		jobs = append(jobs, w)
	}

	// Links: to a connection copied in the same request (under its new name) or to one of the
	// target's with the linked connection's name.
	copiedNames := map[string]string{}
	for i, w := range jobs {
		copiedNames[w.src.Name] = plan.Items[i].Name
	}
	for i, w := range jobs {
		for field, linkedName := range w.links {
			if n, ok := copiedNames[linkedName]; ok {
				w.links[field] = n
				continue
			}
			if byName[linkedName] == nil {
				plan.Items[i].Problems = append(plan.Items[i].Problems,
					fmt.Sprintf("It uses '%s' (%s), which %s doesn't have: copy it in the same go.", linkedName, field, dst.Project.Name))
			}
		}
	}

	// Stores to copy along.
	names := make([]string, 0, len(needs))
	for n := range needs {
		names = append(names, n)
	}
	sort.Strings(names)
	var storeJobs []*models.SecretStore
	for _, n := range names {
		need := needs[n]
		sort.Strings(need.UsedBy)
		if need.Action == "copy" {
			s, err := db.Find[models.SecretStore](ctx, q, `SELECT * FROM secret_stores WHERE project_id = $1 AND name = $2`, src.Project.ID, n)
			if err != nil {
				return plan, err
			}
			if s == nil {
				for i := range plan.Items {
					if slices.Contains(need.UsedBy, plan.Items[i].SourceName) {
						plan.Items[i].Problems = append(plan.Items[i].Problems, fmt.Sprintf("The secret store '%s' doesn't exist in this project either.", n))
					}
				}
			} else {
				storeJobs = append(storeJobs, s)
			}
		}
		plan.Stores = append(plan.Stores, *need)
	}

	plan.Ready = len(plan.Items) > 0
	for _, it := range plan.Items {
		if len(it.Problems) > 0 {
			plan.Ready = false
		}
	}
	if !plan.Ready {
		return plan, nil
	}

	// Perform: stores first (references need them), then connections that others link to.
	for _, s := range storeJobs {
		if _, _, err := CopyStore(ctx, q, src, dst, s, req.Secrets); err != nil {
			return plan, err
		}
	}
	sort.SliceStable(jobs, func(i, j int) bool { return len(jobs[i].links) < len(jobs[j].links) })
	index := map[uuid.UUID]int{}
	for i := range plan.Items {
		index[plan.Items[i].SourceID] = i
	}
	for _, w := range jobs {
		item := &plan.Items[index[w.src.ID]]
		for field, name := range w.links {
			w.config[field] = name
		}
		cfg, sec, err := ValidateModel(w.desc.ConfigSchema, WithoutRequired(w.desc.SecretSchema), w.config, w.secrets)
		if err != nil {
			return plan, err
		}
		if err := NormalizeLinks(ctx, q, dst.Project.ID, w.src.Kind, w.src.Type, cfg); err != nil {
			return plan, err
		}
		if err := ValidateRefs(ctx, q, dst.Project.ID, sec); err != nil {
			return plan, err
		}
		enc, err := EncryptOrNil(sec)
		if err != nil {
			return plan, err
		}
		now := db.Now()
		secretsNote := "without its saved secrets"
		if req.Secrets == SecretsCopy {
			secretsNote = "with its saved secrets"
		}
		var target *models.Connection
		if item.Action == "update" {
			target = w.target
			target.Config, target.SecretsEnc, target.Provider, target.Description = db.Map(cfg), enc, w.src.Provider, w.src.Description
			target.LastCheck, target.LastCheckOK, target.LastCheckedAt = nil, nil, nil
			target.Version++
			target.UpdatedAt = now
			if err := db.Update(ctx, q, "connections", target); err != nil {
				return plan, err
			}
		} else {
			target = &models.Connection{ID: db.NewID(), ProjectID: dst.Project.ID, Kind: w.src.Kind, Type: w.src.Type, Name: item.Name,
				Description: w.src.Description, Provider: w.src.Provider, Config: db.Map(cfg), SecretsEnc: enc, Version: 1,
				CreatedByID: &src.Principal.User.ID, CreatedAt: now, UpdatedAt: now}
			if err := db.Insert(ctx, q, "connections", target); err != nil {
				if db.IsUniqueViolation(err) {
					return plan, problem.Conflict("connection_exists", fmt.Sprintf("A connection named '%s' already exists in %s.", item.Name, dst.Project.Name), nil)
				}
				return plan, err
			}
		}
		id := target.ID
		item.ID = &id
		verb := map[string]string{"create": "Copied", "update": "Updated"}[item.Action]
		if _, err := Record(ctx, q, AuditEvent{Kind: "connection.copied", ProjectID: &dst.Project.ID, Principal: src.Principal,
			Summary:      fmt.Sprintf("%s %s %s from %s, %s", verb, w.src.Kind, item.Name, src.Project.Name, secretsNote),
			ResourceType: "connection", ResourceID: &target.ID,
			Details: map[string]any{"from_project": src.Project.Name, "from_connection": w.src.Name, "secrets": req.Secrets, "action": item.Action, "secret_fields": sortedFieldNames(sec)}}); err != nil {
			return plan, err
		}
		if _, err := Record(ctx, q, AuditEvent{Kind: "connection.copied", ProjectID: &src.Project.ID, Principal: src.Principal,
			Summary:      fmt.Sprintf("Copied %s %s to %s as %s, %s", w.src.Kind, w.src.Name, dst.Project.Name, item.Name, secretsNote),
			ResourceType: "connection", ResourceID: &w.src.ID,
			Details: map[string]any{"to_project": dst.Project.Name, "to_connection": item.Name, "secrets": req.Secrets, "action": item.Action}}); err != nil {
			return plan, err
		}
	}
	return plan, nil
}

func sortedFieldNames(m map[string]any) []any {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]any, len(keys))
	for i, k := range keys {
		out[i] = k
	}
	return out
}

// CopyStore copies a secret store into dst under the same name; when dst already has one, its
// settings are replaced and its own credentials kept (unless secrets are copied). It returns the
// stored row and "create" or "update". The caller has checked project.manage in both projects.
func CopyStore(ctx context.Context, q db.Querier, src, dst *auth.ProjectAccess, s *models.SecretStore, secrets string) (*models.SecretStore, string, error) {
	existing, err := findStore(ctx, q, dst.Project.ID, s.Name)
	if err != nil {
		return nil, "", err
	}
	if existing != nil && existing.Kind != s.Kind {
		return nil, "", problem.New(409, "store_kind_differs", fmt.Sprintf("%s already has a secret store named '%s' of another kind.", dst.Project.Name, s.Name))
	}
	now := db.Now()
	var enc *string
	var secretsAt *time.Time
	if secrets == SecretsCopy && s.SecretsEnc != nil && *s.SecretsEnc != "" {
		values, err := security.DecryptSecret(*s.SecretsEnc)
		if err != nil {
			return nil, "", err
		}
		if enc, err = EncryptOrNil(values); err != nil {
			return nil, "", err
		}
		secretsAt = &now
	}
	action := "create"
	note := "without its credentials"
	if secrets == SecretsCopy {
		note = "with its credentials"
	}
	out := existing
	if existing != nil {
		action = "update"
		existing.Config = s.Config
		if secrets == SecretsCopy {
			existing.SecretsEnc, existing.SecretsUpdatedAt = enc, secretsAt
		}
		existing.LastTest = nil
		existing.Version++
		existing.UpdatedAt = now
		ForgetStore(existing.ID)
		if err := db.Update(ctx, q, "secret_stores", existing); err != nil {
			return nil, "", err
		}
	} else {
		out = &models.SecretStore{ID: db.NewID(), ProjectID: dst.Project.ID, Name: s.Name, Kind: s.Kind, Config: s.Config, SecretsEnc: enc,
			SecretsUpdatedAt: secretsAt, Version: 1, CreatedAt: now, UpdatedAt: now}
		if err := db.Insert(ctx, q, "secret_stores", out); err != nil {
			return nil, "", err
		}
	}
	verb := map[string]string{"create": "Copied", "update": "Updated"}[action]
	if _, err := Record(ctx, q, AuditEvent{Kind: "secret_store.copied", ProjectID: &dst.Project.ID, Principal: src.Principal,
		Summary:      fmt.Sprintf("%s secret store %s from %s, %s", verb, s.Name, src.Project.Name, note),
		ResourceType: "secret_store", ResourceID: &out.ID, Details: map[string]any{"from_project": src.Project.Name, "secrets": secrets, "action": action}}); err != nil {
		return nil, "", err
	}
	if _, err := Record(ctx, q, AuditEvent{Kind: "secret_store.copied", ProjectID: &src.Project.ID, Principal: src.Principal,
		Summary:      fmt.Sprintf("Copied secret store %s to %s, %s", s.Name, dst.Project.Name, note),
		ResourceType: "secret_store", ResourceID: &s.ID, Details: map[string]any{"to_project": dst.Project.Name, "secrets": secrets, "action": action}}); err != nil {
		return nil, "", err
	}
	return out, action, nil
}
