package services

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/oidc"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

// Map an SSO identity to a BeziSync user and its project roles.

var roleRank = map[string]int{models.RoleViewer: 0, models.RoleOperator: 1, models.RoleEditor: 2}

// legacyAdmin: older rules naming project "admin" grant editor with the approver right.
const legacyAdmin = "admin"

func ruleRole(value string) (string, bool, bool) {
	if value == legacyAdmin {
		return models.RoleEditor, true, true
	}
	if _, ok := roleRank[value]; ok {
		return value, false, true
	}
	return "", false, false
}

// Access is one project grant from SSO rules.
type Access struct {
	Role     string
	Approver bool
}

func ruleStr(rule map[string]any, key, def string) string {
	v, ok := rule[key]
	if !ok {
		return def
	}
	if v == nil {
		return "None"
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

func ruleTruthy(rule map[string]any, key string) bool {
	return truthyAny(rule[key])
}

// DesiredAccess computes per-project grants from group rules (highest role wins) and
// whether the user is an Admin; with no grant the default role applies.
func DesiredAccess(rules []map[string]any, groups []string, projects map[string]*models.Project, projectOrder []string, defaultRole string, defaultProjects []string) (map[string]Access, []string, bool) {
	access := map[string]Access{}
	var order []string
	superuser := false
	inGroups := func(g any) bool {
		s, ok := g.(string)
		if !ok {
			return false
		}
		for _, x := range groups {
			if x == s {
				return true
			}
		}
		return false
	}
	for _, rule := range rules {
		if !inGroups(rule["group"]) {
			continue
		}
		if ruleTruthy(rule, "superuser") {
			superuser = true
			continue
		}
		role, legacyApprover, ok := ruleRole(ruleStr(rule, "role", "viewer"))
		if !ok {
			continue
		}
		targets := []string{ruleStr(rule, "project", "None")}
		if rule["project"] == "*" {
			targets = projectOrder
		}
		for _, slug := range targets {
			if _, ok := projects[slug]; !ok {
				continue
			}
			current, has := access[slug]
			best := role
			if has && roleRank[role] <= roleRank[current.Role] {
				best = current.Role
			}
			approver := ruleTruthy(rule, "approver") || legacyApprover || (has && current.Approver)
			if !has {
				order = append(order, slug)
			}
			access[slug] = Access{best, approver}
		}
	}
	if len(access) == 0 && defaultRole != "" {
		for _, slug := range defaultProjects {
			if _, ok := projects[slug]; ok {
				role, approver, ok := ruleRole(defaultRole)
				if !ok {
					continue
				}
				if _, has := access[slug]; !has {
					order = append(order, slug)
				}
				access[slug] = Access{role, approver}
			}
		}
	}
	return access, order, superuser
}

// SyncSSOUser creates or updates the user of an identity (signed in with provider p) and its
// SSO-granted memberships. A person already bound to another identity is refused unless the
// provider moves such people (link_existing): the Admin's explicit choice when switching providers.
func SyncSSOUser(ctx context.Context, q db.Querier, id *oidc.Identity, p *models.SSOProvider) (*models.User, error) {
	rules, defaultProjects := RoleRules(p), DefaultProjects(p)
	defaultRole := ""
	if p.DefaultRole != nil {
		defaultRole = *p.DefaultRole
	}
	user, err := db.Find[models.User](ctx, q, `SELECT * FROM users WHERE sso_subject = $1`, id.Subject)
	if err != nil {
		return nil, err
	}
	var movedFrom string
	if user == nil {
		if user, err = db.Find[models.User](ctx, q, `SELECT * FROM users WHERE lower(email) = $1`, id.Email); err != nil {
			return nil, err
		}
		// Linking to an account someone already has needs an address the provider vouched for:
		// otherwise anyone who can type that address at the provider would take the account over.
		if user != nil && !id.EmailVerified {
			return nil, &oidc.Error{Msg: "An account with this email already exists, and " + p.Label +
				" did not verify the address, so BeziSync won't sign you in to it. Ask an Admin."}
		}
		if user != nil && user.SSOSubject != nil && *user.SSOSubject != id.Subject {
			if !cfgBool(p, "link_existing") {
				return nil, &oidc.Error{Msg: "This email already signs in with another sign-in option. Ask an Admin to let " + p.Label +
					" take over such accounts (Settings → Sign-in)."}
			}
			movedFrom = *user.SSOSubject
		}
	}
	created := user == nil
	now := db.Now()
	if user == nil {
		user = &models.User{ID: db.NewID(), Email: id.Email, Name: id.Name, IsActive: true, UIMode: "guided",
			MutedNotifications: db.List{}, CreatedAt: now, UpdatedAt: now}
	}
	if !user.IsActive {
		return nil, &oidc.Error{Msg: "Your BeziSync account is disabled."}
	}
	subject := id.Subject
	user.SSOSubject = &subject
	if id.Name != "" && user.Name == "" {
		user.Name = id.Name
	}
	projects := map[string]*models.Project{}
	all, err := db.Select[models.Project](ctx, q, `SELECT * FROM projects ORDER BY created_at, id`)
	if err != nil {
		return nil, err
	}
	var slugs []string
	for i := range all {
		projects[all[i].Slug] = &all[i]
		slugs = append(slugs, all[i].Slug)
	}
	access, order, superuser := DesiredAccess(rules, id.Groups, projects, slugs, defaultRole, defaultProjects)
	for _, r := range rules {
		if ruleTruthy(r, "superuser") {
			user.IsSuperuser = superuser // superuser status follows the rule when one exists
			break
		}
	}
	user.LastLoginAt = &now
	user.UpdatedAt = now
	if created {
		if err := db.Insert(ctx, q, "users", user); err != nil {
			return nil, err
		}
	} else if err := db.Update(ctx, q, "users", user); err != nil {
		return nil, err
	}
	members, err := db.Select[models.Membership](ctx, q, `SELECT * FROM memberships WHERE user_id = $1`, user.ID)
	if err != nil {
		return nil, err
	}
	existing := map[uuid.UUID]*models.Membership{}
	for i := range members {
		existing[members[i].ProjectID] = &members[i]
	}
	for _, slug := range order {
		a := access[slug]
		project := projects[slug]
		m, ok := existing[project.ID]
		delete(existing, project.ID)
		switch {
		case !ok:
			if err := db.Insert(ctx, q, "memberships", &models.Membership{ID: db.NewID(), ProjectID: project.ID, UserID: user.ID,
				Role: a.Role, CanApprove: a.Approver, GrantedBySSO: true, CreatedAt: now, UpdatedAt: now}); err != nil {
				return nil, err
			}
		case m.GrantedBySSO:
			m.Role, m.CanApprove, m.UpdatedAt = a.Role, a.Approver, now
			if err := db.Update(ctx, q, "memberships", m); err != nil {
				return nil, err
			}
		}
	}
	for _, m := range existing {
		if m.GrantedBySSO { // the group that granted it is gone
			if err := db.Delete(ctx, q, "memberships", m.ID); err != nil {
				return nil, err
			}
		}
	}
	verb := "Signed in"
	if created {
		verb = "Created and signed in"
	}
	if movedFrom != "" {
		if _, err := Record(ctx, q, AuditEvent{Kind: "user.sso_moved", Summary: fmt.Sprintf("%s now signs in with %s", user.Email, p.Label),
			Principal: &auth.Principal{User: user}, ResourceType: "user", ResourceID: &user.ID,
			Details: map[string]any{"from": movedFrom, "to": id.Subject}}); err != nil {
			return nil, err
		}
	}
	groups := make([]any, len(id.Groups))
	for i, g := range id.Groups {
		groups[i] = g
	}
	_, err = Record(ctx, q, AuditEvent{Kind: "user.sso_login", Summary: fmt.Sprintf("%s %s via single sign-on", verb, user.Email),
		Principal: &auth.Principal{User: user}, ResourceType: "user", ResourceID: &user.ID, Details: map[string]any{"groups": groups}})
	return user, err
}

// SSOProviders lists the configured identity providers in the order they were added.
func SSOProviders(ctx context.Context, q db.Querier, enabledOnly bool) ([]models.SSOProvider, error) {
	where := ""
	if enabledOnly {
		where = "WHERE enabled "
	}
	return db.Select[models.SSOProvider](ctx, q, `SELECT * FROM sso_providers `+where+`ORDER BY created_at, id`)
}

// EnabledSSOProvider loads a provider that people may sign in with (nil when missing or off).
func EnabledSSOProvider(ctx context.Context, q db.Querier, id string) (*models.SSOProvider, error) {
	pid, err := uuid.Parse(id)
	if err != nil {
		return nil, nil //nolint:nilerr // a malformed id is simply not a provider
	}
	return db.Find[models.SSOProvider](ctx, q, `SELECT * FROM sso_providers WHERE id = $1 AND enabled`, pid)
}

// RoleRules are a provider's group rules in the shape DesiredAccess reads.
func RoleRules(m *models.SSOProvider) []map[string]any {
	out := make([]map[string]any, 0, len(m.RoleRules))
	for _, x := range m.RoleRules {
		if r, ok := x.(map[string]any); ok {
			out = append(out, r)
		}
	}
	return out
}

// DefaultProjects are a provider's default project slugs.
func DefaultProjects(m *models.SSOProvider) []string {
	out := make([]string, 0, len(m.DefaultProjects))
	for _, x := range m.DefaultProjects {
		out = append(out, fmt.Sprint(x))
	}
	return out
}

// RequireSSO fails when no provider can be used.
func RequireSSO(p *models.SSOProvider) error {
	if p == nil {
		return problem.New(404, "sso_disabled", "This sign-in option is not available.")
	}
	return nil
}

// authStateKey holds the account-wide sign-in settings in instance_state.
const authStateKey = "auth"

// PasswordLogin is whether email and password sign-in is wanted and whether it is in force:
// it stays in force while no identity provider is turned on, so nobody can be locked out.
func PasswordLogin(ctx context.Context, q db.Querier) (wanted, effective bool, err error) {
	row, err := db.Find[models.InstanceState](ctx, q, `SELECT * FROM instance_state WHERE key = $1`, authStateKey)
	if err != nil {
		return false, false, err
	}
	wanted = true
	if row != nil {
		if v, ok := row.Value["password_login"].(bool); ok {
			wanted = v
		}
	}
	if wanted {
		return true, true, nil
	}
	n, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM sso_providers WHERE enabled`)
	return false, n == 0, err
}

// SetPasswordLogin turns email and password sign-in on or off. Turning it off needs a working
// provider and an active Admin who has signed in with single sign-on.
func SetPasswordLogin(ctx context.Context, q db.Querier, on bool, p *auth.Principal) error {
	if !on {
		working, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM sso_providers WHERE enabled AND (last_test->>'ok')::boolean`)
		if err != nil {
			return err
		}
		if working == 0 {
			return problem.Conflict("sso_not_ready", "Turn on a sign-in provider that passed its test first, so people can still sign in.", nil)
		}
		admins, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM users WHERE is_superuser AND is_active AND sso_subject IS NOT NULL`)
		if err != nil {
			return err
		}
		if admins == 0 {
			return problem.Conflict("no_sso_admin", "No Admin has signed in with single sign-on yet. Sign in with a provider as an Admin first, so someone can still manage BeziSync.", nil)
		}
	}
	row, err := instanceState(ctx, q, authStateKey)
	if err != nil {
		return err
	}
	row.Value["password_login"] = on
	if err := saveInstanceState(ctx, q, row); err != nil {
		return err
	}
	summary := "Turned off sign-in with email and password"
	if on {
		summary = "Turned on sign-in with email and password"
	}
	_, err = Record(ctx, q, AuditEvent{Kind: "auth.settings_updated", Summary: summary, Principal: p, Details: map[string]any{"password_login": on}})
	return err
}

// GuardLastProvider refuses turning off or deleting the last enabled provider while password
// sign-in is off: nobody could sign in afterwards.
func GuardLastProvider(ctx context.Context, q db.Querier, id uuid.UUID) error {
	wanted, _, err := PasswordLogin(ctx, q)
	if err != nil || wanted {
		return err
	}
	others, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM sso_providers WHERE enabled AND id <> $1`, id)
	if err != nil {
		return err
	}
	if others == 0 {
		return problem.Conflict("last_sign_in_option", "This is the only way to sign in while email and password sign-in is off. Turn password sign-in on first.", nil)
	}
	return nil
}
