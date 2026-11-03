package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/oidc"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/samlsp"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// Settings → Sign-in: identity providers and whether email and password sign-in is allowed.

// SSORuleRead is one group rule: members of the group get a role in a project (or every
// project), or become Admins.
type SSORuleRead struct {
	Group    string  `json:"group"`
	Admin    bool    `json:"admin"`
	Project  *string `json:"project"`
	Role     *string `json:"role"`
	Approver bool    `json:"approver"`
}

// SSOProviderRead is an identity provider; its client secret is never returned.
type SSOProviderRead struct {
	ID                   uuid.UUID     `json:"id"`
	Label                string        `json:"label"`
	Kind                 string        `json:"kind"`
	Issuer               string        `json:"issuer"`
	ClientID             string        `json:"client_id"`
	Scopes               []string      `json:"scopes"`
	GroupsClaim          string        `json:"groups_claim"`
	AllowUnverifiedEmail bool          `json:"allow_unverified_email"`
	RoleRules            []SSORuleRead `json:"role_rules"`
	DefaultRole          *string       `json:"default_role"`
	DefaultProjects      []string      `json:"default_projects"`
	Enabled              bool          `json:"enabled"`
	LastTest             *StoreTest    `json:"last_test"`
	Version              int           `json:"version"`
	CreatedAt            pytime.Time   `json:"created_at"`
	UpdatedAt            pytime.Time   `json:"updated_at"`
	OIDC                 *OIDCRead     `json:"oidc"`
	OAuth2               *OAuth2Read   `json:"oauth2"`
	SAML                 *SAMLRead     `json:"saml"`
	// LinkExisting: people bound to another sign-in option with the same email move to this one.
	LinkExisting bool `json:"link_existing"`
}

// OIDCRead names the ID token claims an OpenID Connect provider is read with ("" = standard).
type OIDCRead struct {
	EmailClaim         string `json:"email_claim"`
	EmailVerifiedClaim string `json:"email_verified_claim"`
	NameClaim          string `json:"name_claim"`
}

// OAuth2Read is what an OAuth2 provider uses beyond the common fields.
type OAuth2Read struct {
	Flavor             string `json:"flavor"`
	AuthorizationURL   string `json:"authorization_url"`
	TokenURL           string `json:"token_url"`
	UserinfoURL        string `json:"userinfo_url"`
	SubjectClaim       string `json:"subject_claim"`
	EmailClaim         string `json:"email_claim"`
	EmailVerifiedClaim string `json:"email_verified_claim"`
	NameClaim          string `json:"name_claim"`
	SecretInBody       bool   `json:"secret_in_body"`
	ResponseIssuer     string `json:"response_issuer"`
}

// SAMLRead is a SAML provider's identity provider.
type SAMLRead struct {
	IdPEntityID       string     `json:"idp_entity_id"`
	SSOURL            string     `json:"sso_url"`
	MetadataURL       string     `json:"metadata_url"`
	Certificates      []certInfo `json:"certificates"`
	SignRequests      bool       `json:"sign_requests"`
	AllowIdPInitiated bool       `json:"allow_idp_initiated"`
	EmailAttribute    string     `json:"email_attribute"`
	NameAttribute     string     `json:"name_attribute"`
	GroupsAttribute   string     `json:"groups_attribute"`
}

// AuthSettingsRead is the account-wide sign-in configuration.
type AuthSettingsRead struct {
	PasswordLogin bool `json:"password_login"`
	// PasswordLoginInForce is false only while a provider is turned on to replace it.
	PasswordLoginInForce bool   `json:"password_login_in_force"`
	RedirectURI          string `json:"redirect_uri"`
	SSOAdmins            int64  `json:"sso_admins"`
	// What SAML identity providers need to know about BeziSync.
	SAMLEntityID    string `json:"saml_entity_id"`
	SAMLACSURL      string `json:"saml_acs_url"`
	SAMLMetadataURL string `json:"saml_metadata_url"`
}

func ssoProviderRead(m *models.SSOProvider) (SSOProviderRead, error) {
	out := SSOProviderRead{ID: m.ID, Label: m.Label, Kind: m.Kind, Issuer: m.Issuer, ClientID: m.ClientID, Scopes: []string{}, GroupsClaim: m.GroupsClaim,
		AllowUnverifiedEmail: m.AllowUnverifiedEmail, RoleRules: []SSORuleRead{}, DefaultRole: m.DefaultRole,
		DefaultProjects: services.DefaultProjects(m), Enabled: m.Enabled, Version: m.Version,
		CreatedAt: pytime.T(m.CreatedAt), UpdatedAt: pytime.T(m.UpdatedAt)}
	for _, s := range m.Scopes {
		out.Scopes = append(out.Scopes, fmt.Sprint(s))
	}
	for _, r := range services.RoleRules(m) {
		rule := SSORuleRead{Group: fmt.Sprint(r["group"])}
		rule.Admin, _ = r["superuser"].(bool)
		rule.Approver, _ = r["approver"].(bool)
		if p, ok := r["project"].(string); ok {
			rule.Project = &p
		}
		if role, ok := r["role"].(string); ok {
			rule.Role = &role
		}
		out.RoleRules = append(out.RoleRules, rule)
	}
	cs := func(k string) string { v, _ := m.Config[k].(string); return v }
	cb := func(k string) bool { v, _ := m.Config[k].(bool); return v }
	out.LinkExisting = cb("link_existing")
	switch m.Kind {
	case oidc.KindOIDC:
		out.OIDC = &OIDCRead{EmailClaim: cs("email_claim"), EmailVerifiedClaim: cs("email_verified_claim"), NameClaim: cs("name_claim")}
	case oidc.KindOAuth2:
		out.OAuth2 = &OAuth2Read{Flavor: cs("flavor"), AuthorizationURL: cs("authorization_url"), TokenURL: cs("token_url"), UserinfoURL: cs("userinfo_url"),
			SubjectClaim: cs("subject_claim"), EmailClaim: cs("email_claim"), EmailVerifiedClaim: cs("email_verified_claim"), NameClaim: cs("name_claim"),
			SecretInBody: cb("secret_in_body"), ResponseIssuer: cs("response_issuer")}
	case oidc.KindSAML:
		out.SAML = &SAMLRead{IdPEntityID: cs("idp_entity_id"), SSOURL: cs("sso_url"), MetadataURL: cs("metadata_url"), Certificates: certInfos(m),
			SignRequests: cb("sign_requests"), AllowIdPInitiated: cb("allow_idp_initiated"), EmailAttribute: cs("email_attribute"),
			NameAttribute: cs("name_attribute"), GroupsAttribute: cs("groups_attribute")}
	}
	if len(m.LastTest) > 0 {
		ok, _ := m.LastTest["ok"].(bool)
		msg, _ := m.LastTest["message"].(string)
		at, _ := m.LastTest["tested_at"].(string)
		t, err := pytime.Parse(at)
		if err != nil {
			return out, err
		}
		out.LastTest = &StoreTest{OK: ok, Message: msg, TestedAt: pytime.T(t)}
	}
	return out, nil
}

func testSSOProvider(ctx context.Context, issuer string) StoreTest {
	now := pytime.T(db.Now())
	msg, err := oidc.Test(ctx, issuer)
	if err != nil {
		var oe *oidc.Error
		if errors.As(err, &oe) {
			return StoreTest{Message: oe.Msg, TestedAt: now}
		}
		return StoreTest{Message: "The provider could not be checked.", TestedAt: now}
	}
	return StoreTest{OK: true, Message: msg, TestedAt: now}
}

// scopeRe is one OAuth scope token (RFC 6749 §3.3).
var scopeRe = regexp.MustCompile(`^[\x21\x23-\x5b\x5d-\x7e]{1,100}$`)

var projectRoles = []string{models.RoleViewer, models.RoleOperator, models.RoleEditor}

// ssoInput is a provider as sent; nil fields were not sent (PATCH).
type ssoInput struct {
	Label, Issuer, ClientID, ClientSecret, GroupsClaim *string
	Scopes, DefaultProjects                            []string
	AllowUnverifiedEmail, Enabled                      *bool
	RoleRules                                          []map[string]any
	HasRules, HasDefaultRole                           bool
	DefaultRole                                        *string
}

func issuerURL(v *val.V, f val.Field) *string {
	s, ok := v.Constrained(f.Loc, f.Raw, val.StrRules{Min: 1, Max: 500})
	if !ok {
		return nil
	}
	s = strings.TrimRight(strings.TrimSpace(s), "/")
	u, err := url.Parse(s)
	if err != nil || u.Host == "" || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && u.Scheme != "http") {
		v.ValueError(f.Loc, "enter the provider's issuer URL, such as https://accounts.google.com")
		return nil
	}
	if s0 := config.Get(); u.Scheme == "http" && s0.Environment == "production" && !s0.AllowPlainHTTP {
		v.ValueError(f.Loc, "the issuer must use https")
		return nil
	}
	return &s
}

// ssoBody is a provider as sent: the common fields and the kind-specific ones.
type ssoBody struct {
	ssoInput
	kind kindInput
}

func parseSSO(v *val.V, o *val.Object, create bool) ssoBody {
	in := ssoInput{}
	if f := o.Get("label"); f.Present || create {
		if f.Required() {
			if s, ok := v.Constrained(f.Loc, f.Raw, val.StrRules{Min: 1, Max: 60}); ok {
				s = strings.TrimSpace(s)
				if s == "" {
					v.ValueError(f.Loc, "name the button")
				}
				in.Label = &s
			}
		}
	}
	if f := o.Get("issuer"); f.Present && !f.IsNull() {
		in.Issuer = issuerURL(v, f)
	}
	if f := o.Get("client_id"); f.Present && !f.IsNull() {
		if s, ok := v.Constrained(f.Loc, f.Raw, val.StrRules{Min: 1, Max: 500}); ok {
			s = strings.TrimSpace(s)
			in.ClientID = &s
		}
	}
	if f := o.Get("client_secret"); f.Present && !f.IsNull() {
		if s, ok := v.Constrained(f.Loc, f.Raw, val.StrRules{Min: 1, Max: 2000}); ok {
			in.ClientSecret = &s
		}
	}
	if f := o.Get("scopes"); f.Present && !f.IsNull() {
		if l, ok := v.StrList(f.Loc, f.Raw); ok && v.ListLen(f.Loc, len(l), 0, 20) {
			seen := map[string]bool{}
			in.Scopes = []string{}
			for i, s := range l {
				if !scopeRe.MatchString(s) {
					v.ValueError(f.Loc.Add(i), "a scope is one word without spaces or quotes")
				} else if !seen[s] {
					seen[s] = true
					in.Scopes = append(in.Scopes, s)
				}
			}
		}
	}
	if f := o.Get("groups_claim"); f.Present && !f.IsNull() {
		if s, ok := v.Constrained(f.Loc, f.Raw, val.StrRules{Min: 1, Max: 100}); ok {
			if !claimRe.MatchString(s) {
				v.ValueError(f.Loc, "a claim is a field name such as groups")
			}
			in.GroupsClaim = &s
		}
	}
	in.AllowUnverifiedEmail = optBool(v, o, "allow_unverified_email")
	in.Enabled = optBool(v, o, "enabled")
	if f := o.Get("role_rules"); f.Present && !f.IsNull() {
		in.HasRules = true
		in.RoleRules = []map[string]any{}
		if items, ok := v.List(f.Loc, f.Raw); ok && v.ListLen(f.Loc, len(items), 0, 100) {
			for i, item := range items {
				ro := v.Model(f.Loc.Add(i), item, "SSORule")
				rule := map[string]any{"group": str(v, ro, "group", "", val.StrRules{Min: 1, Max: 200}, true)}
				if boolean(v, ro, "admin", false) {
					rule["superuser"] = true
					ro.Get("project")
					ro.Get("role")
					ro.Get("approver")
				} else {
					rule["project"] = str(v, ro, "project", "", val.StrRules{Min: 1, Max: 63}, true)
					if rf := ro.Get("role"); rf.Required() {
						rule["role"], _ = v.Literal(rf.Loc, rf.Raw, projectRoles...)
					}
					if boolean(v, ro, "approver", false) {
						rule["approver"] = true
					}
				}
				ro.Forbid()
				in.RoleRules = append(in.RoleRules, rule)
			}
		}
	}
	if f := o.Get("default_role"); f.Present {
		in.HasDefaultRole = true
		if !f.IsNull() {
			if s, ok := v.Literal(f.Loc, f.Raw, projectRoles...); ok {
				in.DefaultRole = &s
			}
		}
	}
	in.DefaultProjects = optStrList(v, o, "default_projects", 200)
	k := parseKindFields(v, o)
	// Reported with the other field errors; resolveKind checks the merged provider again.
	if in.Scopes != nil && (k.Kind == nil && create || k.Kind != nil && *k.Kind == oidc.KindOIDC) && !slices.Contains(in.Scopes, "openid") {
		v.ValueError(val.Loc{"body", "scopes"}, "include the openid scope")
	}
	o.Forbid()
	return ssoBody{ssoInput: in, kind: k}
}

// checkProjects reports rule and default projects that do not exist (as 422s).
func checkProjects(ctx context.Context, q db.Querier, in ssoInput) error {
	slugs, err := db.Select[struct {
		Slug string `db:"slug"`
	}](ctx, q, `SELECT slug FROM projects`)
	if err != nil {
		return err
	}
	known := map[string]bool{"*": true}
	for _, s := range slugs {
		known[s.Slug] = true
	}
	v := &val.V{}
	for i, r := range in.RoleRules {
		if p, ok := r["project"].(string); ok && !known[p] {
			v.ValueError(val.Loc{"body", "role_rules"}.Add(i, "project"), fmt.Sprintf("there is no project '%s'", p))
		}
	}
	for i, p := range in.DefaultProjects {
		if p == "*" || !known[p] {
			v.ValueError(val.Loc{"body", "default_projects"}.Add(i), fmt.Sprintf("there is no project '%s'", p))
		}
	}
	if p := v.Problem(); p != nil {
		return p
	}
	return nil
}

// applySSO applies the fields every kind shares; resolveKind does the rest.
func applySSO(m *models.SSOProvider, in ssoInput) {
	if in.Label != nil {
		m.Label = *in.Label
	}
	if in.Enabled != nil {
		m.Enabled = *in.Enabled
	}
	if in.HasRules {
		m.RoleRules = db.List{}
		for _, r := range in.RoleRules {
			m.RoleRules = append(m.RoleRules, r)
		}
	}
	if in.HasDefaultRole {
		m.DefaultRole = in.DefaultRole
	}
	if in.DefaultProjects != nil {
		m.DefaultProjects = db.List{}
		for _, p := range in.DefaultProjects {
			m.DefaultProjects = append(m.DefaultProjects, p)
		}
	}
}

// sameIdP refuses a second SAML provider for one identity provider: IdP-initiated sign-in and
// the replay guard tell them apart by entity ID.
func sameIdP(ctx context.Context, q db.Querier, m *models.SSOProvider) error {
	if m.Kind != oidc.KindSAML {
		return nil
	}
	n, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM sso_providers WHERE kind = 'saml' AND issuer = $1 AND id <> $2`, m.Issuer, m.ID)
	if err != nil {
		return err
	}
	if n > 0 {
		return problem.Conflict("sso_provider_exists", "Another sign-in option already uses this identity provider ("+m.Issuer+").", nil)
	}
	return nil
}

func cloneProvider(m *models.SSOProvider) *models.SSOProvider {
	c := *m
	c.Config = db.Map{}
	for k, v := range m.Config {
		c.Config[k] = v
	}
	c.Scopes = append(db.List{}, m.Scopes...)
	return &c
}

func findSSOProvider(ctx context.Context, q db.Querier, r *http.Request) (*models.SSOProvider, error) {
	id, err := pathID(r, "provider_id")
	if err != nil {
		return nil, err
	}
	m, err := db.Find[models.SSOProvider](ctx, q, `SELECT * FROM sso_providers WHERE id = $1`, id)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, problem.NotFound("Sign-in provider")
	}
	return m, nil
}

func authSettings(ctx context.Context, q db.Querier) (AuthSettingsRead, error) {
	wanted, inForce, err := services.PasswordLogin(ctx, q)
	if err != nil {
		return AuthSettingsRead{}, err
	}
	admins, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM users WHERE is_superuser AND is_active AND sso_subject IS NOT NULL`)
	if err != nil {
		return AuthSettingsRead{}, err
	}
	return AuthSettingsRead{PasswordLogin: wanted, PasswordLoginInForce: inForce, RedirectURI: oidc.RedirectURI(), SSOAdmins: admins,
		SAMLEntityID: samlsp.EntityID(), SAMLACSURL: samlsp.ACSURL(), SAMLMetadataURL: samlsp.EntityID()}, nil
}

func ssoProviderRoutes(r chi.Router) {
	r.Get("/auth/settings", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		if _, err := superuser(r); err != nil {
			return err
		}
		out, err := authSettings(r.Context(), pool(r.Context()))
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Put("/auth/settings", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		b, err := body(r, func(v *val.V, o *val.Object) bool {
			on := false
			if f := o.Get("password_login"); f.Required() {
				on, _ = v.Bool(f.Loc, f.Raw)
			}
			o.Forbid()
			return on
		})
		if err != nil {
			return err
		}
		p, err := superuser(r)
		if err != nil {
			return err
		}
		ctx := r.Context()
		var out AuthSettingsRead
		err = tx(ctx, func(q db.Querier) error {
			if err := services.SetPasswordLogin(ctx, q, b, p); err != nil {
				return err
			}
			out, err = authSettings(ctx, q)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Get("/sso-providers", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		if _, err := superuser(r); err != nil {
			return err
		}
		ctx := r.Context()
		rows, err := services.SSOProviders(ctx, pool(ctx), false)
		if err != nil {
			return err
		}
		out := make([]SSOProviderRead, len(rows))
		for i := range rows {
			if out[i], err = ssoProviderRead(&rows[i]); err != nil {
				return err
			}
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Post("/sso-providers", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		in, err := body(r, func(v *val.V, o *val.Object) ssoBody { return parseSSO(v, o, true) })
		if err != nil {
			return err
		}
		p, err := superuser(r)
		if err != nil {
			return err
		}
		ctx := r.Context()
		var out SSOProviderRead
		err = tx(ctx, func(q db.Querier) error {
			if err := checkProjects(ctx, q, in.ssoInput); err != nil {
				return err
			}
			now := db.Now()
			m := &models.SSOProvider{ID: db.NewID(), RoleRules: db.List{}, DefaultProjects: db.List{}, Scopes: db.List{}, Config: db.Map{},
				Enabled: true, Version: 1, CreatedAt: now, UpdatedAt: now}
			applySSO(m, in.ssoInput)
			if err := resolveKind(ctx, m, in.ssoInput, in.kind, true); err != nil {
				return err
			}
			if err := sameIdP(ctx, q, m); err != nil {
				return err
			}
			res, _ := testProvider(ctx, m)
			m.LastTest = res.dump()
			if err := db.Insert(ctx, q, "sso_providers", m); err != nil {
				if db.IsUniqueViolation(err) {
					return problem.Conflict("sso_provider_exists", fmt.Sprintf("A sign-in option labelled '%s' already exists.", m.Label), nil)
				}
				return err
			}
			if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "sso_provider.created", Summary: "Added sign-in with " + m.Label,
				Principal: p, ResourceType: "sso_provider", ResourceID: &m.ID, Details: map[string]any{"kind": m.Kind, "issuer": m.Issuer}}); err != nil {
				return err
			}
			out, err = ssoProviderRead(m)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusCreated, out)
		return nil
	}))
	r.Patch("/sso-providers/{provider_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		in, err := body(r, func(v *val.V, o *val.Object) ssoBody { return parseSSO(v, o, false) })
		if err != nil {
			return err
		}
		p, err := superuser(r)
		if err != nil {
			return err
		}
		ctx := r.Context()
		var out SSOProviderRead
		err = tx(ctx, func(q db.Querier) error {
			m, err := findSSOProvider(ctx, q, r)
			if err != nil {
				return err
			}
			edited, err := parseIfMatch(r)
			if err != nil {
				return err
			}
			if edited != m.Version {
				return problem.New(412, "version_conflict", "Someone else changed this sign-in option. Reload and try again.")
			}
			if err := checkProjects(ctx, q, in.ssoInput); err != nil {
				return err
			}
			if in.Enabled != nil && !*in.Enabled && m.Enabled {
				if err := services.GuardLastProvider(ctx, q, m.ID); err != nil {
					return err
				}
			}
			// The change is applied to a copy and the copy validated whole.
			next := cloneProvider(m)
			applySSO(next, in.ssoInput)
			if err := resolveKind(ctx, next, in.ssoInput, in.kind, false); err != nil {
				return err
			}
			if err := sameIdP(ctx, q, next); err != nil {
				return err
			}
			connection := in.Issuer != nil || in.ClientID != nil || in.ClientSecret != nil || len(in.kind.sent) > 0
			m = next
			if connection {
				res, _ := testProvider(ctx, m)
				m.LastTest = res.dump()
			}
			m.Version++
			m.UpdatedAt = db.Now()
			if err := db.Update(ctx, q, "sso_providers", m); err != nil {
				if db.IsUniqueViolation(err) {
					return problem.Conflict("sso_provider_exists", fmt.Sprintf("A sign-in option labelled '%s' already exists.", m.Label), nil)
				}
				return err
			}
			if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "sso_provider.updated", Summary: "Updated sign-in with " + m.Label,
				Principal: p, ResourceType: "sso_provider", ResourceID: &m.ID,
				Details: map[string]any{"client_secret_changed": in.ClientSecret != nil, "enabled": m.Enabled, "kind": m.Kind}}); err != nil {
				return err
			}
			out, err = ssoProviderRead(m)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Delete("/sso-providers/{provider_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		p, err := superuser(r)
		if err != nil {
			return err
		}
		ctx := r.Context()
		err = tx(ctx, func(q db.Querier) error {
			m, err := findSSOProvider(ctx, q, r)
			if err != nil {
				return err
			}
			if m.Enabled {
				if err := services.GuardLastProvider(ctx, q, m.ID); err != nil {
					return err
				}
			}
			if err := db.Delete(ctx, q, "sso_providers", m.ID); err != nil {
				return err
			}
			_, err = services.Record(ctx, q, services.AuditEvent{Kind: "sso_provider.deleted", Summary: "Removed sign-in with " + m.Label,
				Principal: p, ResourceType: "sso_provider", ResourceID: &m.ID})
			return err
		})
		if err != nil {
			return err
		}
		httpapi.NoContent(w)
		return nil
	}))
	r.Post("/sso-providers/{provider_id}/test", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		p, err := superuser(r)
		if err != nil {
			return err
		}
		ctx := r.Context()
		var out StoreTest
		err = tx(ctx, func(q db.Querier) error {
			m, err := findSSOProvider(ctx, q, r)
			if err != nil {
				return err
			}
			var refreshed bool
			out, refreshed = testProvider(ctx, m)
			if refreshed { // new certificates or sign-in URL from the IdP's metadata URL
				m.LastTest, m.Version, m.UpdatedAt = out.dump(), m.Version+1, db.Now()
				if err := db.Update(ctx, q, "sso_providers", m); err != nil {
					return err
				}
				_, err = services.Record(ctx, q, services.AuditEvent{Kind: "sso_provider.updated", Summary: "Refreshed " + m.Label + "'s certificates from its metadata",
					Principal: p, ResourceType: "sso_provider", ResourceID: &m.ID})
				return err
			}
			_, err = q.Exec(ctx, `UPDATE sso_providers SET last_test = $1 WHERE id = $2`, out.dump(), m.ID)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
}
