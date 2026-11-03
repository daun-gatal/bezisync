package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
	"github.com/daun-gatal/bezisync/backend/internal/mcpserver"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/ratelimit"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// Settings → MCP, the agents people connected, and BeziSync's OAuth authorization server.

// MCPInfo is what anyone signed in needs to connect an agent.
type MCPInfo struct {
	Enabled          bool   `json:"enabled"`
	URL              string `json:"url"`
	OAuthEnabled     bool   `json:"oauth_enabled"`
	APITokensAllowed bool   `json:"api_tokens_allowed"`
}

// MCPSettingsRead is Settings → MCP.
type MCPSettingsRead struct {
	services.MCPSettings
	URL string `json:"url"`
}

// MCPToolRead is one tool in Settings → MCP.
type MCPToolRead struct {
	Name           string  `json:"name"`
	Title          string  `json:"title"`
	Description    string  `json:"description"`
	Group          string  `json:"group"`
	Kind           string  `json:"kind"`
	AdminOnly      bool    `json:"admin_only"`
	Permission     *string `json:"permission"`
	Enabled        bool    `json:"enabled"`
	DefaultEnabled bool    `json:"default_enabled"`
}

// OAuthClientRead is a registered agent.
type OAuthClientRead struct {
	ID           string       `json:"id"`
	Name         string       `json:"name"`
	RedirectURIs []string     `json:"redirect_uris"`
	ClientURI    *string      `json:"client_uri"`
	Status       string       `json:"status"`
	CreatedAt    pytime.Time  `json:"created_at"`
	LastUsedAt   *pytime.Time `json:"last_used_at"`
	ActiveGrants int          `json:"active_grants"`
}

// OAuthGrantRead is one connected agent of one person.
type OAuthGrantRead struct {
	ID         uuid.UUID    `json:"id"`
	ClientID   string       `json:"client_id"`
	ClientName string       `json:"client_name"`
	UserEmail  string       `json:"user_email"`
	Project    *ProjectRef  `json:"project"`
	ReadOnly   bool         `json:"read_only"`
	CreatedAt  pytime.Time  `json:"created_at"`
	LastUsedAt *pytime.Time `json:"last_used_at"`
	ExpiresAt  pytime.Time  `json:"expires_at"`
}

// OAuthRequestRead is the consent page.
type OAuthRequestRead struct {
	ID           uuid.UUID `json:"id"`
	ClientName   string    `json:"client_name"`
	ClientURI    *string   `json:"client_uri"`
	ClientStatus string    `json:"client_status"`
	RedirectHost string    `json:"redirect_host"`
	MaxDays      int       `json:"max_days"`
}

var hostRe = regexp.MustCompile(`^(\*\.)?[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)*$`)

func mcpInfo(s services.MCPSettings) MCPInfo {
	return MCPInfo{Enabled: s.Enabled, URL: mcpserver.ResourceURL(), OAuthEnabled: s.Enabled && s.OAuthEnabled, APITokensAllowed: s.AllowAPITokens}
}

func toolsRead(s services.MCPSettings) []MCPToolRead {
	out := []MCPToolRead{}
	for _, t := range mcpserver.Catalog() {
		var perm *string
		if t.Perm != "" {
			p := string(t.Perm)
			perm = &p
		}
		out = append(out, MCPToolRead{Name: t.Name, Title: t.Title, Description: t.Description, Group: t.Group(), Kind: string(t.Kind),
			AdminOnly: t.Admin, Permission: perm, Enabled: mcpserver.Enabled(t, s), DefaultEnabled: t.Kind != mcpserver.Destructive})
	}
	return out
}

func clientRead(ctx context.Context, q db.Querier, c *models.OAuthClient) (OAuthClientRead, error) {
	n, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM oauth_grants WHERE client_id = $1 AND revoked_at IS NULL AND expires_at > $2`, c.ID, db.Now())
	if err != nil {
		return OAuthClientRead{}, err
	}
	uris := make([]string, 0, len(c.RedirectURIs))
	for _, u := range c.RedirectURIs {
		uris = append(uris, fmt.Sprint(u))
	}
	return OAuthClientRead{ID: c.ID, Name: c.Name, RedirectURIs: uris, ClientURI: c.ClientURI, Status: c.Status, CreatedAt: pytime.T(c.CreatedAt),
		LastUsedAt: pytime.P(c.LastUsedAt), ActiveGrants: int(n)}, nil
}

func grantsRead(ctx context.Context, q db.Querier, where string, args ...any) ([]OAuthGrantRead, error) {
	rows, err := db.Select[models.OAuthGrant](ctx, q, `SELECT * FROM oauth_grants WHERE revoked_at IS NULL AND expires_at > now() AND `+where+` ORDER BY created_at DESC`, args...)
	if err != nil {
		return nil, err
	}
	out := []OAuthGrantRead{}
	for i := range rows {
		g := &rows[i]
		c, err := db.Find[models.OAuthClient](ctx, q, `SELECT * FROM oauth_clients WHERE id = $1`, g.ClientID)
		if err != nil {
			return nil, err
		}
		u, err := auth.GetUser(ctx, q, g.UserID)
		if err != nil {
			return nil, err
		}
		r := OAuthGrantRead{ID: g.ID, ClientID: g.ClientID, ReadOnly: g.ReadOnly, CreatedAt: pytime.T(g.CreatedAt), LastUsedAt: pytime.P(g.LastUsedAt),
			ExpiresAt: pytime.T(g.ExpiresAt)}
		if c != nil {
			r.ClientName = c.Name
		}
		if u != nil {
			r.UserEmail = u.Email
		}
		if g.ProjectID != nil {
			if p, err := db.Find[models.Project](ctx, q, `SELECT * FROM projects WHERE id = $1`, *g.ProjectID); err != nil {
				return nil, err
			} else if p != nil {
				r.Project = &ProjectRef{ID: p.ID, Slug: p.Slug, Name: p.Name, Permissions: []string{}}
			}
		}
		out = append(out, r)
	}
	return out, nil
}

func parseMCPSettings(v *val.V, o *val.Object, cur services.MCPSettings) services.MCPSettings {
	s := cur
	if b := optBool(v, o, "enabled"); b != nil {
		s.Enabled = *b
	}
	if b := optBool(v, o, "allow_api_tokens"); b != nil {
		s.AllowAPITokens = *b
	}
	if b := optBool(v, o, "oauth_enabled"); b != nil {
		s.OAuthEnabled = *b
	}
	if f := o.Get("oauth_registration"); f.Present {
		if m, ok := v.Literal(f.Loc, f.Raw, services.RegistrationOpen, services.RegistrationApproval); ok {
			s.OAuthRegistration = m
		}
	}
	if f := o.Get("oauth_redirect_hosts"); f.Present {
		if l, ok := v.StrList(f.Loc, f.Raw); ok && v.ListLen(f.Loc, len(l), 0, 50) {
			hosts := []string{}
			for i, h := range l {
				h = strings.ToLower(strings.TrimSpace(h))
				if !hostRe.MatchString(h) {
					v.ValueError(f.Loc.Add(i), "use a host name such as agent.example.com or *.example.com")
				} else if !slices.Contains(hosts, h) {
					hosts = append(hosts, h)
				}
			}
			s.OAuthRedirectHosts = hosts
		}
	}
	if f := o.Get("oauth_max_grant_days"); f.Present {
		if n, ok := v.IntIn(f.Loc, f.Raw, val.Num{GE: val.F(1), LE: val.F(365)}); ok {
			s.OAuthMaxGrantDays = n
		}
	}
	if f := o.Get("allowed_origins"); f.Present {
		if l, ok := v.StrList(f.Loc, f.Raw); ok && v.ListLen(f.Loc, len(l), 0, 50) {
			origins := []string{}
			for i, x := range l {
				x = strings.TrimRight(strings.TrimSpace(x), "/")
				if !regexp.MustCompile(`^https?://[^/?#\s]+$`).MatchString(x) {
					v.ValueError(f.Loc.Add(i), "use an origin such as https://agent.example.com")
				} else if !slices.Contains(origins, x) {
					origins = append(origins, x)
				}
			}
			s.AllowedOrigins = origins
		}
	}
	if f := o.Get("tools"); f.Present {
		if m, ok := v.Dict(f.Loc, f.Raw); ok {
			tools := map[string]bool{}
			for name, raw := range m {
				t := mcpserver.Find(name)
				if t == nil {
					v.ValueError(f.Loc.Add(name), "there is no such tool")
					continue
				}
				if b, ok := v.Bool(f.Loc.Add(name), raw); ok && b != (t.Kind != mcpserver.Destructive) {
					tools[name] = b // only choices that differ from the default are kept
				}
			}
			s.Tools = tools
		}
	}
	o.Forbid()
	return s
}

func mcpRoutes(r chi.Router) {
	r.Get("/mcp/info", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		if _, err := principal(r); err != nil {
			return err
		}
		s, err := services.GetMCPSettings(r.Context(), pool(r.Context()))
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, mcpInfo(s))
		return nil
	}))
	r.Get("/mcp/settings", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		if _, err := superuser(r); err != nil {
			return err
		}
		s, err := services.GetMCPSettings(r.Context(), pool(r.Context()))
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, MCPSettingsRead{MCPSettings: s, URL: mcpserver.ResourceURL()})
		return nil
	}))
	r.Put("/mcp/settings", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		ctx := r.Context()
		cur, err := services.GetMCPSettings(ctx, pool(ctx))
		if err != nil {
			return err
		}
		next, err := body(r, func(v *val.V, o *val.Object) services.MCPSettings { return parseMCPSettings(v, o, cur) })
		if err != nil {
			return err
		}
		p, err := superuser(r)
		if err != nil {
			return err
		}
		if err := interactive(p); err != nil {
			return err // an agent never changes its own access
		}
		err = tx(ctx, func(q db.Querier) error { return services.SaveMCPSettings(ctx, q, next, p) })
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, MCPSettingsRead{MCPSettings: next, URL: mcpserver.ResourceURL()})
		return nil
	}))
	r.Get("/mcp/tools", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		if _, err := superuser(r); err != nil {
			return err
		}
		s, err := services.GetMCPSettings(r.Context(), pool(r.Context()))
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, toolsRead(s))
		return nil
	}))
	r.Get("/mcp/clients", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		if _, err := superuser(r); err != nil {
			return err
		}
		ctx := r.Context()
		q := pool(ctx)
		rows, err := db.Select[models.OAuthClient](ctx, q, `SELECT * FROM oauth_clients ORDER BY status = 'pending' DESC, created_at DESC LIMIT 500`)
		if err != nil {
			return err
		}
		out := make([]OAuthClientRead, len(rows))
		for i := range rows {
			if out[i], err = clientRead(ctx, q, &rows[i]); err != nil {
				return err
			}
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Patch("/mcp/clients/{client_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		status, err := body(r, func(v *val.V, o *val.Object) string {
			s := ""
			if f := o.Get("status"); f.Required() {
				s, _ = v.Literal(f.Loc, f.Raw, services.ClientAllowed, services.ClientBlocked)
			}
			o.Forbid()
			return s
		})
		if err != nil {
			return err
		}
		p, err := superuser(r)
		if err != nil {
			return err
		}
		if err := interactive(p); err != nil {
			return err
		}
		ctx := r.Context()
		var out OAuthClientRead
		err = tx(ctx, func(q db.Querier) error {
			c, err := db.Find[models.OAuthClient](ctx, q, `SELECT * FROM oauth_clients WHERE id = $1`, chi.URLParam(r, "client_id"))
			if err != nil {
				return err
			}
			if c == nil {
				return problem.NotFound("Agent")
			}
			now := db.Now()
			c.Status, c.DecidedByID, c.DecidedAt = status, &p.User.ID, &now
			if err := db.Update(ctx, q, "oauth_clients", c, "id"); err != nil {
				return err
			}
			if status == services.ClientBlocked {
				if _, err := q.Exec(ctx, `UPDATE oauth_grants SET revoked_at = $1 WHERE client_id = $2 AND revoked_at IS NULL`, now, c.ID); err != nil {
					return err
				}
			}
			verb := map[string]string{services.ClientAllowed: "Allowed", services.ClientBlocked: "Blocked"}[status]
			if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "mcp.client_" + status, Summary: verb + " the MCP agent " + c.Name,
				Principal: p, ResourceType: "oauth_client", Details: map[string]any{"client_id": c.ID}}); err != nil {
				return err
			}
			out, err = clientRead(ctx, q, c)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Delete("/mcp/clients/{client_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		p, err := superuser(r)
		if err != nil {
			return err
		}
		if err := interactive(p); err != nil {
			return err
		}
		ctx := r.Context()
		err = tx(ctx, func(q db.Querier) error {
			c, err := db.Find[models.OAuthClient](ctx, q, `SELECT * FROM oauth_clients WHERE id = $1`, chi.URLParam(r, "client_id"))
			if err != nil {
				return err
			}
			if c == nil {
				return problem.NotFound("Agent")
			}
			if _, err := q.Exec(ctx, `DELETE FROM oauth_clients WHERE id = $1`, c.ID); err != nil {
				return err
			}
			_, err = services.Record(ctx, q, services.AuditEvent{Kind: "mcp.client_removed", Summary: "Removed the MCP agent " + c.Name + " and its access",
				Principal: p, ResourceType: "oauth_client", Details: map[string]any{"client_id": c.ID}})
			return err
		})
		if err != nil {
			return err
		}
		httpapi.NoContent(w)
		return nil
	}))
	r.Get("/mcp/grants", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		p, err := principal(r)
		if err != nil {
			return err
		}
		all := httpapi.Q(r).Bool("all", false)
		if all {
			if err := auth.RequireSuperuser(p); err != nil {
				return err
			}
		}
		ctx := r.Context()
		var out []OAuthGrantRead
		if all {
			out, err = grantsRead(ctx, pool(ctx), "true")
		} else {
			out, err = grantsRead(ctx, pool(ctx), "user_id = $1", p.User.ID)
		}
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Delete("/mcp/grants/{grant_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "grant_id")
		if err != nil {
			return err
		}
		p, err := principal(r)
		if err != nil {
			return err
		}
		ctx := r.Context()
		err = tx(ctx, func(q db.Querier) error {
			g, err := db.Find[models.OAuthGrant](ctx, q, `SELECT * FROM oauth_grants WHERE id = $1 AND revoked_at IS NULL`, id)
			if err != nil {
				return err
			}
			if g == nil || (g.UserID != p.User.ID && !p.User.IsSuperuser) {
				return problem.NotFound("Connected agent")
			}
			return services.RevokeGrant(ctx, q, g, p)
		})
		if err != nil {
			return err
		}
		httpapi.NoContent(w)
		return nil
	}))

	// The consent page (a signed-in person in the browser).
	consent := func(r *http.Request) (*auth.Principal, *models.OAuthRequest, *models.OAuthClient, error) {
		id, err := pathID(r, "request_id")
		if err != nil {
			return nil, nil, nil, err
		}
		p, err := principal(r)
		if err != nil {
			return nil, nil, nil, err
		}
		if err := interactive(p); err != nil {
			return nil, nil, nil, err
		}
		ctx := r.Context()
		req, c, err := services.PendingRequest(ctx, pool(ctx), id)
		if err != nil {
			return nil, nil, nil, err
		}
		if req == nil || c == nil {
			return nil, nil, nil, problem.New(404, "oauth_request_expired", "This sign-in request was already used or has expired. Start again from your agent.")
		}
		return p, req, c, nil
	}
	r.Get("/oauth/requests/{request_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		_, req, c, err := consent(r)
		if err != nil {
			return err
		}
		s, err := services.GetMCPSettings(r.Context(), pool(r.Context()))
		if err != nil {
			return err
		}
		host := req.RedirectURI
		if u, err := parseURL(req.RedirectURI); err == nil {
			host = u
		}
		httpapi.JSON(w, http.StatusOK, OAuthRequestRead{ID: req.ID, ClientName: c.Name, ClientURI: c.ClientURI, ClientStatus: c.Status,
			RedirectHost: host, MaxDays: s.OAuthMaxGrantDays})
		return nil
	}))
	r.Post("/oauth/requests/{request_id}/approve", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		type decision struct {
			ProjectID  *uuid.UUID
			ReadOnly   bool
			Days       int
			AllowAgent bool
		}
		b, err := body(r, func(v *val.V, o *val.Object) decision {
			d := decision{ProjectID: optUUID(v, o, "project_id"), ReadOnly: boolean(v, o, "read_only", false), AllowAgent: boolean(v, o, "allow_agent", false)}
			if f := o.Get("days"); f.Required() {
				d.Days, _ = v.IntIn(f.Loc, f.Raw, val.Num{GE: val.F(1), LE: val.F(365)})
			}
			o.Forbid()
			return d
		})
		if err != nil {
			return err
		}
		p, req, c, err := consent(r)
		if err != nil {
			return err
		}
		ctx := r.Context()
		var redirectTo string
		err = tx(ctx, func(q db.Querier) error {
			s, err := services.GetMCPSettings(ctx, q)
			if err != nil {
				return err
			}
			if !s.Enabled || !s.OAuthEnabled {
				return problem.New(409, "oauth_disabled", "Signing in agents is turned off. An Admin can turn it on in Settings → MCP.")
			}
			if b.Days > s.OAuthMaxGrantDays {
				return problem.New(422, "too_long", fmt.Sprintf("Agents may keep access for at most %d days.", s.OAuthMaxGrantDays))
			}
			if c.Status == services.ClientPending {
				if !b.AllowAgent || !p.User.IsSuperuser {
					return problem.New(409, "agent_pending", "An Admin has to allow this agent first (Settings → MCP).")
				}
				now := db.Now()
				c.Status, c.DecidedByID, c.DecidedAt = services.ClientAllowed, &p.User.ID, &now
				if err := db.Update(ctx, q, "oauth_clients", c, "id"); err != nil {
					return err
				}
				if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "mcp.client_allowed", Summary: "Allowed the MCP agent " + c.Name,
					Principal: p, ResourceType: "oauth_client", Details: map[string]any{"client_id": c.ID}}); err != nil {
					return err
				}
			}
			if b.ProjectID != nil {
				if _, err := auth.Project(ctx, q, p, *b.ProjectID); err != nil {
					return err
				}
			}
			code, err := services.ApproveRequest(ctx, q, req, p, b.ProjectID, b.ReadOnly, b.Days)
			if err != nil {
				var oe *services.OAuthError
				if errors.As(err, &oe) {
					return problem.New(409, "oauth_request_expired", oe.Description)
				}
				return err
			}
			redirectTo = services.Redirect(req, issuer(), map[string]string{"code": code})
			return nil
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, obj("redirect_to", redirectTo))
		return nil
	}))
	r.Post("/oauth/requests/{request_id}/deny", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		_, req, _, err := consent(r)
		if err != nil {
			return err
		}
		ctx := r.Context()
		if err := services.DenyRequest(ctx, pool(ctx), req); err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, obj("redirect_to", services.Redirect(req, issuer(), map[string]string{"error": "access_denied",
			"error_description": "The person declined."})))
		return nil
	}))

	oauthProtocolRoutes(r)
}

func issuer() string { return strings.TrimRight(config.Get().PublicURL, "/") }

func parseURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if u.Host != "" {
		return u.Host, nil
	}
	return u.Scheme + ":", nil
}

// oauthEnabled answers 404 unless agents may sign in.
func oauthEnabled(ctx context.Context) (services.MCPSettings, bool, error) {
	s, err := services.GetMCPSettings(ctx, pool(ctx))
	return s, err == nil && s.Enabled && s.OAuthEnabled, err
}

func oauthJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	httpapi.JSON(w, status, v)
}

func oauthFail(w http.ResponseWriter, err error) error {
	var oe *services.OAuthError
	if errors.As(err, &oe) {
		status := oe.Status
		if status == 0 {
			status = 400
		}
		oauthJSON(w, status, obj("error", oe.Code, "error_description", oe.Description))
		return nil
	}
	return err
}

// oauthProtocolRoutes are the RFC 7591 / 6749 / 7009 endpoints agents call.
func oauthProtocolRoutes(r chi.Router) {
	r.Post("/oauth/register", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		ctx := r.Context()
		s, on, err := oauthEnabled(ctx)
		if err != nil {
			return err
		}
		if !on {
			return problem.New(404, "oauth_disabled", "Signing in agents is turned off.")
		}
		if err := ratelimit.OAuthRegister.Check(httpapi.ClientHost(r)); err != nil {
			return err
		}
		var in struct {
			ClientName              string   `json:"client_name"`
			RedirectURIs            []string `json:"redirect_uris"`
			ClientURI               *string  `json:"client_uri"`
			TokenEndpointAuthMethod string   `json:"token_endpoint_auth_method"`
			GrantTypes              []string `json:"grant_types"`
			ResponseTypes           []string `json:"response_types"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&in); err != nil {
			return oauthFail(w, &services.OAuthError{Status: 400, Code: "invalid_client_metadata", Description: "The body must be JSON client metadata."})
		}
		if in.TokenEndpointAuthMethod != "" && in.TokenEndpointAuthMethod != "none" {
			return oauthFail(w, &services.OAuthError{Status: 400, Code: "invalid_client_metadata", Description: "Only public clients (token_endpoint_auth_method none, with PKCE) are supported."})
		}
		for _, g := range in.GrantTypes {
			if g != "authorization_code" && g != "refresh_token" {
				return oauthFail(w, &services.OAuthError{Status: 400, Code: "invalid_client_metadata", Description: "Supported grant types: authorization_code, refresh_token."})
			}
		}
		if in.ClientURI != nil && !strings.HasPrefix(*in.ClientURI, "https://") {
			in.ClientURI = nil
		}
		var c *models.OAuthClient
		err = tx(ctx, func(q db.Querier) error {
			var err error
			c, err = services.RegisterClient(ctx, q, in.ClientName, in.RedirectURIs, in.ClientURI, s)
			return err
		})
		if err != nil {
			return oauthFail(w, err)
		}
		uris := make([]string, 0, len(c.RedirectURIs))
		for _, u := range c.RedirectURIs {
			uris = append(uris, fmt.Sprint(u))
		}
		oauthJSON(w, http.StatusCreated, obj("client_id", c.ID, "client_id_issued_at", c.CreatedAt.Unix(), "client_name", c.Name, "redirect_uris", uris,
			"token_endpoint_auth_method", "none", "grant_types", []string{"authorization_code", "refresh_token"}, "response_types", []string{"code"}))
		return nil
	}))
	r.Get("/oauth/authorize", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		ctx := r.Context()
		_, on, err := oauthEnabled(ctx)
		if err != nil {
			return err
		}
		qs := r.URL.Query()
		if !on {
			redirect(w, "/oauth/consent?error="+quote("Signing in agents is turned off. An Admin can turn it on in Settings → MCP."))
			return nil
		}
		a := services.AuthorizeParams{ClientID: qs.Get("client_id"), RedirectURI: qs.Get("redirect_uri"), ResponseType: qs.Get("response_type"),
			CodeChallenge: qs.Get("code_challenge"), Method: qs.Get("code_challenge_method"), State: qs.Get("state"), Resource: qs.Get("resource")}
		var req *models.OAuthRequest
		err = tx(ctx, func(q db.Querier) error {
			var err error
			req, err = services.StartAuthorization(ctx, q, a, mcpserver.ResourceURL())
			return err
		})
		var oe *services.OAuthError
		switch {
		case errors.As(err, &oe) && oe.Status == 0:
			redirect(w, "/oauth/consent?error="+quote(oe.Description))
		case errors.As(err, &oe):
			fake := &models.OAuthRequest{RedirectURI: a.RedirectURI}
			if a.State != "" {
				fake.State = &a.State
			}
			redirect(w, services.Redirect(fake, issuer(), map[string]string{"error": oe.Code, "error_description": oe.Description}))
		case err != nil:
			return err
		default:
			redirect(w, "/oauth/consent?request="+req.ID.String())
		}
		return nil
	}))
	r.Post("/oauth/token", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		ctx := r.Context()
		_, on, err := oauthEnabled(ctx)
		if err != nil {
			return err
		}
		if !on {
			return oauthFail(w, &services.OAuthError{Status: 400, Code: "invalid_client", Description: "Signing in agents is turned off."})
		}
		if err := ratelimit.OAuthToken.Check(httpapi.ClientHost(r)); err != nil {
			return err
		}
		r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
		if err := r.ParseForm(); err != nil {
			return oauthFail(w, &services.OAuthError{Status: 400, Code: "invalid_request", Description: "Send a form-encoded body."})
		}
		f := r.PostForm
		var pair *services.TokenPair
		var refused *services.OAuthError
		err = tx(ctx, func(q db.Querier) error {
			var err error
			switch f.Get("grant_type") {
			case "authorization_code":
				if res := f.Get("resource"); res != "" && strings.TrimRight(res, "/") != mcpserver.ResourceURL() {
					return &services.OAuthError{Status: 400, Code: "invalid_target", Description: "BeziSync issues tokens for its MCP endpoint only."}
				}
				pair, err = services.ExchangeCode(ctx, q, f.Get("client_id"), f.Get("code"), f.Get("redirect_uri"), f.Get("code_verifier"))
			case "refresh_token":
				pair, err = services.Refresh(ctx, q, f.Get("client_id"), f.Get("refresh_token"))
			default:
				err = &services.OAuthError{Status: 400, Code: "unsupported_grant_type", Description: "Use authorization_code or refresh_token."}
			}
			// Commit what the error path wrote (a reuse revocation) and still answer with the error.
			if errors.As(err, &refused) {
				return nil
			}
			return err
		})
		if err != nil {
			return err
		}
		if refused != nil {
			return oauthFail(w, refused)
		}
		oauthJSON(w, http.StatusOK, obj("access_token", pair.Access, "token_type", "Bearer", "expires_in", pair.ExpiresIn,
			"refresh_token", pair.Refresh, "scope", "mcp"))
		return nil
	}))
	r.Post("/oauth/revoke", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
		if err := r.ParseForm(); err != nil {
			return oauthFail(w, &services.OAuthError{Status: 400, Code: "invalid_request", Description: "Send a form-encoded body."})
		}
		ctx := r.Context()
		if tok := r.PostForm.Get("token"); tok != "" {
			if err := tx(ctx, func(q db.Querier) error { return services.RevokeToken(ctx, q, tok) }); err != nil {
				return err
			}
		}
		w.WriteHeader(http.StatusOK)
		return nil
	}))
}

// oauthDiscoveryRoutes serve the metadata agents look for at the site's root (RFC 8414, RFC 9728).
func oauthDiscoveryRoutes(r chi.Router) {
	resource := httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		if _, on, err := oauthEnabled(r.Context()); err != nil || !on {
			if err != nil {
				return err
			}
			return problem.New(404, "oauth_disabled", "Signing in agents is turned off.")
		}
		w.Header().Set("Access-Control-Allow-Origin", "*")
		httpapi.JSON(w, http.StatusOK, obj("resource", mcpserver.ResourceURL(), "authorization_servers", []string{issuer()},
			"bearer_methods_supported", []string{"header"}, "scopes_supported", []string{"mcp"}, "resource_name", "BeziSync"))
		return nil
	})
	server := httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		if _, on, err := oauthEnabled(r.Context()); err != nil || !on {
			if err != nil {
				return err
			}
			return problem.New(404, "oauth_disabled", "Signing in agents is turned off.")
		}
		base := issuer() + "/api/v1/oauth"
		w.Header().Set("Access-Control-Allow-Origin", "*")
		httpapi.JSON(w, http.StatusOK, obj("issuer", issuer(), "authorization_endpoint", base+"/authorize", "token_endpoint", base+"/token",
			"registration_endpoint", base+"/register", "revocation_endpoint", base+"/revoke", "response_types_supported", []string{"code"},
			"grant_types_supported", []string{"authorization_code", "refresh_token"}, "code_challenge_methods_supported", []string{"S256"},
			"token_endpoint_auth_methods_supported", []string{"none"}, "scopes_supported", []string{"mcp"},
			"authorization_response_iss_parameter_supported", true))
		return nil
	})
	r.Get("/.well-known/oauth-protected-resource", resource)
	r.Get("/.well-known/oauth-protected-resource"+mcpserver.Path, resource)
	r.Get("/.well-known/oauth-authorization-server", server)
}
