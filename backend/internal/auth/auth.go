// Package auth authenticates requests (session cookie + CSRF, or bearer API token) and
// authorises project access with explicit permissions.
package auth

import (
	"context"
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/security"
)

// Cookie and header names.
const (
	SessionCookie = "bz_session"
	CSRFCookie    = "bz_csrf"
	CSRFHeader    = "x-csrf-token"
)

// Perm is a permission; routers ask for permissions, never role names.
type Perm string

// Permissions.
const (
	ProjectView         Perm = "project.view"
	ProjectManage       Perm = "project.manage"
	MemberManage        Perm = "member.manage"
	RuntimeManage       Perm = "runtime.manage"
	ConnectionView      Perm = "connection.view"
	ConnectionManage    Perm = "connection.manage"
	PipelineView        Perm = "pipeline.view"
	PipelineEdit        Perm = "pipeline.edit"
	PipelineDeploy      Perm = "pipeline.deploy"
	PipelineOperate     Perm = "pipeline.operate"
	PipelinePreviewData Perm = "pipeline.preview_data"
	PipelineApprove     Perm = "pipeline.approve"
	OffsetsReset        Perm = "offsets.reset"
	AlertView           Perm = "alert.view"
	AlertAck            Perm = "alert.ack"
	AlertManage         Perm = "alert.manage"
	AuditView           Perm = "audit.view"
)

// AllPerms in declaration order.
var AllPerms = []Perm{ProjectView, ProjectManage, MemberManage, RuntimeManage, ConnectionView, ConnectionManage,
	PipelineView, PipelineEdit, PipelineDeploy, PipelineOperate, PipelinePreviewData, PipelineApprove, OffsetsReset,
	AlertView, AlertAck, AlertManage, AuditView}

var (
	viewer   = []Perm{ProjectView, ConnectionView, PipelineView, AlertView}
	operator = append(append([]Perm{}, viewer...), PipelineOperate, PipelinePreviewData, AlertAck)
	editor   = append(append([]Perm{}, operator...), PipelineEdit, PipelineDeploy, ConnectionManage, OffsetsReset)
	// RolePermissions maps project roles to permissions.
	RolePermissions = map[string][]Perm{models.RoleViewer: viewer, models.RoleOperator: operator, models.RoleEditor: editor}
)

// PermSet is a set of permissions.
type PermSet map[Perm]bool

// PermissionsFor is the permission set of a membership (Admins hold every permission).
func PermissionsFor(m *models.Membership, superuser bool) PermSet {
	out := PermSet{}
	if superuser {
		for _, p := range AllPerms {
			out[p] = true
		}
		return out
	}
	if m == nil {
		return out
	}
	for _, p := range RolePermissions[m.Role] {
		out[p] = true
	}
	if m.CanApprove {
		out[PipelineApprove] = true
	}
	return out
}

// Sorted lists permissions alphabetically.
func (s PermSet) Sorted() []string {
	out := make([]string, 0, len(s))
	for p := range s {
		out = append(out, string(p))
	}
	sort.Strings(out)
	return out
}

// Principal is the authenticated caller.
type Principal struct {
	User     *models.User
	Session  *models.UserSession
	APIToken *models.APIToken
	// OAuthGrant is set for an MCP agent signed in with OAuth; APIToken then describes the grant
	// (name, project scope, read-only), so every token rule applies to it too.
	OAuthGrant *models.OAuthGrant
}

// Token prefixes.
const (
	APITokenPrefix    = "bzt_"
	OAuthAccessPrefix = "bzo_"
)

type ctxKey int

const viaMCPKey ctxKey = 1

// WithMCP marks a request as coming through the MCP endpoint: only those accept OAuth access tokens.
func WithMCP(ctx context.Context) context.Context { return context.WithValue(ctx, viaMCPKey, true) }

// ViaMCP reports whether the request came through the MCP endpoint.
func ViaMCP(ctx context.Context) bool { v, _ := ctx.Value(viaMCPKey).(bool); return v }

// Label names the actor in audit events.
func (p *Principal) Label() string {
	if p.APIToken != nil {
		return p.User.Email + " (token " + p.APIToken.Name + ")"
	}
	return p.User.Email
}

var safeMethods = map[string]bool{http.MethodGet: true, http.MethodHead: true, http.MethodOptions: true}

// StartSession creates a session and sets its cookies.
func StartSession(ctx context.Context, q db.Querier, w http.ResponseWriter, user *models.User, userAgent string) (*models.UserSession, error) {
	s := config.Get()
	token := security.NewToken("bzs")
	csrf := security.NewToken("csrf")
	if len(csrf) > 64 {
		csrf = csrf[:64]
	}
	if len([]rune(userAgent)) > 400 {
		userAgent = string([]rune(userAgent)[:400])
	}
	now := db.Now()
	sess := &models.UserSession{ID: db.NewID(), UserID: user.ID, TokenHash: security.TokenHash(token), CSRFToken: csrf,
		ExpiresAt: now.Add(time.Duration(s.SessionTTLHours) * time.Hour), UserAgent: userAgent, CreatedAt: now, UpdatedAt: now}
	if _, err := q.Exec(ctx, `INSERT INTO user_sessions (id, user_id, token_hash, csrf_token, expires_at, user_agent, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, sess.ID, sess.UserID, sess.TokenHash, sess.CSRFToken, sess.ExpiresAt, sess.UserAgent, now, now); err != nil {
		return nil, err
	}
	maxAge := s.SessionTTLHours * 3600
	//nolint:gosec // Secure follows BEZISYNC_COOKIE_SECURE (production requires it)
	http.SetCookie(w, &http.Cookie{Name: SessionCookie, Value: token, MaxAge: maxAge, Path: "/", HttpOnly: true, Secure: s.CookieSecure, SameSite: http.SameSiteLaxMode,
		Expires: time.Now().Add(time.Duration(maxAge) * time.Second)})
	//nolint:gosec // the CSRF cookie is read by the SPA by design (double-submit token)
	http.SetCookie(w, &http.Cookie{Name: CSRFCookie, Value: csrf, MaxAge: maxAge, Path: "/", HttpOnly: false, Secure: s.CookieSecure, SameSite: http.SameSiteLaxMode,
		Expires: time.Now().Add(time.Duration(maxAge) * time.Second)})
	return sess, nil
}

// EndSession deletes the session and clears its cookies.
func EndSession(ctx context.Context, q db.Querier, w http.ResponseWriter, p *Principal) error {
	if p.Session != nil {
		if _, err := q.Exec(ctx, `DELETE FROM user_sessions WHERE id = $1`, p.Session.ID); err != nil {
			return err
		}
	}
	for _, name := range []string{SessionCookie, CSRFCookie} {
		http.SetCookie(w, &http.Cookie{Name: name, Value: "", Path: "/", MaxAge: -1, Expires: time.Unix(0, 0)}) //nolint:gosec // deleting the cookie
	}
	return nil
}

// GetUser loads a user.
func GetUser(ctx context.Context, q db.Querier, id uuid.UUID) (*models.User, error) {
	return db.Find[models.User](ctx, q, `SELECT * FROM users WHERE id = $1`, id)
}

func fromToken(ctx context.Context, q db.Querier, raw string, r *http.Request) (*Principal, error) {
	tok, err := db.Find[models.APIToken](ctx, q, `SELECT * FROM api_tokens WHERE token_hash = ANY($1)`, security.TokenHashes(raw))
	if err != nil {
		return nil, err
	}
	now := db.Now()
	var user *models.User
	if tok != nil {
		if user, err = GetUser(ctx, q, tok.UserID); err != nil {
			return nil, err
		}
	}
	if tok == nil || (tok.ExpiresAt != nil && tok.ExpiresAt.Before(now)) || user == nil || !user.IsActive {
		return nil, problem.New(401, "invalid_token", "The API token is invalid or expired.")
	}
	if tok.ReadOnly && !safeMethods[r.Method] {
		return nil, problem.New(403, "read_only_token", "This API token is read-only.")
	}
	current := security.TokenHash(raw)
	stale := tok.LastUsedAt == nil || now.Sub(*tok.LastUsedAt) > time.Minute
	if tok.TokenHash != current || stale {
		// Persist even on read-only requests: usage time (at most once a minute) and, after
		// a master-key rotation, the hash under the current key.
		tok.TokenHash, tok.LastUsedAt = current, &now
		if _, err := q.Exec(ctx, `UPDATE api_tokens SET token_hash = $1, last_used_at = $2, updated_at = $2 WHERE id = $3`, current, now, tok.ID); err != nil {
			return nil, err
		}
	}
	return &Principal{User: user, APIToken: tok}, nil
}

func fromSession(ctx context.Context, q db.Querier, raw string, r *http.Request) (*Principal, error) {
	sess, err := db.Find[models.UserSession](ctx, q, `SELECT * FROM user_sessions WHERE token_hash = ANY($1)`, security.TokenHashes(raw))
	if err != nil {
		return nil, err
	}
	var user *models.User
	if sess != nil {
		if user, err = GetUser(ctx, q, sess.UserID); err != nil {
			return nil, err
		}
	}
	if sess == nil || sess.ExpiresAt.Before(db.Now()) || user == nil || !user.IsActive {
		return nil, problem.New(401, "session_expired", "Your session has expired. Sign in again.")
	}
	if !safeMethods[r.Method] {
		sent := r.Header.Get(CSRFHeader)
		if sent == "" || !security.ConstantTimeEqual(sent, sess.CSRFToken) {
			return nil, problem.New(403, "csrf_failed", "Missing or invalid CSRF token. Reload the page and retry.")
		}
	}
	if h := security.TokenHash(raw); sess.TokenHash != h {
		sess.TokenHash = h // the master key was rotated: move to the new key
		if _, err := q.Exec(ctx, `UPDATE user_sessions SET token_hash = $1, updated_at = $2 WHERE id = $3`, h, db.Now(), sess.ID); err != nil {
			return nil, err
		}
	}
	return &Principal{User: user, Session: sess}, nil
}

func fromOAuth(ctx context.Context, q db.Querier, raw string, r *http.Request) (*Principal, error) {
	invalid := problem.New(401, "invalid_token", "The access token is invalid or expired. Sign in again from your agent.")
	if !ViaMCP(ctx) {
		return nil, problem.New(401, "invalid_token", "OAuth access tokens work only on the MCP endpoint.")
	}
	g, err := db.Find[models.OAuthGrant](ctx, q, `SELECT * FROM oauth_grants WHERE access_hash = ANY($1)`, security.TokenHashes(raw))
	if err != nil {
		return nil, err
	}
	now := db.Now()
	if g == nil || g.RevokedAt != nil || g.AccessExpiresAt.Before(now) || g.ExpiresAt.Before(now) {
		return nil, invalid
	}
	client, err := db.Find[models.OAuthClient](ctx, q, `SELECT * FROM oauth_clients WHERE id = $1`, g.ClientID)
	if err != nil {
		return nil, err
	}
	user, err := GetUser(ctx, q, g.UserID)
	if err != nil {
		return nil, err
	}
	if client == nil || client.Status != "allowed" || user == nil || !user.IsActive {
		return nil, invalid
	}
	if g.ReadOnly && !safeMethods[r.Method] {
		return nil, problem.New(403, "read_only_token", "This agent was given read-only access.")
	}
	if g.LastUsedAt == nil || now.Sub(*g.LastUsedAt) > time.Minute {
		g.LastUsedAt = &now
		if _, err := q.Exec(ctx, `UPDATE oauth_grants SET last_used_at = $1 WHERE id = $2`, now, g.ID); err != nil {
			return nil, err
		}
	}
	tok := &models.APIToken{ID: g.ID, UserID: g.UserID, Name: "MCP · " + client.Name, ProjectID: g.ProjectID, ReadOnly: g.ReadOnly}
	return &Principal{User: user, APIToken: tok, OAuthGrant: g}, nil
}

// Current authenticates a request.
func Current(ctx context.Context, q db.Querier, r *http.Request) (*Principal, error) {
	header := r.Header.Get("authorization")
	if strings.HasPrefix(strings.ToLower(header), "bearer ") {
		raw := strings.TrimSpace(header[7:])
		if strings.HasPrefix(raw, OAuthAccessPrefix) {
			return fromOAuth(ctx, q, raw, r)
		}
		return fromToken(ctx, q, raw, r)
	}
	if c, err := r.Cookie(SessionCookie); err == nil && c.Value != "" {
		return fromSession(ctx, q, c.Value, r)
	}
	return nil, problem.New(401, "not_authenticated", "Sign in to continue.")
}

// RequireSuperuser fails unless the caller is an Admin.
func RequireSuperuser(p *Principal) error {
	if !p.User.IsSuperuser {
		return problem.New(403, "forbidden", "Only Admins can do this.")
	}
	return nil
}

// ProjectAccess is a resolved project with the caller's permissions.
type ProjectAccess struct {
	Project     *models.Project
	Principal   *Principal
	Permissions PermSet
}

// Require fails without the permission.
func (a *ProjectAccess) Require(p Perm) error {
	if !a.Permissions[p] {
		return problem.New(403, "forbidden", "You need the '"+string(p)+"' permission in this project.")
	}
	return nil
}

// Can reports a permission.
func (a *ProjectAccess) Can(p Perm) bool { return a.Permissions[p] }

// Project resolves a project for the caller; unknown and unauthorised look the same (404).
func Project(ctx context.Context, q db.Querier, p *Principal, projectID uuid.UUID) (*ProjectAccess, error) {
	tok := p.APIToken
	if tok != nil && tok.ProjectID != nil && *tok.ProjectID != projectID {
		return nil, problem.NotFound("Project")
	}
	project, err := db.Find[models.Project](ctx, q, `SELECT * FROM projects WHERE id = $1`, projectID)
	if err != nil {
		return nil, err
	}
	if project == nil {
		return nil, problem.NotFound("Project")
	}
	m, err := db.Find[models.Membership](ctx, q, `SELECT * FROM memberships WHERE project_id = $1 AND user_id = $2`, projectID, p.User.ID)
	if err != nil {
		return nil, err
	}
	perms := PermissionsFor(m, p.User.IsSuperuser)
	if len(perms) == 0 {
		return nil, problem.NotFound("Project")
	}
	if tok != nil && tok.ReadOnly {
		ro := PermSet{}
		for perm := range perms {
			if strings.HasSuffix(string(perm), ".view") {
				ro[perm] = true
			}
		}
		perms = ro
	}
	return &ProjectAccess{Project: project, Principal: p, Permissions: perms}, nil
}

// IsTx reports whether q is a transaction.
func IsTx(q db.Querier) bool {
	_, ok := q.(pgx.Tx)
	return ok
}

var _ = slices.Contains[[]string]
