package services

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/security"
)

// OAuth 2.1 for MCP agents (BeziSync is the authorization server): dynamic client
// registration (public clients, PKCE S256 only), a consent page in the app, one-time codes,
// short-lived access tokens and rotating refresh tokens with reuse detection. Codes and tokens
// are stored hashed; every state change that two replicas could race on is a conditional
// UPDATE … RETURNING.

// OAuth lifetimes.
const (
	OAuthRequestTTL = 10 * time.Minute
	OAuthCodeTTL    = time.Minute
	OAuthAccessTTL  = time.Hour
)

// Client statuses.
const (
	ClientAllowed = "allowed"
	ClientPending = "pending"
	ClientBlocked = "blocked"
)

// OAuthError is an RFC 6749 error (error code and a description safe to show).
type OAuthError struct {
	Status      int
	Code        string
	Description string
}

func (e *OAuthError) Error() string { return e.Code + ": " + e.Description }

func oauthErr(status int, code, desc string) *OAuthError {
	return &OAuthError{Status: status, Code: code, Description: desc}
}

// RedirectProblem says what is wrong with a redirect URI ("" when acceptable).
// Accepted: https, http on a loopback address (native apps, RFC 8252 §7.3), and private-use
// schemes that look like a reversed domain (RFC 8252 §7.1). allowedHosts, when not empty,
// limits https hosts ("example.com" or "*.example.com").
func RedirectProblem(raw string, allowedHosts []string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Fragment != "" {
		return "must be an absolute URL without a fragment"
	}
	switch u.Scheme {
	case "https":
		if u.Host == "" {
			return "needs a host"
		}
		if len(allowedHosts) > 0 && !hostAllowed(u.Hostname(), allowedHosts) {
			return "is not on a host an Admin allowed for agents"
		}
		return ""
	case "http":
		host := u.Hostname()
		if host == "localhost" {
			return ""
		}
		if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
			return ""
		}
		return "plain http is allowed only on this computer (localhost)"
	default:
		if strings.Contains(u.Scheme, ".") {
			return ""
		}
		return "must use https, http on localhost, or an app scheme such as com.example.app"
	}
}

func hostAllowed(host string, allowed []string) bool {
	host = strings.ToLower(host)
	for _, a := range allowed {
		a = strings.ToLower(a)
		if strings.HasPrefix(a, "*.") {
			if strings.HasSuffix(host, a[1:]) {
				return true
			}
		} else if host == a {
			return true
		}
	}
	return false
}

// RegisterClient creates a public client (RFC 7591).
func RegisterClient(ctx context.Context, q db.Querier, name string, redirects []string, clientURI *string, s MCPSettings) (*models.OAuthClient, error) {
	if len(redirects) == 0 || len(redirects) > 10 {
		return nil, oauthErr(400, "invalid_redirect_uri", "Register 1 to 10 redirect URIs.")
	}
	uris := db.List{}
	for _, r := range redirects {
		if p := RedirectProblem(r, s.OAuthRedirectHosts); p != "" {
			return nil, oauthErr(400, "invalid_redirect_uri", fmt.Sprintf("%s %s.", r, p))
		}
		uris = append(uris, r)
	}
	name = strings.TrimSpace(name)
	if name == "" {
		name = "MCP agent"
	}
	if len([]rune(name)) > 200 {
		name = string([]rune(name)[:200])
	}
	status := ClientAllowed
	if s.OAuthRegistration == RegistrationApproval {
		status = ClientPending
	}
	c := &models.OAuthClient{ID: "bzc_" + security.TokenURLSafe(18), Name: name, RedirectURIs: uris, ClientURI: clientURI, Status: status, CreatedAt: db.Now()}
	if err := db.Insert(ctx, q, "oauth_clients", c); err != nil {
		return nil, err
	}
	_, err := Record(ctx, q, AuditEvent{Kind: "mcp.client_registered", Summary: fmt.Sprintf("MCP agent %s registered (%s)", name, status),
		ResourceType: "oauth_client", Details: map[string]any{"client_id": c.ID, "redirect_uris": []any(uris)}})
	return c, err
}

// AuthorizeParams are the parameters of /oauth/authorize.
type AuthorizeParams struct {
	ClientID, RedirectURI, ResponseType, CodeChallenge, Method, State, Resource string
}

// StartAuthorization validates an authorization request. A *OAuthError with Status 0 must be
// shown to the person (the redirect URI is not trusted); any other error is sent back to the
// agent's redirect URI.
func StartAuthorization(ctx context.Context, q db.Querier, a AuthorizeParams, resourceURL string) (*models.OAuthRequest, error) {
	client, err := db.Find[models.OAuthClient](ctx, q, `SELECT * FROM oauth_clients WHERE id = $1`, a.ClientID)
	if err != nil {
		return nil, err
	}
	if client == nil {
		return nil, oauthErr(0, "invalid_client", "This agent is not registered with BeziSync. Connect it again.")
	}
	registered := false
	for _, r := range client.RedirectURIs {
		if r == a.RedirectURI {
			registered = true
		}
	}
	if !registered {
		return nil, oauthErr(0, "invalid_request", "The agent asked to return to an address it did not register.")
	}
	if client.Status == ClientBlocked {
		return nil, oauthErr(400, "access_denied", "An Admin blocked this agent.")
	}
	if a.ResponseType != "code" {
		return nil, oauthErr(400, "unsupported_response_type", "Only the authorization code flow is supported.")
	}
	if a.Method != "S256" || len(a.CodeChallenge) < 43 || len(a.CodeChallenge) > 128 {
		return nil, oauthErr(400, "invalid_request", "PKCE with S256 is required.")
	}
	if a.Resource != "" && strings.TrimRight(a.Resource, "/") != resourceURL {
		return nil, oauthErr(400, "invalid_target", "BeziSync issues tokens for its MCP endpoint only ("+resourceURL+").")
	}
	if len(a.State) > 500 {
		return nil, oauthErr(400, "invalid_request", "The state is too long.")
	}
	now := db.Now()
	r := &models.OAuthRequest{ID: db.NewID(), ClientID: client.ID, RedirectURI: a.RedirectURI, CodeChallenge: a.CodeChallenge, Status: "pending",
		ExpiresAt: now.Add(OAuthRequestTTL), CreatedAt: now}
	if a.State != "" {
		r.State = &a.State
	}
	if a.Resource != "" {
		r.Resource = &a.Resource
	}
	if _, err := q.Exec(ctx, `DELETE FROM oauth_requests WHERE expires_at < $1`, now.Add(-time.Hour)); err != nil {
		return nil, err
	}
	return r, db.Insert(ctx, q, "oauth_requests", r)
}

// PendingRequest loads a request still waiting for a decision.
func PendingRequest(ctx context.Context, q db.Querier, id uuid.UUID) (*models.OAuthRequest, *models.OAuthClient, error) {
	r, err := db.Find[models.OAuthRequest](ctx, q, `SELECT * FROM oauth_requests WHERE id = $1`, id)
	if err != nil || r == nil {
		return nil, nil, err
	}
	if r.Status != "pending" || r.ExpiresAt.Before(db.Now()) {
		return nil, nil, nil
	}
	c, err := db.Find[models.OAuthClient](ctx, q, `SELECT * FROM oauth_clients WHERE id = $1`, r.ClientID)
	return r, c, err
}

// Redirect builds the agent's redirect URL with extra query parameters (RFC 9207 iss included).
func Redirect(r *models.OAuthRequest, issuer string, params map[string]string) string {
	u, _ := url.Parse(r.RedirectURI)
	v := u.Query()
	for k, val := range params {
		v.Set(k, val)
	}
	if r.State != nil {
		v.Set("state", *r.State)
	}
	v.Set("iss", issuer)
	u.RawQuery = v.Encode()
	return u.String()
}

// ApproveRequest records the person's decision and returns the one-time code.
func ApproveRequest(ctx context.Context, q db.Querier, r *models.OAuthRequest, p *auth.Principal, projectID *uuid.UUID, readOnly bool, days int) (string, error) {
	code := "bzk_" + security.TokenURLSafe(32)
	hash := security.TokenHash(code)
	now := db.Now()
	tag, err := q.Exec(ctx, `UPDATE oauth_requests SET status = 'approved', user_id = $1, project_id = $2, read_only = $3, grant_days = $4,
		code_hash = $5, expires_at = $6 WHERE id = $7 AND status = 'pending' AND expires_at > $8`,
		p.User.ID, projectID, readOnly, days, hash, now.Add(OAuthCodeTTL), r.ID, now)
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() != 1 {
		return "", oauthErr(0, "invalid_request", "This sign-in request was already used or has expired. Start again from your agent.")
	}
	return code, nil
}

// DenyRequest records a refusal.
func DenyRequest(ctx context.Context, q db.Querier, r *models.OAuthRequest) error {
	_, err := q.Exec(ctx, `UPDATE oauth_requests SET status = 'denied' WHERE id = $1 AND status = 'pending'`, r.ID)
	return err
}

// TokenPair is what the token endpoint returns.
type TokenPair struct {
	Access, Refresh string
	ExpiresIn       int
}

func newTokens() (access, refresh string) {
	return auth.OAuthAccessPrefix + security.TokenURLSafe(32), "bzr_" + security.TokenURLSafe(32)
}

func verifyPKCE(verifier, challenge string) bool {
	if len(verifier) < 43 || len(verifier) > 128 {
		return false
	}
	sum := sha256.Sum256([]byte(verifier))
	return security.ConstantTimeEqual(strings.TrimRight(base64.URLEncoding.EncodeToString(sum[:]), "="), challenge)
}

// ExchangeCode redeems a one-time code for a grant and its first tokens.
func ExchangeCode(ctx context.Context, q db.Querier, clientID, code, redirectURI, verifier string) (*TokenPair, error) {
	invalid := oauthErr(400, "invalid_grant", "The code is invalid, used or expired.")
	now := db.Now()
	r, err := db.Find[models.OAuthRequest](ctx, q, `UPDATE oauth_requests SET status = 'used' WHERE code_hash = ANY($1) AND status = 'approved'
		AND expires_at > $2 RETURNING *`, security.TokenHashes(code), now)
	if err != nil {
		return nil, err
	}
	if r == nil || r.ClientID != clientID || r.RedirectURI != redirectURI || r.UserID == nil {
		return nil, invalid
	}
	if !verifyPKCE(verifier, r.CodeChallenge) {
		return nil, oauthErr(400, "invalid_grant", "The code verifier does not match (PKCE).")
	}
	client, err := db.Find[models.OAuthClient](ctx, q, `SELECT * FROM oauth_clients WHERE id = $1`, clientID)
	if err != nil {
		return nil, err
	}
	if client == nil || client.Status != ClientAllowed {
		return nil, oauthErr(400, "invalid_client", "This agent is not allowed (yet). An Admin decides in Settings → MCP.")
	}
	access, refresh := newTokens()
	g := &models.OAuthGrant{ID: db.NewID(), ClientID: clientID, UserID: *r.UserID, ProjectID: r.ProjectID, ReadOnly: r.ReadOnly,
		AccessHash: security.TokenHash(access), AccessExpiresAt: now.Add(OAuthAccessTTL), RefreshHash: security.TokenHash(refresh),
		ExpiresAt: now.Add(time.Duration(r.GrantDays) * 24 * time.Hour), CreatedAt: now}
	if g.AccessExpiresAt.After(g.ExpiresAt) {
		g.AccessExpiresAt = g.ExpiresAt
	}
	if err := db.Insert(ctx, q, "oauth_grants", g); err != nil {
		return nil, err
	}
	if _, err := q.Exec(ctx, `UPDATE oauth_clients SET last_used_at = $1 WHERE id = $2`, now, clientID); err != nil {
		return nil, err
	}
	user, err := auth.GetUser(ctx, q, g.UserID)
	if err != nil {
		return nil, err
	}
	if _, err := Record(ctx, q, AuditEvent{Kind: "mcp.agent_connected", Summary: fmt.Sprintf("%s connected the MCP agent %s", user.Email, client.Name),
		Principal: &auth.Principal{User: user}, ProjectID: g.ProjectID, ResourceType: "oauth_grant", ResourceID: &g.ID,
		Details: map[string]any{"client_id": clientID, "read_only": g.ReadOnly, "days": r.GrantDays}}); err != nil {
		return nil, err
	}
	return &TokenPair{Access: access, Refresh: refresh, ExpiresIn: int(time.Until(g.AccessExpiresAt).Seconds())}, nil
}

// Refresh rotates a grant's tokens. A refresh token used twice revokes the grant (it was stolen,
// or the agent lost track of it): the agent signs in again.
func Refresh(ctx context.Context, q db.Querier, clientID, refresh string) (*TokenPair, error) {
	invalid := oauthErr(400, "invalid_grant", "The refresh token is invalid or expired. Sign in again.")
	hashes := security.TokenHashes(refresh)
	now := db.Now()
	g, err := db.Find[models.OAuthGrant](ctx, q, `SELECT * FROM oauth_grants WHERE refresh_hash = ANY($1)`, hashes)
	if err != nil {
		return nil, err
	}
	if g == nil {
		reused, err := db.Find[models.OAuthGrant](ctx, q, `UPDATE oauth_grants SET revoked_at = $1 WHERE previous_refresh_hash = ANY($2)
			AND revoked_at IS NULL RETURNING *`, now, hashes)
		if err != nil {
			return nil, err
		}
		if reused != nil {
			if _, err := Record(ctx, q, AuditEvent{Kind: "mcp.agent_revoked", Summary: "Revoked an MCP agent's access: its refresh token was used twice",
				ProjectID: reused.ProjectID, ResourceType: "oauth_grant", ResourceID: &reused.ID}); err != nil {
				return nil, err
			}
		}
		return nil, invalid
	}
	// A refresh token lives as long as its grant (an Admin caps grants in Settings → MCP).
	if g.ClientID != clientID || g.RevokedAt != nil || g.ExpiresAt.Before(now) {
		return nil, invalid
	}
	user, err := auth.GetUser(ctx, q, g.UserID)
	if err != nil {
		return nil, err
	}
	client, err := db.Find[models.OAuthClient](ctx, q, `SELECT * FROM oauth_clients WHERE id = $1`, clientID)
	if err != nil {
		return nil, err
	}
	if user == nil || !user.IsActive || client == nil || client.Status != ClientAllowed {
		return nil, invalid
	}
	access, next := newTokens()
	accessExp := now.Add(OAuthAccessTTL)
	if accessExp.After(g.ExpiresAt) {
		accessExp = g.ExpiresAt
	}
	updated, err := db.Find[models.OAuthGrant](ctx, q, `UPDATE oauth_grants SET access_hash = $1, access_expires_at = $2, refresh_hash = $3,
		previous_refresh_hash = refresh_hash, last_used_at = $4 WHERE id = $5 AND refresh_hash = $6 RETURNING *`,
		security.TokenHash(access), accessExp, security.TokenHash(next), now, g.ID, g.RefreshHash)
	if err != nil {
		return nil, err
	}
	if updated == nil { // another replica rotated it first
		return nil, invalid
	}
	return &TokenPair{Access: access, Refresh: next, ExpiresIn: int(time.Until(accessExp).Seconds())}, nil
}

// RevokeToken revokes the grant an access or refresh token belongs to (RFC 7009: unknown
// tokens are not an error).
func RevokeToken(ctx context.Context, q db.Querier, token string) error {
	hashes := security.TokenHashes(token)
	g, err := db.Find[models.OAuthGrant](ctx, q, `UPDATE oauth_grants SET revoked_at = $1 WHERE (access_hash = ANY($2) OR refresh_hash = ANY($2))
		AND revoked_at IS NULL RETURNING *`, db.Now(), hashes)
	if err != nil || g == nil {
		return err
	}
	_, err = Record(ctx, q, AuditEvent{Kind: "mcp.agent_revoked", Summary: "An MCP agent signed out", ProjectID: g.ProjectID,
		ResourceType: "oauth_grant", ResourceID: &g.ID})
	return err
}

// RevokeGrant ends one agent's access (its owner or an Admin).
func RevokeGrant(ctx context.Context, q db.Querier, g *models.OAuthGrant, p *auth.Principal) error {
	if _, err := q.Exec(ctx, `UPDATE oauth_grants SET revoked_at = $1 WHERE id = $2 AND revoked_at IS NULL`, db.Now(), g.ID); err != nil {
		return err
	}
	_, err := Record(ctx, q, AuditEvent{Kind: "mcp.agent_revoked", Summary: "Disconnected an MCP agent", Principal: p, ProjectID: g.ProjectID,
		ResourceType: "oauth_grant", ResourceID: &g.ID})
	return err
}
