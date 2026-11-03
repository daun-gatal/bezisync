package services

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"strings"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/oidc"
	"github.com/daun-gatal/bezisync/backend/internal/samlsp"
	"github.com/daun-gatal/bezisync/backend/internal/security"
)

// Sign-in providers of every kind: OpenID Connect, plain OAuth2 (GitHub, …) and SAML 2.0.
// sso_providers.config holds what a kind needs beyond the common columns.

func cfgStr(m *models.SSOProvider, key string) string {
	v, _ := m.Config[key].(string)
	return v
}

func cfgBool(m *models.SSOProvider, key string) bool {
	v, _ := m.Config[key].(bool)
	return v
}

// CfgStrs reads a list of strings from a provider's config.
func CfgStrs(m *models.SSOProvider, key string) []string {
	var l []any
	switch x := m.Config[key].(type) {
	case []any:
		l = x
	case db.List:
		l = x
	}
	out := make([]string, 0, len(l))
	for _, x := range l {
		if s, ok := x.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// CodeProvider is the sign-in configuration of an OpenID Connect or OAuth2 provider, with its
// client secret decrypted.
func CodeProvider(m *models.SSOProvider) (*oidc.Provider, error) {
	secret := ""
	if m.ClientSecretEnc != nil {
		sec, err := security.DecryptSecret(*m.ClientSecretEnc)
		if err != nil {
			return nil, err
		}
		secret, _ = sec["client_secret"].(string)
	}
	scopes := make([]string, 0, len(m.Scopes))
	for _, s := range m.Scopes {
		scopes = append(scopes, fmt.Sprint(s))
	}
	p := &oidc.Provider{ID: m.ID.String(), Kind: m.Kind, Issuer: m.Issuer, ClientID: m.ClientID, ClientSecret: secret, Scopes: scopes,
		GroupsClaim: m.GroupsClaim, AllowUnverifiedEmail: m.AllowUnverifiedEmail,
		Claims: oidc.Claims{Email: cfgStr(m, "email_claim"), EmailVerified: cfgStr(m, "email_verified_claim"), Name: cfgStr(m, "name_claim")}}
	if m.Kind == oidc.KindOAuth2 {
		p.AuthURL, p.TokenURL, p.UserinfoURL = cfgStr(m, "authorization_url"), cfgStr(m, "token_url"), cfgStr(m, "userinfo_url")
		p.Claims.Subject = cfgStr(m, "subject_claim")
		p.GitHub = cfgStr(m, "flavor") == "github"
		p.SecretInBody = cfgBool(m, "secret_in_body")
		p.ResponseIssuer = cfgStr(m, "response_issuer")
	}
	return p, nil
}

// SAMLIdP is a SAML provider's identity provider settings.
func SAMLIdP(m *models.SSOProvider) *samlsp.IdP {
	return &samlsp.IdP{EntityID: cfgStr(m, "idp_entity_id"), SSOURL: cfgStr(m, "sso_url"), Certificates: CfgStrs(m, "certificates"),
		SignRequests: cfgBool(m, "sign_requests"), AllowIdPInitiated: cfgBool(m, "allow_idp_initiated"),
		EmailAttr: cfgStr(m, "email_attribute"), NameAttr: cfgStr(m, "name_attribute"), GroupsAttr: cfgStr(m, "groups_attribute")}
}

const samlKeysState = "saml_sp"

// SAMLKeys returns BeziSync's SAML key pair, made on first use (one per install, shared by every
// replica: the first insert wins).
func SAMLKeys(ctx context.Context, q db.Querier) (*samlsp.Keys, string, error) {
	load := func() (*samlsp.Keys, string, bool, error) {
		row, err := db.Find[models.InstanceState](ctx, q, `SELECT * FROM instance_state WHERE key = $1`, samlKeysState)
		if err != nil || row == nil {
			return nil, "", false, err
		}
		enc, _ := row.Value["enc"].(string)
		sec, err := security.DecryptSecret(enc)
		if err != nil {
			return nil, "", false, err
		}
		key, _ := sec["key"].(string)
		cert, _ := sec["cert"].(string)
		k, err := samlsp.LoadKeys(key, cert)
		return k, cert, err == nil, err
	}
	if k, cert, ok, err := load(); err != nil || ok {
		return k, cert, err
	}
	key, cert, err := samlsp.NewKeys()
	if err != nil {
		return nil, "", err
	}
	enc, err := security.EncryptSecret(map[string]any{"key": key, "cert": cert})
	if err != nil {
		return nil, "", err
	}
	if _, err := q.Exec(ctx, `INSERT INTO instance_state (key, value, updated_at) VALUES ($1, $2, $3) ON CONFLICT (key) DO NOTHING`,
		samlKeysState, db.Map{"enc": enc}, db.Now()); err != nil {
		return nil, "", err
	}
	k, c, _, err := load()
	return k, c, err
}

// rotateSAMLKeys re-encrypts the SAML key pair under the current master key.
func rotateSAMLKeys(ctx context.Context, q db.Querier) (int, error) {
	row, err := db.Find[models.InstanceState](ctx, q, `SELECT * FROM instance_state WHERE key = $1`, samlKeysState)
	if err != nil || row == nil {
		return 0, err
	}
	enc, _ := row.Value["enc"].(string)
	if enc == "" || security.SecretKeyID(enc) == security.CurrentKeyID() {
		return 0, nil
	}
	plain, err := security.DecryptSecret(enc)
	if err != nil {
		return 0, err
	}
	if row.Value["enc"], err = security.EncryptSecret(plain); err != nil {
		return 0, err
	}
	return 1, saveInstanceState(ctx, q, row)
}

// SAML request lifetimes.
const samlRequestTTL = 10 * time.Minute

// StartSAML records an AuthnRequest for this browser and returns where to send it and the relay
// token the browser carries (as RelayState and a cookie).
func StartSAML(ctx context.Context, q db.Querier, m *models.SSOProvider, next string) (redirect, relay string, err error) {
	keys, _, err := SAMLKeys(ctx, q)
	if err != nil {
		return "", "", err
	}
	relay = security.TokenURLSafe(24)
	id, redirect, err := samlsp.BuildRequest(SAMLIdP(m), keys, relay)
	if err != nil {
		return "", "", err
	}
	now := db.Now()
	if _, err := q.Exec(ctx, `DELETE FROM sso_requests WHERE expires_at < $1`, now); err != nil {
		return "", "", err
	}
	if err := db.Insert(ctx, q, "sso_requests", &models.SSORequest{ID: id, ProviderID: m.ID, RelayHash: security.TokenHash(relay),
		NextPath: next, ExpiresAt: now.Add(samlRequestTTL)}); err != nil {
		return "", "", err
	}
	return redirect, relay, nil
}

// peekIssuer reads a response's Issuer without trusting it: only to pick which provider's
// certificate then verifies the whole response (IdP-initiated sign-in).
func peekIssuer(encoded string) string {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(raw) > 4*samlsp.MaxMetadata {
		return ""
	}
	var r struct {
		Issuer string `xml:"urn:oasis:names:tc:SAML:2.0:assertion Issuer"`
	}
	if xml.Unmarshal(raw, &r) != nil {
		return ""
	}
	return strings.TrimSpace(r.Issuer)
}

// FinishSAML verifies a posted response. relay is the RelayState it came with and cookie the
// relay token from this browser's cookie ("" when the browser sent none). requireCookie refuses
// responses this browser did not start (login CSRF), where the cookie can travel (https).
func FinishSAML(ctx context.Context, q db.Querier, encoded, relay, cookie string, requireCookie bool) (*oidc.Identity, *models.SSOProvider, string, error) {
	now := db.Now()
	var req *models.SSORequest
	if relay != "" {
		if cookie != "" && !security.ConstantTimeEqual(cookie, relay) || cookie == "" && requireCookie {
			return nil, nil, "", &samlsp.Error{Msg: "This sign-in was started in another browser. Start again."}
		}
		var err error
		// Single use: the row goes as it is read.
		req, err = db.Find[models.SSORequest](ctx, q, `DELETE FROM sso_requests WHERE relay_hash = ANY($1) RETURNING *`, security.TokenHashes(relay))
		if err != nil {
			return nil, nil, "", err
		}
		if req == nil || req.ExpiresAt.Before(now) {
			return nil, nil, "", &samlsp.Error{Msg: "The sign-in session expired or was already used. Start again."}
		}
	}
	var m *models.SSOProvider
	expected, next := "", "/"
	if req != nil {
		var err error
		if m, err = db.Find[models.SSOProvider](ctx, q, `SELECT * FROM sso_providers WHERE id = $1 AND enabled AND kind = 'saml'`, req.ProviderID); err != nil {
			return nil, nil, "", err
		}
		if m == nil {
			return nil, nil, "", &samlsp.Error{Msg: "This sign-in option was turned off. Start again."}
		}
		expected, next = req.ID, req.NextPath
	} else {
		issuer := peekIssuer(encoded)
		rows, err := db.Select[models.SSOProvider](ctx, q, `SELECT * FROM sso_providers WHERE enabled AND kind = 'saml' AND issuer = $1`, issuer)
		if err != nil {
			return nil, nil, "", err
		}
		for i := range rows {
			if cfgBool(&rows[i], "allow_idp_initiated") {
				m = &rows[i]
			}
		}
		if issuer == "" || m == nil {
			return nil, nil, "", &samlsp.Error{Msg: "Start signing in from BeziSync: this identity provider may not sign people in on its own."}
		}
	}
	keys, _, err := SAMLKeys(ctx, q)
	if err != nil {
		return nil, nil, "", err
	}
	res, err := samlsp.ParseResponse(SAMLIdP(m), keys, encoded, expected, now)
	if err != nil {
		return nil, nil, "", err
	}
	if _, err := q.Exec(ctx, `DELETE FROM sso_replays WHERE expires_at < $1`, now.Add(-samlsp.Skew)); err != nil {
		return nil, nil, "", err
	}
	replayKey := sha256.Sum256([]byte(m.Issuer + "\x00" + res.AssertionID))
	tag, err := q.Exec(ctx, `INSERT INTO sso_replays (id, expires_at) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING`,
		hex.EncodeToString(replayKey[:]), res.Expires.Add(samlsp.Skew))
	if err != nil {
		return nil, nil, "", err
	}
	if tag.RowsAffected() != 1 {
		return nil, nil, "", &samlsp.Error{Msg: "This SAML response was already used. Sign in again."}
	}
	// A signed assertion from the identity provider an Admin configured vouches for the address.
	return &oidc.Identity{Subject: "saml:" + m.Issuer + "|" + res.NameID, Email: res.Email, Name: res.Name, Groups: res.Groups, EmailVerified: true}, m, next, nil
}
