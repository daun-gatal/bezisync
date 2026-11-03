// Package oidc implements OpenID Connect sign-in (authorization code + PKCE).
//
// The login transaction (state, nonce, PKCE verifier, return path) travels in a short-lived,
// encrypted, HttpOnly cookie, so no server-side storage is needed. ID tokens are verified
// against the issuer's JWKS: signature, issuer, audience, expiry and nonce.
package oidc

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	gooidc "github.com/coreos/go-oidc/v3/oidc"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/security"
	"github.com/daun-gatal/bezisync/backend/internal/ssrf"
)

// Cookie settings of the login transaction.
const (
	Cookie     = "bz_oidc"
	CookiePath = "/api/v1/auth/oidc"
	TTL        = 600 * time.Second
)

// Error means sign-in failed; the message is safe to show.
type Error struct{ Msg string }

func (e *Error) Error() string { return e.Msg }

// Identity is a verified SSO identity.
type Identity struct {
	Subject, Email, Name string
	Groups               []string
	// EmailVerified: the provider vouched for the address. Only a verified address may be
	// linked to an existing account; an unverified one (allowed by an Admin) only creates one.
	EmailVerified bool
}

// Provider kinds.
const (
	KindOIDC   = "oidc"
	KindOAuth2 = "oauth2"
	KindSAML   = "saml"
)

// Provider is one configured identity provider (Settings → Sign-in) that signs in with an
// authorization code: OpenID Connect (ID token) or plain OAuth2 (userinfo endpoint).
type Provider struct {
	ID, Kind, Issuer, ClientID, ClientSecret string
	Scopes                                   []string
	GroupsClaim                              string
	AllowUnverifiedEmail                     bool
	// Claims to read (OIDC: from the ID token; OAuth2: from userinfo). Empty names use the
	// standard ones (email, email_verified, name).
	Claims Claims
	// OAuth2 only: endpoints, GitHub's extra lookups, and the issuer identifier the provider
	// puts in its callback (RFC 9207 iss; checked when set).
	AuthURL, TokenURL, UserinfoURL string
	ResponseIssuer                 string
	GitHub                         bool
	// SecretInBody sends the client secret as form fields (GitHub) instead of HTTP Basic.
	SecretInBody bool
}

// Claims name the userinfo fields of a plain OAuth2 provider.
type Claims struct {
	Subject, Email, EmailVerified, Name string
}

// RedirectURI is the callback URL, the same for every provider (the transaction names it).
func RedirectURI() string {
	return strings.TrimRight(config.Get().PublicURL, "/") + "/api/v1/auth/oidc/callback"
}

// client reaches identity providers through the SSRF guard: issuers are typed in by Admins.
func client() *http.Client {
	c := ssrf.HTTPClient(10 * time.Second)
	c.Transport = userAgent{c.Transport}
	return c
}

func claimName(set, def string) string {
	if set != "" {
		return set
	}
	return def
}

var (
	cacheMu   sync.Mutex
	discovery = map[string]struct {
		at  time.Time
		doc map[string]any
	}{}
	keysets = map[string]*gooidc.RemoteKeySet{}
)

// Discovery reads (and caches for an hour) the provider's configuration.
func Discovery(ctx context.Context, issuer string) (map[string]any, error) {
	issuer = strings.TrimRight(issuer, "/")
	cacheMu.Lock()
	cached, ok := discovery[issuer]
	cacheMu.Unlock()
	if ok && time.Since(cached.at) < time.Hour {
		return cached.doc, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, issuer+"/.well-known/openid-configuration", nil)
	if err != nil {
		return nil, &Error{"The identity provider is not reachable (InvalidURL)."}
	}
	resp, err := client().Do(req)
	if err != nil {
		return nil, &Error{"The identity provider is not reachable (ConnectError)."}
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		return nil, &Error{"The identity provider is not reachable (HTTPStatusError)."}
	}
	var doc map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return nil, &Error{"The identity provider is not reachable (JSONDecodeError)."}
	}
	cacheMu.Lock()
	discovery[issuer] = struct {
		at  time.Time
		doc map[string]any
	}{time.Now(), doc}
	cacheMu.Unlock()
	return doc, nil
}

// Test reads the issuer's discovery document and signing keys afresh (Settings → Sign-in → Test)
// and returns a plain sentence about what it found.
func Test(ctx context.Context, issuer string) (string, error) {
	issuer = strings.TrimRight(issuer, "/")
	cacheMu.Lock()
	delete(discovery, issuer)
	cacheMu.Unlock()
	meta, err := Discovery(ctx, issuer)
	if err != nil {
		return "", err
	}
	if got, _ := meta["issuer"].(string); strings.TrimRight(got, "/") != issuer {
		return "", &Error{fmt.Sprintf("The provider calls itself %q, not %q: use its exact issuer URL.", got, issuer)}
	}
	for _, key := range []string{"authorization_endpoint", "token_endpoint", "jwks_uri"} {
		if v, _ := meta[key].(string); v == "" {
			return "", &Error{fmt.Sprintf("The provider's discovery document has no %s.", key)}
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, meta["jwks_uri"].(string), nil)
	if err != nil {
		return "", &Error{"The provider's signing keys are not reachable (InvalidURL)."}
	}
	resp, err := client().Do(req)
	if err != nil {
		return "", &Error{"The provider's signing keys are not reachable (ConnectError)."}
	}
	defer func() { _ = resp.Body.Close() }()
	var set struct {
		Keys []any `json:"keys"`
	}
	if resp.StatusCode >= 400 || json.NewDecoder(resp.Body).Decode(&set) != nil || len(set.Keys) == 0 {
		return "", &Error{"The provider published no signing keys."}
	}
	return fmt.Sprintf("Found the provider and %d signing key(s). Finish with a real sign-in to check the client ID and secret.", len(set.Keys)), nil
}

func b64(raw []byte) string { return strings.TrimRight(base64.URLEncoding.EncodeToString(raw), "=") }

// Begin returns the authorization URL and the encrypted transaction cookie value.
func Begin(ctx context.Context, p *Provider, nextPath string) (string, string, error) {
	authURL := p.AuthURL
	if p.Kind != KindOAuth2 {
		meta, err := Discovery(ctx, p.Issuer)
		if err != nil {
			return "", "", err
		}
		authURL = fmt.Sprint(meta["authorization_endpoint"])
	}
	state, nonce, verifier := security.TokenURLSafe(24), security.TokenURLSafe(24), security.TokenURLSafe(48)
	sum := sha256.Sum256([]byte(verifier))
	params := url.Values{}
	params.Set("response_type", "code")
	params.Set("client_id", p.ClientID)
	params.Set("redirect_uri", RedirectURI())
	params.Set("scope", strings.Join(p.Scopes, " "))
	params.Set("state", state)
	if p.Kind != KindOAuth2 {
		params.Set("nonce", nonce)
	}
	params.Set("code_challenge", b64(sum[:]))
	params.Set("code_challenge_method", "S256")
	cookie, err := security.EncryptSecret(map[string]any{
		"state": state, "nonce": nonce, "verifier": verifier, "next": nextPath, "provider": p.ID,
		"exp": float64(time.Now().Add(TTL).UnixNano()) / 1e9,
	})
	if err != nil {
		return "", "", err
	}
	sep := "?"
	if strings.Contains(authURL, "?") {
		sep = "&"
	}
	return authURL + sep + encodePython(params), cookie, nil
}

// encodePython orders parameters like Python's urlencode of the dict (insertion order).
func encodePython(v url.Values) string {
	order := []string{"response_type", "client_id", "redirect_uri", "scope", "state", "nonce", "code_challenge", "code_challenge_method"}
	parts := make([]string, 0, len(order))
	for _, k := range order {
		if !v.Has(k) {
			continue
		}
		parts = append(parts, url.QueryEscape(k)+"="+url.QueryEscape(v.Get(k)))
	}
	return strings.Join(parts, "&")
}

// ErrUnknownProvider means the transaction names a provider that was removed or turned off.
var ErrUnknownProvider = &Error{"This sign-in option was turned off. Start again."}

// Finish exchanges the code, verifies the ID token and returns the identity and return path;
// find looks up the provider the transaction started with (nil when it is gone or turned off).
// iss is the callback's RFC 9207 issuer parameter ("" when absent).
func Finish(ctx context.Context, find func(id string) (*Provider, error), cookie, code, state, iss string) (*Identity, string, error) {
	if cookie == "" {
		return nil, "", &Error{"The sign-in session expired. Start again."}
	}
	tx, err := security.DecryptSecret(cookie)
	if err != nil {
		return nil, "", &Error{"The sign-in session is invalid. Start again."}
	}
	exp := 0.0
	if v, ok := tx["exp"]; ok {
		if n, ok := v.(json.Number); ok {
			exp, _ = n.Float64()
		}
	}
	if exp < float64(time.Now().UnixNano())/1e9 {
		return nil, "", &Error{"The sign-in session expired. Start again."}
	}
	if !security.ConstantTimeEqual(fmt.Sprint(tx["state"]), state) {
		return nil, "", &Error{"The sign-in response does not match this browser session."}
	}
	p, err := find(fmt.Sprint(tx["provider"]))
	if err != nil {
		return nil, "", err
	}
	if p == nil {
		return nil, "", ErrUnknownProvider
	}
	if p.Kind == KindOAuth2 {
		// Mix-up defence (RFC 9207): a provider with a known issuer identifier must name itself.
		if p.ResponseIssuer != "" && !security.ConstantTimeEqual(iss, p.ResponseIssuer) {
			return nil, "", &Error{"The sign-in response came from another identity provider than the one you chose. Start again."}
		}
		identity, err := finishOAuth2(ctx, p, fmt.Sprint(tx["verifier"]), code)
		if err != nil {
			return nil, "", err
		}
		return identity, nextOf(tx), nil
	}
	meta, err := Discovery(ctx, p.Issuer)
	if err != nil {
		return nil, "", err
	}
	// Mix-up defence (RFC 9207): when the response names its issuer it must be this provider's,
	// and a provider that promises to name itself must.
	metaIssuer, _ := meta["issuer"].(string)
	if metaIssuer == "" {
		metaIssuer = p.Issuer
	}
	if iss != "" && !security.ConstantTimeEqual(strings.TrimRight(iss, "/"), strings.TrimRight(metaIssuer, "/")) ||
		iss == "" && meta["authorization_response_iss_parameter_supported"] == true {
		return nil, "", &Error{"The sign-in response came from another identity provider than the one you chose. Start again."}
	}
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", RedirectURI())
	form.Set("code_verifier", fmt.Sprint(tx["verifier"]))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprint(meta["token_endpoint"]), strings.NewReader(form.Encode()))
	if err != nil {
		return nil, "", &Error{"The identity provider did not answer."}
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(p.ClientID, p.ClientSecret)
	resp, err := client().Do(req)
	if err != nil {
		return nil, "", &Error{"The identity provider did not answer."}
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "", &Error{fmt.Sprintf("The identity provider refused the sign-in (HTTP %d).", resp.StatusCode)}
	}
	var tokens map[string]any
	_ = json.Unmarshal(raw, &tokens)
	idToken, _ := tokens["id_token"].(string)
	if idToken == "" {
		return nil, "", &Error{"The identity provider returned no ID token."}
	}
	issuer := p.Issuer
	if v, ok := meta["issuer"].(string); ok && v != "" {
		issuer = v
	}
	algs := []string{"RS256"}
	if l, ok := meta["id_token_signing_alg_values_supported"].([]any); ok && len(l) > 0 {
		algs = algs[:0]
		for _, a := range l {
			algs = append(algs, fmt.Sprint(a))
		}
	}
	jwks := fmt.Sprint(meta["jwks_uri"])
	cacheMu.Lock()
	ks, ok := keysets[jwks]
	if !ok {
		ks = gooidc.NewRemoteKeySet(gooidc.ClientContext(context.WithoutCancel(ctx), client()), jwks)
		keysets[jwks] = ks
	}
	cacheMu.Unlock()
	verifier := gooidc.NewVerifier(issuer, ks, &gooidc.Config{ClientID: p.ClientID, SupportedSigningAlgs: algs})
	tok, err := verifier.Verify(ctx, idToken)
	if err != nil {
		return nil, "", &Error{fmt.Sprintf("The ID token is not valid (%s).", jwtErrorName(err))}
	}
	var claims map[string]any
	if err := tok.Claims(&claims); err != nil {
		return nil, "", &Error{"The ID token is not valid (DecodeError)."}
	}
	for _, required := range []string{"exp", "iat", "sub"} {
		if _, ok := claims[required]; !ok {
			return nil, "", &Error{"The ID token is not valid (MissingRequiredClaimError)."}
		}
	}
	nonce := ""
	if v, ok := claims["nonce"]; ok && v != nil {
		nonce = fmt.Sprint(v)
	}
	if !security.ConstantTimeEqual(nonce, fmt.Sprint(tx["nonce"])) {
		return nil, "", &Error{"The ID token was not issued for this sign-in."}
	}
	email := strings.ToLower(claimString(Lookup(claims, claimName(p.Claims.Email, "email"))))
	if email == "" {
		return nil, "", &Error{"The identity provider did not share an email address (add the email scope)."}
	}
	// Only an address the provider vouches for counts: a missing claim is not a yes (some
	// providers let users set any address, and an unverified one must never reach an account).
	verified := claimBool(Lookup(claims, claimName(p.Claims.EmailVerified, "email_verified")))
	if !verified && !p.AllowUnverifiedEmail {
		return nil, "", &Error{"Your email address is not verified at the identity provider."}
	}
	groups, ok := groupsFrom(claims, p.GroupsClaim)
	if !ok {
		return nil, "", ErrGroupsOverage
	}
	name := nameFrom(claims, p.Claims.Name)
	next := nextOf(tx)
	return &Identity{Subject: issuer + "|" + pyStr(claims["sub"]), Email: email, Name: name, Groups: groups, EmailVerified: verified}, next, nil
}

// nameFrom reads the display name: the configured claim, else name, preferred_username, or
// given and family name together.
func nameFrom(claims map[string]any, configured string) string {
	if configured != "" {
		if n := claimString(Lookup(claims, configured)); n != "" {
			return n
		}
	}
	for _, k := range []string{"name", "preferred_username"} {
		if n := claimString(claims[k]); n != "" {
			return n
		}
	}
	return strings.TrimSpace(claimString(claims["given_name"]) + " " + claimString(claims["family_name"]))
}

func nextOf(tx map[string]any) string {
	if v, ok := tx["next"]; ok && truthy(v) {
		return pyStr(v)
	}
	return "/"
}

func truthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case string:
		return x != ""
	case bool:
		return x
	case float64:
		return x != 0
	case []any:
		return len(x) > 0
	}
	return true
}

func pyStr(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case bool:
		if x {
			return "True"
		}
		return "False"
	case float64:
		if x == float64(int64(x)) {
			return fmt.Sprint(int64(x))
		}
	}
	return fmt.Sprint(v)
}

// jwtErrorName names verification failures like PyJWT's exception classes.
func jwtErrorName(err error) string {
	var expired *gooidc.TokenExpiredError
	msg := err.Error()
	switch {
	case errors.As(err, &expired):
		return "ExpiredSignatureError"
	case strings.Contains(msg, "audience"):
		return "InvalidAudienceError"
	case strings.Contains(msg, "issuer"):
		return "InvalidIssuerError"
	case strings.Contains(msg, "signature"):
		return "InvalidSignatureError"
	case strings.Contains(msg, "keys"):
		return "PyJWKClientError"
	}
	return "DecodeError"
}
