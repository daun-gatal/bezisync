package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/oidc"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/samlsp"
	"github.com/daun-gatal/bezisync/backend/internal/security"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/ssrf"
	"github.com/daun-gatal/bezisync/backend/internal/val"
	"github.com/daun-gatal/bezisync/backend/internal/version"
)

// Provider kinds beyond OpenID Connect: their fields, defaults, validation and tests. A change is
// applied to a copy of the provider, which is then validated as a whole for its kind, so no edit
// can leave a provider half-configured.

var kinds = []string{oidc.KindOIDC, oidc.KindOAuth2, oidc.KindSAML}

// GitHub's endpoints (github.com; GitHub Enterprise Server uses its own host and /api/v3).
var githubDefaults = map[string]string{ //nolint:gosec // G101: public GitHub endpoints, not credentials
	"authorization_url": "https://github.com/login/oauth/authorize",
	"token_url":         "https://github.com/login/oauth/access_token",
	"userinfo_url":      "https://api.github.com/user",
}

// claimRe is a JSON field name in a userinfo answer.
var claimRe = regexp.MustCompile(`^[A-Za-z0-9_:./-]{1,100}$`)

// kindInput are the fields of OAuth2 and SAML providers as sent (nil: not sent).
type kindInput struct {
	Kind, Flavor                                   *string
	AuthorizationURL, TokenURL, UserinfoURL        *string
	SubjectClaim, EmailClaim, EmailVerifiedClaim   *string
	NameClaim                                      *string
	SecretInBody, SignRequests, AllowIdPInitiated  *bool
	MetadataXML, MetadataURL, IdPEntityID, SSOURL  *string
	Certificate                                    *string
	EmailAttribute, NameAttribute, GroupsAttribute *string
	LinkExisting                                   *bool
	ResponseIssuer                                 *string
	sent                                           map[string]bool
}

func optStrField(v *val.V, o *val.Object, key string, max int) *string {
	f := o.Get(key)
	if !f.Present || f.IsNull() {
		return nil
	}
	s, ok := v.Constrained(f.Loc, f.Raw, val.StrRules{Max: max})
	if !ok {
		return nil
	}
	s = strings.TrimSpace(s)
	return &s
}

func parseKindFields(v *val.V, o *val.Object) kindInput {
	in := kindInput{sent: map[string]bool{}}
	for _, k := range []string{"kind", "flavor", "authorization_url", "token_url", "userinfo_url", "subject_claim", "email_claim",
		"email_verified_claim", "name_claim", "secret_in_body", "metadata_xml", "metadata_url", "idp_entity_id", "sso_url", "certificate",
		"sign_requests", "allow_idp_initiated", "email_attribute", "name_attribute", "groups_attribute", "response_issuer"} {
		if o.Has(k) {
			in.sent[k] = true
		}
	}
	if f := o.Get("kind"); f.Present {
		if s, ok := v.Literal(f.Loc, f.Raw, kinds...); ok {
			in.Kind = &s
		}
	}
	if f := o.Get("flavor"); f.Present {
		if s, ok := v.Literal(f.Loc, f.Raw, "generic", "github"); ok {
			in.Flavor = &s
		}
	}
	in.AuthorizationURL = optStrField(v, o, "authorization_url", 500)
	in.TokenURL = optStrField(v, o, "token_url", 500)
	in.UserinfoURL = optStrField(v, o, "userinfo_url", 500)
	in.SubjectClaim = optStrField(v, o, "subject_claim", 100)
	in.EmailClaim = optStrField(v, o, "email_claim", 100)
	in.EmailVerifiedClaim = optStrField(v, o, "email_verified_claim", 100)
	in.NameClaim = optStrField(v, o, "name_claim", 100)
	in.SecretInBody = optBool(v, o, "secret_in_body")
	in.MetadataXML = optStrField(v, o, "metadata_xml", samlsp.MaxMetadata)
	in.MetadataURL = optStrField(v, o, "metadata_url", 500)
	in.IdPEntityID = optStrField(v, o, "idp_entity_id", 500)
	in.SSOURL = optStrField(v, o, "sso_url", 500)
	in.Certificate = optStrField(v, o, "certificate", 20000)
	in.SignRequests = optBool(v, o, "sign_requests")
	in.AllowIdPInitiated = optBool(v, o, "allow_idp_initiated")
	in.EmailAttribute = optStrField(v, o, "email_attribute", 200)
	in.NameAttribute = optStrField(v, o, "name_attribute", 200)
	in.GroupsAttribute = optStrField(v, o, "groups_attribute", 200)
	in.LinkExisting = optBool(v, o, "link_existing")
	in.ResponseIssuer = optStrField(v, o, "response_issuer", 500)
	return in
}

// endpointURL validates an Admin-typed provider URL (https, or http where plain HTTP is allowed).
func endpointURL(v *val.V, loc val.Loc, s string) bool {
	u, err := url.Parse(s)
	if err != nil || u.Host == "" || u.Fragment != "" || u.User != nil || (u.Scheme != "https" && u.Scheme != "http") {
		v.ValueError(loc, "enter a full https URL")
		return false
	}
	if c := config.Get(); u.Scheme == "http" && c.Environment == "production" && !c.AllowPlainHTTP {
		v.ValueError(loc, "the URL must use https")
		return false
	}
	return true
}

func origin(s string) string {
	u, _ := url.Parse(s)
	return u.Scheme + "://" + u.Host
}

// fetchMetadata reads IdP metadata from its URL through the SSRF guard.
func fetchMetadata(ctx context.Context, u string) (*samlsp.Metadata, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil) //nolint:gosec // G704: an Admin's URL, dialled through the SSRF guard
	if err != nil {
		return nil, &samlsp.Error{Msg: "The metadata URL is not valid."}
	}
	req.Header.Set("User-Agent", "BeziSync/"+version.Version)
	resp, err := ssrf.HTTPClient(10 * time.Second).Do(req) //nolint:gosec // G704: the SSRF guard checks every address
	if err != nil {
		var blocked *ssrf.BlockedAddress
		if errors.As(err, &blocked) {
			return nil, &samlsp.Error{Msg: blocked.Error()}
		}
		return nil, &samlsp.Error{Msg: "The metadata URL is not reachable."}
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, &samlsp.Error{Msg: fmt.Sprintf("The metadata URL answered HTTP %d.", resp.StatusCode)}
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, samlsp.MaxMetadata+1))
	if err != nil {
		return nil, &samlsp.Error{Msg: "The metadata could not be read."}
	}
	return samlsp.ParseMetadata(raw)
}

func setCfg(m *models.SSOProvider, key string, v any) {
	if m.Config == nil {
		m.Config = db.Map{}
	}
	m.Config[key] = v
}

func certList(certs []string) db.List {
	out := db.List{}
	for _, c := range certs {
		out = append(out, c)
	}
	return out
}

// checkCerts refuses certificates that can never verify a response (expired, not valid yet).
func checkCerts(v *val.V, loc val.Loc, certs []string) {
	now := time.Now()
	usable := 0
	for _, c := range certs {
		cert, err := samlsp.ParseCertificate(c)
		if err != nil {
			v.ValueError(loc, err.Error())
			return
		}
		if now.Before(cert.NotAfter) && now.After(cert.NotBefore) {
			usable++
		}
	}
	if usable == 0 {
		v.ValueError(loc, "the identity provider's certificate has expired or is not valid yet")
	}
}

// resolveKind applies the kind-specific fields to m (a copy) and validates the whole provider.
// creating: m is new. Returns a 422 problem, or nil.
func resolveKind(ctx context.Context, m *models.SSOProvider, in ssoInput, k kindInput, creating bool) error {
	v := &val.V{}
	loc := func(f string) val.Loc { return val.Loc{"body", f} }
	if creating {
		m.Kind = oidc.KindOIDC
		if k.Kind != nil {
			m.Kind = *k.Kind
		}
	} else if k.Kind != nil && *k.Kind != m.Kind {
		v.ValueError(loc("kind"), "a provider's kind can't change; add a new sign-in option instead")
		return v.Problem()
	}
	notFor := func(names []string, what string) {
		for _, n := range names {
			if k.sent[n] || (n == "client_id" && in.ClientID != nil) || (n == "client_secret" && in.ClientSecret != nil) ||
				(n == "scopes" && in.Scopes != nil) || (n == "issuer" && in.Issuer != nil) || (n == "groups_claim" && in.GroupsClaim != nil) ||
				(n == "allow_unverified_email" && in.AllowUnverifiedEmail != nil) {
				v.ValueError(loc(n), "not used by "+what)
			}
		}
	}
	oauthOnly := []string{"flavor", "authorization_url", "token_url", "userinfo_url", "subject_claim", "secret_in_body", "response_issuer"}
	claimFields := []string{"email_claim", "email_verified_claim", "name_claim"}
	samlOnly := []string{"metadata_xml", "metadata_url", "idp_entity_id", "sso_url", "certificate", "sign_requests", "allow_idp_initiated",
		"email_attribute", "name_attribute", "groups_attribute"}
	setSecret := func() {
		if in.ClientSecret != nil {
			enc, err := security.EncryptSecret(map[string]any{"client_secret": *in.ClientSecret})
			if err != nil {
				v.ValueError(loc("client_secret"), "could not be stored")
				return
			}
			m.ClientSecretEnc = &enc
		}
		if m.ClientSecretEnc == nil {
			v.Fail(loc("client_secret"), "missing", "Field required")
		}
	}
	setScopes := func(def []string) {
		if in.Scopes != nil {
			m.Scopes = db.List{}
			for _, s := range in.Scopes {
				m.Scopes = append(m.Scopes, s)
			}
		} else if creating {
			m.Scopes = db.List{}
			for _, s := range def {
				m.Scopes = append(m.Scopes, s)
			}
		}
	}
	scopes := func() []string {
		out := []string{}
		for _, s := range m.Scopes {
			out = append(out, fmt.Sprint(s))
		}
		return out
	}
	if in.ClientID != nil {
		m.ClientID = *in.ClientID
	}
	if in.AllowUnverifiedEmail != nil {
		m.AllowUnverifiedEmail = *in.AllowUnverifiedEmail
	}
	if in.GroupsClaim != nil {
		m.GroupsClaim = *in.GroupsClaim
	}

	switch m.Kind {
	case oidc.KindOIDC:
		notFor(append(append([]string{}, oauthOnly...), samlOnly...), "OpenID Connect providers")
		kept := db.Map{}
		for _, key := range []string{"link_existing", "email_claim", "email_verified_claim", "name_claim"} {
			if val, ok := m.Config[key]; ok {
				kept[key] = val
			}
		}
		m.Config = kept
		// Empty claim names read the standard claims (email, email_verified, name).
		for key, sent := range map[string]*string{"email_claim": k.EmailClaim, "email_verified_claim": k.EmailVerifiedClaim, "name_claim": k.NameClaim} {
			if sent != nil {
				setCfg(m, key, *sent)
			}
			if c, _ := m.Config[key].(string); c != "" && !claimRe.MatchString(c) {
				v.ValueError(loc(key), "a claim is a field name such as email, or a path such as realm_access.roles")
			}
		}
		if in.Issuer != nil {
			m.Issuer = *in.Issuer
		}
		if m.Issuer == "" {
			v.Fail(loc("issuer"), "missing", "Field required")
		}
		if m.ClientID == "" {
			v.Fail(loc("client_id"), "missing", "Field required")
		}
		setSecret()
		setScopes([]string{"openid", "email", "profile"})
		if !slices.Contains(scopes(), "openid") {
			v.ValueError(loc("scopes"), "include the openid scope")
		}
		if m.GroupsClaim == "" {
			m.GroupsClaim = "groups"
		}
	case oidc.KindOAuth2:
		notFor(append([]string{"issuer"}, samlOnly...), "OAuth2 providers")
		flavor, _ := m.Config["flavor"].(string)
		if creating || flavor == "" {
			flavor = "generic"
		}
		if k.Flavor != nil {
			flavor = *k.Flavor
		}
		if !creating && m.Config["flavor"] != nil && flavor != m.Config["flavor"] {
			v.ValueError(loc("flavor"), "can't change; add a new sign-in option instead")
		}
		setCfg(m, "flavor", flavor)
		github := flavor == "github"
		str := func(key string, sent *string, def string) string {
			cur, _ := m.Config[key].(string)
			if sent != nil {
				cur = *sent
			} else if cur == "" {
				cur = def
			}
			setCfg(m, key, cur)
			return cur
		}
		urls := map[string]*string{"authorization_url": k.AuthorizationURL, "token_url": k.TokenURL, "userinfo_url": k.UserinfoURL}
		for _, key := range []string{"authorization_url", "token_url", "userinfo_url"} {
			def := ""
			if github {
				def = githubDefaults[key]
			}
			u := str(key, urls[key], def)
			if u == "" {
				v.Fail(loc(key), "missing", "Field required")
			} else {
				endpointURL(v, loc(key), u)
			}
		}
		if github {
			if u, _ := m.Config["userinfo_url"].(string); u != "" && !strings.HasSuffix(strings.TrimRight(u, "/"), "/user") {
				v.ValueError(loc("userinfo_url"), "GitHub's user URL ends in /user (https://api.github.com/user, or https://<host>/api/v3/user)")
			}
			for _, n := range []string{"subject_claim", "email_claim", "email_verified_claim", "name_claim", "groups_claim"} {
				if k.sent[n] || (n == "groups_claim" && in.GroupsClaim != nil) {
					v.ValueError(loc(n), "GitHub's fields are fixed")
				}
			}
			setCfg(m, "subject_claim", "id")
			setCfg(m, "email_claim", "email")
			setCfg(m, "email_verified_claim", "")
			setCfg(m, "name_claim", "name")
			m.GroupsClaim = ""
			if k.SecretInBody == nil {
				if _, ok := m.Config["secret_in_body"]; !ok {
					setCfg(m, "secret_in_body", true)
				}
			}
		} else {
			for key, def := range map[string]string{"subject_claim": "sub", "email_claim": "email", "email_verified_claim": "email_verified", "name_claim": "name"} {
				sent := map[string]*string{"subject_claim": k.SubjectClaim, "email_claim": k.EmailClaim, "email_verified_claim": k.EmailVerifiedClaim, "name_claim": k.NameClaim}[key]
				c := str(key, sent, def)
				if c == "" && key != "email_verified_claim" {
					v.Fail(loc(key), "missing", "Field required")
				} else if c != "" && !claimRe.MatchString(c) {
					v.ValueError(loc(key), "a claim is a field name such as sub or email")
				}
			}
			if m.GroupsClaim == "" {
				m.GroupsClaim = "groups"
			}
			if _, ok := m.Config["secret_in_body"]; !ok {
				setCfg(m, "secret_in_body", false)
			}
		}
		if k.SecretInBody != nil {
			setCfg(m, "secret_in_body", *k.SecretInBody)
		}
		// The issuer identifier the provider names in its callback (RFC 9207); checked when set.
		// github.com sends https://github.com/login/oauth.
		if k.ResponseIssuer != nil {
			setCfg(m, "response_issuer", *k.ResponseIssuer)
		} else if _, ok := m.Config["response_issuer"]; !ok {
			def := ""
			if u, _ := m.Config["authorization_url"].(string); github && strings.HasPrefix(u, "https://github.com/") {
				def = "https://github.com/login/oauth"
			}
			setCfg(m, "response_issuer", def)
		}
		if ri, _ := m.Config["response_issuer"].(string); ri != "" && !regexp.MustCompile(`^\S{1,500}$`).MatchString(ri) {
			v.ValueError(loc("response_issuer"), "an issuer identifier has no spaces, such as https://github.com/login/oauth")
		}
		if m.ClientID == "" {
			v.Fail(loc("client_id"), "missing", "Field required")
		}
		setSecret()
		if github {
			setScopes([]string{"read:user", "user:email", "read:org"})
			if !slices.Contains(scopes(), "user:email") {
				v.ValueError(loc("scopes"), "include user:email: BeziSync needs the verified email address")
			}
		} else {
			setScopes([]string{"openid", "email", "profile"})
		}
		if u, _ := m.Config["authorization_url"].(string); u != "" {
			m.Issuer = origin(u)
		}
	case oidc.KindSAML:
		notFor(append(append([]string{"issuer", "client_id", "client_secret", "scopes", "groups_claim", "allow_unverified_email"}, oauthOnly...), claimFields...), "SAML providers")
		sources := 0
		for _, s := range []bool{k.MetadataXML != nil, k.MetadataURL != nil, k.IdPEntityID != nil || k.SSOURL != nil || k.Certificate != nil} {
			if s {
				sources++
			}
		}
		if sources > 1 {
			v.ValueError(loc("metadata_xml"), "give the metadata, its URL, or the entity ID, sign-in URL and certificate: one of them")
			return v.Problem()
		}
		var md *samlsp.Metadata
		switch {
		case k.MetadataXML != nil:
			var err error
			if md, err = samlsp.ParseMetadata([]byte(*k.MetadataXML)); err != nil {
				v.ValueError(loc("metadata_xml"), err.Error())
			}
			setCfg(m, "metadata_url", "")
		case k.MetadataURL != nil:
			if endpointURL(v, loc("metadata_url"), *k.MetadataURL) {
				var err error
				if md, err = fetchMetadata(ctx, *k.MetadataURL); err != nil {
					v.ValueError(loc("metadata_url"), err.Error())
				}
				setCfg(m, "metadata_url", *k.MetadataURL)
			}
		case sources == 1:
			cur := &samlsp.Metadata{}
			cur.EntityID, _ = m.Config["idp_entity_id"].(string)
			cur.SSOURL, _ = m.Config["sso_url"].(string)
			cur.Certificates = services.CfgStrs(m, "certificates")
			if k.IdPEntityID != nil {
				cur.EntityID = *k.IdPEntityID
			}
			if k.SSOURL != nil {
				cur.SSOURL = *k.SSOURL
			}
			if k.Certificate != nil {
				cert, err := samlsp.ParseCertificate(*k.Certificate)
				if err != nil {
					v.ValueError(loc("certificate"), err.Error())
				} else {
					cur.Certificates = []string{samlsp.PEM(cert)}
				}
			}
			md = cur
			setCfg(m, "metadata_url", "")
		}
		if md != nil {
			setCfg(m, "idp_entity_id", md.EntityID)
			setCfg(m, "sso_url", md.SSOURL)
			setCfg(m, "certificates", certList(md.Certificates))
			if creating && k.SignRequests == nil && md.WantsSigned {
				setCfg(m, "sign_requests", true)
			}
		}
		entity, _ := m.Config["idp_entity_id"].(string)
		sso, _ := m.Config["sso_url"].(string)
		certs := services.CfgStrs(m, "certificates")
		if entity == "" {
			v.Fail(loc("idp_entity_id"), "missing", "Give the identity provider's metadata, its URL, or its entity ID")
		} else if !regexp.MustCompile(`^\S{1,500}$`).MatchString(entity) {
			v.ValueError(loc("idp_entity_id"), "an entity ID has no spaces")
		}
		if sso == "" {
			v.Fail(loc("sso_url"), "missing", "Field required")
		} else if p := samlsp.ValidSSOURL(sso); p != "" {
			v.ValueError(loc("sso_url"), p)
		}
		if len(certs) == 0 {
			v.Fail(loc("certificate"), "missing", "Field required")
		} else {
			checkCerts(v, loc("certificate"), certs)
		}
		for key, sent := range map[string]*bool{"sign_requests": k.SignRequests, "allow_idp_initiated": k.AllowIdPInitiated} {
			if sent != nil {
				setCfg(m, key, *sent)
			} else if _, ok := m.Config[key]; !ok {
				setCfg(m, key, false)
			}
		}
		for key, sent := range map[string]*string{"email_attribute": k.EmailAttribute, "name_attribute": k.NameAttribute, "groups_attribute": k.GroupsAttribute} {
			if sent != nil {
				setCfg(m, key, *sent)
			} else if _, ok := m.Config[key]; !ok {
				setCfg(m, key, "")
			}
		}
		m.Issuer, m.ClientID, m.ClientSecretEnc, m.Scopes, m.GroupsClaim, m.AllowUnverifiedEmail = entity, "", nil, db.List{}, "", false
	}
	// Every kind: whether people bound to another provider move to this one (same email).
	if k.LinkExisting != nil {
		setCfg(m, "link_existing", *k.LinkExisting)
	} else if _, ok := m.Config["link_existing"]; !ok {
		setCfg(m, "link_existing", false)
	}
	if p := v.Problem(); p != nil {
		return p
	}
	return nil
}

// testProvider checks what can be checked without a person signing in. For SAML with a metadata
// URL it also refreshes the certificates and sign-in URL (identity providers rotate them).
func testProvider(ctx context.Context, m *models.SSOProvider) (StoreTest, bool) {
	now := pytime.T(db.Now())
	ok := func(msg string) (StoreTest, bool) { return StoreTest{OK: true, Message: msg, TestedAt: now}, false }
	failed := func(msg string) (StoreTest, bool) { return StoreTest{Message: msg, TestedAt: now}, false }
	reach := func(u string) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil) //nolint:gosec // G704: an Admin's URL, dialled through the SSRF guard
		if err != nil {
			return err
		}
		req.Header.Set("User-Agent", "BeziSync/"+version.Version) // GitHub refuses requests without one
		c := ssrf.HTTPClient(10 * time.Second)
		c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		resp, err := c.Do(req) //nolint:gosec // G704: the SSRF guard checks every address
		if err != nil {
			return err
		}
		_ = resp.Body.Close()
		return nil
	}
	switch m.Kind {
	case oidc.KindOAuth2:
		for _, key := range []string{"authorization_url", "token_url", "userinfo_url"} {
			u, _ := m.Config[key].(string)
			if err := reach(u); err != nil {
				var blocked *ssrf.BlockedAddress
				if errors.As(err, &blocked) {
					return failed(blocked.Error())
				}
				return failed(fmt.Sprintf("%s is not reachable.", u))
			}
		}
		return ok("Reached the provider's sign-in, token and user endpoints. Finish with a real sign-in to check the client ID, secret and scopes.")
	case oidc.KindSAML:
		changed := false
		if mu, _ := m.Config["metadata_url"].(string); mu != "" {
			md, err := fetchMetadata(ctx, mu)
			if err != nil {
				return failed(err.Error())
			}
			if md.EntityID != m.Issuer {
				return failed(fmt.Sprintf("The metadata now describes %q, not %q: edit the provider to accept it.", md.EntityID, m.Issuer))
			}
			if md.SSOURL != m.Config["sso_url"] || strings.Join(md.Certificates, "") != strings.Join(services.CfgStrs(m, "certificates"), "") {
				setCfg(m, "sso_url", md.SSOURL)
				setCfg(m, "certificates", certList(md.Certificates))
				changed = true
			}
		}
		var soonest time.Time
		usable := 0
		for _, c := range services.CfgStrs(m, "certificates") {
			cert, err := samlsp.ParseCertificate(c)
			if err != nil {
				return failed(err.Error())
			}
			if time.Now().Before(cert.NotAfter) && time.Now().After(cert.NotBefore) {
				usable++
				if soonest.IsZero() || cert.NotAfter.Before(soonest) {
					soonest = cert.NotAfter
				}
			}
		}
		if usable == 0 {
			return StoreTest{Message: "The identity provider's certificate has expired: update it (or its metadata).", TestedAt: now}, changed
		}
		sso, _ := m.Config["sso_url"].(string)
		if err := reach(sso); err != nil {
			return StoreTest{Message: fmt.Sprintf("%s is not reachable.", sso), TestedAt: now}, changed
		}
		msg := "Reached the identity provider's sign-in URL and its certificate is valid"
		if days := int(time.Until(soonest).Hours() / 24); days < 30 {
			msg += fmt.Sprintf(" (it expires in %d days: plan the rotation)", days)
		}
		if changed {
			msg += "; refreshed the certificate and sign-in URL from the metadata"
		}
		return StoreTest{OK: true, Message: msg + ". Finish with a real sign-in.", TestedAt: now}, changed
	}
	return testSSOProvider(ctx, m.Issuer), false
}

// certInfo describes a SAML certificate for the settings page (never the key: there is none).
type certInfo struct {
	Subject     string      `json:"subject"`
	NotAfter    pytime.Time `json:"not_after"`
	Fingerprint string      `json:"sha256"`
}

func certInfos(m *models.SSOProvider) []certInfo {
	out := []certInfo{}
	for _, c := range services.CfgStrs(m, "certificates") {
		cert, err := samlsp.ParseCertificate(c)
		if err != nil {
			continue
		}
		sum := sha256.Sum256(cert.Raw)
		out = append(out, certInfo{Subject: cert.Subject.String(), NotAfter: pytime.T(cert.NotAfter), Fingerprint: hex.EncodeToString(sum[:])})
	}
	return out
}
