package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// failure is the sign-in error a callback redirected to ("" when it signed in).
func failure(r *resp) string {
	loc := r.Header.Get("location")
	if !strings.HasPrefix(loc, "/login?error=") {
		return ""
	}
	msg, _ := url.QueryUnescape(strings.TrimPrefix(loc, "/login?error="))
	return msg
}

func TestOIDCNeedsAVerifiedEmailToReachAnAccount(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	idp := newFakeIdP(t, admin)
	// No email_verified claim (Entra ID by default): not a yes.
	idp.set(map[string]any{"email_verified": nil})
	if _, back := signIn(t, srv, idp, "/", ""); !strings.Contains(failure(back), "not verified") {
		t.Fatal(failure(back))
	}
	// Entra's xms_edov says the address's domain is verified: name it as the verified claim.
	idp.edit(t, map[string]any{"email_verified_claim": "xms_edov"}).expect(200)
	idp.set(map[string]any{"email_verified": nil, "xms_edov": true})
	if browser, back := signIn(t, srv, idp, "/", ""); failure(back) != "" || browser.get("/api/v1/auth/me").Str("user", "email") != "dana@example.com" {
		t.Fatal(failure(back))
	}
	// Accepting unverified addresses creates new accounts, never links an existing one:
	// otherwise anyone who types the Admin's address at the provider becomes the Admin.
	idp.edit(t, map[string]any{"email_verified_claim": "", "allow_unverified_email": true}).expect(200)
	idp.set(map[string]any{"email_verified": nil, "sub": "intruder", "email": adminEmail})
	if _, back := signIn(t, srv, idp, "/", ""); !strings.Contains(failure(back), "did not verify the address") {
		t.Fatal(failure(back))
	}
	idp.set(map[string]any{"email_verified": nil, "sub": "newcomer", "email": "new@example.com"})
	if browser, back := signIn(t, srv, idp, "/", ""); failure(back) != "" || browser.get("/api/v1/auth/me").Str("user", "email") != "new@example.com" {
		t.Fatal(failure(back))
	}
}

func TestOIDCClaimNamesAndNestedGroups(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	idp := newFakeIdP(t, admin)
	// Entra without a mailbox: the address is in upn; the name comes from given and family name.
	idp.edit(t, map[string]any{"email_claim": "upn", "default_role": nil,
		"role_rules": []map[string]any{{"group": "cdc-ops", "project": "default", "role": "operator"}}}).expect(200)
	idp.set(map[string]any{"email": nil, "upn": "Lee@Example.com", "name": nil, "given_name": "Lee", "family_name": "Park",
		"groups": nil, "realm_access": map[string]any{"roles": []any{"cdc-ops"}}})
	if _, back := signIn(t, srv, idp, "/", ""); failure(back) != "" {
		t.Fatal(failure(back))
	}
	// Keycloak's realm roles: a dotted path into the token.
	idp.edit(t, map[string]any{"groups_claim": "realm_access.roles"}).expect(200)
	browser, back := signIn(t, srv, idp, "/", "")
	if failure(back) != "" {
		t.Fatal(failure(back))
	}
	me := browser.get("/api/v1/auth/me")
	if me.Str("user", "email") != "lee@example.com" || me.Str("user", "name") != "Lee Park" {
		t.Fatal(string(me.Raw))
	}
	if r := roleOf(t, browser); r["default"] != [2]any{"operator", false} {
		t.Fatal(r)
	}
	// Auth0's namespaced claims contain dots: the exact name wins over a path.
	idp.edit(t, map[string]any{"groups_claim": "https://acme.com/groups"}).expect(200)
	idp.set(map[string]any{"email": nil, "upn": "lee@example.com", "https://acme.com/groups": []any{"cdc-ops"}})
	if browser, _ := signIn(t, srv, idp, "/", ""); roleOf(t, browser)["default"] != [2]any{"operator", false} {
		t.Fatal(roleOf(t, browser))
	}
	// Too many groups: Entra sends a link instead. Refused, never synced as "no groups".
	idp.edit(t, map[string]any{"groups_claim": "groups"}).expect(200)
	idp.set(map[string]any{"email": nil, "upn": "lee@example.com", "groups": nil, "_claim_names": map[string]any{"groups": "src1"},
		"_claim_sources": map[string]any{"src1": map[string]any{"endpoint": "https://graph.microsoft.com/v1.0/users/x/getMemberObjects"}}})
	if _, back := signIn(t, srv, idp, "/", ""); !strings.Contains(failure(back), "link to your groups") {
		t.Fatal(failure(back))
	}
	if r := roleOf(t, browser); r["default"] != [2]any{"operator", false} {
		t.Fatal("a refused sign-in changed roles", r)
	}
}

func TestOIDCChecksTheIssuerInTheCallback(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	idp := newFakeIdP(t, admin)
	if _, back := signInIss(t, srv, idp, "/", "", "https://evil.example"); !strings.Contains(failure(back), "another identity provider") {
		t.Fatal(back.Status, back.Header.Get("location"), failure(back)) // mix-up: another provider's response
	}
	if _, back := signInIss(t, srv, idp, "/", "", idp.URL); failure(back) != "" {
		t.Fatal(failure(back))
	}
	// A provider that promises to name itself must.
	idp.mu.Lock()
	idp.issSupported = true
	idp.mu.Unlock()
	admin.post("/api/v1/sso-providers/"+idp.id+"/test", nil).expect(200) // re-reads the discovery document
	if _, back := signInIss(t, srv, idp, "/", "", ""); !strings.Contains(failure(back), "another identity provider") {
		t.Fatal(failure(back))
	}
}

func TestOAuth2IssuerUserAgentAndGitHubSSO(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	gh := newFakeGitHub(t)
	var agents []string
	inner := gh.Config.Handler
	gh.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gh.mu.Lock()
		agents = append(agents, r.Header.Get("User-Agent"))
		gh.mu.Unlock()
		inner.ServeHTTP(w, r)
	})
	created := admin.post("/api/v1/sso-providers", map[string]any{"label": "GitHub", "kind": "oauth2", "flavor": "github", "client_id": "gh-client",
		"client_secret": "gh-secret", "authorization_url": gh.URL + "/login/oauth/authorize", "token_url": gh.URL + "/login/oauth/access_token",
		"userinfo_url": gh.URL + "/api/v3/user", "response_issuer": "https://github.com/login/oauth", "default_role": "viewer",
		"default_projects": []string{"default"}}).expect(201)
	if created.Str("oauth2", "response_issuer") != "https://github.com/login/oauth" {
		t.Fatal(string(created.Raw))
	}
	id := created.Str("id")
	start := func() (*client, string) {
		browser := newClient(t, srv)
		loc, _ := url.Parse(browser.get("/api/v1/auth/oidc/login?provider=" + id).expect(302).Header.Get("location"))
		return browser, loc.Query().Get("state")
	}
	// GitHub names itself in the callback (RFC 9207); another name, or none, is refused.
	for _, iss := range []string{"", "https://evil.example/login/oauth"} {
		b, state := start()
		cb := "/api/v1/auth/oidc/callback?code=c&state=" + url.QueryEscape(state)
		if iss != "" {
			cb += "&iss=" + url.QueryEscape(iss)
		}
		if msg := failure(b.get(cb)); !strings.Contains(msg, "another identity provider") {
			t.Fatal(iss, msg)
		}
	}
	b, state := start()
	if msg := failure(b.get("/api/v1/auth/oidc/callback?code=c&state=" + url.QueryEscape(state) + "&iss=" + url.QueryEscape("https://github.com/login/oauth"))); msg != "" {
		t.Fatal(msg)
	}
	gh.mu.Lock()
	for _, ua := range agents {
		if !strings.HasPrefix(ua, "BeziSync/") {
			t.Errorf("a request to GitHub had User-Agent %q", ua)
		}
	}
	gh.mu.Unlock()

	// An organization behind SAML single sign-on was left out: refused, not a silent role loss.
	gh.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/v3/user/orgs") {
			w.Header().Set("X-GitHub-SSO", "partial-results; organizations=21955855")
		}
		inner.ServeHTTP(w, r)
	})
	b, state = start()
	if msg := failure(b.get("/api/v1/auth/oidc/callback?code=c&state=" + url.QueryEscape(state) + "&iss=" + url.QueryEscape("https://github.com/login/oauth"))); !strings.Contains(msg, "SAML single sign-on") {
		t.Fatal(msg)
	}
	// The GitHub preset fills github.com's issuer identifier; GitHub Enterprise gets none.
	def := admin.post("/api/v1/sso-providers", map[string]any{"label": "GitHub.com", "kind": "oauth2", "flavor": "github", "client_id": "c",
		"client_secret": "s"}).expect(201)
	if def.Str("oauth2", "response_issuer") != "https://github.com/login/oauth" {
		t.Fatal(string(def.Raw))
	}
}

func TestGitHubAppTokensGetAClearMessage(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	mux := http.NewServeMux()
	app := httptest.NewServer(mux)
	t.Cleanup(app.Close)
	mux.HandleFunc("/login/oauth/access_token", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "ghu_apptoken", "token_type": "bearer"})
	})
	mux.HandleFunc("/api/v3/user", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 9, "login": "app-user"})
	})
	mux.HandleFunc("/api/v3/user/emails", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusForbidden) })
	created := admin.post("/api/v1/sso-providers", map[string]any{"label": "GH App", "kind": "oauth2", "flavor": "github", "client_id": "Iv1.x",
		"client_secret": "s", "authorization_url": app.URL + "/login/oauth/authorize", "token_url": app.URL + "/login/oauth/access_token",
		"userinfo_url": app.URL + "/api/v3/user", "default_role": "viewer", "default_projects": []string{"default"}}).expect(201)
	_, back := codeSignIn(t, srv, created.Str("id"), "/")
	if msg := failure(back); !strings.Contains(msg, "GitHub App") {
		t.Fatal(msg)
	}
}

func TestSAMLMergesRepeatedGroupAttributesAndRefusesOverage(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	idp := newSAMLIdP(t, "urn:idp:attrs")
	created := admin.post("/api/v1/sso-providers", map[string]any{"label": "SAML", "kind": "saml", "metadata_xml": idp.metadata("https://idp.example/sso"),
		"default_role": nil, "role_rules": []map[string]any{{"group": "b", "project": "default", "role": "editor"}}}).expect(201)
	// Each group as its own Attribute element (ADFS does this): both are read.
	browser, reqID, relay := samlStart(t, srv, created.Str("id"))
	r := acs(browser, idp.response(t, assertion{inResponseTo: reqID, groupsSplit: []string{"a", "b"}, nameParts: true}), relay)
	if r.Status != 303 {
		t.Fatal(errorOf(r))
	}
	if roleOf(t, browser)["default"] != [2]any{"editor", false} {
		t.Fatal(roleOf(t, browser))
	}
	if n := browser.get("/api/v1/auth/me").Str("user", "name"); n != "Sam" {
		t.Fatal(n) // displayName wins; given and surname only fill in
	}
	// Entra past 150 groups: a link instead of the groups is refused.
	b, reqID, relay := samlStart(t, srv, created.Str("id"))
	if msg := errorOf(acs(b, idp.response(t, assertion{inResponseTo: reqID, groupsLink: true}), relay)); !strings.Contains(msg, "link to your groups") {
		t.Fatal(msg)
	}
}
