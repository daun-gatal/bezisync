package api_test

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"

	"github.com/daun-gatal/bezisync/backend/internal/config"
)

type fakeIdP struct {
	*httptest.Server
	key    *rsa.PrivateKey
	mu     sync.Mutex
	claims map[string]any
	nonce  string
	// the provider created for it in Settings → Sign-in
	id, version string
	admin       *client
	// issSupported advertises RFC 9207 (the callback must then carry iss).
	issSupported bool
}

// newFakeIdP starts an identity provider and adds it as a sign-in option (admin is an Admin).
func newFakeIdP(t *testing.T, admin *client) *fakeIdP {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	idp := &fakeIdP{key: key, claims: map[string]any{}}
	mux := http.NewServeMux()
	idp.Server = httptest.NewServer(mux)
	t.Cleanup(idp.Close)
	issuer := idp.URL
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		idp.mu.Lock()
		iss := idp.issSupported
		idp.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"issuer": issuer, "authorization_endpoint": issuer + "/auth", "token_endpoint": issuer + "/token",
			"jwks_uri": issuer + "/jwks", "id_token_signing_alg_values_supported": []string{"RS256"}, "authorization_response_iss_parameter_supported": iss})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &key.PublicKey, KeyID: "k1", Algorithm: "RS256", Use: "sig"}}})
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		user, pass, _ := r.BasicAuth()
		_ = r.ParseForm()
		if user != "bezisync" || pass != "s3cret" || r.Form.Get("code") != "the-code" || r.Form.Get("code_verifier") == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		idp.mu.Lock()
		now := time.Now().Unix()
		claims := map[string]any{"iss": issuer, "aud": "bezisync", "sub": "u-1", "iat": now, "exp": now + 300, "nonce": idp.nonce,
			"email": "Dana@Example.com", "email_verified": true, "name": "Dana", "groups": []string{"cdc-editors"}}
		for k, v := range idp.claims {
			if v == nil {
				delete(claims, k) // nil removes a default claim
				continue
			}
			claims[k] = v
		}
		idp.mu.Unlock()
		signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: key}, (&jose.SignerOptions{}).WithHeader("kid", "k1"))
		if err != nil {
			t.Error(err)
			return
		}
		payload, _ := json.Marshal(claims)
		obj, err := signer.Sign(payload)
		if err != nil {
			t.Error(err)
			return
		}
		token, _ := obj.CompactSerialize()
		_ = json.NewEncoder(w).Encode(map[string]any{"id_token": token})
	})
	created := admin.post("/api/v1/sso-providers", map[string]any{"label": "Example IdP", "issuer": issuer, "client_id": "bezisync", "client_secret": "s3cret",
		"scopes": []string{"openid", "email", "profile", "groups"}, "default_role": "editor", "default_projects": []string{"default"},
		"role_rules": []map[string]any{{"group": "cdc-editors", "project": "default", "role": "editor"},
			{"group": "approvers", "project": "*", "role": "viewer", "approver": true}}}).expect(201)
	if created.At("last_test", "ok") != true {
		t.Fatal(string(created.Raw))
	}
	idp.id, idp.version, idp.admin = created.Str("id"), fmt.Sprint(created.At("version")), admin
	return idp
}

// edit changes the provider in Settings → Sign-in.
func (idp *fakeIdP) edit(t *testing.T, change map[string]any) *resp {
	t.Helper()
	r := idp.admin.patch("/api/v1/sso-providers/"+idp.id, change, "If-Match", `"`+idp.version+`"`)
	if r.Status == 200 {
		idp.version = fmt.Sprint(r.At("version"))
	}
	return r
}

func (idp *fakeIdP) set(claims map[string]any) {
	idp.mu.Lock()
	defer idp.mu.Unlock()
	idp.claims = claims
}

// signIn runs the browser side of the authorization-code flow.
func signIn(t *testing.T, srv *httptest.Server, idp *fakeIdP, next, state string) (*client, *resp) {
	t.Helper()
	return signInIss(t, srv, idp, next, state, "")
}

// signInIss is signIn with an RFC 9207 iss in the callback ("" for none).
func signInIss(t *testing.T, srv *httptest.Server, idp *fakeIdP, next, state, iss string) (*client, *resp) {
	t.Helper()
	browser := newClient(t, srv)
	start := browser.get("/api/v1/auth/oidc/login?provider=" + idp.id + "&next=" + url.QueryEscape(next)).expect(302)
	loc, _ := url.Parse(start.Header.Get("location"))
	params := loc.Query()
	if params.Get("code_challenge_method") != "S256" || !strings.HasSuffix(params.Get("redirect_uri"), "/api/v1/auth/oidc/callback") {
		t.Fatal(loc)
	}
	idp.mu.Lock()
	idp.nonce = params.Get("nonce")
	idp.mu.Unlock()
	if state == "" {
		state = params.Get("state")
	}
	cb := "/api/v1/auth/oidc/callback?code=the-code&state=" + url.QueryEscape(state)
	if iss != "" {
		cb += "&iss=" + url.QueryEscape(iss)
	}
	back := browser.get(cb)
	return browser, back
}

func TestProvidersAndDisabled(t *testing.T) {
	srv := server(t)
	anon := newClient(t, srv)
	if p := anon.get("/api/v1/auth/providers").expect(200); string(p.Raw) != `{"password":true,"providers":[],"lab":false}` {
		t.Fatal(string(p.Raw))
	}
	anon.get("/api/v1/auth/oidc/login").expect(404)
}

func roleOf(t *testing.T, c *client) map[string][2]any {
	out := map[string][2]any{}
	for _, p := range c.get("/api/v1/auth/me").expect(200).List("projects") {
		out[at(t, p, "slug").(string)] = [2]any{at(t, p, "role"), hasPerm(t, p, "pipeline.approve")}
	}
	return out
}

func TestSSOSignInCreatesUserWithGroupRoles(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	idp := newFakeIdP(t, admin)
	if s := newClient(t, srv).get("/api/v1/auth/providers").Str("providers", 0, "label"); s != "Example IdP" {
		t.Fatal(s)
	}
	browser, back := signIn(t, srv, idp, "/p/default/pipelines", "")
	if back.Status != 302 || back.Header.Get("location") != "/p/default/pipelines" {
		t.Fatal(back.Status, back.Header)
	}
	if e := browser.get("/api/v1/auth/me").Str("user", "email"); e != "dana@example.com" {
		t.Fatal(e)
	}
	if r := roleOf(t, browser); len(r) != 1 || r["default"] != [2]any{"editor", false} {
		t.Fatal(r)
	}
	manual := admin.post("/api/v1/projects", map[string]any{"slug": "manual", "name": "Manual"}).expect(201).Str("id")
	admin.put("/api/v1/projects/"+manual+"/members", map[string]any{"email": "dana@example.com", "role": "operator"}).expect(200)
	idp.set(map[string]any{"groups": []string{"approvers"}})
	browser, _ = signIn(t, srv, idp, "/", "")
	r := roleOf(t, browser)
	if r["default"] != [2]any{"viewer", true} || r["manual"] != [2]any{"operator", false} {
		t.Fatal(r)
	}
}

func TestSSORejections(t *testing.T) {
	cases := []struct {
		claims  map[string]any
		state   string
		message string
	}{
		{map[string]any{}, "forged", "does not match"},
		{map[string]any{"nonce": "replayed"}, "", "not issued for this sign-in"},
		{map[string]any{"email_verified": false}, "", "not verified"},
		{map[string]any{"aud": "someone-else"}, "", "ID token is not valid"},
		{map[string]any{"email": nil}, "", "did not share an email"},
	}
	srv := server(t)
	idp := newFakeIdP(t, adminClient(t, srv))
	for _, c := range cases {
		idp.set(c.claims)
		browser, back := signIn(t, srv, idp, "/p/default/pipelines", c.state)
		loc := back.Header.Get("location")
		if back.Status != 302 || !strings.HasPrefix(loc, "/login?error=") || !strings.Contains(loc, strings.ReplaceAll(c.message, " ", "%20")) {
			t.Fatalf("%v: %d %s", c.claims, back.Status, loc)
		}
		browser.get("/api/v1/auth/me").expect(401)
	}
}

func TestOpenRedirectsAreRefused(t *testing.T) {
	srv := server(t)
	idp := newFakeIdP(t, adminClient(t, srv))
	for _, evil := range []string{"//evil.example", "https://evil.example", `/\evil.example`} {
		if _, back := signIn(t, srv, idp, evil, ""); back.Header.Get("location") != "/" {
			t.Fatal(evil, back.Header.Get("location"))
		}
	}
}

func TestUsersWithoutAMatchingGroupGetTheDefaultRole(t *testing.T) {
	srv := server(t)
	idp := newFakeIdP(t, adminClient(t, srv))
	idp.set(map[string]any{"groups": []string{}})
	browser, _ := signIn(t, srv, idp, "/", "")
	if r := roleOf(t, browser); len(r) != 1 || r["default"][0] != "editor" {
		t.Fatal(r)
	}
	idp.set(map[string]any{"groups": []string{"approvers"}})
	browser, _ = signIn(t, srv, idp, "/", "")
	if r := roleOf(t, browser); r["default"] != [2]any{"viewer", true} {
		t.Fatal(r)
	}
	idp.edit(t, map[string]any{"default_role": nil}).expect(200)
	idp.set(map[string]any{"groups": []string{"nobody"}})
	browser, _ = signIn(t, srv, idp, "/", "")
	if r := roleOf(t, browser); len(r) != 0 {
		t.Fatal(r)
	}
}

func TestAnAdminEditSurvivesTheNextSSOSignIn(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	idp := newFakeIdP(t, admin)
	idp.set(map[string]any{"groups": []string{}})
	signIn(t, srv, idp, "/", "")
	var dana any
	for _, m := range admin.get("/api/v1/projects/" + pid + "/members").List() {
		if at(t, m, "email") == "dana@example.com" {
			dana = m
		}
	}
	if at(t, dana, "role") != "editor" || at(t, dana, "can_approve") != false || at(t, dana, "granted_by_sso") != true {
		t.Fatal(dana)
	}
	promoted := admin.put("/api/v1/projects/"+pid+"/members", map[string]any{"email": "dana@example.com", "role": "editor", "can_approve": true}).expect(200)
	if promoted.At("granted_by_sso") != false {
		t.Fatal(string(promoted.Raw))
	}
	browser, _ := signIn(t, srv, idp, "/", "")
	if r := roleOf(t, browser); r["default"] != [2]any{"editor", true} {
		t.Fatal(r)
	}
	admin.put("/api/v1/projects/"+pid+"/members", map[string]any{"email": "dana@example.com", "role": "operator", "can_approve": true}).expect(200)
	browser, _ = signIn(t, srv, idp, "/", "")
	if r := roleOf(t, browser); r["default"] != [2]any{"operator", true} {
		t.Fatal(r)
	}
}

func TestPlainHTTPIsAnExplicitProductionOptIn(t *testing.T) {
	load := func(extra map[string]string) []string {
		env := map[string]string{"BEZISYNC_ENVIRONMENT": "production", "BEZISYNC_MASTER_KEY": newMasterKey(),
			"BEZISYNC_PUBLIC_URL": "http://bezisync.tailnet.ts.net", "BEZISYNC_COOKIE_SECURE": "false"}
		for k, v := range extra {
			env[k] = v
		}
		s, err := config.Load(func(k string) (string, bool) { v, ok := env[k]; return v, ok })
		if err != nil {
			t.Fatal(err)
		}
		return s.ValidateForProduction()
	}
	problems := strings.Join(load(nil), "; ")
	if !strings.Contains(problems, "must be https") || !strings.Contains(problems, "COOKIE_SECURE") {
		t.Fatal(problems)
	}
	if p := load(map[string]string{"BEZISYNC_ALLOW_PLAIN_HTTP": "true"}); len(p) != 0 {
		t.Fatal(p)
	}
}

func TestSignInProvidersAreAdminSettingsWithAWriteOnlySecret(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	editor := member(t, srv, admin, pid, "editor")
	editor.get("/api/v1/sso-providers").expect(403)
	editor.post("/api/v1/sso-providers", map[string]any{"label": "Mine", "issuer": "https://idp.example", "client_id": "x", "client_secret": "y"}).expect(403)
	editor.get("/api/v1/auth/settings").expect(403)
	idp := newFakeIdP(t, admin)
	listed := admin.get("/api/v1/sso-providers").expect(200)
	if strings.Contains(string(listed.Raw), "s3cret") || listed.Str(0, "client_id") != "bezisync" {
		t.Fatal(string(listed.Raw))
	}
	if r := listed.List(0, "role_rules"); len(r) != 2 || at(t, r[1], "project") != "*" || at(t, r[1], "approver") != true {
		t.Fatal(r)
	}
	bad := admin.post("/api/v1/sso-providers", map[string]any{"label": "Bad", "issuer": "ftp://idp", "client_id": "x", "client_secret": "y",
		"scopes": []string{"email"}, "role_rules": []map[string]any{{"group": "g", "project": "nope", "role": "owner"}}}).expect(422)
	for _, want := range []string{"issuer URL", "openid scope", "Input should be 'viewer', 'operator' or 'editor'"} {
		if !strings.Contains(string(bad.Raw), want) {
			t.Fatal(want, string(bad.Raw))
		}
	}
	unknown := admin.post("/api/v1/sso-providers", map[string]any{"label": "Bad", "issuer": idp.URL, "client_id": "x", "client_secret": "y",
		"role_rules": []map[string]any{{"group": "g", "project": "nope", "role": "viewer"}}}).expect(422)
	if !strings.Contains(string(unknown.Raw), "there is no project 'nope'") {
		t.Fatal(string(unknown.Raw))
	}
	admin.post("/api/v1/sso-providers", map[string]any{"label": "Example IdP", "issuer": idp.URL, "client_id": "x", "client_secret": "y"}).expect(409)
	admin.patch("/api/v1/sso-providers/"+idp.id, map[string]any{"label": "Stale"}, "If-Match", `"99"`).expect(412)
	// A new secret is used at the next sign-in; leaving it out keeps the stored one.
	idp.edit(t, map[string]any{"client_secret": "wrong"}).expect(200)
	_, back := signIn(t, srv, idp, "/", "")
	if loc := back.Header.Get("location"); !strings.Contains(loc, "refused") {
		t.Fatal(loc)
	}
	idp.edit(t, map[string]any{"client_secret": "s3cret"}).expect(200)
	idp.edit(t, map[string]any{"label": "Renamed"}).expect(200)
	if browser, _ := signIn(t, srv, idp, "/", ""); browser.get("/api/v1/auth/me").Status != 200 {
		t.Fatal("sign-in failed after a rename")
	}
	if r := admin.post("/api/v1/sso-providers/"+idp.id+"/test", nil).expect(200); r.At("ok") != true {
		t.Fatal(string(r.Raw))
	}
}

func TestPasswordSignInCanBeTurnedOffOnlyWithAWorkingProviderAndAnSSOAdmin(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	admin.put("/api/v1/auth/settings", map[string]any{"password_login": false}).expect(409)
	idp := newFakeIdP(t, admin)
	if c := admin.put("/api/v1/auth/settings", map[string]any{"password_login": false}).expect(409).Code(); c != "no_sso_admin" {
		t.Fatal(c)
	}
	// The Admin signs in once with the provider (same email), which links their account.
	idp.set(map[string]any{"email": adminEmail})
	signIn(t, srv, idp, "/", "")
	off := admin.put("/api/v1/auth/settings", map[string]any{"password_login": false}).expect(200)
	if off.At("password_login") != false || off.At("password_login_in_force") != false || off.At("sso_admins") != float64(1) {
		t.Fatal(string(off.Raw))
	}
	anon := newClient(t, srv)
	if p := anon.get("/api/v1/auth/providers"); p.At("password") != false {
		t.Fatal(string(p.Raw))
	}
	if c := anon.post("/api/v1/auth/login", map[string]any{"email": adminEmail, "password": adminPassword}).expect(403).Code(); c != "password_login_disabled" {
		t.Fatal(c)
	}
	// The last way in can be neither turned off nor removed.
	if c := idp.edit(t, map[string]any{"enabled": false}).expect(409).Code(); c != "last_sign_in_option" {
		t.Fatal(c)
	}
	admin.del("/api/v1/sso-providers/" + idp.id).expect(409)
	// API tokens keep working.
	tok := admin.post("/api/v1/auth/tokens", map[string]any{"name": "ci"}).expect(201).Str("token")
	bearer := newClient(t, srv)
	bearer.get("/api/v1/auth/me", "Authorization", "Bearer "+tok).expect(200)
	// The break-glass command turns it back on from the server.
	if code, out, _ := runCLI(t, "", "enable-password-login"); code != 0 || !strings.Contains(out, "is on") {
		t.Fatal(code, out)
	}
	anon.post("/api/v1/auth/login", map[string]any{"email": adminEmail, "password": adminPassword}).expect(200)
	admin.put("/api/v1/auth/settings", map[string]any{"password_login": false}).expect(200)
	admin.put("/api/v1/auth/settings", map[string]any{"password_login": true}).expect(200)
	idp.edit(t, map[string]any{"enabled": false}).expect(200)
	if p := newClient(t, srv).get("/api/v1/auth/providers"); len(p.List("providers")) != 0 {
		t.Fatal(string(p.Raw))
	}
}

func TestSignInWithATurnedOffOrUnknownProviderIsRefused(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	idp := newFakeIdP(t, admin)
	anon := newClient(t, srv)
	anon.get("/api/v1/auth/oidc/login?provider=" + db0).expect(404)
	browser := newClient(t, srv)
	start := browser.get("/api/v1/auth/oidc/login?provider=" + idp.id).expect(302)
	loc, _ := url.Parse(start.Header.Get("location"))
	idp.mu.Lock()
	idp.nonce = loc.Query().Get("nonce")
	idp.mu.Unlock()
	idp.edit(t, map[string]any{"enabled": false}).expect(200)
	back := browser.get("/api/v1/auth/oidc/callback?code=the-code&state=" + url.QueryEscape(loc.Query().Get("state")))
	if l := back.Header.Get("location"); !strings.Contains(l, "turned%20off") {
		t.Fatal(l)
	}
}

const db0 = "00000000-0000-0000-0000-000000000000"
