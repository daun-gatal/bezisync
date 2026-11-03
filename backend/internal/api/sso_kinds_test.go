package api_test

import (
	"bytes"
	"compress/flate"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beevik/etree"
	dsig "github.com/russellhaering/goxmldsig"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/samlsp"
)

// --- a fake GitHub (github.com plus api.github.com on one server, like GitHub Enterprise) ---

type fakeGitHub struct {
	*httptest.Server
	mu         sync.Mutex
	emails     []map[string]any
	tokenError string
	emailsCode int
	orgs       []string
	teams      [][2]string
	verifier   string
}

func newFakeGitHub(t *testing.T) *fakeGitHub {
	t.Helper()
	gh := &fakeGitHub{emails: []map[string]any{
		{"email": "Public@Example.com", "primary": false, "verified": false},
		{"email": "Octo@Example.com", "primary": true, "verified": true},
	}, orgs: []string{"Acme", "Other"}, teams: [][2]string{{"Acme", "data-platform"}}, emailsCode: 200}
	mux := http.NewServeMux()
	gh.Server = httptest.NewServer(mux)
	t.Cleanup(gh.Close)
	authed := func(w http.ResponseWriter, r *http.Request) bool {
		if r.Header.Get("Authorization") != "Bearer gho_token" || r.Header.Get("X-GitHub-Api-Version") == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return false
		}
		return true
	}
	mux.HandleFunc("/login/oauth/access_token", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		gh.mu.Lock()
		defer gh.mu.Unlock()
		gh.verifier = r.PostForm.Get("code_verifier")
		w.Header().Set("Content-Type", "application/json")
		// GitHub reads the client secret from the form and answers errors with 200.
		if gh.tokenError != "" || r.PostForm.Get("client_secret") != "gh-secret" || r.Header.Get("Accept") != "application/json" {
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "bad_verification_code"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "gho_token", "token_type": "bearer", "scope": "read:user,user:email,read:org"})
	})
	mux.HandleFunc("/api/v3/user", func(w http.ResponseWriter, r *http.Request) {
		if authed(w, r) {
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 12345, "login": "octo", "name": "Octo Cat", "email": "public@example.com"})
		}
	})
	mux.HandleFunc("/api/v3/user/emails", func(w http.ResponseWriter, r *http.Request) {
		gh.mu.Lock()
		defer gh.mu.Unlock()
		if authed(w, r) {
			w.WriteHeader(gh.emailsCode)
			_ = json.NewEncoder(w).Encode(gh.emails)
		}
	})
	mux.HandleFunc("/api/v3/user/orgs", func(w http.ResponseWriter, r *http.Request) {
		if !authed(w, r) {
			return
		}
		gh.mu.Lock()
		defer gh.mu.Unlock()
		// Two pages; the second's Link points at another host, which must not be followed.
		if r.URL.Query().Get("page") == "2" {
			w.Header().Set("Link", `<https://evil.example/user/orgs?page=3>; rel="next"`)
			_ = json.NewEncoder(w).Encode([]map[string]any{{"login": gh.orgs[1]}})
			return
		}
		w.Header().Set("Link", fmt.Sprintf(`<%s/api/v3/user/orgs?per_page=100&page=2>; rel="next"`, gh.URL))
		_ = json.NewEncoder(w).Encode([]map[string]any{{"login": gh.orgs[0]}})
	})
	mux.HandleFunc("/api/v3/user/teams", func(w http.ResponseWriter, r *http.Request) {
		if authed(w, r) {
			gh.mu.Lock()
			defer gh.mu.Unlock()
			var out []map[string]any
			for _, t := range gh.teams {
				out = append(out, map[string]any{"slug": t[1], "organization": map[string]any{"login": t[0]}})
			}
			_ = json.NewEncoder(w).Encode(out)
		}
	})
	return gh
}

// codeSignIn runs the browser side of an authorization-code sign-in with any provider.
func codeSignIn(t *testing.T, srv *httptest.Server, providerID, next string) (*client, *resp) {
	t.Helper()
	browser := newClient(t, srv)
	start := browser.get("/api/v1/auth/oidc/login?provider=" + providerID + "&next=" + url.QueryEscape(next)).expect(302)
	loc, _ := url.Parse(start.Header.Get("location"))
	if loc.Query().Get("code_challenge_method") != "S256" || loc.Query().Get("state") == "" {
		t.Fatal(loc)
	}
	back := browser.get("/api/v1/auth/oidc/callback?code=the-code&state=" + url.QueryEscape(loc.Query().Get("state")))
	return browser, back
}

func TestGitHubSignInUsesTheVerifiedEmailAndOrgsAndTeams(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	gh := newFakeGitHub(t)
	// Validation: GitHub needs user:email; its fields are fixed; SAML fields don't belong here.
	base := map[string]any{"label": "GitHub", "kind": "oauth2", "flavor": "github", "client_id": "gh-client", "client_secret": "gh-secret",
		"authorization_url": gh.URL + "/login/oauth/authorize", "token_url": gh.URL + "/login/oauth/access_token", "userinfo_url": gh.URL + "/api/v3/user"}
	with := func(extra map[string]any) map[string]any {
		out := map[string]any{}
		for k, v := range base {
			out[k] = v
		}
		for k, v := range extra {
			out[k] = v
		}
		return out
	}
	for want, body := range map[string]map[string]any{
		"include user:email":            with(map[string]any{"scopes": []string{"read:user"}}),
		"GitHub's fields are fixed":     with(map[string]any{"subject_claim": "login"}),
		"not used by OAuth2 providers":  with(map[string]any{"sso_url": "https://idp.example/sso"}),
		"ends in /user":                 with(map[string]any{"userinfo_url": gh.URL + "/api/v3/users"}),
		"not used by OAuth2 providers ": with(map[string]any{"issuer": "https://github.com"}),
	} {
		if r := admin.post("/api/v1/sso-providers", body).expect(422); !strings.Contains(string(r.Raw), strings.TrimSpace(want)) {
			t.Fatal(want, string(r.Raw))
		}
	}
	created := admin.post("/api/v1/sso-providers", with(map[string]any{"default_role": "viewer", "default_projects": []string{"default"},
		"role_rules": []map[string]any{{"group": "acme/data-platform", "project": "default", "role": "editor"}}})).expect(201)
	if created.Str("kind") != "oauth2" || created.Str("oauth2", "flavor") != "github" || created.At("oauth2", "secret_in_body") != true ||
		created.Str("issuer") != gh.URL || !strings.Contains(string(created.Raw), `"read:org"`) {
		t.Fatal(string(created.Raw))
	}
	id := created.Str("id")
	// The kind and flavor can't change.
	admin.patch("/api/v1/sso-providers/"+id, map[string]any{"kind": "saml"}, "If-Match", `"1"`).expect(422)

	browser, back := codeSignIn(t, srv, id, "/p/default/pipelines")
	if back.Status != 302 || back.Header.Get("location") != "/p/default/pipelines" {
		t.Fatal(back.Status, back.Header)
	}
	me := browser.get("/api/v1/auth/me").expect(200)
	if me.Str("user", "email") != "octo@example.com" || me.Str("user", "name") != "Octo Cat" {
		t.Fatal(string(me.Raw)) // the primary verified address, never the public profile one
	}
	if r := roleOf(t, browser); r["default"] != [2]any{"editor", false} {
		t.Fatal(r) // "acme/data-platform" from the teams endpoint
	}
	gh.mu.Lock()
	verifier := gh.verifier
	gh.mu.Unlock()
	if len(verifier) < 43 {
		t.Fatal("no PKCE verifier sent", verifier)
	}

	fail := func(change func(), want string) {
		t.Helper()
		gh.mu.Lock()
		change()
		gh.mu.Unlock()
		_, back := codeSignIn(t, srv, id, "/")
		if loc := back.Header.Get("location"); !strings.Contains(loc, url.QueryEscape(want)) && !strings.Contains(loc, strings.ReplaceAll(url.QueryEscape(want), "+", "%20")) {
			t.Fatalf("want %q, got %s", want, loc)
		}
	}
	fail(func() {
		gh.emails = []map[string]any{{"email": "octo@example.com", "primary": true, "verified": false}}
	}, "not verified")
	fail(func() { gh.emails = []map[string]any{} }, "did not share an email")
	fail(func() { gh.emailsCode = http.StatusForbidden }, "add the user:email scope")
	fail(func() { gh.emailsCode, gh.tokenError = 200, "bad" }, "bad_verification_code")
}

func TestGenericOAuth2SignInMapsClaims(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	var mu sync.Mutex
	info := map[string]any{"uid": 77, "mail": "Kim@Example.com", "verified": true, "display": "Kim", "teams": []any{"ops", 5}}
	mux := http.NewServeMux()
	idp := httptest.NewServer(mux)
	t.Cleanup(idp.Close)
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth() // HTTP Basic by default (RFC 6749 §2.3.1)
		if !ok || user != "c1" || pass != "s3cret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "at", "token_type": "Bearer"})
	})
	mux.HandleFunc("/me", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer at" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		_ = json.NewEncoder(w).Encode(info)
	})
	admin.post("/api/v1/sso-providers", map[string]any{"label": "Bad", "kind": "oauth2", "client_id": "c1", "client_secret": "s"}).expect(422)
	created := admin.post("/api/v1/sso-providers", map[string]any{"label": "Gitea", "kind": "oauth2", "client_id": "c1", "client_secret": "s3cret",
		"authorization_url": idp.URL + "/authorize", "token_url": idp.URL + "/token", "userinfo_url": idp.URL + "/me",
		"subject_claim": "uid", "email_claim": "mail", "email_verified_claim": "verified", "name_claim": "display", "groups_claim": "teams",
		"scopes": []string{"read"}, "default_projects": []string{"default"},
		"role_rules": []map[string]any{{"group": "ops", "project": "default", "role": "operator"}}}).expect(201)
	browser, back := codeSignIn(t, srv, created.Str("id"), "/")
	if back.Status != 302 || back.Header.Get("location") != "/" {
		t.Fatal(back.Header)
	}
	if e := browser.get("/api/v1/auth/me").Str("user", "email"); e != "kim@example.com" {
		t.Fatal(e)
	}
	if r := roleOf(t, browser); r["default"] != [2]any{"operator", false} {
		t.Fatal(r)
	}
	for _, c := range []struct {
		set  func()
		want string
	}{
		{func() { info["verified"] = false }, "not verified"},
		{func() { info["verified"], info["uid"] = true, "" }, "sent no"},
		{func() { info["uid"], info["mail"] = 77, "not-an-email" }, "not valid"},
	} {
		mu.Lock()
		c.set()
		mu.Unlock()
		_, back := codeSignIn(t, srv, created.Str("id"), "/")
		if loc := back.Header.Get("location"); !strings.Contains(loc, strings.ReplaceAll(url.QueryEscape(c.want), "+", "%20")) {
			t.Fatalf("want %q, got %s", c.want, loc)
		}
	}
}

// --- a SAML identity provider that signs real responses ---

type samlIdP struct {
	ks       dsig.X509KeyStore
	certPEM  string
	entityID string
}

func newSAMLIdP(t *testing.T, entityID string) *samlIdP {
	t.Helper()
	ks := dsig.RandomKeyStoreForTest()
	_, der, err := ks.GetKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	return &samlIdP{ks: ks, entityID: entityID, certPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))}
}

func (idp *samlIdP) metadata(sso string) string {
	cert, _ := x509.ParseCertificate(mustDER(idp.certPEM))
	return fmt.Sprintf(`<EntityDescriptor xmlns="urn:oasis:names:tc:SAML:2.0:metadata" entityID="%s"><IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol"><KeyDescriptor use="signing"><KeyInfo xmlns="http://www.w3.org/2000/09/xmldsig#"><X509Data><X509Certificate>%s</X509Certificate></X509Data></KeyInfo></KeyDescriptor><SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="%s"/></IDPSSODescriptor></EntityDescriptor>`,
		idp.entityID, base64.StdEncoding.EncodeToString(cert.Raw), sso)
}

func mustDER(p string) []byte {
	b, _ := pem.Decode([]byte(p))
	return b.Bytes
}

// assertion describes one response; zero values are the valid defaults.
type assertion struct {
	inResponseTo, issuer, audience, recipient, destination, nameID, nameFormat, email, status string
	notBefore, notOnOrAfter                                                                   time.Time
	noAudience, unsigned, tamper, twoAssertions, otherKey                                     bool
	groups                                                                                    []string
	id                                                                                        string
	groupsSplit                                                                               []string // one Attribute element per group
	nameParts, groupsLink                                                                     bool
}

func (idp *samlIdP) response(t *testing.T, a assertion) string {
	t.Helper()
	now := time.Now().UTC()
	def := func(v *string, d string) {
		if *v == "" {
			*v = d
		}
	}
	def(&a.issuer, idp.entityID)
	def(&a.audience, samlsp.EntityID())
	def(&a.recipient, samlsp.ACSURL())
	def(&a.destination, samlsp.ACSURL())
	def(&a.nameID, "u-1")
	def(&a.nameFormat, "urn:oasis:names:tc:SAML:2.0:nameid-format:persistent")
	def(&a.email, "Sam@Example.com")
	def(&a.status, "urn:oasis:names:tc:SAML:2.0:status:Success")
	def(&a.id, fmt.Sprintf("_a%d", time.Now().UnixNano()))
	if a.notBefore.IsZero() {
		a.notBefore = now.Add(-time.Minute)
	}
	if a.notOnOrAfter.IsZero() {
		a.notOnOrAfter = now.Add(5 * time.Minute)
	}
	ts := func(t time.Time) string { return t.Format(time.RFC3339) }
	build := func(id, email string) *etree.Element {
		el := etree.NewElement("saml:Assertion")
		el.CreateAttr("xmlns:saml", "urn:oasis:names:tc:SAML:2.0:assertion")
		el.CreateAttr("ID", id)
		el.CreateAttr("Version", "2.0")
		el.CreateAttr("IssueInstant", ts(now))
		el.CreateElement("saml:Issuer").SetText(a.issuer)
		subj := el.CreateElement("saml:Subject")
		nid := subj.CreateElement("saml:NameID")
		nid.CreateAttr("Format", a.nameFormat)
		nid.SetText(a.nameID)
		sc := subj.CreateElement("saml:SubjectConfirmation")
		sc.CreateAttr("Method", "urn:oasis:names:tc:SAML:2.0:cm:bearer")
		scd := sc.CreateElement("saml:SubjectConfirmationData")
		if a.inResponseTo != "" {
			scd.CreateAttr("InResponseTo", a.inResponseTo)
		}
		scd.CreateAttr("Recipient", a.recipient)
		scd.CreateAttr("NotOnOrAfter", ts(a.notOnOrAfter))
		cond := el.CreateElement("saml:Conditions")
		cond.CreateAttr("NotBefore", ts(a.notBefore))
		cond.CreateAttr("NotOnOrAfter", ts(a.notOnOrAfter))
		if !a.noAudience {
			cond.CreateElement("saml:AudienceRestriction").CreateElement("saml:Audience").SetText(a.audience)
		}
		attrs := el.CreateElement("saml:AttributeStatement")
		add := func(name string, values ...string) {
			at := attrs.CreateElement("saml:Attribute")
			at.CreateAttr("Name", name)
			for _, v := range values {
				at.CreateElement("saml:AttributeValue").SetText(v)
			}
		}
		if email != "" {
			add("http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress", email)
		}
		add("displayName", "Sam")
		if len(a.groups) > 0 {
			add("groups", a.groups...)
		}
		for _, g := range a.groupsSplit {
			add("groups", g)
		}
		if a.nameParts {
			add("http://schemas.xmlsoap.org/ws/2005/05/identity/claims/givenname", "Sam")
			add("http://schemas.xmlsoap.org/ws/2005/05/identity/claims/surname", "Lee")
		}
		if a.groupsLink {
			add("http://schemas.microsoft.com/claims/groups.link", "https://graph.microsoft.com/v1.0/users/x/getMemberObjects")
		}
		return el
	}
	ks := idp.ks
	if a.otherKey {
		ks = dsig.RandomKeyStoreForTest()
	}
	sign := func(el *etree.Element) *etree.Element {
		if a.unsigned {
			return el
		}
		ctx := dsig.NewDefaultSigningContext(ks)
		ctx.Canonicalizer = dsig.MakeC14N10ExclusiveCanonicalizerWithPrefixList("") // what IdPs use
		signed, err := ctx.SignEnveloped(el)
		if err != nil {
			t.Fatal(err)
		}
		return signed
	}
	doc := etree.NewDocument()
	resp := doc.CreateElement("samlp:Response")
	resp.CreateAttr("xmlns:samlp", "urn:oasis:names:tc:SAML:2.0:protocol")
	resp.CreateAttr("xmlns:saml", "urn:oasis:names:tc:SAML:2.0:assertion")
	resp.CreateAttr("ID", "_r"+a.id)
	resp.CreateAttr("Version", "2.0")
	resp.CreateAttr("IssueInstant", ts(now))
	resp.CreateAttr("Destination", a.destination)
	if a.inResponseTo != "" {
		resp.CreateAttr("InResponseTo", a.inResponseTo)
	}
	resp.CreateElement("saml:Issuer").SetText(a.issuer)
	resp.CreateElement("samlp:Status").CreateElement("samlp:StatusCode").CreateAttr("Value", a.status)
	signed := sign(build(a.id, a.email))
	if a.tamper { // change the email after signing
		for _, v := range signed.FindElements(".//AttributeValue") {
			if strings.Contains(v.Text(), "@") {
				v.SetText("admin@bezisync.local")
			}
		}
	}
	resp.AddChild(signed)
	if a.twoAssertions { // XML wrapping: a second, unsigned assertion for someone else
		resp.AddChild(build(a.id+"x", "admin@bezisync.local"))
	}
	raw, err := doc.WriteToBytes()
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(raw)
}

// samlStart begins a sign-in and returns the browser, the AuthnRequest ID and the RelayState.
func samlStart(t *testing.T, srv *httptest.Server, providerID string) (*client, string, string) {
	t.Helper()
	browser := newClient(t, srv)
	start := browser.get("/api/v1/auth/oidc/login?provider=" + providerID + "&next=%2Fp%2Fdefault%2Fpipelines").expect(302)
	loc, _ := url.Parse(start.Header.Get("location"))
	raw, err := base64.StdEncoding.DecodeString(loc.Query().Get("SAMLRequest"))
	if err != nil {
		t.Fatal(loc)
	}
	xmlReq, err := io.ReadAll(flate.NewReader(bytes.NewReader(raw)))
	if err != nil {
		t.Fatal(err)
	}
	doc := etree.NewDocument()
	if err := doc.ReadFromBytes(xmlReq); err != nil {
		t.Fatal(err)
	}
	if doc.Root().SelectAttrValue("AssertionConsumerServiceURL", "") != samlsp.ACSURL() {
		t.Fatal(string(xmlReq))
	}
	return browser, doc.Root().SelectAttrValue("ID", ""), loc.Query().Get("RelayState")
}

func acs(c *client, response, relay string) *resp {
	form := url.Values{"SAMLResponse": {response}, "RelayState": {relay}}.Encode()
	return c.do("POST", "/api/v1/auth/saml/acs", form, "Content-Type", "application/x-www-form-urlencoded")
}

func errorOf(r *resp) string {
	loc := r.Header.Get("location")
	if !strings.HasPrefix(loc, "/login?error=") {
		return ""
	}
	msg, _ := url.QueryUnescape(strings.TrimPrefix(loc, "/login?error="))
	return msg
}

func TestSAMLSignInRefusesEveryForgeryAndReplay(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	idp := newSAMLIdP(t, "https://idp.example.com/saml")
	sso := "https://idp.example.com/sso"
	md := admin.get("/api/v1/auth/saml/metadata").expect(200)
	if !strings.Contains(string(md.Raw), `entityID="`+samlsp.EntityID()+`"`) || !strings.Contains(string(md.Raw), samlsp.ACSURL()) {
		t.Fatal(string(md.Raw))
	}

	// Validation.
	bad := map[string]map[string]any{
		"not used by SAML providers": {"label": "S", "kind": "saml", "metadata_xml": idp.metadata(sso), "client_secret": "x"},
		"one of them":                {"label": "S", "kind": "saml", "metadata_xml": idp.metadata(sso), "certificate": idp.certPEM},
		"HTTP-Redirect":              {"label": "S", "kind": "saml", "metadata_xml": strings.ReplaceAll(idp.metadata(sso), "HTTP-Redirect", "HTTP-POST")},
		"This is not SAML metadata":  {"label": "S", "kind": "saml", "metadata_xml": "<html/>"},
		"Field required":             {"label": "S", "kind": "saml", "idp_entity_id": "urn:x"},
		"neither PEM nor base64":     {"label": "S", "kind": "saml", "idp_entity_id": "urn:x", "sso_url": sso, "certificate": "nope!"},
	}
	for want, body := range bad {
		if r := admin.post("/api/v1/sso-providers", body).expect(422); !strings.Contains(string(r.Raw), want) {
			t.Fatal(want, string(r.Raw))
		}
	}
	created := admin.post("/api/v1/sso-providers", map[string]any{"label": "Okta SAML", "kind": "saml", "metadata_xml": idp.metadata(sso),
		"default_projects": []string{"default"}, "default_role": "viewer",
		"role_rules": []map[string]any{{"group": "cdc-admins", "admin": true}}}).expect(201)
	id := created.Str("id")
	if created.Str("saml", "idp_entity_id") != idp.entityID || created.Str("saml", "sso_url") != sso || len(created.List("saml", "certificates")) != 1 ||
		created.Str("client_id") != "" {
		t.Fatal(string(created.Raw))
	}
	admin.post("/api/v1/sso-providers", map[string]any{"label": "Dup", "kind": "saml", "metadata_xml": idp.metadata(sso)}).expect(409)

	// A good sign-in.
	browser, reqID, relay := samlStart(t, srv, id)
	ok := acs(browser, idp.response(t, assertion{inResponseTo: reqID, groups: []string{"cdc-admins"}}), relay)
	if ok.Status != 303 || ok.Header.Get("location") != "/p/default/pipelines" {
		t.Fatal(ok.Status, ok.Header, errorOf(ok))
	}
	me := browser.get("/api/v1/auth/me").expect(200)
	if me.Str("user", "email") != "sam@example.com" || me.At("user", "is_superuser") != true {
		t.Fatal(string(me.Raw))
	}
	// The same response again: the request was used.
	if msg := errorOf(acs(browser, idp.response(t, assertion{inResponseTo: reqID}), relay)); !strings.Contains(msg, "already used") {
		t.Fatal(msg)
	}

	cases := []struct {
		name string
		a    func(reqID string) assertion
		want string
	}{
		{"unsigned", func(r string) assertion { return assertion{inResponseTo: r, unsigned: true} }, "not signed"},
		{"signed by someone else", func(r string) assertion { return assertion{inResponseTo: r, otherKey: true} }, "signature"},
		{"tampered after signing", func(r string) assertion { return assertion{inResponseTo: r, tamper: true} }, "not valid"},
		{"a second, unsigned assertion", func(r string) assertion { return assertion{inResponseTo: r, twoAssertions: true} }, "not valid"},
		{"another identity provider", func(r string) assertion { return assertion{inResponseTo: r, issuer: "https://evil.example"} }, "another identity provider"},
		{"another audience", func(r string) assertion { return assertion{inResponseTo: r, audience: "https://other.example"} }, "for another service"},
		{"no audience", func(r string) assertion { return assertion{inResponseTo: r, noAudience: true} }, "names no audience"},
		{"another recipient", func(r string) assertion { return assertion{inResponseTo: r, recipient: "https://evil.example/acs"} }, "another address"},
		{"another destination", func(r string) assertion { return assertion{inResponseTo: r, destination: "https://evil.example/acs"} }, "another address"},
		{"expired", func(r string) assertion {
			return assertion{inResponseTo: r, notBefore: time.Now().Add(-time.Hour), notOnOrAfter: time.Now().Add(-10 * time.Minute)}
		}, "expired"},
		{"not valid yet", func(r string) assertion {
			return assertion{inResponseTo: r, notBefore: time.Now().Add(10 * time.Minute), notOnOrAfter: time.Now().Add(20 * time.Minute)}
		}, "not valid yet"},
		{"answers another request", func(string) assertion { return assertion{inResponseTo: "_other"} }, "does not answer"},
		{"transient NameID", func(r string) assertion {
			return assertion{inResponseTo: r, nameFormat: "urn:oasis:names:tc:SAML:2.0:nameid-format:transient"}
		}, "transient"},
		{"failed at the IdP", func(r string) assertion {
			return assertion{inResponseTo: r, status: "urn:oasis:names:tc:SAML:2.0:status:Requester"}
		}, "reported a failure"},
		{"no email", func(r string) assertion { return assertion{inResponseTo: r, email: "-", nameID: "u-9"} }, "not valid"},
	}
	for _, c := range cases {
		b, reqID, relay := samlStart(t, srv, id)
		r := acs(b, idp.response(t, c.a(reqID)), relay)
		if msg := errorOf(r); !strings.Contains(msg, c.want) {
			t.Errorf("%s: want %q, got %q (%d)", c.name, c.want, msg, r.Status)
		}
		b.get("/api/v1/auth/me").expect(401)
	}

	// A response this browser did not ask for is refused (IdP-initiated is off by default).
	if msg := errorOf(acs(newClient(t, srv), idp.response(t, assertion{}), "")); !strings.Contains(msg, "Start signing in from BeziSync") {
		t.Fatal(msg)
	}
	// Turned on, IdP-initiated responses work once each.
	admin.patch("/api/v1/sso-providers/"+id, map[string]any{"allow_idp_initiated": true}, "If-Match", `"1"`).expect(200)
	unsolicited := idp.response(t, assertion{nameID: "u-2", email: "lee@example.com"})
	if r := acs(newClient(t, srv), unsolicited, ""); r.Status != 303 {
		t.Fatal(errorOf(r))
	}
	if msg := errorOf(acs(newClient(t, srv), unsolicited, "")); !strings.Contains(msg, "already used") {
		t.Fatal(msg)
	}
	// An unsolicited response must not claim to answer a request.
	if msg := errorOf(acs(newClient(t, srv), idp.response(t, assertion{inResponseTo: "_x", nameID: "u-3"}), "")); !strings.Contains(msg, "did not start") {
		t.Fatal(msg)
	}

	// Over https the relay cookie must come back: a response started in another browser fails.
	withConfig(t, func(s *config.Settings) { s.CookieSecure = true })
	_, reqID, relay = samlStart(t, srv, id)
	if msg := errorOf(acs(newClient(t, srv), idp.response(t, assertion{inResponseTo: reqID}), relay)); !strings.Contains(msg, "another browser") {
		t.Fatal(msg)
	}
}

func TestSAMLCertificatesAreCheckedAndRefreshed(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	idp := newSAMLIdP(t, "urn:idp:test")
	// A certificate that has expired can never verify a response.
	expired := expiredCert(t)
	r := admin.post("/api/v1/sso-providers", map[string]any{"label": "Old", "kind": "saml", "idp_entity_id": "urn:idp:old",
		"sso_url": "https://idp.example/sso", "certificate": expired}).expect(422)
	if !strings.Contains(string(r.Raw), "expired") {
		t.Fatal(string(r.Raw))
	}
	// Metadata from a URL; Test re-reads it and picks up a rotated certificate.
	var mu sync.Mutex
	current := idp.metadata("https://idp.example/sso")
	mdSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		_, _ = io.WriteString(w, current)
	}))
	t.Cleanup(mdSrv.Close)
	created := admin.post("/api/v1/sso-providers", map[string]any{"label": "URL", "kind": "saml", "metadata_url": mdSrv.URL}).expect(201)
	before := created.Str("saml", "certificates", 0, "sha256")
	rotated := newSAMLIdP(t, "urn:idp:test")
	mu.Lock()
	current = rotated.metadata("https://idp.example/sso")
	mu.Unlock()
	admin.post("/api/v1/sso-providers/"+created.Str("id")+"/test", nil).expect(200)
	after := admin.get("/api/v1/sso-providers").Str(0, "saml", "certificates", 0, "sha256")
	if after == before {
		t.Fatal("the rotated certificate was not picked up")
	}
	// Another entity in the metadata is never adopted silently.
	mu.Lock()
	current = newSAMLIdP(t, "urn:idp:someone-else").metadata("https://idp.example/sso")
	mu.Unlock()
	if res := admin.post("/api/v1/sso-providers/"+created.Str("id")+"/test", nil).expect(200); res.At("ok") != false {
		t.Fatal(string(res.Raw))
	}
}

func expiredCert(t *testing.T) string {
	t.Helper()
	key, certPEM, err := samlsp.NewKeys()
	if err != nil {
		t.Fatal(err)
	}
	k, err := samlsp.LoadKeys(key, certPEM)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(7), NotBefore: time.Now().AddDate(-2, 0, 0), NotAfter: time.Now().AddDate(-1, 0, 0)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, k.Signer.(*rsa.PrivateKey).Public(), k.Signer)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

func TestMovingPeopleToAnotherProviderIsAnAdminChoice(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	oidcIdP := newFakeIdP(t, admin) // signs in dana@example.com
	signIn(t, srv, oidcIdP, "/", "")
	idp := newSAMLIdP(t, "urn:idp:move")
	created := admin.post("/api/v1/sso-providers", map[string]any{"label": "New SAML", "kind": "saml", "metadata_xml": idp.metadata("https://idp.example/sso"),
		"default_projects": []string{"default"}}).expect(201)
	if created.At("link_existing") != false {
		t.Fatal(string(created.Raw))
	}
	// Dana already signs in with the OIDC provider: by default the new one may not take her over.
	b, reqID, relay := samlStart(t, srv, created.Str("id"))
	if msg := errorOf(acs(b, idp.response(t, assertion{inResponseTo: reqID, email: "dana@example.com"}), relay)); !strings.Contains(msg, "another sign-in option") {
		t.Fatal(msg)
	}
	admin.patch("/api/v1/sso-providers/"+created.Str("id"), map[string]any{"link_existing": true}, "If-Match", `"1"`).expect(200)
	b, reqID, relay = samlStart(t, srv, created.Str("id"))
	if r := acs(b, idp.response(t, assertion{inResponseTo: reqID, email: "dana@example.com"}), relay); r.Status != 303 {
		t.Fatal(errorOf(r))
	}
	if e := b.get("/api/v1/auth/me").Str("user", "email"); e != "dana@example.com" {
		t.Fatal(e)
	}
	// It moved (audited), so the old provider now needs the same choice to take her back.
	_, back := signIn(t, srv, oidcIdP, "/", "")
	if loc := back.Header.Get("location"); !strings.Contains(loc, "another%20sign-in%20option") {
		t.Fatal(loc)
	}
}
