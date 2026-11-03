package api_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/ratelimit"
)

// rpc posts one MCP JSON-RPC request with a bearer token and returns the status and decoded body.
func rpc(t *testing.T, srv *httptest.Server, token, method string, params any, headers ...string) (int, map[string]any, http.Header) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/api/v1/mcp", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("MCP-Protocol-Version", "2025-06-18")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	raw, _ := io.ReadAll(res.Body)
	out := map[string]any{}
	_ = json.Unmarshal(raw, &out)
	if strings.Contains(string(raw), "secrets_enc") || strings.Contains(string(raw), canary) {
		t.Fatalf("MCP leaked a secret: %s", raw)
	}
	return res.StatusCode, out, res.Header
}

func toolNames(t *testing.T, srv *httptest.Server, token string) []string {
	t.Helper()
	status, out, _ := rpc(t, srv, token, "tools/list", map[string]any{})
	if status != 200 {
		t.Fatal(status, out)
	}
	var names []string
	for _, x := range at(t, out, "result", "tools").([]any) {
		names = append(names, at(t, x, "name").(string))
	}
	return names
}

// callTool runs a tool and returns its text and whether it was an error.
func callTool(t *testing.T, srv *httptest.Server, token, name string, args map[string]any) (string, bool) {
	t.Helper()
	status, out, _ := rpc(t, srv, token, "tools/call", map[string]any{"name": name, "arguments": args})
	if status != 200 {
		t.Fatal(status, out)
	}
	if e, ok := out["error"]; ok {
		return at(t, e, "message").(string), true
	}
	isErr, _ := at(t, out, "result").(map[string]any)["isError"].(bool)
	return at(t, out, "result", "content", 0, "text").(string), isErr
}

func token(t *testing.T, c *client, body map[string]any) string {
	t.Helper()
	return c.post("/api/v1/auth/tokens", body).expect(201).Str("token")
}

func enableMCP(t *testing.T, admin *client, extra map[string]any) {
	t.Helper()
	body := map[string]any{"enabled": true}
	for k, v := range extra {
		body[k] = v
	}
	admin.put("/api/v1/mcp/settings", body).expect(200)
}

func TestMCPToolsFollowRolesTokensAndSettings(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	viewer := member(t, e.srv, admin, e.pid, "viewer")
	adminTok := token(t, admin, map[string]any{"name": "agent"})
	viewerTok := token(t, viewer, map[string]any{"name": "agent"})
	readOnlyTok := token(t, admin, map[string]any{"name": "ro", "read_only": true})

	// Off until an Admin turns it on.
	if status, _, _ := rpc(t, e.srv, adminTok, "tools/list", map[string]any{}); status != 404 {
		t.Fatal(status)
	}
	enableMCP(t, admin, nil)
	// An agent can't change MCP settings, even an Admin's token.
	newClient(t, e.srv).put("/api/v1/mcp/settings", map[string]any{"enabled": false}, "Authorization", "Bearer "+adminTok).expect(403)

	all := toolNames(t, e.srv, adminTok)
	for _, want := range []string{"list_projects", "create_pipeline", "deploy_pipeline", "create_project", "list_users", "create_connection"} {
		if !slices.Contains(all, want) {
			t.Fatal("admin lacks", want, all)
		}
	}
	for _, never := range []string{"delete_pipeline", "reset_pipeline_offsets", "delete_project"} {
		if slices.Contains(all, never) {
			t.Fatal("destructive tool on by default:", never)
		}
	}
	viewed := toolNames(t, e.srv, viewerTok)
	if !slices.Contains(viewed, "list_pipelines") || slices.Contains(viewed, "create_pipeline") || slices.Contains(viewed, "deploy_pipeline") ||
		slices.Contains(viewed, "list_users") || slices.Contains(viewed, "check_connection") == false {
		t.Fatal(viewed)
	}
	ro := toolNames(t, e.srv, readOnlyTok)
	if slices.Contains(ro, "create_pipeline") || !slices.Contains(ro, "list_users") {
		t.Fatal(ro)
	}

	// Tools call the API as the caller.
	text, isErr := callTool(t, e.srv, viewerTok, "list_projects", nil)
	if isErr || !strings.Contains(text, `"slug": "default"`) {
		t.Fatal(text)
	}
	if text, isErr := callTool(t, e.srv, viewerTok, "create_pipeline", map[string]any{"name": "x", "source": "shop-pg"}); !isErr {
		t.Fatal("a viewer called a write tool:", text)
	}
	pipe := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	text, isErr = callTool(t, e.srv, viewerTok, "get_pipeline", map[string]any{"pipeline": "orders-cdc"})
	if isErr || !strings.Contains(text, pipe.Str("id")) {
		t.Fatal(text)
	}
	text, isErr = callTool(t, e.srv, adminTok, "update_pipeline", map[string]any{"project": "default", "pipeline": "orders-cdc", "note": "via MCP",
		"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders", "shop.refunds"}}}}})
	// The undeployed draft is updated in place: one change, one version.
	if isErr || !strings.Contains(text, `"current_version": 1`) {
		t.Fatal(text)
	}
	// Errors come back as the API's explanation.
	if text, isErr := callTool(t, e.srv, adminTok, "get_pipeline", map[string]any{"pipeline": "nope"}); !isErr || !strings.Contains(text, `No pipeline named "nope"`) {
		t.Fatal(text)
	}
	if text, isErr := callTool(t, e.srv, adminTok, "get_pipeline", map[string]any{"pipeline": "orders-cdc", "extra": 1}); !isErr || !strings.Contains(text, "Unknown argument") {
		t.Fatal(text)
	}

	// Credentials are never inputs: only secret-store references pass.
	text, isErr = callTool(t, e.srv, adminTok, "create_connection", map[string]any{"kind": "source", "type": "postgresql", "name": "leak",
		"config": map[string]any{"host": "db", "port": 5432, "database": "d", "user": "u"}, "secret_refs": map[string]any{"password": "hunter2"}})
	if !isErr || !strings.Contains(text, "secret-store reference") {
		t.Fatal(text)
	}
	text, isErr = callTool(t, e.srv, adminTok, "create_connection", map[string]any{"kind": "sink", "type": "http", "name": "hook",
		"config": map[string]any{"url": "https://hooks.example.com/cdc"}})
	if isErr || !strings.Contains(text, `"name": "hook"`) {
		t.Fatal(text)
	}
	// The audit log names the token.
	found := false
	for _, ev := range admin.get("/api/v1/projects/" + e.pid + "/events?audit_only=true").List("items") {
		found = found || strings.Contains(at(t, ev, "actor_label").(string), "(token agent)")
	}
	if !found {
		t.Fatal("no audit event names the token")
	}

	// Admin choices: a destructive tool on, a read tool off.
	enableMCP(t, admin, map[string]any{"tools": map[string]any{"delete_pipeline": true, "list_alerts": false}})
	all = toolNames(t, e.srv, adminTok)
	if !slices.Contains(all, "delete_pipeline") || slices.Contains(all, "list_alerts") {
		t.Fatal(all)
	}
	tools := admin.get("/api/v1/mcp/tools").List()
	if len(tools) < 70 {
		t.Fatal("catalog too small", len(tools))
	}
	admin.put("/api/v1/mcp/settings", map[string]any{"tools": map[string]any{"no_such_tool": true}}).expect(422)

	// API tokens can be turned off for MCP; requests without a bearer are refused.
	enableMCP(t, admin, map[string]any{"allow_api_tokens": false})
	if status, out, _ := rpc(t, e.srv, adminTok, "tools/list", map[string]any{}); status != 401 || out["code"] != "api_tokens_disabled" {
		t.Fatal(status, out)
	}
	enableMCP(t, admin, map[string]any{"allow_api_tokens": true})
	if status, _, _ := rpc(t, e.srv, "", "tools/list", map[string]any{}); status != 401 {
		t.Fatal(status)
	}
	// Browser origins need allowing (DNS rebinding); agents send none.
	if status, _, _ := rpc(t, e.srv, adminTok, "tools/list", map[string]any{}, "Origin", "https://evil.example"); status != 403 {
		t.Fatal(status)
	}
}

func pkce() (verifier, challenge string) {
	verifier = strings.Repeat("v", 50)
	sum := sha256.Sum256([]byte(verifier))
	return verifier, strings.TrimRight(base64.URLEncoding.EncodeToString(sum[:]), "=")
}

func form(t *testing.T, srv *httptest.Server, path string, values url.Values) (int, map[string]any) {
	t.Helper()
	res, err := http.PostForm(srv.URL+path, values)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	out := map[string]any{}
	_ = json.NewDecoder(res.Body).Decode(&out)
	return res.StatusCode, out
}

func TestMCPAgentsSignInWithOAuth(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	ratelimit.OAuthRegister.Clear()
	anon := newClient(t, e.srv)
	anon.get("/.well-known/oauth-authorization-server").expect(404)
	enableMCP(t, admin, map[string]any{"oauth_enabled": true})

	// Discovery from the 401 an agent gets with only the URL.
	_, _, hdr := rpc(t, e.srv, "", "tools/list", map[string]any{})
	if !strings.Contains(hdr.Get("WWW-Authenticate"), "/.well-known/oauth-protected-resource/api/v1/mcp") {
		t.Fatal(hdr)
	}
	prm := anon.get("/.well-known/oauth-protected-resource/api/v1/mcp").expect(200)
	if !strings.HasSuffix(prm.Str("resource"), "/api/v1/mcp") {
		t.Fatal(string(prm.Raw))
	}
	meta := anon.get("/.well-known/oauth-authorization-server").expect(200)
	if meta.At("code_challenge_methods_supported", 0) != "S256" {
		t.Fatal(string(meta.Raw))
	}

	// Dynamic registration: public clients with safe redirect URIs only.
	anon.post("/api/v1/oauth/register", map[string]any{"client_name": "Evil", "redirect_uris": []string{"http://evil.example/cb"}}).expect(400)
	anon.post("/api/v1/oauth/register", map[string]any{"client_name": "Secretive", "redirect_uris": []string{"https://a.example/cb"},
		"token_endpoint_auth_method": "client_secret_basic"}).expect(400)
	reg := anon.post("/api/v1/oauth/register", map[string]any{"client_name": "Claude", "redirect_uris": []string{"http://127.0.0.1:33418/callback"}}).expect(201)
	clientID := reg.Str("client_id")

	verifier, challenge := pkce()
	authorize := func(c *client, extra ...string) *resp {
		q := url.Values{"response_type": {"code"}, "client_id": {clientID}, "redirect_uri": {"http://127.0.0.1:33418/callback"},
			"code_challenge": {challenge}, "code_challenge_method": {"S256"}, "state": {"xyz"}}
		for i := 0; i+1 < len(extra); i += 2 {
			q.Set(extra[i], extra[i+1])
		}
		return c.get("/api/v1/oauth/authorize?" + q.Encode())
	}
	// A redirect the agent did not register is shown to the person, never followed.
	if loc := authorize(admin, "redirect_uri", "https://evil.example/cb").expect(302).Header.Get("location"); !strings.HasPrefix(loc, "/oauth/consent?error=") {
		t.Fatal(loc)
	}
	start := authorize(admin).expect(302).Header.Get("location")
	if !strings.HasPrefix(start, "/oauth/consent?request=") {
		t.Fatal(start)
	}
	reqID := strings.TrimPrefix(start, "/oauth/consent?request=")
	info := admin.get("/api/v1/oauth/requests/" + reqID).expect(200)
	if info.Str("client_name") != "Claude" || info.Str("redirect_host") != "127.0.0.1:33418" {
		t.Fatal(string(info.Raw))
	}
	// Agents can't approve on a person's behalf.
	adminTok := token(t, admin, map[string]any{"name": "t"})
	newClient(t, e.srv).post("/api/v1/oauth/requests/"+reqID+"/approve", map[string]any{"days": 30}, "Authorization", "Bearer "+adminTok).expect(403)
	admin.post("/api/v1/oauth/requests/"+reqID+"/approve", map[string]any{"days": 400}).expect(422)
	approved := admin.post("/api/v1/oauth/requests/"+reqID+"/approve", map[string]any{"days": 30, "project_id": e.pid}).expect(200)
	back, _ := url.Parse(approved.Str("redirect_to"))
	if back.Query().Get("state") != "xyz" || back.Query().Get("iss") == "" {
		t.Fatal(back)
	}
	code := back.Query().Get("code")
	admin.post("/api/v1/oauth/requests/"+reqID+"/approve", map[string]any{"days": 30}).expect(404)

	exchange := url.Values{"grant_type": {"authorization_code"}, "client_id": {clientID}, "code": {code},
		"redirect_uri": {"http://127.0.0.1:33418/callback"}, "code_verifier": {strings.Repeat("w", 50)}}
	if status, out := form(t, e.srv, "/api/v1/oauth/token", exchange); status != 400 || out["error"] != "invalid_grant" {
		t.Fatal(status, out) // wrong verifier: and the code is now spent
	}
	start = authorize(admin).expect(302).Header.Get("location")
	reqID = strings.TrimPrefix(start, "/oauth/consent?request=")
	back, _ = url.Parse(admin.post("/api/v1/oauth/requests/"+reqID+"/approve", map[string]any{"days": 30, "project_id": e.pid}).expect(200).Str("redirect_to"))
	exchange.Set("code", back.Query().Get("code"))
	exchange.Set("code_verifier", verifier)
	status, tokens := form(t, e.srv, "/api/v1/oauth/token", exchange)
	if status != 200 || !strings.HasPrefix(tokens["access_token"].(string), "bzo_") {
		t.Fatal(status, tokens)
	}
	if status, out := form(t, e.srv, "/api/v1/oauth/token", exchange); status != 400 || out["error"] != "invalid_grant" {
		t.Fatal("a code worked twice", status, out)
	}
	access := tokens["access_token"].(string)
	text, isErr := callTool(t, e.srv, access, "list_projects", nil)
	if isErr || !strings.Contains(text, e.pid) {
		t.Fatal(text)
	}
	// OAuth tokens work on MCP only, not on the REST API.
	newClient(t, e.srv).get("/api/v1/auth/me", "Authorization", "Bearer "+access).expect(401)

	// The person sees and can disconnect the agent.
	grants := admin.get("/api/v1/mcp/grants").List()
	if len(grants) != 1 || at(t, grants[0], "client_name") != "Claude" {
		t.Fatal(grants)
	}

	// Refresh rotates; reusing an old refresh token revokes the grant.
	refresh := url.Values{"grant_type": {"refresh_token"}, "client_id": {clientID}, "refresh_token": {tokens["refresh_token"].(string)}}
	status, rotated := form(t, e.srv, "/api/v1/oauth/token", refresh)
	if status != 200 || rotated["refresh_token"] == tokens["refresh_token"] {
		t.Fatal(status, rotated)
	}
	if text, isErr := callTool(t, e.srv, rotated["access_token"].(string), "list_projects", nil); isErr {
		t.Fatal(text)
	}
	if status, out := form(t, e.srv, "/api/v1/oauth/token", refresh); status != 400 || out["error"] != "invalid_grant" {
		t.Fatal(status, out)
	}
	if status, _, _ := rpc(t, e.srv, rotated["access_token"].(string), "tools/list", map[string]any{}); status != 401 {
		t.Fatal("a stolen refresh token did not end the grant", status)
	}
	if len(admin.get("/api/v1/mcp/grants").List()) != 0 {
		t.Fatal("the revoked grant is still listed")
	}

	// With approval required, a new agent waits for an Admin.
	enableMCP(t, admin, map[string]any{"oauth_enabled": true, "oauth_registration": "approval"})
	pending := anon.post("/api/v1/oauth/register", map[string]any{"client_name": "Unknown", "redirect_uris": []string{"https://agent.example/cb"}}).expect(201)
	editor := member(t, e.srv, admin, e.pid, "editor")
	q := url.Values{"response_type": {"code"}, "client_id": {pending.Str("client_id")}, "redirect_uri": {"https://agent.example/cb"},
		"code_challenge": {challenge}, "code_challenge_method": {"S256"}}
	reqID = strings.TrimPrefix(editor.get("/api/v1/oauth/authorize?"+q.Encode()).expect(302).Header.Get("location"), "/oauth/consent?request=")
	if editor.get("/api/v1/oauth/requests/"+reqID).Str("client_status") != "pending" {
		t.Fatal("not pending")
	}
	if c := editor.post("/api/v1/oauth/requests/"+reqID+"/approve", map[string]any{"days": 7, "allow_agent": true}).expect(409).Code(); c != "agent_pending" {
		t.Fatal(c)
	}
	clients := admin.get("/api/v1/mcp/clients").List()
	if at(t, clients[0], "status") != "pending" {
		t.Fatal(clients)
	}
	admin.patch("/api/v1/mcp/clients/"+pending.Str("client_id"), map[string]any{"status": "allowed"}).expect(200)
	editor.post("/api/v1/oauth/requests/"+reqID+"/approve", map[string]any{"days": 7, "read_only": true}).expect(200)

	// Revocation endpoint (RFC 7009) answers 200 for anything.
	if status, _ := form(t, e.srv, "/api/v1/oauth/revoke", url.Values{"token": {"bzo_unknown"}}); status != 200 {
		t.Fatal(status)
	}
}

// Every read tool reaches a real route (a typo in a path would answer 404/405 for everyone).
func TestEveryMCPReadToolReachesItsRoute(t *testing.T) {
	e := newPipelineEnv(t)
	enableMCP(t, e.admin, nil)
	tok := token(t, e.admin, map[string]any{"name": "sweep"})
	createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	args := map[string]map[string]any{
		"read_pipeline_events":        {"table": "shop.orders"},
		"diff_pipeline_versions":      {"from": 1, "to": 1},
		"preview_pipeline_transforms": {"table": "shop.orders"},
		"adopt_pipeline_properties":   {"properties": "debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector\n"},
		"check_secret_reference":      {"store_id": db0, "path": "x"},
		"list_secret_store_secrets":   {"store_id": db0},
		"list_secret_keys":            {"store_id": db0, "path": "x"},
	}
	for _, tool := range e.admin.get("/api/v1/mcp/tools").List() {
		name := at(t, tool, "name").(string)
		if at(t, tool, "kind") != "read" || name == "list_projects" {
			continue
		}
		a := map[string]any{}
		for k, v := range args[name] {
			a[k] = v
		}
		if strings.Contains(name, "pipeline") && !strings.HasSuffix(name, "pipelines") && name != "adopt_pipeline_properties" {
			a["pipeline"] = "orders-cdc"
		}
		if strings.Contains(name, "connection") && name != "list_connections" {
			a["connection"] = "shop-pg"
		}
		if strings.Contains(name, "runtime") && name != "list_runtimes" {
			a["runtime"] = "k8s"
		}
		text, _ := callTool(t, e.srv, tok, name, a)
		if strings.Contains(text, "HTTP 405") || strings.HasPrefix(text, "Not Found (HTTP 404") || strings.Contains(text, "Missing argument") {
			t.Errorf("%s: %s", name, text)
		}
	}
}
