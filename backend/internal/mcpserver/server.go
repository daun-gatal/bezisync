// Package mcpserver is BeziSync's Model Context Protocol endpoint (/api/v1/mcp).
//
// It is a machine surface: callers present a personal API token (bzt_) or an OAuth access
// token (bzo_) an agent got by signing in. Each request builds a stateless MCP server that holds
// only the tools the caller may use (their live roles, the token's project scope and read-only
// flag, and what an Admin turned on in Settings → MCP). A tool calls BeziSync's own REST API in
// process with the caller's credentials, so permissions, validation, policies, approvals and the
// audit log apply exactly as they do for the UI and the CLI. No tool takes or returns a
// credential.
package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/version"
)

// Path is where the endpoint is mounted.
const Path = "/api/v1/mcp"

// maxResult caps a tool result so one call cannot flood the agent's context.
const maxResult = 200_000

const instructions = `BeziSync is a control plane for Debezium Server change-data-capture pipelines.
Work happens in projects: pass "project" (its slug or id) to project tools; list_projects shows the ones you can open and what you may do in each.
Pipelines, connections and runtimes can be named by id or by name.
Changing a pipeline saves it as a version (your undeployed draft is updated, so one change is one version); deploy_pipeline runs the final checks (preflight) first and may wait for approval, and discard_pipeline_changes goes back to the deployed version without restarting anything.
Secrets are never accepted or returned: people enter passwords and keys in the BeziSync UI, and connections may point at a secret store with ref+<store>://<path>#<key>.`

// Handler serves MCP; api is BeziSync's REST router, which tools call in process.
func Handler(api http.Handler) http.Handler {
	streams := mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server {
		c, _ := r.Context().Value(callerKey).(*caller)
		if c == nil {
			return nil
		}
		return c.server(api)
	}, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true, DisableLocalhostProtection: true})
	return httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		ctx := auth.WithMCP(r.Context())
		q := db.MustPool(ctx)
		settings, err := services.GetMCPSettings(ctx, q)
		if err != nil {
			return err
		}
		if !settings.Enabled {
			return problem.New(404, "mcp_disabled", "MCP is turned off. An Admin can turn it on in Settings → MCP.")
		}
		if origin := r.Header.Get("Origin"); origin != "" && !slices.Contains(settings.AllowedOrigins, origin) && origin != publicOrigin() {
			return problem.New(403, "origin_not_allowed", "This browser origin may not call MCP. An Admin can allow it in Settings → MCP.")
		}
		p, err := authenticate(ctx, q, r, settings, w)
		if err != nil {
			return err
		}
		me, err := whoAmI(ctx, api, r.Header.Get("Authorization"))
		if err != nil {
			return err
		}
		c := &caller{principal: p, me: me, settings: settings, auth: r.Header.Get("Authorization")}
		streams.ServeHTTP(w, r.WithContext(context.WithValue(ctx, callerKey, c)))
		return nil
	})
}

type ctxKey int

const callerKey ctxKey = 1

func publicOrigin() string {
	u, err := url.Parse(config.Get().PublicURL)
	if err != nil {
		return ""
	}
	return u.Scheme + "://" + u.Host
}

// authenticate accepts bearer API tokens (when allowed) and OAuth access tokens (when on).
func authenticate(ctx context.Context, q db.Querier, r *http.Request, s services.MCPSettings, w http.ResponseWriter) (*auth.Principal, error) {
	header := r.Header.Get("Authorization")
	challenge := func(presented bool) {
		if s.OAuthEnabled {
			v := `Bearer resource_metadata="` + ResourceMetadataURL() + `"`
			if presented {
				v += `, error="invalid_token"`
			}
			w.Header().Set("WWW-Authenticate", v)
		}
	}
	if !strings.HasPrefix(strings.ToLower(header), "bearer ") {
		challenge(false)
		msg := "MCP needs Authorization: Bearer <API token> (create one under Account → API tokens)."
		if s.OAuthEnabled {
			msg = "Sign in: connect your agent to this URL and it opens BeziSync to sign in, or send Authorization: Bearer <API token>."
		}
		return nil, problem.New(401, "not_authenticated", msg)
	}
	raw := strings.TrimSpace(header[7:])
	switch {
	case strings.HasPrefix(raw, auth.OAuthAccessPrefix):
		if !s.OAuthEnabled {
			challenge(true)
			return nil, problem.New(401, "oauth_disabled", "Signing in with OAuth is turned off for MCP. An Admin can turn it on in Settings → MCP.")
		}
	case strings.HasPrefix(raw, auth.APITokenPrefix):
		if !s.AllowAPITokens {
			challenge(true)
			return nil, problem.New(401, "api_tokens_disabled", "API tokens are turned off for MCP. Connect your agent with sign-in instead.")
		}
	default:
		challenge(true)
		return nil, problem.New(401, "invalid_token", "MCP accepts BeziSync API tokens and OAuth access tokens only.")
	}
	// MCP always POSTs: authenticate as a read so read-only tokens get in (their tool list is
	// read-only, and every API call a tool makes is checked with its real method).
	asRead := r.Clone(ctx)
	asRead.Method = http.MethodGet
	p, err := auth.Current(ctx, q, asRead)
	if err != nil {
		if pr := (*problem.Problem)(nil); errors.As(err, &pr) && pr.Status == 401 {
			challenge(true)
		}
		return nil, err
	}
	return p, nil
}

// ResourceMetadataURL is the OAuth protected-resource metadata of the MCP endpoint (RFC 9728).
func ResourceMetadataURL() string {
	return strings.TrimRight(config.Get().PublicURL, "/") + "/.well-known/oauth-protected-resource" + Path
}

// ResourceURL is the MCP endpoint's canonical URL (the OAuth resource indicator).
func ResourceURL() string { return strings.TrimRight(config.Get().PublicURL, "/") + Path }

// meRead is the part of GET /auth/me the server needs.
type meRead struct {
	User struct {
		Email       string `json:"email"`
		IsSuperuser bool   `json:"is_superuser"`
	} `json:"user"`
	Projects []projectRef `json:"projects"`
}

type projectRef struct {
	ID          string   `json:"id"`
	Slug        string   `json:"slug"`
	Name        string   `json:"name"`
	Permissions []string `json:"permissions"`
}

func whoAmI(ctx context.Context, api http.Handler, authorization string) (*meRead, error) {
	status, body := invoke(ctx, api, authorization, http.MethodGet, "/api/v1/auth/me", nil)
	if status != http.StatusOK {
		return nil, problem.New(status, "not_authenticated", "Could not read who you are.")
	}
	var me meRead
	if err := json.Unmarshal(body, &me); err != nil {
		return nil, err
	}
	return &me, nil
}

// invoke runs one request through the REST router as the caller.
func invoke(ctx context.Context, api http.Handler, authorization, method, path string, body any) (int, []byte) {
	return invokeWith(ctx, api, authorization, method, path, body)
}

// invokeWith is invoke with extra headers (name, value pairs).
func invokeWith(ctx context.Context, api http.Handler, authorization, method, path string, body any, headers ...string) (int, []byte) {
	var rd io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rd = bytes.NewReader(raw)
	}
	// A fresh routing context: the outer request's would route this one as /mcp.
	inner := context.WithValue(auth.WithMCP(ctx), chi.RouteCtxKey, nil)
	req := httptest.NewRequestWithContext(inner, method, path, rd)
	req.Header.Set("Authorization", authorization)
	req.Header.Set("User-Agent", "bezisync-mcp/"+version.Version)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)
	return rec.Code, rec.Body.Bytes()
}

// caller is one authenticated MCP request.
type caller struct {
	principal *auth.Principal
	me        *meRead
	settings  services.MCPSettings
	auth      string
}

func (c *caller) can(perm auth.Perm) bool {
	for _, p := range c.me.Projects {
		if slices.Contains(p.Permissions, string(perm)) {
			return true
		}
	}
	return false
}

func (c *caller) readOnly() bool {
	return c.principal.APIToken != nil && c.principal.APIToken.ReadOnly
}

// Allowed reports whether this caller gets a tool, and why not.
func (c *caller) allowed(t *Tool) bool {
	if !Enabled(t, c.settings) {
		return false
	}
	if c.readOnly() && t.Kind != Read {
		return false
	}
	if t.Admin && !c.me.User.IsSuperuser {
		return false
	}
	if t.Perm != "" && !c.can(t.Perm) {
		return false
	}
	return true
}

// Enabled reports whether an Admin left a tool on (defaults: reads and writes on, destructive off).
func Enabled(t *Tool, s services.MCPSettings) bool {
	if v, ok := s.Tools[t.Name]; ok {
		return v
	}
	return t.Kind != Destructive
}

func (c *caller) server(api http.Handler) *mcp.Server {
	s := mcp.NewServer(&mcp.Implementation{Name: "bezisync", Title: "BeziSync", Version: version.Version}, &mcp.ServerOptions{Instructions: instructions})
	for _, t := range Catalog() {
		if !c.allowed(t) {
			continue
		}
		s.AddTool(t.mcpTool(), c.handler(api, t))
	}
	return s
}

func (c *caller) handler(api http.Handler, t *Tool) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := map[string]any{}
		if len(req.Params.Arguments) > 0 {
			if json.Unmarshal(req.Params.Arguments, &args) != nil {
				//nolint:nilerr // bad arguments are a tool error the agent can read, not a protocol failure
				return errorResult("The arguments are not a JSON object."), nil
			}
		}
		if msg := t.check(args); msg != "" {
			return errorResult(msg), nil
		}
		cl := &call{ctx: ctx, api: api, c: c, args: args}
		out, err := t.Call(cl)
		if err != nil {
			return errorResult(err.Error()), nil
		}
		raw, _ := json.MarshalIndent(out, "", " ")
		text := string(raw)
		if len(text) > maxResult {
			text = text[:maxResult] + "\n… (cut: ask for less, for example a smaller limit or one table)"
		}
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: text}}}, nil
	}
}

func errorResult(msg string) *mcp.CallToolResult {
	return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: msg}}}
}

// call is one tool invocation.
type call struct {
	ctx  context.Context
	api  http.Handler
	c    *caller
	args map[string]any
	// resolved project id
	project string
}

// apiError is a REST problem turned into something an agent can act on.
type apiError struct{ msg string }

func (e *apiError) Error() string { return e.msg }

// do calls the REST API as the caller and decodes the JSON answer.
func (cl *call) do(method, path string, body any) (any, error) {
	status, raw := invoke(cl.ctx, cl.api, cl.c.auth, method, path, body)
	return decode(status, raw)
}

func decode(status int, raw []byte) (any, error) {
	if status == http.StatusNoContent {
		return map[string]any{"ok": true}, nil
	}
	var out any
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			out = string(raw)
		}
	}
	if status >= 400 {
		return nil, &apiError{problemText(status, out)}
	}
	return out, nil
}

func problemText(status int, body any) string {
	m, _ := body.(map[string]any)
	detail, _ := m["detail"].(string)
	if detail == "" {
		detail = http.StatusText(status)
	}
	code, _ := m["code"].(string)
	var b strings.Builder
	fmt.Fprintf(&b, "%s (HTTP %d", detail, status)
	if code != "" {
		b.WriteString(", " + code)
	}
	b.WriteString(")")
	if errs, ok := m["errors"].([]any); ok {
		for _, e := range errs {
			em, _ := e.(map[string]any)
			loc, _ := em["loc"].([]any)
			parts := make([]string, 0, len(loc))
			for _, l := range loc {
				parts = append(parts, fmt.Sprint(l))
			}
			fmt.Fprintf(&b, "\n- %s: %v", strings.Join(parts, "."), em["message"])
		}
	}
	// Preflight failures carry the report: list what blocks.
	if pre, ok := m["preflight"].(map[string]any); ok {
		for _, s := range asList(pre["sections"]) {
			report, _ := s.(map[string]any)["report"].(map[string]any)
			for _, ch := range asList(report["checks"]) {
				cm, _ := ch.(map[string]any)
				if cm["status"] == "fail" {
					fmt.Fprintf(&b, "\n- %v: %v", cm["code"], cm["message"])
				}
			}
		}
	}
	return b.String()
}

func asList(v any) []any { l, _ := v.([]any); return l }
