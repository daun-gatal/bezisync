package mcpserver

import (
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
)

// Kind is what a tool may do: read, change, or destroy (off until an Admin turns it on).
type Kind string

// Tool kinds.
const (
	Read        Kind = "read"
	Write       Kind = "write"
	Destructive Kind = "destructive"
)

// Param is one tool argument.
type Param struct {
	Name     string
	Schema   map[string]any
	Required bool
}

// Tool is one MCP tool: who may use it and which REST calls it makes.
type Tool struct {
	Name, Title, Description string
	Kind                     Kind
	// Perm: the project permission it needs in at least one project ("" = none).
	Perm auth.Perm
	// Admin: account-wide Admins only.
	Admin bool
	// Project: takes a "project" argument (slug or id).
	Project bool
	Params  []Param
	Call    func(*call) (any, error)
}

// Group names the area a tool belongs to (Settings → MCP groups its list by it).
func (t *Tool) Group() string {
	for _, g := range []struct{ prefix, name string }{
		{"pipeline", "Pipelines"}, {"engine_settings", "Pipelines"}, {"connection", "Connections"}, {"runtime", "Runtimes"}, {"alert", "Alerts"},
		{"change_request", "Approvals"}, {"template", "Templates"}, {"saved_position", "Pipelines"},
		{"debezium", "Engine versions"}, {"engine", "Engine versions"},
		{"member", "People"}, {"user", "People"}, {"project", "Projects"}, {"secret", "Secret stores"},
	} {
		if strings.Contains(t.Name, g.prefix) {
			return g.name
		}
	}
	return "General"
}

func (t *Tool) params() []Param {
	if !t.Project {
		return t.Params
	}
	return append([]Param{{Name: "project", Schema: str("The project's slug or id (list_projects shows them). May be left out when you can open only one project.")}}, t.Params...)
}

func (t *Tool) mcpTool() *mcp.Tool {
	props := map[string]any{}
	required := []string{}
	for _, p := range t.params() {
		props[p.Name] = p.Schema
		if p.Required {
			required = append(required, p.Name)
		}
	}
	schema := map[string]any{"type": "object", "properties": props, "additionalProperties": false}
	if len(required) > 0 {
		schema["required"] = required
	}
	destructive := t.Kind == Destructive
	return &mcp.Tool{Name: t.Name, Title: t.Title, Description: t.Description, InputSchema: schema,
		Annotations: &mcp.ToolAnnotations{Title: t.Title, ReadOnlyHint: t.Kind == Read, DestructiveHint: &destructive, IdempotentHint: t.Kind == Read}}
}

// check validates arguments against the declared parameters (names, required, JSON types, enums).
func (t *Tool) check(args map[string]any) string {
	known := map[string]Param{}
	for _, p := range t.params() {
		known[p.Name] = p
		if _, ok := args[p.Name]; p.Required && !ok {
			return fmt.Sprintf("Missing argument %q.", p.Name)
		}
	}
	names := make([]string, 0, len(args))
	for k := range args {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		p, ok := known[k]
		if !ok {
			return fmt.Sprintf("Unknown argument %q.", k)
		}
		v := args[k]
		if v == nil {
			continue
		}
		want, _ := p.Schema["type"].(string)
		okType := true
		switch want {
		case "string":
			_, okType = v.(string)
		case "integer":
			f, isNum := v.(float64)
			okType = isNum && f == float64(int64(f))
		case "boolean":
			_, okType = v.(bool)
		case "object":
			_, okType = v.(map[string]any)
		case "array":
			_, okType = v.([]any)
		}
		if !okType {
			return fmt.Sprintf("Argument %q must be a %s.", k, want)
		}
		if enum, ok := p.Schema["enum"].([]string); ok && !slices.Contains(enum, fmt.Sprint(v)) {
			return fmt.Sprintf("Argument %q must be one of: %s.", k, strings.Join(enum, ", "))
		}
	}
	return ""
}

// --- schema helpers ---

func str(desc string) map[string]any  { return map[string]any{"type": "string", "description": desc} }
func num(desc string) map[string]any  { return map[string]any{"type": "integer", "description": desc} }
func flag(desc string) map[string]any { return map[string]any{"type": "boolean", "description": desc} }
func obj(desc string) map[string]any  { return map[string]any{"type": "object", "description": desc} }
func strs(desc string) map[string]any {
	return map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": desc}
}
func oneOf(desc string, values ...string) map[string]any {
	return map[string]any{"type": "string", "enum": values, "description": desc}
}

func req(name string, schema map[string]any) Param {
	return Param{Name: name, Schema: schema, Required: true}
}
func opt(name string, schema map[string]any) Param { return Param{Name: name, Schema: schema} }

// secretRefs is the only way a tool touches secrets: references into a secret store.
func secretRefs() Param {
	return opt("secret_refs", map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string", "pattern": "^ref\\+"},
		"description": "Secret fields as secret-store references only, e.g. {\"password\": \"ref+vault://cdc/orders-db#password\"}. " +
			"Passwords and keys themselves are never accepted here: a person enters them in the BeziSync UI."})
}

// --- argument helpers ---

func (cl *call) s(name string) string { v, _ := cl.args[name].(string); return v }
func (cl *call) has(name string) bool { _, ok := cl.args[name]; return ok && cl.args[name] != nil }

// body copies the named arguments that were sent into a request body.
func (cl *call) body(names ...string) map[string]any {
	out := map[string]any{}
	for _, n := range names {
		if cl.has(n) {
			out[n] = cl.args[n]
		}
	}
	return out
}

// refs turns secret_refs into the API's secrets, refusing anything that is not a reference.
func (cl *call) refs(body map[string]any) error {
	if !cl.has("secret_refs") {
		return nil
	}
	m, _ := cl.args["secret_refs"].(map[string]any)
	out := map[string]any{}
	for k, v := range m {
		s, _ := v.(string)
		if !strings.HasPrefix(s, "ref+") {
			return &apiError{fmt.Sprintf("secret_refs.%s must be a secret-store reference (ref+<store>://<path>#<key>); "+
				"passwords and keys are entered by a person in the BeziSync UI.", k)}
		}
		out[k] = s
	}
	body["secrets"] = out
	return nil
}

func query(pairs ...string) string {
	v := url.Values{}
	for i := 0; i+1 < len(pairs); i += 2 {
		if pairs[i+1] != "" {
			v.Set(pairs[i], pairs[i+1])
		}
	}
	if len(v) == 0 {
		return ""
	}
	return "?" + v.Encode()
}

func (cl *call) intArg(name string) string {
	if f, ok := cl.args[name].(float64); ok {
		return fmt.Sprint(int64(f))
	}
	return ""
}

func (cl *call) boolArg(name string) string {
	if b, ok := cl.args[name].(bool); ok {
		return fmt.Sprint(b)
	}
	return ""
}

// projectID resolves "project" among the projects the caller can open.
func (cl *call) projectID() (string, error) {
	if cl.project != "" {
		return cl.project, nil
	}
	want := cl.s("project")
	ps := cl.c.me.Projects
	if want == "" {
		if len(ps) == 1 {
			cl.project = ps[0].ID
			return cl.project, nil
		}
		return "", &apiError{"Say which project: pass \"project\" (list_projects shows them)."}
	}
	for _, p := range ps {
		if p.ID == want || p.Slug == want {
			cl.project = p.ID
			return cl.project, nil
		}
	}
	return "", &apiError{fmt.Sprintf("You can't open a project %q (list_projects shows the ones you can).", want)}
}

func (cl *call) base() (string, error) {
	id, err := cl.projectID()
	return "/api/v1/projects/" + id, err
}

// named resolves an id or a name among a project collection (pipelines, connections, …).
func (cl *call) named(arg, collection, what string) (string, error) {
	v := cl.s(arg)
	if v == "" {
		return "", &apiError{fmt.Sprintf("Missing argument %q.", arg)}
	}
	if _, err := uuid.Parse(v); err == nil {
		return v, nil
	}
	base, err := cl.base()
	if err != nil {
		return "", err
	}
	list, err := cl.do(http.MethodGet, base+"/"+collection, nil)
	if err != nil {
		return "", err
	}
	for _, item := range asList(list) {
		m, _ := item.(map[string]any)
		if m["name"] == v {
			return fmt.Sprint(m["id"]), nil
		}
	}
	return "", &apiError{fmt.Sprintf("No %s named %q in this project.", what, v)}
}

func (cl *call) pipeline() (string, error) {
	id, err := cl.named("pipeline", "pipelines", "pipeline")
	if err != nil {
		return "", err
	}
	base, _ := cl.base()
	return base + "/pipelines/" + id, nil
}

func (cl *call) connection(arg string) (string, error) {
	return cl.named(arg, "connections", "connection")
}

func (cl *call) runtime(arg string) (string, error) { return cl.named(arg, "runtimes", "runtime") }

// sub resolves a path under the project with a plain id argument.
func (cl *call) sub(arg, collection string) (string, error) {
	base, err := cl.base()
	if err != nil {
		return "", err
	}
	id := cl.s(arg)
	if _, err := uuid.Parse(id); err != nil {
		return "", &apiError{fmt.Sprintf("%q must be an id.", arg)}
	}
	return base + "/" + collection + "/" + id, nil
}

// withVersion calls PATCH with If-Match: the expected version, or the current one.
func (cl *call) withVersion(path string, body any) (any, error) {
	version := cl.intArg("expected_version")
	if version == "" {
		cur, err := cl.do(http.MethodGet, path, nil)
		if err != nil {
			return nil, err
		}
		m, _ := cur.(map[string]any)
		version = fmt.Sprint(m["version"])
	}
	status, raw := invokeWith(cl.ctx, cl.api, cl.c.auth, http.MethodPatch, path, body, "If-Match", `"`+version+`"`)
	return decode(status, raw)
}

func expectedVersion() Param {
	return opt("expected_version", num("The version you read. Leave it out to change the current version (someone else's change in between is then overwritten)."))
}
