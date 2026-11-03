package cli

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"
)

// ConfigPath is where `remote login` stores credentials (mode 0600).
func ConfigPath() string {
	base := os.Getenv("XDG_CONFIG_HOME")
	if base == "" {
		home, _ := os.UserHomeDir()
		base = filepath.Join(home, ".config")
	}
	return filepath.Join(base, "bezisync", "cli.json")
}

type remoteSettings struct{ URL, Token, Project string }

func loadSettings() (remoteSettings, error) {
	stored := map[string]string{}
	if raw, err := os.ReadFile(ConfigPath()); err == nil {
		_ = json.Unmarshal(raw, &stored)
	}
	pick := func(env, key, def string) string {
		if v := os.Getenv(env); v != "" {
			return v
		}
		if v, ok := stored[key]; ok {
			return v
		}
		return def
	}
	s := remoteSettings{URL: pick("BEZISYNC_URL", "url", ""), Token: pick("BEZISYNC_TOKEN", "token", ""), Project: pick("BEZISYNC_PROJECT", "project", "default")}
	if s.URL == "" || s.Token == "" {
		return s, &usageError{"Invalid value: Not logged in: run `bezisync remote login` or set BEZISYNC_URL and BEZISYNC_TOKEN."}
	}
	return s, nil
}

// Client talks to a control plane with an API token.
type Client struct {
	BaseURL, Token, ProjectSlug string
	HTTP                        *http.Client
	Stderr                      io.Writer
	projectID                   string
}

// NewClient is overridable in tests.
var NewClient = func(baseURL, token, project string, stderr io.Writer) *Client {
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), Token: token, ProjectSlug: project, HTTP: &http.Client{Timeout: 60 * time.Second}, Stderr: stderr}
}

const red, bold, green, reset = "\x1b[31m", "\x1b[1m", "\x1b[32m", "\x1b[0m"

func (c *Client) fail(msg string) error {
	fmt.Fprintf(c.Stderr, "%serror: %s%s\n", red, msg, reset)
	return &ExitError{1}
}

// Call sends a request and decodes the JSON answer; HTTP errors print the problem.
func (c *Client) Call(method, path string, query url.Values, body any) (any, error) {
	var rd io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rd = bytes.NewReader(raw)
	}
	u := c.BaseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequest(method, u, rd) //nolint:noctx // CLI requests are bounded by the client timeout
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()
	raw, _ := io.ReadAll(res.Body)
	if res.StatusCode >= 400 {
		var problem map[string]any
		if json.Unmarshal(raw, &problem) != nil {
			return nil, c.fail(string(raw))
		}
		message, _ := problem["detail"].(string)
		if message == "" {
			message = string(raw)
		}
		var errs []string
		list, _ := problem["errors"].([]any)
		for _, e := range list {
			m, _ := e.(map[string]any)
			var loc []string
			parts, _ := m["loc"].([]any)
			for _, p := range parts {
				loc = append(loc, fmt.Sprint(p))
			}
			errs = append(errs, fmt.Sprintf("%s: %v", strings.Join(loc, "."), m["message"]))
		}
		if len(errs) > 0 {
			message += " (" + strings.Join(errs, "; ") + ")"
		}
		return nil, c.fail(message)
	}
	if len(raw) == 0 {
		return nil, nil
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Base is the project's API prefix.
func (c *Client) Base() (string, error) {
	if c.projectID == "" {
		me, err := c.Call(http.MethodGet, "/api/v1/auth/me", nil, nil)
		if err != nil {
			return "", err
		}
		projects, _ := me.(map[string]any)["projects"].([]any)
		for _, p := range projects {
			m, _ := p.(map[string]any)
			if m["slug"] == c.ProjectSlug {
				c.projectID = fmt.Sprint(m["id"])
			}
		}
		if c.projectID == "" {
			return "", c.fail(fmt.Sprintf("no access to project '%s'", c.ProjectSlug))
		}
	}
	return "/api/v1/projects/" + c.projectID, nil
}

// PipelineID finds a pipeline by name.
func (c *Client) PipelineID(name string) (string, error) {
	base, err := c.Base()
	if err != nil {
		return "", err
	}
	rows, err := c.Call(http.MethodGet, base+"/pipelines", url.Values{"q": {name}}, nil)
	if err != nil {
		return "", err
	}
	list, _ := rows.([]any)
	for _, p := range list {
		m, _ := p.(map[string]any)
		if m["name"] == name {
			return fmt.Sprint(m["id"]), nil
		}
	}
	return "", c.fail(fmt.Sprintf("no pipeline named '%s'", name))
}

func remoteClient(project string, stderr io.Writer) (*Client, error) {
	s, err := loadSettings()
	if err != nil {
		return nil, err
	}
	if project == "" {
		project = s.Project
	}
	return NewClient(s.URL, s.Token, project, stderr), nil
}

func str(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

// readManifests reads a manifest file, or every *.yaml / *.yml file of a directory (in name
// order) as one multi-document manifest.
func readManifests(path string) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return os.ReadFile(path) //nolint:gosec // the user's manifest
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var docs []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !(strings.HasSuffix(name, ".yaml") || strings.HasSuffix(name, ".yml")) {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(path, name)) //nolint:gosec // the user's manifests
		if err != nil {
			return nil, err
		}
		docs = append(docs, strings.TrimRight(string(raw), "\n"))
	}
	if len(docs) == 0 {
		return nil, &usageError{fmt.Sprintf("Invalid value: %s has no .yaml or .yml files", path)}
	}
	return []byte(strings.Join(docs, "\n---\n") + "\n"), nil
}

func remoteCmd(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	remote := &cobra.Command{Use: "remote", Short: "Manage pipelines on a BeziSync server (needs an API token).", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error { return cmd.Help() }}
	var project string
	addProject := func(cmd *cobra.Command) {
		cmd.Flags().StringVarP(&project, "project", "p", "", "Project slug (default: from login or BEZISYNC_PROJECT).")
	}

	var loginURL, loginToken, loginProject string
	login := &cobra.Command{Use: "login", Short: "Verify an API token and store it for later commands.", Args: cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			if loginToken == "" {
				fmt.Fprint(stdout, "Token: ")
				if f, ok := stdin.(*os.File); ok && term.IsTerminal(int(f.Fd())) { //nolint:gosec // a terminal file descriptor
					raw, err := term.ReadPassword(int(f.Fd())) //nolint:gosec // a terminal file descriptor
					fmt.Fprintln(stdout)
					if err != nil {
						return err
					}
					loginToken = string(raw)
				} else {
					line, _ := bufio.NewReader(stdin).ReadString('\n')
					loginToken = strings.TrimSpace(line)
				}
			}
			me, err := NewClient(loginURL, loginToken, loginProject, stderr).Call(http.MethodGet, "/api/v1/auth/me", nil, nil)
			if err != nil {
				return err
			}
			path := ConfigPath()
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				return err
			}
			raw, _ := json.Marshal(map[string]string{"url": loginURL, "token": loginToken, "project": loginProject})
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				return err
			}
			if err := os.Chmod(path, 0o600); err != nil {
				return err
			}
			user, _ := me.(map[string]any)["user"].(map[string]any)
			fmt.Fprintf(stdout, "Logged in to %s as %v (project %s).\n", loginURL, user["email"], loginProject)
			return nil
		}}
	login.Flags().StringVar(&loginURL, "url", "", "Control-plane URL, e.g. https://bezisync.example.com")
	login.Flags().StringVar(&loginToken, "token", "", "API token (Settings → API tokens).")
	login.Flags().StringVar(&loginProject, "project", "default", "Default project slug.")
	_ = login.MarkFlagRequired("url")

	get := &cobra.Command{Use: "get", Short: "List pipelines with their lifecycle and health.", Args: cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			c, err := remoteClient(project, stderr)
			if err != nil {
				return err
			}
			base, err := c.Base()
			if err != nil {
				return err
			}
			rows, err := c.Call(http.MethodGet, base+"/pipelines", nil, nil)
			if err != nil {
				return err
			}
			fmt.Fprintf(stdout, "%-32s %-13s %-9s %-8s SOURCE → DESTINATION\n", "NAME", "LIFECYCLE", "HEALTH", "DEPLOYED")
			list, _ := rows.([]any)
			for _, r := range list {
				p, _ := r.(map[string]any)
				sink := "-"
				if s, ok := p["sink"].(map[string]any); ok {
					sink = str(s["name"])
				}
				deployed := "-"
				if v, ok := p["deployed_version"].(float64); ok && v != 0 {
					deployed = fmt.Sprintf("v%d", int(v))
				}
				source, _ := p["source"].(map[string]any)
				fmt.Fprintf(stdout, "%-32s %-13s %-9s %-8s %s → %s\n", str(p["name"]), str(p["lifecycle"]), str(p["health"]), deployed, str(source["name"]), sink)
			}
			return nil
		}}
	addProject(get)

	export := &cobra.Command{Use: "export [NAME]", Short: "Print pipeline manifests as YAML.", Args: cobra.MaximumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			c, err := remoteClient(project, stderr)
			if err != nil {
				return err
			}
			base, err := c.Base()
			if err != nil {
				return err
			}
			path := base + "/pipelines:export"
			if len(args) == 1 {
				id, err := c.PipelineID(args[0])
				if err != nil {
					return err
				}
				path = base + "/pipelines/" + id + "/manifest"
			}
			out, err := c.Call(http.MethodGet, path, nil, nil)
			if err != nil {
				return err
			}
			fmt.Fprint(stdout, str(out.(map[string]any)["yaml"]))
			return nil
		}}
	addProject(export)

	var file string
	var dryRun, deploy bool
	runApply := func(dry, dep bool) error {
		var text []byte
		var err error
		if file == "-" {
			text, err = io.ReadAll(stdin)
		} else {
			text, err = readManifests(file)
		}
		if err != nil {
			return err
		}
		c, err := remoteClient(project, stderr)
		if err != nil {
			return err
		}
		base, err := c.Base()
		if err != nil {
			return err
		}
		out, err := c.Call(http.MethodPost, base+"/pipelines:apply", nil, map[string]any{"manifest": string(text), "dry_run": dry, "deploy": dep})
		if err != nil {
			return err
		}
		results, _ := out.([]any)
		for _, x := range results {
			r, _ := x.(map[string]any)
			line := fmt.Sprintf("%s: %s", str(r["name"]), str(r["action"]))
			if v, ok := r["version"].(float64); ok && v != 0 {
				line += fmt.Sprintf(" (v%d)", int(v))
			}
			if d := str(r["deploy"]); d != "" {
				line += ", " + d
			}
			fmt.Fprintln(stdout, bold+line+reset)
			diff, _ := r["diff"].([]any)
			for _, l := range diff {
				s := str(l)
				switch {
				case strings.HasPrefix(s, "+"):
					fmt.Fprintln(stdout, green+"  "+s+reset)
				case strings.HasPrefix(s, "-"):
					fmt.Fprintln(stdout, red+"  "+s+reset)
				default:
					fmt.Fprintln(stdout, "  "+s)
				}
			}
			notes, _ := r["notes"].([]any)
			for _, n := range notes {
				fmt.Fprintln(stdout, "  • "+str(n))
			}
		}
		return nil
	}
	apply := &cobra.Command{Use: "apply", Short: "Create or update pipelines so they match the manifest.", Args: cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error { return runApply(dryRun, deploy) }}
	apply.Flags().StringVarP(&file, "file", "f", "", "Manifest file or directory (YAML, several documents allowed; - for stdin).")
	apply.Flags().BoolVar(&dryRun, "dry-run", false, "Show what would change without changing anything.")
	apply.Flags().BoolVar(&deploy, "deploy", false, "Deploy changed pipelines (or request approval).")
	_ = apply.MarkFlagRequired("file")
	addProject(apply)
	diff := &cobra.Command{Use: "diff", Short: "Show what `apply` would change.", Args: cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error { return runApply(true, false) }}
	diff.Flags().StringVarP(&file, "file", "f", "", "Manifest file or directory.")
	_ = diff.MarkFlagRequired("file")
	addProject(diff)

	pipelines := &cobra.Command{Use: "pipelines", Short: "Operate pipelines.", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error { return cmd.Help() }}
	for _, action := range []string{"pause", "resume", "restart"} {
		cmd := &cobra.Command{Use: action + " NAME", Short: strings.ToUpper(action[:1]) + action[1:] + " a pipeline.", Args: cobra.ExactArgs(1),
			RunE: func(_ *cobra.Command, args []string) error {
				c, err := remoteClient(project, stderr)
				if err != nil {
					return err
				}
				base, err := c.Base()
				if err != nil {
					return err
				}
				id, err := c.PipelineID(args[0])
				if err != nil {
					return err
				}
				out, err := c.Call(http.MethodPost, base+"/pipelines/"+id+"/"+action, nil, nil)
				if err != nil {
					return err
				}
				p, _ := out.(map[string]any)
				fmt.Fprintf(stdout, "%s: %s requested (desired state %s).\n", str(p["name"]), action, str(p["desired_state"]))
				return nil
			}}
		addProject(cmd)
		pipelines.AddCommand(cmd)
	}
	remote.AddCommand(login, get, export, apply, diff, pipelines)
	return remote
}
