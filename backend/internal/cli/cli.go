// Package cli is the `bezisync` command line: run the server roles and administer an
// install. `bezisync remote` talks to a control plane with an API token.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"

	"github.com/daun-gatal/bezisync/backend/internal/agent"
	"github.com/daun-gatal/bezisync/backend/internal/api"
	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/controller"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
	"github.com/daun-gatal/bezisync/backend/internal/legal"
	"github.com/daun-gatal/bezisync/backend/internal/logging"
	"github.com/daun-gatal/bezisync/backend/internal/migrate"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/version"
)

// ExitError ends a command with a status code (after printing its own message).
type ExitError struct{ Code int }

func (e *ExitError) Error() string { return fmt.Sprintf("exit %d", e.Code) }

// usageError is a bad parameter (exit code 2, like Click).
type usageError struct{ msg string }

func (e *usageError) Error() string { return e.msg }

// Execute runs the command line and returns the process exit code.
func Execute(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	root := newRoot(stdin, stdout, stderr)
	root.SetArgs(args)
	root.SetIn(stdin)
	root.SetOut(stdout)
	root.SetErr(stderr)
	err := root.Execute()
	var exit *ExitError
	var usage *usageError
	switch {
	case err == nil:
		return 0
	case errors.As(err, &exit):
		return exit.Code
	case errors.As(err, &usage):
		fmt.Fprintf(stderr, "Error: %s\n", usage.msg)
		return 2
	default:
		fmt.Fprintf(stderr, "Error: %s\n", err)
		return 1
	}
}

// signalContext is cancelled on SIGTERM or SIGINT.
func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
}

func envOr(name, def string) string {
	if v, ok := os.LookupEnv(name); ok {
		return v
	}
	return def
}

func envBool(name string) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func newRoot(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	root := &cobra.Command{Use: "bezisync", Short: "BeziSync control plane", SilenceUsage: true, SilenceErrors: true,
		Version: version.Version, Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error { return cmd.Help() }}
	root.CompletionOptions.DisableDefaultCmd = true
	root.AddCommand(engineCmd(), installSelfCmd())
	root.AddCommand(apiCmd(), controllerCmd(), agentCmd(), migrateCmd(stdout), openapiCmd(stdout), uninstallCmd(stdout),
		tunnelCmd(), tunnelProbeCmd(), healthcheckCmd(), checkDocsCmd(stdout), rotateKeyCmd(stdout), generateKeyCmd(stdout), passwordLoginCmd(stdout), licensesCmd(stdout), remoteCmd(stdin, stdout, stderr))
	return root
}

func apiCmd() *cobra.Command {
	var host string
	var port, workers int
	cmd := &cobra.Command{Use: "api", Short: "Serve the REST API.", Args: cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			logging.Configure()
			if err := httpapi.CheckProduction(); err != nil {
				return err
			}
			ctx, stop := signalContext()
			defer stop()
			defer db.Close()
			srv := &http.Server{Addr: net.JoinHostPort(host, strconv.Itoa(port)), Handler: api.Router(), ReadHeaderTimeout: 15 * time.Second,
				IdleTimeout: 120 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
			errc := make(chan error, 1)
			go func() { errc <- srv.ListenAndServe() }()
			logging.Logger("bezisync.http").Info(fmt.Sprintf("api %s listening on %s", version.Version, srv.Addr))
			if retired := config.Get().Retired; len(retired) > 0 {
				logging.Logger("bezisync.http").Warn("These settings are no longer read; set up single sign-on in Settings → Sign-in instead: " + strings.Join(retired, ", "))
			}
			select {
			case err := <-errc:
				return err
			case <-ctx.Done():
			}
			shutdown, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			return srv.Shutdown(shutdown)
		}}
	cmd.Flags().StringVar(&host, "host", "0.0.0.0", "Address to listen on.")
	cmd.Flags().IntVar(&port, "port", 8000, "Port to listen on.")
	cmd.Flags().IntVar(&workers, "workers", 1, "Accepted for compatibility; one process serves all requests.")
	return cmd
}

func controllerCmd() *cobra.Command {
	return &cobra.Command{Use: "controller", Short: "Run the controller (reconciliation, health, jobs). Safe to run several replicas.", Args: cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			logging.Configure()
			ctx, stop := signalContext()
			defer stop()
			defer db.Close()
			err := controller.Main(ctx)
			if ctx.Err() != nil {
				return nil
			}
			return err
		}}
}

func agentCmd() *cobra.Command {
	var url, token, kind, namespace, dockerHost, dockerNetwork, dockerPullTimeout string
	var insecure bool
	cmd := &cobra.Command{Use: "agent", Short: "Run an agent next to a remote runtime. It only makes outbound connections.", Args: cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			if url == "" {
				return &usageError{"Missing option '--url'."}
			}
			if token == "" {
				return &usageError{"Missing option '--token'."}
			}
			logging.Configure()
			cfg := map[string]any{"host": dockerHost, "tls_verify": false, "network": dockerNetwork, "image_pull_timeout": dockerPullTimeout}
			if kind == "kubernetes" {
				cfg = map[string]any{"namespace": namespace, "in_cluster": true}
			}
			ctx, stop := signalContext()
			defer stop()
			a, err := agent.New(ctx, url, token, kind, cfg, !insecure)
			if err != nil {
				return err
			}
			logging.Logger("bezisync.agent").Info(fmt.Sprintf("agent %s (%s) connecting to %s", version.Version, kind, url))
			return a.Run(ctx)
		}}
	f := cmd.Flags()
	f.StringVar(&url, "url", os.Getenv("BEZISYNC_AGENT_URL"), "Control-plane URL, e.g. https://bezisync.example.com [env BEZISYNC_AGENT_URL]")
	f.StringVar(&token, "token", os.Getenv("BEZISYNC_AGENT_TOKEN"), "Enrollment token shown when the runtime was added. [env BEZISYNC_AGENT_TOKEN]")
	f.StringVar(&kind, "kind", envOr("BEZISYNC_AGENT_KIND", "kubernetes"), "kubernetes or docker [env BEZISYNC_AGENT_KIND]")
	f.StringVar(&namespace, "namespace", os.Getenv("BEZISYNC_AGENT_NAMESPACE"), "Kubernetes namespace for connectors. [env BEZISYNC_AGENT_NAMESPACE]")
	f.StringVar(&dockerHost, "docker-host", os.Getenv("BEZISYNC_AGENT_DOCKER_HOST"), "Docker endpoint (default: local). [env BEZISYNC_AGENT_DOCKER_HOST]")
	f.StringVar(&dockerNetwork, "docker-network", os.Getenv("BEZISYNC_AGENT_DOCKER_NETWORK"), "[env BEZISYNC_AGENT_DOCKER_NETWORK]")
	f.StringVar(&dockerPullTimeout, "docker-image-pull-timeout", envOr("BEZISYNC_AGENT_DOCKER_IMAGE_PULL_TIMEOUT", "30m"),
		"How long one image download may take: 10m, 30m, 1h or 2h. [env BEZISYNC_AGENT_DOCKER_IMAGE_PULL_TIMEOUT]")
	f.BoolVar(&insecure, "insecure", envBool("BEZISYNC_AGENT_INSECURE"), "Skip TLS verification (testing only). [env BEZISYNC_AGENT_INSECURE]")
	return cmd
}

func migrateCmd(stdout io.Writer) *cobra.Command {
	return &cobra.Command{Use: "migrate", Short: "Apply database migrations, then run first-start bootstrap.", Args: cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			logging.Configure()
			ctx := context.Background()
			defer db.Close()
			if err := migrate.Up(ctx, db.NormalizeURL(config.Get().DatabaseURL)); err != nil {
				return err
			}
			if err := db.Tx(ctx, func(tx pgx.Tx) error {
				if _, err := services.Bootstrap(ctx, tx); err != nil {
					return err
				}
				_, err := services.EnsureSandbox(ctx, tx)
				return err
			}); err != nil {
				return err
			}
			fmt.Fprintln(stdout, "migrations applied")
			return nil
		}}
}

// OpenAPIDocument is the API document with this build's version, written like
// json.dumps(indent=2, sort_keys=True).
func OpenAPIDocument() (string, error) {
	doc, err := pyjson.DecodeGeneric(api.OpenAPI())
	if err != nil {
		return "", err
	}
	if m, ok := doc.(map[string]any); ok {
		if info, ok := m["info"].(map[string]any); ok {
			info["version"] = version.Version
		}
	}
	out, err := pyjson.DumpsIndent(doc, 2)
	return out + "\n", err
}

func writeOutput(stdout io.Writer, output, content string) error {
	if output == "-" {
		_, err := io.WriteString(stdout, content)
		return err
	}
	return os.WriteFile(output, []byte(content), 0o644) //nolint:gosec // a document meant to be read
}

func openapiCmd(stdout io.Writer) *cobra.Command {
	var output string
	cmd := &cobra.Command{Use: "openapi", Short: "Write the OpenAPI document (the frontend client is generated from it).", Args: cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			doc, err := OpenAPIDocument()
			if err != nil {
				return err
			}
			return writeOutput(stdout, output, doc)
		}}
	cmd.Flags().StringVar(&output, "output", "-", "File to write (- for stdout).")
	return cmd
}

func checkDocsCmd(stdout io.Writer) *cobra.Command {
	var output string
	cmd := &cobra.Command{Use: "check-docs", Short: "Write the preflight check reference (docs/reference/checks.md) from the source.", Args: cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			return writeOutput(stdout, output, checks.Render(checks.LoadCatalog()))
		}}
	cmd.Flags().StringVar(&output, "output", "-", "File to write (- for stdout).")
	return cmd
}

func uninstallCmd(stdout io.Writer) *cobra.Command {
	var yes, dropDestinations bool
	cmd := &cobra.Command{Use: "uninstall", Short: "Remove every connector, replication slot, publication and BeziSync table on sources.",
		Long: "Remove every connector, replication slot, publication and BeziSync table on sources.\n\n" +
			"Run it before deleting BeziSync itself, while sources and runtimes are still reachable.\n" +
			"BeziSync's own database is left alone: drop it yourself afterwards.",
		Args: cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			logging.Configure()
			ctx := context.Background()
			defer db.Close()
			items, err := controller.Plan(ctx, db.MustPool(ctx))
			if err != nil {
				return err
			}
			if len(items) == 0 {
				fmt.Fprintln(stdout, "Nothing to remove: no pipelines exist.")
				return nil
			}
			for _, it := range items {
				rt := "-"
				if it.Runtime != nil && *it.Runtime != "" {
					rt = *it.Runtime
				}
				fmt.Fprintf(stdout, "%s/%s (runtime %s)\n", it.Project, it.Pipeline, rt)
				for _, a := range it.Artifacts {
					fmt.Fprintf(stdout, "  - %s\n", a)
				}
			}
			if dropDestinations {
				fmt.Fprintln(stdout, "  + destination topics/streams of each pipeline")
			}
			if !yes {
				fmt.Fprintln(stdout, "\nDry run. Re-run with --yes to remove the items above.")
				return nil
			}
			report, err := controller.Uninstall(ctx, dropDestinations)
			if err != nil {
				return err
			}
			failed := 0
			for _, e := range report {
				state := "removed"
				if !e.OK {
					state, failed = "INCOMPLETE", failed+1
				}
				fmt.Fprintf(stdout, "%s: %s\n", e.Pipeline, state)
				for _, r := range e.Results {
					detail, _ := r["detail"].(string)
					fmt.Fprintln(stdout, strings.TrimRight(fmt.Sprintf("  %-8v %v %v %s", r["outcome"], r["kind"], r["name"], detail), " "))
				}
			}
			if failed > 0 {
				fmt.Fprintf(stdout, "\n%d pipeline(s) could not be fully cleaned up; fix the cause and run again.\n", failed)
				return &ExitError{1}
			}
			fmt.Fprintln(stdout, "\nDone. You can now delete BeziSync and its database.")
			return nil
		}}
	cmd.Flags().BoolVar(&yes, "yes", false, "Actually remove; without it only the plan is printed.")
	cmd.Flags().BoolVar(&dropDestinations, "drop-destinations", false, "Also delete topics/streams.")
	return cmd
}

func tunnelCmd() *cobra.Command {
	var target, via, hostKey, listenHost string
	var listenPort int
	cmd := &cobra.Command{Use: "tunnel", Short: "Keep an SSH tunnel open for a connector (sidecar). The key comes from BZ_SSH_KEY.", Args: cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			user, server, _ := strings.Cut(via, "@")
			sshHost, sshPort, _ := strings.Cut(server, ":")
			i := strings.LastIndex(target, ":")
			host, port := "", ""
			if i >= 0 {
				host, port = target[:i], target[i+1:]
			}
			key := os.Getenv("BZ_SSH_KEY")
			portNum, perr := strconv.Atoi(port)
			if user == "" || sshHost == "" || host == "" || perr != nil || portNum < 0 || key == "" {
				return &usageError{"Invalid value: need --target host:port, --via user@host[:port] and BZ_SSH_KEY"}
			}
			sp := 22
			if sshPort != "" {
				if sp, perr = strconv.Atoi(sshPort); perr != nil {
					return &usageError{"Invalid value: need --target host:port, --via user@host[:port] and BZ_SSH_KEY"}
				}
			}
			logging.Configure()
			ctx, stop := signalContext()
			defer stop()
			err := connectors.ServeOn(ctx, &connectors.SSHConfig{Host: sshHost, Port: sp, User: user, HostKey: hostKey}, key, host, portNum, listenHost, listenPort)
			if ctx.Err() != nil {
				return nil
			}
			return err
		}}
	f := cmd.Flags()
	f.StringVar(&target, "target", "", "Database host:port as seen from the bastion.")
	f.StringVar(&via, "via", "", "user@bastion[:port]")
	f.IntVar(&listenPort, "listen-port", connectors.TunnelPort, "Port the connector connects to.")
	f.StringVar(&listenHost, "listen-host", "127.0.0.1", "Address to listen on (a Flink CDC pipeline's private network on Docker: 0.0.0.0).")
	f.StringVar(&hostKey, "host-key", os.Getenv("BZ_SSH_HOST_KEY"), "Pinned bastion host key. [env BZ_SSH_HOST_KEY]")
	_ = cmd.MarkFlagRequired("target")
	_ = cmd.MarkFlagRequired("via")
	return cmd
}

// tunnelProbeCmd is the SSH sidecar's startup probe: the tunnel accepts connections.
func tunnelProbeCmd() *cobra.Command {
	var port int
	cmd := &cobra.Command{Use: "tunnel-probe", Short: "Exit 0 when the local tunnel port accepts connections (sidecar startup probe).", Args: cobra.NoArgs,
		Hidden: true,
		RunE: func(*cobra.Command, []string) error {
			d := net.Dialer{Timeout: 2 * time.Second}
			c, err := d.DialContext(context.Background(), "tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
			if err != nil {
				return &ExitError{1}
			}
			_ = c.Close()
			return nil
		}}
	cmd.Flags().IntVar(&port, "port", connectors.TunnelPort, "Local tunnel port.")
	return cmd
}

// healthcheckCmd is a container healthcheck for images without a shell or curl.
func healthcheckCmd() *cobra.Command {
	var url string
	cmd := &cobra.Command{Use: "healthcheck", Short: "Exit 0 when the URL answers 2xx (container healthcheck).", Args: cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				return &usageError{"Invalid value for '--url': " + err.Error()}
			}
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				return &ExitError{1}
			}
			_ = res.Body.Close()
			if res.StatusCode < 200 || res.StatusCode >= 300 {
				return &ExitError{1}
			}
			return nil
		}}
	cmd.Flags().StringVar(&url, "url", "http://127.0.0.1:8000/readyz", "URL to check.")
	return cmd
}

func rotateKeyCmd(stdout io.Writer) *cobra.Command {
	return &cobra.Command{Use: "rotate-master-key", Short: "Re-encrypt every stored secret under BEZISYNC_MASTER_KEY.",
		Long: "Re-encrypt every stored secret under BEZISYNC_MASTER_KEY.\n\n" +
			"1. Generate a key; set it as BEZISYNC_MASTER_KEY and move the old one to\n" +
			"   BEZISYNC_PREVIOUS_MASTER_KEYS; restart BeziSync. 2. Run this command.\n" +
			"3. Keep the old key listed until users have signed in again and API/agent tokens were\n" +
			"   used (they move to the new key on use) or re-issued; then remove it.",
		Args: cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			ctx := context.Background()
			defer db.Close()
			var counts map[string]int
			var order []string
			if err := db.Tx(ctx, func(tx pgx.Tx) error {
				var err error
				counts, order, err = services.RotateKeys(ctx, tx)
				return err
			}); err != nil {
				return err
			}
			for _, table := range order {
				fmt.Fprintf(stdout, "%s: %d re-encrypted\n", table, counts[table])
			}
			return nil
		}}
}

// passwordLoginCmd is the way back in when single sign-on breaks while password sign-in is off.
func passwordLoginCmd(stdout io.Writer) *cobra.Command {
	return &cobra.Command{Use: "enable-password-login", Short: "Turn sign-in with email and password back on (when single sign-on is broken).",
		Args: cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			ctx := context.Background()
			defer db.Close()
			if err := db.Tx(ctx, func(tx pgx.Tx) error { return services.SetPasswordLogin(ctx, tx, true, nil) }); err != nil {
				return err
			}
			fmt.Fprintln(stdout, "Sign-in with email and password is on.")
			return nil
		}}
}

func licensesCmd(stdout io.Writer) *cobra.Command {
	return &cobra.Command{Use: "licenses", Short: "Print the licences of BeziSync and of the third-party software in this binary.", Args: cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			_, err := io.WriteString(stdout, legal.Licenses)
			return err
		}}
}

func generateKeyCmd(stdout io.Writer) *cobra.Command {
	return &cobra.Command{Use: "generate-master-key", Short: "Print a new random master key for BEZISYNC_MASTER_KEY.", Args: cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			fmt.Fprintln(stdout, config.GenerateMasterKey())
			return nil
		}}
}
