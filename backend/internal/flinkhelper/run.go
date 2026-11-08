package flinkhelper

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RESTBase is the JobManager's REST API inside the container.
const RESTBase = "http://127.0.0.1:8081"

// execLauncher runs Flink's scripts and the Flink CDC client.
type execLauncher struct{ layout Layout }

func (l execLauncher) StartJobManager(context.Context) (<-chan error, func(), error) {
	dyn, err := l.layout.DynamicProperties()
	if err != nil {
		return nil, nil, err
	}
	cmd := exec.Command(filepath.Join(l.layout.FlinkHome, "bin", "jobmanager.sh"), append([]string{"start-foreground"}, dyn...)...) //nolint:gosec,noctx // Flink's own script; stopped with SIGTERM, not a context
	cmd.Env = append(os.Environ(), l.layout.Env()...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, nil, err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	stop := func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case err := <-done:
			logf("the JobManager stopped: %v", err)
		case <-time.After(20 * time.Second):
			_ = cmd.Process.Kill()
		}
	}
	return done, stop, nil
}

func (l execLauncher) Submit(ctx context.Context, args []string) (string, error) {
	cp, err := l.layout.Classpath()
	if err != nil {
		return "", err
	}
	java := "java"
	if home := os.Getenv("JAVA_HOME"); home != "" {
		java = filepath.Join(home, "bin", "java")
	}
	dyn, err := l.layout.DynamicProperties()
	if err != nil {
		return "", err
	}
	full := append([]string{"-cp", cp, "-Dlog4j.configurationFile=file:" + filepath.Join(l.layout.FlinkCDCHome, "conf", "log4j-cli.properties"),
		"org.apache.flink.cdc.cli.CliFrontend"}, args...)
	// The client talks to the JobManager in this container.
	full = append(full, append(dyn, "-D", "rest.address=127.0.0.1")...)
	sctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(sctx, java, full...) //nolint:gosec // the image's JVM running the Flink CDC client
	cmd.Env = append(os.Environ(), l.layout.Env()...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// RunTaskManager prepares the classpath and becomes the Flink TaskManager.
func RunTaskManager(layout Layout) error {
	s, err := LoadSettings(filepath.Join(layout.ConfigDir, "settings.json"))
	if err != nil {
		return err
	}
	if err := layout.Prepare(s); err != nil {
		return err
	}
	dyn, err := layout.DynamicProperties()
	if err != nil {
		return err
	}
	script := filepath.Join(layout.FlinkHome, "bin", "taskmanager.sh")
	return syscall.Exec(script, append([]string{script, "start-foreground"}, dyn...), append(os.Environ(), layout.Env()...)) //nolint:gosec // Flink's own script
}

// RunJobManager runs the JobManager side until SIGTERM.
func RunJobManager(layout Layout) error {
	s, err := LoadSettings(filepath.Join(layout.ConfigDir, "settings.json"))
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	m := &JobManager{Layout: layout, Settings: s, REST: NewREST(RESTBase), Launch: execLauncher{layout}}
	go func() {
		if err := m.ServeHealth(ctx, s.HealthPort); err != nil {
			logf("health server: %v", err)
		}
	}()
	store, err := openStoreRetrying(ctx, m, s)
	if err != nil {
		return nil // stopped before the state database answered
	}
	defer store.Close()
	store.Generation = s.Generation
	m.Store = store
	return m.Run(ctx)
}

// openStoreRetrying connects to the state database, retrying while it is unreachable; the health
// endpoint says why the pipeline waits.
func openStoreRetrying(ctx context.Context, m *JobManager, s Settings) (*Store, error) {
	for {
		store, err := openStore(ctx, s)
		if err == nil {
			return store, nil
		}
		m.fail(PhaseStarting, "state_store", "Can't reach BeziSync's state database to load and save positions: "+err.Error())
		if !sleep(ctx, 5*time.Second) {
			return nil, ctx.Err()
		}
	}
}

func openStore(ctx context.Context, s Settings) (*Store, error) {
	dsn := os.Getenv("BZ_STATE_DSN")
	if dsn == "" {
		return nil, errors.New("BZ_STATE_DSN is not set")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("BZ_STATE_DSN: %w", err)
	}
	cfg.ConnConfig.User, cfg.ConnConfig.Password = os.Getenv("BZ_STATE_USER"), os.Getenv("BZ_STATE_PASSWORD")
	cfg.MaxConns = 2
	octx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	return OpenStore(octx, cfg, s.ShortID, os.Getenv("BZ_STATE_KEY"))
}

// InstallSelf copies the running binary to dir/bezisync (init containers inject the helper into
// a Flink CDC image this way).
func InstallSelf(dir string) error {
	self, err := os.Executable()
	if err != nil {
		return err
	}
	in, err := os.Open(self) //nolint:gosec // this binary
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	// Streamed: the init container that runs this has a small memory limit.
	tmp := filepath.Join(dir, ".bezisync.tmp")
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755) //nolint:gosec // an executable for the engine's containers
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, "bezisync"))
}
