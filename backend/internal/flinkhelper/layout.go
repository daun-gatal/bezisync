package flinkhelper

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Layout is where things are in the container.
type Layout struct {
	ConfigDir, WorkDir, FlinkHome, FlinkCDCHome string
}

// DefaultLayout is the layout of the reference image (deploy/images/flink-cdc).
func DefaultLayout() Layout {
	return Layout{ConfigDir: DefaultConfigDir, WorkDir: DefaultWorkDir, FlinkHome: DefaultFlinkHome, FlinkCDCHome: DefaultFlinkCDCHome}
}

// LibDir and ConfDir are the per-pipeline Flink lib and conf directories the helper builds.
func (l Layout) LibDir() string  { return filepath.Join(l.WorkDir, "lib") }
func (l Layout) ConfDir() string { return filepath.Join(l.WorkDir, "conf") }

// driverHelp says how to add a JDBC driver the image lacks.
var driverHelp = map[string]string{
	"mysql": "MySQL Connector/J is missing from the Flink CDC image: MySQL/MariaDB sources and StarRocks need it. " +
		"Rebuild the image with --build-arg MYSQL_DRIVER=8.4.0, or copy the driver into /opt/flink-cdc/drivers (see the guide: Flink CDC engine).",
}

// Prepare builds the pipeline's Flink classpath and configuration: LibDir holds links to Flink's
// own JARs, Flink CDC's runtime, the pipeline's connectors and every driver the image carries;
// ConfDir holds Flink's configuration files with BeziSync's config.yaml. Any part the image lacks
// is an error that says how to fix the image.
func (l Layout) Prepare(s Settings) error {
	lib, conf := l.LibDir(), l.ConfDir()
	for _, d := range []string{lib, conf} {
		if err := os.RemoveAll(d); err != nil {
			return err
		}
		if err := os.MkdirAll(d, 0o750); err != nil {
			return err
		}
	}
	link := func(src string) error {
		return os.Symlink(src, filepath.Join(lib, filepath.Base(src)))
	}
	flinkJars, err := filepath.Glob(filepath.Join(l.FlinkHome, "lib", "*.jar"))
	if err != nil {
		return err
	}
	if len(flinkJars) == 0 {
		return fmt.Errorf("this image has no Flink in %s: use a Flink CDC image built from deploy/images/flink-cdc", l.FlinkHome)
	}
	dist, _ := filepath.Glob(filepath.Join(l.FlinkCDCHome, "lib", "flink-cdc-dist-*.jar"))
	if len(dist) != 1 {
		return fmt.Errorf("this image has no Flink CDC runtime (%s/lib/flink-cdc-dist-*.jar): use a Flink CDC image built from deploy/images/flink-cdc", l.FlinkCDCHome)
	}
	jars := append(flinkJars, dist[0])
	for _, c := range s.Connectors {
		path := filepath.Join(l.FlinkCDCHome, "connectors", c+".jar")
		if _, err := os.Stat(path); err != nil {
			return fmt.Errorf("the Flink CDC image has no %s connector (%s): rebuild it from deploy/images/flink-cdc", c, path)
		}
		jars = append(jars, path)
	}
	drivers, _ := filepath.Glob(filepath.Join(l.FlinkCDCHome, "drivers", "*.jar"))
	for _, d := range s.Drivers {
		if !hasDriver(drivers, d) {
			if help, ok := driverHelp[d]; ok {
				return errors.New(help)
			}
			return fmt.Errorf("the Flink CDC image has no %s JDBC driver in %s/drivers", d, l.FlinkCDCHome)
		}
	}
	jars = append(jars, drivers...)
	for _, j := range jars {
		if err := link(j); err != nil {
			return fmt.Errorf("classpath: %w", err)
		}
	}
	// Flink's own configuration (logging, the image's config.yaml with its JVM options); BeziSync's
	// settings are passed as dynamic properties on top (DynamicProperties).
	entries, err := os.ReadDir(filepath.Join(l.FlinkHome, "conf"))
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if err := copyFile(filepath.Join(l.FlinkHome, "conf", e.Name()), filepath.Join(conf, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// DynamicProperties are BeziSync's Flink settings (flink-conf.yaml: one "key: value" per line) as
// "-D", "key=value" argument pairs of Flink's scripts and of the Flink CDC client (the scripts read
// a "-Dkey=value" in first place as a host name): they override the image's config.yaml, and
// Flink masks sensitive keys (passwords) when it logs them.
func (l Layout) DynamicProperties() ([]string, error) {
	raw, err := os.ReadFile(filepath.Join(l.ConfigDir, "flink-conf.yaml")) //nolint:gosec // BeziSync's config dir
	if err != nil {
		return nil, err
	}
	var out []string
	for n, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		if !ok || key == "" || strings.ContainsAny(key, " \t") {
			return nil, fmt.Errorf("flink-conf.yaml line %d: want \"key: value\"", n+1)
		}
		out = append(out, "-D", key+"="+value)
	}
	return out, nil
}

// hasDriver reports a driver JAR for name among the image's drivers ("mysql" matches
// mysql-connector-j-8.4.0.jar).
func hasDriver(jars []string, name string) bool {
	for _, j := range jars {
		if strings.HasPrefix(strings.ToLower(filepath.Base(j)), name) {
			return true
		}
	}
	return false
}

// Classpath is every JAR of LibDir, for the client that submits the pipeline.
func (l Layout) Classpath() (string, error) {
	jars, err := filepath.Glob(filepath.Join(l.LibDir(), "*.jar"))
	if err != nil {
		return "", err
	}
	sort.Strings(jars)
	return strings.Join(jars, string(os.PathListSeparator)), nil
}

// Env is the environment Flink's scripts need to use the prepared directories.
func (l Layout) Env() []string {
	return []string{"FLINK_HOME=" + l.FlinkHome, "FLINK_LIB_DIR=" + l.LibDir(), "FLINK_CONF_DIR=" + l.ConfDir()}
}

func copyFile(src, dst string) error {
	in, err := os.Open(src) //nolint:gosec // a file of the image or of BeziSync's config dir
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600) //nolint:gosec // the helper's own work dir
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}
