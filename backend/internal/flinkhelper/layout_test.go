package flinkhelper

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeImage lays out a Flink CDC image under a temp dir.
func fakeImage(t *testing.T, connectors []string, drivers ...string) Layout {
	t.Helper()
	root := t.TempDir()
	l := Layout{ConfigDir: filepath.Join(root, "config"), WorkDir: filepath.Join(root, "work"),
		FlinkHome: filepath.Join(root, "flink"), FlinkCDCHome: filepath.Join(root, "flink-cdc")}
	write := func(path, content string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(l.FlinkHome, "lib", "flink-dist-2.2.1.jar"), "x")
	write(filepath.Join(l.FlinkHome, "lib", "flink-shaded-guava-31.1-jre-17.0.jar"), "x")
	write(filepath.Join(l.FlinkHome, "conf", "log4j-console.properties"), "rootLogger.level = INFO")
	write(filepath.Join(l.FlinkHome, "conf", "config.yaml"), "image: default")
	write(filepath.Join(l.FlinkCDCHome, "lib", "flink-cdc-dist-3.7.0-2.2.jar"), "x")
	for _, c := range connectors {
		write(filepath.Join(l.FlinkCDCHome, "connectors", c+".jar"), "x")
	}
	if err := os.MkdirAll(filepath.Join(l.FlinkCDCHome, "drivers"), 0o750); err != nil {
		t.Fatal(err)
	}
	for _, d := range drivers {
		write(filepath.Join(l.FlinkCDCHome, "drivers", d), "x")
	}
	write(filepath.Join(l.ConfigDir, "flink-conf.yaml"), "jobmanager.rpc.address: bz-abc")
	return l
}

func validSettings(connectors ...string) Settings {
	s := Settings{ShortID: "0123456789ab", Connectors: connectors, TaskManagers: 2}
	if err := s.check(); err != nil {
		panic(err)
	}
	return s
}

func TestPrepareBuildsClasspathAndConf(t *testing.T) {
	l := fakeImage(t, []string{"postgresql", "iceberg", "starrocks", "hadoop-client-api"}, "postgresql-42.jar")
	s := validSettings("postgresql", "iceberg")
	if err := l.Prepare(s); err != nil {
		t.Fatal(err)
	}
	cp, err := l.Classpath()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"flink-dist-2.2.1.jar", "flink-shaded-guava", "flink-cdc-dist-3.7.0-2.2.jar", "postgresql.jar", "iceberg.jar", "postgresql-42.jar"} {
		if !strings.Contains(cp, want) {
			t.Errorf("classpath lacks %s: %s", want, cp)
		}
	}
	if strings.Contains(cp, "starrocks.jar") || strings.Contains(cp, "hadoop-client-api") {
		t.Errorf("classpath holds connectors the pipeline doesn't use: %s", cp)
	}
	// The image's config.yaml (its JVM options) is kept; BeziSync's settings are dynamic properties.
	conf, err := os.ReadFile(filepath.Join(l.ConfDir(), "config.yaml"))
	if err != nil || string(conf) != "image: default" {
		t.Fatalf("config.yaml = %q, %v", conf, err)
	}
	dyn, err := l.DynamicProperties()
	if err != nil || len(dyn) != 2 || dyn[0] != "-D" || dyn[1] != "jobmanager.rpc.address=bz-abc" {
		t.Fatalf("dynamic properties %v, %v", dyn, err)
	}
	// Logging is the console only (the image's would also write rolling files).
	logging, err := os.ReadFile(filepath.Join(l.ConfDir(), "log4j-console.properties"))
	if err != nil || !strings.Contains(string(logging), "appender.console.type = CONSOLE") || strings.Contains(string(logging), "RollingFile") {
		t.Fatalf("logging %q, %v", logging, err)
	}
	env := strings.Join(l.Env(), " ")
	if !strings.Contains(env, "FLINK_LIB_DIR="+l.LibDir()) || !strings.Contains(env, "FLINK_CONF_DIR="+l.ConfDir()) {
		t.Fatal(env)
	}
	// Preparing again (a restarted container) starts from scratch.
	if err := l.Prepare(validSettings("postgresql")); err != nil {
		t.Fatal(err)
	}
	if cp, _ := l.Classpath(); strings.Contains(cp, "iceberg.jar") {
		t.Fatal("a second Prepare kept the previous connectors")
	}
}

func TestPrepareWritesTheKeystore(t *testing.T) {
	l := fakeImage(t, []string{"mysql"})
	l.SecretsDir = filepath.Join(t.TempDir(), "secrets")
	if err := os.MkdirAll(l.SecretsDir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := l.Prepare(validSettings("mysql")); err != nil {
		t.Fatal(err) // no keystore: nothing to write
	}
	if err := os.WriteFile(filepath.Join(l.SecretsDir, "keystore.p12.b64"), []byte("a2V5c3RvcmUtYnl0ZXM=\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := l.Prepare(validSettings("mysql")); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(l.KeystorePath())
	got, _ := os.ReadFile(l.KeystorePath())
	if err != nil || string(got) != "keystore-bytes" || info.Mode().Perm() != 0o600 {
		t.Fatalf("keystore %q %v %v", got, info, err)
	}
	if err := os.WriteFile(filepath.Join(l.SecretsDir, "keystore.p12.b64"), []byte("not base64!"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := l.Prepare(validSettings("mysql")); err == nil || !strings.Contains(err.Error(), "base64") {
		t.Fatalf("invalid keystore: %v", err)
	}
}

// Secret settings (the keystore password) go into the private config.yaml, quoted, never into
// the dynamic properties a command line carries.
func TestPrepareAppendsSecretSettings(t *testing.T) {
	l := fakeImage(t, []string{"mysql"})
	l.SecretsDir = filepath.Join(t.TempDir(), "secrets")
	if err := os.MkdirAll(l.SecretsDir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(l.SecretsDir, SecretSettingsFile), []byte("security.ssl.internal.keystore-password: p\"w: x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := l.Prepare(validSettings("mysql")); err != nil {
		t.Fatal(err)
	}
	conf, _ := os.ReadFile(filepath.Join(l.ConfDir(), "config.yaml"))
	if !strings.HasPrefix(string(conf), "image: default\n") || !strings.Contains(string(conf), `security.ssl.internal.keystore-password: "p\"w: x"`) {
		t.Fatalf("%s", conf)
	}
	dyn, _ := l.DynamicProperties()
	if strings.Contains(strings.Join(dyn, " "), "keystore-password") {
		t.Fatal("a secret reached the dynamic properties")
	}
	if err := os.WriteFile(filepath.Join(l.SecretsDir, SecretSettingsFile), []byte("no colon\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := l.Prepare(validSettings("mysql")); err == nil {
		t.Fatal("accepted a bad line")
	}
}

func TestPrepareExplainsWhatTheImageLacks(t *testing.T) {
	l := fakeImage(t, []string{"mysql", "starrocks"})
	s := validSettings("mysql", "starrocks")
	s.Drivers = []string{"mysql"}
	err := l.Prepare(s)
	if err == nil || !strings.Contains(err.Error(), "/opt/flink-cdc/drivers") || !strings.Contains(err.Error(), "MySQL/MariaDB sources") {
		t.Fatalf("missing MySQL driver: %v", err)
	}
	with := fakeImage(t, []string{"mysql", "starrocks"}, "mysql-connector-j-8.4.0.jar")
	if err := with.Prepare(s); err != nil {
		t.Fatal(err)
	}
	if err := l.Prepare(validSettings("fluss")); err == nil || !strings.Contains(err.Error(), "fluss connector") {
		t.Fatalf("missing connector: %v", err)
	}
	empty := Layout{ConfigDir: l.ConfigDir, WorkDir: l.WorkDir, FlinkHome: t.TempDir(), FlinkCDCHome: l.FlinkCDCHome}
	if err := empty.Prepare(validSettings("mysql")); err == nil || !strings.Contains(err.Error(), "no Flink") {
		t.Fatalf("no Flink: %v", err)
	}
	noCDC := Layout{ConfigDir: l.ConfigDir, WorkDir: l.WorkDir, FlinkHome: l.FlinkHome, FlinkCDCHome: t.TempDir()}
	if err := noCDC.Prepare(validSettings("mysql")); err == nil || !strings.Contains(err.Error(), "Flink CDC runtime") {
		t.Fatalf("no Flink CDC: %v", err)
	}
}

func TestDynamicProperties(t *testing.T) {
	l := fakeImage(t, []string{"mysql"})
	write := func(s string) {
		if err := os.WriteFile(filepath.Join(l.ConfigDir, "flink-conf.yaml"), []byte(s), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("# BeziSync\njobmanager.rpc.address: bz-abc-jm\n\nexecution.checkpointing.interval: 60 s\nsecurity.ssl.internal.keystore-password: p:w\n")
	dyn, err := l.DynamicProperties()
	want := []string{"-D", "jobmanager.rpc.address=bz-abc-jm", "-D", "execution.checkpointing.interval=60 s", "-D", "security.ssl.internal.keystore-password=p:w"}
	if err != nil || strings.Join(dyn, "|") != strings.Join(want, "|") {
		t.Fatalf("%v, %v", dyn, err)
	}
	for _, bad := range []string{"no colon here", "two words: x", ": empty key"} {
		write(bad)
		if _, err := l.DynamicProperties(); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestSettings(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "settings.json")
	write := func(s string) {
		if err := os.WriteFile(path, []byte(s), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(`{"short_id": "0123456789ab", "connectors": ["mysql", "starrocks"], "drivers": ["mysql"]}`)
	s, err := LoadSettings(path)
	if err != nil {
		t.Fatal(err)
	}
	if s.TaskManagers != 1 || s.StateDir != "/state" || s.KeepCheckpoints != 5 || s.HealthPort != 8080 || s.StopTimeoutSeconds != 90 {
		t.Fatalf("defaults: %+v", s)
	}
	for _, bad := range []string{
		`{"short_id": "nope", "connectors": ["mysql"]}`,
		`{"short_id": "0123456789ab", "connectors": []}`,
		`{"short_id": "0123456789ab", "connectors": ["../etc"]}`,
		`{"short_id": "0123456789ab", "connectors": ["mysql"], "drivers": ["a/b"]}`,
		`{"short_id": "0123456789ab", "connectors": ["mysql"], "state_dir": "relative"}`,
		`{not json`,
	} {
		write(bad)
		if _, err := LoadSettings(path); err == nil {
			t.Errorf("accepted %s", bad)
		}
	}
}
