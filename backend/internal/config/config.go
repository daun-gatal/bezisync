// Package config reads the process configuration from BEZISYNC_* environment variables
// (and an optional .env file in the working directory, which real variables override).
//
// Everything that must be known before the database is reachable lives here; everything
// users change at runtime (runtimes, policies, alert channels, ...) lives in the database.
package config

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/daun-gatal/bezisync/backend/internal/version"
)

// DevMasterKey is the development default; production refuses to start with it.
var DevMasterKey = base64.StdEncoding.EncodeToString([]byte("bezisync-development-master-key!"))

// Settings mirrors the Python Settings model field for field.
type Settings struct {
	Environment                       string
	DatabaseURL                       string
	StateDatabaseURL                  string
	ConnectorStateDatabaseURLOverride string
	MasterKey                         string
	PreviousMasterKeys                string
	PublicURL                         string
	AllowPlainHTTP                    bool
	CookieSecure                      bool
	SessionTTLHours                   int
	BootstrapAdminEmail               string
	BootstrapAdminPassword            string
	CORSOrigins                       []string
	SSRFExtraDenyCIDRs                []string
	SSRFAllowCIDRs                    []string
	ControllerTickSeconds             float64
	SchemaCheckSeconds                int
	CanaryIntervalSeconds             int
	CanaryTimeoutSeconds              int
	MetricsRetentionDays              int
	TelemetryEnabled                  bool
	TelemetryURL                      string
	Lab                               bool
	MetricsToken                      string
	SMTPHost                          string
	SMTPPort                          int
	SMTPUsername                      string
	SMTPPassword                      string
	SMTPFrom                          string
	SMTPStartTLS                      bool
	LogLevel                          string
	LogJSON                           bool
	DebeziumDefaultImage              string
	// IcebergImage overrides the Iceberg sink image (a mirror); "" uses the pinned upstream image.
	IcebergImage          string
	AgentURL              string
	AgentImage            string
	AgentChart            string
	AgentChartVersion     string
	SandboxSourceHost     string
	SandboxSourcePort     int
	SandboxSourceDatabase string
	SandboxSourceUser     string
	SandboxSourcePassword string
	SandboxKafkaBootstrap string
	SandboxDockerNetwork  string
	// Retired names settings still present that this release no longer reads.
	Retired []string
}

// retired settings: single sign-on moved to Settings → Sign-in.
var retired = []string{"OIDC_ISSUER", "OIDC_CLIENT_ID", "OIDC_CLIENT_SECRET", "OIDC_SCOPES", "OIDC_GROUPS_CLAIM", "OIDC_LABEL",
	"OIDC_ROLE_RULES", "OIDC_ALLOW_UNVERIFIED_EMAIL", "OIDC_DEFAULT_ROLE", "OIDC_DEFAULT_PROJECTS"}

// EffectiveStateDatabaseURL is where Debezium offsets and schema history live.
func (s *Settings) EffectiveStateDatabaseURL() string {
	if s.StateDatabaseURL != "" {
		return s.StateDatabaseURL
	}
	return s.DatabaseURL
}

// ConnectorStateDatabaseURL is the state database as connectors reach it.
func (s *Settings) ConnectorStateDatabaseURL() string {
	if s.ConnectorStateDatabaseURLOverride != "" {
		return s.ConnectorStateDatabaseURLOverride
	}
	return s.EffectiveStateDatabaseURL()
}

// ValidateForProduction returns fatal misconfigurations; production refuses to start with any.
func (s *Settings) ValidateForProduction() []string {
	var problems []string
	if s.Environment != "production" {
		return problems
	}
	if s.MasterKey == DevMasterKey {
		problems = append(problems, "BEZISYNC_MASTER_KEY uses the development key")
	}
	if !s.AllowPlainHTTP {
		if !s.CookieSecure {
			problems = append(problems, "BEZISYNC_COOKIE_SECURE must be true in production")
		}
		if !strings.HasPrefix(s.PublicURL, "https://") {
			problems = append(problems, "BEZISYNC_PUBLIC_URL must be https in production "+
				"(or set BEZISYNC_ALLOW_PLAIN_HTTP when the network encrypts traffic, e.g. Tailscale)")
		}
	}
	if s.BootstrapAdminPassword == "admin" || s.BootstrapAdminPassword == "password" {
		problems = append(problems, "BEZISYNC_BOOTSTRAP_ADMIN_PASSWORD is too weak")
	}
	return problems
}

// GenerateMasterKey returns base64 of 32 random bytes.
func GenerateMasterKey() string {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		panic(err)
	}
	return base64.StdEncoding.EncodeToString(raw)
}

var (
	mu      sync.Mutex
	current *Settings
)

// Get returns the process settings, loading them on first use. Invalid configuration is
// fatal, exactly like the Python process refusing to start.
func Get() *Settings {
	mu.Lock()
	defer mu.Unlock()
	if current == nil {
		s, err := Load(os.LookupEnv)
		if err != nil {
			fmt.Fprintln(os.Stderr, "invalid configuration:", err)
			os.Exit(2)
		}
		current = s
	}
	return current
}

// Set replaces the process settings (tests and key rotation).
func Set(s *Settings) {
	mu.Lock()
	defer mu.Unlock()
	current = s
}

// Reset forgets the loaded settings so the next Get reads the environment again.
func Reset() { Set(nil) }

// Load reads settings through lookup (os.LookupEnv in production) plus ./.env.
func Load(lookup func(string) (string, bool)) (*Settings, error) {
	dotenv := readDotenv(".env")
	get := func(name string) (string, bool) {
		key := "BEZISYNC_" + name
		if v, ok := lookup(key); ok {
			return v, true
		}
		// pydantic-settings matches env names case-insensitively.
		if v, ok := lookup(strings.ToLower(key)); ok {
			return v, true
		}
		v, ok := dotenv[strings.ToUpper(key)]
		return v, ok
	}
	l := loader{get: get}
	s := &Settings{
		Environment:                       l.choice("ENVIRONMENT", "development", "development", "production", "test"),
		DatabaseURL:                       l.str("DATABASE_URL", "postgresql+psycopg://bezisync:bezisync@localhost:5432/bezisync"),
		StateDatabaseURL:                  l.str("STATE_DATABASE_URL", ""),
		ConnectorStateDatabaseURLOverride: l.str("CONNECTOR_STATE_DATABASE_URL_OVERRIDE", ""),
		MasterKey:                         l.str("MASTER_KEY", DevMasterKey),
		PreviousMasterKeys:                l.str("PREVIOUS_MASTER_KEYS", ""),
		PublicURL:                         l.str("PUBLIC_URL", "http://localhost:8080"),
		AllowPlainHTTP:                    l.boolean("ALLOW_PLAIN_HTTP", false),
		CookieSecure:                      l.boolean("COOKIE_SECURE", false),
		SessionTTLHours:                   l.integer("SESSION_TTL_HOURS", 12),
		BootstrapAdminEmail:               l.str("BOOTSTRAP_ADMIN_EMAIL", "admin@bezisync.local"),
		BootstrapAdminPassword:            l.str("BOOTSTRAP_ADMIN_PASSWORD", ""),
		CORSOrigins:                       l.strList("CORS_ORIGINS", nil),
		SSRFExtraDenyCIDRs:                l.strList("SSRF_EXTRA_DENY_CIDRS", nil),
		SSRFAllowCIDRs:                    l.strList("SSRF_ALLOW_CIDRS", nil),
		ControllerTickSeconds:             l.float("CONTROLLER_TICK_SECONDS", 5.0),
		SchemaCheckSeconds:                l.integer("SCHEMA_CHECK_SECONDS", 300),
		CanaryIntervalSeconds:             l.integer("CANARY_INTERVAL_SECONDS", 300),
		CanaryTimeoutSeconds:              l.integer("CANARY_TIMEOUT_SECONDS", 600),
		MetricsRetentionDays:              l.integer("METRICS_RETENTION_DAYS", 30),
		TelemetryEnabled:                  l.boolean("TELEMETRY_ENABLED", false),
		TelemetryURL:                      l.str("TELEMETRY_URL", ""),
		Lab:                               l.boolean("LAB", false),
		MetricsToken:                      l.str("METRICS_TOKEN", ""),
		SMTPHost:                          l.str("SMTP_HOST", ""),
		SMTPPort:                          l.integer("SMTP_PORT", 587),
		SMTPUsername:                      l.str("SMTP_USERNAME", ""),
		SMTPPassword:                      l.str("SMTP_PASSWORD", ""),
		SMTPFrom:                          l.str("SMTP_FROM", "bezisync@localhost"),
		SMTPStartTLS:                      l.boolean("SMTP_STARTTLS", true),
		LogLevel:                          l.str("LOG_LEVEL", "INFO"),
		LogJSON:                           l.boolean("LOG_JSON", true),
		DebeziumDefaultImage:              l.str("DEBEZIUM_DEFAULT_IMAGE", "quay.io/debezium/server:3.7.0.Final"),
		IcebergImage:                      l.str("ICEBERG_IMAGE", ""),
		AgentURL:                          l.str("AGENT_URL", ""),
		AgentImage:                        l.str("AGENT_IMAGE", "ghcr.io/daun-gatal/bezisync:"+version.Version),
		AgentChart:                        l.str("AGENT_CHART", "oci://ghcr.io/daun-gatal/charts/bezisync-agent"),
		AgentChartVersion:                 l.str("AGENT_CHART_VERSION", version.Version),
		SandboxSourceHost:                 l.str("SANDBOX_SOURCE_HOST", ""),
		SandboxSourcePort:                 l.integer("SANDBOX_SOURCE_PORT", 5432),
		SandboxSourceDatabase:             l.str("SANDBOX_SOURCE_DATABASE", "shop"),
		SandboxSourceUser:                 l.str("SANDBOX_SOURCE_USER", "cdc_reader"),
		SandboxSourcePassword:             l.str("SANDBOX_SOURCE_PASSWORD", ""),
		SandboxKafkaBootstrap:             l.str("SANDBOX_KAFKA_BOOTSTRAP", ""),
		SandboxDockerNetwork:              l.str("SANDBOX_DOCKER_NETWORK", ""),
	}
	for _, name := range retired {
		if _, ok := get(name); ok {
			s.Retired = append(s.Retired, "BEZISYNC_"+name)
		}
	}
	if len(l.errs) > 0 {
		return nil, fmt.Errorf("%s", strings.Join(l.errs, "; "))
	}
	raw, err := base64.StdEncoding.Strict().DecodeString(s.MasterKey)
	if err != nil {
		return nil, fmt.Errorf("BEZISYNC_MASTER_KEY must be base64")
	}
	if len(raw) != 32 {
		return nil, fmt.Errorf("BEZISYNC_MASTER_KEY must decode to exactly 32 bytes")
	}
	return s, nil
}

type loader struct {
	get  func(string) (string, bool)
	errs []string
}

func (l *loader) fail(name, msg string) {
	l.errs = append(l.errs, "BEZISYNC_"+name+": "+msg)
}

func (l *loader) str(name, def string) string {
	if v, ok := l.get(name); ok {
		return v
	}
	return def
}

func (l *loader) choice(name, def string, allowed ...string) string {
	v := l.str(name, def)
	if !slices.Contains(allowed, v) {
		l.fail(name, fmt.Sprintf("must be one of %s", strings.Join(allowed, ", ")))
	}
	return v
}

// boolean accepts what pydantic accepts for bool fields.
func (l *loader) boolean(name string, def bool) bool {
	v, ok := l.get(name)
	if !ok {
		return def
	}
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "t", "yes", "y", "on":
		return true
	case "0", "false", "f", "no", "n", "off":
		return false
	}
	l.fail(name, "must be a boolean")
	return def
}

func (l *loader) integer(name string, def int) int {
	v, ok := l.get(name)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		l.fail(name, "must be an integer")
		return def
	}
	return n
}

func (l *loader) float(name string, def float64) float64 {
	v, ok := l.get(name)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil {
		l.fail(name, "must be a number")
		return def
	}
	return f
}

// strList parses a JSON array, as pydantic-settings does for list fields.
func (l *loader) strList(name string, def []string) []string {
	v, ok := l.get(name)
	if !ok {
		return def
	}
	var out []string
	if err := json.Unmarshal([]byte(v), &out); err != nil {
		l.fail(name, "must be a JSON array of strings")
		return def
	}
	return out
}

// readDotenv parses KEY=VALUE lines (optional quotes, # comments, optional "export ").
func readDotenv(path string) map[string]string {
	out := map[string]string{}
	f, err := os.Open(path) //nolint:gosec // fixed relative path, like pydantic-settings
	if err != nil {
		return out
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		if len(v) >= 2 && (v[0] == '"' && v[len(v)-1] == '"' || v[0] == '\'' && v[len(v)-1] == '\'') {
			v = v[1 : len(v)-1]
		} else if i := strings.Index(v, " #"); i >= 0 {
			v = strings.TrimSpace(v[:i])
		}
		out[strings.ToUpper(k)] = v
	}
	return out
}
