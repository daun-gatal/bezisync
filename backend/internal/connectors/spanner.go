package connectors

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/spanner"
	dbadmin "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

// Google Cloud Spanner source (GoogleSQL databases) through a change stream. Debezium's Spanner
// connector coordinates its partitions through two Kafka topics of its own even in Debezium
// Server, so the connection names a Kafka connection of the project (coordination_kafka). It
// cannot copy existing rows: it streams from the moment it first starts.

const (
	spKeyFile     = SecretDir + "/spanner-service-account.json"
	spRetention   = "7d"
	spCoordField  = "coordination_kafka"
	spCaptureType = "NEW_ROW_AND_OLD_VALUES"
)

var spTypeRe = regexp.MustCompile(`\([^)]*\)`)

type spannerSource struct{ BaseSource }

func init() { Register(&spannerSource{BaseSource{Title: "Google Cloud Spanner"}}) }

func (*spannerSource) Kind() string { return "source" }
func (*spannerSource) Type() string { return "spanner" }

// NamesTopicsByConnector: Spanner topics are <name>.<table>, so the name is the topic prefix.
func (*spannerSource) NamesTopicsByConnector() bool { return true }

type spConfig struct {
	Project, Instance, Database, StreamMode, Stream, Emulator string
	KeyJSON                                                   string
}

func spCfg(conn Resolved) (spConfig, error) {
	d, _ := Describe("source", "spanner")
	cfg, err := ValidateConfig(d.ConfigSchema, conn.Config)
	if err != nil {
		return spConfig{}, err
	}
	sec, err := ValidateConfig(d.SecretSchema, conn.Secrets)
	if err != nil {
		return spConfig{}, err
	}
	return spConfig{Project: Str(cfg, "project_id"), Instance: Str(cfg, "instance_id"), Database: Str(cfg, "database_id"),
		StreamMode: Str(cfg, "change_stream_mode"), Stream: strings.TrimSpace(Str(cfg, "change_stream")),
		Emulator: strings.TrimSpace(Str(cfg, "emulator_host")), KeyJSON: Str(sec, "service_account_json")}, nil
}

func (c spConfig) path() string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", c.Project, c.Instance, c.Database)
}

// stream is the change stream the pipeline reads: BeziSync's own bz_<id>, or the named one.
func (c spConfig) stream(cc CompileContext) string {
	if c.StreamMode == "existing" {
		return c.Stream
	}
	return "bz_" + cc.ShortID
}

func (c spConfig) options() []option.ClientOption {
	switch {
	case c.Emulator != "":
		return []option.ClientOption{option.WithEndpoint(c.Emulator), option.WithoutAuthentication(),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))}
	case c.KeyJSON != "":
		return []option.ClientOption{option.WithCredentialsJSON([]byte(c.KeyJSON))} //nolint:staticcheck // the key is the user's own credential
	}
	return nil // Application Default Credentials (Workload Identity)
}

func spClient(ctx context.Context, cfg spConfig) (*spanner.Client, error) {
	return spanner.NewClientWithConfig(ctx, cfg.path(), spanner.ClientConfig{
		DisableNativeMetrics: true,
	}, cfg.options()...)
}

// spSyncTopics are the Kafka topics the connector coordinates its partitions through.
func spSyncTopics(c CompileContext) []string {
	return []string{"_sync_topic_spanner_connector_" + c.Prefix(), "_rebalancing_topic_spanner_connector_" + c.Prefix()}
}

func spGrpcCode(err error) codes.Code {
	if s, ok := status.FromError(err); ok {
		return s.Code()
	}
	if c := spanner.ErrCode(err); c != codes.Unknown {
		return c
	}
	return codes.Unknown
}

func spQuery(ctx context.Context, client *spanner.Client, sql string, params map[string]any, fn func(*spanner.Row) error) error {
	it := client.Single().Query(ctx, spanner.Statement{SQL: sql, Params: params})
	defer it.Stop()
	for {
		row, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := fn(row); err != nil {
			return err
		}
	}
}

// spDuration parses Spanner's retention_period ("7d", "36h", "90m", "3600s").
func spDuration(v string) (int64, bool) {
	v = strings.TrimSpace(v)
	if len(v) < 2 {
		return 0, false
	}
	n, err := strconv.ParseInt(v[:len(v)-1], 10, 64)
	if err != nil {
		return 0, false
	}
	switch v[len(v)-1] {
	case 'd':
		return n * 86400, true
	case 'h':
		return n * 3600, true
	case 'm':
		return n * 60, true
	case 's':
		return n, true
	}
	return 0, false
}

type spStream struct {
	exists, all bool
	tables      map[string]bool
	// columns lists the watched columns of tables that don't watch all of theirs.
	columns   map[string][]string
	retention *int64 // nil: the default, one day
	capture   string
}

func spReadStream(ctx context.Context, client *spanner.Client, name string) (spStream, error) {
	s := spStream{tables: map[string]bool{}, columns: map[string][]string{}}
	if err := spQuery(ctx, client, "SELECT `ALL` FROM INFORMATION_SCHEMA.CHANGE_STREAMS WHERE CHANGE_STREAM_NAME = @n", map[string]any{"n": name},
		func(r *spanner.Row) error {
			s.exists = true
			return r.Columns(&s.all)
		}); err != nil || !s.exists {
		return s, err
	}
	partial := map[string]bool{}
	if err := spQuery(ctx, client, "SELECT TABLE_NAME, ALL_COLUMNS FROM INFORMATION_SCHEMA.CHANGE_STREAM_TABLES WHERE CHANGE_STREAM_NAME = @n", map[string]any{"n": name},
		func(r *spanner.Row) error {
			var t string
			var all bool
			if err := r.Columns(&t, &all); err != nil {
				return err
			}
			s.tables[t] = true
			partial[t] = !all
			return nil
		}); err != nil {
		return s, err
	}
	if err := spQuery(ctx, client, "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_COLUMNS WHERE CHANGE_STREAM_NAME = @n ORDER BY TABLE_NAME, COLUMN_NAME",
		map[string]any{"n": name}, func(r *spanner.Row) error {
			var t, c string
			if err := r.Columns(&t, &c); err != nil {
				return err
			}
			if partial[t] {
				s.columns[t] = append(s.columns[t], c)
			}
			return nil
		}); err != nil {
		return s, err
	}
	for t, p := range partial {
		if p && s.columns[t] == nil {
			s.columns[t] = []string{} // only the key columns
		}
	}
	err := spQuery(ctx, client, "SELECT OPTION_NAME, OPTION_VALUE FROM INFORMATION_SCHEMA.CHANGE_STREAM_OPTIONS WHERE CHANGE_STREAM_NAME = @n",
		map[string]any{"n": name}, func(r *spanner.Row) error {
			var k, v string
			if err := r.Columns(&k, &v); err != nil {
				return err
			}
			switch k {
			case "retention_period":
				if d, ok := spDuration(v); ok {
					s.retention = &d
				}
			case "value_capture_type":
				s.capture = v
			}
			return nil
		})
	return s, err
}

func (s spStream) retentionSeconds() int64 {
	if s.retention != nil {
		return *s.retention
	}
	return 86400
}

func (s spStream) covers(table string) bool { return s.all || s.tables[table] }

func spIAMFix(cfg spConfig, role string) *checks.Fix {
	return &checks.Fix{Kind: "shell", Who: "GCP admin", Code: fmt.Sprintf("gcloud spanner databases add-iam-policy-binding %s --instance=%s --project=%s \\\n", cfg.Database, cfg.Instance, cfg.Project) +
		fmt.Sprintf("  --member=serviceAccount:<service-account-email> --role=%s", role)}
}

func (s *spannerSource) Check(ctx context.Context, conn Resolved, tables []string) (checks.Report, error) {
	started := time.Now()
	cfg, err := spCfg(conn)
	if err != nil {
		return checks.Report{}, err
	}
	run := &checks.Run{}
	if cfg.Emulator != "" {
		host, port, ok := strings.Cut(cfg.Emulator, ":")
		p, _ := strconv.Atoi(port)
		if !ok || p == 0 {
			run.Add(checks.Check{Code: "sp_emulator", Title: "Emulator", Status: checks.Fail, Category: checks.CatNetwork, Blocking: true,
				Message: fmt.Sprintf("'%s' is not host:port.", cfg.Emulator)})
			return checks.Summarize(run.Checks, started), nil
		}
		if !NetworkCheck(ctx, run, host, p, "sp_emulator", "the Spanner emulator", 0) {
			return checks.Summarize(run.Checks, started), nil
		}
	} else if cfg.KeyJSON == "" {
		run.Add(checks.Check{Code: "sp_credentials", Title: "Credentials", Status: checks.Warn, Category: checks.CatAuth,
			Message: "No service-account key: BeziSync and the connector use the default credentials where they run (Workload Identity).",
			Why:     "On GKE with Workload Identity that is the runtime's service account; elsewhere, add a key."})
	}
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	client, err := spClient(cctx, cfg)
	if err != nil {
		run.Add(checks.Check{Code: "sp_database", Title: "Database", Status: checks.Fail, Category: checks.CatAuth, Blocking: true,
			Message: "BeziSync could not connect to Spanner with these credentials.", Technical: firstRunes(err.Error(), 300)})
		return checks.Summarize(run.Checks, started), nil
	}
	defer client.Close()
	var dialect string
	err = spQuery(cctx, client, "SELECT OPTION_VALUE FROM INFORMATION_SCHEMA.DATABASE_OPTIONS WHERE OPTION_NAME = 'database_dialect'", nil,
		func(r *spanner.Row) error { return r.Columns(&dialect) })
	if err != nil {
		c := checks.Check{Code: "sp_database", Title: "Database", Status: checks.Fail, Category: checks.CatAuth, Blocking: true,
			Message: fmt.Sprintf("BeziSync could not read %s.", cfg.path()), Technical: firstRunes(err.Error(), 300)}
		switch spGrpcCode(err) {
		case codes.NotFound:
			c.Message = fmt.Sprintf("The database %s does not exist.", cfg.path())
		case codes.PermissionDenied, codes.Unauthenticated:
			c.Category = checks.CatPermissions
			c.Message = "Google Cloud denied reading the database."
			c.Fix = spIAMFix(cfg, "roles/spanner.databaseReader")
		case codes.Unavailable, codes.DeadlineExceeded:
			c.Category = checks.CatNetwork
			c.Message = "Spanner did not answer."
		}
		run.Add(c)
		return checks.Summarize(run.Checks, started), nil
	}
	if dialect == "POSTGRESQL" {
		run.Add(checks.Check{Code: "sp_dialect", Title: "SQL dialect", Status: checks.Fail, Category: checks.CatCDC, Blocking: true,
			Message: "This database uses the PostgreSQL dialect, which Debezium's Spanner connector doesn't support."})
		return checks.Summarize(run.Checks, started), nil
	}
	run.Passed("sp_database", "Database", checks.CatAuth, fmt.Sprintf("Read %s.", cfg.path()), "GoogleSQL dialect")
	if err := s.streamChecks(cctx, client, run, cfg, tables); err != nil {
		return checks.Report{}, err
	}
	s.coordinationChecks(ctx, run, conn)
	return checks.Summarize(run.Checks, started), nil
}

func (s *spannerSource) streamChecks(ctx context.Context, client *spanner.Client, run *checks.Run, cfg spConfig, tables []string) error {
	existing := map[string]bool{}
	if err := spQuery(ctx, client, "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '' AND TABLE_TYPE = 'BASE TABLE'", nil,
		func(r *spanner.Row) error {
			var t string
			if err := r.Columns(&t); err != nil {
				return err
			}
			existing[cfg.Database+"."+t] = true
			return nil
		}); err != nil {
		return err
	}
	var missing []string
	for _, t := range tables {
		if !existing[t] {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		run.Add(checks.Check{Code: "sp_tables_exist", Title: "Selected tables exist", Status: checks.Fail, Category: checks.CatTables, Blocking: true,
			Message: fmt.Sprintf("Not found: %s.", strings.Join(first(missing, 10), ", "))})
	}
	if cfg.StreamMode == "existing" {
		if cfg.Stream == "" {
			run.Add(checks.Check{Code: "sp_change_stream", Title: "Change stream", Status: checks.Fail, Category: checks.CatCDC, Blocking: true,
				Message: "Name the change stream the pipelines read."})
			return nil
		}
		stream, err := spReadStream(ctx, client, cfg.Stream)
		if err != nil {
			return err
		}
		if !stream.exists {
			run.Add(checks.Check{Code: "sp_change_stream", Title: "Change stream", Status: checks.Fail, Category: checks.CatCDC, Blocking: true,
				Message: fmt.Sprintf("The database has no change stream %s.", cfg.Stream),
				Fix: &checks.Fix{Kind: "sql", Who: "DBA", Code: fmt.Sprintf("CREATE CHANGE STREAM %s FOR ALL OPTIONS (retention_period = '%s', value_capture_type = '%s');",
					cfg.Stream, spRetention, spCaptureType)}})
			return nil
		}
		var uncovered []string
		for _, t := range tables {
			_, table, _ := strings.Cut(t, ".")
			if existing[t] && !stream.covers(table) {
				uncovered = append(uncovered, table)
			}
		}
		if len(uncovered) > 0 {
			run.Add(checks.Check{Code: "sp_change_stream", Title: "Change stream", Status: checks.Fail, Category: checks.CatCDC, Blocking: true,
				Message: fmt.Sprintf("The change stream %s doesn't watch %s.", cfg.Stream, strings.Join(first(uncovered, 10), ", ")),
				Fix:     &checks.Fix{Kind: "sql", Who: "DBA", Code: fmt.Sprintf("ALTER CHANGE STREAM %s SET FOR %s;", cfg.Stream, spForList(stream, uncovered))}})
		} else {
			run.Passed("sp_change_stream", "Change stream", checks.CatCDC, fmt.Sprintf("Change stream %s watches the selected tables.", cfg.Stream), "")
		}
		if stream.capture == "OLD_AND_NEW_VALUES" || stream.capture == "NEW_VALUES" || stream.capture == "" {
			run.Add(checks.Check{Code: "sp_capture_type", Title: "Captured values", Status: checks.Warn, Category: checks.CatCDC,
				Message:   "The change stream records only the columns an update changed, so update events carry partial rows.",
				Technical: "value_capture_type = " + pick(stream.capture == "", "OLD_AND_NEW_VALUES", stream.capture),
				Fix: &checks.Fix{Kind: "sql", Who: "DBA", Code: fmt.Sprintf("ALTER CHANGE STREAM %s SET OPTIONS (value_capture_type = '%s');",
					cfg.Stream, spCaptureType)}})
		}
		days := stream.retentionSeconds()
		run.Passed("sp_retention", "Change stream retention", checks.CatResilience,
			fmt.Sprintf("A stopped pipeline can resume within %s (the change stream's retention).", HumanSeconds(days)), "")
		return nil
	}
	// BeziSync creates the pipeline's change stream: it needs to change the schema.
	run.Passed("sp_retention", "Change stream retention", checks.CatResilience,
		fmt.Sprintf("BeziSync's change streams keep changes for %s, so a stopped pipeline can resume within that.", HumanSeconds(7*86400)), "")
	if cfg.Emulator != "" {
		run.Passed("sp_change_stream", "Change stream", checks.CatCDC, "BeziSync creates each pipeline's change stream (bz_<id>).", "")
		return nil
	}
	admin, err := dbadmin.NewDatabaseAdminClient(ctx, cfg.options()...)
	if err != nil {
		return err
	}
	defer func() { _ = admin.Close() }()
	resp, err := admin.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: cfg.path(),
		Permissions: []string{"spanner.databases.updateDdl", "spanner.databases.select"}})
	granted := map[string]bool{}
	if err == nil {
		for _, p := range resp.GetPermissions() {
			granted[p] = true
		}
	}
	switch {
	case err != nil:
		run.Add(checks.Check{Code: "sp_change_stream", Title: "Change stream", Status: checks.Warn, Category: checks.CatPermissions,
			Message: "BeziSync can't tell whether it may create the pipeline's change stream.", Technical: firstRunes(err.Error(), 300)})
	case !granted["spanner.databases.updateDdl"]:
		run.Add(checks.Check{Code: "sp_change_stream", Title: "Change stream", Status: checks.Fail, Category: checks.CatPermissions, Blocking: true,
			Message: "The service account may not change the schema, so BeziSync can't create the pipeline's change stream.",
			Why:     "Grant it schema changes, or create a change stream yourself and choose \"Use an existing change stream\".",
			Fix:     spIAMFix(cfg, "roles/spanner.databaseAdmin")})
	default:
		run.Passed("sp_change_stream", "Change stream", checks.CatCDC, "BeziSync can create each pipeline's change stream (bz_<id>).", "")
	}
	return nil
}

// spForList is the FOR clause that adds tables to what a stream already watches.
func spForList(stream spStream, add []string) string {
	all := map[string]bool{}
	for t := range stream.tables {
		all[t] = true
	}
	for _, t := range add {
		all[t] = true
	}
	names := make([]string, 0, len(all))
	for t := range all {
		names = append(names, "`"+t+"`")
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

// coordinationChecks: the Kafka the connector keeps its partition state in.
func (s *spannerSource) coordinationChecks(ctx context.Context, run *checks.Run, conn Resolved) {
	linked, ok := conn.Linked[spCoordField]
	if !ok {
		run.Add(checks.Check{Code: "sp_coordination", Title: "Coordination Kafka", Status: checks.Fail, Category: checks.CatCDC, Blocking: true,
			Message: "Choose the project's Kafka connection the connector coordinates through.",
			Why:     "Debezium's Spanner connector keeps its partition state in two Kafka topics of its own, even in Debezium Server."})
		return
	}
	k, _ := GetSink("kafka")
	report, err := k.Check(ctx, linked, nil)
	if err != nil {
		run.Add(checks.Check{Code: "sp_coordination", Title: "Coordination Kafka", Status: checks.Fail, Category: checks.CatCDC, Blocking: true,
			Message: "BeziSync could not check the coordination Kafka.", Technical: err.Error()})
		return
	}
	for _, c := range report.Checks {
		c.Title = "Coordination Kafka · " + c.Title
		c.Doc = ""
		run.Add(c)
	}
	if report.OK {
		run.Passed("sp_coordination", "Coordination Kafka", checks.CatCDC, fmt.Sprintf("The connector coordinates through %s.", pick(linked.Name != "", linked.Name, "this Kafka")), "")
	}
}

func spTypeName(t string) string { return spTypeRe.ReplaceAllString(t, "") }

func (s *spannerSource) Discover(ctx context.Context, conn Resolved) (Discovery, error) {
	cfg, err := spCfg(conn)
	if err != nil {
		return Discovery{}, err
	}
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	client, err := spClient(cctx, cfg)
	if err != nil {
		return Discovery{}, err
	}
	defer client.Close()
	var names []string
	if err := spQuery(cctx, client, "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '' AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME", nil,
		func(r *spanner.Row) error {
			var t string
			if err := r.Columns(&t); err != nil {
				return err
			}
			names = append(names, t)
			return nil
		}); err != nil {
		return Discovery{}, err
	}
	cols := map[string][]Column{}
	if err := spQuery(cctx, client, `SELECT TABLE_NAME, COLUMN_NAME, SPANNER_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS
	        WHERE TABLE_SCHEMA = '' ORDER BY TABLE_NAME, ORDINAL_POSITION`, nil, func(r *spanner.Row) error {
		var table, column, typ, nullable string
		if err := r.Columns(&table, &column, &typ, &nullable); err != nil {
			return err
		}
		cols[table] = append(cols[table], Column{Name: column, Type: spTypeName(typ), Nullable: nullable == "YES", PII: LooksLikePII(column)})
		return nil
	}); err != nil {
		return Discovery{}, err
	}
	pks := map[string][]string{}
	if err := spQuery(cctx, client, `SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.INDEX_COLUMNS
	        WHERE TABLE_SCHEMA = '' AND INDEX_TYPE = 'PRIMARY_KEY' ORDER BY TABLE_NAME, ORDINAL_POSITION`, nil, func(r *spanner.Row) error {
		var table, column string
		if err := r.Columns(&table, &column); err != nil {
			return err
		}
		pks[table] = append(pks[table], column)
		return nil
	}); err != nil {
		return Discovery{}, err
	}
	var stream spStream
	if cfg.StreamMode == "existing" && cfg.Stream != "" {
		if stream, err = spReadStream(cctx, client, cfg.Stream); err != nil {
			return Discovery{}, err
		}
	}
	tables := make([]TableInfo, 0, len(names))
	for _, name := range names {
		var warnings []string
		if cfg.StreamMode == "existing" && cfg.Stream != "" && stream.exists && !stream.covers(name) {
			warnings = append(warnings, fmt.Sprintf("The change stream %s doesn't watch this table yet: the final checks show the fix.", cfg.Stream))
		}
		tables = append(tables, TableInfo{Schema: cfg.Database, Name: name, PrimaryKey: pks[name], Columns: cols[name], Warnings: warnings, Selectable: true})
	}
	return NewDiscovery(tables, false), nil
}

// RoutePattern: Spanner topics are <prefix>.<table>; {schema} is the database.
func (s *spannerSource) RoutePattern(c CompileContext) (string, string) {
	database, _, _ := strings.Cut(firstOr(c.Spec.Source.Tables.Include, "."), ".")
	naming := strings.ReplaceAll(c.Spec.Sink.Naming, "{schema}", database)
	naming = strings.ReplaceAll(naming, "{table}", "$1")
	naming = strings.ReplaceAll(naming, "{pipeline}", c.Name)
	return JavaRegexLiteral(c.Prefix()) + `\.([^.]+)`, naming
}

// TopicPattern matches "<prefix>.<table>".
func (s *spannerSource) TopicPattern(c CompileContext, table string) string {
	_, name, _ := strings.Cut(table, ".")
	return JavaRegexLiteral(c.Prefix() + "." + name)
}

func firstOr(l []string, def string) string {
	if len(l) == 0 {
		return def
	}
	return l[0]
}

func (s *spannerSource) CompileSource(conn Resolved, c CompileContext) (map[string]string, map[string]string, bool, error) {
	cfg, err := spCfg(conn)
	if err != nil {
		return nil, nil, false, err
	}
	excludes, masks := ColumnRules(c.Spec)
	if len(masks) > 0 {
		return nil, nil, false, problem.New(422, "masking_unsupported", "Spanner sources can't mask columns: remove them or keep them as they are.")
	}
	if len(excludes) > 0 && cfg.StreamMode == "existing" {
		return nil, nil, false, problem.New(422, "exclude_unsupported", fmt.Sprintf(
			"Removing columns needs the change stream BeziSync creates; %s is yours, so its columns can't be left out. Keep them, or let BeziSync create the change stream.",
			cfg.Stream))
	}
	linked, ok := conn.Linked[spCoordField]
	if !ok {
		return nil, nil, false, problem.New(422, "coordination_kafka_missing",
			"Choose the Kafka connection this Spanner source coordinates through (the one it named is gone).")
	}
	props := map[string]string{
		"debezium.source.connector.class":           "io.debezium.connector.spanner.SpannerConnector",
		"debezium.source.gcp.spanner.project.id":    cfg.Project,
		"debezium.source.gcp.spanner.instance.id":   cfg.Instance,
		"debezium.source.gcp.spanner.database.id":   cfg.Database,
		"debezium.source.gcp.spanner.change.stream": cfg.stream(c),
		// Debezium Server runs one task; the connector's own default asks for two.
		"debezium.source.tasks.max":             "1",
		"debezium.source.tasks.min":             "1",
		"debezium.source.heartbeat.interval.ms": strconv.Itoa(c.Spec.Policies.HeartbeatSeconds * 1000),
	}
	switch {
	case cfg.Emulator != "":
		props["debezium.source.gcp.spanner.emulator.host"] = "http://" + cfg.Emulator
	case cfg.KeyJSON != "":
		props["debezium.source.gcp.spanner.credentials.path"] = spKeyFile
	}
	kafka, env, err := KafkaClientProps(linked, "debezium.source.kafka.internal.client", "BZ_COORD")
	if err != nil {
		return nil, nil, false, err
	}
	for k, v := range kafka {
		props[k] = v
	}
	props["debezium.source.connector.spanner.sync.kafka.bootstrap.servers"] = kafka["debezium.source.kafka.internal.client.bootstrap.servers"]
	return props, env, false, nil
}

// SourceFiles mounts the service-account key.
func (s *spannerSource) SourceFiles(conn Resolved) (map[string]string, error) {
	cfg, err := spCfg(conn)
	if err != nil {
		return nil, err
	}
	if cfg.KeyJSON != "" && cfg.Emulator == "" {
		return map[string]string{spKeyFile: cfg.KeyJSON}, nil
	}
	return map[string]string{}, nil
}

func (s *spannerSource) ddl(ctx context.Context, cfg spConfig, statements ...string) error {
	admin, err := dbadmin.NewDatabaseAdminClient(ctx, cfg.options()...)
	if err != nil {
		return err
	}
	defer func() { _ = admin.Close() }()
	op, err := admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{Database: cfg.path(), Statements: statements})
	if err != nil {
		return err
	}
	return op.Wait(ctx)
}

// Prepare creates (or updates) the pipeline's own change stream for the selected tables.
func (s *spannerSource) Prepare(ctx context.Context, conn Resolved, c CompileContext) ([]ArtifactResult, error) {
	cfg, err := spCfg(conn)
	if err != nil {
		return nil, err
	}
	if cfg.StreamMode == "existing" {
		return []ArtifactResult{{Kind: "change_stream", Name: cfg.Stream, Outcome: "exists", Detail: "Your change stream; BeziSync does not change it."}}, nil
	}
	cctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	client, err := spClient(cctx, cfg)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	name := cfg.stream(c)
	stream, err := spReadStream(cctx, client, name)
	if err != nil {
		return nil, err
	}
	want, err := spWatchList(cctx, client, c)
	if err != nil {
		return nil, err
	}
	forList := spForClause(want)
	if !stream.exists {
		err := s.ddl(cctx, cfg, fmt.Sprintf("CREATE CHANGE STREAM %s FOR %s OPTIONS (retention_period = '%s', value_capture_type = '%s')",
			name, forList, spRetention, spCaptureType))
		if err != nil {
			return nil, err
		}
		return []ArtifactResult{{Kind: "change_stream", Name: name, Outcome: "created"}}, nil
	}
	same := len(stream.tables) == len(want) && !stream.all
	for table, cols := range want {
		have, partial := stream.columns[table]
		same = same && stream.tables[table] && (cols == nil) == !partial && strings.Join(cols, ",") == strings.Join(have, ",")
	}
	if !same {
		if err := s.ddl(cctx, cfg, fmt.Sprintf("ALTER CHANGE STREAM %s SET FOR %s", name, forList)); err != nil {
			return nil, err
		}
	}
	return []ArtifactResult{{Kind: "change_stream", Name: name, Outcome: "exists", Detail: pick(same, "", "Now watches the selected tables.")}}, nil
}

// spWatchList is what the pipeline's own change stream watches: each table, with the column list
// that leaves its removed columns out (nil: all columns). Key columns are always watched.
func spWatchList(ctx context.Context, client *spanner.Client, c CompileContext) (map[string][]string, error) {
	removed := map[string]map[string]bool{}
	for t, rules := range c.Spec.Source.Columns {
		_, table, _ := strings.Cut(t, ".")
		for _, col := range rules.Exclude {
			if removed[table] == nil {
				removed[table] = map[string]bool{}
			}
			removed[table][col] = true
		}
	}
	want := map[string][]string{}
	for _, t := range c.Spec.Source.Tables.Include {
		_, table, _ := strings.Cut(t, ".")
		want[table] = nil
	}
	for table, cols := range removed {
		if _, ok := want[table]; !ok {
			continue
		}
		keys := map[string]bool{}
		if err := spQuery(ctx, client, `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.INDEX_COLUMNS
		        WHERE TABLE_SCHEMA = '' AND TABLE_NAME = @t AND INDEX_TYPE = 'PRIMARY_KEY'`, map[string]any{"t": table}, func(r *spanner.Row) error {
			var k string
			if err := r.Columns(&k); err != nil {
				return err
			}
			keys[k] = true
			return nil
		}); err != nil {
			return nil, err
		}
		watched := []string{}
		if err := spQuery(ctx, client, `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '' AND TABLE_NAME = @t
		        ORDER BY COLUMN_NAME`, map[string]any{"t": table}, func(r *spanner.Row) error {
			var col string
			if err := r.Columns(&col); err != nil {
				return err
			}
			if !keys[col] && !cols[col] {
				watched = append(watched, col)
			}
			return nil
		}); err != nil {
			return nil, err
		}
		want[table] = watched
	}
	return want, nil
}

// spForClause renders a watch list as a FOR clause: `T1`, `T2`(`c1`, `c2`).
func spForClause(want map[string][]string) string {
	names := make([]string, 0, len(want))
	for t := range want {
		names = append(names, t)
	}
	sort.Strings(names)
	parts := make([]string, len(names))
	for i, t := range names {
		parts[i] = "`" + t + "`"
		if cols := want[t]; cols != nil {
			quoted := make([]string, len(cols))
			for j, c := range cols {
				quoted[j] = "`" + c + "`"
			}
			parts[i] += "(" + strings.Join(quoted, ", ") + ")"
		}
	}
	return strings.Join(parts, ", ")
}

// spDeleteTopics removes the connector's coordination topics (missing ones are not an error).
func spDeleteTopics(ctx context.Context, conn Resolved, c CompileContext) ([]ArtifactResult, error) {
	linked, ok := conn.Linked[spCoordField]
	if !ok {
		return []ArtifactResult{{Kind: "topic", Name: strings.Join(spSyncTopics(c), ", "), Outcome: "retained",
			Detail: "The coordination Kafka connection is gone; delete these topics there yourself."}}, nil
	}
	cfg, err := kafkaCfg(linked)
	if err != nil {
		return nil, err
	}
	client, err := kafkaClient(cfg, 8*time.Second)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	dctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	resp, err := kadm.NewClient(client).DeleteTopics(dctx, spSyncTopics(c)...)
	if err != nil {
		return nil, err
	}
	var results []ArtifactResult
	for _, t := range spSyncTopics(c) {
		r, ok := resp[t]
		switch {
		case !ok:
			results = append(results, ArtifactResult{Kind: "topic", Name: t, Outcome: "failed", Detail: "no response"})
		case r.Err == nil:
			results = append(results, ArtifactResult{Kind: "topic", Name: t, Outcome: "removed"})
		case errors.Is(r.Err, kerr.UnknownTopicOrPartition):
			results = append(results, ArtifactResult{Kind: "topic", Name: t, Outcome: "not_found"})
		default:
			results = append(results, ArtifactResult{Kind: "topic", Name: t, Outcome: "failed", Detail: r.Err.Error()})
		}
	}
	return results, nil
}

func (s *spannerSource) Cleanup(ctx context.Context, conn Resolved, c CompileContext) ([]ArtifactResult, error) {
	cfg, err := spCfg(conn)
	if err != nil {
		return nil, err
	}
	results, err := spDeleteTopics(ctx, conn, c)
	if err != nil {
		return nil, err
	}
	if cfg.StreamMode == "existing" {
		return append(results, ArtifactResult{Kind: "change_stream", Name: cfg.Stream, Outcome: "retained", Detail: "Your change stream; BeziSync does not drop it."}), nil
	}
	cctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	client, err := spClient(cctx, cfg)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	name := cfg.stream(c)
	stream, err := spReadStream(cctx, client, name)
	if err != nil {
		return nil, err
	}
	if !stream.exists {
		return append(results, ArtifactResult{Kind: "change_stream", Name: name, Outcome: "not_found"}), nil
	}
	if err := s.ddl(cctx, cfg, "DROP CHANGE STREAM "+name); err != nil {
		return nil, err
	}
	return append(results, ArtifactResult{Kind: "change_stream", Name: name, Outcome: "removed"}), nil
}

// ForgetPosition drops the coordination topics too: they hold the partitions' progress.
func (s *spannerSource) ForgetPosition(ctx context.Context, conn Resolved, c CompileContext) ([]ArtifactResult, error) {
	return spDeleteTopics(ctx, conn, c)
}

func (s *spannerSource) Probe(ctx context.Context, conn Resolved, c CompileContext, _ int64) (*SourceImpact, error) {
	cfg, err := spCfg(conn)
	if err != nil {
		return nil, err
	}
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	client, err := spClient(cctx, cfg)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	stream, err := spReadStream(cctx, client, cfg.stream(c))
	if err != nil {
		return nil, err
	}
	var kept *int64
	if stream.exists {
		v := stream.retentionSeconds()
		kept = &v
	}
	var behind *int64
	if OffsetsReader != nil {
		rows, err := OffsetsReader(ctx, c.ShortID)
		if err != nil {
			return nil, err
		}
		// One stored position per change stream partition: the newest is how far the stream has read.
		var newest time.Time
		for _, r := range rows {
			raw, _ := r["raw"].(map[string]any)
			if t, err := time.Parse(time.RFC3339Nano, PyStr(raw["offset"])); err == nil && t.After(newest) {
				newest = t
			}
		}
		if !newest.IsZero() {
			var now time.Time
			if err := spQuery(cctx, client, "SELECT CURRENT_TIMESTAMP()", nil, func(r *spanner.Row) error { return r.Columns(&now) }); err != nil {
				return nil, err
			}
			v := max(int64(now.Sub(newest).Seconds()), 0)
			behind = &v
		}
	}
	impact := TimeWindowImpact(behind, kept, "the change stream", "Raise the change stream's retention_period (up to 30 days).")
	if !stream.exists {
		impact.Summary = fmt.Sprintf("The change stream %s does not exist yet.", cfg.stream(c))
	}
	return impact, nil
}
