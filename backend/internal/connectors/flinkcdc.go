package connectors

import (
	"fmt"
	"hash/crc32"
	"regexp"
	"strconv"
	"strings"

	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

// Flink CDC: sources and destinations render their part of the pipeline definition
// (pipeline.yaml). Secret values never appear there: an option holds a ${BZ_…} placeholder that
// the helper in the JobManager fills from its environment.

// FlinkOptions are a block's options in the order they are written.
type FlinkOptions [][2]string

// Set appends or replaces an option.
func (o *FlinkOptions) Set(key, value string) {
	for i := range *o {
		if (*o)[i][0] == key {
			(*o)[i][1] = value
			return
		}
	}
	*o = append(*o, [2]string{key, value})
}

// Get is an option's value ("" when unset).
func (o FlinkOptions) Get(key string) string {
	for _, kv := range o {
		if kv[0] == key {
			return kv[1]
		}
	}
	return ""
}

// FlinkBlock is a source's or destination's part of a Flink CDC pipeline.
type FlinkBlock struct {
	Options FlinkOptions
	// Env holds the secrets the placeholders in Options refer to.
	Env map[string]string
	// Connector is the connector JAR the image keeps under /opt/flink-cdc/connectors.
	Connector string
	// Drivers are JDBC drivers the image must add (drivers/), e.g. "mysql".
	Drivers []string
	// Extra are more connector JARs the destination needs (e.g. Iceberg's AWS bundle).
	Extra []string
	// Projection are columns a destination adds to every table ("__data_event_type__ AS __op"),
	// and Converter how its transform turns changes into rows ("SOFT_DELETE": keep deleted rows).
	Projection []string
	Converter  string
	// NoDefaults: the destination refuses the column defaults Flink CDC copies from the source as
	// the source writes them (StarRocks, Doris). Every column that has one is written as an
	// expression, which carries none, so new columns must be held back until a deploy.
	NoDefaults bool
	// SoftDeleteField: the column "keep deleted rows" writes the change type into, when it isn't
	// __op (StarRocks reserves __op: domain.FlinkSoftDeleteField).
	SoftDeleteField string
}

// FlinkEndpoint is where the pipeline reaches the source: the connection's host, or the SSH
// tunnel's.
type FlinkEndpoint struct {
	Host string
	Port int
}

// FlinkSource is a source Flink CDC reads.
type FlinkSource interface {
	// FlinkSource renders the source block for the spec's tables.
	FlinkSource(conn Resolved, c CompileContext, at FlinkEndpoint, parallelism int) (*FlinkBlock, error)
	// FlinkTableID is the id Flink CDC gives a spec table's events (routes, transforms).
	FlinkTableID(conn Resolved, table string) string
}

// FlinkSink is a destination Flink CDC writes.
type FlinkSink interface {
	FlinkSink(conn Resolved, c CompileContext) (*FlinkBlock, error)
	// FlinkDestination is the sink table a routed name ("{schema}_{table}" from the naming
	// pattern) becomes, e.g. "<database>.<name>".
	FlinkDestination(conn Resolved, c CompileContext, routed string) string
}

// Flink CDC reads table lists as regular expressions, and splits database, schema and table at
// unescaped dots; a dot inside a name can't be written. BeziSync only passes exact names made of
// characters that mean nothing to a regular expression.
var flinkName = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// FlinkTableNames checks every spec table can be named to Flink CDC.
func FlinkTableNames(tables []string) error {
	for _, t := range tables {
		schema, table, ok := strings.Cut(t, ".")
		if !ok || !flinkName.MatchString(schema) || !flinkName.MatchString(table) {
			return problem.New(422, "flink_table_name",
				fmt.Sprintf("Flink CDC can't capture %s: names may only use letters, digits and underscores. Run this pipeline on Debezium instead.", t))
		}
	}
	return nil
}

// FlinkSourceIgnores is why a raw source option does nothing on this source's Flink CDC reader
// ("" when it is passed on): Oracle's never hands jdbc.properties.* to its driver.
func FlinkSourceIgnores(sourceType, option string) string {
	if sourceType == "oracle" && strings.HasPrefix(option, "jdbc.properties.") {
		return "Flink CDC's Oracle reader doesn't pass jdbc.properties.* to its driver, so they would do nothing."
	}
	return ""
}

// MyServerIDRange is the MySQL server ids a Flink CDC pipeline's readers use (one each), in
// blocks of 8 so two Flink CDC pipelines either share a block (a hash collision preflight
// reports) or never overlap. Flink CDC reads server ids as signed 32-bit numbers: the blocks end
// below 2^31.
func MyServerIDRange(pipelineID string, readers int) string {
	base := int64(1_000_000_000) + int64(crc32.ChecksumIEEE([]byte(pipelineID))%140_000_000)*8
	return strconv.FormatInt(base, 10) + "-" + strconv.FormatInt(base+int64(readers)-1, 10)
}

// MySQL Connector/J's sslMode for each ssl_mode.
var mySSLModes = map[string]string{"disabled": "DISABLED", "preferred": "PREFERRED", "required": "REQUIRED",
	"verify_ca": "VERIFY_CA", "verify_identity": "VERIFY_IDENTITY"}

func (s *mysqlSource) FlinkSource(conn Resolved, c CompileContext, at FlinkEndpoint, parallelism int) (*FlinkBlock, error) {
	cfg, sec, err := myCfg(conn)
	if err != nil {
		return nil, err
	}
	o := FlinkOptions{
		{"type", "mysql"},
		{"hostname", at.Host},
		{"port", strconv.Itoa(at.Port)},
		{"username", cfg.User},
		{"password", "${BZ_SRC_PASSWORD}"},
		{"tables", strings.Join(c.Spec.Source.Tables.Include, ",")},
		{"server-id", MyServerIDRange(c.PipelineID, parallelism)},
		{"scan.newly-added-table.enabled", "true"},
		{"schema-change.enabled", "true"},
		{"jdbc.properties.sslMode", mySSLModes[cfg.SSLMode]},
		{"debezium.database.ssl.mode", cfg.SSLMode},
	}
	if cfg.SSLMode == "disabled" {
		// caching_sha2_password sends the password RSA-encrypted when there is no TLS.
		o.Set("jdbc.properties.allowPublicKeyRetrieval", "true")
	}
	if hb := c.Spec.Policies.HeartbeatSeconds; hb > 0 {
		o.Set("heartbeat.interval", strconv.Itoa(hb)+"s")
	}
	return &FlinkBlock{Options: o, Env: map[string]string{"BZ_SRC_PASSWORD": Str(sec, "password")}, Connector: "mysql", Drivers: []string{"mysql"}}, nil
}

func (*mysqlSource) FlinkTableID(_ Resolved, table string) string { return table }

func (s *postgresSource) FlinkSource(conn Resolved, c CompileContext, at FlinkEndpoint, _ int) (*FlinkBlock, error) {
	cfg, sec, err := pgCfg(conn)
	if err != nil {
		return nil, err
	}
	if !flinkName.MatchString(cfg.Database) {
		return nil, problem.New(422, "flink_table_name",
			fmt.Sprintf("Flink CDC can't read the database %s: its name may only use letters, digits and underscores.", cfg.Database))
	}
	tables := make([]string, len(c.Spec.Source.Tables.Include))
	for i, t := range c.Spec.Source.Tables.Include {
		tables[i] = cfg.Database + "." + t
	}
	o := FlinkOptions{
		{"type", "postgres"},
		{"hostname", at.Host},
		{"port", strconv.Itoa(at.Port)},
		{"username", cfg.User},
		{"password", "${BZ_SRC_PASSWORD}"},
		{"tables", strings.Join(tables, ",")},
		// The slot and publication Debezium would use: Prepare, source protection and cleanup
		// work the same for both engines.
		{"slot.name", pgSlotName(c)},
		{"decoding.plugin.name", "pgoutput"},
		{"debezium.publication.name", pgSlotName(c)},
		{"debezium.publication.autocreate.mode", "filtered"},
		{"debezium.database.sslmode", cfg.SSLMode},
		{"scan.newly-added-table.enabled", "true"},
		{"schema-change.enabled", "true"},
	}
	if hb := c.Spec.Policies.HeartbeatSeconds; hb > 0 {
		o.Set("heartbeat.interval", strconv.Itoa(hb)+"s")
		// Keeps the slot moving on a quiet database (the WAL it holds back stays small).
		o.Set("debezium.heartbeat.action.query", fmt.Sprintf(
			"INSERT INTO %s.heartbeat (pipeline, ts) VALUES ('%s', now()) ON CONFLICT (pipeline) DO UPDATE SET ts = EXCLUDED.ts",
			pgManagedSchema, c.PipelineID))
	}
	return &FlinkBlock{Options: o, Env: map[string]string{"BZ_SRC_PASSWORD": Str(sec, "password")}, Connector: "postgresql"}, nil
}

// FlinkTableID: PostgreSQL events are named schema.table (the database is the connection's).
func (*postgresSource) FlinkTableID(_ Resolved, table string) string { return table }

// SQL Server: one database per pipeline (the connection's); Flink CDC names events by all three
// parts, database.schema.table.
func (s *sqlserverSource) FlinkSource(conn Resolved, c CompileContext, at FlinkEndpoint, _ int) (*FlinkBlock, error) {
	cfg, sec, err := msCfg(conn)
	if err != nil {
		return nil, err
	}
	if !flinkName.MatchString(cfg.Database) {
		return nil, problem.New(422, "flink_table_name",
			fmt.Sprintf("Flink CDC can't read the database %s: its name may only use letters, digits and underscores.", cfg.Database))
	}
	tables := make([]string, len(c.Spec.Source.Tables.Include))
	for i, t := range c.Spec.Source.Tables.Include {
		tables[i] = cfg.Database + "." + t
	}
	o := FlinkOptions{
		{"type", "sqlserver"},
		{"hostname", at.Host},
		{"port", strconv.Itoa(at.Port)},
		{"username", cfg.User},
		{"password", "${BZ_SRC_PASSWORD}"},
		{"tables", strings.Join(tables, ",")},
		{"scan.newly-added-table.enabled", "true"},
		{"schema-change.enabled", "true"},
		{"jdbc.properties.encrypt", boolStr(cfg.Encrypt)},
		{"jdbc.properties.trustServerCertificate", boolStr(cfg.TrustCert)},
		{"debezium.database.encrypt", boolStr(cfg.Encrypt)},
		{"debezium.database.trustServerCertificate", boolStr(cfg.TrustCert)},
	}
	return &FlinkBlock{Options: o, Env: map[string]string{"BZ_SRC_PASSWORD": Str(sec, "password")}, Connector: "sqlserver"}, nil
}

// FlinkTableID: SQL Server events carry the database too.
func (*sqlserverSource) FlinkTableID(conn Resolved, table string) string {
	return Str(conn.Config, "database") + "." + table
}
