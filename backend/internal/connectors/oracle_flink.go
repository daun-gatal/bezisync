package connectors

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

// Oracle on Flink CDC (ADR 0006). Flink CDC 3.7's Oracle pipeline source embeds Debezium 1.9.8 and
// differs from Debezium Server's Oracle connector in what it can reach and what it needs:
//   - it lists the tables over its own connection, so on a multitenant database that connection
//     must land in the PDB (jdbc.url) while LogMiner still runs from the CDB root (pdb.name);
//   - Debezium 1.9 then creates its LOG_MINING_FLUSH table in the PDB but writes it from the root,
//     so BeziSync creates it in the root first (Prepare);
//   - its first copy runs ANALYZE TABLE … COMPUTE STATISTICS on every table (ANALYZE ANY);
//   - it reads only the version banners Debezium 1.9.8 can parse (FlinkReadsOracleBanner).

// OraFlushTable is the table Debezium's LogMiner reader keeps its last flushed SCN in, in the
// connector user's schema of the container it mines from (the CDB root of a multitenant database).
const OraFlushTable = "LOG_MINING_FLUSH"

// Oracle stores unquoted names in upper case; Flink CDC's Oracle reader lists them lower-cased and
// names events as Debezium does, so only upper-case names mean the same table to both.
var oraFlinkName = regexp.MustCompile(`^[A-Z0-9_]+$`)

// oraService is what may stand in the JDBC URL's service part: a service name or SID.
var oraService = regexp.MustCompile(`^[A-Za-z0-9_.$#-]+$`)

// oraHost is a host name or IPv4 address as the JDBC URL writes it (IPv6 goes in brackets).
var oraHost = regexp.MustCompile(`^[A-Za-z0-9.-]+$`)

// oraFlinkURL is the thin-driver URL of the service BeziSync reads tables from.
func oraFlinkURL(host string, port int, service string) (string, error) {
	if !oraService.MatchString(service) {
		return "", problem.New(422, "flink_oracle_service",
			fmt.Sprintf("Flink CDC can't connect to the Oracle service %q: a service name may only use letters, digits and _ . $ # -.", service))
	}
	switch {
	case oraHost.MatchString(host):
	case net.ParseIP(host) != nil && strings.Contains(host, ":"):
		host = "[" + host + "]"
	default:
		return "", problem.New(422, "flink_oracle_service", fmt.Sprintf("Flink CDC can't connect to the Oracle host %q.", host))
	}
	if port < 1 || port > 65535 {
		return "", problem.New(422, "flink_oracle_service", fmt.Sprintf("Flink CDC can't connect to port %d.", port))
	}
	return "jdbc:oracle:thin:@//" + host + ":" + strconv.Itoa(port) + "/" + service, nil
}

// FlinkSource: the connection's database (the CDB of a multitenant one) is what LogMiner mines;
// the tables are listed and copied through the PDB's service.
func (s *oracleSource) FlinkSource(conn Resolved, c CompileContext, at FlinkEndpoint, _ int) (*FlinkBlock, error) {
	cfg, sec, err := oraCfg(conn)
	if err != nil {
		return nil, err
	}
	for _, t := range c.Spec.Source.Tables.Include {
		schema, table, _ := strings.Cut(t, ".")
		if !oraFlinkName.MatchString(schema) || !oraFlinkName.MatchString(table) {
			return nil, problem.New(422, "flink_table_name",
				fmt.Sprintf("Flink CDC can't capture %s: its Oracle reader matches only upper-case names of letters, digits and underscores. Run this pipeline on Debezium instead.", t))
		}
	}
	if !oraService.MatchString(cfg.Database) {
		return nil, problem.New(422, "flink_oracle_service",
			fmt.Sprintf("Flink CDC can't read the Oracle database %q: its name may only use letters, digits and _ . $ # -.", cfg.Database))
	}
	url, err := oraFlinkURL(at.Host, at.Port, cfg.tableService())
	if err != nil {
		return nil, err
	}
	o := FlinkOptions{
		{"type", "oracle"},
		{"hostname", at.Host},
		{"port", strconv.Itoa(at.Port)},
		{"username", cfg.User},
		{"password", "${BZ_SRC_PASSWORD}"},
		{"database", cfg.Database},
		{"jdbc.url", url},
		{"tables", strings.Join(c.Spec.Source.Tables.Include, ",")},
	}
	if cfg.PDB != "" {
		o.Set("debezium.database.pdb.name", strings.ToUpper(cfg.PDB))
	}
	o.Set("debezium.lob.enabled", boolStr(cfg.LOB))
	if hb := c.Spec.Policies.HeartbeatSeconds; hb > 0 {
		// The reader takes no heartbeat.interval: Debezium's own keeps the position moving on a
		// quiet database, so a restart doesn't need redo the archive no longer holds.
		o.Set("debezium.heartbeat.interval.ms", strconv.Itoa(hb*1000))
	}
	return &FlinkBlock{Options: o, Env: map[string]string{"BZ_SRC_PASSWORD": PyStr(sec["password"])}, Connector: "oracle"}, nil
}

// FlinkTableID: Oracle events are named SCHEMA.TABLE.
func (*oracleSource) FlinkTableID(_ Resolved, table string) string { return table }

// The two banner forms Debezium 1.9.8's OracleDatabaseVersion.parse accepts, as whole-string
// matches without DOTALL: Java's . excludes \r and \n, so they are excluded here too.
var (
	oraBanner18 = regexp.MustCompile(`^(?:[^\r\n]*)(?:- Production(?:\r\n|\r|\n)(?:Version ))([0-9]+)\.([0-9]+)\.([0-9]+)\.([0-9]+)\.([0-9]+)$`)
	oraBanner   = regexp.MustCompile(`^(?:[^\r\n]*)(?:Release )([0-9]+)\.([0-9]+)\.([0-9]+)\.([0-9]+)\.([0-9]+)(?:[^\r\n]*)$`)
)

// FlinkReadsOracleBanner reports whether Flink CDC 3.7's Oracle reader (Debezium 1.9.8) can tell
// this database's version: it reads the first V$VERSION banner starting with "Oracle Database"
// (found false when there is none, as on Oracle AI Database 26ai) and parses one of two forms.
// Enterprise, Standard and Express Edition banners end in "- Production"; Oracle Database Free's
// don't, so Free fails at every release.
func FlinkReadsOracleBanner(banner string, found bool) bool {
	if !found || !strings.HasPrefix(banner, "Oracle Database") {
		return false
	}
	return oraBanner18.MatchString(banner) || oraBanner.MatchString(banner)
}

// prepareFlush makes sure the connector user has its LOG_MINING_FLUSH table in the CDB root, with
// the one row Debezium updates. Debezium Server's Oracle connector creates the same table there
// itself, so an existing one is shared and left as it is.
func (s *oracleSource) prepareFlush(ctx context.Context, conn Resolved, cfg oraConfig) (ArtifactResult, error) {
	name := cfg.owner() + "." + OraFlushTable
	db, done, err := oraConnect(ctx, conn, cfg.Database, 6*time.Second)
	if err != nil {
		return ArtifactResult{}, err
	}
	defer done()
	exists, err := oraOne[int64](ctx, db, "SELECT COUNT(*) FROM user_tables WHERE table_name = :1", OraFlushTable)
	if err != nil {
		return ArtifactResult{}, err
	}
	qualified := OraIdent(cfg.owner()) + "." + OraIdent(OraFlushTable)
	outcome := "exists"
	if exists == 0 {
		// ORA-00955: another pipeline created it in the meantime.
		if _, err := db.ExecContext(ctx, "CREATE TABLE "+qualified+" (LAST_SCN NUMBER(19,0))"); err != nil && oraCode(err) != 955 {
			return ArtifactResult{}, err
		}
		outcome = "created"
	}
	// Debezium flushes by updating the one row: an empty table (a DBA's CREATE without the INSERT,
	// or a Prepare that stopped after its CREATE) would never flush. Autocommit commits it.
	if _, err := db.ExecContext(ctx, "INSERT INTO "+qualified+" (LAST_SCN) SELECT 0 FROM dual WHERE NOT EXISTS (SELECT 1 FROM "+qualified+")"); err != nil {
		return ArtifactResult{}, err
	}
	return ArtifactResult{Kind: "table", Name: name, Outcome: outcome}, nil
}
