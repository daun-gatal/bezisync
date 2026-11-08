package connectors

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"

	"github.com/daun-gatal/bezisync/backend/internal/domain"
)

// FlinkColumns is a Flink CDC destination whose tables BeziSync reads and adds columns to at a
// start without a saved position (ADR 0012).
type FlinkColumns interface {
	// FlinkLayoutTarget is how the destination maps a job's rows onto a table.
	FlinkLayoutTarget() domain.LayoutTarget
	// FlinkTableColumns reads a destination table's columns in the table's order; exists is false
	// when the table doesn't exist (the job creates it).
	FlinkTableColumns(ctx context.Context, conn Resolved, destination string) (cols []domain.DestColumn, exists bool, err error)
	// AddFlinkColumns adds columns at the end of the table, each one empty for the rows it has,
	// and returns what it ran (for the audit). ErrNotSupported: BeziSync can't change this
	// destination's tables.
	AddFlinkColumns(ctx context.Context, conn Resolved, destination string, cols []domain.FlinkColumn) ([]string, error)
}

// FlinkColumnsByHand is a Flink CDC destination whose tables BeziSync reads but can't change
// (Paimon): AddFlinkColumns refuses, and these are the statements someone runs instead.
type FlinkColumnsByHand interface {
	FlinkAddStatements(destination string, cols []domain.FlinkColumn) []string
}

// NoFlinkTable is a Flink CDC destination without tables to plan: Kafka (events) and
// Elasticsearch (documents, mapped by name as they come).
type NoFlinkTable interface{ NoFlinkTable() }

// --- Iceberg: positional, through the catalog ---

func (s *icebergSink) FlinkLayoutTarget() domain.LayoutTarget {
	return domain.LayoutTarget{Positional: true}
}

func (s *icebergSink) FlinkTableColumns(ctx context.Context, conn Resolved, destination string) ([]domain.DestColumn, bool, error) {
	cfg, err := icebergCfg(conn)
	if err != nil {
		return nil, false, err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cat, ctx, err := cfg.open(ctx)
	if err != nil {
		return nil, false, err
	}
	tbl, err := cat.LoadTable(ctx, icebergTableIdent(cfg, destination))
	if errors.Is(err, catalog.ErrNoSuchTable) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	schema := tbl.Schema()
	keys := map[int]bool{}
	for _, id := range schema.IdentifierFieldIDs {
		keys[id] = true
	}
	parts := map[int]bool{}
	spec := tbl.Spec()
	for _, f := range spec.Fields() {
		parts[f.SourceID()] = true
	}
	var out []domain.DestColumn
	for _, f := range schema.Fields() {
		raw := f.Type.String()
		out = append(out, domain.DestColumn{Name: f.Name, Type: domain.IcebergFlinkType(raw), Raw: raw, Nullable: !f.Required, Key: keys[f.ID], Partition: parts[f.ID]})
	}
	return out, true, nil
}

// icebergTypeOf is the Iceberg type of a Flink type BeziSync writes (domain.FlinkTypeOfKind, the
// soft-delete column, added fields).
func icebergTypeOf(flinkType string) (iceberg.Type, error) {
	t := strings.ToUpper(strings.TrimSpace(flinkType))
	switch {
	case strings.HasPrefix(t, "VARCHAR"):
		return iceberg.PrimitiveTypes.String, nil
	case t == "BIGINT":
		return iceberg.PrimitiveTypes.Int64, nil
	case t == "INT":
		return iceberg.PrimitiveTypes.Int32, nil
	case t == "DOUBLE":
		return iceberg.PrimitiveTypes.Float64, nil
	case t == "FLOAT":
		return iceberg.PrimitiveTypes.Float32, nil
	case t == "BOOLEAN":
		return iceberg.PrimitiveTypes.Bool, nil
	case t == "DATE":
		return iceberg.PrimitiveTypes.Date, nil
	case t == "BYTES":
		return iceberg.PrimitiveTypes.Binary, nil
	}
	return nil, fmt.Errorf("BeziSync doesn't add %s columns to Iceberg tables", flinkType)
}

func (s *icebergSink) AddFlinkColumns(ctx context.Context, conn Resolved, destination string, cols []domain.FlinkColumn) ([]string, error) {
	if len(cols) == 0 {
		return nil, nil
	}
	cfg, err := icebergCfg(conn)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	cat, ctx, err := cfg.open(ctx)
	if err != nil {
		return nil, err
	}
	tbl, err := cat.LoadTable(ctx, icebergTableIdent(cfg, destination))
	if err != nil {
		return nil, err
	}
	tx := tbl.NewTransaction()
	upd := tx.UpdateSchema(true, false)
	var ran []string
	for _, c := range cols {
		typ, err := icebergTypeOf(c.Type)
		if err != nil {
			return nil, err
		}
		upd = upd.AddColumn([]string{c.Name}, typ, "", false, nil)
		ran = append(ran, fmt.Sprintf("add column %s %s to %s", c.Name, typ, destination))
	}
	if err := upd.Commit(); err != nil {
		return nil, err
	}
	if _, err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return ran, nil
}

// --- Paimon: positional, read from the warehouse's schema files ---

func (s *paimonSink) FlinkLayoutTarget() domain.LayoutTarget {
	return domain.LayoutTarget{Positional: true}
}

func (s *paimonSink) FlinkTableColumns(ctx context.Context, conn Resolved, destination string) ([]domain.DestColumn, bool, error) {
	cfg, err := paimonCfg(conn)
	if err != nil {
		return nil, false, err
	}
	c, err := cfg.client(ctx)
	if err != nil {
		return nil, false, err
	}
	return paimonColumns(ctx, paimonS3{c: c, bucket: cfg.Bucket}, cfg.Prefix, destination)
}

// paimonColumns reads <warehouse>/<db>.db/<table>/schema/schema-<n> with the highest n.
func paimonColumns(ctx context.Context, store paimonObjects, prefix, destination string) ([]domain.DestColumn, bool, error) {
	db, table, _ := strings.Cut(destination, ".")
	dir := strings.TrimPrefix(prefix+"/"+db+".db/"+table+"/schema/schema-", "/")
	keys, err := store.list(ctx, dir)
	if err != nil {
		return nil, false, err
	}
	latest, n := "", -1
	for _, k := range keys {
		if v, err := strconv.Atoi(strings.TrimPrefix(k, dir)); err == nil && v > n {
			latest, n = k, v
		}
	}
	if latest == "" {
		return nil, false, nil
	}
	body, err := store.get(ctx, latest)
	if err != nil {
		return nil, false, err
	}
	var schema struct {
		Fields []struct {
			Name string          `json:"name"`
			Type json.RawMessage `json:"type"`
		} `json:"fields"`
		PartitionKeys []string `json:"partitionKeys"`
		PrimaryKeys   []string `json:"primaryKeys"`
	}
	if err := json.Unmarshal(body, &schema); err != nil {
		return nil, false, fmt.Errorf("the schema of %s can't be read: %w", destination, err)
	}
	var out []domain.DestColumn
	for _, f := range schema.Fields {
		var raw string
		if json.Unmarshal(f.Type, &raw) != nil {
			raw = string(f.Type) // a nested type is an object: never mapped
		}
		typ, nullable := domain.PaimonFlinkType(raw)
		out = append(out, domain.DestColumn{Name: f.Name, Type: typ, Raw: strings.ToLower(raw), Nullable: nullable,
			Key: contains(schema.PrimaryKeys, f.Name), Partition: contains(schema.PartitionKeys, f.Name)})
	}
	return out, true, nil
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func (s *paimonSink) AddFlinkColumns(_ context.Context, _ Resolved, destination string, cols []domain.FlinkColumn) ([]string, error) {
	if len(cols) == 0 {
		return nil, nil
	}
	return nil, &ErrNotSupported{Msg: "BeziSync can't change Paimon tables. Add the columns with Flink SQL or Spark on the same catalog, then start again:\n" +
		strings.Join(s.FlinkAddStatements(destination, cols), "\n")}
}

func (s *paimonSink) FlinkAddStatements(destination string, cols []domain.FlinkColumn) []string {
	var stmts []string
	for _, c := range cols {
		stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s ADD %s %s;", destination, olapQuote(c.Name), paimonTypeOf(c.Type)))
	}
	return stmts
}

func paimonTypeOf(flinkType string) string {
	if strings.HasPrefix(strings.ToUpper(flinkType), "VARCHAR") {
		return "STRING"
	}
	return strings.ToUpper(flinkType)
}

// --- StarRocks, Doris, OceanBase: by name, over SQL ---

func (s *starrocksSink) FlinkLayoutTarget() domain.LayoutTarget {
	return domain.LayoutTarget{CaseInsensitive: true}
}
func (s *dorisSink) FlinkLayoutTarget() domain.LayoutTarget {
	return domain.LayoutTarget{CaseInsensitive: true}
}
func (s *oceanbaseSink) FlinkLayoutTarget() domain.LayoutTarget {
	return domain.LayoutTarget{CaseInsensitive: true}
}

func (s *starrocksSink) FlinkTableColumns(ctx context.Context, conn Resolved, destination string) ([]domain.DestColumn, bool, error) {
	return starrocks.tableColumns(ctx, conn, destination)
}
func (s *dorisSink) FlinkTableColumns(ctx context.Context, conn Resolved, destination string) ([]domain.DestColumn, bool, error) {
	return doris.tableColumns(ctx, conn, destination)
}
func (s *oceanbaseSink) FlinkTableColumns(ctx context.Context, conn Resolved, destination string) ([]domain.DestColumn, bool, error) {
	return oceanbase.tableColumns(ctx, conn, destination)
}
func (s *starrocksSink) AddFlinkColumns(ctx context.Context, conn Resolved, destination string, cols []domain.FlinkColumn) ([]string, error) {
	return starrocks.addColumns(ctx, conn, destination, cols)
}
func (s *dorisSink) AddFlinkColumns(ctx context.Context, conn Resolved, destination string, cols []domain.FlinkColumn) ([]string, error) {
	return doris.addColumns(ctx, conn, destination, cols)
}
func (s *oceanbaseSink) AddFlinkColumns(ctx context.Context, conn Resolved, destination string, cols []domain.FlinkColumn) ([]string, error) {
	return oceanbase.addColumns(ctx, conn, destination, cols)
}

func (o olapSink) tableColumns(ctx context.Context, conn Resolved, destination string) ([]domain.DestColumn, bool, error) {
	cfg, err := o.cfg(conn)
	if err != nil {
		return nil, false, err
	}
	db, done, err := cfg.open(ctx, 10*time.Second)
	if err != nil {
		return nil, false, err
	}
	defer done()
	schema, table, _ := strings.Cut(destination, ".")
	rows, err := db.QueryContext(ctx, `SELECT column_name, column_type, is_nullable, column_key FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position`, schema, table)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = rows.Close() }()
	var out []domain.DestColumn
	for rows.Next() {
		var name, typ, nullable string
		var key sql.NullString
		if err := rows.Scan(&name, &typ, &nullable, &key); err != nil {
			return nil, false, err
		}
		out = append(out, domain.DestColumn{Name: name, Raw: strings.ToLower(typ), Nullable: strings.EqualFold(nullable, "YES"),
			Key: key.String == "PRI" || key.String == "UNI"})
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	return out, len(out) > 0, nil
}

// olapColumnType is the SQL type a column of a Flink type gets on StarRocks, Doris or OceanBase.
func (o olapSink) olapColumnType(flinkType string) (string, error) {
	t := strings.ToUpper(strings.TrimSpace(flinkType))
	text := map[string]string{"starrocks": "STRING", "doris": "STRING", "oceanbase": "LONGTEXT"}[o.typ]
	switch {
	case strings.HasPrefix(t, "VARCHAR"):
		return text, nil
	case t == "BIGINT", t == "INT", t == "DOUBLE", t == "FLOAT", t == "DATE":
		return t, nil
	case t == "BOOLEAN":
		return "BOOLEAN", nil
	}
	return "", fmt.Errorf("BeziSync doesn't add %s columns to %s tables", flinkType, o.title)
}

func (o olapSink) addColumns(ctx context.Context, conn Resolved, destination string, cols []domain.FlinkColumn) ([]string, error) {
	if len(cols) == 0 {
		return nil, nil
	}
	cfg, err := o.cfg(conn)
	if err != nil {
		return nil, err
	}
	db, done, err := cfg.open(ctx, 10*time.Second)
	if err != nil {
		return nil, err
	}
	defer done()
	var ran []string
	for _, c := range cols {
		typ, err := o.olapColumnType(c.Type)
		if err != nil {
			return ran, err
		}
		stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s NULL", olapTable(destination), olapQuote(c.Name), typ)
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return ran, fmt.Errorf("%s: %w", stmt, err)
		}
		ran = append(ran, stmt)
		// StarRocks and Doris may run a column change as a job: wait until the column is there.
		if err := o.waitColumn(ctx, db, destination, c.Name); err != nil {
			return ran, err
		}
	}
	return ran, nil
}

func (o olapSink) waitColumn(ctx context.Context, db *sql.Conn, destination, column string) error {
	schema, table, _ := strings.Cut(destination, ".")
	deadline := time.Now().Add(2 * time.Minute)
	for {
		var n int
		err := db.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND lower(column_name) = lower(?)`,
			schema, table, column).Scan(&n)
		if err == nil && n > 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s has no column %s two minutes after adding it", destination, column)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// --- No table to plan ---

func (s *kafkaSink) NoFlinkTable()         {}
func (s *elasticsearchSink) NoFlinkTable() {}
