package connectors

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// Partition by (Flink CDC on Iceberg): options.partition_by maps a captured table to up to four
// partition fields — a column and identity, year, month, day, hour, bucket[N] or truncate[N] —
// compiled into the sink's partition.key ("ns.table:day(col),bucket[16](id);…"). Flink CDC applies
// it when it creates a table; an existing table keeps its partitioning, so preflight compares
// them (T18). Debezium's Iceberg build partitions by its own option (Partitioning).

// PartitionField is one field of a table's partitioning.
type PartitionField struct {
	Column    string
	Transform string
	N         int
}

// Expr is the field as partition.key writes it.
func (f PartitionField) Expr() string {
	switch f.Transform {
	case "identity":
		return f.Column
	case "bucket", "truncate":
		return fmt.Sprintf("%s[%d](%s)", f.Transform, f.N, f.Column)
	}
	return f.Transform + "(" + f.Column + ")"
}

// IcebergPartitionBy reads options.partition_by (already schema-validated): table → fields, with
// the rules the schema can't say: bucket's N is 1 to 1024 (T19), truncate's at least 1, the other
// transforms take none, no field twice, and only captured tables.
func IcebergPartitionBy(spec domain.PipelineSpec, opts map[string]any) (map[string][]PartitionField, error) {
	raw, _ := opts["partition_by"].(map[string]any)
	out := map[string][]PartitionField{}
	v := &val.V{}
	tables := make([]string, 0, len(raw))
	for t := range raw {
		tables = append(tables, t)
	}
	sort.Strings(tables)
	for _, t := range tables {
		loc := val.Loc{"sink", "options", "partition_by", t}
		if !slices.Contains(spec.Source.Tables.Include, t) {
			v.ValueError(loc, t+" is not captured by this pipeline")
			continue
		}
		items, _ := raw[t].([]any)
		var fields []PartitionField
		for i, item := range items {
			m, _ := item.(map[string]any)
			f := PartitionField{Column: Str(m, "column"), Transform: Str(m, "transform"), N: Int(m, "n")}
			at := loc.Add(i)
			switch {
			case f.Transform == "bucket" && (f.N < 1 || f.N > 1024):
				v.ValueError(at.Add("n"), "a bucket count from 1 to 1024")
			case f.Transform == "truncate" && f.N < 1:
				v.ValueError(at.Add("n"), "a width of at least 1")
			case f.Transform != "bucket" && f.Transform != "truncate" && f.N != 0:
				v.ValueError(at.Add("n"), f.Transform+" takes no number")
			case slices.ContainsFunc(fields, func(g PartitionField) bool { return g.Expr() == f.Expr() }):
				v.ValueError(at, f.Expr()+" is listed twice")
			}
			fields = append(fields, f)
		}
		out[t] = fields
	}
	if len(v.Errors) > 0 {
		return nil, problem.Validation(v.Errors)
	}
	return out, nil
}

// icebergPartitionKey is partition.key for the destinations (dest → fields): tables a route sends
// to one destination must ask for the same partitioning.
func icebergPartitionKey(byTable map[string][]PartitionField, destinations map[string]string) (string, error) {
	byDest := map[string]string{}
	from := map[string]string{}
	for _, t := range keysSorted(byTable) {
		dest := destinations[t]
		exprs := make([]string, len(byTable[t]))
		for i, f := range byTable[t] {
			exprs[i] = f.Expr()
		}
		key := strings.Join(exprs, ",")
		if prev, ok := byDest[dest]; ok && prev != key {
			return "", problem.New(422, "engine_settings", fmt.Sprintf("%s and %s are routed to %s with different partitioning: give them the same.", from[dest], t, dest))
		}
		byDest[dest], from[dest] = key, t
	}
	parts := make([]string, 0, len(byDest))
	for _, dest := range keysSorted(byDest) {
		parts = append(parts, dest+":"+byDest[dest])
	}
	return strings.Join(parts, ";"), nil
}

// flinkDestinations are the captured tables' destinations as the Flink CDC compile names them.
func (s *icebergSink) flinkDestinations(conn Resolved, c CompileContext) map[string]string {
	routed := domain.RouteTargets(c.Spec)
	out := map[string]string{}
	for _, t := range c.Spec.Source.Tables.Include {
		schema, name, _ := strings.Cut(t, ".")
		r := c.Destination(schema, name)
		if to, ok := routed[t]; ok {
			r = to
		}
		out[t] = s.Destination(conn, c, r)
	}
	return out
}

func keysSorted[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// icebergTables is what partitionChecks reads from the catalog.
type icebergTables interface {
	LoadTable(context.Context, table.Identifier) (*table.Table, error)
}

// partitionChecks (ADR 0005): each partition column is one the table writes, of a kind its
// transform takes (iceberg_partition_column, blocking: the engine fails creating the table — on
// Debezium the per-table setting is strict), and a destination table that already exists is
// partitioned as asked (iceberg_partition_existing: Iceberg keeps a table's partitioning). Only a
// table the catalog says doesn't exist is one the pipeline will create; any other error leaves
// the check unverified (Warn), never passed.
func partitionChecks(ctx context.Context, cat icebergTables, cfg icebergConfig, c CompileContext, facts PipelineFacts) []checks.Check {
	byTable, err := IcebergPartitionBy(c.Spec, facts.Options)
	if err != nil || len(byTable) == 0 {
		return nil
	}
	engine := "debezium"
	if facts.FlinkCDC {
		engine = "flink-cdc"
	}
	var out []checks.Check
	switch {
	case facts.SourceType == "mongodb":
		// The build takes a collection's columns from its first document: there are none to name.
		return []checks.Check{{Code: "iceberg_partition_column", Title: "Partitioning", Status: checks.Fail, Blocking: true, Category: checks.CatDestination,
			Message: "Partition by isn't available for MongoDB: a collection has no fixed columns, and its table takes them from the first document.",
			Fix:     &checks.Fix{Kind: "text", Who: "you", Code: "Remove Partition by in the Transform step (the tables keep the automatic partitioning)."}}}
	case facts.Tables == nil:
		out = append(out, checks.Check{Code: "iceberg_partition_column", Title: "Partitioning", Status: checks.Warn, Category: checks.CatDestination,
			Message: "Couldn't check the partition columns: the source's tables couldn't be read.",
			Fix:     &checks.Fix{Kind: "text", Who: "you", Code: "Run preflight again once the source is reachable."}})
	default:
		var bad []string
		for _, t := range facts.Tables {
			fields, ok := byTable[t.Identifier()]
			if !ok {
				continue
			}
			cols := writtenColumns(c.Spec, t, facts.SourceType, engine)
			timed := map[string][]string{}
			for _, f := range fields {
				if msg := partitionFieldProblem(t.Identifier(), f, cols); msg != "" {
					bad = append(bad, msg)
				}
				if f.Transform == "year" || f.Transform == "month" || f.Transform == "day" || f.Transform == "hour" {
					timed[f.Column] = append(timed[f.Column], f.Expr())
				}
			}
			// Iceberg refuses two time transforms of one column (they are redundant).
			for _, col := range keysSorted(timed) {
				if len(timed[col]) > 1 {
					bad = append(bad, fmt.Sprintf("%s: %s are redundant (one time transform per column)", t.Identifier(), strings.Join(timed[col], " and ")))
				}
			}
		}
		if len(bad) > 0 {
			out = append(out, checks.Check{Code: "iceberg_partition_column", Title: "Partitioning", Status: checks.Fail, Blocking: true, Category: checks.CatDestination,
				Message: "Partitioning that can't be applied: " + strings.Join(bad, "; ") + ".",
				Fix:     &checks.Fix{Kind: "text", Who: "you", Code: "Change Partition by in the Transform step."}})
		} else {
			out = append(out, checks.Check{Code: "iceberg_partition_column", Title: "Partitioning", Status: checks.Pass, Category: checks.CatDestination,
				Message: "Every partition column is one the tables write, of a kind its transform takes."})
		}
	}
	if cat == nil {
		return out
	}
	var differ, unread, same []string
	for _, t := range keysSorted(byTable) {
		dest, ok := facts.Destinations[t]
		if !ok {
			continue
		}
		tbl, err := cat.LoadTable(ctx, icebergTableIdent(cfg, dest))
		if errors.Is(err, catalog.ErrNoSuchTable) {
			continue
		}
		if err != nil {
			unread = append(unread, fmt.Sprintf("%s (%v)", dest, err))
			continue
		}
		have := TablePartitioning(tbl)
		want := make([]string, len(byTable[t]))
		for i, f := range byTable[t] {
			want[i] = f.Expr()
		}
		if strings.Join(have, ",") != strings.Join(want, ",") {
			now := strings.Join(have, ", ")
			if now == "" {
				now = "nothing"
			}
			differ = append(differ, fmt.Sprintf("%s (partitioned by %s)", dest, now))
		} else {
			same = append(same, dest)
		}
	}
	switch {
	case len(differ) > 0:
		out = append(out, checks.Check{Code: "iceberg_partition_existing", Title: "Partitioning", Status: checks.Fail, Blocking: true, Category: checks.CatDestination,
			Message: "These tables already exist partitioned otherwise, and Iceberg keeps a table's partitioning: " + strings.Join(differ, "; ") + ".",
			Fix:     &checks.Fix{Kind: "text", Who: "you", Code: "Partition by what the table has, or drop the table (Positions → Start over, dropping destinations) so the pipeline creates it again."}})
	case len(unread) > 0:
		out = append(out, checks.Check{Code: "iceberg_partition_existing", Title: "Partitioning", Status: checks.Warn, Category: checks.CatDestination,
			Message: "Couldn't read these tables from the catalog, so their partitioning is unchecked: " + strings.Join(unread, "; ") + ".",
			Fix:     &checks.Fix{Kind: "text", Who: "you", Code: "Check the catalog connection's access to the namespace, then run preflight again."}})
	case len(same) > 0:
		out = append(out, checks.Check{Code: "iceberg_partition_existing", Title: "Partitioning", Status: checks.Pass, Category: checks.CatDestination,
			Message: strings.Join(same, ", ") + " already " + pluralVerb(len(same), "exists", "exist") + ", partitioned as Partition by asks."})
	}
	return out
}

// unreachableCatalog answers every table with the error that kept the catalog from opening, so
// the checks that read tables report them unverified instead of to be created.
type unreachableCatalog struct{ err error }

func (u unreachableCatalog) LoadTable(context.Context, table.Identifier) (*table.Table, error) {
	return nil, u.err
}

// partitionFieldProblem is why a field can't be applied to a table writing cols (name → written
// kind), or "".
func partitionFieldProblem(table string, f PartitionField, cols map[string]string) string {
	kind, found := cols[f.Column]
	switch {
	case !found:
		return fmt.Sprintf("%s has no column %s", table, f.Column)
	case domain.PartitionFits(f.Transform, kind):
		return ""
	case kind == "":
		return fmt.Sprintf("%s can't be partitioned: it is written as a list, a document or a structure (on Debezium also a PostgreSQL NUMERIC, which may have no fixed scale)", f.Column)
	}
	switch f.Transform {
	case "year", "month", "day":
		return fmt.Sprintf("%s needs a date or a timestamp (%s is written as %s)", f.Expr(), f.Column, kindLabel(kind))
	case "hour":
		return fmt.Sprintf("%s needs a timestamp (%s is written as %s)", f.Expr(), f.Column, kindLabel(kind))
	case "truncate":
		return fmt.Sprintf("%s needs text, a whole number, a decimal or bytes (%s is written as %s)", f.Expr(), f.Column, kindLabel(kind))
	case "bucket":
		return fmt.Sprintf("%s can't take %s (Iceberg hashes no true/false or floating-point value)", f.Expr(), kindLabel(kind))
	}
	return fmt.Sprintf("%s can't take %s", f.Expr(), kindLabel(kind))
}

// kindLabel is a written kind in words.
func kindLabel(kind string) string {
	if l, ok := kindLabels[kind]; ok {
		return l
	}
	return kind
}

var kindLabels = map[string]string{"int": "a whole number", "decimal": "a decimal", "float": "a floating-point number", "bool": "true/false", "text": "text", "uuid": "a UUID",
	"date": "a date", "time": "a time of day", "timestamp": "a timestamp", "timestamptz": "a timestamp with time zone", "binary": "bytes"}

// TablePartitioning is an Iceberg table's partition fields as partition.key would write them.
func TablePartitioning(tbl *table.Table) []string {
	spec := tbl.Spec()
	schema := tbl.Schema()
	var out []string
	for _, f := range spec.Fields() {
		col, ok := schema.FindColumnName(f.SourceID())
		if !ok {
			col = fmt.Sprint(f.SourceID())
		}
		if tr := f.Transform.String(); tr == "identity" {
			out = append(out, col)
		} else {
			out = append(out, tr+"("+col+")") // day(col), bucket[16](col), truncate[10](col)
		}
	}
	return out
}

// writtenColumns are the columns a table's rows carry into the Iceberg table on an engine, with
// the kind the engine writes (domain.PartitionKind): the source's, minus those left out, renamed
// (renamedTo), plus added and computed
// columns and the engine's own: Debezium's __op, __deleted and __source_ts_ms (the time of the
// change), Flink CDC's soft-delete __op.
func writtenColumns(spec domain.PipelineSpec, t TableInfo, sourceType, engine string) map[string]string {
	id := t.Identifier()
	rules := spec.Source.Columns[id]
	out := map[string]string{}
	for _, col := range t.Columns {
		if slices.Contains(rules.Exclude, col.Name) {
			continue
		}
		out[renamedTo(spec, id, col.Name, engine)] = domain.PartitionKind(engine, sourceType, col.Type, spec.Values)
	}
	for _, step := range spec.Transforms {
		switch {
		case step.Type == "add_field" && (step.Tables == nil || slices.Contains(step.Tables, id)):
			out[step.Name] = "text"
		case step.Type == "computed" && step.Table == id && engine == "flink-cdc":
			out[step.Name] = domain.ComputedFunctions[step.Function].Out
		case step.Type == "soft_delete":
			out[domain.SoftDeleteField[engine]] = "text"
		}
	}
	if engine != "flink-cdc" {
		out["__op"], out["__deleted"] = "text", "text"
		out[domain.SourceTimestampField] = "timestamptz"
	}
	return out
}

// debeziumAutomatic is the partitioning the Debezium compile gives a table Partition by doesn't
// list (ADR 0005 R2): a change log by the day of the change; an upserted table by buckets of the
// key column the wizard recorded (the day of the change when it recorded none); nothing for a
// table it didn't record, an unusable recorded name, or partitioning "none".
func debeziumAutomatic(opts map[string]any, table string) []PartitionField {
	if Str(opts, "partitioning") == "none" {
		return nil
	}
	day := []PartitionField{{Column: domain.SourceTimestampField, Transform: "day"}}
	if Str(opts, "write_mode") == "append" {
		return day
	}
	keys, _ := opts["partitions"].(map[string]any)
	col, listed := keys[table].(string)
	switch {
	case !listed:
		return nil
	case col == "":
		return day
	case icebergColumnRe.MatchString(col):
		n, _ := strconv.Atoi(Str(opts, "buckets"))
		return []PartitionField{{Column: col, Transform: "bucket", N: n}}
	}
	return nil
}

// debeziumKey is the event key Debezium sends for a table: the chosen key columns it has, else
// its primary key (domain.messageKey's fallback, checked on 3.7.0).
func debeziumKey(spec domain.PipelineSpec, t TableInfo) []string {
	var found []string
	for _, k := range spec.Keys[t.Identifier()] {
		if slices.ContainsFunc(t.Columns, func(c Column) bool { return c.Name == k }) {
			found = append(found, k)
		}
	}
	if len(found) > 0 {
		return found
	}
	return t.PrimaryKey
}

// equalityFields are the columns an upsert into a new table will delete by (ADR 0005, Terms): on
// Flink CDC the key columns (chosen, else the primary key) under their written names; on
// Debezium, in an upsert mode, the key columns (debeziumKey) the written row still carries under
// their own names (a rename changes the event value, not its key). Empty: the table is appended,
// nothing is deleted.
func equalityFields(spec domain.PipelineSpec, t TableInfo, opts map[string]any, engine string) []string {
	id := t.Identifier()
	if engine != "flink-cdc" {
		if Str(opts, "write_mode") == "append" {
			return nil
		}
		// The build makes a column an identifier field when the event's key and value both carry
		// it by that name: a key column renamed or left out of the value drops out.
		written := writtenColumns(spec, t, "", engine)
		var out []string
		for _, k := range debeziumKey(spec, t) {
			if _, ok := written[k]; ok {
				out = append(out, k)
			}
		}
		return out
	}
	keys := t.PrimaryKey
	if chosen := spec.Keys[id]; len(chosen) > 0 {
		keys = chosen
	}
	out := make([]string, len(keys))
	for i, k := range keys {
		out[i] = renamedTo(spec, id, k, engine)
	}
	return out
}

// renamedTo is a column's name in the written row after the table's rename steps. On Debezium each
// step is its own ReplaceField, so a rename of a rename lands on the last name; Flink CDC's
// projection reads one map of every step's renames (a later rename of the same column wins).
func renamedTo(spec domain.PipelineSpec, table, name, engine string) string {
	flat := map[string]string{}
	for _, step := range spec.Transforms {
		if step.Type != "rename" || (step.Tables != nil && !slices.Contains(step.Tables, table)) {
			continue
		}
		for _, r := range step.Renames {
			if engine == "flink-cdc" {
				flat[r.From] = r.To
			} else if r.From == name {
				name = r.To
				break
			}
		}
	}
	if to, ok := flat[name]; ok {
		return to
	}
	return name
}

// partitionKeyChecks is iceberg_partition_key (ADR 0005 R5): equality deletes reach only data
// files in the same partition, so on an upserted table every partition column must be one the
// upsert deletes by; otherwise an update that changes it leaves the old row behind. A table the
// pipeline will create is checked against the fields its destination would get (Partition by,
// else Debezium's automatic partitioning, after routes); a table that exists, against its own partitioning and identifier
// fields, which are what the writer uses (fixed when the table was created).
func partitionKeyChecks(ctx context.Context, cat icebergTables, cfg icebergConfig, c CompileContext, facts PipelineFacts) []checks.Check {
	if facts.SourceType == "mongodb" {
		return nil
	}
	byTable, err := IcebergPartitionBy(c.Spec, facts.Options)
	if err != nil {
		return nil
	}
	engine := "debezium"
	if facts.FlinkCDC {
		engine = "flink-cdc"
	}
	// What each table's destination is partitioned by: Partition by on Flink CDC; on Debezium the
	// destination's fields after routes (Partition by, else the agreeing automatic default).
	fieldsOf := func(table string) []PartitionField { return byTable[table] }
	if engine != "flink-cdc" {
		if byTopic, topicOf, err := debeziumDestinationFields(c, facts.Options); err == nil {
			fieldsOf = func(table string) []PartitionField { return byTopic[topicOf[table]] }
		}
	}
	if facts.Tables == nil {
		for _, t := range c.Spec.Source.Tables.Include {
			if len(fieldsOf(t)) > 0 {
				return []checks.Check{{Code: "iceberg_partition_key", Title: "Partitioning", Status: checks.Warn, Category: checks.CatDestination,
					Message: "Couldn't check that each table is partitioned by its key columns: the source's tables couldn't be read.",
					Fix:     &checks.Fix{Kind: "text", Who: "you", Code: "Run preflight again once the source is reachable."}}}
			}
		}
		return nil
	}
	var unsafe, atRisk, unread []string
	checked := false
	for _, t := range facts.Tables {
		id := t.Identifier()
		if dest, ok := facts.Destinations[id]; ok && cat != nil {
			tbl, err := cat.LoadTable(ctx, icebergTableIdent(cfg, dest))
			switch {
			case err == nil:
				checked = true
				if msg := livePartitionRisk(tbl, facts); msg != "" {
					atRisk = append(atRisk, dest+" "+msg)
				}
				continue
			case !errors.Is(err, catalog.ErrNoSuchTable):
				unread = append(unread, dest)
				continue
			}
		}
		fields := fieldsOf(id)
		if len(fields) == 0 {
			continue
		}
		checked = true
		keys := equalityFields(c.Spec, t, facts.Options, engine)
		if len(keys) == 0 {
			continue
		}
		for _, f := range fields {
			if !slices.Contains(keys, f.Column) {
				unsafe = append(unsafe, fmt.Sprintf("%s by %s (its key: %s)", id, f.Expr(), strings.Join(keys, ", ")))
			}
		}
	}
	var out []checks.Check
	if len(unsafe) > 0 {
		out = append(out, checks.Check{Code: "iceberg_partition_key", Title: "Partitioning", Status: checks.Fail, Blocking: true, Category: checks.CatDestination,
			Message: "Rows are upserted by key, and a delete only reaches rows in its own partition: partitioning by a column outside the key leaves the old row behind whenever an update changes it. Partitioned outside the key: " + strings.Join(unsafe, "; ") + ".",
			Fix:     &checks.Fix{Kind: "text", Who: "you", Code: "In the Transform step (Partition by), partition these tables by their key columns (buckets or the leading part of the key), or leave them unpartitioned."}})
	}
	if len(atRisk) > 0 {
		out = append(out, checks.Check{Code: "iceberg_partition_key", Title: "Partitioning", Status: checks.Fail, Blocking: true, Category: checks.CatDestination,
			Message: "These tables exist partitioned by a column outside their row identity, so upserts leave old rows behind and the tables may already hold duplicate rows: " + strings.Join(atRisk, "; ") + ".",
			Fix: &checks.Fix{Kind: "text", Who: "you", Code: "Drop and recreate them (Positions → Start over, dropping destinations). Outside BeziSync, a data engineer can instead give the table identifier fields that cover its partition columns and remove the duplicates " +
				"(SELECT <key>, count(*) FROM <table> GROUP BY <key> HAVING count(*) > 1)."}})
	}
	if len(unread) > 0 {
		out = append(out, checks.Check{Code: "iceberg_partition_key", Title: "Partitioning", Status: checks.Warn, Category: checks.CatDestination,
			Message: "Couldn't read these tables from the catalog, so their partitioning against their key is unchecked: " + strings.Join(unread, ", ") + ".",
			Fix:     &checks.Fix{Kind: "text", Who: "you", Code: "Check the catalog connection's access to the namespace, then run preflight again."}})
	}
	if len(out) == 0 && checked {
		out = append(out, checks.Check{Code: "iceberg_partition_key", Title: "Partitioning", Status: checks.Pass, Category: checks.CatDestination,
			Message: "Every upserted table is partitioned by its key columns (or not at all), so updates and deletes reach the rows they replace."})
	}
	return out
}

// livePartitionRisk is why an existing table's own partitioning loses deletes ("" when it
// doesn't): upserted (it has identifier fields, and the pipeline doesn't append), and partitioned
// by a column outside them.
func livePartitionRisk(tbl *table.Table, facts PipelineFacts) string {
	if !facts.FlinkCDC && Str(facts.Options, "write_mode") == "append" {
		return ""
	}
	schema := tbl.Schema()
	ids := schema.IdentifierFieldIDs
	if len(ids) == 0 {
		return ""
	}
	spec := tbl.Spec()
	outside := false
	for _, f := range spec.Fields() {
		outside = outside || !slices.Contains(ids, f.SourceID())
	}
	if !outside {
		return ""
	}
	names := make([]string, 0, len(ids))
	for _, id := range ids {
		n, _ := schema.FindColumnName(id)
		names = append(names, n)
	}
	return fmt.Sprintf("(partitioned by %s; identity %s)", strings.Join(TablePartitioning(tbl), ", "), strings.Join(names, ", "))
}

// icebergKeyColumnChecks is iceberg_key_columns (Debezium upserts): the build makes a column an
// identifier field only when the event's key and value both carry it by that name. A key column
// renamed or left out of the value drops out of the table's identity: with some left, upserts
// delete every row sharing the rest (rows lost), with none the table is appended.
func icebergKeyColumnChecks(spec domain.PipelineSpec, facts PipelineFacts) []checks.Check {
	if facts.SourceType == "mongodb" {
		return nil
	}
	var narrowed, dropped []string
	for _, t := range facts.Tables {
		key := debeziumKey(spec, t)
		written := writtenColumns(spec, t, "", "debezium")
		var gone []string
		for _, k := range key {
			if _, ok := written[k]; !ok {
				gone = append(gone, k)
			}
		}
		switch {
		case len(gone) == 0:
		case len(gone) < len(key):
			narrowed = append(narrowed, fmt.Sprintf("%s (%s)", t.Identifier(), strings.Join(gone, ", ")))
		default:
			dropped = append(dropped, fmt.Sprintf("%s (%s)", t.Identifier(), strings.Join(gone, ", ")))
		}
	}
	fix := &checks.Fix{Kind: "text", Who: "you", Code: "Keep key columns under their own names: remove them from the rename steps and the left-out columns, or choose other key columns in the Transform step."}
	var out []checks.Check
	if len(narrowed) > 0 {
		out = append(out, checks.Check{Code: "iceberg_key_columns", Title: "Row identity", Status: checks.Fail, Blocking: true, Category: checks.CatDestination,
			Message: "Key columns renamed or left out drop out of the table's identity, so upserts would replace every row that shares the rest of the key: " + strings.Join(narrowed, "; ") + ".", Fix: fix})
	}
	if len(dropped) > 0 {
		out = append(out, checks.Check{Code: "iceberg_key_columns", Title: "Row identity", Status: checks.Warn, Category: checks.CatDestination,
			Message: "Every key column is renamed or left out, so these tables get every change appended instead of upserted: " + strings.Join(dropped, "; ") + ".", Fix: fix})
	}
	return out
}
