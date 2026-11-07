package connectors

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
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
	if facts.Tables == nil {
		out = append(out, checks.Check{Code: "iceberg_partition_column", Title: "Partitioning", Status: checks.Warn, Category: checks.CatDestination,
			Message: "Couldn't check the partition columns: the source's tables couldn't be read.",
			Fix:     &checks.Fix{Kind: "text", Who: "you", Code: "Run preflight again once the source is reachable."}})
	} else {
		var bad []string
		for _, t := range facts.Tables {
			fields, ok := byTable[t.Identifier()]
			if !ok {
				continue
			}
			cols := writtenColumns(c.Spec, t, facts.SourceType, engine)
			for _, f := range fields {
				if msg := partitionFieldProblem(t.Identifier(), f, cols); msg != "" {
					bad = append(bad, msg)
				}
			}
		}
		if len(bad) > 0 {
			out = append(out, checks.Check{Code: "iceberg_partition_column", Title: "Partitioning", Status: checks.Fail, Blocking: true, Category: checks.CatDestination,
				Message: "Partitioning that can't be applied: " + strings.Join(bad, "; ") + ".",
				Fix:     &checks.Fix{Kind: "text", Who: "you", Code: "Change Partition by in the Destination step."}})
		} else {
			out = append(out, checks.Check{Code: "iceberg_partition_column", Title: "Partitioning", Status: checks.Pass, Category: checks.CatDestination,
				Message: "Every partition column is one the tables write, of a kind its transform takes."})
		}
	}
	if cat == nil {
		return out
	}
	var differ, unread []string
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
	}
	return out
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
		return fmt.Sprintf("%s is a document or a list: no transform takes it", f.Column)
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

var kindLabels = map[string]string{"int": "a whole number", "decimal": "a decimal", "float": "a floating-point number", "bool": "true/false", "text": "text",
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
// (in step order, so a rename of a rename lands on the last name), plus added and computed
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
		name := col.Name
		for _, step := range spec.Transforms {
			if step.Type != "rename" || (step.Tables != nil && !slices.Contains(step.Tables, id)) {
				continue
			}
			for _, r := range step.Renames {
				if r.From == name {
					name = r.To
					break
				}
			}
		}
		out[name] = domain.PartitionKind(engine, sourceType, col.Type, spec.Values)
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
