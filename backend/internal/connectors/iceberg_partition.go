package connectors

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

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

// partitionChecks: each partition column is one the table sends, of a kind its transform takes
// (T17, blocking: Flink CDC would fail creating the table), and a destination table that
// already exists is partitioned as asked (T18: Iceberg keeps a table's partitioning).
func partitionChecks(ctx context.Context, cat interface {
	LoadTable(context.Context, table.Identifier) (*table.Table, error)
}, cfg icebergConfig, c CompileContext, facts PipelineFacts) []checks.Check {
	byTable, err := IcebergPartitionBy(c.Spec, facts.Options)
	if err != nil || len(byTable) == 0 {
		return nil
	}
	var bad []string
	for _, t := range facts.Tables {
		fields, ok := byTable[t.Identifier()]
		if !ok {
			continue
		}
		out := outputColumns(c.Spec, t, facts.SourceType)
		for _, f := range fields {
			kind, found := out[f.Column]
			switch {
			case !found:
				bad = append(bad, fmt.Sprintf("%s has no column %s", t.Identifier(), f.Column))
			case (f.Transform == "year" || f.Transform == "month" || f.Transform == "day") && kind != "timestamp" && kind != "timestamptz" && kind != "date":
				bad = append(bad, fmt.Sprintf("%s(%s) needs a date or a timestamp", f.Transform, f.Column))
			case f.Transform == "hour" && kind != "timestamp" && kind != "timestamptz":
				bad = append(bad, fmt.Sprintf("hour(%s) needs a timestamp", f.Column))
			case f.Transform == "truncate" && kind != "text" && kind != "int" && kind != "decimal":
				bad = append(bad, fmt.Sprintf("truncate(%s) needs text, a whole number or a decimal", f.Column))
			}
		}
	}
	var out []checks.Check
	if facts.Tables != nil {
		if len(bad) > 0 {
			out = append(out, checks.Check{Code: "iceberg_partition_column", Title: "Partitioning", Status: checks.Fail, Blocking: true, Category: checks.CatDestination,
				Message: "Partitioning that can't be applied: " + strings.Join(bad, "; ") + ".",
				Fix:     &checks.Fix{Kind: "text", Who: "you", Code: "Change Partition by in the Destination step."}})
		} else {
			out = append(out, checks.Check{Code: "iceberg_partition_column", Title: "Partitioning", Status: checks.Pass, Category: checks.CatDestination,
				Message: "Every partition column is one the tables send, of a kind its transform takes."})
		}
	}
	if cat == nil {
		return out
	}
	var differ []string
	for _, t := range keysSorted(byTable) {
		dest, ok := facts.Destinations[t]
		if !ok {
			continue
		}
		tbl, err := cat.LoadTable(ctx, icebergTableIdent(cfg, dest))
		if err != nil {
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
	if len(differ) > 0 {
		out = append(out, checks.Check{Code: "iceberg_partition_existing", Title: "Partitioning", Status: checks.Fail, Blocking: true, Category: checks.CatDestination,
			Message: "These tables already exist partitioned otherwise, and Iceberg keeps a table's partitioning: " + strings.Join(differ, "; ") + ".",
			Fix:     &checks.Fix{Kind: "text", Who: "you", Code: "Partition by what the table has, or drop the table (Positions → Start over, dropping destinations) so the pipeline creates it again."}})
	}
	return out
}

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

// outputColumns are the columns a table's rows carry into the destination on Flink CDC, with
// their kinds: the source's, minus those left out, renamed, plus
// added and computed ones.
func outputColumns(spec domain.PipelineSpec, t TableInfo, sourceType string) map[string]string {
	id := t.Identifier()
	rules := spec.Source.Columns[id]
	out := map[string]string{}
	renamed := map[string]string{}
	for _, step := range spec.Transforms {
		if step.Tables != nil && !slices.Contains(step.Tables, id) {
			continue
		}
		for _, r := range step.Renames {
			renamed[r.From] = r.To
		}
	}
	for _, col := range t.Columns {
		if slices.Contains(rules.Exclude, col.Name) {
			continue
		}
		name := col.Name
		if to, ok := renamed[name]; ok {
			name = to
		}
		out[name] = domain.ColumnKind(sourceType, col.Type)
	}
	for _, step := range spec.Transforms {
		switch {
		case step.Type == "add_field" && (step.Tables == nil || slices.Contains(step.Tables, id)):
			out[step.Name] = "text"
		case step.Type == "computed" && step.Table == id:
			out[step.Name] = domain.ComputedFunctions[step.Function].Out
		case step.Type == "soft_delete":
			out[domain.SoftDeleteField["flink-cdc"]] = "text"
		}
	}
	return out
}
