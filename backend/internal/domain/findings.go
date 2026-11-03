package domain

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
)

// What a spec's steps mean for the tables they run on, from the tables' columns: the transform
// preflight checks and, after the controller re-reads the schema, the drift warnings.

// KnownTransformClasses are the SMT classes Debezium Server 3.7.0.Final ships (lib/ and
// lib_opt/); a custom step with another class needs an image that adds it.
var KnownTransformClasses = []string{
	"io.debezium.connector.mongodb.transforms.ExtractNewDocumentState",
	"io.debezium.connector.postgresql.transforms.DecodeLogicalDecodingMessageContent",
	"io.debezium.transforms.ByLogicalTableRouter",
	"io.debezium.transforms.ContentBasedRouter",
	"io.debezium.transforms.EnforceRecordSize",
	"io.debezium.transforms.ExtractChangedRecordState",
	"io.debezium.transforms.ExtractNewRecordState",
	"io.debezium.transforms.ExtractSchemaToNewRecord",
	"io.debezium.transforms.Filter",
	"io.debezium.transforms.GeometryFormatTransformer",
	"io.debezium.transforms.HeaderToValue",
	"io.debezium.transforms.SchemaChangeEventFilter",
	"io.debezium.transforms.SwapGeometryCoordinates",
	"io.debezium.transforms.TimezoneConverter",
	"io.debezium.transforms.ToLogicalTopicRouter",
	"io.debezium.transforms.outbox.EventRouter",
	"io.debezium.transforms.partitions.PartitionRouting",
	"org.apache.kafka.connect.transforms.Cast$Key",
	"org.apache.kafka.connect.transforms.Cast$Value",
	"org.apache.kafka.connect.transforms.DropHeaders",
	"org.apache.kafka.connect.transforms.ExtractField$Key",
	"org.apache.kafka.connect.transforms.ExtractField$Value",
	"org.apache.kafka.connect.transforms.Filter",
	"org.apache.kafka.connect.transforms.Flatten$Key",
	"org.apache.kafka.connect.transforms.Flatten$Value",
	"org.apache.kafka.connect.transforms.HeaderFrom$Key",
	"org.apache.kafka.connect.transforms.HeaderFrom$Value",
	"org.apache.kafka.connect.transforms.HoistField$Key",
	"org.apache.kafka.connect.transforms.HoistField$Value",
	"org.apache.kafka.connect.transforms.InsertField$Key",
	"org.apache.kafka.connect.transforms.InsertField$Value",
	"org.apache.kafka.connect.transforms.InsertHeader",
	"org.apache.kafka.connect.transforms.MaskField$Key",
	"org.apache.kafka.connect.transforms.MaskField$Value",
	"org.apache.kafka.connect.transforms.RegexRouter",
	"org.apache.kafka.connect.transforms.ReplaceField$Key",
	"org.apache.kafka.connect.transforms.ReplaceField$Value",
	"org.apache.kafka.connect.transforms.SetSchemaMetadata$Key",
	"org.apache.kafka.connect.transforms.SetSchemaMetadata$Value",
	"org.apache.kafka.connect.transforms.TimestampConverter$Key",
	"org.apache.kafka.connect.transforms.TimestampConverter$Value",
	"org.apache.kafka.connect.transforms.TimestampRouter",
	"org.apache.kafka.connect.transforms.ValueToKey",
}

// TableColumns is what BeziSync knows about one captured table.
type TableColumns struct {
	Columns         []PreviewColumn
	PrimaryKey      []string
	ReplicaIdentity *string
	KeyKnown        bool // PrimaryKey was read (the controller's schema check does not read it)
}

// TransformFinding is one problem of a step on a table.
type TransformFinding struct {
	Code    string `json:"code"`
	Fail    bool   `json:"fail"` // would stop the connector
	Table   string `json:"table"`
	Step    int    `json:"step"` // -1 for spec.keys
	Message string `json:"message"`
}

// TransformFindings checks the steps against the tables' columns. Tables without known
// columns are skipped.
func TransformFindings(sourceType, pipeline string, spec PipelineSpec, tables map[string]TableColumns) []TransformFinding {
	var out []TransformFinding
	names := make([]string, 0, len(tables))
	for t := range tables {
		if contains(spec.Source.Tables.Include, t) {
			names = append(names, t)
		}
	}
	sort.Strings(names)
	for _, table := range names {
		info := tables[table]
		if len(info.Columns) == 0 {
			continue
		}
		res := Preview(PreviewInput{SourceType: sourceType, Pipeline: pipeline, Spec: spec, Table: table, Operation: "insert",
			Columns: info.Columns, PrimaryKey: info.PrimaryKey, ReplicaIdentity: info.ReplicaIdentity})
		for _, s := range res.Steps {
			t := spec.Transforms[s.Index]
			switch {
			case s.Status == "error":
				out = append(out, TransformFinding{Code: "transform_field_conflict", Fail: true, Table: table, Step: s.Index,
					Message: fmt.Sprintf("%s, %s: %s", table, StepLabel(s.Index, t), s.Note)})
			case t.Type == "rename" && strings.HasPrefix(s.Note, "No field "):
				out = append(out, TransformFinding{Code: "transform_column_missing", Table: table, Step: s.Index,
					Message: fmt.Sprintf("%s, %s: %s", table, StepLabel(s.Index, t), s.Note)})
			}
		}
		rules := spec.Source.Columns[table]
		for i, t := range spec.Transforms {
			if !t.Typed() || (t.Tables != nil && !slices.Contains(t.Tables, table)) {
				continue
			}
			for _, r := range t.Rules {
				col, found := columnOf(info.Columns, r.Field)
				switch {
				case !found || slices.Contains(rules.Exclude, r.Field):
					out = append(out, TransformFinding{Code: "transform_column_missing", Table: table, Step: i,
						Message: fmt.Sprintf("%s, %s checks %s, which this table does not send; the rule sees an empty value.",
							table, StepLabel(i, t), r.Field)})
				case slices.Contains(rules.Mask, r.Field):
					out = append(out, TransformFinding{Code: "transform_rule_type", Table: table, Step: i,
						Message: fmt.Sprintf("%s, %s checks %s, which is masked: the rule compares the hashed value.", table, StepLabel(i, t), r.Field)})
				default:
					if msg := ruleTypeProblem(sourceType, spec.Values, col, r); msg != "" {
						out = append(out, TransformFinding{Code: "transform_rule_type", Table: table, Step: i,
							Message: fmt.Sprintf("%s, %s: %s", table, StepLabel(i, t), msg)})
					}
				}
			}
		}
		if keyCols, ok := spec.Keys[table]; ok {
			var missing []string
			for _, c := range keyCols {
				if _, found := columnOf(info.Columns, c); !found {
					missing = append(missing, c)
				}
			}
			if len(missing) > 0 {
				fallback := "falls back to the primary key"
				switch {
				case len(missing) < len(keyCols):
					fallback = "uses only the other key columns"
				case !info.KeyKnown:
					fallback = "falls back to the primary key, if the table has one"
				case len(info.PrimaryKey) == 0:
					fallback = "has no key at all"
				}
				out = append(out, TransformFinding{Code: "transform_key_column_missing", Table: table, Step: -1,
					Message: fmt.Sprintf("%s has no column %s, so its message key %s.", table, strings.Join(missing, ", "), fallback)})
			}
		}
	}
	return out
}

func columnOf(cols []PreviewColumn, name string) (PreviewColumn, bool) {
	for _, c := range cols {
		if c.Name == name {
			return c, true
		}
	}
	return PreviewColumn{}, false
}

// ruleTypeProblem explains a rule that can never match the column's values.
func ruleTypeProblem(sourceType string, formats ValueFormats, col PreviewColumn, r FilterRule) string {
	kind := ColumnKind(sourceType, col.Type)
	numeric := kind == "int" || kind == "decimal" || kind == "float"
	if formats.Timestamps != "isostring" && (kind == "timestamp" || kind == "date" || kind == "time") {
		numeric = true // epoch numbers
	}
	literals := r.Values
	if r.Value != nil {
		literals = []any{r.Value}
	}
	switch r.Op {
	case "gt", "gte", "lt", "lte":
		if !numeric {
			return fmt.Sprintf("%s is %s, but %s compares numbers; it never matches.", col.Name, col.Type, r.Op)
		}
	case "is_true", "is_false":
		if kind != "bool" {
			return fmt.Sprintf("%s is %s, not a boolean; %s never matches.", col.Name, col.Type, strings.ReplaceAll(r.Op, "_", " "))
		}
	case "eq", "ne", "in", "not_in":
		for _, l := range literals {
			switch l.(type) {
			case bool:
				if kind != "bool" {
					return fmt.Sprintf("%s is %s, so comparing it with true or false never matches.", col.Name, col.Type)
				}
			case string:
				if numeric && kind != "decimal" {
					return fmt.Sprintf("%s is %s; compare it with numbers, not text.", col.Name, col.Type)
				}
				if kind == "decimal" && formats.Decimals != "string" {
					return fmt.Sprintf("%s is %s; compare it with numbers, not text.", col.Name, col.Type)
				}
			default:
				if !numeric && kind != "text" {
					return fmt.Sprintf("%s is %s, so comparing it with a number never matches.", col.Name, col.Type)
				}
			}
		}
	}
	return ""
}

// TransformChecks turns findings and the steps' own properties into preflight checks.
func TransformChecks(spec PipelineSpec, findings []TransformFinding) []checks.Check {
	var out []checks.Check
	group := map[string][]string{}
	order := []string{}
	for _, f := range findings {
		if _, ok := group[f.Code]; !ok {
			order = append(order, f.Code)
		}
		group[f.Code] = append(group[f.Code], f.Message)
	}
	titles := map[string]string{
		"transform_field_conflict":     "Transforms: field names",
		"transform_column_missing":     "Transforms: columns",
		"transform_rule_type":          "Transforms: filter rules",
		"transform_key_column_missing": "Transforms: message keys",
	}
	for _, code := range order {
		msgs := group[code]
		c := checks.Check{Code: code, Title: titles[code], Category: checks.CatDestination, Status: checks.Warn, Message: strings.Join(first(msgs, 5), " ")}
		if len(msgs) > 5 {
			c.Message += fmt.Sprintf(" (and %d more)", len(msgs)-5)
		}
		if code == "transform_field_conflict" {
			c.Status, c.Blocking = checks.Fail, true
			c.Why = "Two fields with one name make Debezium Server stop with \"Cannot create field because of field name duplication\"."
			c.Fix = &checks.Fix{Kind: "text", Who: "You", Code: "Pick a field name the events do not have yet, under Transform."}
		}
		out = append(out, c)
	}
	var unknown []string
	for i, t := range spec.Transforms {
		if t.Type == "custom" && !slices.Contains(KnownTransformClasses, t.Class) {
			unknown = append(unknown, fmt.Sprintf("%s (%s)", StepLabel(i, t), t.Class))
		}
	}
	if len(unknown) > 0 {
		out = append(out, checks.Check{Code: "transform_custom_class_unknown", Title: "Transforms: custom classes", Category: checks.CatDestination,
			Status: checks.Warn, Message: "Debezium Server 3.7 does not ship " + strings.Join(unknown, ", ") +
				". The pipeline's image must add it, or the connector stops when it starts.",
			Why: "BeziSync cannot look inside images; it knows the classes the official image ships."})
	}
	return out
}

func first(l []string, n int) []string {
	if len(l) > n {
		return l[:n]
	}
	return l
}
