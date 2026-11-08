package domain

import (
	"fmt"
	"slices"
	"strings"

	"github.com/daun-gatal/bezisync/backend/internal/expr"
)

// Flink CDC primary keys (ADR 0010). spec.keys names a table's key columns: a source column by its
// source name (Flink CDC writes it under its renamed name), or a column a step adds to the table (an
// added field or a computed column) by its own name. Debezium keys are source columns only: it
// builds the message key in the source connector, before any step runs.

// Key column origins.
const (
	KeyFromColumn   = "column"
	KeyFromAdded    = "added"
	KeyFromComputed = "computed"
)

// FlinkKeyColumn is one chosen key column as the table writes it.
type FlinkKeyColumn struct {
	Name    string // as chosen (spec.keys)
	Written string // the destination column
	From    string // KeyFromColumn, KeyFromAdded or KeyFromComputed
	Step    int    // the step adding it (-1 for a source column)
	Source  string // the source column (for KeyFromColumn)
}

// FlinkKeyError is a chosen key column the table doesn't write.
type FlinkKeyError struct {
	Table, Column string
	LeftOut       bool // a source column the pipeline leaves out
}

func (e *FlinkKeyError) Error() string {
	if e.LeftOut {
		return fmt.Sprintf("%s: the key column %s is left out of the table. Keep it, or pick another key column under Transform.", e.Table, e.Column)
	}
	return fmt.Sprintf("%s writes no column %s to key its rows by: pick the key columns again under Transform.", e.Table, e.Column)
}

// FlinkRenames is the table's renames as Flink CDC's projection reads them: one map of every
// rename step that applies (a later rename of the same column wins).
func FlinkRenames(spec PipelineSpec, table string) map[string]string {
	out := map[string]string{}
	for _, t := range spec.Transforms {
		if t.Type != "rename" || (t.Tables != nil && !slices.Contains(t.Tables, table)) {
			continue
		}
		for _, r := range t.Renames {
			out[r.From] = r.To
		}
	}
	return out
}

// StepColumns are the columns steps add to a table on Flink CDC, name → step index: added fields
// that apply to it and its computed columns (the last step of a name wins; two steps of one name
// are refused by the compile).
func StepColumns(spec PipelineSpec, table string) map[string]int {
	out := map[string]int{}
	for i, t := range spec.Transforms {
		switch {
		case t.Type == "add_field" && t.Name != "" && (t.Tables == nil || slices.Contains(t.Tables, table)):
			out[t.Name] = i
		case t.Type == "computed" && t.Table == table && t.Name != "":
			out[t.Name] = i
		}
	}
	return out
}

// FlinkKey resolves the table's chosen key columns, in their order. A name is a source column the
// table writes first (written under its renamed name), else the new name a rename gives a source
// column it writes (the compile before ADR 0010 wrote that name as it is), else a column a step
// adds. With columns nil (not read) every name is taken as it is, through the renames: the compile
// before ADR 0010.
func FlinkKey(spec PipelineSpec, table string, columns []string) ([]FlinkKeyColumn, error) {
	chosen := spec.Keys[table]
	if len(chosen) == 0 {
		return nil, nil
	}
	renames := FlinkRenames(spec, table)
	added := StepColumns(spec, table)
	excluded := spec.Source.Columns[table].Exclude
	out := make([]FlinkKeyColumn, 0, len(chosen))
	for _, k := range chosen {
		isSource := columns == nil || slices.Contains(columns, k)
		left := slices.Contains(excluded, k)
		step, isStep := added[k]
		renamedFrom := ""
		if columns != nil {
			for from, to := range renames {
				if to == k && slices.Contains(columns, from) && !slices.Contains(excluded, from) && renames[k] == "" {
					renamedFrom = from
				}
			}
		}
		switch {
		case isSource && !left && !(columns == nil && isStep && renames[k] == ""):
			w := k
			if to, ok := renames[k]; ok {
				w = to
			}
			out = append(out, FlinkKeyColumn{Name: k, Written: w, From: KeyFromColumn, Step: -1, Source: k})
		case renamedFrom != "" && !(isSource && !left):
			out = append(out, FlinkKeyColumn{Name: k, Written: k, From: KeyFromColumn, Step: -1, Source: renamedFrom})
		case isStep:
			from := KeyFromAdded
			if spec.Transforms[step].Type == "computed" {
				from = KeyFromComputed
			}
			out = append(out, FlinkKeyColumn{Name: k, Written: k, From: from, Step: step})
		default:
			return nil, &FlinkKeyError{Table: table, Column: k, LeftOut: left}
		}
	}
	return out, nil
}

// KeyKinds are the kinds a column a step adds can have in a Flink CDC primary key: BeziSync makes
// it NOT NULL with IFNULL and a fallback of that kind (ADR 0010).
var KeyKinds = []expr.Kind{expr.Text, expr.Int}

// keyNotNull says which of a table's columns are NOT NULL: the source says so, or they are in its
// primary key.
func keyNotNull(info TableColumns) func(string) bool {
	return func(c string) bool {
		if slices.Contains(info.PrimaryKey, c) {
			return true
		}
		col, ok := columnOf(info.Columns, c)
		return ok && !col.Nullable // callers set Nullable when they don't know (fail closed)
	}
}

// FlinkKeyProblems are the reasons a table's chosen key can't key its rows on Flink CDC: a source
// column cut short (rows that start alike would replace each other), a computed column that can't
// be a key (ComputedKeyProblem), and, on a table with a primary key, a key with a column a step adds
// that leaves out part of the primary key (rows would share a key: a constant, a month). A key
// that doesn't resolve is FlinkKey's error, not repeated here.
func FlinkKeyProblems(sourceType string, spec PipelineSpec, table string, info TableColumns) []string {
	names := make([]string, len(info.Columns))
	for i, c := range info.Columns {
		names[i] = c.Name
	}
	key, err := FlinkKey(spec, table, names)
	if err != nil {
		return nil
	}
	var out []string
	var sources []string
	steps := false
	for _, k := range key {
		switch k.From {
		case KeyFromColumn:
			sources = append(sources, k.Source)
			for _, t := range spec.Transforms {
				for _, c := range t.Truncate {
					if t.Type == "truncate" && c.Table == table && c.Column == k.Source {
						out = append(out, fmt.Sprintf("%s: the key column %s is cut to %d characters, so rows that start alike would replace each other. Don't cut a key column.", table, k.Written, c.Length))
					}
				}
			}
		case KeyFromComputed:
			steps = true
			if why := ComputedKeyProblem(sourceType, spec, table, spec.Transforms[k.Step], info); why != "" {
				out = append(out, fmt.Sprintf("%s: the key column %s: %s", table, k.Written, why))
			}
		case KeyFromAdded:
			steps = true
		}
	}
	if steps && len(info.PrimaryKey) > 0 {
		var missing []string
		for _, c := range info.PrimaryKey {
			if !slices.Contains(sources, c) {
				missing = append(missing, c)
			}
		}
		if len(missing) > 0 {
			out = append(out, fmt.Sprintf("%s: a key with a column a step adds must keep the whole primary key (%s), or rows would share a key and replace each other.", table, strings.Join(missing, ", ")))
		}
	}
	return out
}

// ComputedKeyProblem is why a computed column can't be in a Flink CDC primary key ("" when it can):
// its kind, as the compile reads it without the table's columns, must be text or a whole number
// (KeyColumnSQL writes it NOT NULL with a fallback of that kind); it must be the same each time a
// row is written; every column it reads must be NOT NULL (in the source, or in the primary key) and
// no form in it may give an empty value (expr.NeverNull). A fallback for a column that can be empty
// is not enough: those rows would share one key.
func ComputedKeyProblem(sourceType string, spec PipelineSpec, table string, t Transform, info TableColumns) string {
	n := ComputedTree(t)
	if t.Expression != "" {
		var err error
		if n, err = ExpressionOf(t); err != nil {
			return "its expression can't be read."
		}
	}
	compiled, err := expr.Check(n, expr.Env{}, expr.UseColumn)
	if err != nil {
		return "its formula has an error."
	}
	res, err := expr.Check(n, ExpressionEnv(sourceType, info, spec, table), expr.UseColumn)
	if err != nil {
		return "its formula has an error."
	}
	notNull := keyNotNull(info)
	switch {
	case !res.Deterministic:
		return "it changes each time a row is written, so every change would be a new row."
	case !slices.Contains(KeyKinds, compiled.Kind):
		if compiled.Kind == expr.Unknown {
			return "its kind follows the columns it reads, which BeziSync can't write NOT NULL: a computed key column is text or a whole number (CAST it AS STRING)."
		}
		if compiled.Kind == expr.Date || compiled.Kind == expr.Timestamp || compiled.Kind == expr.TimestampTZ {
			return "it is " + expr.KindWords(compiled.Kind) + "; a computed key column is text or a whole number. To partition by a date, put the timestamp column itself in the key and partition it by its day under Partition by."
		}
		return "it is " + expr.KindWords(compiled.Kind) + "; a computed key column is text or a whole number."
	case !slices.ContainsFunc(res.Columns, func(c string) bool { return !notNull(c) }) && expr.NeverNull(n, notNull):
		return ""
	}
	return "it can be empty: compute it from columns that are never empty (NOT NULL or in the primary key), without a division, a conversion that can fail or Date from text."
}

// KeyColumnSQL is the Flink CDC projection of a column a step adds that is in the table's primary
// key: IFNULL(…, a fallback of its kind). Flink CDC 3.7.0 types every computed column as nullable
// except an outer IFNULL, and a destination such as Iceberg refuses a nullable key column. The
// fallback is never written: preflight (FlinkKeyProblems) allows only values that can't be empty.
// The kind is read without the table's columns, so a key column whose kind depends on them (a
// rounded number, the first of some columns) is refused.
func KeyColumnSQL(t Transform, sql string) (string, error) {
	if t.Type == "add_field" {
		return "IFNULL(" + sql + ", '')", nil
	}
	n := ComputedTree(t)
	if t.Expression != "" {
		var err error
		if n, err = ExpressionOf(t); err != nil {
			return "", err
		}
	}
	res, err := expr.Check(n, expr.Env{}, expr.UseColumn)
	if err != nil {
		return "", err
	}
	switch res.Kind {
	case expr.Text:
		return "IFNULL(" + sql + ", '')", nil
	case expr.Int:
		return "IFNULL(" + sql + ", 0)", nil
	}
	return "", fmt.Errorf("the key column %s is %s; a computed key column is text or a whole number", t.Name, expr.KindWords(res.Kind))
}

// FlinkKeyWarnings are a table's chosen source key columns that can be empty (NULL in the source
// and not in its primary key), when the key doesn't also hold the whole primary key. On Flink CDC
// 3.7.0 into Iceberg such a row is written with an empty text for the key, so those rows would share
// a key and replace each other (ADR 0010, the probe); with the primary key in the key they can't.
// Kept a warning: keys of source columns were always the user's choice.
func FlinkKeyWarnings(spec PipelineSpec, table string, info TableColumns) []string {
	names := make([]string, len(info.Columns))
	for i, c := range info.Columns {
		names[i] = c.Name
	}
	key, err := FlinkKey(spec, table, names)
	if err != nil {
		return nil
	}
	if keepsPrimaryKey(key, info.PrimaryKey) {
		return nil
	}
	notNull := keyNotNull(info)
	var out []string
	for _, k := range key {
		if k.From == KeyFromColumn && !notNull(k.Source) {
			out = append(out, fmt.Sprintf("%s: the key column %s can be empty in the source: rows where it is empty would share one key and replace each other.", table, k.Written))
		}
	}
	return out
}

// keepsPrimaryKey reports a resolved key that holds every primary-key column (a table without one
// never does): its rows can't share a key whatever its other columns hold.
func keepsPrimaryKey(key []FlinkKeyColumn, primaryKey []string) bool {
	if len(primaryKey) == 0 {
		return false
	}
	for _, c := range primaryKey {
		if !slices.ContainsFunc(key, func(k FlinkKeyColumn) bool { return k.From == KeyFromColumn && k.Source == c }) {
			return false
		}
	}
	return true
}
