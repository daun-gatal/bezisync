package domain

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math/big"
	"slices"
	"strings"

	"github.com/daun-gatal/bezisync/backend/internal/om"
)

// The transform preview: a sample change event built from a table's discovered columns, run
// through the spec's steps the way Debezium Server 3.7 runs the compiled chain (the e2e parity
// spec compares both). Steps BeziSync cannot simulate (raw scripts, custom classes, outbox)
// make everything after them Unknown: the preview never guesses.

// PreviewColumn is a discovered column.
type PreviewColumn struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
	PII      bool   `json:"pii"`
	// HasDefault: the column has a default in the source (connectors.Column). Not sent.
	HasDefault bool `json:"-"`
}

// PreviewInput is what to preview.
type PreviewInput struct {
	SourceType      string
	Pipeline        string // pipeline name ({pipeline} in naming, add_field "pipeline")
	Prefix          string // Debezium topic prefix
	Spec            PipelineSpec
	Table           string // schema.table
	Operation       string // insert | update | delete | snapshot
	Columns         []PreviewColumn
	PrimaryKey      []string
	ReplicaIdentity *string // PostgreSQL: default | full | nothing | index
}

// PreviewEvent is a key and value as the destination would receive them (JSON form).
type PreviewEvent struct {
	Key   any `json:"key"`
	Value any `json:"value"`
}

// PreviewStep is the event after one step.
type PreviewStep struct {
	Index  int           `json:"index"`
	ID     string        `json:"id"`
	Type   string        `json:"type"`
	Status string        `json:"status"` // applied | skipped | dropped | unknown | error
	Note   string        `json:"note"`
	Event  *PreviewEvent `json:"event"`
}

// PreviewResult is the whole preview.
type PreviewResult struct {
	Table       string        `json:"table"`
	Operation   string        `json:"operation"`
	Destination string        `json:"destination"`
	Input       PreviewEvent  `json:"input"`
	Steps       []PreviewStep `json:"steps"`
	Outcome     string        `json:"outcome"` // delivered | dropped | unknown | error
	Output      *PreviewEvent `json:"output"`
	Fields      []string      `json:"fields"` // value fields the destination receives (nil when unknown)
	Notes       []string      `json:"notes"`
}

// decimal is a Java BigDecimal; bytes is a Java byte[] (the JSON converter writes both as
// base64 when they reach the destination as such).
type decimal struct {
	unscaled *big.Int
	scale    int
}
type bytesValue []byte

// row is an ordered record (a Connect Struct).
type row = *om.Map

const (
	sampleMillis = int64(1793351642118) // 2026-10-30T09:14:02.118Z
	sampleISO    = "2026-10-30T09:14:02.118Z"
)

// ColumnKind classifies a discovered column type.
func ColumnKind(sourceType, dbType string) string {
	t := strings.ToLower(strings.TrimSpace(dbType))
	switch {
	case sourceType == "mongodb":
		switch t {
		case "int":
			return "int"
		case "float":
			return "float"
		case "bool":
			return "bool"
		case "dict":
			return "document"
		case "list":
			return "list"
		case "datetime":
			return "timestamp"
		}
		return "text"
	case sourceType == "spanner":
		switch {
		case t == "int64":
			return "int"
		case t == "float64" || t == "float32":
			return "float"
		case t == "numeric":
			return "decimal"
		case t == "bool":
			return "bool"
		case t == "timestamp":
			return "timestamptz" // ISO-8601 text, like DATE (which stays text)
		case t == "bytes":
			return "binary"
		case t == "json":
			return "json"
		case strings.HasPrefix(t, "array"):
			return "list"
		}
		return "text"
	case sourceType == "oracle" && t == "number":
		return "decimal"
	case sourceType == "oracle" && (t == "binary_float" || t == "binary_double"):
		return "float"
	case sourceType == "oracle" && t == "date": // an Oracle DATE has a time of day
		return "timestamp"
	case sourceType == "oracle" && (t == "timestamp with time zone" || t == "timestamp with local time zone"):
		return "timestamptz"
	case sourceType == "oracle" && (t == "raw" || t == "long raw"):
		return "binary"
	case t == "boolean" || t == "bool" || (t == "bit" && sourceType == "sqlserver"):
		return "bool"
	case t == "integer" || t == "int" || t == "bigint" || t == "smallint" || t == "tinyint" || t == "mediumint" || t == "serial" || t == "bigserial":
		return "int"
	case strings.HasPrefix(t, "numeric") || strings.HasPrefix(t, "decimal") || t == "money" || t == "smallmoney":
		return "decimal"
	case t == "real" || t == "double precision" || t == "float" || t == "double":
		return "float"
	case t == "timestamp with time zone" || t == "datetimeoffset" || (t == "timestamp" && (sourceType == "mysql" || sourceType == "vitess")):
		return "timestamptz"
	case strings.HasPrefix(t, "timestamp") || t == "datetime" || t == "datetime2" || t == "smalldatetime":
		return "timestamp"
	case t == "date":
		return "date"
	case strings.HasPrefix(t, "time"):
		return "time"
	case t == "bytea" || strings.Contains(t, "blob") || t == "binary" || t == "varbinary" || t == "image":
		return "binary"
	case t == "json" || t == "jsonb":
		return "json"
	case t == "uuid" || t == "uniqueidentifier":
		return "uuid"
	}
	return "text"
}

// sampleValue is a column's value as Debezium puts it in the change event (before the
// converter), for the spec's value formats.
func sampleValue(sourceType string, c PreviewColumn, formats ValueFormats, pk bool) any {
	name := strings.ToLower(c.Name)
	switch ColumnKind(sourceType, c.Type) {
	case "int":
		switch {
		case pk || name == "id":
			return int64(1042)
		case strings.HasSuffix(name, "_id") || strings.HasSuffix(name, "id"):
			return int64(88)
		}
		return int64(7)
	case "decimal":
		unscaled := big.NewInt(1250)
		if strings.Contains(name, "total") || strings.Contains(name, "amount") || strings.Contains(name, "price") {
			unscaled = big.NewInt(12990)
		}
		if sourceType == "cockroachdb" || sourceType == "vitess" || sourceType == "spanner" { // DECIMAL always arrives as text
			return new(big.Rat).SetFrac(unscaled, big.NewInt(100)).FloatString(2)
		}
		switch formats.Decimals {
		case "double":
			f, _ := new(big.Rat).SetFrac(unscaled, big.NewInt(100)).Float64()
			return f
		case "string":
			return new(big.Rat).SetFrac(unscaled, big.NewInt(100)).FloatString(2)
		}
		return decimal{unscaled: unscaled, scale: 2}
	case "float":
		return 3.75
	case "bool":
		return strings.HasPrefix(name, "is_") || strings.HasPrefix(name, "has_")
	case "timestamp":
		iso := formats.Timestamps == "isostring"
		switch sourceType {
		case "mysql", "vitess":
			if iso {
				return "2026-10-30T09:14:02Z"
			}
			return sampleMillis - 118
		case "sqlserver":
			return sampleMillis * 1_000_000
		case "mongodb":
			return sampleMillis
		case "oracle":
			if strings.EqualFold(strings.TrimSpace(c.Type), "date") {
				return sampleMillis - 118 // DATE has whole seconds, sent as epoch milliseconds
			}
		}
		if iso {
			return sampleISO
		}
		return sampleMillis * 1000
	case "timestamptz":
		if formats.Timestamps == "isostring" && sourceType == "postgresql" {
			return "2026-10-30T09:14:02.118000Z"
		}
		return sampleISO
	case "date":
		if formats.Timestamps == "isostring" {
			return "2026-10-30Z"
		}
		return int64(20756)
	case "time":
		if formats.Timestamps == "isostring" {
			return "09:14:02.118Z"
		}
		return int64(33242118000)
	case "binary":
		raw := []byte{0xab, 0xcd, 0xef}
		switch formats.Binary {
		case "base64":
			return base64.StdEncoding.EncodeToString(raw)
		case "hex":
			return hex.EncodeToString(raw)
		}
		return bytesValue(raw)
	case "json":
		return `{"city": "Lyon"}`
	case "uuid":
		if sourceType == "sqlserver" {
			return "0B9F7E2A-6C1D-4F5E-9A3B-2C4D6E8F0A1B"
		}
		return "0b9f7e2a-6c1d-4f5e-9a3b-2c4d6e8f0a1b"
	case "document":
		return om.New().Set("city", "Lyon").Set("zip", "69001")
	case "list":
		return []any{"a", "b"}
	}
	switch {
	case name == "status" || name == "state":
		return "paid"
	case strings.Contains(name, "email"):
		return "ana@example.com"
	case strings.Contains(name, "name"):
		return "Ana Lima"
	case strings.Contains(name, "country"):
		return "FR"
	case strings.Contains(name, "city"):
		return "Lyon"
	case sourceType == "mongodb" && name == "_id":
		return "6720f1a2b3c4d5e6f7a8b9c0"
	}
	return "sample text"
}

// sampleRow builds the row after the change: excluded columns are gone, masked ones hashed.
func sampleRow(in PreviewInput) row {
	rules := in.Spec.Source.Columns[in.Table]
	r := om.New()
	for _, c := range in.Columns {
		if slices.Contains(rules.Exclude, c.Name) {
			continue
		}
		v := sampleValue(in.SourceType, c, in.Spec.Values, slices.Contains(in.PrimaryKey, c.Name))
		if slices.Contains(rules.Mask, c.Name) {
			sum := sha256.Sum256([]byte(fmt.Sprint(v)))
			v = hex.EncodeToString(sum[:])
		}
		r.Set(c.Name, v)
	}
	return r
}

func cloneRow(r row) row {
	if r == nil {
		return nil
	}
	out := om.New()
	for _, k := range r.Keys() {
		v, _ := r.Get(k)
		if m, ok := v.(*om.Map); ok {
			v = cloneRow(m)
		}
		out.Set(k, v)
	}
	return out
}

// beforeRow is the row before an update or delete, as the source reports it.
func beforeRow(in PreviewInput, after row) (row, string) {
	identity := ""
	if in.ReplicaIdentity != nil {
		identity = strings.ToLower(*in.ReplicaIdentity)
	}
	switch {
	case in.SourceType == "mongodb":
		return nil, "MongoDB change events carry no row before the change."
	case in.SourceType == "postgresql" && identity != "full":
		if in.Operation == "update" {
			return nil, "PostgreSQL sends no row before an update unless the table has REPLICA IDENTITY FULL."
		}
		keyOnly := om.New()
		for _, k := range after.Keys() {
			v, _ := after.Get(k)
			if !slices.Contains(in.PrimaryKey, k) {
				v = nil
			}
			keyOnly.Set(k, v)
		}
		return keyOnly, "PostgreSQL sends only the key columns of a deleted row unless the table has REPLICA IDENTITY FULL; rules on other columns see empty values."
	}
	before := cloneRow(after)
	if in.Operation == "update" {
		changed := ""
		for _, k := range before.Keys() {
			if strings.EqualFold(k, "status") {
				changed = k
			}
		}
		if changed != "" {
			before.Set(changed, "pending")
		}
	}
	return before, ""
}

var connectorNames = map[string]string{"postgresql": "postgresql", "mysql": "mysql", "sqlserver": "sqlserver", "mongodb": "mongodb", "oracle": "oracle",
	"cockroachdb": "cockroachdb", "vitess": "vitess", "spanner": "spanner"}

// envelope builds the Debezium change event (field order as Debezium 3.7.0 sends it).
func envelope(in PreviewInput, before, after row) row {
	schema, table, _ := strings.Cut(in.Table, ".")
	snapshot := "false"
	if in.Operation == "snapshot" {
		snapshot = "true"
	}
	source := om.New().Set("version", "3.7.0.Final").Set("connector", connectorNames[in.SourceType]).Set("name", in.Prefix).
		Set("ts_ms", sampleMillis).Set("snapshot", snapshot)
	switch in.SourceType {
	case "postgresql":
		source.Set("db", "app").Set("sequence", `[null,"22497448"]`).Set("ts_us", sampleMillis*1000).Set("ts_ns", sampleMillis*1_000_000).
			Set("schema", schema).Set("table", table).Set("txId", int64(771)).Set("lsn", int64(22497448)).Set("xmin", nil).
			Set("origin", nil).Set("origin_lsn", nil)
	case "mysql":
		source.Set("db", schema).Set("sequence", nil).Set("ts_us", sampleMillis*1000).Set("ts_ns", sampleMillis*1_000_000).
			Set("table", table).Set("server_id", int64(1)).Set("gtid", nil).Set("file", "binlog.000002").Set("pos", int64(1234)).
			Set("row", int64(0)).Set("thread", nil).Set("query", nil)
	case "sqlserver":
		source.Set("db", "app").Set("sequence", nil).Set("ts_us", sampleMillis*1000).Set("ts_ns", sampleMillis*1_000_000).
			Set("schema", schema).Set("table", table).Set("change_lsn", nil).Set("commit_lsn", nil).Set("event_serial_no", nil)
	case "cockroachdb":
		source.Set("db", "app").Set("sequence", nil).Set("ts_us", sampleMillis*1000).Set("ts_ns", sampleMillis*1_000_000).
			Set("schema", schema).Set("table", table).Set("cluster", in.Prefix).Set("resolved_ts", nil).Set("ts_hlc", nil)
	case "spanner":
		source.Set("db", schema).Set("sequence", "0").Set("ts_us", sampleMillis*1000).Set("ts_ns", sampleMillis*1_000_000).
			Set("project_id", "my-project").Set("instance_id", "my-instance").Set("database_id", schema).Set("change_stream_name", "bz_stream").
			Set("table", table).Set("server_transaction_id", "1793521344299110").Set("record_sequence_prefix", nil).Set("low_watermark", nil).
			Set("read_at_timestamp", sampleMillis).Set("number_records_in_transaction", int64(1)).Set("transaction_tag", "").
			Set("system_transaction", false).Set("value_capture_type", "NEW_ROW_AND_OLD_VALUES").Set("partition_token", "sample-partition-token").
			Set("mod_number", int64(0)).Set("is_last_record_in_transaction_in_partition", true).Set("number_of_partitions_in_transaction", int64(1))
	case "vitess":
		source.Set("db", "").Set("sequence", nil).Set("ts_us", sampleMillis*1000).Set("ts_ns", sampleMillis*1_000_000).
			Set("keyspace", schema).Set("table", table).Set("shard", "-80").
			Set("vgtid", `[{"keyspace":"`+schema+`","shard":"-80","gtid":"MySQL56/ee868a2c-d5ce-11f1-b5f4-0242ac140007:1-32","table_p_ks":[]}]`)
	case "oracle":
		source.Set("db", "ORCLPDB1").Set("sequence", nil).Set("ts_us", sampleMillis*1000).Set("ts_ns", sampleMillis*1_000_000).
			Set("schema", schema).Set("table", table).Set("txId", "0a001f0052020000").Set("scn", "2299592").Set("commit_scn", "2299593").
			Set("lcr_position", nil).Set("rs_id", nil).Set("ssn", int64(0)).Set("redo_thread", int64(1)).Set("user_name", schema).
			Set("redo_sql", nil).Set("row_id", "AAAR1fAAMAAAAEbAAA")
	default:
		source.Set("db", schema).Set("sequence", nil).Set("ts_us", sampleMillis*1000).Set("ts_ns", sampleMillis*1_000_000).
			Set("collection", table)
	}
	var b, a any
	if before != nil {
		b = before
	}
	if after != nil {
		a = after
	}
	ts := sampleMillis + 82
	return om.New().Set("before", b).Set("after", a).Set("source", source).Set("transaction", nil).
		Set("op", FilterOperations[in.Operation]).Set("ts_ms", ts).Set("ts_us", ts*1000).Set("ts_ns", ts*1_000_000)
}

// Preview runs the steps on a sample event of one table.
func Preview(in PreviewInput) PreviewResult {
	res := PreviewResult{Table: in.Table, Operation: in.Operation, Steps: []PreviewStep{}, Notes: []string{}}
	schema, table, _ := strings.Cut(in.Table, ".")
	res.Destination = strings.NewReplacer("{schema}", schema, "{table}", table, "{pipeline}", in.Pipeline).Replace(in.Spec.Sink.Naming)
	if len(in.Columns) == 0 {
		res.Notes = append(res.Notes, "BeziSync has not read this table's columns yet, so the sample has no fields.")
	}
	full := sampleRow(in)
	var before, after row
	switch in.Operation {
	case "insert", "snapshot":
		after = full
	case "update":
		after = full
		var note string
		before, note = beforeRow(in, full)
		if note != "" {
			res.Notes = append(res.Notes, note)
		}
	case "delete":
		var note string
		before, note = beforeRow(in, full)
		if note != "" {
			res.Notes = append(res.Notes, note)
		}
	}
	if in.SourceType == "mongodb" {
		// MongoDB events carry the document as JSON text until the event shape step.
		for _, r := range []*row{&before, &after} {
			if *r != nil {
				text, _ := (*r).MarshalJSON()
				doc := om.New().Set("__document", *r).Set("__text", string(text))
				*r = doc
			}
		}
	}
	key := messageKey(in, before, after)
	value := envelope(in, before, after)
	res.Input = PreviewEvent{Key: jsonForm(key), Value: jsonForm(mongoText(value))}
	state := "delivered"
	var current any = value
	// A soft delete makes every unwrap step mark deletes, as the compiler does.
	softDelete := slices.ContainsFunc(in.Spec.Transforms, func(t Transform) bool { return t.Type == "soft_delete" })
	for i, t := range in.Spec.Transforms {
		if softDelete && t.Type == "unwrap" {
			t.DeleteHandling = "rewrite"
		}
		step := PreviewStep{Index: i, ID: t.ID, Type: t.Type}
		switch {
		case state == "dropped":
			step.Status, step.Note = "skipped", "The event was dropped by an earlier step."
		case state == "error":
			step.Status, step.Note = "skipped", "The connector would stop at an earlier step."
		case state == "unknown":
			step.Status = "unknown"
		case t.Tables != nil && !slices.Contains(t.Tables, in.Table):
			step.Status = "skipped"
			step.Note = "Not applied to this table."
		default:
			var err string
			current, step.Status, step.Note, err = applyStep(in, t, current)
			if err != "" {
				step.Status, step.Note = "error", err
			}
		}
		switch step.Status {
		case "dropped", "unknown", "error":
			state = step.Status
		}
		if step.Status == "applied" {
			step.Event = &PreviewEvent{Key: jsonForm(key), Value: jsonForm(mongoText(current))}
		}
		res.Steps = append(res.Steps, step)
	}
	res.Outcome = state
	if state == "delivered" {
		res.Output = &PreviewEvent{Key: jsonForm(key), Value: jsonForm(mongoText(current))}
		if m, ok := current.(*om.Map); ok {
			res.Fields = m.Keys()
		} else {
			res.Fields = []string{}
		}
	}
	if f := in.Spec.Sink.Format.Type; f != "json" {
		res.Notes = append(res.Notes, fmt.Sprintf("The preview shows events as JSON; with %s the destination receives the same fields in that format.", f))
	}
	return res
}

// mongoText replaces MongoDB documents that are still JSON text in the envelope.
func mongoText(v any) any {
	m, ok := v.(*om.Map)
	if !ok {
		return v
	}
	out := om.New()
	for _, k := range m.Keys() {
		x, _ := m.Get(k)
		if d, ok := x.(*om.Map); ok {
			if text, ok := d.Get("__text"); ok {
				x = text
			}
		}
		out.Set(k, x)
	}
	return out
}

func messageKey(in PreviewInput, before, after row) any {
	cols := in.PrimaryKey
	if k, ok := in.Spec.Keys[in.Table]; ok {
		// Debezium ignores key columns the table does not have, and falls back to the
		// primary key when none is left (checked on 3.7.0).
		var found []string
		for _, c := range k {
			if slices.ContainsFunc(in.Columns, func(col PreviewColumn) bool { return col.Name == c }) {
				found = append(found, c)
			}
		}
		if len(found) > 0 {
			cols = found
		}
	}
	if len(cols) == 0 {
		return nil
	}
	src := after
	if src == nil {
		src = before
	}
	if src == nil {
		return nil
	}
	if d, ok := src.Get("__document"); ok {
		src = d.(*om.Map)
	}
	key := om.New()
	for _, c := range cols {
		v, _ := src.Get(c)
		if in.SourceType == "mongodb" && c == "_id" {
			key.Set("id", fmt.Sprintf("%v", v))
			continue
		}
		key.Set(c, v)
	}
	return key
}

// applyStep runs one step: the new value, status, a note and an error that would fail the
// connector.
func applyStep(in PreviewInput, t Transform, value any) (any, string, string, string) {
	switch t.Type {
	case "filter":
		if !t.Typed() {
			return value, "unknown", "A filter script cannot be previewed.", ""
		}
		env := value.(*om.Map)
		if keep(t, env) {
			return value, "applied", "Kept.", ""
		}
		return nil, "dropped", "Dropped: the event does not match this filter.", ""
	case "unwrap":
		return unwrap(in, t, value.(*om.Map))
	case "truncate":
		var cut []string
		for _, c := range t.Truncate {
			if c.Table != in.Table {
				continue
			}
			text := false
			for _, col := range in.Columns {
				text = text || (col.Name == c.Column && previewText(col.Type))
			}
			if !text {
				continue
			}
			cutField(value, c.Column, c.Length)
			cut = append(cut, fmt.Sprintf("%s to %d characters", c.Column, c.Length))
		}
		if len(cut) == 0 {
			return value, "applied", "Nothing to cut in this table (text columns only).", ""
		}
		return value, "applied", "Cuts " + strings.Join(cut, ", ") + ".", ""
	case "soft_delete":
		field := SoftDeleteField["debezium"]
		for _, c := range in.Columns {
			if c.Name == field {
				return value, "error", "", fmt.Sprintf("The table already has a column %s, which the soft delete would overwrite (field name duplication).", field)
			}
		}
		if env, ok := value.(*om.Map); ok && env != nil {
			if _, isEnvelope := env.Get("op"); isEnvelope {
				// No unwrap step before it: the compiler adds one that marks deletes.
				return unwrap(in, Transform{Type: "unwrap", DeleteHandling: "rewrite", AddFields: []string{"op", "source.ts_ms"}}, env)
			}
		}
		return value, "applied", "Deleted rows are kept, marked " + field + ".", ""
	case "outbox":
		return value, "unknown", "The outbox router cannot be previewed.", ""
	case "custom":
		return value, "unknown", "Custom transforms cannot be previewed; check the Events tab after deploying.", ""
	}
	m, ok := value.(*om.Map)
	if !ok || m == nil {
		return value, "applied", "Tombstones pass through unchanged.", ""
	}
	switch t.Type {
	case "rename":
		out := om.New()
		renamed := map[string]string{}
		for _, r := range t.Renames {
			renamed[r.From] = r.To
		}
		var missing []string
		for _, r := range t.Renames {
			if _, ok := m.Get(r.From); !ok {
				missing = append(missing, r.From)
			}
		}
		for _, k := range m.Keys() {
			v, _ := m.Get(k)
			name := k
			if to, ok := renamed[k]; ok {
				name = to
			}
			if _, dup := out.Get(name); dup {
				return value, "error", "", fmt.Sprintf("Two fields would be called %s; the connector would stop with an error.", name)
			}
			out.Set(name, v)
		}
		note := ""
		if len(missing) > 0 {
			note = "No field " + strings.Join(missing, ", ") + " at this point: nothing to rename."
		}
		return out, "applied", note, ""
	case "add_field":
		if _, exists := m.Get(t.Name); exists {
			return value, "error", "", fmt.Sprintf("The event already has a field %s; the connector would stop with an error.", t.Name)
		}
		out := cloneRow(m)
		text := t.Value.Text
		if t.Value.Kind == "pipeline" {
			text = in.Pipeline
		}
		out.Set(t.Name, text)
		return out, "applied", "", ""
	case "flatten":
		out := om.New()
		if dup := flattenInto(out, "", t.Delimiter, m); dup != "" {
			return value, "error", "", fmt.Sprintf("Flattening makes two fields called %s; the connector would stop with an error.", dup)
		}
		return out, "applied", "", ""
	}
	return value, "unknown", "", ""
}

func flattenInto(out row, prefix, delim string, m row) string {
	for _, k := range m.Keys() {
		v, _ := m.Get(k)
		name := k
		if prefix != "" {
			name = prefix + delim + k
		}
		if nested, ok := v.(*om.Map); ok {
			if dup := flattenInto(out, name, delim, nested); dup != "" {
				return dup
			}
			continue
		}
		if _, dup := out.Get(name); dup {
			return name
		}
		out.Set(name, v)
	}
	return ""
}

// unwrap is ExtractNewRecordState / ExtractNewDocumentState.
func unwrap(in PreviewInput, t Transform, env row) (any, string, string, string) {
	op, _ := env.Get("op")
	before, _ := env.Get("before")
	after, _ := env.Get("after")
	pick := after
	if op == "d" {
		switch t.DeleteHandling {
		case "drop":
			return nil, "dropped", "Delete events are dropped.", ""
		case "tombstone":
			return nil, "applied", "Deletes become a tombstone (empty value).", ""
		}
		pick = before
	}
	var r row
	if m, ok := pick.(*om.Map); ok && m != nil {
		if d, ok := m.Get("__document"); ok {
			r = cloneRow(d.(*om.Map))
		} else {
			r = cloneRow(m)
		}
	}
	if r == nil {
		r = om.New()
	}
	if t.DeleteHandling == "rewrite" {
		r.Set("__deleted", map[bool]string{true: "true", false: "false"}[op == "d"])
	}
	source, _ := env.Get("source")
	for _, f := range t.AddFields {
		var v any
		path := strings.TrimPrefix(f, "source.")
		if strings.HasPrefix(f, "source.") {
			v, _ = source.(*om.Map).Get(path)
		} else if x, ok := env.Get(f); ok {
			v = x
		} else {
			v, _ = source.(*om.Map).Get(f)
		}
		r.Set("__"+strings.ReplaceAll(f, ".", "_"), v)
	}
	return r, "applied", "", ""
}

// keep evaluates a typed filter like the generated Groovy expression.
func keep(t Transform, env row) bool {
	if len(t.Operations) > 0 {
		op, _ := env.Get("op")
		ok := false
		for _, o := range t.Operations {
			ok = ok || FilterOperations[o] == op
		}
		if !ok {
			return false
		}
	}
	if len(t.Rules) == 0 {
		return true
	}
	var r row
	for _, side := range []string{"after", "before"} {
		if v, _ := env.Get(side); v != nil {
			if m, ok := v.(*om.Map); ok && m != nil {
				r = m
				break
			}
		}
	}
	get := func(f string) any {
		if r == nil {
			return nil
		}
		v, _ := r.Get(f)
		return v
	}
	matchAny := t.Match == "any"
	for _, rule := range t.Rules {
		hit := ruleHolds(rule, get(rule.Field))
		if matchAny && hit {
			return true
		}
		if !matchAny && !hit {
			return false
		}
	}
	return !matchAny
}

func ruleHolds(r FilterRule, x any) bool {
	switch r.Op {
	case "eq":
		return valueEquals(x, r.Value)
	case "ne":
		return !valueEquals(x, r.Value)
	case "in", "not_in":
		hit := false
		for _, v := range r.Values {
			hit = hit || valueEquals(x, v)
		}
		return hit == (r.Op == "in")
	case "gt", "gte", "lt", "lte":
		a, b := numberOf(x), numberOf(r.Value)
		if a == nil || b == nil {
			return false
		}
		c := a.Cmp(b)
		return map[string]bool{"gt": c > 0, "gte": c >= 0, "lt": c < 0, "lte": c <= 0}[r.Op]
	case "is_null":
		return x == nil
	case "is_not_null":
		return x != nil
	case "is_true":
		return x == true
	case "is_false":
		return x == false
	}
	return false
}

func valueEquals(x, lit any) bool {
	switch l := lit.(type) {
	case string:
		s, ok := x.(string)
		return ok && s == l
	case bool:
		b, ok := x.(bool)
		return ok && b == l
	}
	a, b := numberOf(x), numberOf(lit)
	return a != nil && b != nil && a.Cmp(b) == 0
}

// numberOf is n() of the generated expression: numbers and numeric text, as exact values.
func numberOf(v any) *big.Rat {
	switch x := v.(type) {
	case int64:
		return new(big.Rat).SetInt64(x)
	case float64:
		r, ok := new(big.Rat).SetString(fmt.Sprint(x))
		if !ok {
			return nil
		}
		return r
	case decimal:
		return new(big.Rat).SetFrac(x.unscaled, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(x.scale)), nil))
	case string:
		r, ok := new(big.Rat).SetString(strings.TrimSpace(x))
		if !ok {
			return nil
		}
		return r
	case fmt.Stringer:
		r, ok := new(big.Rat).SetString(x.String())
		if !ok {
			return nil
		}
		return r
	}
	return nil
}

// jsonForm converts event values to what the JSON converter writes.
func jsonForm(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case *om.Map:
		if x == nil {
			return nil
		}
		out := om.New()
		for _, k := range x.Keys() {
			e, _ := x.Get(k)
			out.Set(k, jsonForm(e))
		}
		return out
	case decimal:
		return base64.StdEncoding.EncodeToString(twosComplement(x.unscaled))
	case bytesValue:
		return base64.StdEncoding.EncodeToString(x)
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = jsonForm(e)
		}
		return out
	}
	return v
}

// twosComplement is BigInteger.toByteArray().
func twosComplement(n *big.Int) []byte {
	if n.Sign() >= 0 {
		b := n.Bytes()
		if len(b) == 0 || b[0]&0x80 != 0 {
			b = append([]byte{0}, b...)
		}
		return b
	}
	// Negative: 2^(8k) + n with enough bytes for the sign bit.
	byteLen := (n.BitLen() + 8) / 8
	mod := new(big.Int).Lsh(big.NewInt(1), uint(byteLen*8))
	b := new(big.Int).Add(mod, n).Bytes()
	for len(b) < byteLen {
		b = append([]byte{0xff}, b...)
	}
	return b
}

// previewText: a column type truncation cuts (text, not JSON).
func previewText(typ string) bool {
	t := strings.ToLower(typ)
	return !strings.Contains(t, "json") && (strings.Contains(t, "char") || strings.Contains(t, "text") || strings.Contains(t, "string") || strings.Contains(t, "clob"))
}

// cutField cuts a field's string value to n characters, in a row or in an envelope's before and
// after.
func cutField(value any, field string, n int) {
	m, ok := value.(*om.Map)
	if !ok || m == nil {
		return
	}
	cut := func(r *om.Map) {
		if v, ok := r.Get(field); ok {
			if s, ok := v.(string); ok && len([]rune(s)) > n {
				r.Set(field, string([]rune(s)[:n]))
			}
		}
	}
	if _, envelope := m.Get("op"); envelope {
		for _, side := range []string{"before", "after"} {
			if r, ok := m.Get(side); ok {
				if rm, ok := r.(*om.Map); ok && rm != nil {
					cut(rm)
				}
			}
		}
		return
	}
	cut(m)
}
