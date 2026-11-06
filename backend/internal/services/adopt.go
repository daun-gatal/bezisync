package services

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/daun-gatal/bezisync/backend/internal/compiler"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/om"
)

// Adopt an existing Debezium Server deployment from its application.properties: every
// property is reported as mapped, kept (advanced), managed by BeziSync, or replaced.

var adoptConnectors = map[string]string{
	"io.debezium.connector.postgresql.PostgresConnector":     "postgresql",
	"io.debezium.connector.mysql.MySqlConnector":             "mysql",
	"io.debezium.connector.mariadb.MariaDbConnector":         "mysql",
	"io.debezium.connector.sqlserver.SqlServerConnector":     "sqlserver",
	"io.debezium.connector.mongodb.MongoDbConnector":         "mongodb",
	"io.debezium.connector.oracle.OracleConnector":           "oracle",
	"io.debezium.connector.cockroachdb.CockroachDBConnector": "cockroachdb",
	"io.debezium.connector.vitess.VitessConnector":           "vitess",
	"io.debezium.connector.spanner.SpannerConnector":         "spanner",
}

var adoptSinks = map[string]bool{"kafka": true, "http": true, "pubsub": true, "kinesis": true, "redis": true, "nats-jetstream": true, "iceberg": true, "jdbc": true}

var generatedRe = regexp.MustCompile(`^debezium\.(format\..*|transforms.*|predicates.*|source\.(table|collection)\.include\.list` +
	`|source\.(heartbeat\.interval\.ms|offset\.flush\.interval\.ms|notification\..*|max\.(batch|queue)\.size` +
	`|errors\.max\.retries|event\.processing\.failure\.handling\.mode|snapshot\.(mode|fetch\.size|delay\.ms)` +
	`|incremental\.snapshot\.chunk\.size|plugin\.name|publication\.autocreate\.mode|column\.mask\..*` +
	`|column\.exclude\.list)|sink\..*)$`)

// AdoptReport explains how a properties file maps to BeziSync.
type AdoptReport struct {
	SourceType *string        `json:"source_type"`
	SinkType   *string        `json:"sink_type"`
	SourceHint map[string]any `json:"source_hint"`
	SinkHint   map[string]any `json:"sink_hint"`
	Spec       *om.Map        `json:"spec"`
	Mapped     []string       `json:"mapped"`
	Kept       []string       `json:"kept"`
	Managed    []string       `json:"managed"`
	Replaced   []string       `json:"replaced"`
	Warnings   []string       `json:"warnings"`
}

var propLineRe = regexp.MustCompile(`^([^=:\s]+)\s*[=:\s]\s*(.*)$`)

// ParseProperties reads Java .properties (comments, continuations).
func ParseProperties(text string) map[string]string {
	props := map[string]string{}
	pending := ""
	for _, raw := range domain.SplitLines(text) {
		line := domain.PyStrip(raw)
		if pending != "" {
			line = pending + line
		}
		pending = ""
		if line == "" || line[0] == '#' || line[0] == '!' {
			continue
		}
		if strings.HasSuffix(line, `\`) && !strings.HasSuffix(line, `\\`) {
			pending = line[:len(line)-1]
			continue
		}
		if m := propLineRe.FindStringSubmatch(line); m != nil {
			props[m[1]] = strings.ReplaceAll(m[2], `\\`, `\`)
		}
	}
	return props
}

var adoptTableRe = regexp.MustCompile(`^[A-Za-z0-9_$\-]+\.[A-Za-z0-9_$\-]+$`)

func adoptTables(value string) []string {
	out := []string{}
	for _, item := range strings.Split(value, ",") {
		item = domain.PyStrip(item)
		if item == "" {
			continue
		}
		literal := strings.ReplaceAll(item, `\.`, ".")
		if adoptTableRe.MatchString(literal) {
			out = append(out, literal)
		}
	}
	return out
}

func pyRepr(s string) string {
	if strings.Contains(s, "'") && !strings.Contains(s, `"`) {
		return `"` + s + `"`
	}
	return "'" + strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), "'", `\'`) + "'"
}

// Adopt maps an application.properties onto a pipeline spec.
func Adopt(text string) AdoptReport {
	props := ParseProperties(text)
	r := AdoptReport{SourceHint: map[string]any{}, SinkHint: map[string]any{}, Mapped: []string{}, Kept: []string{},
		Managed: []string{}, Replaced: []string{}, Warnings: []string{}}
	src := map[string]string{}
	for k, v := range props {
		if strings.HasPrefix(k, "debezium.source.") {
			src[strings.TrimPrefix(k, "debezium.source.")] = v
		}
	}
	if t, ok := adoptConnectors[src["connector.class"]]; ok {
		r.SourceType = &t
	} else {
		r.Warnings = append(r.Warnings, "No supported source connector.class found ("+adoptSourceTitles()+").")
	}
	setHint := func(k string, v any) {
		switch x := v.(type) {
		case string:
			if x != "" {
				r.SourceHint[k] = x
			}
		case int:
			if x != 0 {
				r.SourceHint[k] = x
			}
		}
	}
	setHint("host", src["database.hostname"])
	if p := src["database.port"]; isDigitString(p) {
		n, _ := strconv.Atoi(p)
		setHint("port", n)
	}
	db := src["database.dbname"]
	if db == "" {
		db = src["database.names"]
	}
	setHint("database", db)
	setHint("pdb", src["database.pdb.name"])
	setHint("keyspace", src["vitess.keyspace"])
	setHint("project_id", src["gcp.spanner.project.id"])
	setHint("instance_id", src["gcp.spanner.instance.id"])
	setHint("database_id", src["gcp.spanner.database.id"])
	if stream := src["gcp.spanner.change.stream"]; stream != "" {
		// An adopted deployment already has its change stream: keep reading that one.
		setHint("change_stream_mode", "existing")
		setHint("change_stream", stream)
	}
	if t := map[string]string{"MASTER": "primary", "PRIMARY": "primary", "REPLICA": "replica", "RDONLY": "rdonly"}[strings.ToUpper(src["vitess.tablet.type"])]; t != "" {
		setHint("tablet_type", t)
	}
	setHint("user", src["database.user"])
	hosts := src["mongodb.connection.string"]
	if hosts == "" {
		hosts = src["mongodb.hosts"]
	}
	setHint("hosts", hosts)
	if pw := src["database.password"]; domain.PyStrip(pw) != "" && !strings.HasPrefix(pw, "${") {
		r.Warnings = append(r.Warnings, "The file contains a plain-text database password. BeziSync did not keep it; store it in the connection (or a secret store) and rotate it.")
	}
	sinkSpec := om.New()
	spec := om.New().Set("source", om.New().Set("tables", om.New().Set("include", []any{}))).Set("sink", sinkSpec)
	include := src["table.include.list"]
	if include == "" {
		include = src["collection.include.list"]
	}
	tables := adoptTables(include)
	tl := make([]any, len(tables))
	for i, t := range tables {
		tl[i] = t
	}
	srcV, _ := spec.Get("source")
	tblV, _ := srcV.(*om.Map).Get("tables")
	tblV.(*om.Map).Set("include", tl)
	nonEmpty := 0
	for _, v := range strings.Split(include, ",") {
		if domain.PyStrip(v) != "" {
			nonEmpty++
		}
	}
	if include != "" && len(tables) < nonEmpty {
		r.Warnings = append(r.Warnings, "Some include-list entries are regular expressions; pick those tables explicitly in BeziSync.")
	}
	if mode, ok := src["snapshot.mode"]; ok {
		if _, known := domain.SnapshotModeHelp[mode]; known {
			spec.Set("snapshot", om.New().Set("mode", mode))
		} else if mode != "" {
			r.Warnings = append(r.Warnings, fmt.Sprintf("Snapshot mode '%s' is not offered; BeziSync uses 'initial' unless you choose otherwise.", mode))
		}
	}
	if sinkType, ok := props["debezium.sink.type"]; ok {
		if adoptSinks[sinkType] {
			t := sinkType
			if t == "nats-jetstream" {
				t = "nats"
			}
			r.SinkType = &t
			switch sinkType {
			case "kafka":
				r.SinkHint = map[string]any{"bootstrap_servers": props["debezium.sink.kafka.producer.bootstrap.servers"]}
			case "iceberg":
				hint := map[string]any{}
				for key, field := range map[string]string{"type": "catalog_type", "uri": "uri", "warehouse": "warehouse", "table-namespace": "namespace"} {
					if v := props["debezium.sink.iceberg."+key]; v != "" {
						hint[field] = v
					}
				}
				r.SinkHint = hint
			case "jdbc":
				u := props["debezium.sink.jdbc.connection.url"]
				for _, db := range []string{"postgresql", "mysql", "mariadb", "sqlserver"} {
					if strings.HasPrefix(u, "jdbc:"+db+":") {
						r.SinkHint = map[string]any{"database_type": db}
					}
				}
			}
		} else if sinkType != "" {
			r.Warnings = append(r.Warnings, fmt.Sprintf("Sink type '%s' is not supported by BeziSync 1.0.", sinkType))
		}
	}
	prefix := src["topic.prefix"]
	if prefix == "" {
		prefix = src["database.server.name"]
	}
	if prefix != "" {
		sinkSpec.Set("naming", prefix+".{schema}.{table}")
	}
	var transforms []string
	for _, t := range strings.Split(props["debezium.transforms"], ",") {
		if t = domain.PyStrip(t); t != "" {
			transforms = append(transforms, t)
		}
	}
	steps, stepWarnings := adoptSteps(transforms, props)
	r.Warnings = append(r.Warnings, stepWarnings...)
	hasTransforms := false
	for _, st := range steps {
		if typ, _ := st.(*om.Map).Get("type"); typ == "unwrap" {
			hasTransforms = true
		}
	}
	spec.Set("transforms", steps)
	if !hasTransforms && (len(transforms) > 0 || len(props) > 0) {
		r.Warnings = append(r.Warnings, "No unwrap transform: events keep the full Debezium envelope (before/after/op).")
	}
	switch f := props["debezium.format.value"]; f {
	case "json", "avro", "protobuf", "cloudevents":
		sinkSpec.Set("format", om.New().Set("type", f))
	}
	slot := src["slot.name"]
	if r.SourceType != nil && *r.SourceType == "postgresql" {
		reuse := ""
		if slot != "" {
			reuse = fmt.Sprintf(" (it does not reuse %s)", pyRepr(slot))
		}
		r.Warnings = append(r.Warnings, "Stop the old deployment first. BeziSync creates its own slot and publication"+reuse+
			"; drop the old slot afterwards so it stops retaining WAL. To avoid copying existing rows again, choose 'Only new changes' (no_data).")
	}
	if src["offset.storage"] != "" || props["debezium.source.offset.storage.file.filename"] != "" {
		r.Warnings = append(r.Warnings, "Existing offsets stay where they are; BeziSync keeps its own offsets in its state database.")
	}
	if r.SourceType != nil && (*r.SourceType == "mysql" || *r.SourceType == "sqlserver" || *r.SourceType == "oracle") {
		r.Warnings = append(r.Warnings, "BeziSync keeps its own schema history; the old one is not reused.")
	}
	mappedKeys := map[string]bool{"debezium.source.connector.class": true, "debezium.source.table.include.list": true,
		"debezium.source.collection.include.list": true, "debezium.source.snapshot.mode": true, "debezium.sink.type": true,
		"debezium.source.topic.prefix": true, "debezium.format.value": true, "debezium.transforms": true}
	inTransforms := map[string]bool{}
	for _, t := range transforms {
		inTransforms[t] = true
	}
	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	values, keyColumns, optionKeys := adoptSourceOptions(src, tables)
	if values.Len() > 0 {
		spec.Set("values", values)
	}
	if keyColumns.Len() > 0 {
		spec.Set("keys", keyColumns)
	}
	advanced := om.New()
	for _, key := range keys {
		value := props[key]
		parts := strings.Split(key, ".")
		switch {
		case mappedKeys[key] || optionKeys[key] || (strings.HasPrefix(key, "debezium.transforms.") && len(parts) > 2 && inTransforms[parts[2]]):
			r.Mapped = append(r.Mapped, key)
		case managed(key):
			r.Managed = append(r.Managed, key)
		case generatedRe.MatchString(key):
			r.Replaced = append(r.Replaced, key)
		case strings.HasPrefix(key, "debezium.source.") && !compiler.ClassifyAdvanced(key, value, nil).Refused():
			advanced.Set(key, value)
			r.Kept = append(r.Kept, key)
		default:
			r.Replaced = append(r.Replaced, key)
		}
	}
	if advanced.Len() > 0 {
		spec.Set("advanced", advanced)
	}
	r.Spec = spec
	return r
}

func managed(key string) bool {
	for _, p := range compiler.ManagedPatterns {
		if p.MatchString(key) {
			return true
		}
	}
	return false
}

func isDigitString(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// adoptSourceTitles lists the sources an application.properties can be adopted from.
func adoptSourceTitles() string {
	seen := map[string]bool{}
	var titles []string
	for _, d := range connectors.Descriptors() {
		if d.Kind != "source" || seen[d.Type] {
			continue
		}
		for _, t := range adoptConnectors {
			if t == d.Type {
				seen[d.Type] = true
				titles = append(titles, d.Title)
				break
			}
		}
	}
	return strings.Join(titles, ", ")
}
