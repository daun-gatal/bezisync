package domain

import (
	"slices"
	"strings"
)

// Iceberg partitioning rules shared by preflight and (mirrored) the wizard's Partition by editor
// (ADR 0005). The frontend keeps a copy in wizard/PartitionBy.tsx; the vectors in
// backend/testdata/partition_kinds.json keep the two equal.

// PartitionTransforms maps each partition transform to the written kinds it accepts: Iceberg's
// transform table (spec, "Partition Transforms") restricted to BeziSync's kinds. bucket takes no
// boolean or floating point value; truncate only numbers, text and bytes; the time transforms a
// date or a timestamp, hour a timestamp only.
var PartitionTransforms = map[string][]string{
	"identity": {"int", "decimal", "float", "bool", "text", "uuid", "date", "time", "timestamp", "timestamptz", "binary"},
	"bucket":   {"int", "decimal", "text", "uuid", "date", "time", "timestamp", "timestamptz", "binary"},
	"truncate": {"int", "decimal", "text", "binary"},
	"year":     {"date", "timestamp", "timestamptz"},
	"month":    {"date", "timestamp", "timestamptz"},
	"day":      {"date", "timestamp", "timestamptz"},
	"hour":     {"timestamp", "timestamptz"},
}

// PartitionFits reports whether a transform takes a column of the written kind.
func PartitionFits(transform, kind string) bool {
	return slices.Contains(PartitionTransforms[transform], kind)
}

// PartitionKind is the kind of value an engine writes into the Iceberg table for a source column:
// ColumnKind, then what the engine's own type mapping and (on Debezium) the spec's value formats
// make of it. "" for a value no transform takes (a document, a list, a struct), or one BeziSync
// can't tell apart from such a value.
//
//   - Debezium (the Iceberg build's StructSchemaConverter): decimals as the decimal mode says
//     (double → float, string → text); CockroachDB, Vitess and Spanner decimals always as text; a
//     PostgreSQL NUMERIC in the precise mode is "" (discovery doesn't say whether it has a scale,
//     and one without is written as a struct); PostgreSQL arrays and user-defined types are "";
//     binary as text unless sent as bytes; a time of day as a number (int); JSON as a string;
//     UUID as Iceberg's uuid; Spanner TIMESTAMP as ISO text; dates and timestamps (ISO strings or
//     Kafka Connect's types) as Iceberg dates and timestamps.
//   - Flink CDC: JSON and UUID as strings; a MySQL TINYINT may be a boolean (TINYINT(1)), so it
//     counts as one.
func PartitionKind(engine, sourceType, dbType string, formats ValueFormats) string {
	t := strings.ToLower(strings.TrimSpace(dbType))
	kind := ColumnKind(sourceType, dbType)
	switch {
	case kind == "document" || kind == "list":
		return ""
	case kind == "json":
		return "text"
	case sourceType == "postgresql" && (t == "array" || t == "user-defined" || strings.HasPrefix(t, "_") || strings.HasSuffix(t, "[]")):
		return ""
	}
	if engine == "flink-cdc" {
		switch {
		case kind == "uuid":
			return "text"
		case sourceType == "mysql" && strings.HasPrefix(t, "tinyint"):
			return "bool"
		}
		return kind
	}
	switch {
	case kind == "decimal" && (sourceType == "cockroachdb" || sourceType == "vitess" || sourceType == "spanner"):
		return "text"
	case kind == "decimal" && formats.Decimals == "double":
		return "float"
	case kind == "decimal" && formats.Decimals == "string":
		return "text"
	case kind == "decimal" && sourceType == "postgresql":
		return ""
	case kind == "binary" && formats.Binary != "" && formats.Binary != "bytes":
		return "text"
	case kind == "time":
		return "int"
	case kind == "timestamptz" && sourceType == "spanner":
		return "text"
	}
	return kind
}

// SourceTimestampField is Debezium's time of the change in the Iceberg table (the source's commit
// time, UTC), written as a timestamp with time zone.
const SourceTimestampField = "__source_ts_ms"
