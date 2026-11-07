package domain

import "slices"

// Iceberg partitioning rules shared by preflight and (mirrored) the wizard's Partition by editor
// (ADR 0005). The frontend keeps a copy in wizard/PartitionBy.tsx; the vectors in
// backend/testdata/partition_kinds.json keep the two equal.

// PartitionTransforms maps each partition transform to the written kinds it accepts: Iceberg's
// transform table (spec, "Partition Transforms") restricted to BeziSync's kinds. bucket takes no
// boolean or floating point value; truncate only numbers, text and bytes; the time transforms a
// date or a timestamp, hour a timestamp only.
var PartitionTransforms = map[string][]string{
	"identity": {"int", "decimal", "float", "bool", "text", "date", "time", "timestamp", "timestamptz", "binary"},
	"bucket":   {"int", "decimal", "text", "date", "time", "timestamp", "timestamptz", "binary"},
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
// make of it. "" for a value no transform takes (a document or a list).
//
//   - Debezium (the Iceberg build's StructSchemaConverter): decimals as the decimal mode says
//     (double → float, string → text), CockroachDB, Vitess and Spanner decimals always as text;
//     binary as text unless sent as bytes; TIME as a number (int); JSON and UUID as strings;
//     Spanner TIMESTAMP as ISO text; dates and timestamps (ISO strings or Kafka Connect's types)
//     as Iceberg dates and timestamps.
//   - Flink CDC: JSON and UUID as strings; the rest as its type.
func PartitionKind(engine, sourceType, dbType string, formats ValueFormats) string {
	kind := ColumnKind(sourceType, dbType)
	switch kind {
	case "document", "list":
		return ""
	case "json", "uuid":
		return "text"
	}
	if engine == "flink-cdc" {
		return kind
	}
	switch {
	case kind == "decimal" && (sourceType == "cockroachdb" || sourceType == "vitess" || sourceType == "spanner"):
		return "text"
	case kind == "decimal" && formats.Decimals == "double":
		return "float"
	case kind == "decimal" && formats.Decimals == "string":
		return "text"
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
