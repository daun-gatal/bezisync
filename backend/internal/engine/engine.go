// Package engine names the change-data-capture engines BeziSync runs and what each one can do.
//
// Debezium Server streams change events to every destination BeziSync supports. Flink CDC keeps
// tables in sync in analytics and lakehouse stores (Iceberg, StarRocks, Doris, Fluss), reading
// MySQL/MariaDB, PostgreSQL and SQL Server. A pipeline's engine is chosen when its destination is
// chosen and fixed once it is deployed.
//
// The package is pure: capabilities come from these tables, never from the destination type
// alone, so the API, the UI, the CLI and MCP all offer the same actions for a pipeline.
package engine

import "slices"

// ID names an engine; it is stored on every pipeline.
type ID string

const (
	// Debezium is Debezium Server, one container per pipeline.
	Debezium ID = "debezium"
	// FlinkCDC is Flink CDC on a per-pipeline Flink cluster (a JobManager and TaskManagers).
	FlinkCDC ID = "flink-cdc"
)

// All lists the engines in the order they are offered.
var All = []ID{Debezium, FlinkCDC}

// Valid reports a known engine.
func (id ID) Valid() bool { return slices.Contains(All, id) }

// Title is the engine's name for people.
func (id ID) Title() string {
	switch id {
	case Debezium:
		return "Debezium"
	case FlinkCDC:
		return "Flink CDC"
	}
	return string(id)
}

// flinkSources are the source types Flink CDC reads in BeziSync (its pipeline sources; MongoDB
// and Oracle stay on Debezium).
var flinkSources = []string{"mysql", "postgresql", "sqlserver"}

// flinkSinks are the destination types Flink CDC writes in BeziSync.
var flinkSinks = []string{"iceberg", "starrocks", "doris", "fluss"}

// flinkOnlySinks have no Debezium Server sink.
var flinkOnlySinks = []string{"starrocks", "doris", "fluss"}

// Runs lists what an engine reads and writes: nil means every source (or destination) except
// those in exceptSinks.
func Runs(id ID) (sources, sinks, exceptSinks []string) {
	if id == FlinkCDC {
		return slices.Clone(flinkSources), slices.Clone(flinkSinks), nil
	}
	return nil, nil, slices.Clone(flinkOnlySinks)
}

// FlinkOnlySink reports a destination only Flink CDC can write.
func FlinkOnlySink(sinkType string) bool { return slices.Contains(flinkOnlySinks, sinkType) }

// Supports reports whether the engine can run a pipeline from sourceType to sinkType. An empty
// sinkType asks about the source alone.
func Supports(id ID, sourceType, sinkType string) bool {
	switch id {
	case Debezium:
		return sinkType == "" || !FlinkOnlySink(sinkType)
	case FlinkCDC:
		if !slices.Contains(flinkSources, sourceType) {
			return false
		}
		return sinkType == "" || slices.Contains(flinkSinks, sinkType)
	}
	return false
}

// Choices are the engines that can run the pair, in the order they are offered.
func Choices(sourceType, sinkType string) []ID {
	var out []ID
	for _, id := range All {
		if Supports(id, sourceType, sinkType) {
			out = append(out, id)
		}
	}
	return out
}

// Default is the engine a new pipeline gets for the pair: Debezium wherever it can run (it is the
// lighter engine and the only one with event features), otherwise Flink CDC. "" when neither can.
func Default(sourceType, sinkType string) ID {
	choices := Choices(sourceType, sinkType)
	if len(choices) == 0 {
		return ""
	}
	return choices[0]
}

// Of returns a stored engine, treating the empty value of rows written before engines existed as
// Debezium.
func Of(stored string) ID {
	if stored == "" {
		return Debezium
	}
	return ID(stored)
}
