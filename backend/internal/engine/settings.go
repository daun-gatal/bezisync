package engine

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// The settings registry: every engine, source and destination option a pipeline may tune, typed
// and bounded, with what changing it does to a running pipeline. The compiler, preflight, the
// wizard, policies and MCP read it, so they never disagree. Keys BeziSync sets itself are never in
// it; raw keys outside it are judged by compiler.ClassifyAdvanced.

// Impact is what changing a setting does to a running pipeline, lightest first.
type Impact string

const (
	// ImpactLive applies without a restart.
	ImpactLive Impact = "live"
	// ImpactRestart restarts the pipeline, which resumes where it stopped.
	ImpactRestart Impact = "restart"
	// ImpactFirstCopy only matters while the pipeline copies existing rows the first time.
	ImpactFirstCopy Impact = "first_copy"
	// ImpactNextCopy takes effect on the next copy of a table.
	ImpactNextCopy Impact = "next_copy"
	// ImpactBreaksPositions means the saved position can no longer be used: the pipeline starts over.
	ImpactBreaksPositions Impact = "breaks_positions"
)

// Impacts are the impact classes, lightest first.
var Impacts = []Impact{ImpactLive, ImpactRestart, ImpactFirstCopy, ImpactNextCopy, ImpactBreaksPositions}

// Rank orders impacts: the heaviest of a change decides how it is confirmed.
func (i Impact) Rank() int { return slices.Index(Impacts, i) }

// Where a setting goes.
const (
	ScopeEngine   = "engine"   // the engine itself: Flink settings (flink_conf), Debezium Server
	ScopeSource   = "source"   // the source connector
	ScopeSink     = "sink"     // the destination
	ScopePipeline = "pipeline" // a Flink CDC pipeline option
)

// The stages a setting belongs to, in the order the wizard groups them.
const (
	StageSnapshot  = "snapshot"
	StageStreaming = "streaming"
	StageDelivery  = "delivery"
	StageEngine    = "engine"
)

// Setting is one tunable option.
type Setting struct {
	// Engine runs it; Scope and Stage say where it goes and how the wizard groups it.
	Engine ID     `json:"engine"`
	Scope  string `json:"scope"`
	Stage  string `json:"stage"`
	// Impact is what changing it does to a running pipeline.
	Impact Impact `json:"impact"`
	Key    string `json:"key"`
	Title  string `json:"title"`
	// Type is "duration" (a number with ms, s or min), "int", "fraction" (0 to 1), "enum" or "bool".
	Type    string   `json:"type"`
	Default string   `json:"default"`
	Choices []string `json:"choices,omitempty"`
	// Min and Max bound durations (as Go durations) and numbers.
	Min string `json:"min,omitempty"`
	Max string `json:"max,omitempty"`
	Why string `json:"why"`
	// Connectors are the source or destination types it applies to (nil: every one).
	Connectors []string `json:"connectors,omitempty"`
	// Derived: BeziSync sets a default for it (from the size), which the setting replaces. Any
	// other key BeziSync compiles is never in the registry.
	Derived bool `json:"derived,omitempty"`
}

// Settings is the registry, in the order the wizard lists it.
func Settings() []Setting {
	return slices.Concat(DebeziumSettings, FlinkSettings, FlinkPipelineSettings)
}

// TuningScopes are the scopes a spec's tuning holds (a Flink CDC pipeline's engine-scoped Flink
// settings live in flink_cdc.flink_conf).
func TuningScopes(id ID) []string {
	if id == FlinkCDC {
		return []string{ScopeSource, ScopeSink, ScopePipeline}
	}
	return []string{ScopeEngine, ScopeSource, ScopeSink, ScopePipeline}
}

// Relational Debezium sources that copy with snapshot threads.
var threadedSources = []string{"postgresql", "mysql", "sqlserver", "oracle", "mongodb"}

// DebeziumSettings are the Debezium Server options a pipeline may tune.
var DebeziumSettings = []Setting{
	{Engine: Debezium, Scope: ScopeSource, Stage: StageSnapshot, Impact: ImpactFirstCopy, Key: "debezium.source.snapshot.max.threads",
		Title: "Snapshot threads", Type: "int", Default: "1", Min: "1", Max: "16", Connectors: threadedSources,
		Why: "Copies several chunks of a table at once. Each thread opens one more connection to the source."},
	{Engine: Debezium, Scope: ScopeSource, Stage: StageStreaming, Impact: ImpactRestart, Key: "debezium.source.poll.interval.ms",
		Title: "Poll interval", Type: "int", Default: "500", Min: "50", Max: "10000",
		Why: "How long, in milliseconds, to wait for new changes before passing on a batch. Lower is fresher but busier."},
}

// FlinkPipelineSettings are Flink CDC source, destination and pipeline options a pipeline may
// tune; their keys carry the YAML block they go in (source., sink., pipeline.).
var FlinkPipelineSettings = []Setting{
	{Engine: FlinkCDC, Scope: ScopeSource, Stage: StageStreaming, Impact: ImpactRestart, Key: "source.connection.pool.size",
		Title: "Connection pool size", Type: "int", Default: "20", Min: "2", Max: "100", Connectors: []string{"mysql", "postgresql"},
		Why: "Connections the readers share. Each one counts against the database's connection limit."},
}

// CanonicalSetting is value written the one way a spec stores it ("" when it isn't valid).
func CanonicalSetting(s Setting, value string) string {
	if CheckSetting(s, value) != "" {
		return ""
	}
	switch s.Type {
	case "int":
		n, _ := strconv.Atoi(value)
		return strconv.Itoa(n)
	case "fraction":
		f, _ := strconv.ParseFloat(value, 64)
		return strconv.FormatFloat(f, 'f', -1, 64)
	case "duration":
		m := flinkDuration.FindStringSubmatch(strings.TrimSpace(value))
		n, _ := strconv.Atoi(m[1])
		return strconv.Itoa(n) + " " + m[2]
	}
	return value
}

// SettingsFor are the settings a pipeline on engine id, from sourceType to sinkType, may tune.
func SettingsFor(id ID, sourceType, sinkType string) []Setting {
	var out []Setting
	for _, s := range Settings() {
		if s.Engine != id {
			continue
		}
		switch s.Scope {
		case ScopeSource:
			if s.Connectors != nil && !slices.Contains(s.Connectors, sourceType) {
				continue
			}
		case ScopeSink:
			if s.Connectors != nil && !slices.Contains(s.Connectors, sinkType) {
				continue
			}
		}
		out = append(out, s)
	}
	return out
}

// SettingOf is engine id's setting key, or nil.
func SettingOf(id ID, key string) *Setting {
	for _, s := range Settings() {
		if s.Engine == id && s.Key == key {
			return &s
		}
	}
	return nil
}

// CheckSetting checks one value against its setting ("" when valid). It must keep accepting
// what it accepted before: the compiler re-checks deployed specs on every reconcile.
func CheckSetting(s Setting, value string) string {
	switch s.Type {
	case "duration":
		d, ok := ParseFlinkDuration(value)
		if !ok {
			return "must be a duration such as 30 s, 5 min or 500 ms"
		}
		lo, _ := time.ParseDuration(s.Min)
		hi, _ := time.ParseDuration(s.Max)
		if d < lo || d > hi {
			return fmt.Sprintf("must be between %s and %s", s.Min, s.Max)
		}
	case "int":
		n, err := strconv.Atoi(value)
		lo, _ := strconv.Atoi(s.Min)
		hi, _ := strconv.Atoi(s.Max)
		if err != nil || n < lo || n > hi {
			return fmt.Sprintf("must be a whole number from %s to %s", s.Min, s.Max)
		}
	case "fraction":
		f, err := strconv.ParseFloat(value, 64)
		lo, _ := strconv.ParseFloat(s.Min, 64)
		hi, _ := strconv.ParseFloat(s.Max, 64)
		if err != nil || f < lo || f > hi || strings.ContainsAny(value, "eE") {
			return fmt.Sprintf("must be a number from %s to %s", s.Min, s.Max)
		}
	case "enum":
		if !slices.Contains(s.Choices, value) {
			return "must be one of " + strings.Join(s.Choices, ", ")
		}
	case "bool":
		if value != "true" && value != "false" {
			return "must be true or false"
		}
	}
	return ""
}
