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
}

// Settings is the registry, in the order the wizard lists it.
func Settings() []Setting { return slices.Clone(FlinkSettings) }

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
