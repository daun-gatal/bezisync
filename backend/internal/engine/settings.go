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
	// Since and Until bound the Debezium releases that have it ("2.6": from 2.6.0; Until: gone
	// from that release); Renamed is its key from Until on.
	Since   string `json:"since,omitempty"`
	Until   string `json:"until,omitempty"`
	Renamed string `json:"renamed,omitempty"`
	// Experimental: the engine marks it experimental; preflight warns when it is set.
	Experimental bool `json:"experimental,omitempty"`
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
	{Engine: Debezium, Scope: ScopeSource, Stage: StageStreaming, Impact: ImpactRestart, Key: "debezium.source.max.queue.size",
		Title: "Queue size", Type: "int", Default: "8192", Min: "1024", Max: "131072", Derived: true,
		Why: "Changes held in memory between reading and sending. BeziSync sets it from the size; more absorbs bursts but needs heap."},
	{Engine: Debezium, Scope: ScopeSource, Stage: StageStreaming, Impact: ImpactRestart, Key: "debezium.source.max.batch.size",
		Title: "Batch size", Type: "int", Default: "2048", Min: "128", Max: "32768", Derived: true,
		Why: "Changes passed on at once. BeziSync sets it from the size; it must stay below the queue size."},
	{Engine: Debezium, Scope: ScopeSource, Stage: StageStreaming, Impact: ImpactRestart, Key: "debezium.source.max.queue.size.in.bytes",
		Title: "Queue memory limit", Type: "int", Default: "0", Min: "0", Max: "4294967296", Since: "1.9",
		Why: "Bytes the queue may hold (0: no limit beyond its size). Caps memory when changes are large."},
	{Engine: Debezium, Scope: ScopeSource, Stage: StageStreaming, Impact: ImpactRestart, Key: "debezium.source.retriable.restart.connector.wait.ms",
		Title: "Wait before reconnecting", Type: "int", Default: "10000", Min: "1000", Max: "600000",
		Why: "Milliseconds the connector waits after a connection error before it tries again."},
	{Engine: Debezium, Scope: ScopeSource, Stage: StageDelivery, Impact: ImpactRestart, Key: "debezium.source.skip.messages.without.change",
		Title: "Skip unchanged updates", Type: "bool", Default: "false", Since: "2.0",
		Why: "Leaves out an update when none of the columns the pipeline sends changed (only columns left out of the pipeline changed)."},
	// PostgreSQL.
	{Engine: Debezium, Scope: ScopeSource, Stage: StageStreaming, Impact: ImpactRestart, Key: "debezium.source.status.update.interval.ms",
		Title: "Slot feedback interval", Type: "int", Default: "10000", Min: "1000", Max: "600000", Connectors: []string{"postgresql"},
		Why: "Milliseconds between telling PostgreSQL what was read, which lets it free WAL. Longer keeps more WAL on the source."},
	{Engine: Debezium, Scope: ScopeSource, Stage: StageStreaming, Impact: ImpactRestart, Key: "debezium.source.slot.max.retries",
		Title: "Slot connect attempts", Type: "int", Default: "6", Min: "1", Max: "60", Connectors: []string{"postgresql"},
		Why: "How often the connector tries to take its replication slot when another session still holds it."},
	{Engine: Debezium, Scope: ScopeSource, Stage: StageStreaming, Impact: ImpactRestart, Key: "debezium.source.slot.retry.delay.ms",
		Title: "Slot retry delay", Type: "int", Default: "10000", Min: "1000", Max: "120000", Connectors: []string{"postgresql"},
		Why: "Milliseconds between those attempts."},
	// MySQL / MariaDB.
	{Engine: Debezium, Scope: ScopeSource, Stage: StageStreaming, Impact: ImpactRestart, Key: "debezium.source.connect.keep.alive.interval.ms",
		Title: "Keep-alive interval", Type: "int", Default: "60000", Min: "1000", Max: "600000", Connectors: []string{"mysql"},
		Why: "Milliseconds between checks that the binlog connection is still alive; it reconnects when it isn't."},
	{Engine: Debezium, Scope: ScopeSource, Stage: StageStreaming, Impact: ImpactRestart, Key: "debezium.source.connect.timeout.ms",
		Title: "Connect timeout", Type: "int", Default: "30000", Min: "1000", Max: "300000", Connectors: []string{"mysql"},
		Why: "Milliseconds to wait for the server when connecting."},
	{Engine: Debezium, Scope: ScopeSource, Stage: StageStreaming, Impact: ImpactRestart, Key: "debezium.source.binlog.buffer.size",
		Title: "Binlog look-ahead buffer", Type: "int", Default: "0", Min: "0", Max: "100000", Connectors: []string{"mysql"},
		Why: "Binlog events read ahead to tell committed from rolled-back transactions (0: none). Needs heap for that many events."},
	// Oracle (LogMiner).
	{Engine: Debezium, Scope: ScopeSource, Stage: StageStreaming, Impact: ImpactRestart, Key: "debezium.source.log.mining.batch.size.default",
		Title: "LogMiner batch", Type: "int", Default: "20000", Min: "1000", Max: "100000", Connectors: []string{"oracle"},
		Why: "System change numbers LogMiner reads at a time to begin with; it grows and shrinks between its minimum and maximum."},
	{Engine: Debezium, Scope: ScopeSource, Stage: StageStreaming, Impact: ImpactRestart, Key: "debezium.source.log.mining.sleep.time.default.ms",
		Title: "LogMiner pause", Type: "int", Default: "1000", Min: "0", Max: "10000", Connectors: []string{"oracle"},
		Why: "Milliseconds LogMiner waits between reads to begin with. Lower is fresher but loads the database more."},
	// MongoDB.
	{Engine: Debezium, Scope: ScopeSource, Stage: StageStreaming, Impact: ImpactRestart, Key: "debezium.source.mongodb.poll.interval.ms",
		Title: "New collections check", Type: "int", Default: "30000", Min: "1000", Max: "600000", Connectors: []string{"mongodb"},
		Why: "Milliseconds between checks for new or removed collections that the pipeline captures."},
	// Destinations: batching and retries a connection field doesn't set.
	{Engine: Debezium, Scope: ScopeSink, Stage: StageDelivery, Impact: ImpactRestart, Key: "debezium.sink.kafka.producer.linger.ms",
		Title: "Producer linger", Type: "int", Default: "5", Min: "0", Max: "1000", Connectors: []string{"kafka"},
		Why: "Milliseconds the producer waits to fill a batch. Higher sends bigger batches, a little later."},
	{Engine: Debezium, Scope: ScopeSink, Stage: StageDelivery, Impact: ImpactRestart, Key: "debezium.sink.kafka.producer.batch.size",
		Title: "Producer batch bytes", Type: "int", Default: "16384", Min: "1024", Max: "4194304", Connectors: []string{"kafka"},
		Why: "Bytes per batch to one partition."},
	{Engine: Debezium, Scope: ScopeSink, Stage: StageDelivery, Impact: ImpactRestart, Key: "debezium.sink.kafka.producer.compression.type",
		Title: "Compression", Type: "enum", Default: "zstd", Choices: []string{"zstd", "lz4", "snappy", "gzip", "none"}, Connectors: []string{"kafka"}, Derived: true,
		Why: "How batches are compressed on the wire and on the brokers. Consumers read every one of these."},
	{Engine: Debezium, Scope: ScopeSink, Stage: StageDelivery, Impact: ImpactRestart, Key: "debezium.sink.pubsub.batch.delay.threshold.ms",
		Title: "Batch delay", Type: "int", Default: "100", Min: "1", Max: "10000", Connectors: []string{"pubsub"},
		Why: "Milliseconds a batch may wait before it is published."},
	{Engine: Debezium, Scope: ScopeSink, Stage: StageDelivery, Impact: ImpactRestart, Key: "debezium.sink.pubsub.batch.element.count.threshold",
		Title: "Batch messages", Type: "int", Default: "100", Min: "1", Max: "1000", Connectors: []string{"pubsub"},
		Why: "Messages that make a batch publish at once."},
	{Engine: Debezium, Scope: ScopeSink, Stage: StageDelivery, Impact: ImpactRestart, Key: "debezium.sink.redis.batch.size",
		Title: "Batch size", Type: "int", Default: "500", Min: "1", Max: "10000", Connectors: []string{"redis"},
		Why: "Changes written to Redis in one round trip."},
	{Engine: Debezium, Scope: ScopeSink, Stage: StageDelivery, Impact: ImpactRestart, Key: "debezium.sink.redis.retry.max.delay.ms",
		Title: "Retry delay limit", Type: "int", Default: "10000", Min: "100", Max: "600000", Connectors: []string{"redis"},
		Why: "The longest wait, in milliseconds, between retries when Redis is unreachable."},
	{Engine: Debezium, Scope: ScopeEngine, Stage: StageEngine, Impact: ImpactRestart, Key: "debezium.source.offset.flush.interval.ms",
		Title: "Position save interval", Type: "int", Default: "5000", Min: "1000", Max: "600000", Derived: true,
		Why: "Milliseconds between saves of the position. After a crash the changes since the last save are sent again."},
	{Engine: Debezium, Scope: ScopeEngine, Stage: StageEngine, Impact: ImpactRestart, Key: "quarkus.log.level",
		Title: "Log level", Type: "enum", Default: "INFO", Choices: []string{"INFO", "WARN", "ERROR"}, Derived: true,
		Why: "How much Debezium Server logs. WARN and ERROR hide progress messages that help when something goes wrong."},
}

// FlinkPipelineSettings are Flink CDC source, destination and pipeline options a pipeline may
// tune; their keys carry the YAML block they go in (source., sink., pipeline.).
var FlinkPipelineSettings = []Setting{
	{Engine: FlinkCDC, Scope: ScopeSource, Stage: StageStreaming, Impact: ImpactRestart, Key: "source.connection.pool.size",
		Title: "Connection pool size", Type: "int", Default: "20", Min: "2", Max: "100", Connectors: []string{"mysql", "postgresql", "oracle"},
		Why: "Connections the readers share. Each one counts against the database's connection limit."},
	{Engine: FlinkCDC, Scope: ScopeSource, Stage: StageStreaming, Impact: ImpactRestart, Key: "source.connect.timeout",
		Title: "Connect timeout", Type: "duration", Default: "30 s", Min: "5s", Max: "5m", Connectors: []string{"mysql", "postgresql", "oracle"},
		Why: "How long a reader waits for the database when it connects."},
	{Engine: FlinkCDC, Scope: ScopeSource, Stage: StageStreaming, Impact: ImpactRestart, Key: "source.connect.max-retries",
		Title: "Connect attempts", Type: "int", Default: "3", Min: "0", Max: "100", Connectors: []string{"mysql", "postgresql", "oracle"},
		Why: "How often a reader tries to connect again before the job fails."},
	{Engine: FlinkCDC, Scope: ScopeSource, Stage: StageStreaming, Impact: ImpactRestart, Key: "source.scan.lsn-commit.checkpoints-num-delay",
		Title: "WAL release delay", Type: "int", Default: "3", Min: "0", Max: "100", Connectors: []string{"postgresql"},
		Why: "Saved positions to wait before telling PostgreSQL it may free WAL: restoring an older one still finds its WAL."},
	{Engine: FlinkCDC, Scope: ScopeSource, Stage: StageSnapshot, Impact: ImpactFirstCopy, Key: "source.scan.incremental.snapshot.unbounded-chunk-first.enabled",
		Title: "Copy the open-ended chunk first", Type: "bool", Default: "false", Connectors: []string{"mysql", "postgresql"}, Experimental: true,
		Why: "Copies the last, open-ended chunk of a table first, which can keep a reader from running out of memory on a large table."},
	{Engine: FlinkCDC, Scope: ScopeSink, Stage: StageDelivery, Impact: ImpactRestart, Key: "sink.sink.buffer-flush.max-bytes",
		Title: "Load batch bytes", Type: "int", Default: "157286400", Min: "1048576", Max: "10737418240", Connectors: []string{"starrocks"},
		Why: "Bytes buffered before a load into StarRocks; more makes fewer, bigger loads and needs TaskManager memory."},
	{Engine: FlinkCDC, Scope: ScopeSink, Stage: StageDelivery, Impact: ImpactRestart, Key: "sink.sink.io.thread-count",
		Title: "Load threads", Type: "int", Default: "2", Min: "1", Max: "64", Connectors: []string{"starrocks"},
		Why: "Loads into StarRocks that run at once from each writer."},
	{Engine: FlinkCDC, Scope: ScopeSink, Stage: StageDelivery, Impact: ImpactRestart, Key: "sink.table.properties.write.target-file-size-bytes",
		Title: "Target file size", Type: "int", Default: "536870912", Min: "1048576", Max: "2147483648", Connectors: []string{"iceberg"},
		Why: "Bytes Iceberg aims for in each data file of a table it creates. Smaller commits sooner; larger reads faster."},
	{Engine: FlinkCDC, Scope: ScopePipeline, Stage: StageEngine, Impact: ImpactRestart, Key: "pipeline.schema-operator.rpc-timeout",
		Title: "Schema change timeout", Type: "duration", Default: "3 min", Min: "30s", Max: "1h",
		Why: "How long a schema change may take to reach the destination before the job fails and restarts."},
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
