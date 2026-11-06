package engine

import (
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Flink settings a Flink CDC pipeline may choose (spec flink_cdc.flink_conf). BeziSync sets
// everything it depends on itself (addresses, ports, TLS, state, memory, slots, parallelism);
// those keys are not in the catalogue and can never be chosen. Every key here is safe to change
// on a deployed pipeline: none changes what a saved position holds.

// FlinkSetting is one key of the Flink settings catalogue (a Setting of the registry).
type FlinkSetting = Setting

// FlinkSettings is the catalogue, in the order the UI lists it.
var FlinkSettings = []FlinkSetting{
	{Engine: FlinkCDC, Scope: ScopeEngine, Stage: StageEngine, Impact: ImpactRestart, Key: "execution.checkpointing.timeout", Title: "Save timeout", Type: "duration", Default: "10 min", Min: "1m", Max: "1h",
		Why: "How long saving the position may take before that attempt is abandoned."},
	{Engine: FlinkCDC, Scope: ScopeEngine, Stage: StageEngine, Impact: ImpactRestart, Key: "execution.checkpointing.tolerable-failed-checkpoints", Title: "Failed saves tolerated", Type: "int", Default: "3", Min: "0", Max: "100",
		Why: "How many saves in a row may fail before the job restarts."},
	{Engine: FlinkCDC, Scope: ScopeEngine, Stage: StageEngine, Impact: ImpactRestart, Key: "execution.checkpointing.min-pause", Title: "Pause between saves", Type: "duration", Default: "0 ms", Min: "0s", Max: "10m",
		Why: "The least time between two saves, so a slow destination still gets time to write."},
	{Engine: FlinkCDC, Scope: ScopeEngine, Stage: StageEngine, Impact: ImpactRestart, Key: "restart-strategy.type", Title: "Restart strategy", Type: "enum", Default: "exponential-delay",
		Choices: []string{"exponential-delay", "fixed-delay", "failure-rate"}, Why: "How the job restarts after a failure."},
	{Engine: FlinkCDC, Scope: ScopeEngine, Stage: StageEngine, Impact: ImpactRestart, Key: "restart-strategy.exponential-delay.initial-backoff", Title: "First restart delay", Type: "duration", Default: "1 s", Min: "1s", Max: "10m",
		Why: "Exponential delay: the wait before the first restart."},
	{Engine: FlinkCDC, Scope: ScopeEngine, Stage: StageEngine, Impact: ImpactRestart, Key: "restart-strategy.exponential-delay.max-backoff", Title: "Longest restart delay", Type: "duration", Default: "1 min", Min: "10s", Max: "1h",
		Why: "Exponential delay: the longest wait between restarts."},
	{Engine: FlinkCDC, Scope: ScopeEngine, Stage: StageEngine, Impact: ImpactRestart, Key: "restart-strategy.fixed-delay.attempts", Title: "Restart attempts", Type: "int", Default: "1", Min: "1", Max: "1000",
		Why: "Fixed delay: restarts before the job fails for good."},
	{Engine: FlinkCDC, Scope: ScopeEngine, Stage: StageEngine, Impact: ImpactRestart, Key: "restart-strategy.fixed-delay.delay", Title: "Restart delay", Type: "duration", Default: "1 s", Min: "1s", Max: "1h",
		Why: "Fixed delay: the wait between restarts."},
	{Engine: FlinkCDC, Scope: ScopeEngine, Stage: StageEngine, Impact: ImpactRestart, Key: "taskmanager.memory.managed.fraction", Title: "Managed memory", Type: "fraction", Default: "0.05", Min: "0", Max: "0.9",
		Why: "The share of TaskManager memory Flink keeps off the heap for RocksDB and batch work. A pipeline keeps its state on the heap, so little is needed."},
	{Engine: FlinkCDC, Scope: ScopeEngine, Stage: StageEngine, Impact: ImpactRestart, Key: "taskmanager.memory.network.fraction", Title: "Network memory", Type: "fraction", Default: "0.1", Min: "0.05", Max: "0.5",
		Why: "The share of TaskManager memory for buffers between operators."},
	{Engine: FlinkCDC, Scope: ScopeEngine, Stage: StageEngine, Impact: ImpactRestart, Key: "pekko.ask.timeout", Title: "Internal call timeout", Type: "duration", Default: "10 s", Min: "10s", Max: "10m",
		Why: "How long the JobManager and TaskManagers wait for each other's answers."},
	{Engine: FlinkCDC, Scope: ScopeEngine, Stage: StageEngine, Impact: ImpactRestart, Key: "heartbeat.timeout", Title: "Heartbeat timeout", Type: "duration", Default: "50 s", Min: "10s", Max: "10m",
		Why: "How long without a heartbeat before a TaskManager counts as lost."},
}

// FlinkDefaults are catalogue keys BeziSync sets unless a pipeline chooses them. The heap state
// backend never uses managed memory: Flink's 40% would leave a 2 GB TaskManager about 700 MB of
// heap, which a large first copy runs out of.
var FlinkDefaults = map[string]string{"taskmanager.memory.managed.fraction": "0.05"}

var flinkDuration = regexp.MustCompile(`^(\d{1,7})\s*(ms|s|min)$`)

// ParseFlinkDuration reads a duration as the catalogue writes it ("30 s", "10min", "500 ms").
func ParseFlinkDuration(v string) (time.Duration, bool) {
	m := flinkDuration.FindStringSubmatch(strings.TrimSpace(v))
	if m == nil {
		return 0, false
	}
	n, _ := strconv.Atoi(m[1])
	unit := map[string]time.Duration{"ms": time.Millisecond, "s": time.Second, "min": time.Minute}[m[2]]
	return time.Duration(n) * unit, true
}

// FlinkSettingOf is the catalogue entry of key, or nil.
func FlinkSettingOf(key string) *FlinkSetting {
	for i := range FlinkSettings {
		if FlinkSettings[i].Key == key {
			return &FlinkSettings[i]
		}
	}
	return nil
}

// CheckFlinkSetting checks one value against its catalogue entry ("" when valid).
func CheckFlinkSetting(key, value string) string {
	set := FlinkSettingOf(key)
	if set == nil {
		return "is not a Flink setting BeziSync lets pipelines change"
	}
	return CheckSetting(*set, value)
}

// CheckFlinkSettings checks a pipeline's Flink settings: key → problem, empty when all are valid.
func CheckFlinkSettings(conf map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range conf {
		if msg := CheckFlinkSetting(k, v); msg != "" {
			out[k] = msg
		}
	}
	return out
}

// SortedKeys is conf's keys in order (for stable errors and output).
func SortedKeys(conf map[string]string) []string {
	keys := make([]string, 0, len(conf))
	for k := range conf {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// FlinkSize is a size preset: one JobManager and TaskManagers with two slots each. Parallelism
// is TaskManagers × slots, fixed once deployed (a saved position holds state per reader).
type FlinkSize struct {
	JobManagerCPU, JobManagerMemory   string
	TaskManagers                      int
	TaskManagerCPU, TaskManagerMemory string
}

// FlinkSlots is the task slots of each TaskManager.
const FlinkSlots = 2

// FlinkSizes are the presets, by the spec's policies.resources.
var FlinkSizes = map[string]FlinkSize{
	"small":  {"500m", "1Gi", 1, "1", "2Gi"},
	"medium": {"500m", "1Gi", 2, "1", "2Gi"},
	"large":  {"1", "1536Mi", 4, "1", "2Gi"},
}

// Parallelism is how many readers and writers the size runs.
func (s FlinkSize) Parallelism() int { return s.TaskManagers * FlinkSlots }

// TotalMemoryMiB is what the whole cluster asks for.
func (s FlinkSize) TotalMemoryMiB() int {
	return mib(s.JobManagerMemory) + s.TaskManagers*mib(s.TaskManagerMemory)
}

func mib(v string) int {
	switch {
	case strings.HasSuffix(v, "Gi"):
		n, _ := strconv.Atoi(strings.TrimSuffix(v, "Gi"))
		return n * 1024
	case strings.HasSuffix(v, "Mi"):
		n, _ := strconv.Atoi(strings.TrimSuffix(v, "Mi"))
		return n
	}
	return 0
}
