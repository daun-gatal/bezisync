package domain

import (
	"strconv"
	"strings"

	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// FlinkCDCSpec are the choices of a pipeline that runs on Flink CDC. Zero values mean the
// default, so a pipeline that never touched them dumps nothing ("flink_cdc" is omitted).
//
// On Flink CDC the spec's "keys" (table → columns) are the destination primary key picked for a
// table that has none (Pick key columns); Debezium reads them as message keys.
type FlinkCDCSpec struct {
	// SchemaChanges is what happens when a source table changes shape: "apply" (Flink CDC's
	// lenient mode: new and renamed columns are added, nothing is dropped), "ignore" or "stop".
	SchemaChanges string `json:"schema_changes"`
	// NewColumns is "hold" (a new source column is left out until someone reviews it) or "send".
	NewColumns string `json:"new_columns"`
	// FreshnessSeconds is how long a change may wait in a load batch (StarRocks, Doris).
	FreshnessSeconds int `json:"freshness_seconds"`
	// CommitSeconds is the checkpoint interval: how often the position is saved and, for Iceberg,
	// how often new data becomes visible.
	CommitSeconds int `json:"commit_seconds"`
	// FlinkConf are Flink settings chosen from the catalogue (engine/flinkcdc), key → value.
	FlinkConf map[string]string `json:"flink_conf"`
	// Cluster is the custom cluster (policies.resources "custom"). Dumped only when set.
	Cluster *FlinkCluster `json:"cluster"`
}

// Flink CDC choices and their defaults.
var (
	SchemaChangeModes   = []string{"apply", "ignore", "stop"}
	NewColumnModes      = []string{"hold", "send"}
	FreshnessChoices    = []int{2, 5, 30}
	CommitChoices       = []int{30, 60, 300}
	DefaultFlinkCDCSpec = FlinkCDCSpec{SchemaChanges: "apply", NewColumns: "hold", FreshnessSeconds: 5, CommitSeconds: 60}
)

// Empty reports a block with nothing chosen.
func (f FlinkCDCSpec) Empty() bool {
	return f.SchemaChanges == "" && f.NewColumns == "" && f.FreshnessSeconds == 0 && f.CommitSeconds == 0 && len(f.FlinkConf) == 0 && f.Cluster == nil
}

// Effective fills every unset choice with its default.
func (f FlinkCDCSpec) Effective() FlinkCDCSpec {
	out := f.clone()
	d := DefaultFlinkCDCSpec
	if out.SchemaChanges == "" {
		out.SchemaChanges = d.SchemaChanges
	}
	if out.NewColumns == "" {
		out.NewColumns = d.NewColumns
	}
	if out.FreshnessSeconds == 0 {
		out.FreshnessSeconds = d.FreshnessSeconds
	}
	if out.CommitSeconds == 0 {
		out.CommitSeconds = d.CommitSeconds
	}
	if out.FlinkConf == nil {
		out.FlinkConf = map[string]string{}
	}
	return out
}

func (f FlinkCDCSpec) clone() FlinkCDCSpec {
	out := f
	out.Cluster = f.Cluster.clone()
	if f.FlinkConf != nil {
		out.FlinkConf = make(map[string]string, len(f.FlinkConf))
		for k, v := range f.FlinkConf {
			out.FlinkConf[k] = v
		}
	}
	return out
}

func decodeFlinkCDC(v *val.V, loc val.Loc, raw any) FlinkCDCSpec {
	var out FlinkCDCSpec
	o := v.Model(loc, raw, "FlinkCDCSpec")
	if o.Failed {
		return out
	}
	if f := o.Get("schema_changes"); f.Present && !f.IsNull() {
		out.SchemaChanges, _ = v.Literal(f.Loc, f.Raw, SchemaChangeModes...)
	}
	if f := o.Get("new_columns"); f.Present && !f.IsNull() {
		out.NewColumns, _ = v.Literal(f.Loc, f.Raw, NewColumnModes...)
	}
	if f := o.Get("freshness_seconds"); f.Present && !f.IsNull() {
		out.FreshnessSeconds = intChoice(v, f.Loc, f.Raw, FreshnessChoices)
	}
	if f := o.Get("commit_seconds"); f.Present && !f.IsNull() {
		out.CommitSeconds = intChoice(v, f.Loc, f.Raw, CommitChoices)
	}
	if f := o.Get("flink_conf"); f.Present && !f.IsNull() {
		if m, ok := v.StrDict(f.Loc, f.Raw); ok && len(m) > 0 {
			out.FlinkConf = m
		}
	}
	if f := o.Get("cluster"); f.Present && !f.IsNull() {
		out.Cluster = decodeCluster(v, f.Loc, f.Raw)
	}
	o.Forbid()
	return out
}

// intChoice reads an integer that must be one of choices.
func intChoice(v *val.V, loc val.Loc, raw any, choices []int) int {
	n, ok := v.IntIn(loc, raw, val.Num{})
	if !ok {
		return 0
	}
	for _, c := range choices {
		if n == c {
			return n
		}
	}
	v.ValueError(loc, "must be one of "+joinInts(choices))
	return 0
}

func joinInts(l []int) string {
	parts := make([]string, len(l))
	for i, n := range l {
		parts[i] = strconv.Itoa(n)
	}
	return strings.Join(parts, ", ")
}

func dumpFlinkCDC(f FlinkCDCSpec) *om.Map {
	m := om.New()
	if f.SchemaChanges != "" {
		m.Set("schema_changes", f.SchemaChanges)
	}
	if f.NewColumns != "" {
		m.Set("new_columns", f.NewColumns)
	}
	if f.FreshnessSeconds != 0 {
		m.Set("freshness_seconds", f.FreshnessSeconds)
	}
	if f.CommitSeconds != 0 {
		m.Set("commit_seconds", f.CommitSeconds)
	}
	if len(f.FlinkConf) > 0 {
		conf := om.New()
		for _, k := range sortedStrKeys(f.FlinkConf) {
			conf.Set(k, f.FlinkConf[k])
		}
		m.Set("flink_conf", conf)
	}
	if f.Cluster != nil {
		m.Set("cluster", dumpCluster(f.Cluster))
	}
	return m
}
