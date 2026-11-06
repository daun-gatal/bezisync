package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// The pipeline spec: the config-as-code contract and the API body for pipeline changes.

var (
	tableRe       = regexp.MustCompile(`^[A-Za-z0-9_$\-]+\.[A-Za-z0-9_$\-]+$`)
	placeholderRe = regexp.MustCompile(`\{[^}]*\}`)
	namingCharsRe = regexp.MustCompile(`^[A-Za-z0-9._\-{}]+$`)
	placeholders  = map[string]bool{"{schema}": true, "{table}": true, "{pipeline}": true}
)

// Snapshot modes and their plain-language help.
var SnapshotModes = []string{"initial", "no_data", "when_needed", "initial_only", "always"}

var SnapshotModeHelp = map[string]string{
	"initial":      "Copy existing rows first, then stream changes.",
	"no_data":      "Stream new changes only; skip existing rows.",
	"when_needed":  "Copy existing rows only if the stored position is missing or invalid.",
	"initial_only": "Copy existing rows once, then stop (no streaming).",
	"always":       "Copy existing rows every time the pipeline starts.",
}

type ColumnRules struct {
	Mask    []string `json:"mask"`
	Exclude []string `json:"exclude"`
	// Keep lists columns someone reviewed and chose to send as they are (personal data the
	// Tables step flagged); it changes nothing in the connector and silences the
	// pii_unprotected warning. Added after the Python releases: dumped only when set.
	Keep []string `json:"keep,omitempty"`
}

type Tables struct {
	Include []string `json:"include"`
}

type SourceSpec struct {
	Tables  Tables                 `json:"tables"`
	Columns map[string]ColumnRules `json:"columns"`
}

type FormatSpec struct {
	Type          string  `json:"type"`
	IncludeSchema bool    `json:"include_schema"`
	RegistryURL   *string `json:"registry_url"`
	RegistryKind  string  `json:"registry_kind"`
}

type SinkSpec struct {
	Naming  string         `json:"naming"`
	Format  FormatSpec     `json:"format"`
	Options map[string]any `json:"options"`
}

type SnapshotSpec struct {
	Mode         string  `json:"mode"`
	ChunkSize    int     `json:"chunk_size"`
	FetchSize    int     `json:"fetch_size"`
	DelaySeconds int     `json:"delay_seconds"`
	Window       *string `json:"window"`
}

type Transform struct {
	Type           string   `json:"type"`
	DeleteHandling string   `json:"delete_handling"`
	AddFields      []string `json:"add_fields"`
	Condition      *string  `json:"condition"`
	// Added after the Python releases (see transforms.go); zero values are not dumped.
	ID         string            `json:"id"`
	Tables     []string          `json:"tables"`     // nil = every table
	Operations []string          `json:"operations"` // filter
	Match      string            `json:"match"`      // filter: all | any
	Rules      []FilterRule      `json:"rules"`      // filter
	Renames    []Rename          `json:"renames"`    // rename
	Name       string            `json:"name"`       // add_field
	Value      FieldValue        `json:"value"`      // add_field
	Delimiter  string            `json:"delimiter"`  // flatten
	Class      string            `json:"class"`      // custom
	Properties map[string]string `json:"properties"` // custom
}

type ErrorPolicy struct {
	Retries       int    `json:"retries"`
	OnPoisonEvent string `json:"on_poison_event"`
}

type Policies struct {
	HeartbeatSeconds      int          `json:"heartbeat_seconds"`
	Resources             string       `json:"resources"`
	Errors                ErrorPolicy  `json:"errors"`
	LatencyTargetSeconds  int          `json:"latency_target_seconds"`
	SourceRetainedLimitGB pyjson.Float `json:"source_retained_limit_gb"`
	DeliveryCanary        bool         `json:"delivery_canary"`
	// Custom is the size when Resources is "custom" (Debezium only). Dumped only when set.
	Custom *CustomResources `json:"custom_resources"`
}

// tuningKeyRe is a setting name: lower-case ASCII parts separated by dots (registry keys only;
// which ones a pipeline may use is checked against engine.SettingsFor).
var tuningKeyRe = regexp.MustCompile(`^[a-z][a-zA-Z0-9_-]*(\.[a-zA-Z0-9_-]+)+$`)

// PipelineSpec is the full spec with every default filled in.
type PipelineSpec struct {
	Source        SourceSpec        `json:"source"`
	Sink          SinkSpec          `json:"sink"`
	Snapshot      SnapshotSpec      `json:"snapshot"`
	Transforms    []Transform       `json:"transforms"`
	Policies      Policies          `json:"policies"`
	DebeziumImage *string           `json:"debezium_image"`
	Advanced      map[string]string `json:"advanced"`
	// Added after the Python releases; dumped only when set.
	Values ValueFormats        `json:"values"`
	Keys   map[string][]string `json:"keys"` // table → message key columns (Flink CDC: destination primary key)
	// FlinkCDC are the choices of a pipeline that runs on Flink CDC; FlinkCDCImage pins its image
	// like DebeziumImage does for Debezium. Both are dumped only when set.
	FlinkCDC      FlinkCDCSpec `json:"flink_cdc"`
	FlinkCDCImage *string      `json:"flink_cdc_image"`
	// Tuning are typed settings from the registry (engine.SettingsFor), key → canonical value.
	// Dumped only when set.
	Tuning map[string]string `json:"tuning"`
}

// DefaultTransform is Transform(type=...) with defaults.
func DefaultTransform(typ string) Transform {
	return Transform{Type: typ, DeleteHandling: "rewrite", AddFields: []string{"op", "source.ts_ms"}}
}

// DefaultSpec is PipelineSpec() with every default.
func DefaultSpec() PipelineSpec {
	return PipelineSpec{
		Source: SourceSpec{Tables: Tables{Include: []string{}}, Columns: map[string]ColumnRules{}},
		Sink: SinkSpec{
			Naming:  "cdc.{schema}.{table}",
			Format:  FormatSpec{Type: "json", RegistryKind: "apicurio"},
			Options: map[string]any{},
		},
		Snapshot:   SnapshotSpec{Mode: "initial", ChunkSize: 1024, FetchSize: 10_000},
		Transforms: []Transform{DefaultTransform("unwrap")},
		Policies: Policies{
			HeartbeatSeconds: 10, Resources: "small", Errors: ErrorPolicy{Retries: -1, OnPoisonEvent: "fail"},
			LatencyTargetSeconds: 60, SourceRetainedLimitGB: 10,
		},
		Advanced: map[string]string{},
	}
}

func cloneKeep(l []string) []string {
	if len(l) == 0 {
		return nil
	}
	return append([]string{}, l...)
}

// Clone deep-copies the spec.
func (s PipelineSpec) Clone() PipelineSpec {
	out := s
	out.Source.Tables.Include = append([]string{}, s.Source.Tables.Include...)
	out.Source.Columns = make(map[string]ColumnRules, len(s.Source.Columns))
	for k, v := range s.Source.Columns {
		out.Source.Columns[k] = ColumnRules{Mask: append([]string{}, v.Mask...), Exclude: append([]string{}, v.Exclude...), Keep: cloneKeep(v.Keep)}
	}
	out.Sink.Options = cloneAny(s.Sink.Options).(map[string]any)
	if s.Policies.Custom != nil {
		c := *s.Policies.Custom
		out.Policies.Custom = &c
	}
	out.Transforms = make([]Transform, len(s.Transforms))
	for i, t := range s.Transforms {
		out.Transforms[i] = cloneTransform(t)
	}
	if s.Keys != nil {
		out.Keys = make(map[string][]string, len(s.Keys))
		for k, v := range s.Keys {
			out.Keys[k] = append([]string{}, v...)
		}
	}
	out.Advanced = make(map[string]string, len(s.Advanced))
	for k, v := range s.Advanced {
		out.Advanced[k] = v
	}
	if s.Tuning != nil {
		out.Tuning = make(map[string]string, len(s.Tuning))
		for k, v := range s.Tuning {
			out.Tuning[k] = v
		}
	}
	out.FlinkCDC = s.FlinkCDC.clone()
	if s.FlinkCDCImage != nil {
		image := *s.FlinkCDCImage
		out.FlinkCDCImage = &image
	}
	return out
}

func cloneAny(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = cloneAny(e)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = cloneAny(e)
		}
		return out
	case nil:
		return map[string]any{}
	}
	return v
}

// Equal is Pydantic model equality (same field values).
func (s PipelineSpec) Equal(o PipelineSpec) bool {
	a, _ := pyjson.DumpsCompact(s.Dump().Plain())
	b, _ := pyjson.DumpsCompact(o.Dump().Plain())
	return a == b
}

// Dump is model_dump(mode="json") in declaration order.
func (s PipelineSpec) Dump() *om.Map {
	columns := om.New()
	for _, t := range sortedColumnKeys(s.Source.Columns) {
		r := s.Source.Columns[t]
		rules := om.New().Set("mask", strList(r.Mask)).Set("exclude", strList(r.Exclude))
		if len(r.Keep) > 0 {
			rules.Set("keep", strList(r.Keep))
		}
		columns.Set(t, rules)
	}
	transforms := make([]any, len(s.Transforms))
	for i, t := range s.Transforms {
		transforms[i] = dumpTransform(t)
	}
	advanced := om.New()
	for _, k := range sortedStrKeys(s.Advanced) {
		advanced.Set(k, s.Advanced[k])
	}
	var options = om.FromJSONB(s.Sink.Options)
	if s.Sink.Options == nil {
		options = om.New()
	}
	out := om.New().
		Set("source", om.New().
			Set("tables", om.New().Set("include", strList(s.Source.Tables.Include))).
			Set("columns", columns)).
		Set("sink", om.New().
			Set("naming", s.Sink.Naming).
			Set("format", om.New().Set("type", s.Sink.Format.Type).Set("include_schema", s.Sink.Format.IncludeSchema).
				Set("registry_url", strPtr(s.Sink.Format.RegistryURL)).Set("registry_kind", s.Sink.Format.RegistryKind)).
			Set("options", options)).
		Set("snapshot", om.New().Set("mode", s.Snapshot.Mode).Set("chunk_size", s.Snapshot.ChunkSize).
			Set("fetch_size", s.Snapshot.FetchSize).Set("delay_seconds", s.Snapshot.DelaySeconds).
			Set("window", strPtr(s.Snapshot.Window))).
		Set("transforms", transforms).
		Set("policies", om.New().Set("heartbeat_seconds", s.Policies.HeartbeatSeconds).
			Set("resources", s.Policies.Resources).
			Set("errors", om.New().Set("retries", s.Policies.Errors.Retries).Set("on_poison_event", s.Policies.Errors.OnPoisonEvent)).
			Set("latency_target_seconds", s.Policies.LatencyTargetSeconds).
			Set("source_retained_limit_gb", s.Policies.SourceRetainedLimitGB).
			Set("delivery_canary", s.Policies.DeliveryCanary)).
		Set("debezium_image", strPtr(s.DebeziumImage)).
		Set("advanced", advanced)
	if s.Policies.Custom != nil {
		if p, ok := out.Get("policies"); ok {
			p.(*om.Map).Set("custom_resources", om.New().Set("cpu", s.Policies.Custom.CPU).Set("memory", s.Policies.Custom.Memory))
		}
	}
	if !s.Values.Empty() {
		out.Set("values", dumpValues(s.Values))
	}
	if len(s.Keys) > 0 {
		out.Set("keys", dumpKeys(s.Keys))
	}
	if !s.FlinkCDC.Empty() {
		out.Set("flink_cdc", dumpFlinkCDC(s.FlinkCDC))
	}
	if s.FlinkCDCImage != nil {
		out.Set("flink_cdc_image", *s.FlinkCDCImage)
	}
	if len(s.Tuning) > 0 {
		tuning := om.New()
		for _, k := range sortedStrKeys(s.Tuning) {
			tuning.Set(k, s.Tuning[k])
		}
		out.Set("tuning", tuning)
	}
	return out
}

// MarshalJSON renders the spec in declaration order (API responses, stored versions).
func (s PipelineSpec) MarshalJSON() ([]byte, error) {
	return s.Dump().MarshalJSON()
}

// Dict keys are dumped in JSONB order: Python saw every stored spec's dicts in the order
// PostgreSQL returns them (hashes sort keys anyway).
// ColumnKeys lists column-rule tables in JSONB order (the order Python iterated them).
func ColumnKeys(m map[string]ColumnRules) []string { return sortedColumnKeys(m) }

func sortedColumnKeys(m map[string]ColumnRules) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return om.JSONBKeys(out)
}

func sortedStrKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return om.JSONBKeys(out)
}

func strList(s []string) []any {
	out := make([]any, len(s))
	for i, v := range s {
		out[i] = v
	}
	return out
}

func strPtr(p *string) any {
	if p == nil {
		return nil
	}
	return *p
}

// SpecHash is sha256 of the canonical JSON (sorted keys, compact separators).
func SpecHash(s PipelineSpec) string {
	canonical, err := pyjson.DumpsCompact(s.Dump().Plain())
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}

// ParseSpec validates a stored or submitted spec (generic JSON) at loc.
func ParseSpec(v *val.V, loc val.Loc, raw any) (PipelineSpec, bool) {
	before := v.Count()
	spec := decodeSpec(v, loc, raw)
	return spec, v.Count() == before
}

// MustSpec parses a spec that is known to be valid (stored versions).
func MustSpec(raw any) PipelineSpec {
	v := &val.V{}
	spec, ok := ParseSpec(v, nil, raw)
	if !ok {
		panic(fmt.Sprintf("stored spec is invalid: %v", v.Errors))
	}
	return spec
}

func decodeSpec(v *val.V, loc val.Loc, raw any) PipelineSpec {
	spec := DefaultSpec()
	o := v.Model(loc, raw, "PipelineSpec")
	if o.Failed {
		return spec
	}
	start := v.Count()
	if f := o.Get("source"); f.Present {
		spec.Source = decodeSource(v, f.Loc, f.Raw)
	}
	if f := o.Get("sink"); f.Present {
		spec.Sink = decodeSink(v, f.Loc, f.Raw)
	}
	if f := o.Get("snapshot"); f.Present {
		spec.Snapshot = decodeSnapshot(v, f.Loc, f.Raw)
	}
	if f := o.Get("transforms"); f.Present {
		if items, ok := v.List(f.Loc, f.Raw); ok {
			spec.Transforms = make([]Transform, 0, len(items))
			for i, item := range items {
				spec.Transforms = append(spec.Transforms, decodeTransform(v, f.Loc.Add(i), item))
			}
		}
	}
	if f := o.Get("policies"); f.Present {
		spec.Policies = decodePolicies(v, f.Loc, f.Raw)
	}
	if f := o.Get("debezium_image"); f.Present && !f.IsNull() {
		if s, ok := v.Constrained(f.Loc, f.Raw, val.StrRules{Max: 300, Pattern: `^[\w.\-/:@]+$`}); ok {
			spec.DebeziumImage = &s
		}
	}
	if f := o.Get("advanced"); f.Present {
		if m, ok := v.StrDict(f.Loc, f.Raw); ok {
			spec.Advanced = m
		}
	}
	if f := o.Get("tuning"); f.Present && !f.IsNull() {
		if m, ok := v.StrDict(f.Loc, f.Raw); ok {
			for _, k := range sortedStrKeys(m) {
				if len(k) > 255 || !tuningKeyRe.MatchString(k) {
					v.Fail(f.Loc.Add(k), "value_error", "Value error, not a setting name")
				} else if len(m[k]) > 256 || strings.ContainsAny(m[k], "\r\n\t\x00") || strings.Contains(m[k], "${") {
					v.Fail(f.Loc.Add(k), "value_error", "Value error, a setting's value is at most 256 characters, on one line, without ${…}")
				}
			}
			if len(m) > 0 {
				spec.Tuning = m
			}
		}
	}
	if f := o.Get("values"); f.Present && !f.IsNull() {
		spec.Values = decodeValues(v, f.Loc, f.Raw)
	}
	if f := o.Get("keys"); f.Present && !f.IsNull() {
		if keys := decodeKeys(v, f.Loc, f.Raw); len(keys) > 0 {
			spec.Keys = keys
		}
	}
	if f := o.Get("flink_cdc"); f.Present && !f.IsNull() {
		spec.FlinkCDC = decodeFlinkCDC(v, f.Loc, f.Raw)
	}
	if f := o.Get("flink_cdc_image"); f.Present && !f.IsNull() {
		if s, ok := v.Constrained(f.Loc, f.Raw, val.StrRules{Max: 300, Pattern: `^[\w.\-/:@]+$`}); ok {
			spec.FlinkCDCImage = &s
		}
	}
	o.Forbid()
	if v.Count() == start {
		validateTransforms(v, loc.Add("transforms"), spec)
		validateKeys(v, loc.Add("keys"), spec)
	}
	if v.Count() == start {
		var unknown []string
		for t := range spec.Source.Columns {
			if !contains(spec.Source.Tables.Include, t) {
				unknown = append(unknown, t)
			}
		}
		if len(unknown) > 0 {
			sort.Strings(unknown)
			v.ValueError(loc, "column rules for tables that are not captured: "+strings.Join(unknown, ", "))
		}
	}
	return spec
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func decodeSource(v *val.V, loc val.Loc, raw any) SourceSpec {
	out := SourceSpec{Tables: Tables{Include: []string{}}, Columns: map[string]ColumnRules{}}
	o := v.Model(loc, raw, "SourceSpec")
	if o.Failed {
		return out
	}
	if f := o.Get("tables"); f.Present {
		t := v.Model(f.Loc, f.Raw, "Tables")
		if !t.Failed {
			if inc := t.Get("include"); inc.Present {
				if list, ok := v.StrList(inc.Loc, inc.Raw); ok {
					var bad []string
					for _, s := range list {
						if !tableRe.MatchString(s) {
							bad = append(bad, s)
						}
					}
					seen := map[string]bool{}
					dup := false
					for _, s := range list {
						if seen[s] {
							dup = true
						}
						seen[s] = true
					}
					switch {
					case len(bad) > 0:
						if len(bad) > 5 {
							bad = bad[:5]
						}
						v.ValueError(inc.Loc, "tables must look like schema.table: "+strings.Join(bad, ", "))
					case dup:
						v.ValueError(inc.Loc, "duplicate tables")
					default:
						sorted := append([]string{}, list...)
						sort.Strings(sorted)
						out.Tables.Include = sorted
					}
				}
			}
			t.Forbid()
		}
	}
	if f := o.Get("columns"); f.Present {
		if m, ok := v.Dict(f.Loc, f.Raw); ok {
			for _, k := range sortedAnyKeys(m) {
				rl := f.Loc.Add(k)
				r := v.Model(rl, m[k], "ColumnRules")
				rules := ColumnRules{Mask: []string{}, Exclude: []string{}}
				if !r.Failed {
					if g := r.Get("mask"); g.Present {
						if l, ok := v.StrList(g.Loc, g.Raw); ok {
							rules.Mask = l
						}
					}
					if g := r.Get("exclude"); g.Present {
						if l, ok := v.StrList(g.Loc, g.Raw); ok {
							rules.Exclude = l
						}
					}
					if g := r.Get("keep"); g.Present && !g.IsNull() {
						if l, ok := v.StrList(g.Loc, g.Raw); ok && len(l) > 0 {
							rules.Keep = l
						}
					}
					r.Forbid()
				}
				out.Columns[k] = rules
			}
		}
	}
	o.Forbid()
	return out
}

func sortedAnyKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func decodeSink(v *val.V, loc val.Loc, raw any) SinkSpec {
	out := DefaultSpec().Sink
	o := v.Model(loc, raw, "SinkSpec")
	if o.Failed {
		return out
	}
	if f := o.Get("naming"); f.Present {
		if s, ok := v.Str(f.Loc, f.Raw); ok {
			if msg := namingError(s); msg != "" {
				v.ValueError(f.Loc, msg)
			} else {
				out.Naming = s
			}
		}
	}
	if f := o.Get("format"); f.Present {
		fo := v.Model(f.Loc, f.Raw, "FormatSpec")
		if !fo.Failed {
			if g := fo.Get("type"); g.Present {
				if s, ok := v.Literal(g.Loc, g.Raw, "json", "avro", "protobuf", "cloudevents"); ok {
					out.Format.Type = s
				}
			}
			if g := fo.Get("include_schema"); g.Present {
				if b, ok := v.Bool(g.Loc, g.Raw); ok {
					out.Format.IncludeSchema = b
				}
			}
			if g := fo.Get("registry_url"); g.Present && !g.IsNull() {
				if s, ok := v.Str(g.Loc, g.Raw); ok {
					out.Format.RegistryURL = &s
				}
			}
			if g := fo.Get("registry_kind"); g.Present {
				if s, ok := v.Literal(g.Loc, g.Raw, "apicurio", "confluent"); ok {
					out.Format.RegistryKind = s
				}
			}
			fo.Forbid()
		}
	}
	if f := o.Get("options"); f.Present {
		if m, ok := v.Dict(f.Loc, f.Raw); ok {
			out.Options = m
		}
	}
	o.Forbid()
	return out
}

func namingError(value string) string {
	if !strings.Contains(value, "{table}") {
		return "naming must contain {table}"
	}
	var unknown []string
	seen := map[string]bool{}
	for _, p := range placeholderRe.FindAllString(value, -1) {
		if !placeholders[p] && !seen[p] {
			unknown = append(unknown, p)
			seen[p] = true
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return "unknown placeholders: " + strings.Join(unknown, ", ")
	}
	if !namingCharsRe.MatchString(value) {
		return "use letters, digits, '.', '_', '-' and placeholders only"
	}
	return ""
}

func decodeSnapshot(v *val.V, loc val.Loc, raw any) SnapshotSpec {
	out := DefaultSpec().Snapshot
	o := v.Model(loc, raw, "SnapshotSpec")
	if o.Failed {
		return out
	}
	if f := o.Get("mode"); f.Present {
		if s, ok := v.Literal(f.Loc, f.Raw, SnapshotModes...); ok {
			out.Mode = s
		}
	}
	if f := o.Get("chunk_size"); f.Present {
		if n, ok := v.IntIn(f.Loc, f.Raw, val.Num{GE: val.F(64), LE: val.F(1_000_000)}); ok {
			out.ChunkSize = n
		}
	}
	if f := o.Get("fetch_size"); f.Present {
		if n, ok := v.IntIn(f.Loc, f.Raw, val.Num{GE: val.F(100), LE: val.F(1_000_000)}); ok {
			out.FetchSize = n
		}
	}
	if f := o.Get("delay_seconds"); f.Present {
		if n, ok := v.IntIn(f.Loc, f.Raw, val.Num{GE: val.F(0), LE: val.F(3600)}); ok {
			out.DelaySeconds = n
		}
	}
	if f := o.Get("window"); f.Present && !f.IsNull() {
		if s, ok := v.Constrained(f.Loc, f.Raw, val.StrRules{Pattern: `^\d{2}:\d{2}-\d{2}:\d{2}$`}); ok {
			out.Window = &s
		}
	}
	o.Forbid()
	return out
}

func decodeTransform(v *val.V, loc val.Loc, raw any) Transform {
	out := DefaultTransform("")
	o := v.Model(loc, raw, "Transform")
	if o.Failed {
		return out
	}
	if f := o.Get("type"); f.Required() {
		if s, ok := v.Literal(f.Loc, f.Raw, TransformTypes...); ok {
			out.Type = s
		}
	}
	if !out.Legacy() && out.Type != "" {
		for _, k := range []string{"delete_handling", "add_fields", "condition"} {
			if f := o.Get(k); f.Present {
				v.ValueError(f.Loc, "not used by "+out.Type+" steps")
			}
		}
	}
	if f := o.Get("delete_handling"); f.Present {
		if s, ok := v.Literal(f.Loc, f.Raw, "rewrite", "tombstone", "drop"); ok {
			out.DeleteHandling = s
		}
	}
	if f := o.Get("add_fields"); f.Present {
		if l, ok := v.StrList(f.Loc, f.Raw); ok {
			out.AddFields = l
		}
	}
	if f := o.Get("condition"); f.Present && !f.IsNull() {
		if s, ok := v.Str(f.Loc, f.Raw); ok {
			out.Condition = &s
		}
	}
	decodeTransformExtras(v, o, &out)
	o.Forbid()
	return out
}

func decodePolicies(v *val.V, loc val.Loc, raw any) Policies {
	out := DefaultSpec().Policies
	o := v.Model(loc, raw, "Policies")
	if o.Failed {
		return out
	}
	if f := o.Get("heartbeat_seconds"); f.Present {
		if n, ok := v.IntIn(f.Loc, f.Raw, val.Num{GE: val.F(0), LE: val.F(3600)}); ok {
			out.HeartbeatSeconds = n
		}
	}
	if f := o.Get("resources"); f.Present {
		if s, ok := v.Literal(f.Loc, f.Raw, "small", "medium", "large", "custom"); ok {
			out.Resources = s
		}
	}
	if f := o.Get("custom_resources"); f.Present && !f.IsNull() {
		c := v.Model(f.Loc, f.Raw, "CustomResources")
		if !c.Failed {
			custom := CustomResources{}
			if g := c.Get("cpu"); !g.Present {
				v.Fail(g.Loc, "missing", "Field required")
			} else if s, ok := v.Str(g.Loc, g.Raw); ok {
				if _, good := ParseCPU(s); !good {
					v.Fail(g.Loc, "value_error", "Value error, CPU is 0.25 to 8 in quarters, like 1.5")
				}
				custom.CPU = s
			}
			if g := c.Get("memory"); !g.Present {
				v.Fail(g.Loc, "missing", "Field required")
			} else if s, ok := v.Str(g.Loc, g.Raw); ok {
				if _, good := ParseMemory(s); !good {
					v.Fail(g.Loc, "value_error", "Value error, memory is 512Mi to 16Gi in steps of 256Mi, whole GiB written as Gi (2Gi)")
				}
				custom.Memory = s
			}
			c.Forbid()
			out.Custom = &custom
		}
	}
	if (out.Resources == "custom") != (out.Custom != nil) {
		v.Fail(o.Loc().Add("custom_resources"), "value_error", "Value error, a custom size (resources: custom) needs custom_resources, and only it")
	}
	if f := o.Get("errors"); f.Present {
		e := v.Model(f.Loc, f.Raw, "ErrorPolicy")
		if !e.Failed {
			if g := e.Get("retries"); g.Present {
				if n, ok := v.IntIn(g.Loc, g.Raw, val.Num{GE: val.F(-1), LE: val.F(1000)}); ok {
					out.Errors.Retries = n
				}
			}
			if g := e.Get("on_poison_event"); g.Present {
				if s, ok := v.Literal(g.Loc, g.Raw, "fail", "warn", "skip"); ok {
					out.Errors.OnPoisonEvent = s
				}
			}
			e.Forbid()
		}
	}
	if f := o.Get("latency_target_seconds"); f.Present {
		if n, ok := v.IntIn(f.Loc, f.Raw, val.Num{GE: val.F(1), LE: val.F(86_400)}); ok {
			out.LatencyTargetSeconds = n
		}
	}
	if f := o.Get("source_retained_limit_gb"); f.Present {
		if x, ok := v.FloatIn(f.Loc, f.Raw, val.Num{GT: val.F(0), LE: val.F(10_000)}); ok {
			out.SourceRetainedLimitGB = pyjson.Float(x)
		}
	}
	if f := o.Get("delivery_canary"); f.Present {
		if b, ok := v.Bool(f.Loc, f.Raw); ok {
			out.DeliveryCanary = b
		}
	}
	o.Forbid()
	return out
}

// ChangeSummary explains what deploying a spec over another will do.
type ChangeSummary struct {
	TablesAdded         []string `json:"tables_added"`
	TablesRemoved       []string `json:"tables_removed"`
	SnapshotModeChanged bool     `json:"snapshot_mode_changed"`
	NamingChanged       bool     `json:"naming_changed"`
	FormatChanged       bool     `json:"format_changed"`
	RestartsConnector   bool     `json:"restarts_connector"`
	Notes               []string `json:"notes"`
}

// SettingsChange is what a deploy changes in a pipeline's settings: the typed settings, Flink
// settings and advanced properties that differ, each with what it does to a running pipeline, and
// the heaviest of those. Reported next to ChangeSummary (whose shape is the recorded contract).
type SettingsChange struct {
	Settings []SettingChange `json:"settings"`
	Impact   engine.Impact   `json:"impact"`
}

// SummarizeSettings is SettingsChange between two specs (nil when nothing differs).
func SummarizeSettings(old *PipelineSpec, new PipelineSpec) *SettingsChange {
	before := PipelineSpec{}
	if old != nil {
		before = *old
	}
	changes := SettingChanges(before, new)
	if len(changes) == 0 {
		return nil
	}
	return &SettingsChange{Settings: changes, Impact: HeaviestImpact(changes)}
}

// SettingChange is one setting a deploy changes. From or To is nil when the setting is unset
// (its default applies).
type SettingChange struct {
	Key    string        `json:"key"`
	Title  string        `json:"title"`
	From   *string       `json:"from"`
	To     *string       `json:"to"`
	Impact engine.Impact `json:"impact"`
	// Unverified: an advanced property, passed as typed; BeziSync can't say what it does beyond
	// the restart.
	Unverified bool `json:"unverified,omitempty"`
}

// SettingChanges lists what changes between two specs' settings, sorted by key within each kind:
// typed settings, then Flink settings, then advanced properties.
func SettingChanges(old, new PipelineSpec) []SettingChange {
	var out []SettingChange
	diff := func(a, b map[string]string, describe func(key string) (string, engine.Impact, bool)) {
		keys := map[string]bool{}
		for k := range a {
			keys[k] = true
		}
		for k := range b {
			keys[k] = true
		}
		sorted := make([]string, 0, len(keys))
		for k := range keys {
			sorted = append(sorted, k)
		}
		slices.Sort(sorted)
		for _, k := range sorted {
			from, hadFrom := a[k]
			to, hasTo := b[k]
			if hadFrom == hasTo && from == to {
				continue
			}
			title, impact, unverified := describe(k)
			c := SettingChange{Key: k, Title: title, Impact: impact, Unverified: unverified}
			if hadFrom {
				c.From = &from
			}
			if hasTo {
				c.To = &to
			}
			out = append(out, c)
		}
	}
	registry := func(key string) (string, engine.Impact, bool) {
		for _, id := range engine.All {
			if s := engine.SettingOf(id, key); s != nil {
				return s.Title, s.Impact, false
			}
		}
		return key, engine.ImpactRestart, false // a setting no longer in the registry: a restart
	}
	diff(old.Tuning, new.Tuning, registry)
	diff(old.FlinkCDC.FlinkConf, new.FlinkCDC.FlinkConf, registry)
	diff(old.Advanced, new.Advanced, func(key string) (string, engine.Impact, bool) { return key, engine.ImpactRestart, true })
	return out
}

// HeaviestImpact is the heaviest impact of changes ("" when there are none).
func HeaviestImpact(changes []SettingChange) engine.Impact {
	var out engine.Impact
	for _, c := range changes {
		if out == "" || c.Impact.Rank() > out.Rank() {
			out = c.Impact
		}
	}
	return out
}

// SummarizeChange explains, in plain words, what deploying new over old does.
func SummarizeChange(old *PipelineSpec, new PipelineSpec) ChangeSummary {
	if old == nil {
		return ChangeSummary{
			TablesAdded: append([]string{}, new.Source.Tables.Include...), TablesRemoved: []string{},
			RestartsConnector: true, Notes: []string{"First deploy. " + SnapshotModeHelp[new.Snapshot.Mode]},
		}
	}
	before := setOf(old.Source.Tables.Include)
	after := setOf(new.Source.Tables.Include)
	s := ChangeSummary{
		TablesAdded:         diffSorted(after, before),
		TablesRemoved:       diffSorted(before, after),
		SnapshotModeChanged: old.Snapshot.Mode != new.Snapshot.Mode,
		NamingChanged:       old.Sink.Naming != new.Sink.Naming,
		FormatChanged:       !reflect.DeepEqual(old.Sink.Format, new.Sink.Format),
		RestartsConnector:   true,
		Notes:               []string{},
	}
	if len(s.TablesAdded) > 0 {
		s.Notes = append(s.Notes, fmt.Sprintf("Adds %d table(s); an incremental snapshot copies only their existing rows "+
			"while other tables keep streaming.", len(s.TablesAdded)))
	}
	if len(s.TablesRemoved) > 0 {
		s.Notes = append(s.Notes, fmt.Sprintf("Stops capturing %d table(s) immediately.", len(s.TablesRemoved)))
	}
	if s.NamingChanged {
		s.Notes = append(s.Notes, "Events go to new destination names; consumers of the old names stop receiving data.")
	}
	if s.FormatChanged {
		s.Notes = append(s.Notes, "Event format changes; make sure consumers can read the new format.")
	}
	if s.SnapshotModeChanged {
		s.Notes = append(s.Notes, "Snapshot mode only applies when the pipeline has no stored position.")
	}
	if old.Equal(new) {
		s.RestartsConnector = false
		s.Notes = append(s.Notes, "No changes.")
	} else {
		s.Notes = append(s.Notes, "The connector restarts from its stored position; no data is lost.")
	}
	return s
}

func setOf(l []string) map[string]bool {
	m := make(map[string]bool, len(l))
	for _, s := range l {
		m[s] = true
	}
	return m
}

func diffSorted(a, b map[string]bool) []string {
	out := []string{}
	for k := range a {
		if !b[k] {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}
