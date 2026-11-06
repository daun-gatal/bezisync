package domain

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// Transform steps beyond the Python releases. The Python types (unwrap, filter with a raw
// condition, outbox) keep their fields and dump exactly as before; everything added here is
// dumped only when set, so stored specs, their hashes and the recorded corpus do not change.
//
// Steps run in the order listed, inside BeziSync's own chain (bzmeta first, bzroute last).
// Typed steps belong to a stage, and the decoder enforces the stage order so the chain is
// valid by construction: "keep events" filters read the full change envelope, so they come
// before the event shape; field changes need the row, so they come after it.

// TransformTypes are the step types, in the order the UI offers them.
var TransformTypes = []string{"unwrap", "filter", "outbox", "rename", "add_field", "flatten", "custom", "soft_delete"}

// SoftDeleteField is the field a soft delete marks deleted rows with, per engine: Debezium's
// unwrap adds __deleted ("true" for a delete); Flink CDC writes its change type as __op ("-D").
var SoftDeleteField = map[string]string{"debezium": "__deleted", "flink-cdc": "__op"}

// Operations a filter keeps, with Debezium's op codes.
var FilterOperations = map[string]string{"insert": "c", "update": "u", "delete": "d", "snapshot": "r"}

// FilterOps are the rule operators.
var FilterOps = []string{"eq", "ne", "in", "not_in", "gt", "gte", "lt", "lte", "is_null", "is_not_null", "is_true", "is_false"}

// Value formats (Debezium source options) a spec may choose.
var (
	DecimalModes   = []string{"precise", "double", "string"}
	TimestampModes = []string{"epoch", "isostring"}
	BinaryModes    = []string{"bytes", "base64", "hex"}
)

// ScriptedClasses evaluate code from their properties (JSR 223).
var ScriptedClasses = []string{"io.debezium.transforms.Filter", "io.debezium.transforms.ContentBasedRouter"}

const (
	maxSteps       = 30
	maxRules       = 20
	maxRenames     = 50
	maxCustomProps = 30
	maxKeyColumns  = 10
)

var (
	stepIDRe     = `^[a-z0-9]{4,12}$`
	fieldRe      = `^[A-Za-z0-9_$\-]{1,128}$`
	newNameRe    = `^[A-Za-z_][A-Za-z0-9_]{0,127}$`
	classRe      = `^[A-Za-z_$][A-Za-z0-9_$]*(\.[A-Za-z_$][A-Za-z0-9_$]*)+$`
	propKeyRe    = `^[a-z0-9][A-Za-z0-9._\-]{0,127}$`
	reservedProp = []string{"type", "predicate", "negate"}
	expressionRe = regexp.MustCompile(`\$\{`)
)

// FilterRule is one condition of a filter step.
type FilterRule struct {
	Field  string `json:"field"`
	Op     string `json:"op"`
	Value  any    `json:"value"`  // string, json.Number or bool; nil when the operator takes none
	Values []any  `json:"values"` // for in / not_in
}

// Rename is one field rename.
type Rename struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// FieldValue is what an add_field step writes.
type FieldValue struct {
	Kind string `json:"kind"` // static | pipeline
	Text string `json:"text"`
}

// ValueFormats are connector options for how values are encoded ("" = connector default).
type ValueFormats struct {
	Decimals   string `json:"decimals"`
	Timestamps string `json:"timestamps"`
	Binary     string `json:"binary"`
}

// Empty reports no format chosen.
func (f ValueFormats) Empty() bool { return f == ValueFormats{} }

// Legacy reports a step type of the Python releases (their dump shape is fixed).
func (t Transform) Legacy() bool {
	return t.Type == "unwrap" || t.Type == "filter" || t.Type == "outbox"
}

// Scripted reports whether the step runs user-written script code.
func (t Transform) Scripted() bool {
	switch t.Type {
	case "filter":
		return t.Condition != nil && *t.Condition != ""
	case "custom":
		if slices.Contains(ScriptedClasses, t.Class) {
			return true
		}
		_, lang := t.Properties["language"]
		return lang
	}
	return false
}

// Typed reports a filter built from operations and rules (compiled by BeziSync).
func (t Transform) Typed() bool {
	return t.Type == "filter" && (len(t.Operations) > 0 || len(t.Rules) > 0)
}

// Stage orders steps: 0 keep events, 1 event shape, 2 field changes. Legacy raw filters and
// outbox steps keep the order they were written in (stage -1).
func (t Transform) Stage() int {
	switch {
	case t.Typed():
		return 0
	case t.Type == "unwrap" || t.Type == "soft_delete":
		return 1
	case t.Type == "rename" || t.Type == "add_field" || t.Type == "flatten" || t.Type == "custom":
		return 2
	}
	return -1
}

// ScriptedSteps lists the steps that run a script the user wrote, as "step N (type)".
// Scripts run inside the connector next to its credentials, so they need the project policy
// allow_scripted_transforms.
func ScriptedSteps(spec PipelineSpec) []string {
	var out []string
	for i, t := range spec.Transforms {
		if t.Scripted() {
			out = append(out, StepLabel(i, t))
		}
	}
	return out
}

// BuiltInTransformTypes are the types a project may allow or disallow (custom has its own
// policy).
func BuiltInTransformTypes() []string {
	return slices.DeleteFunc(slices.Clone(TransformTypes), func(t string) bool { return t == "custom" })
}

// transformPolicyChecks are the policy failures of a spec's steps (before scripts).
func transformPolicyChecks(p ProjectPolicies, spec PipelineSpec) []checks.Check {
	var out []checks.Check
	var disallowed, custom []string
	for i, t := range spec.Transforms {
		switch {
		case t.Type == "custom":
			if !p.AllowCustomTransforms {
				custom = append(custom, StepLabel(i, t))
			}
		case p.AllowedTransforms != nil && !slices.Contains(p.AllowedTransforms, t.Type):
			disallowed = append(disallowed, StepLabel(i, t))
		}
	}
	if len(disallowed) > 0 {
		out = append(out, policyFail("policy_transform_type", "Transforms",
			fmt.Sprintf("This project does not allow %s.", strings.Join(disallowed, ", ")), AskAdmin))
	}
	if len(custom) > 0 {
		out = append(out, policyFail("policy_custom_transform", "Custom transforms",
			fmt.Sprintf("%s run(s) a custom transform class (%s); this project does not allow custom transforms.",
				pluralSteps(len(custom)), strings.Join(custom, ", ")), AskAdmin))
	}
	return out
}

func pluralSteps(n int) string {
	if n == 1 {
		return "One step"
	}
	return fmt.Sprintf("%d steps", n)
}

// noExpression refuses Quarkus/SmallRye "${…}" in values that end up in
// application.properties: Debezium Server expands them, so "${BZ_SRC_PASSWORD}" in a static
// field would copy a secret into every event.
func noExpression(v *val.V, loc val.Loc, s string) bool {
	if expressionRe.MatchString(s) {
		v.ValueError(loc, "must not contain \"${\": Debezium Server would read it as a variable")
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			v.ValueError(loc, "must not contain control characters (line breaks, tabs)")
			return false
		}
	}
	return true
}

// decodeTransformExtras reads the fields added after the Python releases.
func decodeTransformExtras(v *val.V, o *val.Object, out *Transform) {
	if f := o.Get("id"); f.Present && !f.IsNull() {
		if s, ok := v.Constrained(f.Loc, f.Raw, val.StrRules{Pattern: stepIDRe}); ok {
			out.ID = s
		}
	}
	if f := o.Get("tables"); f.Present && !f.IsNull() {
		if l, ok := v.StrList(f.Loc, f.Raw); ok {
			if len(l) == 0 {
				v.ValueError(f.Loc, "leave tables out to apply the step to every table")
			} else if !unique(l) {
				v.ValueError(f.Loc, "tables must not repeat")
			} else {
				out.Tables = l
			}
		}
	}
	scoped := map[string]bool{"filter": true, "rename": true, "add_field": true, "flatten": true, "custom": true}
	switch out.Type {
	case "filter":
		decodeFilter(v, o, out)
	case "rename":
		decodeRenames(v, o, out)
	case "add_field":
		decodeAddField(v, o, out)
	case "flatten":
		out.Delimiter = "_"
		if f := o.Get("delimiter"); f.Present {
			if s, ok := v.Literal(f.Loc, f.Raw, "_", "."); ok {
				out.Delimiter = s
			}
		}
	case "custom":
		decodeCustom(v, o, out)
	}
	if out.Tables != nil && !scoped[out.Type] {
		v.ValueError(o.Loc().Add("tables"), out.Type+" steps apply to every table")
	}
}

func decodeFilter(v *val.V, o *val.Object, out *Transform) {
	if f := o.Get("operations"); f.Present && !f.IsNull() {
		if items, ok := v.List(f.Loc, f.Raw); ok {
			ops := make([]string, 0, len(items))
			for i, item := range items {
				if s, ok := v.Literal(f.Loc.Add(i), item, "insert", "update", "delete", "snapshot"); ok {
					ops = append(ops, s)
				}
			}
			if !unique(ops) {
				v.ValueError(f.Loc, "operations must not repeat")
			} else if len(ops) > 0 {
				out.Operations = ops
			}
		}
	}
	if f := o.Get("match"); f.Present {
		if s, ok := v.Literal(f.Loc, f.Raw, "all", "any"); ok {
			out.Match = s
		}
	}
	if f := o.Get("rules"); f.Present && !f.IsNull() {
		if items, ok := v.List(f.Loc, f.Raw); ok && v.ListLen(f.Loc, len(items), 0, maxRules) {
			for i, item := range items {
				out.Rules = append(out.Rules, decodeRule(v, f.Loc.Add(i), item))
			}
		}
	}
	typed := len(out.Operations) > 0 || len(out.Rules) > 0
	switch {
	case typed && out.Condition != nil:
		v.ValueError(o.Loc(), "a filter uses either operations and rules, or a raw condition, not both")
	case !typed && out.Condition == nil && (o.Has("operations") || o.Has("rules")):
		v.ValueError(o.Loc(), "a filter needs at least one operation or rule")
	case out.Match != "" && len(out.Rules) == 0:
		v.ValueError(o.Loc().Add("match"), "match needs rules")
	}
	if typed && out.Match == "" {
		out.Match = "all"
	}
	if !typed && out.Tables != nil {
		v.ValueError(o.Loc().Add("tables"), "only filters built from operations and rules can be limited to tables")
	}
}

func decodeRule(v *val.V, loc val.Loc, raw any) FilterRule {
	var r FilterRule
	o := v.Model(loc, raw, "FilterRule")
	if o.Failed {
		return r
	}
	if f := o.Get("field"); f.Required() {
		if s, ok := v.Constrained(f.Loc, f.Raw, val.StrRules{Pattern: fieldRe}); ok {
			r.Field = s
		}
	}
	if f := o.Get("op"); f.Required() {
		if s, ok := v.Literal(f.Loc, f.Raw, FilterOps...); ok {
			r.Op = s
		}
	}
	scalar := func(loc val.Loc, raw any, number bool) (any, bool) {
		switch x := raw.(type) {
		case string:
			if number {
				break
			}
			if len([]rune(x)) > 500 {
				v.Fail(loc, "string_too_long", "String should have at most 500 characters")
				return nil, false
			}
			return x, noExpression(v, loc, x)
		case bool:
			if number {
				break
			}
			return x, true
		default:
			if n, ok := v.Float(loc, raw); ok {
				_ = n
				return raw, true
			}
			return nil, false
		}
		v.ValueError(loc, "must be a number for this operator")
		return nil, false
	}
	value, values := o.Get("value"), o.Get("values")
	switch r.Op {
	case "eq", "ne", "gt", "gte", "lt", "lte":
		if value.Required() {
			r.Value, _ = scalar(value.Loc, value.Raw, r.Op != "eq" && r.Op != "ne")
		}
		if values.Present {
			v.ValueError(values.Loc, "use value with this operator")
		}
	case "in", "not_in":
		if values.Required() {
			if items, ok := v.List(values.Loc, values.Raw); ok && v.ListLen(values.Loc, len(items), 1, 100) {
				for i, item := range items {
					if x, ok := scalar(values.Loc.Add(i), item, false); ok {
						r.Values = append(r.Values, x)
					}
				}
			}
		}
		if value.Present {
			v.ValueError(value.Loc, "use values with this operator")
		}
	case "":
	default:
		if value.Present || values.Present {
			v.ValueError(loc, r.Op+" takes no value")
		}
	}
	o.Forbid()
	return r
}

func decodeRenames(v *val.V, o *val.Object, out *Transform) {
	f := o.Get("renames")
	if !f.Required() {
		return
	}
	items, ok := v.List(f.Loc, f.Raw)
	if !ok || !v.ListLen(f.Loc, len(items), 1, maxRenames) {
		return
	}
	from, to := map[string]bool{}, map[string]bool{}
	for i, item := range items {
		loc := f.Loc.Add(i)
		ro := v.Model(loc, item, "Rename")
		if ro.Failed {
			continue
		}
		var r Rename
		if g := ro.Get("from"); g.Required() {
			r.From, _ = v.Constrained(g.Loc, g.Raw, val.StrRules{Pattern: fieldRe})
		}
		if g := ro.Get("to"); g.Required() {
			r.To, _ = v.Constrained(g.Loc, g.Raw, val.StrRules{Pattern: newNameRe})
		}
		ro.Forbid()
		switch {
		case r.From == "" || r.To == "":
		case r.From == r.To:
			v.ValueError(loc, "a field cannot be renamed to itself")
		case from[r.From]:
			v.ValueError(loc.Add("from"), r.From+" is renamed twice")
		case to[r.To]:
			v.ValueError(loc.Add("to"), "two fields would be called "+r.To)
		default:
			from[r.From], to[r.To] = true, true
			out.Renames = append(out.Renames, r)
		}
	}
}

func decodeAddField(v *val.V, o *val.Object, out *Transform) {
	if f := o.Get("name"); f.Required() {
		out.Name, _ = v.Constrained(f.Loc, f.Raw, val.StrRules{Pattern: newNameRe})
	}
	f := o.Get("value")
	if !f.Required() {
		return
	}
	vo := v.Model(f.Loc, f.Raw, "FieldValue")
	if vo.Failed {
		return
	}
	if g := vo.Get("kind"); g.Required() {
		out.Value.Kind, _ = v.Literal(g.Loc, g.Raw, "static", "pipeline")
	}
	g := vo.Get("text")
	switch out.Value.Kind {
	case "static":
		if g.Required() {
			if s, ok := v.Constrained(g.Loc, g.Raw, val.StrRules{Min: 1, Max: 200}); ok && noExpression(v, g.Loc, s) {
				out.Value.Text = s
			}
		}
	case "pipeline":
		if g.Present {
			v.ValueError(g.Loc, "the pipeline name needs no text")
		}
	}
	vo.Forbid()
}

func decodeCustom(v *val.V, o *val.Object, out *Transform) {
	if f := o.Get("class"); f.Required() {
		out.Class, _ = v.Constrained(f.Loc, f.Raw, val.StrRules{Max: 300, Pattern: classRe})
	}
	f := o.Get("properties")
	if !f.Present || f.IsNull() {
		out.Properties = map[string]string{}
		return
	}
	m, ok := v.StrDict(f.Loc, f.Raw)
	if !ok {
		return
	}
	if len(m) > maxCustomProps {
		v.ValueError(f.Loc, fmt.Sprintf("at most %d properties", maxCustomProps))
		return
	}
	for _, k := range sortedStrKeys(m) {
		loc := f.Loc.Add(k)
		if _, ok := v.Constrained(loc, k, val.StrRules{Pattern: propKeyRe}); !ok {
			continue
		}
		if slices.Contains(reservedProp, k) {
			v.ValueError(loc, k+" is set by BeziSync (use the class and tables fields)")
			continue
		}
		if len([]rune(m[k])) > 1000 {
			v.Fail(loc, "string_too_long", "String should have at most 1000 characters")
			continue
		}
		noExpression(v, loc, m[k])
	}
	out.Properties = m
}

// validateTransforms checks the spec-wide step rules: count, unique ids, captured tables and
// stage order.
func validateTransforms(v *val.V, loc val.Loc, spec PipelineSpec) {
	steps := spec.Transforms
	if len(steps) > maxSteps {
		v.ValueError(loc, fmt.Sprintf("at most %d transform steps", maxSteps))
		return
	}
	ids := map[string]bool{}
	unwrapAt := -1
	for i, t := range steps {
		at := loc.Add(i)
		if t.ID != "" {
			if ids[t.ID] {
				v.ValueError(at.Add("id"), "step id "+t.ID+" is used twice")
			}
			ids[t.ID] = true
		}
		for _, table := range t.Tables {
			if !contains(spec.Source.Tables.Include, table) {
				v.ValueError(at.Add("tables"), table+" is not captured by this pipeline")
			}
		}
		switch t.Stage() {
		case 0:
			if unwrapAt >= 0 {
				v.ValueError(at, "keep-events filters run on the full change event: move this step above the event shape (step "+
					fmt.Sprint(unwrapAt+1)+")")
			}
		case 1:
			// The first shape step counts (the Python releases accepted several; stored specs
			// must keep parsing).
			if unwrapAt < 0 {
				unwrapAt = i
			}
		case 2:
			if unwrapAt < 0 {
				v.ValueError(at, t.Type+" changes fields of the row: it needs the row-after-change event shape before it")
			}
		}
	}
}

// validateKeys checks message key columns: captured tables, valid column names.
func validateKeys(v *val.V, loc val.Loc, spec PipelineSpec) {
	for _, table := range sortedKeyTables(spec.Keys) {
		if !contains(spec.Source.Tables.Include, table) {
			v.ValueError(loc.Add(table), table+" is not captured by this pipeline")
		}
	}
}

func decodeKeys(v *val.V, loc val.Loc, raw any) map[string][]string {
	m, ok := v.Dict(loc, raw)
	if !ok {
		return nil
	}
	out := map[string][]string{}
	for _, table := range sortedAnyKeys(m) {
		at := loc.Add(table)
		if _, ok := v.Constrained(at, table, val.StrRules{Pattern: tableRe.String()}); !ok {
			continue
		}
		cols, ok := v.StrList(at, m[table])
		if !ok || !v.ListLen(at, len(cols), 1, maxKeyColumns) {
			continue
		}
		good := true
		for i, c := range cols {
			if _, ok := v.Constrained(at.Add(i), c, val.StrRules{Pattern: fieldRe}); !ok {
				good = false
			}
		}
		if !unique(cols) {
			v.ValueError(at, "key columns must not repeat")
			good = false
		}
		if good {
			out[table] = cols
		}
	}
	return out
}

func decodeValues(v *val.V, loc val.Loc, raw any) ValueFormats {
	var out ValueFormats
	o := v.Model(loc, raw, "ValueFormats")
	if o.Failed {
		return out
	}
	if f := o.Get("decimals"); f.Present && !f.IsNull() {
		out.Decimals, _ = v.Literal(f.Loc, f.Raw, DecimalModes...)
	}
	if f := o.Get("timestamps"); f.Present && !f.IsNull() {
		out.Timestamps, _ = v.Literal(f.Loc, f.Raw, TimestampModes...)
	}
	if f := o.Get("binary"); f.Present && !f.IsNull() {
		out.Binary, _ = v.Literal(f.Loc, f.Raw, BinaryModes...)
	}
	o.Forbid()
	return out
}

// dumpTransform renders one step: Python types keep their four fields, typed additions follow
// only when set.
func dumpTransform(t Transform) *om.Map {
	m := om.New().Set("type", t.Type)
	if t.Legacy() {
		m.Set("delete_handling", t.DeleteHandling).Set("add_fields", strList(t.AddFields)).Set("condition", strPtr(t.Condition))
	}
	if t.ID != "" {
		m.Set("id", t.ID)
	}
	if t.Tables != nil {
		m.Set("tables", strList(t.Tables))
	}
	switch t.Type {
	case "filter":
		if len(t.Operations) > 0 {
			m.Set("operations", strList(t.Operations))
		}
		if len(t.Rules) > 0 {
			rules := make([]any, len(t.Rules))
			for i, r := range t.Rules {
				rm := om.New().Set("field", r.Field).Set("op", r.Op)
				if r.Values != nil {
					rm.Set("values", append([]any{}, r.Values...))
				} else if r.Value != nil {
					rm.Set("value", r.Value)
				}
				rules[i] = rm
			}
			m.Set("match", t.Match).Set("rules", rules)
		}
	case "rename":
		renames := make([]any, len(t.Renames))
		for i, r := range t.Renames {
			renames[i] = om.New().Set("from", r.From).Set("to", r.To)
		}
		m.Set("renames", renames)
	case "add_field":
		value := om.New().Set("kind", t.Value.Kind)
		if t.Value.Kind == "static" {
			value.Set("text", t.Value.Text)
		}
		m.Set("name", t.Name).Set("value", value)
	case "flatten":
		m.Set("delimiter", t.Delimiter)
	case "custom":
		props := om.New()
		for _, k := range sortedStrKeys(t.Properties) {
			props.Set(k, t.Properties[k])
		}
		m.Set("class", t.Class).Set("properties", props)
	}
	return m
}

func dumpValues(f ValueFormats) *om.Map {
	m := om.New()
	if f.Decimals != "" {
		m.Set("decimals", f.Decimals)
	}
	if f.Timestamps != "" {
		m.Set("timestamps", f.Timestamps)
	}
	if f.Binary != "" {
		m.Set("binary", f.Binary)
	}
	return m
}

func dumpKeys(keys map[string][]string) *om.Map {
	m := om.New()
	for _, t := range sortedKeyTables(keys) {
		m.Set(t, strList(keys[t]))
	}
	return m
}

func cloneTransform(t Transform) Transform {
	t.AddFields = append([]string{}, t.AddFields...)
	if t.Tables != nil {
		t.Tables = append([]string{}, t.Tables...)
	}
	if t.Operations != nil {
		t.Operations = append([]string{}, t.Operations...)
	}
	if t.Rules != nil {
		rules := make([]FilterRule, len(t.Rules))
		for i, r := range t.Rules {
			if r.Values != nil {
				r.Values = append([]any{}, r.Values...)
			}
			rules[i] = r
		}
		t.Rules = rules
	}
	if t.Renames != nil {
		t.Renames = append([]Rename{}, t.Renames...)
	}
	if t.Properties != nil {
		props := make(map[string]string, len(t.Properties))
		for k, v := range t.Properties {
			props[k] = v
		}
		t.Properties = props
	}
	return t
}

func unique(l []string) bool {
	seen := make(map[string]bool, len(l))
	for _, s := range l {
		if seen[s] {
			return false
		}
		seen[s] = true
	}
	return true
}

// KeyTables lists the tables of spec.keys in a stable order.
func KeyTables(m map[string][]string) []string { return sortedKeyTables(m) }

func sortedKeyTables(m map[string][]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return om.JSONBKeys(out)
}

// StepName is the step's name in debezium.transforms: stable ids give stable names, so
// reordering steps does not rename every property; Python-era steps keep t<index><type>.
func StepName(i int, t Transform) string {
	if t.ID != "" {
		return "t_" + t.ID
	}
	return fmt.Sprintf("t%d%s", i, t.Type)
}

// StepLabel names a step for people: "step 3 (rename)".
func StepLabel(i int, t Transform) string {
	return fmt.Sprintf("step %d (%s)", i+1, strings.ReplaceAll(t.Type, "_", " "))
}
