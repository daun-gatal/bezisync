package compiler

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

// Transform steps: each spec step becomes one SMT named domain.StepName (stable for steps with
// an id), placed between BeziSync's own bzmeta filter and bzroute router. Steps limited to
// some tables carry a TopicNameMatches predicate on the Debezium topics of those tables.

const (
	insertField  = "org.apache.kafka.connect.transforms.InsertField$Value"
	replaceField = "org.apache.kafka.connect.transforms.ReplaceField$Value"
	flatten      = "org.apache.kafka.connect.transforms.Flatten$Value"
	topicMatches = "org.apache.kafka.connect.transforms.predicates.TopicNameMatches"
	debeziumFilt = "io.debezium.transforms.Filter"
)

// stepProps compiles spec.transforms: the step properties, the step names in order and the
// predicate names they add.
func stepProps(c connectors.CompileContext, src connectors.Source, desc *connectors.Descriptor) (map[string]string, []string, []string, error) {
	spec := c.Spec
	props := map[string]string{}
	var chain, predicates []string
	title := "this source"
	var caps connectors.TransformCaps
	if desc != nil {
		title, caps = desc.Title, desc.Transforms
	}
	topics := func(tables []string) string {
		if len(tables) == 0 {
			tables = spec.Source.Tables.Include
		}
		parts := make([]string, len(tables))
		for i, t := range tables {
			parts[i] = src.TopicPattern(c, t)
		}
		if len(parts) == 1 {
			return parts[0]
		}
		return "(?:" + strings.Join(parts, "|") + ")"
	}
	for i, t := range spec.Transforms {
		name := domain.StepName(i, t)
		key := "debezium.transforms." + name + "."
		if t.Type == "route" {
			continue // compiled after BeziSync's own router (routeProps)
		}
		if t.Type == "computed" {
			// T16: Debezium has no expressions short of scripting, which has its own policy.
			return nil, nil, nil, problem.New(422, "transform_unsupported", fmt.Sprintf("Step %d: a computed column runs only on Flink CDC.", i+1))
		}
		if t.Type == "truncate" {
			// A source option, not a transform in the chain: exactly the property a raw one
			// would be, so a pipeline moved to the step keeps its hash (T7).
			if desc == nil || !slices.Contains(domain.TruncateSources, desc.Type) {
				return nil, nil, nil, problem.New(422, "transform_unsupported", fmt.Sprintf("Truncating columns is not available for %s.", title))
			}
			byLength := map[int][]string{}
			var lengths []int
			for _, c := range t.Truncate {
				if _, seen := byLength[c.Length]; !seen {
					lengths = append(lengths, c.Length)
				}
				byLength[c.Length] = append(byLength[c.Length], c.Table+"."+c.Column)
			}
			for _, n := range lengths {
				k := fmt.Sprintf("debezium.source.column.truncate.to.%d.chars", n)
				if props[k] != "" {
					props[k] += ","
				}
				props[k] += strings.Join(byLength[n], ",")
			}
			continue
		}
		switch t.Type {
		case "unwrap":
			props[key+"type"] = desc.UnwrapTransform
			props[key+"delete.tombstone.handling.mode"] = t.DeleteHandling
			if len(t.AddFields) > 0 {
				props[key+"add.fields"] = strings.Join(t.AddFields, ",")
			}
		case "filter":
			if t.Typed() {
				if len(t.Rules) > 0 && !caps.RowFilters {
					return nil, nil, nil, problem.New(422, "transform_unsupported",
						fmt.Sprintf("Filtering on column values is not available for %s; filter on operations instead.", title))
				}
				props[key+"type"] = debeziumFilt
				props[key+"language"] = "jsr223.groovy"
				props[key+"topic.regex"] = topics(t.Tables)
				props[key+"condition"] = FilterCondition(t)
				break
			}
			if t.Condition == nil || *t.Condition == "" {
				return nil, nil, nil, problem.New(422, "filter_condition", "A filter transform needs a condition.")
			}
			props[key+"type"] = debeziumFilt
			props[key+"language"] = "jsr223.groovy"
			props[key+"condition"] = *t.Condition
		case "outbox":
			props[key+"type"] = "io.debezium.transforms.outbox.EventRouter"
		case "rename":
			pairs := make([]string, len(t.Renames))
			for j, r := range t.Renames {
				pairs[j] = r.From + ":" + r.To
			}
			props[key+"type"] = replaceField
			props[key+"renames"] = strings.Join(pairs, ",")
		case "add_field":
			value := t.Value.Text
			if t.Value.Kind == "pipeline" {
				value = c.Name
			}
			props[key+"type"] = insertField
			props[key+"static.field"] = t.Name
			props[key+"static.value"] = value
		case "flatten":
			props[key+"type"] = flatten
			props[key+"delimiter"] = t.Delimiter
		case "custom":
			props[key+"type"] = t.Class
			for k, v := range t.Properties {
				props[key+k] = v
			}
		}
		if t.Tables != nil && t.Type != "filter" {
			pred := "p_" + name
			props[key+"predicate"] = pred
			props["debezium.predicates."+pred+".type"] = topicMatches
			props["debezium.predicates."+pred+".pattern"] = topics(t.Tables)
			predicates = append(predicates, pred)
		}
		chain = append(chain, name)
	}
	return props, chain, predicates, nil
}

// sourceOptions compiles spec.values and spec.keys to connector options.
func sourceOptions(c connectors.CompileContext, desc *connectors.Descriptor) (map[string]string, error) {
	spec := c.Spec
	props := map[string]string{}
	title := "this source"
	var caps connectors.TransformCaps
	if desc != nil {
		title, caps = desc.Title, desc.Transforms
	}
	formats := []struct{ kind, mode, option, what string }{
		{"decimals", spec.Values.Decimals, "decimal.handling.mode", "Decimals"},
		{"timestamps", spec.Values.Timestamps, "time.precision.mode", "Timestamps"},
		{"binary", spec.Values.Binary, "binary.handling.mode", "Binary values"},
	}
	for _, f := range formats {
		if f.mode == "" {
			continue
		}
		allowed := caps.ValueFormats[f.kind]
		if !contains(allowed, f.mode) {
			return nil, problem.New(422, "transform_unsupported",
				fmt.Sprintf("%s as %s are not available for %s.", f.what, valueModeLabel(f.mode), title))
		}
		// The first mode is the connector's default: leave it unset.
		if f.mode != allowed[0] {
			props["debezium.source."+f.option] = f.mode
		}
	}
	if len(spec.Keys) > 0 {
		if !caps.KeyColumns {
			return nil, problem.New(422, "transform_unsupported", fmt.Sprintf("Choosing message key columns is not available for %s.", title))
		}
		var parts []string
		for _, table := range domain.KeyTables(spec.Keys) {
			parts = append(parts, connectors.JavaRegexLiteral(table)+":"+strings.Join(spec.Keys[table], ","))
		}
		props["debezium.source.message.key.columns"] = strings.Join(parts, ";")
	}
	return props, nil
}

func valueModeLabel(mode string) string {
	switch mode {
	case "double":
		return "numbers"
	case "string":
		return "text"
	case "isostring":
		return "ISO-8601 text"
	}
	return mode
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// FilterCondition is the Groovy expression of a typed filter. Every literal and field name is
// a single-quoted Groovy string (escaped), so input can never become code; fields are read
// through g(), which returns null for a field the row does not have (Struct.get would throw
// and fail the connector); numbers compare as BigDecimal whatever their Java type, including
// decimals sent as text (decimal.handling.mode=string). Text that is not a number never
// matches a numeric rule.
func FilterCondition(t domain.Transform) string {
	ops := "true"
	if len(t.Operations) > 0 {
		codes := make([]string, len(t.Operations))
		for i, o := range t.Operations {
			codes[i] = groovyString(domain.FilterOperations[o])
		}
		ops = "(value.op in [" + strings.Join(codes, ", ") + "])"
	}
	rules := "true"
	if len(t.Rules) > 0 {
		parts := make([]string, len(t.Rules))
		for i, r := range t.Rules {
			parts[i] = ruleExpr(r)
		}
		join := " && "
		if t.Match == "any" {
			join = " || "
		}
		rules = "(" + strings.Join(parts, join) + ")"
	}
	return "({ s, f -> s != null && s.schema().field(f) != null ? s.get(f) : null }).with { g -> " +
		"({ v -> try { (v instanceof Number || v instanceof CharSequence) ? new BigDecimal(v.toString().trim()) : null } catch (e) { null } }).with { n -> " +
		"def r = (value.after ?: value.before); " + ops + " && " + rules + " } }"
}

func ruleExpr(r domain.FilterRule) string {
	x := "g(r, " + groovyString(r.Field) + ")"
	switch r.Op {
	case "eq":
		return equals(x, r.Value)
	case "ne":
		return "!" + equals(x, r.Value)
	case "in", "not_in":
		parts := make([]string, len(r.Values))
		for i, v := range r.Values {
			parts[i] = equals(x, v)
		}
		e := "(" + strings.Join(parts, " || ") + ")"
		if r.Op == "not_in" {
			return "!" + e
		}
		return e
	case "gt", "gte", "lt", "lte":
		cmp := map[string]string{"gt": "> 0", "gte": ">= 0", "lt": "< 0", "lte": "<= 0"}[r.Op]
		return "(n(" + x + ") != null && n(" + x + ").compareTo(" + groovyNumber(r.Value) + ") " + cmp + ")"
	case "is_null":
		return "(" + x + " == null)"
	case "is_not_null":
		return "(" + x + " != null)"
	case "is_true":
		return "(" + x + " == true)"
	case "is_false":
		return "(" + x + " == false)"
	}
	return "false"
}

func equals(x string, v any) string {
	switch val := v.(type) {
	case string:
		return "(" + x + " == " + groovyString(val) + ")"
	case bool:
		return fmt.Sprintf("(%s == %t)", x, val)
	}
	return "(n(" + x + ") != null && n(" + x + ").compareTo(" + groovyNumber(v) + ") == 0)"
}

func groovyNumber(v any) string {
	var s string
	switch n := v.(type) {
	case json.Number:
		s = n.String()
	case float64:
		s = fmt.Sprint(n)
	case int:
		s = fmt.Sprint(n)
	default:
		s = fmt.Sprint(n)
	}
	return "new BigDecimal(" + groovyString(s) + ")"
}

// groovyString quotes s as a single-quoted Groovy string (no interpolation).
func groovyString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}

// routeProps sends the tables route steps match to their destination: after bzroute (which
// names each table's topic from the naming pattern), one RegexRouter per destination from those
// topics to it. Nothing for a spec without routes.
func routeProps(c connectors.CompileContext, spec domain.PipelineSpec) (map[string]string, []string) {
	routed := domain.RouteTargets(spec)
	if len(routed) == 0 {
		return nil, nil
	}
	byTarget := map[string][]string{}
	for _, table := range spec.Source.Tables.Include {
		if to, ok := routed[table]; ok {
			schema, name, _ := strings.Cut(table, ".")
			byTarget[to] = append(byTarget[to], connectors.JavaRegexLiteral(c.Destination(schema, name)))
		}
	}
	targets := make([]string, 0, len(byTarget))
	for to := range byTarget {
		targets = append(targets, to)
	}
	slices.Sort(targets)
	props := map[string]string{}
	var chain []string
	for i, to := range targets {
		name := fmt.Sprintf("bzmerge%d", i+1)
		key := "debezium.transforms." + name + "."
		props[key+"type"] = "org.apache.kafka.connect.transforms.RegexRouter"
		props[key+"regex"] = "(?:" + strings.Join(byTarget[to], "|") + ")"
		props[key+"replacement"] = strings.ReplaceAll(to, "$", "\\$")
		chain = append(chain, name)
	}
	return props, chain
}
