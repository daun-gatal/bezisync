package services

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/om"
)

// Adopting transforms: known SMTs become typed steps; anything else becomes a custom step
// (kept, needing the custom-transforms policy) instead of being dropped.

var (
	adoptNameRe  = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)
	adoptFieldRe = regexp.MustCompile(`^[A-Za-z0-9_$\-]{1,128}$`)
	adoptPropRe  = regexp.MustCompile(`^[a-z0-9][A-Za-z0-9._\-]{0,127}$`)
	adoptClassRe = regexp.MustCompile(`^[A-Za-z_$][A-Za-z0-9_$]*(\.[A-Za-z_$][A-Za-z0-9_$]*)+$`)
)

// adoptStepProps returns a step's properties without the debezium.transforms.<name>. prefix.
func adoptStepProps(name string, props map[string]string) map[string]string {
	prefix := "debezium.transforms." + name + "."
	out := map[string]string{}
	for k, v := range props {
		if strings.HasPrefix(k, prefix) {
			out[strings.TrimPrefix(k, prefix)] = v
		}
	}
	return out
}

func safeValue(s string) bool {
	if strings.Contains(s, "${") || len([]rune(s)) > 1000 {
		return false
	}
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// adoptSteps maps the chain to spec steps, in stage order, with the warnings to show.
func adoptSteps(names []string, props map[string]string) ([]any, []string) {
	var warnings []string
	type step struct {
		stage int
		m     *om.Map
	}
	var steps []step
	custom := func(name, class string, p map[string]string, why string) {
		if !adoptClassRe.MatchString(class) {
			warnings = append(warnings, fmt.Sprintf("Transform %s (%s) is not a valid class name; it was not kept.", name, class))
			return
		}
		keep := om.New()
		var dropped []string
		for _, k := range sortedMapKeys(p) {
			switch {
			case k == "type":
			case k == "predicate" || k == "negate":
				dropped = append(dropped, k)
			case adoptPropRe.MatchString(k) && safeValue(p[k]):
				keep.Set(k, p[k])
			default:
				dropped = append(dropped, k)
			}
		}
		steps = append(steps, step{2, om.New().Set("type", "custom").Set("class", class).Set("properties", keep)})
		msg := fmt.Sprintf("Transform %s (%s) is kept as a custom step%s; deploying it needs the project policy “Allow custom transforms”.", name, class, why)
		if _, lang := keep.Get("language"); lang || slices.Contains(domain.ScriptedClasses, class) {
			msg += " It runs a script, so it also needs “Allow scripted transforms”."
		}
		if len(dropped) > 0 {
			msg += " Not kept: " + strings.Join(dropped, ", ") + " (predicates and values with ${…} or line breaks)."
		}
		warnings = append(warnings, msg)
	}
	for _, name := range names {
		p := adoptStepProps(name, props)
		class := p["type"]
		switch {
		case class == "":
			continue
		case strings.HasSuffix(class, "ExtractNewRecordState") || strings.HasSuffix(class, "ExtractNewDocumentState"):
			handling, ok := p["delete.tombstone.handling.mode"]
			if !ok || (handling != "rewrite" && handling != "tombstone" && handling != "drop") {
				handling = "rewrite"
			}
			m := om.New().Set("type", "unwrap").Set("delete_handling", handling)
			if f := p["add.fields"]; f != "" {
				var fields []any
				for _, x := range strings.Split(f, ",") {
					if x = domain.PyStrip(x); x != "" {
						fields = append(fields, x)
					}
				}
				m.Set("add_fields", fields)
			}
			steps = append(steps, step{1, m})
		case strings.HasSuffix(class, "RegexRouter"):
			warnings = append(warnings, "A RegexRouter renamed topics; check the destination naming preview matches what consumers read.")
		case class == "org.apache.kafka.connect.transforms.ReplaceField$Value" && p["renames"] != "" && p["predicate"] == "" &&
			p["exclude"] == "" && p["include"] == "" && p["blacklist"] == "" && p["whitelist"] == "":
			var renames []any
			ok := true
			for _, pair := range strings.Split(p["renames"], ",") {
				from, to, found := strings.Cut(domain.PyStrip(pair), ":")
				if !found || !adoptFieldRe.MatchString(from) || !adoptNameRe.MatchString(to) {
					ok = false
					break
				}
				renames = append(renames, om.New().Set("from", from).Set("to", to))
			}
			if !ok {
				custom(name, class, p, " (its renames are not plain field names)")
				continue
			}
			steps = append(steps, step{2, om.New().Set("type", "rename").Set("renames", renames)})
		case class == "org.apache.kafka.connect.transforms.InsertField$Value" && p["static.field"] != "" && p["predicate"] == "" &&
			len(p) == 3 && adoptNameRe.MatchString(p["static.field"]) && safeValue(p["static.value"]) && p["static.value"] != "" && len([]rune(p["static.value"])) <= 200:
			steps = append(steps, step{2, om.New().Set("type", "add_field").Set("name", p["static.field"]).
				Set("value", om.New().Set("kind", "static").Set("text", p["static.value"]))})
		case class == "org.apache.kafka.connect.transforms.Flatten$Value" && p["predicate"] == "" && (p["delimiter"] == "" || p["delimiter"] == "_" || p["delimiter"] == "."):
			d := p["delimiter"]
			if d == "" {
				d = "." // Kafka's default
			}
			steps = append(steps, step{2, om.New().Set("type", "flatten").Set("delimiter", d)})
		case class == "io.debezium.transforms.Filter" && p["language"] == "jsr223.groovy" && p["condition"] != "" && len(p) <= 3 && safeValue(p["condition"]):
			steps = append(steps, step{0, om.New().Set("type", "filter").Set("condition", p["condition"])})
			warnings = append(warnings, fmt.Sprintf("Transform %s is a filter script; deploying it needs the project policy “Allow scripted transforms”. Consider rebuilding it from rules.", name))
		default:
			custom(name, class, p, "")
		}
	}
	// BeziSync runs filters on the full change event, then the event shape, then field changes.
	sorted := slices.Clone(steps)
	slices.SortStableFunc(sorted, func(a, b step) int { return a.stage - b.stage })
	hasShape := false
	for _, st := range sorted {
		hasShape = hasShape || st.stage == 1
	}
	out := make([]any, 0, len(sorted))
	reordered := false
	for i, st := range sorted {
		if st.m != steps[i].m {
			reordered = true
		}
		if st.stage == 2 && !hasShape {
			typ, _ := st.m.Get("type")
			warnings = append(warnings, fmt.Sprintf("The %s step needs the row-after-change event shape, which the file does not use; it was not kept.", typ))
			continue
		}
		out = append(out, st.m)
	}
	if reordered {
		warnings = append(warnings, "BeziSync runs filters before the event shape and field changes after it, so the steps were reordered; check the preview.")
	}
	return out, warnings
}

// adoptSourceOptions maps value-format and key-column options to spec.values and spec.keys.
func adoptSourceOptions(src map[string]string, tables []string) (*om.Map, *om.Map, map[string]bool) {
	values, keys, mapped := om.New(), om.New(), map[string]bool{}
	for option, field := range map[string]string{"decimal.handling.mode": "decimals", "binary.handling.mode": "binary", "time.precision.mode": "timestamps"} {
		v, ok := src[option]
		if !ok {
			continue
		}
		allowed := map[string][]string{"decimals": domain.DecimalModes, "binary": domain.BinaryModes, "timestamps": {"isostring"}}[field]
		if slices.Contains(allowed, v) {
			values.Set(field, v)
			mapped["debezium.source."+option] = true
		}
	}
	if raw, ok := src["message.key.columns"]; ok {
		parsed := om.New()
		good := true
		for _, entry := range strings.Split(raw, ";") {
			entry = domain.PyStrip(entry)
			if entry == "" {
				continue
			}
			table, cols, found := strings.Cut(entry, ":")
			table = strings.ReplaceAll(table, `\.`, ".")
			var list []any
			for _, c := range strings.Split(cols, ",") {
				if c = domain.PyStrip(c); adoptFieldRe.MatchString(c) {
					list = append(list, c)
				} else {
					good = false
				}
			}
			if !found || !slices.Contains(tables, table) || len(list) == 0 {
				good = false
				break
			}
			parsed.Set(table, list)
		}
		if good && parsed.Len() > 0 {
			keys = parsed
			mapped["debezium.source.message.key.columns"] = true
		}
	}
	// Stable output order.
	ordered := om.New()
	for _, k := range []string{"decimals", "timestamps", "binary"} {
		if v, ok := values.Get(k); ok {
			ordered.Set(k, v)
		}
	}
	return ordered, keys, mapped
}

func sortedMapKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}
