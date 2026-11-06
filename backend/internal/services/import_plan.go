package services

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/daun-gatal/bezisync/backend/internal/compiler"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
)

// The import plan: Adopt's report (recorded from the Python releases, unchanged) sorted into the
// five buckets every raw key falls in — owned by BeziSync, modelled by a wizard choice, a typed
// setting, an expert (raw) property, or forbidden — with the spec to create the draft from:
// Adopt's, with typed settings moved into tuning and excluded and masked columns mapped.

// ImportKey is one property of the imported file and what becomes of it.
type ImportKey struct {
	Key string `json:"key"`
	// Title is the typed setting's (tunable only).
	Title string `json:"title,omitempty"`
	// Problem says why it needs a change before it can be used (out of its bounds, C22).
	Problem string `json:"problem,omitempty"`
}

// ImportPlan is the import in five buckets, and the spec to create.
type ImportPlan struct {
	Owned     []ImportKey    `json:"owned"`
	Modelled  []ImportKey    `json:"modelled"`
	Tunable   []ImportKey    `json:"tunable"`
	Expert    []ImportKey    `json:"expert"`
	Forbidden []ImportKey    `json:"forbidden"`
	Spec      map[string]any `json:"spec"`
	Warnings  []string       `json:"warnings"`
}

var plainColumnRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*\.[A-Za-z_][A-Za-z0-9_$]*\.[A-Za-z_][A-Za-z0-9_$]*$`)
var maskKeyRe = regexp.MustCompile(`^debezium\.source\.column\.mask\.with\.\d+\.chars$`)

// PlanImport sorts an adopted file's properties and builds the spec to create from.
func PlanImport(text string, r AdoptReport) ImportPlan {
	props := ParseProperties(text)
	plan := ImportPlan{Owned: []ImportKey{}, Modelled: []ImportKey{}, Tunable: []ImportKey{}, Expert: []ImportKey{}, Forbidden: []ImportKey{}, Warnings: []string{}}
	var spec map[string]any
	raw, _ := json.Marshal(r.Spec)
	_ = json.Unmarshal(raw, &spec)
	if spec == nil {
		spec = map[string]any{}
	}
	sourceType := ""
	if r.SourceType != nil {
		sourceType = *r.SourceType
	}
	sinkType := ""
	if r.SinkType != nil {
		sinkType = *r.SinkType
	}
	settings := engine.SettingsFor(engine.Debezium, sourceType, sinkType)
	tuning := map[string]any{}
	advanced, _ := spec["advanced"].(map[string]any)
	in := func(list []string, k string) bool { return slices.Contains(list, k) }
	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		value := props[k]
		setting := func() *engine.Setting {
			for i := range settings {
				if settings[i].Key == k {
					return &settings[i]
				}
			}
			return nil
		}()
		switch {
		case in(r.Managed, k):
			plan.Owned = append(plan.Owned, ImportKey{Key: k})
		// C23: a key a wizard choice models stays modelled, even when it is also a setting. A
		// destination's setting is never a connection field (C20), so it imports as one.
		case in(r.Mapped, k) || (generatedRe.MatchString(k) && !(setting != nil && strings.HasPrefix(k, "debezium.sink."))):
			plan.Modelled = append(plan.Modelled, ImportKey{Key: k})
		case setting != nil:
			item := ImportKey{Key: k, Title: setting.Title}
			if why := engine.CheckSetting(*setting, value); why != "" {
				// C22: never clamped; listed with the bound, and left out until changed.
				item.Problem = why
			} else {
				tuning[k] = engine.CanonicalSetting(*setting, value)
			}
			if advanced != nil {
				delete(advanced, k)
			}
			plan.Tunable = append(plan.Tunable, item)
		case in(r.Kept, k):
			plan.Expert = append(plan.Expert, ImportKey{Key: k})
		case strings.HasPrefix(k, "debezium.source.") && compiler.ClassifyAdvanced(k, value, nil).Refused():
			plan.Forbidden = append(plan.Forbidden, ImportKey{Key: k, Problem: compiler.ClassifyAdvanced(k, value, nil).Message})
		default:
			plan.Modelled = append(plan.Modelled, ImportKey{Key: k})
		}
	}
	if advanced != nil && len(advanced) == 0 {
		delete(spec, "advanced")
	}
	if len(tuning) > 0 {
		spec["tuning"] = tuning
	}
	mapColumns(spec, props, &plan)
	plan.Spec = spec
	return plan
}

// mapColumns turns column.exclude.list and column.mask.with.N.chars into the spec's columns, for
// the captured tables (regular expressions can't be mapped and are named in a warning).
func mapColumns(spec map[string]any, props map[string]string, plan *ImportPlan) {
	source, _ := spec["source"].(map[string]any)
	if source == nil {
		return
	}
	tables := map[string]bool{}
	if t, ok := source["tables"].(map[string]any); ok {
		if inc, ok := t["include"].([]any); ok {
			for _, x := range inc {
				tables[fmt.Sprint(x)] = true
			}
		}
	}
	columns, _ := source["columns"].(map[string]any)
	if columns == nil {
		columns = map[string]any{}
	}
	var unmapped []string
	add := func(entry, kind string) {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			return
		}
		if !plainColumnRe.MatchString(entry) {
			unmapped = append(unmapped, entry)
			return
		}
		last := strings.LastIndex(entry, ".")
		table, column := entry[:last], entry[last+1:]
		if !tables[table] {
			return
		}
		rules, _ := columns[table].(map[string]any)
		if rules == nil {
			rules = map[string]any{"mask": []any{}, "exclude": []any{}}
		}
		list, _ := rules[kind].([]any)
		if !slices.Contains(list, any(column)) {
			rules[kind] = append(list, column)
		}
		columns[table] = rules
	}
	for _, e := range strings.Split(props["debezium.source.column.exclude.list"], ",") {
		add(e, "exclude")
	}
	for k, v := range props {
		if maskKeyRe.MatchString(k) {
			for _, e := range strings.Split(v, ",") {
				add(e, "mask")
			}
		}
	}
	if len(columns) > 0 {
		source["columns"] = columns
	}
	if len(unmapped) > 0 {
		sort.Strings(unmapped)
		plan.Warnings = append(plan.Warnings, "Column patterns BeziSync can't map, so they are not left out or masked yet: "+strings.Join(unmapped, ", ")+". Choose those columns in the Tables step.")
	}
}
