package compiler

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

// tuningSettings resolves a spec's tuning against the registry: every key must be a setting of
// the engine for this source and destination, in a scope tuning holds, with a valid value.
// Saving refuses the same (services.ValidateTuning); this guards what reaches a workload.
func tuningSettings(id engine.ID, sourceType, sinkType string, tuning map[string]string) ([]engine.Setting, error) {
	if len(tuning) == 0 {
		return nil, nil
	}
	allowed := map[string]engine.Setting{}
	for _, s := range engine.SettingsFor(id, sourceType, sinkType) {
		if slices.Contains(engine.TuningScopes(id), s.Scope) {
			allowed[s.Key] = s
		}
	}
	keys := make([]string, 0, len(tuning))
	for k := range tuning {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out []engine.Setting
	var errs []map[string]any
	for _, k := range keys {
		s, ok := allowed[k]
		switch {
		case !ok:
			errs = append(errs, map[string]any{"loc": []string{"tuning", k}, "message": "is not a setting this pipeline can use"})
		case engine.CheckSetting(s, tuning[k]) != "":
			errs = append(errs, map[string]any{"loc": []string{"tuning", k}, "message": engine.CheckSetting(s, tuning[k])})
		default:
			out = append(out, s)
		}
	}
	if len(errs) > 0 {
		return nil, problem.New(422, "tuning", "Some settings can't be used.").WithErrors(errs)
	}
	return out, nil
}

// ownedByBeziSync refuses a setting whose key BeziSync already set, unless the setting replaces a
// default BeziSync derives (engine.Setting.Derived).
func ownedByBeziSync(s engine.Setting, set bool) error {
	if set && !s.Derived {
		return problem.New(422, "tuning", fmt.Sprintf("%s is set by BeziSync for this pipeline and can't be tuned.", s.Title)).
			WithErrors([]map[string]any{{"loc": []string{"tuning", s.Key}, "message": "is set by BeziSync for this pipeline"}})
	}
	return nil
}

// flinkTuningBlock splits a Flink CDC setting key into its YAML block and option name.
func flinkTuningBlock(key string) (block, option string) {
	block, option, _ = strings.Cut(key, ".")
	return block, option
}
