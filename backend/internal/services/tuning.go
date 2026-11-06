package services

import (
	"slices"
	"sort"

	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

// ValidateTuning refuses tuning a pipeline can't use, each with why: keys that aren't settings of
// its engine, source and destination, Flink settings (they go in flink_cdc.flink_conf), values
// outside a setting's bounds, and values not written the one way the spec stores them (so what
// is saved round-trips exactly).
func ValidateTuning(id engine.ID, sourceType, sinkType string, spec domain.PipelineSpec) error {
	if len(spec.Tuning) == 0 {
		return nil
	}
	allowed := map[string]engine.Setting{}
	for _, s := range engine.SettingsFor(id, sourceType, sinkType) {
		if slices.Contains(engine.TuningScopes(id), s.Scope) {
			allowed[s.Key] = s
		}
	}
	keys := make([]string, 0, len(spec.Tuning))
	for k := range spec.Tuning {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var errs []map[string]any
	for _, k := range keys {
		value := spec.Tuning[k]
		msg := ""
		if s, ok := allowed[k]; ok {
			if msg = engine.CheckSetting(s, value); msg == "" {
				if c := engine.CanonicalSetting(s, value); c != value {
					msg = "write it as " + c
				}
			}
		} else {
			msg = "is not a setting BeziSync offers"
			for _, other := range engine.All {
				if s := engine.SettingOf(other, k); s != nil {
					switch {
					case other != id:
						msg = "applies only to pipelines that run on " + other.Title()
					case s.Scope == engine.ScopeEngine && id == engine.FlinkCDC:
						msg = "is a Flink setting: it goes in flink_cdc.flink_conf"
					default:
						msg = "doesn't apply to this source or destination"
					}
				}
			}
		}
		if msg != "" {
			errs = append(errs, map[string]any{"loc": []string{"tuning", k}, "message": msg})
		}
	}
	if len(errs) > 0 {
		return problem.New(422, "tuning", "Some settings can't be used.").WithErrors(errs)
	}
	return nil
}
