package services

import (
	"context"
	"slices"
	"sort"

	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/models"
)

// PipelineSetting is one registry setting a pipeline can use: where its value lives, the value
// its current version has ("" for the default) and the project's limit on it.
type PipelineSetting struct {
	engine.Setting
	// In is the spec field that holds it: "tuning", or "flink_cdc.flink_conf" for Flink settings.
	In    string               `json:"in"`
	Value string               `json:"value"`
	Limit *domain.SettingLimit `json:"limit,omitempty"`
}

// PipelineSettingsView is what a pipeline may tune: its engine's settings for its source and
// destination with the project's limits applied (a hidden one left out), and whether raw
// (advanced) properties are allowed, with the keys it has (never their values).
type PipelineSettingsView struct {
	Engine          engine.ID         `json:"engine"`
	Settings        []PipelineSetting `json:"settings"`
	AdvancedAllowed bool              `json:"advanced_allowed"`
	AdvancedKeys    []string          `json:"advanced_keys"`
}

// PipelineSettings reads the view for a pipeline's current version.
func PipelineSettings(ctx context.Context, q db.Querier, p *models.Pipeline) (*PipelineSettingsView, error) {
	eng := EngineOf(p)
	src, err := GetConnection(ctx, q, p.SourceID)
	if err != nil {
		return nil, err
	}
	sinkType, err := connectionType(ctx, q, p.SinkID)
	if err != nil {
		return nil, err
	}
	spec, err := SpecOf(ctx, q, p.CurrentVersionID)
	if err != nil {
		return nil, err
	}
	if spec == nil {
		d := domain.DefaultSpec()
		spec = &d
	}
	rules, err := PoliciesOf(ctx, q, p.ProjectID)
	if err != nil {
		return nil, err
	}
	limits := LimitsFor(rules, eng)
	sourceType := ""
	if src != nil {
		sourceType = src.Type
	}
	out := &PipelineSettingsView{Engine: eng, Settings: []PipelineSetting{}, AdvancedAllowed: rules.AllowAdvancedProperties, AdvancedKeys: []string{}}
	for _, s := range engine.SettingsFor(eng, sourceType, sinkType) {
		ps := PipelineSetting{Setting: s, In: "tuning", Value: spec.Tuning[s.Key]}
		if !slices.Contains(engine.TuningScopes(eng), s.Scope) {
			ps.In, ps.Value = "flink_cdc.flink_conf", spec.FlinkCDC.FlinkConf[s.Key]
		}
		if l, ok := limits[s.Key]; ok {
			if l.Hide {
				continue
			}
			ps.Limit = &l
		}
		out.Settings = append(out.Settings, ps)
	}
	for k := range spec.Advanced {
		out.AdvancedKeys = append(out.AdvancedKeys, k)
	}
	sort.Strings(out.AdvancedKeys)
	return out, nil
}
