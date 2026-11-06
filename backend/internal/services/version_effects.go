package services

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/models"
)

// What moving a pipeline to another Debezium release does to its settings (C16, C24): a tuned
// setting the release doesn't have blocks the change until it is removed, one it renamed moves to
// its new key, and raw properties are listed as not checked — never as fine.

// SettingEffect is one setting's fate in a version change.
type SettingEffect struct {
	Key   string `json:"key"`
	Title string `json:"title,omitempty"`
	// Effect is "removed", "renamed", "missing" (not in that release yet), "unknown" (the release
	// can't be told) or "unchecked" (a raw property).
	Effect  string `json:"effect"`
	Message string `json:"message"`
	// Blocking: the change is refused until the setting is removed.
	Blocking bool `json:"blocking"`
	// To is the key a renamed setting moves to.
	To string `json:"to,omitempty"`
}

// VersionEffects are spec's settings' effects of running image.
func VersionEffects(spec domain.PipelineSpec, image string) []SettingEffect {
	out := []SettingEffect{}
	r, _, known := DebeziumRelease(image)
	for _, k := range tuningKeys(spec.Tuning) {
		s := engine.SettingOf(engine.Debezium, k)
		if s == nil || (s.Since == "" && s.Until == "") {
			continue
		}
		e := SettingEffect{Key: k, Title: s.Title}
		switch {
		case !known:
			e.Effect, e.Message = "unknown", fmt.Sprintf("BeziSync can't tell which Debezium release %s is, so it can't check %s exists there.", image, s.Title)
		case s.Until != "" && atLeast(r, s.Until) && s.Renamed != "":
			e.Effect, e.To = "renamed", s.Renamed
			e.Message = fmt.Sprintf("Debezium %s renamed it %s: it moves to the new key.", s.Until, s.Renamed)
		case s.Until != "" && atLeast(r, s.Until):
			e.Effect, e.Blocking = "removed", true
			e.Message = fmt.Sprintf("Debezium %s removed it: remove it from the pipeline first.", s.Until)
		case s.Since != "" && !atLeast(r, s.Since):
			e.Effect, e.Blocking = "missing", true
			e.Message = fmt.Sprintf("It arrived in Debezium %s: remove it from the pipeline first.", s.Since)
		default:
			continue
		}
		out = append(out, e)
	}
	for _, k := range tuningKeys(spec.Advanced) {
		out = append(out, SettingEffect{Key: k, Effect: "unchecked",
			Message: "A raw property: BeziSync can't check what another Debezium release makes of it."})
	}
	return out
}

// applyRenames moves renamed settings to their new keys (the caller checked nothing blocks).
func applyRenames(spec *domain.PipelineSpec, effects []SettingEffect) {
	for _, e := range effects {
		if e.Effect == "renamed" && spec.Tuning != nil {
			spec.Tuning[e.To] = spec.Tuning[e.Key]
			delete(spec.Tuning, e.Key)
		}
	}
}

// PipelineEffects is a pipeline's settings effects of a version change.
type PipelineEffects struct {
	PipelineID uuid.UUID       `json:"pipeline_id"`
	Name       string          `json:"name"`
	Image      string          `json:"image"`
	Effects    []SettingEffect `json:"effects"`
}

// ImageEffects previews a version change for pipelines (nothing changes): the image each would
// run (target, or its runtime's default when nil) and its settings' effects.
func ImageEffects(ctx context.Context, q db.Querier, a *auth.ProjectAccess, ids []uuid.UUID, target *string) ([]PipelineEffects, error) {
	out := []PipelineEffects{}
	for _, id := range ids {
		p, err := GetPipeline(ctx, q, a, id, false)
		if err != nil {
			return nil, err
		}
		if EngineOf(p) != engine.Debezium {
			continue
		}
		spec, err := SpecOf(ctx, q, p.CurrentVersionID)
		if err != nil || spec == nil {
			return nil, err
		}
		image, err := targetImage(ctx, q, p, *spec, target)
		if err != nil {
			return nil, err
		}
		out = append(out, PipelineEffects{PipelineID: p.ID, Name: p.Name, Image: image, Effects: VersionEffects(*spec, image)})
	}
	return out, nil
}

// targetImage is the image p would run with target (its runtime's default when nil).
func targetImage(ctx context.Context, q db.Querier, p *models.Pipeline, spec domain.PipelineSpec, target *string) (string, error) {
	if target != nil && *target != "" {
		return *target, nil
	}
	var rt *models.Runtime
	if p.RuntimeID != nil {
		var err error
		if rt, err = GetRuntime(ctx, q, *p.RuntimeID); err != nil {
			return "", err
		}
	}
	sinkType, err := SinkTypeOf(ctx, q, p)
	if err != nil {
		return "", err
	}
	next := spec.Clone()
	next.DebeziumImage = nil
	return ResolveImage(ctx, q, next, rt, sinkType)
}
