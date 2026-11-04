package services

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

// ChooseEngine is the engine of a pipeline from sourceType to sinkType (sinkType "" while no
// destination is chosen): the requested one when given, else the current one while it still runs
// the pair, else the pair's default.
func ChooseEngine(requested, current, sourceType, sinkType string) (engine.ID, error) {
	if requested != "" {
		id := engine.ID(requested)
		if !id.Valid() {
			return "", problem.New(422, "unknown_engine", fmt.Sprintf("Unknown engine '%s'.", requested))
		}
		if !engine.Supports(id, sourceType, sinkType) {
			return "", unsupported(id, sourceType, sinkType)
		}
		return id, nil
	}
	if current != "" && engine.Supports(engine.Of(current), sourceType, sinkType) {
		return engine.Of(current), nil
	}
	id := engine.Default(sourceType, sinkType)
	if id == "" {
		return "", problem.New(422, "engine_unsupported",
			fmt.Sprintf("No engine can run a pipeline from %s to %s.", typeTitle("source", sourceType), typeTitle("sink", sinkType)))
	}
	return id, nil
}

func unsupported(id engine.ID, sourceType, sinkType string) error {
	if sinkType == "" {
		return problem.New(422, "engine_unsupported", fmt.Sprintf("%s can't read %s.", id.Title(), typeTitle("source", sourceType)))
	}
	return problem.New(422, "engine_unsupported",
		fmt.Sprintf("%s can't run a pipeline from %s to %s.", id.Title(), typeTitle("source", sourceType), typeTitle("sink", sinkType)))
}

// ValidateEngineSpec refuses settings that belong to the other engine.
func ValidateEngineSpec(id engine.ID, spec domain.PipelineSpec) error {
	switch id {
	case engine.Debezium:
		if !spec.FlinkCDC.Empty() || spec.FlinkCDCImage != nil {
			return problem.New(422, "engine_settings", "Flink CDC settings apply only to pipelines that run on Flink CDC.")
		}
	case engine.FlinkCDC:
		if spec.DebeziumImage != nil {
			return problem.New(422, "engine_settings", "A Debezium image applies only to pipelines that run on Debezium.")
		}
	}
	return nil
}

// enforceEnginePolicy refuses an engine the project does not allow.
func enforceEnginePolicy(ctx context.Context, q db.Querier, projectID uuid.UUID, id engine.ID) error {
	pol, err := PoliciesOf(ctx, q, projectID)
	if err != nil {
		return err
	}
	if c := domain.EngineViolation(pol, string(id)); c != nil {
		return problem.New(422, "policy_violation", c.Message).WithErrors([]map[string]any{{"loc": []string{c.Code}, "message": c.Message}})
	}
	return nil
}

// connectionType is the type of a connection ("" for nil).
func connectionType(ctx context.Context, q db.Querier, id *uuid.UUID) (string, error) {
	if id == nil {
		return "", nil
	}
	c, err := GetConnection(ctx, q, *id)
	if err != nil || c == nil {
		return "", err
	}
	return c.Type, nil
}

// typeTitle is a connector type's title ("PostgreSQL"), or the type itself when unknown.
func typeTitle(kind, typ string) string {
	if d, err := connectors.Describe(kind, typ); err == nil && d != nil {
		return d.Title
	}
	return typ
}

// EngineOf is a pipeline's engine.
func EngineOf(p *models.Pipeline) engine.ID { return engine.Of(p.Engine) }
