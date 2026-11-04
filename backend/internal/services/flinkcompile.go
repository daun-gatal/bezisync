package services

import (
	"context"
	"slices"

	"github.com/daun-gatal/bezisync/backend/internal/compiler"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

// ArtifactColumns: the captured tables' columns when the pipeline was last deployed (table →
// column names). A Flink CDC pipeline that holds new columns back, leaves columns out or renames
// them writes exactly these; the next deploy reads them again.
const ArtifactColumns = "flink_columns"

// ArtifactDefaults: of those columns, the ones with a default in the source (table → names),
// read with them. Destinations that refuse source defaults get these written without one.
const ArtifactDefaults = "flink_defaults"

// NeedsFlinkColumns reports a spec whose tables are written column by column on Flink CDC.
func NeedsFlinkColumns(spec domain.PipelineSpec) bool {
	if spec.FlinkCDC.Effective().NewColumns == "hold" {
		return true
	}
	for _, rules := range spec.Source.Columns {
		if len(rules.Exclude) > 0 {
			return true
		}
	}
	return slices.ContainsFunc(spec.Transforms, func(t domain.Transform) bool { return t.Type == "rename" })
}

// ResolveFlinkImage is the Flink CDC image a pipeline runs: its pin, or the catalog default.
func ResolveFlinkImage(ctx context.Context, q db.Querier, spec domain.PipelineSpec) (string, error) {
	if spec.FlinkCDCImage != nil && *spec.FlinkCDCImage != "" {
		return *spec.FlinkCDCImage, nil
	}
	img, err := DefaultImageFor(ctx, q, engine.FlinkCDC)
	if err != nil {
		return "", err
	}
	if img == "" {
		return "", problem.New(422, "engine_unavailable", "No Flink CDC image is registered: an Admin adds one in Settings → Engine versions.")
	}
	return img, nil
}

// storedColumns reads artifacts[ArtifactColumns] (or another table → names artifact).
func storedColumns(p *models.Pipeline, key ...string) map[string][]string {
	k := ArtifactColumns
	if len(key) > 0 {
		k = key[0]
	}
	out := map[string][]string{}
	m, _ := p.Artifacts[k].(map[string]any)
	for table, v := range m {
		list, _ := v.([]any)
		for _, c := range list {
			if s, ok := c.(string); ok {
				out[table] = append(out[table], s)
			}
		}
	}
	return out
}

// RefreshFlinkColumns reads the captured tables' columns from the source into the pipeline's
// artifacts (on deploy; the caller saves the pipeline).
func RefreshFlinkColumns(ctx context.Context, q db.Querier, p *models.Pipeline, spec domain.PipelineSpec) error {
	if !NeedsFlinkColumns(spec) {
		return nil
	}
	cols, defaults, err := liveColumns(ctx, q, p, spec.Source.Tables.Include)
	if err != nil {
		return err
	}
	artifacts := cloneArtifacts(p)
	artifacts[ArtifactColumns] = namesArtifact(cols)
	artifacts[ArtifactDefaults] = namesArtifact(defaults)
	p.Artifacts = artifacts
	return nil
}

// namesArtifact stores table → names as JSON-ready values.
func namesArtifact(names map[string][]string) map[string]any {
	stored := map[string]any{}
	for t, list := range names {
		l := make([]any, len(list))
		for i, c := range list {
			l[i] = c
		}
		stored[t] = l
	}
	return stored
}

// liveColumns reads tables' columns from the source, and which of them have a default.
func liveColumns(ctx context.Context, q db.Querier, p *models.Pipeline, tables []string) (map[string][]string, map[string][]string, error) {
	conn, err := GetConnection(ctx, q, p.SourceID)
	if err != nil {
		return nil, nil, err
	}
	if conn == nil {
		return nil, nil, problem.NotFound("Source")
	}
	found, err := Discover(ctx, q, conn)
	if err != nil {
		return nil, nil, err
	}
	out, defaults := map[string][]string{}, map[string][]string{}
	for name, t := range TablesOf(found, tables) {
		for _, c := range t.Columns {
			out[name] = append(out[name], c.Name)
			if c.HasDefault {
				defaults[name] = append(defaults[name], c.Name)
			}
		}
	}
	return out, defaults, nil
}

// flinkInput gathers what a Flink CDC compile needs besides the spec and the connections:
// columns stored at deploy (read live for tables deployed without them, as in a preview).
func flinkInput(ctx context.Context, q db.Querier, p *models.Pipeline, spec domain.PipelineSpec, rt *models.Runtime) (*compiler.FlinkInput, error) {
	secrets, err := FlinkSecretsOf(ctx, q, p)
	if err != nil {
		return nil, err
	}
	in := &compiler.FlinkInput{RestoreAfter: restoreFloor(p), StateKey: secrets.StateKey, Keystore: secrets.Keystore,
		KeystorePassword: secrets.KeystorePassword}
	if rt != nil {
		in.RuntimeKind = rt.Kind
	}
	if startup, ok := p.Artifacts[ArtifactStartup].(map[string]any); ok {
		in.Startup = startup
	}
	if NeedsFlinkColumns(spec) {
		in.Columns, in.Defaults = storedColumns(p), storedColumns(p, ArtifactDefaults)
		var missing []string
		for _, t := range spec.Source.Tables.Include {
			if len(in.Columns[t]) == 0 {
				missing = append(missing, t)
			}
		}
		if len(missing) > 0 {
			live, defaults, err := liveColumns(ctx, q, p, missing)
			if err != nil {
				return nil, err
			}
			for t, cols := range live {
				in.Columns[t], in.Defaults[t] = cols, defaults[t]
			}
		}
	}
	return in, nil
}
