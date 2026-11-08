package services

import (
	"context"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/models"
)

// NeedsColumns reports whether the steps refer to columns or field names, so checking them
// needs the tables' columns.
func NeedsColumns(spec domain.PipelineSpec) bool {
	if len(spec.Keys) > 0 {
		return true
	}
	for _, t := range spec.Transforms {
		switch {
		// Computed columns and expressions (ADR 0008) read columns too: their checks, and the drift
		// alert, need them.
		case t.Typed() && len(t.Rules) > 0, t.Type == "rename", t.Type == "add_field", t.Type == "flatten", t.Type == "soft_delete",
			t.Type == "computed", t.Expression != "":
			return true
		}
	}
	return false
}

// TablesOf converts a discovery to the columns of the captured tables.
func TablesOf(d connectors.Discovery, captured []string) map[string]domain.TableColumns {
	out := map[string]domain.TableColumns{}
	for _, t := range d.Tables {
		name := t.Identifier()
		if !contains(captured, name) {
			continue
		}
		cols := make([]domain.PreviewColumn, len(t.Columns))
		for i, c := range t.Columns {
			cols[i] = domain.PreviewColumn{Name: c.Name, Type: c.Type, Nullable: c.Nullable, PII: c.PII, HasDefault: c.HasDefault}
		}
		out[name] = domain.TableColumns{Columns: cols, PrimaryKey: t.PrimaryKey, ReplicaIdentity: t.ReplicaIdentity, KeyKnown: true}
	}
	return out
}

// transformChecks are the preflight checks of the steps against the source's tables.
func transformChecks(ctx context.Context, conn *models.Connection, discover *discoverOnce, engine, pipeline string, spec domain.PipelineSpec) []checks.Check {
	if !NeedsColumns(spec) {
		return domain.TransformChecks(spec, nil)
	}
	found, err := discover.get(ctx)
	if err != nil {
		return append(domain.TransformChecks(spec, nil), checks.Check{Code: "transform_columns_unknown", Title: "Transforms: columns",
			Category: checks.CatDestination, Status: checks.Warn,
			Message: "Could not read the tables' columns to check the transform steps: " + domain.Truncate(pyErrString(err), 200)})
	}
	findings := domain.TransformFindings(engine, conn.Type, pipeline, spec, TablesOf(found, spec.Source.Tables.Include))
	return domain.TransformChecks(spec, findings)
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
