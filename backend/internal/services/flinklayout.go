package services

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/compiler"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
)

// ADR 0012: a Flink CDC start without a saved position writes, into every destination table that
// exists, exactly that table's columns, or doesn't happen. The destination is read only here, at
// the start points (the first start, a start after Start over), and what was planned is stored for
// the compile, which reads no destination.

const (
	// ArtifactLayout: {"floor": saved-position id, "planned_at", "tables": {destination: [layout
	// column…]}} — the destinations whose order a planned start pinned, and the newest saved position
	// from before it (restoring one at or below it is refused: its job wrote other columns).
	ArtifactLayout = "flink_layout"
	// ArtifactFreshStart: the next start was planned without a saved position (settings fresh_start).
	ArtifactFreshStart = "flink_fresh_start"
	// ArtifactPlanned: the deployed version and restore floor the start was planned for, so the
	// controller plans once per start point.
	ArtifactPlanned = "flink_planned"
)

// storedLayout is artifacts.flink_layout's tables (nil when none).
func storedLayout(p *models.Pipeline) map[string][]domain.LayoutColumn {
	m, _ := p.Artifacts[ArtifactLayout].(map[string]any)
	tables, _ := m["tables"].(map[string]any)
	if len(tables) == 0 {
		return nil
	}
	out := map[string][]domain.LayoutColumn{}
	for dest, v := range tables {
		list, _ := v.([]any)
		for _, item := range list {
			c, _ := item.(map[string]any)
			name, _ := c["name"].(string)
			typ, _ := c["type"].(string)
			cast, _ := c["cast"].(bool)
			retired, _ := c["retired"].(bool)
			out[dest] = append(out[dest], domain.LayoutColumn{Name: name, Type: typ, Cast: cast, Retired: retired})
		}
	}
	return out
}

// layoutFloor is artifacts.flink_layout.floor (0 when unset).
func layoutFloor(p *models.Pipeline) int64 {
	m, _ := p.Artifacts[ArtifactLayout].(map[string]any)
	return int64(toFloat(m["floor"]))
}

func freshStartPlanned(p *models.Pipeline) bool {
	b, _ := p.Artifacts[ArtifactFreshStart].(bool)
	return b
}

// layoutPlan is what a start without a saved position would write, per destination table.
type layoutPlan struct {
	tables   map[string]domain.LayoutPlan
	problems []string
	// warnings: what BeziSync can't verify (Fluss tables it can't read).
	warnings []string
	columns  map[string][]string
	defaults map[string][]string
}

// planLayout reads the source's columns and every destination table and plans the start; it
// changes nothing.
func planLayout(ctx context.Context, q db.Querier, p *models.Pipeline, spec domain.PipelineSpec, source, sink connectors.Resolved) (*layoutPlan, error) {
	cols, defaults, err := liveColumns(ctx, q, p, spec.Source.Tables.Include)
	if err != nil {
		return nil, err
	}
	out := &layoutPlan{tables: map[string]domain.LayoutPlan{}, columns: cols, defaults: defaults}
	produced, destinations, err := compiler.FlinkProduced(compileContext(p, spec), source, sink, cols, defaults)
	if err != nil {
		return nil, err
	}
	snk, err := connectors.GetSink(sink.Type)
	if err != nil {
		return nil, err
	}
	if _, ok := snk.(connectors.NoFlinkTable); ok {
		return out, nil
	}
	fc, ok := snk.(connectors.FlinkColumns)
	if !ok {
		out.warnings = append(out.warnings, fmt.Sprintf("BeziSync can't read %s tables: a destination table that already exists drops the columns it doesn't have.", sink.Type))
		return out, nil
	}
	// One plan per destination; tables routed into one destination write the union of their columns.
	byDest := map[string][]domain.FlinkColumn{}
	for _, t := range spec.Source.Tables.Include {
		dest := destinations[t]
		for _, c := range produced[t] {
			if !slices.ContainsFunc(byDest[dest], func(o domain.FlinkColumn) bool { return strings.EqualFold(o.Name, c.Name) }) {
				byDest[dest] = append(byDest[dest], c)
			}
		}
	}
	dests := make([]string, 0, len(byDest))
	for d := range byDest {
		dests = append(dests, d)
	}
	sort.Strings(dests)
	for _, dest := range dests {
		have, exists, err := fc.FlinkTableColumns(ctx, sink, dest)
		if err != nil {
			out.problems = append(out.problems, fmt.Sprintf("%s: BeziSync couldn't read the table (%s), so it can't tell whether the pipeline would write its columns in the right places.", dest, domain.Truncate(err.Error(), 200)))
			continue
		}
		plan, problems := domain.PlanFlinkLayout(byDest[dest], have, exists, fc.FlinkLayoutTarget())
		for _, msg := range problems {
			out.problems = append(out.problems, dest+": "+msg)
		}
		out.tables[dest] = plan
	}
	return out, nil
}

func compileContext(p *models.Pipeline, spec domain.PipelineSpec) connectors.CompileContext {
	return connectors.CompileContext{PipelineID: p.ID.String(), ShortID: p.ShortID(), Name: p.Name, Spec: spec}
}

// EnsureFlinkStartPlanned plans a Flink CDC pipeline's next start once per start point (the
// deployed version and the restore floor): with a saved position left the job resumes and keeps
// its layout; without one BeziSync reads every destination table, adds the columns the job writes
// that a table lacks, and stores the layout and fresh_start (ADR 0012). A start it can't make
// safely is an error, and the pipeline doesn't start. The caller saves the pipeline.
func EnsureFlinkStartPlanned(ctx context.Context, q db.Querier, p *models.Pipeline, spec domain.PipelineSpec) error {
	if EngineOf(p) != engine.FlinkCDC || p.DesiredState != "running" || p.DeployedVersionID == nil {
		return nil
	}
	key := fmt.Sprintf("%s@%d", p.DeployedVersionID, restoreFloor(p))
	if s, _ := p.Artifacts[ArtifactPlanned].(string); s == key {
		return nil
	}
	// Without the state database the job can't start either: plan on a later pass (the helper
	// never starts fresh unplanned).
	conn, err := stateConn(ctx)
	if err != nil {
		return nil //nolint:nilerr // plan on a later pass; the job can't start without the state database
	}
	latest, err := latestSavedID(ctx, conn, p.ShortID())
	_ = conn.Close(ctx)
	if err != nil {
		return nil //nolint:nilerr // as above
	}
	artifacts := cloneArtifacts(p)
	if latest > restoreFloor(p) {
		// It resumes: the job keeps writing what it wrote.
		delete(artifacts, ArtifactFreshStart)
		artifacts[ArtifactPlanned] = key
		p.Artifacts = artifacts
		return nil
	}
	source, sink, err := resolvedPair(ctx, q, p)
	if err != nil {
		return err
	}
	plan, err := planLayout(ctx, q, p, spec, source, sink)
	if err != nil {
		return err
	}
	if len(plan.problems) > 0 {
		return problem.New(409, "flink_destination_columns", "The pipeline can't start without writing columns into the wrong places: "+strings.Join(plan.problems, " "))
	}
	snk, _ := connectors.GetSink(sink.Type)
	var ran []string
	var pinned []string
	tables := map[string]any{}
	for dest, tp := range plan.tables {
		if len(tp.Add) > 0 {
			fc := snk.(connectors.FlinkColumns)
			done, err := fc.AddFlinkColumns(ctx, sink, dest, tp.Add)
			ran = append(ran, done...)
			if err != nil {
				return problem.New(409, "flink_destination_columns", fmt.Sprintf("%s lacks columns the pipeline writes, and adding them failed: %s", dest, err.Error()))
			}
			have, _, err := fc.FlinkTableColumns(ctx, sink, dest)
			if err != nil {
				return fmt.Errorf("%s: couldn't read the table back after adding columns: %w", dest, err)
			}
			for _, c := range tp.Add {
				if !slices.ContainsFunc(have, func(d domain.DestColumn) bool { return strings.EqualFold(d.Name, c.Name) }) {
					return problem.New(409, "flink_destination_columns", fmt.Sprintf("%s still has no column %s after adding it.", dest, c.Name))
				}
			}
		}
		if tp.Pinned {
			pinned = append(pinned, dest)
			list := make([]any, len(tp.Columns))
			for i, c := range tp.Columns {
				m := map[string]any{"name": c.Name}
				if c.Type != "" {
					m["type"] = c.Type
				}
				if c.Cast {
					m["cast"] = true
				}
				if c.Retired {
					m["retired"] = true
				}
				list[i] = m
			}
			tables[dest] = list
		}
	}
	sort.Strings(pinned)
	layout := map[string]any{"floor": latest, "planned_at": pytime.ISOFormat(time.Now().UTC())}
	if len(tables) > 0 {
		layout["tables"] = tables
	}
	artifacts[ArtifactLayout] = layout
	artifacts[ArtifactFreshStart] = true
	artifacts[ArtifactPlanned] = key
	artifacts[ArtifactColumns] = namesArtifact(plan.columns)
	artifacts[ArtifactDefaults] = namesArtifact(plan.defaults)
	p.Artifacts = artifacts
	if len(ran) > 0 || len(pinned) > 0 {
		_, err := Record(ctx, q, AuditEvent{Kind: "pipeline.layout_planned", Summary: fmt.Sprintf("Planned the columns %s writes on its next start", p.Name),
			ProjectID: &p.ProjectID, ResourceType: "pipeline", ResourceID: &p.ID,
			Details: map[string]any{"added": ran, "pinned": pinned, "floor": latest}})
		if err != nil {
			return err
		}
	}
	return nil
}

// resolvedPair resolves a pipeline's source and destination connections.
func resolvedPair(ctx context.Context, q db.Querier, p *models.Pipeline) (connectors.Resolved, connectors.Resolved, error) {
	sourceConn, err := GetConnection(ctx, q, p.SourceID)
	if err != nil {
		return connectors.Resolved{}, connectors.Resolved{}, err
	}
	if sourceConn == nil || p.SinkID == nil {
		return connectors.Resolved{}, connectors.Resolved{}, problem.NotFound("Connection")
	}
	sinkConn, err := GetConnection(ctx, q, *p.SinkID)
	if err != nil {
		return connectors.Resolved{}, connectors.Resolved{}, err
	}
	if sinkConn == nil {
		return connectors.Resolved{}, connectors.Resolved{}, problem.NotFound("Destination")
	}
	source, err := Resolve(ctx, q, sourceConn)
	if err != nil {
		return connectors.Resolved{}, connectors.Resolved{}, err
	}
	sink, err := Resolve(ctx, q, sinkConn)
	return source, sink, err
}

// flinkLayoutChecks are the deploy's checks for ADR 0012. flink_columns_changed blocks a version
// that changes the columns a pipeline writes while it would resume a saved position (resuming would
// drop the change silently). flink_destination_columns, for a start without a saved position, says
// what the start would add, and blocks one that would write columns into the wrong places.
func flinkLayoutChecks(ctx context.Context, q db.Querier, p *models.Pipeline, spec domain.PipelineSpec, source, sink connectors.Resolved) ([]checks.Check, error) {
	left, known := positionLeft(ctx, p)
	if p.DeployedVersionID != nil && (left || !known) {
		deployed, err := SpecOf(ctx, q, p.DeployedVersionID)
		if err != nil || deployed == nil {
			return nil, err
		}
		cols, defaults, err := liveColumns(ctx, q, p, union(deployed.Source.Tables.Include, spec.Source.Tables.Include))
		if err != nil {
			return []checks.Check{{Code: "flink_columns_changed", Title: "Columns written", Category: checks.CatDestination, Status: checks.Warn,
				Message: "BeziSync couldn't read the source's columns to compare what this version writes with the running one: " + domain.Truncate(err.Error(), 200)}}, nil
		}
		was, _, err := compiler.FlinkProduced(compileContext(p, *deployed), source, sink, cols, defaults)
		if err != nil {
			return nil, nil //nolint:nilerr // the deployed version doesn't compile here any more: nothing to compare
		}
		now, _, err := compiler.FlinkProduced(compileContext(p, spec), source, sink, cols, defaults)
		if err != nil {
			return nil, nil //nolint:nilerr // this version's own compile reports it
		}
		if changes := producedChanges(was, now); len(changes) > 0 {
			c := checks.Check{Code: "flink_columns_changed", Title: "Columns written", Category: checks.CatDestination, Status: checks.Fail, Blocking: true,
				Message: "This version changes the columns the pipeline writes (" + strings.Join(changes, "; ") + "), and the pipeline would resume its saved position: " +
					"Flink CDC keeps writing the columns it started with, so the change would be dropped without a word.",
				Fix: &checks.Fix{Kind: "text", Who: "you", Code: "Pause the pipeline and use Positions → Start over (copy the tables again, or read changes from now), then deploy: " +
					"that start writes the new columns, and BeziSync adds them to the destination tables first."}}
			if !known {
				c.Message += " BeziSync couldn't read the saved positions to see whether one is left."
			}
			return []checks.Check{c}, nil
		}
		return nil, nil
	}
	plan, err := planLayout(ctx, q, p, spec, source, sink)
	if err != nil {
		return []checks.Check{{Code: "flink_destination_columns", Title: "Destination columns", Category: checks.CatDestination, Status: checks.Warn,
			Message: "BeziSync couldn't plan the columns the pipeline writes: " + domain.Truncate(err.Error(), 300)}}, nil
	}
	if len(plan.problems) > 0 {
		return []checks.Check{{Code: "flink_destination_columns", Title: "Destination columns", Category: checks.CatDestination, Status: checks.Fail, Blocking: true,
			Message: "The pipeline would start without a saved position and can't write these tables' columns in the right places: " + strings.Join(plan.problems, " "),
			Fix: &checks.Fix{Kind: "text", Who: "you", Code: "Change the steps so the pipeline keeps writing these columns, " +
				"or change the destination tables (or name new ones in the Destination step), then run preflight again."}}}, nil
	}
	var out []checks.Check
	if snk, err := connectors.GetSink(sink.Type); err == nil {
		if byHand, ok := snk.(connectors.FlinkColumnsByHand); ok {
			var stmts []string
			for _, dest := range layoutKeys(plan.tables) {
				if add := plan.tables[dest].Add; len(add) > 0 {
					stmts = append(stmts, byHand.FlinkAddStatements(dest, add)...)
				}
			}
			if len(stmts) > 0 {
				return []checks.Check{{Code: "flink_destination_columns", Title: "Destination columns", Category: checks.CatDestination, Status: checks.Fail, Blocking: true,
					Message: "The pipeline writes columns these tables don't have, and BeziSync can't add them to " + sink.Type + " tables.",
					Fix:     &checks.Fix{Kind: "sql", Who: "data engineer", Code: strings.Join(stmts, "\n")}}}, nil
			}
		}
	}
	if len(plan.warnings) > 0 {
		out = append(out, checks.Check{Code: "flink_destination_columns", Title: "Destination columns", Category: checks.CatDestination, Status: checks.Warn,
			Message: strings.Join(plan.warnings, " ")})
	}
	var adds, pinned []string
	for _, dest := range layoutKeys(plan.tables) {
		tp := plan.tables[dest]
		for _, c := range tp.Add {
			adds = append(adds, dest+"."+c.Name)
		}
		if tp.Pinned {
			pinned = append(pinned, dest)
		}
	}
	if len(adds) > 0 || len(pinned) > 0 {
		msg := ""
		if len(adds) > 0 {
			msg = "Before the start BeziSync adds " + strings.Join(adds, ", ") + " to the destination tables (empty for the rows they have). "
		}
		if len(pinned) > 0 {
			msg += "It writes " + strings.Join(pinned, ", ") + " in the order the table has, so new source columns wait for the next deploy there."
		}
		out = append(out, checks.Check{Code: "flink_destination_columns", Title: "Destination columns", Category: checks.CatDestination, Status: checks.Pass, Message: strings.TrimSpace(msg)})
	}
	return out, nil
}

func layoutKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func union(a, b []string) []string {
	out := slices.Clone(a)
	for _, s := range b {
		if !slices.Contains(out, s) {
			out = append(out, s)
		}
	}
	return out
}

// producedChanges lists, per table both versions capture, the columns added and gone (names; the
// order a by-name destination ignores, and a positional one follows its table).
func producedChanges(was, now map[string][]domain.FlinkColumn) []string {
	var out []string
	for _, table := range layoutKeys(now) {
		before, ok := was[table]
		if !ok {
			continue // an added table: its own checks
		}
		var added, gone []string
		for _, c := range now[table] {
			if !slices.ContainsFunc(before, func(o domain.FlinkColumn) bool { return o.Name == c.Name }) {
				added = append(added, c.Name)
			}
		}
		for _, c := range before {
			if !slices.ContainsFunc(now[table], func(o domain.FlinkColumn) bool { return o.Name == c.Name }) {
				gone = append(gone, c.Name)
			}
		}
		var parts []string
		if len(added) > 0 {
			parts = append(parts, "adds "+strings.Join(added, ", "))
		}
		if len(gone) > 0 {
			parts = append(parts, "no longer writes "+strings.Join(gone, ", "))
		}
		if len(parts) > 0 {
			out = append(out, table+" "+strings.Join(parts, " and "))
		}
	}
	return out
}

// restoreBeforeLayout refuses restoring a saved position from before the current layout was
// planned: its job wrote other columns (ADR 0012).
func restoreBeforeLayout(p *models.Pipeline, id int64) error {
	if floor := layoutFloor(p); floor > 0 && id <= floor {
		return problem.Conflict("position_before_layout",
			fmt.Sprintf("Saved position #%d was saved before the pipeline last started over: its job wrote other columns into the destination tables. Restore a later one, or Start over.", id), nil)
	}
	return nil
}
