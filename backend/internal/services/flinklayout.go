package services

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

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
	// ArtifactWritten: the version whose columns the newest saved position's job writes (ADR
	// 0013). Deploy records the version it replaces when none is recorded yet; every planned start
	// records the deployed one. A resume from a version whose columns differ switches columns.
	ArtifactWritten = "flink_written"
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

// layoutGeneration is artifacts.flink_layout.generation: how many times a deploy switched the
// columns a job with a saved position writes (ADR 0013; 0 when never).
func layoutGeneration(p *models.Pipeline) int {
	m, _ := p.Artifacts[ArtifactLayout].(map[string]any)
	return int(toFloat(m["generation"]))
}

// writtenVersion is artifacts.flink_written (nil when unrecorded).
func writtenVersion(p *models.Pipeline) *uuid.UUID {
	s, _ := p.Artifacts[ArtifactWritten].(string)
	id, err := uuid.Parse(s)
	if err != nil {
		return nil
	}
	return &id
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
// changes nothing. A switch (ADR 0013) passes the source columns BeziSync recorded at the last
// planned start (flink_columns): the restored job knows only the columns it has read, so a source
// column in neither those nor the destination table, which may come after the saved position, is
// left out (writing it by name would fail the job: Flink CDC can't find the column). It waits like
// any new source column of a pinned table.
func planLayout(ctx context.Context, q db.Querier, p *models.Pipeline, spec domain.PipelineSpec, source, sink connectors.Resolved, known map[string][]string) (*layoutPlan, error) {
	conn, err := GetConnection(ctx, q, p.SourceID)
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, problem.NotFound("Source")
	}
	found, err := Discover(ctx, q, conn)
	if err != nil {
		return nil, err
	}
	cols, defaults, types := map[string][]string{}, map[string][]string{}, map[string]map[string]string{}
	for name, t := range TablesOf(found, spec.Source.Tables.Include) {
		types[name] = map[string]string{}
		for _, c := range t.Columns {
			cols[name] = append(cols[name], c.Name)
			types[name][c.Name] = c.Type
			if c.HasDefault {
				defaults[name] = append(defaults[name], c.Name)
			}
		}
	}
	if known != nil {
		if cols, defaults, err = knownColumns(ctx, p, spec, source, sink, cols, defaults, known); err != nil {
			return nil, err
		}
	}
	out := &layoutPlan{tables: map[string]domain.LayoutPlan{}, columns: cols, defaults: defaults}
	produced, destinations, err := compiler.FlinkProduced(compileContext(p, spec), source, sink, cols, defaults)
	if err != nil {
		return nil, err
	}
	// A source column the table lacks gets the type Flink CDC writes it as, where that is fixed.
	for table, list := range produced {
		for i := range list {
			if c := &list[i]; c.Source != "" && c.Type == "" && c.AddType == "" {
				c.AddType = domain.FlinkTypeOfSource(source.Type, types[table][c.Source])
			}
		}
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

// knownColumns keeps, of each table's source columns, those in known or in its destination table
// (a table BeziSync can't read, or that doesn't exist yet, keeps them all when known has none for it).
func knownColumns(ctx context.Context, p *models.Pipeline, spec domain.PipelineSpec, source, sink connectors.Resolved, cols, defaults, known map[string][]string) (map[string][]string, map[string][]string, error) {
	_, destinations, err := compiler.FlinkProduced(compileContext(p, spec), source, sink, cols, defaults)
	if err != nil {
		return nil, nil, err
	}
	snk, err := connectors.GetSink(sink.Type)
	if err != nil {
		return nil, nil, err
	}
	fc, _ := snk.(connectors.FlinkColumns)
	tables := map[string][]domain.DestColumn{}
	if fc != nil {
		for _, dest := range destinations {
			if _, done := tables[dest]; done {
				continue
			}
			if have, exists, err := fc.FlinkTableColumns(ctx, sink, dest); err == nil && exists {
				tables[dest] = have
			} else {
				tables[dest] = nil
			}
		}
	}
	keep := func(table string, list []string) []string {
		have := tables[destinations[table]]
		if len(known[table]) == 0 && have == nil {
			return list
		}
		var out []string
		for _, c := range list {
			if slices.Contains(known[table], c) || slices.ContainsFunc(have, func(d domain.DestColumn) bool { return strings.EqualFold(d.Name, c) }) {
				out = append(out, c)
			}
		}
		return out
	}
	outCols, outDefaults := map[string][]string{}, map[string][]string{}
	for t, list := range cols {
		outCols[t] = keep(t, list)
	}
	for t, list := range defaults {
		outDefaults[t] = keep(t, list)
	}
	return outCols, outDefaults, nil
}

func compileContext(p *models.Pipeline, spec domain.PipelineSpec) connectors.CompileContext {
	return connectors.CompileContext{PipelineID: p.ID.String(), ShortID: p.ShortID(), Name: p.Name, Spec: spec}
}

// EnsureFlinkStartPlanned plans a Flink CDC pipeline's next start once per start point (the
// deployed version and the restore floor). With a saved position left the job resumes: with the
// columns it writes when the deployed version writes the same ones, otherwise BeziSync switches
// columns at the position (ADR 0013): it plans the destination tables again, adds what they lack,
// and raises the column generation, so the helper restores the position without the schema
// coordinator's state. Without a saved position BeziSync reads every destination table, adds the
// columns the job writes that a table lacks, and stores the layout and fresh_start (ADR 0012). A
// start it can't make safely is an error, and the pipeline doesn't start. The caller saves the
// pipeline.
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
	planned := func() {
		artifacts[ArtifactPlanned] = key
		artifacts[ArtifactWritten] = p.DeployedVersionID.String()
		p.Artifacts = artifacts
	}
	if latest > restoreFloor(p) {
		delete(artifacts, ArtifactFreshStart)
		from := writtenVersion(p)
		if from == nil || *from == *p.DeployedVersionID {
			planned() // it resumes writing what it wrote
			return nil
		}
		source, sink, err := resolvedPair(ctx, q, p)
		if err != nil {
			return err
		}
		changes, err := columnChanges(ctx, q, p, from, spec, source, sink)
		if err != nil {
			return fmt.Errorf("BeziSync couldn't read the source's columns to see whether this version changes the columns the pipeline writes: %w", err)
		}
		if len(changes) == 0 {
			planned()
			return nil
		}
		plan, err := planLayout(ctx, q, p, spec, source, sink, storedColumns(p))
		if err != nil {
			return err
		}
		if len(plan.problems) > 0 {
			return problem.New(409, "flink_destination_columns", "The pipeline can't resume with the new columns without writing them into the wrong places: "+strings.Join(plan.problems, " "))
		}
		tables, added, pinned, err := applyLayoutPlan(ctx, sink, plan)
		if err != nil {
			return err
		}
		generation := layoutGeneration(p) + 1
		layout := map[string]any{"floor": layoutFloor(p), "planned_at": pytime.ISOFormat(time.Now().UTC()), "generation": generation}
		if len(tables) > 0 {
			layout["tables"] = tables
		}
		artifacts[ArtifactLayout] = layout
		artifacts[ArtifactColumns] = namesArtifact(plan.columns)
		artifacts[ArtifactDefaults] = namesArtifact(plan.defaults)
		planned()
		_, err = Record(ctx, q, AuditEvent{Kind: "pipeline.columns_switched", Summary: fmt.Sprintf("Switched the columns %s writes at its saved position", p.Name),
			ProjectID: &p.ProjectID, ResourceType: "pipeline", ResourceID: &p.ID,
			Details: map[string]any{"changes": changes, "added": added, "pinned": pinned, "generation": generation, "position": latest}})
		return err
	}
	source, sink, err := resolvedPair(ctx, q, p)
	if err != nil {
		return err
	}
	plan, err := planLayout(ctx, q, p, spec, source, sink, nil)
	if err != nil {
		return err
	}
	if len(plan.problems) > 0 {
		return problem.New(409, "flink_destination_columns", "The pipeline can't start without writing columns into the wrong places: "+strings.Join(plan.problems, " "))
	}
	tables, added, pinned, err := applyLayoutPlan(ctx, sink, plan)
	if err != nil {
		return err
	}
	layout := map[string]any{"floor": latest, "planned_at": pytime.ISOFormat(time.Now().UTC())}
	if g := layoutGeneration(p); g > 0 {
		layout["generation"] = g
	}
	if len(tables) > 0 {
		layout["tables"] = tables
	}
	artifacts[ArtifactLayout] = layout
	artifacts[ArtifactFreshStart] = true
	artifacts[ArtifactColumns] = namesArtifact(plan.columns)
	artifacts[ArtifactDefaults] = namesArtifact(plan.defaults)
	planned()
	if len(added) > 0 || len(pinned) > 0 {
		_, err := Record(ctx, q, AuditEvent{Kind: "pipeline.layout_planned", Summary: fmt.Sprintf("Planned the columns %s writes on its next start", p.Name),
			ProjectID: &p.ProjectID, ResourceType: "pipeline", ResourceID: &p.ID,
			Details: map[string]any{"added": added, "pinned": pinned, "floor": latest}})
		if err != nil {
			return err
		}
	}
	return nil
}

// applyLayoutPlan adds the columns each destination table lacks, reads every changed table back,
// and returns the layout of the pinned destinations (artifacts.flink_layout.tables), the columns
// added and the destinations pinned.
func applyLayoutPlan(ctx context.Context, sink connectors.Resolved, plan *layoutPlan) (map[string]any, []string, []string, error) {
	snk, _ := connectors.GetSink(sink.Type)
	var added, pinned []string
	tables := map[string]any{}
	for _, dest := range layoutKeys(plan.tables) {
		tp := plan.tables[dest]
		if len(tp.Add) > 0 {
			fc := snk.(connectors.FlinkColumns)
			done, err := fc.AddFlinkColumns(ctx, sink, dest, tp.Add)
			added = append(added, done...)
			if err != nil {
				return nil, added, nil, problem.New(409, "flink_destination_columns", fmt.Sprintf("%s lacks columns the pipeline writes, and adding them failed: %s", dest, err.Error()))
			}
			have, _, err := fc.FlinkTableColumns(ctx, sink, dest)
			if err != nil {
				return nil, added, nil, fmt.Errorf("%s: couldn't read the table back after adding columns: %w", dest, err)
			}
			for _, c := range tp.Add {
				if !slices.ContainsFunc(have, func(d domain.DestColumn) bool { return strings.EqualFold(d.Name, c.Name) }) {
					return nil, added, nil, problem.New(409, "flink_destination_columns", fmt.Sprintf("%s still has no column %s after adding it.", dest, c.Name))
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
	return tables, added, pinned, nil
}

// columnChanges lists what the spec changes in the columns the version from writes (producedChanges
// on both compiles, the live source columns for both); nothing when that version is gone or no
// longer compiles.
func columnChanges(ctx context.Context, q db.Querier, p *models.Pipeline, from *uuid.UUID, spec domain.PipelineSpec, source, sink connectors.Resolved) ([]string, error) {
	was, err := SpecOf(ctx, q, from)
	if err != nil || was == nil {
		return nil, err
	}
	cols, defaults, err := liveColumns(ctx, q, p, union(was.Source.Tables.Include, spec.Source.Tables.Include))
	if err != nil {
		return nil, err
	}
	before, _, err := compiler.FlinkProduced(compileContext(p, *was), source, sink, cols, defaults)
	if err != nil {
		return nil, nil //nolint:nilerr // that version doesn't compile here any more: nothing to compare
	}
	now, _, err := compiler.FlinkProduced(compileContext(p, spec), source, sink, cols, defaults)
	if err != nil {
		return nil, nil //nolint:nilerr // this version's own compile reports it
	}
	return producedChanges(before, now), nil
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

// flinkLayoutChecks are the deploy's checks for ADR 0012 and 0013. flink_columns_changed says
// that a version which changes the columns a pipeline writes, while it would resume a saved
// position, switches columns at that position (ADR 0013). flink_destination_columns, for a start
// without a saved position or a switch, says what BeziSync would add, and blocks what would write
// columns into the wrong places.
func flinkLayoutChecks(ctx context.Context, q db.Querier, p *models.Pipeline, spec domain.PipelineSpec, source, sink connectors.Resolved) ([]checks.Check, error) {
	var out []checks.Check
	switching := false
	left, known := positionLeft(ctx, p)
	if p.DeployedVersionID != nil && (left || !known) {
		from := writtenVersion(p)
		if from == nil {
			from = p.DeployedVersionID
		}
		changes, err := columnChanges(ctx, q, p, from, spec, source, sink)
		if err != nil {
			return []checks.Check{{Code: "flink_columns_changed", Title: "Columns written", Category: checks.CatDestination, Status: checks.Warn,
				Message: "BeziSync couldn't read the source's columns to compare what this version writes with the running one: " + domain.Truncate(err.Error(), 200)}}, nil
		}
		if len(changes) == 0 {
			return nil, nil // it resumes writing what it wrote
		}
		c := checks.Check{Code: "flink_columns_changed", Title: "Columns written", Category: checks.CatDestination, Status: checks.Pass,
			Message: "This version changes the columns the pipeline writes (" + strings.Join(changes, "; ") + "). The pipeline switches columns at its saved position: " +
				"once the running job has stopped with a savepoint, it resumes from exactly there writing the new columns. Rows written before keep the columns they had."}
		if !known {
			c.Status = checks.Warn
			c.Message = "This version changes the columns the pipeline writes (" + strings.Join(changes, "; ") + "), and BeziSync couldn't read the saved positions: " +
				"with one left the pipeline switches columns at it; without one it starts fresh writing the new columns."
		}
		out = append(out, c)
		switching = left
	}
	when := "The pipeline would start without a saved position and"
	if switching {
		when = "The pipeline would resume with the new columns and"
	}
	var read map[string][]string
	if switching {
		read = storedColumns(p)
	}
	plan, err := planLayout(ctx, q, p, spec, source, sink, read)
	if err != nil {
		return append(out, checks.Check{Code: "flink_destination_columns", Title: "Destination columns", Category: checks.CatDestination, Status: checks.Warn,
			Message: "BeziSync couldn't plan the columns the pipeline writes: " + domain.Truncate(err.Error(), 300)}), nil
	}
	if len(plan.problems) > 0 {
		return append(out, checks.Check{Code: "flink_destination_columns", Title: "Destination columns", Category: checks.CatDestination, Status: checks.Fail, Blocking: true,
			Message: when + " can't write these tables' columns in the right places: " + strings.Join(plan.problems, " "),
			Fix: &checks.Fix{Kind: "text", Who: "you", Code: "Change the steps so the pipeline keeps writing these columns, " +
				"or change the destination tables (or name new ones in the Destination step), then run preflight again."}}), nil
	}
	if snk, err := connectors.GetSink(sink.Type); err == nil {
		if byHand, ok := snk.(connectors.FlinkColumnsByHand); ok {
			var stmts []string
			for _, dest := range layoutKeys(plan.tables) {
				if add := plan.tables[dest].Add; len(add) > 0 {
					stmts = append(stmts, byHand.FlinkAddStatements(dest, add)...)
				}
			}
			if len(stmts) > 0 {
				return append(out, checks.Check{Code: "flink_destination_columns", Title: "Destination columns", Category: checks.CatDestination, Status: checks.Fail, Blocking: true,
					Message: "The pipeline writes columns these tables don't have, and BeziSync can't add them to " + sink.Type + " tables.",
					Fix:     &checks.Fix{Kind: "sql", Who: "data engineer", Code: strings.Join(stmts, "\n")}}), nil
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
		before := "Before the start"
		if switching {
			before = "Before it resumes"
		}
		msg := ""
		if len(adds) > 0 {
			msg = before + " BeziSync adds " + strings.Join(adds, ", ") + " to the destination tables (empty for the rows they have). "
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
