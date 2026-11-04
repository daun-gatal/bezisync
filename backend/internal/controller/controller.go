// Package controller converges runtimes to desired state, observes health and runs jobs.
//
// Several replicas may run; a Postgres advisory lock elects one leader. Everything the
// leader does is level-triggered and idempotent, so a crash or failover mid-tick is safe.
package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/daun-gatal/bezisync/backend/internal/compiler"
	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/flinkhelper"
	"github.com/daun-gatal/bezisync/backend/internal/logging"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/runtime"
	"github.com/daun-gatal/bezisync/backend/internal/services"
)

// LeaderLock is the advisory lock key, shared with the Python controller so the two never
// lead at once.
const LeaderLock = 0x62657A01

// CarriedStatus are status keys written outside the observe loop that a tick keeps.
var CarriedStatus = []string{"counters", "source_impact", "events_per_second", "forced", "scheduled_snapshot", "schema_pending_breaking", "canary",
	"transform_drift"}

const (
	probeEvery  = 30 * time.Second
	sampleEvery = 15 * time.Second
)

// CanaryPoll is how often a pending delivery canary is looked for, and CanaryTailTimeout how
// long one look waits for the destination (tests shorten both).
var (
	CanaryPoll        = 10 * time.Second
	CanaryTailTimeout = 5 * time.Second
)

var log = logging.Logger("bezisync.controller")

// Identity names this controller in job locks.
var Identity = func() string {
	h, _ := os.Hostname()
	return fmt.Sprintf("%s-%d", h, os.Getpid())
}()

// EnsureStateSchema creates Debezium's state schema.
func EnsureStateSchema(ctx context.Context) error {
	c, err := pgx.Connect(ctx, db.NormalizeURL(config.Get().EffectiveStateDatabaseURL()))
	if err != nil {
		return err
	}
	defer func() { _ = c.Close(ctx) }()
	_, err = c.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+compiler.StateSchema)
	return err
}

func compileCtx(p *models.Pipeline, spec domain.PipelineSpec) connectors.CompileContext {
	return connectors.CompileContext{PipelineID: p.ID.String(), ShortID: p.ShortID(), Name: p.Name, Spec: spec, Engine: p.Engine}
}

// ParseHealth reads Quarkus SmallRye health: 200 + UP is up, 503 is down, else unknown.
func ParseHealth(code int, body string) *bool {
	f, t := false, true
	if code == 503 {
		return &f
	}
	if code != 200 {
		return nil
	}
	g, err := pyjson.DecodeGeneric([]byte(body))
	if err != nil {
		return nil
	}
	m, ok := g.(map[string]any)
	if !ok {
		return nil
	}
	status := ""
	if v, ok := m["status"]; ok {
		status = fmt.Sprint(v)
		if v == nil {
			status = "None"
		}
	}
	if strings.ToUpper(status) == "UP" {
		return &t
	}
	return &f
}

// observed is what one look at a workload found.
type observed struct {
	obs      *runtime.Observation
	healthUp *bool
	// healthError is why the health check gave no answer ("" when it answered or was not asked).
	healthError  string
	metrics      *domain.Metrics
	logs         string
	runtimeError string
	// Flink CDC: the helper's status, and why it reports DOWN.
	flink      *flinkhelper.Status
	engineDown *domain.Reason
}

func observeWorkload(ctx context.Context, d runtime.Driver, derr error, p *models.Pipeline, healthPort int, flink bool) observed {
	if derr != nil {
		return observed{runtimeError: domain.Truncate(derr.Error(), 300)}
	}
	obs, err := d.Observe(ctx, p.ShortID())
	if err != nil {
		return observed{runtimeError: domain.Truncate(err.Error(), 300)}
	}
	o := observed{obs: &obs}
	if obs.Phase == runtime.PhaseRunning && flink {
		observeFlink(ctx, d, p.ShortID(), healthPort, &o)
	} else if obs.Phase == runtime.PhaseRunning {
		code, body, err := d.HTTPGet(ctx, p.ShortID(), healthPort, "/q/health", 4*time.Second)
		switch {
		case err != nil:
			o.healthError = domain.Truncate(err.Error(), 300)
		default:
			if o.healthUp = ParseHealth(code, body); o.healthUp == nil {
				o.healthError = fmt.Sprintf("Debezium Server answered HTTP %d on port %d.", code, healthPort)
				if code == 200 {
					o.healthError = fmt.Sprintf("Debezium Server's answer on port %d is not a health report.", healthPort)
				}
			}
		}
		if code, body, err := d.HTTPGet(ctx, p.ShortID(), 9404, "/metrics", 4*time.Second); err == nil && code == 200 {
			m := domain.ParseMetrics(body)
			o.metrics = &m
		}
	}
	if obs.Phase == runtime.PhaseRestarting || obs.Phase == runtime.PhaseFailed || (o.healthUp != nil && !*o.healthUp) ||
		(o.metrics != nil && o.metrics.Connected != nil && !*o.metrics.Connected) {
		text, err := d.Logs(ctx, p.ShortID(), 400, obs.Phase == runtime.PhaseRestarting)
		if err == nil && domain.PyStrip(text) == "" {
			text, err = d.Logs(ctx, p.ShortID(), 400, false)
		}
		if err == nil {
			o.logs = text
		}
	}
	return o
}

// Controller is one controller process.
type Controller struct {
	Tick          time.Duration
	lastRetention time.Time
	checkedHashes bool
}

// New creates a controller with the configured tick.
func New() *Controller {
	return &Controller{Tick: time.Duration(config.Get().ControllerTickSeconds * float64(time.Second))}
}

// RunForever competes for leadership and leads while holding the lock.
func (c *Controller) RunForever(ctx context.Context) error {
	if err := EnsureStateSchema(ctx); err != nil {
		log.Error("cannot create the state schema", "error", err.Error())
	}
	for ctx.Err() == nil {
		err := func() error {
			conn, err := pgx.Connect(ctx, db.NormalizeURL(config.Get().DatabaseURL))
			if err != nil {
				return err
			}
			defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
			var got bool
			if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", LeaderLock).Scan(&got); err != nil {
				return err
			}
			if !got {
				sleep(ctx, c.Tick)
				return nil
			}
			log.Info("controller " + Identity + " is leader")
			return c.lead(ctx, conn)
		}()
		if err != nil && ctx.Err() == nil {
			log.Error("controller loop error; retrying", "exc", err.Error())
			sleep(ctx, c.Tick)
		}
	}
	return nil
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func (c *Controller) lead(ctx context.Context, lock *pgx.Conn) error {
	for ctx.Err() == nil {
		started := time.Now()
		if _, err := lock.Exec(ctx, "SELECT 1"); err != nil { // keep the session (and the lock) alive
			return err
		}
		c.TickOnce(ctx)
		sleep(ctx, max(200*time.Millisecond, c.Tick-time.Since(started)))
	}
	return nil
}

// TickOnce is one reconciliation round.
func (c *Controller) TickOnce(ctx context.Context) {
	pool := db.MustPool(ctx)
	ids, err := db.Select[struct {
		ID uuid.UUID `db:"id"`
	}](ctx, pool, `SELECT id FROM pipelines WHERE deleted_at IS NULL AND deployed_version_id IS NOT NULL`)
	if err != nil {
		log.Error("cannot list pipelines", "exc", err.Error())
		return
	}
	if !c.checkedHashes {
		c.checkedHashes = true
		c.reportHashDrift(ctx, ids)
	}
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	for _, row := range ids {
		wg.Add(1)
		sem <- struct{}{}
		go func(id uuid.UUID) {
			defer wg.Done()
			defer func() { <-sem }()
			defer func() {
				if rv := recover(); rv != nil {
					log.Error("reconcile failed", "pipeline_id", id.String(), "exc", fmt.Sprint(rv))
				}
			}()
			if err := c.Reconcile(ctx, id); err != nil {
				log.Error("reconcile failed", "pipeline_id", id.String(), "exc", err.Error())
			}
		}(row.ID)
	}
	wg.Wait()
	c.EvaluateAlerts(ctx)
	c.RunJobs(ctx, 10)
	if time.Since(c.lastRetention) > 10*time.Minute {
		c.lastRetention = time.Now()
		if err := db.Tx(ctx, func(tx pgx.Tx) error {
			_, err := services.EnforceMetricRetention(ctx, tx, db.Now())
			return err
		}); err != nil {
			log.Error("metrics retention failed", "exc", err.Error())
		}
		if err := db.Tx(ctx, func(tx pgx.Tx) error {
			_, err := services.MaybeSendTelemetry(ctx, tx, db.Now())
			return err
		}); err != nil {
			log.Error("telemetry failed", "exc", err.Error())
		}
		if _, err := services.ExpireSavedPositions(ctx, db.MustPool(ctx), db.Now()); err != nil {
			log.Error("saved position retention failed", "exc", err.Error())
		}
	}
}

// reportHashDrift warns, once per start, about workloads whose config hash would change on
// this tick (a cutover safety net: identical compilation keeps every hash).
func (c *Controller) reportHashDrift(ctx context.Context, ids []struct {
	ID uuid.UUID `db:"id"`
}) {
	pool := db.MustPool(ctx)
	var drifted []string
	for _, row := range ids {
		p, err := db.Find[models.Pipeline](ctx, pool, `SELECT * FROM pipelines WHERE id = $1`, row.ID)
		if err != nil || p == nil || p.RuntimeID == nil {
			continue
		}
		w, err := services.WorkloadFor(ctx, pool, p)
		if err != nil || w == nil {
			continue
		}
		rt, err := services.GetRuntime(ctx, pool, *p.RuntimeID)
		if err != nil || rt == nil {
			continue
		}
		d, err := services.DriverFor(ctx, pool, rt)
		if err != nil {
			continue
		}
		obs, err := d.Observe(ctx, p.ShortID())
		if err != nil || obs.ConfigHash == nil || obs.Phase == runtime.PhaseMissing {
			continue
		}
		if *obs.ConfigHash != w.ConfigHash {
			drifted = append(drifted, p.Name)
		}
	}
	if len(drifted) > 0 {
		log.Warn(fmt.Sprintf("%d running connector(s) will be replaced because their configuration changed: %s",
			len(drifted), strings.Join(drifted, ", ")))
	}
}

// EvaluateAlerts advances alert rules of every project.
func (c *Controller) EvaluateAlerts(ctx context.Context) {
	pool := db.MustPool(ctx)
	projects, err := db.Select[struct {
		ID uuid.UUID `db:"id"`
	}](ctx, pool, `SELECT id FROM projects`)
	if err != nil {
		log.Error("cannot list projects", "exc", err.Error())
		return
	}
	for _, p := range projects {
		if err := db.Tx(ctx, func(tx pgx.Tx) error {
			_, err := services.EvaluateProject(ctx, tx, p.ID, db.Now())
			return err
		}); err != nil {
			log.Error("alert evaluation failed", "project_id", p.ID.String(), "exc", err.Error())
		}
	}
}

func statusCopy(p *models.Pipeline) db.Map {
	out := db.Map{}
	for k, v := range p.Status {
		out[k] = v
	}
	return out
}

func parseISO(v any) (time.Time, bool) {
	s, ok := v.(string)
	if !ok || s == "" {
		return time.Time{}, false
	}
	t, err := pytime.Parse(s)
	return t, err == nil
}

// Reconcile converges one pipeline and records its observed state.
func (c *Controller) Reconcile(ctx context.Context, id uuid.UUID) error {
	return db.Tx(ctx, func(tx pgx.Tx) error {
		p, err := db.Find[models.Pipeline](ctx, tx, `SELECT * FROM pipelines WHERE id = $1 FOR UPDATE SKIP LOCKED`, id)
		if err != nil || p == nil || p.DeletedAt != nil || p.DeployedVersionID == nil {
			return err
		}
		if p.RuntimeID == nil {
			return errors.New("pipeline has no runtime")
		}
		rt, err := services.GetRuntime(ctx, tx, *p.RuntimeID)
		if err != nil {
			return err
		}
		if rt == nil {
			return errors.New("runtime vanished")
		}
		specPtr, err := services.SpecOf(ctx, tx, p.DeployedVersionID)
		if err != nil || specPtr == nil {
			return err
		}
		spec := *specPtr
		applyError := ""
		d, derr := services.DriverFor(ctx, tx, rt)
		func() {
			if err := c.prepareIfNeeded(ctx, tx, p, spec); err != nil {
				applyError = domain.Truncate(err.Error(), 500)
				return
			}
			w, err := services.WorkloadFor(ctx, tx, p)
			if err != nil {
				applyError = domain.Truncate(errText(err), 500)
				return
			}
			if derr != nil {
				applyError = domain.Truncate(derr.Error(), 500)
				return
			}
			if err := d.Apply(ctx, *w); err != nil {
				applyError = domain.Truncate(err.Error(), 500)
			}
		}()
		if applyError != "" {
			log.Warn(fmt.Sprintf("apply failed for %s: %s", p.Name, applyError))
		}
		flink := services.EngineOf(p) == engine.FlinkCDC
		healthPort := connectors.DefaultHealthPort
		if sinkType, err := services.SinkTypeOf(ctx, tx, p); err == nil && p.DeployedImage != nil && !flink {
			healthPort = connectors.HealthPort(sinkType, *p.DeployedImage)
		}
		seen := observeWorkload(ctx, d, derr, p, healthPort, flink)
		obs, healthUp, metrics, logs, runtimeError := seen.obs, seen.healthUp, seen.metrics, seen.logs, seen.runtimeError
		if metrics != nil {
			metrics.OnlyChosenTables(len(spec.Source.Tables.Include))
		}
		impact := c.probeSource(ctx, tx, p, spec)
		if err := c.trackSchema(ctx, tx, p, spec); err != nil {
			return err
		}
		c.canary(ctx, tx, p, spec)
		positionNote := c.checkPosition(ctx, p, impact)
		var risk *domain.Reason
		if impact != nil && truthy(impact["risk"]) {
			sev := "warn"
			if truthy(impact["lost"]) {
				sev = "error"
			}
			risk = &domain.Reason{Code: "source_at_risk", Message: fmt.Sprint(impact["risk"]), Fix: strOf(impact["risk_fix"]), Severity: sev}
		}
		latencyTarget := float64(spec.Policies.LatencyTargetSeconds * 1000)
		if flink {
			// Changes wait for the next save (Iceberg commits then) or load on purpose.
			fc := spec.FlinkCDC.Effective()
			latencyTarget = max(latencyTarget, 1.5*float64(max(fc.CommitSeconds, fc.FreshnessSeconds)*1000))
		} else if sinkType, err := services.SinkTypeOf(ctx, tx, p); err == nil {
			// A sink that commits at an interval (Iceberg) holds changes back on purpose.
			latencyTarget = max(latencyTarget, 1.5*float64(connectors.DeliveryDelay(sinkType, spec).Milliseconds()))
		}
		in := domain.DeriveInput{Now: db.Now(), Deployed: true, Desired: p.DesiredState, DeployedAt: p.DeployedAt, HealthUp: healthUp, HealthError: seen.healthError,
			Metrics: metrics, Logs: logs, LatencyTargetMS: latencyTarget, RuntimeError: runtimeError,
			SourceRisk: risk, LastLifecycle: p.Lifecycle, EngineDown: seen.engineDown}
		if obs != nil {
			in.Phase, in.Restarts, in.WorkloadReason, in.ImagePull, in.StartedAt = obs.Phase, obs.Restarts, obs.Reason, obs.ImagePull, obs.StartedAt
		}
		verdict := domain.Derive(in)
		if applyError != "" && verdict.Lifecycle != domain.LifecycleStreaming {
			verdict.Reasons = append([]domain.Reason{{Code: "apply_failed", Message: applyError, Severity: "error"}}, verdict.Reasons...)
			if verdict.Lifecycle == domain.LifecycleDeploying && p.DeployedAt != nil && db.Now().Sub(*p.DeployedAt) > 2*time.Minute {
				verdict.Lifecycle, verdict.Health, verdict.Summary = domain.LifecycleFailed, domain.HealthDown, applyError
			}
		}
		if err := c.storeStatus(ctx, tx, p, verdict, obs, metrics, spec); err != nil {
			return err
		}
		if impact != nil {
			st := statusCopy(p)
			st["source_impact"] = impact
			p.Status = st
		}
		if seen.flink != nil {
			st := statusCopy(p)
			st["flink"] = flinkStatus(seen.flink, obs, spec.Source.Tables.Include)
			p.Status = st
		}
		if positionNote != nil {
			st := statusCopy(p)
			for k, v := range positionNote {
				st[k] = v
			}
			p.Status = st
		}
		if err := c.recordSample(ctx, tx, p, metrics, impact, verdict.Health == domain.HealthHealthy); err != nil {
			return err
		}
		if err := services.SavePipeline(ctx, tx, p); err != nil {
			return err
		}
		if rt.Mode != "agent" && runtimeError == "" {
			// Agent runtimes: only the agent's own poll counts.
			if _, err := tx.Exec(ctx, `UPDATE runtimes SET last_seen_at = $1 WHERE id = $2`, db.Now(), rt.ID); err != nil {
				return err
			}
		}
		return nil
	})
}

func errText(err error) string {
	type detailed interface{ Error() string }
	var d detailed = err
	return d.Error()
}

func truthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case string:
		return x != ""
	case json.Number:
		f, _ := x.Float64()
		return f != 0
	case int64:
		return x != 0
	case int:
		return x != 0
	case float64:
		return x != 0
	}
	return true
}

func strOf(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

func (c *Controller) trackSchema(ctx context.Context, tx pgx.Tx, p *models.Pipeline, spec domain.PipelineSpec) error {
	source, err := services.GetConnection(ctx, tx, p.SourceID)
	if err != nil || source == nil {
		return err
	}
	desc, derr := connectors.Describe("source", source.Type)
	if derr != nil || !desc.TracksSchema || (p.Lifecycle != domain.LifecycleStreaming && p.Lifecycle != domain.LifecycleSnapshotting) {
		return nil //nolint:nilerr // unknown connector types simply have no schema tracking
	}
	artifacts := db.Map{}
	for k, v := range p.Artifacts {
		artifacts[k] = v
	}
	state, _ := artifacts["schema"].(map[string]any)
	if state == nil {
		state = map[string]any{}
	}
	now := db.Now()
	every := time.Duration(config.Get().SchemaCheckSeconds) * time.Second
	if checked, ok := parseISO(state["checked_at"]); ok && now.Sub(checked) < every {
		return nil
	}
	wanted := map[string]bool{}
	for _, t := range spec.Source.Tables.Include {
		wanted[t] = true
	}
	src, _ := connectors.GetSource(source.Type)
	resolved, err := services.Resolve(ctx, tx, source)
	var found connectors.Discovery
	if err == nil {
		found, err = src.Discover(ctx, resolved)
	}
	if err != nil {
		st := map[string]any{}
		for k, v := range state {
			st[k] = v
		}
		st["checked_at"], st["error"] = pytime.ISOFormat(now), domain.Truncate(err.Error(), 300)
		artifacts["schema"] = st
		p.Artifacts = artifacts
		return nil
	}
	current := domain.Schema{}
	currentJSON := map[string]any{}
	for _, t := range found.Tables {
		name := t.Identifier()
		if !wanted[name] {
			continue
		}
		cols := map[string]map[string]any{}
		colsJSON := map[string]any{}
		for _, col := range t.Columns {
			cols[col.Name] = map[string]any{"type": col.Type, "nullable": col.Nullable}
			colsJSON[col.Name] = map[string]any{"type": col.Type, "nullable": col.Nullable}
		}
		current[name] = cols
		currentJSON[name] = colsJSON
	}
	if previous, ok := state["tables"].(map[string]any); ok {
		old := domain.Schema{}
		for t, v := range previous {
			if !wanted[t] {
				continue
			}
			cols := map[string]map[string]any{}
			for cn, cv := range mapOf(v) {
				cols[cn] = mapOf(cv)
			}
			old[t] = cols
		}
		for _, ch := range domain.DiffSchema(old, current) {
			if err := db.Insert(ctx, tx, "schema_changes", &models.SchemaChange{ID: db.NewID(), PipelineID: p.ID, Table: ch.Table, Kind: ch.Kind,
				Column: ch.Column, Before: ch.Before, After: ch.After, Breaking: ch.Breaking, DetectedAt: now}); err != nil {
				return err
			}
		}
	}
	artifacts["schema"] = map[string]any{"checked_at": pytime.ISOFormat(now), "tables": currentJSON}
	p.Artifacts = artifacts
	pending, err := db.Scalar[int64](ctx, tx, `SELECT count(*) FROM schema_changes WHERE pipeline_id = $1 AND breaking IS TRUE AND reviewed_at IS NULL`, p.ID)
	if err != nil {
		return err
	}
	st := statusCopy(p)
	st["schema_pending_breaking"] = pending
	st["transform_drift"] = transformDrift(source.Type, p.Name, spec, current)
	p.Status = st
	return nil
}

// transformDrift is what the deployed steps would do with the schema just read: messages for
// steps whose columns are gone, rules that cannot match, fields that would collide.
func transformDrift(sourceType, pipeline string, spec domain.PipelineSpec, current domain.Schema) []any {
	if !services.NeedsColumns(spec) {
		return []any{}
	}
	tables := map[string]domain.TableColumns{}
	for table, cols := range current {
		names := make([]string, 0, len(cols))
		for name := range cols {
			names = append(names, name)
		}
		sort.Strings(names)
		tc := domain.TableColumns{}
		for _, name := range names {
			typ, _ := cols[name]["type"].(string)
			tc.Columns = append(tc.Columns, domain.PreviewColumn{Name: name, Type: typ})
		}
		tables[table] = tc
	}
	out := []any{}
	for _, f := range domain.TransformFindings(sourceType, pipeline, spec, tables) {
		out = append(out, map[string]any{"code": f.Code, "fail": f.Fail, "table": f.Table, "step": f.Step, "message": f.Message})
	}
	return out
}

func mapOf(v any) map[string]any {
	m, _ := v.(map[string]any)
	if m == nil {
		return map[string]any{}
	}
	return m
}

func (c *Controller) canary(ctx context.Context, tx pgx.Tx, p *models.Pipeline, spec domain.PipelineSpec) {
	if !spec.Policies.DeliveryCanary || p.Lifecycle != domain.LifecycleStreaming || p.SinkID == nil {
		return
	}
	sourceConn, err := services.GetConnection(ctx, tx, p.SourceID)
	if err != nil || sourceConn == nil {
		return
	}
	sinkConn, err := services.GetConnection(ctx, tx, *p.SinkID)
	if err != nil || sinkConn == nil {
		return
	}
	srcDesc, _ := connectors.Describe("source", sourceConn.Type)
	sinkDesc, _ := connectors.Describe("sink", sinkConn.Type)
	if srcDesc == nil || sinkDesc == nil || !srcDesc.SupportsCanary || !sinkDesc.Canary {
		return
	}
	source, _ := connectors.GetSource(sourceConn.Type)
	sink, _ := connectors.GetSink(sinkConn.Type)
	s, now := config.Get(), db.Now()
	state := map[string]any{}
	for k, v := range mapOf(p.Status["canary"]) {
		state[k] = v
	}
	cctx := compileCtx(p, spec)
	if pending := mapOf(state["pending"]); len(pending) > 0 {
		polled, ok := parseISO(state["polled_at"])
		if !ok || now.Sub(polled) >= CanaryPoll {
			state["polled_at"] = pytime.ISOFormat(now)
			var records []connectors.TailRecord
			var destination string
			sourceResolved, err := services.Resolve(ctx, tx, sourceConn)
			if err == nil {
				var table string
				if table, err = source.CanaryTable(sourceResolved, cctx); err == nil {
					schema, name, _ := strings.Cut(table, ".")
					destination = cctx.Destination(schema, name)
				}
			}
			var resolved connectors.Resolved
			if err == nil {
				resolved, err = services.Resolve(ctx, tx, sinkConn)
			}
			if err == nil {
				destination = connectors.SinkDestination(sink, resolved, cctx, destination)
				records, err = sink.Tail(ctx, resolved, destination, 20, CanaryTailTimeout)
			}
			if err != nil {
				records = nil
				state["error"] = "Cannot read the destination: " + domain.Truncate(err.Error(), 200)
			} else {
				delete(state, "error")
			}
			sent, _ := parseISO(pending["sent_at"])
			token := strOf(pending["token"])
			found := false
			for _, r := range records {
				raw, _ := json.Marshal(r.Value)
				if strings.Contains(string(raw), token) {
					found = true
					break
				}
			}
			switch {
			case found:
				state["ok"], state["verified_at"] = true, pytime.ISOFormat(now)
				state["latency_ms"] = int64(now.Sub(sent).Seconds() * 1000)
				delete(state, "pending")
			case now.Sub(sent) > time.Duration(s.CanaryTimeoutSeconds)*time.Second:
				state["ok"] = false
			}
		}
	}
	last, ok := parseISO(state["sent_at"])
	due := !ok || now.Sub(last) >= time.Duration(s.CanaryIntervalSeconds)*time.Second
	if _, hasPending := state["pending"]; !truthy(state["pending"]) && due {
		_ = hasPending
		token := strings.ReplaceAll(uuid.NewString(), "-", "")
		resolved, err := services.Resolve(ctx, tx, sourceConn)
		if err == nil {
			err = source.WriteCanary(ctx, resolved, cctx, token)
		}
		if err == nil {
			state["pending"] = map[string]any{"token": token, "sent_at": pytime.ISOFormat(now)}
			state["sent_at"] = pytime.ISOFormat(now)
			delete(state, "error")
		} else {
			state["error"] = "Cannot write the canary: " + domain.Truncate(err.Error(), 200)
			state["sent_at"] = pytime.ISOFormat(now)
		}
	}
	st := statusCopy(p)
	st["canary"] = state
	p.Status = st
}

func (c *Controller) probeSource(ctx context.Context, tx pgx.Tx, p *models.Pipeline, spec domain.PipelineSpec) map[string]any {
	if previous := mapOf(p.Status["source_impact"]); len(previous) > 0 {
		if at, ok := parseISO(previous["at"]); ok && db.Now().Sub(at) < probeEvery {
			out := map[string]any{}
			for k, v := range previous {
				out[k] = v
			}
			return out
		}
	}
	failure := func(err error) map[string]any {
		return map[string]any{"at": pytime.ISOFormat(db.Now()), "error": domain.Truncate(err.Error(), 300), "summary": "Could not measure the source."}
	}
	sourceConn, err := services.GetConnection(ctx, tx, p.SourceID)
	if err != nil || sourceConn == nil {
		if err == nil {
			err = errors.New("source vanished")
		}
		return failure(err)
	}
	src, err := connectors.GetSource(sourceConn.Type)
	if err != nil {
		return failure(err)
	}
	resolved, err := services.Resolve(ctx, tx, sourceConn)
	if err != nil {
		return failure(err)
	}
	limit := int64(float64(spec.Policies.SourceRetainedLimitGB) * 1024 * 1024 * 1024)
	impact, err := src.Probe(ctx, resolved, compileCtx(p, spec), limit)
	if err != nil {
		return failure(err)
	}
	if impact == nil {
		return nil
	}
	m, _ := services.ToMap(impact)
	out := map[string]any{"at": pytime.ISOFormat(db.Now())}
	for k, v := range m {
		out[k] = v
	}
	return out
}

func (c *Controller) checkPosition(ctx context.Context, p *models.Pipeline, impact map[string]any) map[string]any {
	if impact == nil || strOf(mapOf(p.Status["source_impact"])["at"]) == strOf(impact["at"]) {
		return nil
	}
	rows, err := services.ReadOffsets(ctx, p.ShortID())
	if err != nil {
		return nil
	}
	var lsn *int64
	for _, r := range rows {
		raw, ok := r["raw"].(map[string]any)
		if !ok {
			continue
		}
		if v, ok := raw["lsn"]; ok {
			n, err := strconv.ParseInt(strOf(v), 10, 64)
			if err == nil {
				lsn = &n
			}
			break
		}
	}
	if lsn == nil {
		return nil
	}
	note := map[string]any{"last_lsn": *lsn, "position_regressed": nil}
	if prevV, ok := p.Status["last_lsn"]; ok && prevV != nil {
		prev, err := strconv.ParseInt(strOf(prevV), 10, 64)
		if err == nil && *lsn < prev {
			note["position_regressed"] = fmt.Sprintf("Stored position moved from %s back to %s.", services.PgLSN(uint64(prev)), services.PgLSN(uint64(*lsn)))
		}
	}
	return note
}

func (c *Controller) recordSample(ctx context.Context, tx pgx.Tx, p *models.Pipeline, m *domain.Metrics, impact map[string]any, healthy bool) error {
	now := db.Now()
	counters := mapOf(p.Status["counters"])
	lastAt, hasLast := parseISO(counters["at"])
	if hasLast && now.Sub(lastAt) < sampleEvery {
		return nil
	}
	var total *float64
	if m != nil {
		t := 0.0
		if m.EventsSeenStreaming != nil {
			t += *m.EventsSeenStreaming
		}
		if m.EventsSeenSnapshot != nil {
			t += *m.EventsSeenSnapshot
		}
		total = &t
	}
	var eps *float64
	if prevTotal, ok := counters["total"]; total != nil && ok && prevTotal != nil && hasLast {
		elapsed := now.Sub(lastAt).Seconds()
		delta := *total - toFloat(prevTotal)
		if elapsed > 0 && delta >= 0 {
			v := math.RoundToEven(delta/elapsed*1000) / 1000
			eps = &v
		}
	}
	var queue *float64
	if m != nil && m.QueueTotal != nil && *m.QueueTotal != 0 {
		rem := 0.0
		if m.QueueRemaining != nil {
			rem = *m.QueueRemaining
		}
		v := 1 - rem/(*m.QueueTotal)
		queue = &v
	}
	var lag, retained *float64
	if m != nil {
		lag = m.MSBehindSource
	}
	if v, ok := impact["retained_bytes"]; ok && v != nil {
		f := toFloat(v)
		retained = &f
	}
	if _, err := tx.Exec(ctx, `INSERT INTO metric_samples (pipeline_id, at, resolution, events_per_second, lag_ms, events_total, queue_used_ratio, source_retained_bytes, healthy)
		VALUES ($1,$2,0,$3,$4,$5,$6,$7,$8)`, p.ID, now, eps, lag, total, queue, retained, healthy); err != nil {
		return err
	}
	st := statusCopy(p)
	var totalV, epsV any
	if total != nil {
		totalV = pyjson.Float(*total)
	}
	if eps != nil {
		epsV = pyjson.Float(*eps)
	}
	st["counters"] = map[string]any{"at": pytime.ISOFormat(now), "total": totalV}
	st["events_per_second"] = epsV
	p.Status = st
	return nil
}

func toFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int64:
		return float64(x)
	case int:
		return float64(x)
	case json.Number:
		f, _ := x.Float64()
		return f
	case pyjson.Float:
		return float64(x)
	}
	return 0
}

func (c *Controller) prepareIfNeeded(ctx context.Context, tx pgx.Tx, p *models.Pipeline, spec domain.PipelineSpec) error {
	if strOf(p.Artifacts["prepared_version_id"]) == p.DeployedVersionID.String() {
		return nil
	}
	cctx := compileCtx(p, spec)
	sourceConn, err := services.GetConnection(ctx, tx, p.SourceID)
	if err != nil {
		return err
	}
	src, err := connectors.GetSource(sourceConn.Type)
	if err != nil {
		return err
	}
	resolvedSrc, err := services.Resolve(ctx, tx, sourceConn)
	if err != nil {
		return err
	}
	sourceResults, err := src.Prepare(ctx, resolvedSrc, cctx)
	if err != nil {
		return err
	}
	if services.EngineOf(p) == engine.FlinkCDC {
		// The deployed version writes the columns its tables have now.
		if err := services.RefreshFlinkColumns(ctx, tx, p, spec); err != nil {
			return err
		}
	}
	compiled, err := services.CompileFor(ctx, tx, p, spec, "")
	if err != nil {
		return err
	}
	sinkConn, err := services.GetConnection(ctx, tx, *p.SinkID)
	if err != nil {
		return err
	}
	snk, err := connectors.GetSink(sinkConn.Type)
	if err != nil {
		return err
	}
	resolvedSink, err := services.Resolve(ctx, tx, sinkConn)
	if err != nil {
		return err
	}
	sinkResults, err := snk.PrepareSink(ctx, resolvedSink, cctx, compiled.Destinations)
	if err != nil {
		return err
	}
	for _, r := range sinkResults {
		if r.Outcome == "failed" {
			return fmt.Errorf("Could not prepare destination %s: %s", r.Name, r.Detail)
		}
	}
	artifacts := db.Map{}
	for k, v := range p.Artifacts {
		artifacts[k] = v
	}
	toList := func(rs []connectors.ArtifactResult) []any {
		out := make([]any, len(rs))
		for i, r := range rs {
			m, _ := services.ToMap(r)
			out[i] = m
		}
		return out
	}
	dest := map[string]any{}
	for k, v := range compiled.Destinations {
		dest[k] = v
	}
	artifacts["prepared_version_id"] = p.DeployedVersionID.String()
	artifacts["source"] = toList(sourceResults)
	artifacts["sink"] = toList(sinkResults)
	artifacts["destinations"] = dest
	p.Artifacts = artifacts
	return nil
}

func reasonsList(rs []domain.Reason) []any {
	out := make([]any, len(rs))
	for i, r := range rs {
		out[i] = map[string]any{"code": r.Code, "message": r.Message, "fix": r.Fix, "severity": r.Severity}
	}
	return out
}

func (c *Controller) storeStatus(ctx context.Context, tx pgx.Tx, p *models.Pipeline, v domain.Verdict, obs *runtime.Observation, m *domain.Metrics, spec domain.PipelineSpec) error {
	previous := strOf(p.Status["lifecycle"])
	st := db.Map{}
	for _, k := range CarriedStatus {
		if val, ok := p.Status[k]; ok {
			st[k] = val
		}
	}
	version, err := services.VersionOf(ctx, tx, p.DeployedVersionID)
	if err != nil {
		return err
	}
	var deployedVersion any
	if version != nil {
		deployedVersion = version.Number
	}
	workload := map[string]any{"phase": nil, "restarts": nil, "started_at": nil, "reason": nil}
	if obs != nil {
		workload["phase"], workload["restarts"], workload["reason"] = obs.Phase, obs.Restarts, obs.Reason
		if obs.StartedAt != nil {
			workload["started_at"] = pytime.ISOFormat(*obs.StartedAt)
		}
	}
	var metrics any
	if m != nil {
		metrics = m.AsDict()
	}
	st["lifecycle"], st["health"], st["summary"] = v.Lifecycle, v.Health, v.Summary
	st["reasons"] = reasonsList(v.Reasons)
	st["as_of"] = pytime.ISOFormat(time.Now())
	st["deployed_version"] = deployedVersion
	st["workload"] = workload
	st["metrics"] = metrics
	st["tables"] = len(spec.Source.Tables.Include)
	st["latency_target_seconds"] = spec.Policies.LatencyTargetSeconds
	p.Status = st
	p.Lifecycle, p.Health = v.Lifecycle, v.Health
	if previous != v.Lifecycle {
		summary := fmt.Sprintf("%s is %s", p.Name, v.Lifecycle)
		if v.Lifecycle == domain.LifecycleFailed {
			summary += ": " + v.Summary
		}
		reasons := reasonsList(v.Reasons)
		if len(reasons) > 3 {
			reasons = reasons[:3]
		}
		pid := p.ProjectID
		if _, err := services.Record(ctx, tx, services.AuditEvent{Kind: "pipeline." + v.Lifecycle, ProjectID: &pid, Summary: summary,
			ResourceType: "pipeline", ResourceID: &p.ID, NotAudit: true, Details: map[string]any{"reasons": reasons}}); err != nil {
			return err
		}
	}
	return nil
}

// Main runs the controller until the context ends.
func Main(ctx context.Context) error {
	c := New()
	log.Info(fmt.Sprintf("controller %s starting (tick %.1fs)", Identity, c.Tick.Seconds()))
	return c.RunForever(ctx)
}
