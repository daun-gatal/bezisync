package services

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/runtime"
)

// Preflight checks only Flink CDC pipelines have: what the engine needs from the tables, the
// source, the runtime and its agent. They run with the rest and block a deploy the same way.

// FlinkChecks are a Flink CDC pipeline's own preflight checks.
func FlinkChecks(ctx context.Context, q db.Querier, p *models.Pipeline, spec domain.PipelineSpec, rt *models.Runtime,
	sourceConn *models.Connection, discover *discoverOnce, sourceReport checks.Report) ([]checks.Check, error) {
	var out []checks.Check
	if c := flinkAgentCheck(rt); c != nil {
		out = append(out, *c)
	}
	packed := FlinkSizeOf(p, spec, RuntimeCapacityFor(p, spec, rt))
	if c := flinkServicesCheck(ctx, q, rt); c != nil {
		out = append(out, *c)
	}
	found, err := discover.get(ctx)
	if err != nil {
		out = append(out, checks.Check{Code: "flink_tables_unknown", Title: "Tables for Flink CDC", Category: checks.CatTables, Status: checks.Warn,
			Message: "Could not read the tables to check what Flink CDC needs from them: " + domain.Truncate(pyErrString(err), 200)})
	} else {
		out = append(out, flinkTableChecks(sourceConn.Type, spec, found)...)
	}
	if sourceConn.Type == "postgresql" {
		if c := flinkSlotsCheck(p, spec, packed.Size, sourceReport); c != nil {
			out = append(out, *c)
		}
	}
	if sourceConn.Type == "mysql" {
		c, err := flinkServerIDCheck(ctx, q, p, packed.Size)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	if src, err := connectors.GetSource(sourceConn.Type); err == nil {
		if checker, ok := src.(connectors.FlinkSourceChecker); ok {
			out = append(out, checker.FlinkChecks(ctx, discover.src, flinkCheckFacts(ctx, p, spec))...)
		}
	}
	return out, nil
}

// flinkCheckFacts: whether the pipeline copies when it starts without a saved position (Start
// over's choice until the next deploy, else its snapshot mode) and whether this deploy will.
func flinkCheckFacts(ctx context.Context, p *models.Pipeline, spec domain.PipelineSpec) connectors.FlinkCheckFacts {
	copies := spec.Snapshot.Mode != "no_data"
	if startup, _ := p.Artifacts[ArtifactStartup].(map[string]any); startup != nil {
		if m, _ := startup["mode"].(string); m != "" {
			copies = m == "initial"
		}
	}
	left, known := positionLeft(ctx, p)
	return connectors.FlinkCheckFacts{Tables: spec.Source.Tables.Include, Copies: copies, CopyNow: copies && (!left || !known)}
}

// flinkAgentCheck: an agent from before Flink CDC would never get the workload.
func flinkAgentCheck(rt *models.Runtime) *checks.Check {
	if rt.Mode != "agent" || rt.AgentState == nil || rt.AgentState["version"] == nil {
		return nil // never connected: the runtime's own check says so
	}
	if hasFeature(rt.AgentState, runtime.FeatureFlinkCDC) {
		return nil
	}
	return &checks.Check{Code: "flink_agent_version", Title: "Agent version", Category: checks.CatCapacity, Status: checks.Fail, Blocking: true,
		Message: fmt.Sprintf("The agent on %s (%v) can't run Flink CDC pipelines.", rt.Name, rt.AgentState["version"]),
		Fix: &checks.Fix{Kind: "text", Who: "runtime admin",
			Code: "Upgrade the agent to BeziSync " + FlinkCDCAgentVersion + " or later (Settings → Runtimes shows the install command)."}}
}

// flinkServicesRBAC is the Role rule a Kubernetes runtime needs for Flink CDC pipelines.
const flinkServicesRBAC = `- apiGroups: [""]
  resources: [configmaps, secrets, services]
  verbs: [get, list, create, update, patch, delete]`

// flinkServicesCheck: on Kubernetes a Flink CDC pipeline's TaskManagers reach its JobManager
// through a Service.
func flinkServicesCheck(ctx context.Context, q db.Querier, rt *models.Runtime) *checks.Check {
	if rt.Kind != "kubernetes" {
		return nil
	}
	var allowed *bool
	if rt.Mode == "agent" {
		local, _ := rt.AgentState["local_test"].(map[string]any)
		if v, ok := local["flink_cdc"].(bool); ok {
			allowed = &v
		}
	} else if d, err := DriverFor(ctx, q, rt); err == nil {
		if res := d.Test(ctx); res.OK {
			if v, ok := res.Details["flink_cdc"].(bool); ok {
				allowed = &v
			}
		}
	}
	if allowed == nil || *allowed {
		return nil
	}
	return &checks.Check{Code: "flink_kubernetes_services", Title: "Kubernetes Services", Category: checks.CatPermissions, Status: checks.Fail,
		Blocking: true, Message: fmt.Sprintf("BeziSync may not create Services on %s; a Flink CDC pipeline's TaskManagers reach its JobManager through one.", rt.Name),
		Fix: &checks.Fix{Kind: "yaml", Who: "cluster admin", Code: "# The runtime's Role (both BeziSync charts from 0.2.0 grant it)\n" + flinkServicesRBAC}}
}

// flinkTableChecks: every table needs a key the destination can upsert by, and PostgreSQL tables
// must log whole rows (Flink CDC fails on an update without the row's previous values).
func flinkTableChecks(sourceType string, spec domain.PipelineSpec, found connectors.Discovery) []checks.Check {
	tables := TablesOf(found, spec.Source.Tables.Include)
	var noKey, notFull []string
	for _, t := range spec.Source.Tables.Include {
		info, ok := tables[t]
		if !ok {
			continue // a missing table fails in the source checks
		}
		if len(info.PrimaryKey) == 0 && len(spec.Keys[t]) == 0 {
			noKey = append(noKey, t)
		}
		if sourceType == "postgresql" && (info.ReplicaIdentity == nil || *info.ReplicaIdentity != "full") {
			notFull = append(notFull, t)
		}
	}
	var out []checks.Check
	if len(noKey) > 0 {
		out = append(out, checks.Check{Code: "flink_primary_key", Title: "Primary keys", Category: checks.CatTables, Status: checks.Fail, Blocking: true,
			Message: fmt.Sprintf("%s %s no primary key. Flink CDC writes each row by its key.", strings.Join(noKey, ", "), plural(len(noKey), "has", "have")),
			Fix:     &checks.Fix{Kind: "text", Who: "you", Code: "Pick key columns for these tables in the Transform step (Primary key), or add a primary key in the database."}})
	} else {
		out = append(out, checks.Check{Code: "flink_primary_key", Title: "Primary keys", Category: checks.CatTables, Status: checks.Pass,
			Message: "Every table has a key to write its rows by."})
	}
	if sourceType == "postgresql" {
		if len(notFull) > 0 {
			stmts := make([]string, len(notFull))
			for i, t := range notFull {
				schema, table, _ := strings.Cut(t, ".")
				stmts[i] = "ALTER TABLE " + connectors.QuoteIdent(schema) + "." + connectors.QuoteIdent(table) + " REPLICA IDENTITY FULL;"
			}
			out = append(out, checks.Check{Code: "flink_replica_identity", Title: "Whole-row changes", Category: checks.CatTables, Status: checks.Fail,
				Blocking: true, Message: fmt.Sprintf("%s %s only its key on updates; Flink CDC needs the whole previous row.",
					strings.Join(notFull, ", "), plural(len(notFull), "logs", "log")),
				Why: "PostgreSQL logs the old row of an update only with REPLICA IDENTITY FULL. It makes the WAL larger for these tables.",
				Fix: &checks.Fix{Kind: "sql", Who: "DBA", Code: strings.Join(stmts, "\n")}})
		} else {
			out = append(out, checks.Check{Code: "flink_replica_identity", Title: "Whole-row changes", Category: checks.CatTables, Status: checks.Pass,
				Message: "Every table logs whole rows (REPLICA IDENTITY FULL)."})
		}
	}
	return out
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// flinkSlotsCheck: during a copy each reader of a Flink CDC PostgreSQL source opens its own
// temporary replication slot (<slot>_<reader>, each with a WAL sender) next to the pipeline's own
// one, so a copy needs 1 + parallelism of each. Measured by the source's pg_capacity check; nil when
// that didn't run, or when this deploy neither creates the slot nor copies.
func flinkSlotsCheck(p *models.Pipeline, spec domain.PipelineSpec, size engine.FlinkSize, sourceReport checks.Report) *checks.Check {
	var capacity connectors.PGCapacity
	found := false
	for _, c := range sourceReport.Checks {
		if c.Code == "pg_capacity" {
			capacity, found = connectors.ParsePGCapacity(c)
		}
	}
	if !found {
		return nil
	}
	readers := size.Parallelism()
	firstDeploy := p.DeployedVersionID == nil
	copies := firstDeploy && spec.Snapshot.Mode != "no_data"
	need := 0
	if firstDeploy {
		need = 1 // the pipeline's own slot
	}
	if copies {
		need += readers
	}
	if need == 0 {
		return nil
	}
	freeSlots, freeSenders := capacity.MaxSlots-capacity.UsedSlots, capacity.MaxSenders-capacity.UsedSenders
	what := fmt.Sprintf("%d replication slot%s and WAL sender%s", need, plural(need, "", "s"), plural(need, "", "s"))
	if copies {
		what += fmt.Sprintf(" (1 for streaming and 1 for each of its %d readers while it copies)", readers)
	}
	if freeSlots >= need && freeSenders >= need {
		return &checks.Check{Code: "flink_replication_slots", Title: "Replication slots for the copy", Category: checks.CatCapacity, Status: checks.Pass,
			Message: fmt.Sprintf("This deploy needs %s; %d slots and %d WAL senders are free.", what, freeSlots, freeSenders)}
	}
	limit := capacity.UsedSlots + need + 4
	if s := capacity.UsedSenders + need + 4; s > limit {
		limit = s
	}
	return &checks.Check{Code: "flink_replication_slots", Title: "Replication slots for the copy", Category: checks.CatCapacity, Status: checks.Fail,
		Blocking: true, Message: fmt.Sprintf("This deploy needs %s, but only %d slots and %d WAL senders are free: the copy would stop part-way.",
			what, max(freeSlots, 0), max(freeSenders, 0)),
		Fix: &checks.Fix{Kind: "sql", RestartRequired: true, Who: "DBA", Code: "-- drop slots nobody uses:\nSELECT slot_name, active FROM pg_replication_slots;\n" +
			fmt.Sprintf("-- or raise the limits (restart required):\nALTER SYSTEM SET max_replication_slots = %d;\nALTER SYSTEM SET max_wal_senders = %d;", limit, limit)}}
}

// flinkServerIDCheck: each reader of a MySQL source uses its own server id; two pipelines with the
// same id disconnect each other.
func flinkServerIDCheck(ctx context.Context, q db.Querier, p *models.Pipeline, size engine.FlinkSize) (checks.Check, error) {
	ours := connectors.MyServerIDRange(p.ID.String(), size.Parallelism())
	lo, hi := idRange(ours)
	others, err := db.Select[models.Pipeline](ctx, q, `SELECT * FROM pipelines WHERE source_id = $1 AND id <> $2 AND deleted_at IS NULL`, p.SourceID, p.ID)
	if err != nil {
		return checks.Check{}, err
	}
	if o := serverIDCollision(lo, hi, others); o != nil {
		return checks.Check{Code: "flink_server_ids", Title: "MySQL server ids", Category: checks.CatResilience, Status: checks.Fail, Blocking: true,
			Message: fmt.Sprintf("This pipeline's server ids (%s) collide with pipeline %s on the same source; the two would disconnect each other.", ours, o.Name),
			Fix:     &checks.Fix{Kind: "text", Who: "you", Code: "Create this pipeline again: a new pipeline gets other server ids."}}, nil
	}
	return checks.Check{Code: "flink_server_ids", Title: "MySQL server ids", Category: checks.CatResilience, Status: checks.Pass,
		Message: fmt.Sprintf("Server ids %s, one per reader, are used by no other pipeline on this source.", ours)}, nil
}

// serverIDCollision is the first pipeline whose server ids overlap [lo, hi]: a Flink CDC
// pipeline's whole block of 8, a Debezium pipeline's one id.
func serverIDCollision(lo, hi int64, others []models.Pipeline) *models.Pipeline {
	for i := range others {
		o := &others[i]
		olo, ohi := connectors.MyServerID(o.ID.String()), connectors.MyServerID(o.ID.String())
		if EngineOf(o) == engine.FlinkCDC {
			olo, ohi = idRange(connectors.MyServerIDRange(o.ID.String(), 8))
		}
		if olo <= hi && lo <= ohi {
			return o
		}
	}
	return nil
}

func idRange(r string) (int64, int64) {
	a, b, _ := strings.Cut(r, "-")
	lo, _ := strconv.ParseInt(a, 10, 64)
	hi, _ := strconv.ParseInt(b, 10, 64)
	return lo, hi
}

// flinkClusterFitCheck: a custom cluster's TaskManagers must fit a node. Blocking when one slot
// alone is bigger than the largest node; Unknown (never a pass) when the runtime doesn't say how
// big its nodes are. A runtime check: a forced deploy can't skip it (its pods would never start).
func flinkClusterFitCheck(spec domain.PipelineSpec, packed engine.Packed, capacity map[string]any) *checks.Check {
	if spec.Policies.Resources != "custom" || spec.FlinkCDC.Cluster == nil {
		return nil
	}
	node := NodeOf(capacity)
	s := packed.Size
	shape := fmt.Sprintf("%d TaskManager%s × %d slots, %s CPU and %s each", s.TaskManagers, plural(s.TaskManagers, "", "s"), s.SlotsPerTaskManager(), s.TaskManagerCPU, s.TaskManagerMemory)
	switch {
	case !packed.Fits:
		return &checks.Check{Code: "flink_cluster_fit", Title: "Cluster fits the nodes", Category: checks.CatCapacity, Status: checks.Fail, Blocking: true,
			Message: packed.Why, Fix: &checks.Fix{Kind: "text", Who: "you", Code: "Give each slot less CPU or memory in Review → Size, or add bigger nodes to the runtime."}}
	case !node.Known():
		return &checks.Check{Code: "flink_cluster_fit", Title: "Cluster fits the nodes", Category: checks.CatCapacity, Status: checks.Unknown,
			Message: shape + ". The runtime doesn't report its nodes' size, so BeziSync can't check they fit."}
	case runtime.QuantityCPU(s.TaskManagerCPU)*1000 > float64(node.CPUMilli) || runtime.QuantityMem(s.TaskManagerMemory) > int64(node.MemoryMiB)<<20:
		return &checks.Check{Code: "flink_cluster_fit", Title: "Cluster fits the nodes", Category: checks.CatCapacity, Status: checks.Fail, Blocking: true,
			Message: fmt.Sprintf("%s, but the largest node has %.2g CPU and %d MiB.", shape, float64(node.CPUMilli)/1000, node.MemoryMiB),
			Fix:     &checks.Fix{Kind: "text", Who: "you", Code: "Use the automatic shape, or smaller TaskManagers, in Review → Size."}}
	}
	return &checks.Check{Code: "flink_cluster_fit", Title: "Cluster fits the nodes", Category: checks.CatCapacity, Status: checks.Pass, Message: shape + ". " + packed.Why}
}

// FlinkCapacityCheck is the runtime capacity check for a Flink CDC cluster's size.
func FlinkCapacityCheck(capacity map[string]any, size engine.FlinkSize) *checks.Check {
	return capacityCheck(capacity, FlinkNeed(size), Need{}, flinkWhat(size))
}

// FlinkNeed is what a Flink CDC cluster asks for in all.
func FlinkNeed(size engine.FlinkSize) Need {
	return Need{CPU: runtime.QuantityCPU(size.JobManagerCPU) + float64(size.TaskManagers)*runtime.QuantityCPU(size.TaskManagerCPU), MiB: size.TotalMemoryMiB()}
}

func flinkWhat(size engine.FlinkSize) string {
	return fmt.Sprintf(" (a JobManager and %d TaskManager%s)", size.TaskManagers, plural(size.TaskManagers, "", "s"))
}
