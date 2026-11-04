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
	sourceConn *models.Connection, discover *discoverOnce) ([]checks.Check, error) {
	var out []checks.Check
	if c := flinkAgentCheck(rt); c != nil {
		out = append(out, *c)
	}
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
	if sourceConn.Type == "mysql" {
		c, err := flinkServerIDCheck(ctx, q, p, spec)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
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
			Fix:     &checks.Fix{Kind: "text", Who: "you", Code: "Pick key columns for these tables in the Tables step, or add a primary key in the database."}})
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

// flinkServerIDCheck: each reader of a MySQL source uses its own server id; two pipelines with the
// same id disconnect each other.
func flinkServerIDCheck(ctx context.Context, q db.Querier, p *models.Pipeline, spec domain.PipelineSpec) (checks.Check, error) {
	size, ok := engine.FlinkSizes[spec.Policies.Resources]
	if !ok {
		size = engine.FlinkSizes["small"]
	}
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

// FlinkCapacityCheck is the runtime capacity check for a Flink CDC cluster's size.
func FlinkCapacityCheck(capacity map[string]any, resources string) *checks.Check {
	size, ok := engine.FlinkSizes[resources]
	if !ok {
		size = engine.FlinkSizes["small"]
	}
	need := runtime.QuantityCPU(size.JobManagerCPU) + float64(size.TaskManagers)*runtime.QuantityCPU(size.TaskManagerCPU)
	return capacityCheck(capacity, need, fmt.Sprintf(" (a JobManager and %d TaskManager%s, %d MiB in all)", size.TaskManagers,
		plural(size.TaskManagers, "", "s"), size.TotalMemoryMiB()))
}
