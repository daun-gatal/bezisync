package api

import (
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/runtime"
	"github.com/daun-gatal/bezisync/backend/internal/security"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// AgentPollSeconds is how often agents poll.
const AgentPollSeconds = 2

type requestResult struct {
	ID     string
	OK     bool
	Result map[string]any
}

type pollRequest struct {
	Version, Kind       string
	Capacity, LocalTest map[string]any
	HasCapacity         bool
	Features            []any // what this agent build supports, e.g. "health_port" (older agents: none)
	Workloads           *om.Map
	Results             []requestResult
}

// optPair reads tuple[int, str] | None.
func optPair(v *val.V, o *val.Object, key string) any {
	f := o.Get(key)
	if !f.Present || f.IsNull() {
		return nil
	}
	items, ok := v.List(f.Loc, f.Raw)
	if !ok {
		return nil
	}
	if len(items) < 2 {
		v.Fail(f.Loc, "too_short", fmt.Sprintf("Tuple should have at least 2 items after validation, not %d", len(items)))
		return nil
	}
	if len(items) > 2 {
		v.Fail(f.Loc, "too_long", fmt.Sprintf("Tuple should have at most 2 items after validation, not %d", len(items)))
		return nil
	}
	n, ok1 := v.Int(f.Loc.Add(0), items[0])
	s, ok2 := v.Str(f.Loc.Add(1), items[1])
	if !ok1 || !ok2 {
		return nil
	}
	return []any{n, s}
}

func workloadReport(v *val.V, loc val.Loc, raw any) *om.Map {
	o := v.Model(loc, raw, "WorkloadReport")
	out := om.New()
	out.Set("phase", str(v, o, "phase", "", val.StrRules{}, true))
	out.Set("ready", boolean(v, o, "ready", false))
	restarts := 0
	if f := o.Get("restarts"); f.Present {
		restarts, _ = v.Int(f.Loc, f.Raw)
	}
	out.Set("restarts", restarts)
	out.Set("started_at", optStr(v, o, "started_at", val.StrRules{}))
	out.Set("exit_code", optInt(v, o, "exit_code"))
	out.Set("reason", str(v, o, "reason", "", val.StrRules{}, false))
	out.Set("config_hash", optStr(v, o, "config_hash", val.StrRules{}))
	if note := str(v, o, "image_pull", "", val.StrRules{}, false); note != "" {
		out.Set("image_pull", domain.Truncate(note, 300)) // only when downloading: older agents never send it
	}
	if port := optIntIn(v, o, "health_port", val.Num{GE: val.F(1), LE: val.F(65535)}); port != nil {
		// The port the agent probed /q/health on (Apache Iceberg: 9000); older agents never send it.
		out.Set("health_port", *port)
	}
	out.Set("health", optPair(v, o, "health"))
	out.Set("metrics", optPair(v, o, "metrics"))
	// A Flink CDC pipeline's helper status and its components; older agents never send them.
	if status := optPair(v, o, "status"); status != nil {
		out.Set("status", status)
	}
	if details, ok := optDict(v, o, "details"); ok && details != nil {
		out.Set("details", details)
	}
	return out
}

func parsePoll(v *val.V, o *val.Object) pollRequest {
	p := pollRequest{Version: str(v, o, "version", "", val.StrRules{}, true), Kind: str(v, o, "kind", "", val.StrRules{}, true), Workloads: om.New()}
	p.Capacity, p.HasCapacity = optDict(v, o, "capacity")
	p.LocalTest, _ = optDict(v, o, "local_test")
	p.Features = []any{}
	if f := o.Get("features"); f.Present && !f.IsNull() {
		if items, ok := v.List(f.Loc, f.Raw); ok {
			for i, item := range items {
				if s, ok := v.Str(f.Loc.Add(i), item); ok {
					p.Features = append(p.Features, s)
				}
			}
		}
	}
	if f := o.Get("workloads"); f.Present {
		if m, ok := v.Dict(f.Loc, f.Raw); ok {
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				p.Workloads.Set(k, workloadReport(v, f.Loc.Add(k), m[k]))
			}
		}
	}
	if f := o.Get("results"); f.Present {
		if items, ok := v.List(f.Loc, f.Raw); ok {
			for i, item := range items {
				ro := v.Model(f.Loc.Add(i), item, "RequestResult")
				rr := requestResult{ID: str(v, ro, "id", "", val.StrRules{}, true), OK: boolean(v, ro, "ok", false)}
				if f := ro.Get("ok"); !f.Present {
					f.Required()
				}
				if rr.Result = dict(v, ro, "result", false); rr.Result == nil {
					rr.Result = map[string]any{}
				}
				p.Results = append(p.Results, rr)
			}
		}
	}
	return p
}

func agentRoutes(r chi.Router) {
	// Agents dial out to here; nothing ever connects to them.
	r.Post("/agent/poll", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		b, err := body(r, parsePoll)
		if err != nil {
			return err
		}
		invalid := problem.New(401, "invalid_agent_token", "Missing or invalid agent token.")
		header := r.Header.Get("Authorization")
		if !strings.HasPrefix(strings.ToLower(header), "bearer bza_") {
			return invalid
		}
		raw := strings.TrimSpace(header[7:])
		ctx := r.Context()
		var out *om.Map
		err = tx(ctx, func(q db.Querier) error {
			rt, err := db.Find[models.Runtime](ctx, q, `SELECT * FROM runtimes WHERE agent_token_hash = ANY($1)`, security.TokenHashes(raw))
			if err != nil {
				return err
			}
			if rt != nil && (rt.AgentTokenHash == nil || *rt.AgentTokenHash != security.TokenHash(raw)) {
				// The master key rotated: move the hash to the current key.
				h := security.TokenHash(raw)
				rt.AgentTokenHash = &h
				if _, err := q.Exec(ctx, `UPDATE runtimes SET agent_token_hash = $1 WHERE id = $2`, h, rt.ID); err != nil {
					return err
				}
			}
			if rt == nil || rt.Mode != "agent" {
				return invalid
			}
			if b.Kind != rt.Kind {
				return problem.New(409, "agent_kind_mismatch", "This runtime expects a "+rt.Kind+" agent, not "+b.Kind+".")
			}
			now := db.Now()
			var capacity, localTest any
			if b.Capacity != nil {
				capacity = b.Capacity
			}
			if b.LocalTest != nil {
				localTest = b.LocalTest
			}
			state := db.Map{"version": b.Version, "capacity": capacity, "local_test": localTest, "workloads": b.Workloads, "features": b.Features,
				"reported_at": pytime.ISOFormat(now)}
			rtCapacity := rt.Capacity
			if len(b.Capacity) > 0 {
				rtCapacity = b.Capacity
			}
			if _, err := q.Exec(ctx, `UPDATE runtimes SET last_seen_at = $1, agent_version = $2, agent_state = $3, capacity = $4 WHERE id = $5`,
				now, b.Version, state, rtCapacity, rt.ID); err != nil {
				return err
			}
			for _, item := range b.Results {
				id, err := uuid.Parse(item.ID)
				if err != nil {
					continue
				}
				st := "failed"
				if item.OK {
					st = "done"
				}
				if _, err := q.Exec(ctx, `UPDATE agent_requests SET state = $1, result = $2 WHERE id = $3 AND runtime_id = $4`,
					st, db.Map(item.Result), id, rt.ID); err != nil {
					return err
				}
			}
			workloads, err := db.Select[models.AgentWorkload](ctx, q, `SELECT * FROM agent_workloads WHERE runtime_id = $1`, rt.ID)
			if err != nil {
				return err
			}
			flink := slices.Contains(b.Features, any(runtime.FeatureFlinkCDC))
			desired := make([]any, 0, len(workloads))
			for _, wl := range workloads {
				spec, err := security.DecryptSecret(wl.SpecEnc)
				if err != nil {
					return err
				}
				if spec["engine"] == "flink-cdc" && !flink {
					continue // an older agent would run it as a Debezium container; Apply says to upgrade
				}
				desired = append(desired, spec)
			}
			reqs, err := db.Select[models.AgentRequest](ctx, q, `SELECT * FROM agent_requests WHERE runtime_id = $1 AND state = 'pending'`, rt.ID)
			if err != nil {
				return err
			}
			pending := make([]any, 0, len(reqs))
			for _, req := range reqs {
				args := map[string]any(req.Args)
				if enc, ok := req.Args["enc"].(string); ok {
					if args, err = security.DecryptSecret(enc); err != nil {
						return err
					}
				}
				if args == nil {
					args = map[string]any{}
				}
				pending = append(pending, obj("id", req.ID.String(), "op", req.Op, "args", args))
			}
			out = obj("runtime_id", rt.ID.String(), "workloads", desired, "requests", pending, "poll_seconds", AgentPollSeconds)
			return nil
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
}
