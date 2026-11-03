package api

import (
	"net/http"
	"sort"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// StoreCopyRead is the result of copying a secret store to another project.
type StoreCopyRead struct {
	TargetProjectID uuid.UUID `json:"target_project_id"`
	TargetProject   string    `json:"target_project"`
	// "create" or "update" (a store of that name exists: its settings are replaced).
	Action string    `json:"action"`
	Store  StoreRead `json:"store"`
}

func secretsChoice(v *val.V, o *val.Object) string {
	f := o.Get("secrets")
	if !f.Present || f.IsNull() {
		return services.SecretsLeave
	}
	s, _ := v.Literal(f.Loc, f.Raw, services.SecretsLeave, services.SecretsCopy)
	return s
}

// copyTarget resolves the project to copy into: another project the caller can open (404 otherwise).
func copyTarget(r *http.Request, q db.Querier, a *auth.ProjectAccess, target uuid.UUID) (*auth.ProjectAccess, error) {
	if target == a.Project.ID {
		return nil, problem.New(422, "same_project", "Pick a different project to copy to.")
	}
	return auth.Project(r.Context(), q, a.Principal, target)
}

func copyRoutes(r chi.Router) {
	r.Post("/projects/{project_id}/connections:copy", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		type copyBody struct {
			Target uuid.UUID
			Req    services.CopyRequest
			DryRun bool
		}
		b, err := body(r, func(v *val.V, o *val.Object) copyBody {
			out := copyBody{Target: reqUUID(v, o, "target_project_id"), DryRun: boolean(v, o, "dry_run", false)}
			out.Req.Secrets = secretsChoice(v, o)
			if f := o.Get("connections"); f.Required() {
				if list, ok := v.List(f.Loc, f.Raw); ok && v.ListLen(f.Loc, len(list), 1, 50) {
					for i, raw := range list {
						io := v.Model(f.Loc.Add(i), raw, "CopyConnection")
						if io == nil {
							continue
						}
						it := services.CopyItem{ID: reqUUID(v, io, "id"), Update: boolean(v, io, "update", false)}
						if n := optStr(v, io, "name", val.StrRules{Pattern: SlugRe.String()}); n != nil {
							it.Name = *n
						}
						out.Req.Items = append(out.Req.Items, it)
					}
				}
			}
			if raw, ok := optDict(v, o, "stores"); ok {
				out.Req.Stores = map[string]services.StoreChoice{}
				names := make([]string, 0, len(raw))
				for k := range raw {
					names = append(names, k)
				}
				sort.Strings(names)
				for _, name := range names {
					so := v.Model(o.Get("stores").Loc.Add(name), raw[name], "StoreChoice")
					if so == nil {
						continue
					}
					ch := services.StoreChoice{}
					if f := so.Get("action"); f.Required() {
						ch.Action, _ = v.Literal(f.Loc, f.Raw, "copy", "use")
					}
					if u := optStr(v, so, "use", val.StrRules{}); u != nil {
						ch.Use = *u
					}
					out.Req.Stores[name] = ch
				}
			}
			return out
		})
		if err != nil {
			return err
		}
		if err := a.Require(auth.ConnectionManage); err != nil {
			return err
		}
		if b.Req.Secrets == services.SecretsCopy && !services.CanCopySecrets(a.Principal) {
			return problem.New(403, "admins_only", "Only Admins can copy saved secrets to another project.")
		}
		copiesStores := false
		for _, ch := range b.Req.Stores {
			copiesStores = copiesStores || ch.Action == "copy"
		}
		if copiesStores {
			if err := a.Require(auth.ProjectManage); err != nil {
				return problem.New(403, "admins_only", "Only Admins can copy secret stores to another project.")
			}
		}
		ctx := r.Context()
		var plan services.CopyPlan
		err = dryTx(ctx, b.DryRun, func(q db.Querier) error {
			target, err := copyTarget(r, q, a, b.Target)
			if err != nil {
				return err
			}
			if err := target.Require(auth.ConnectionManage); err != nil {
				return err
			}
			if copiesStores {
				if err := target.Require(auth.ProjectManage); err != nil {
					return problem.New(403, "admins_only", "Only Admins can copy secret stores to another project.")
				}
			}
			if plan, err = services.CopyConnections(ctx, q, a, target, b.Req); err != nil {
				return err
			}
			if !b.DryRun && !plan.Ready {
				var msgs []string
				var errs []map[string]any
				for i, it := range plan.Items {
					for _, p := range it.Problems {
						msgs = append(msgs, p)
						errs = append(errs, map[string]any{"loc": []any{"connections", i}, "message": p})
					}
				}
				return problem.New(422, "copy_blocked", strings.Join(msgs, " ")).WithErrors(errs)
			}
			return nil
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, plan)
		return nil
	}))

	r.Post("/projects/{project_id}/secret-stores/{store_id}/copy", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "store_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		type copyBody struct {
			Target  uuid.UUID
			Secrets string
			DryRun  bool
		}
		b, err := body(r, func(v *val.V, o *val.Object) copyBody {
			return copyBody{Target: reqUUID(v, o, "target_project_id"), Secrets: secretsChoice(v, o), DryRun: boolean(v, o, "dry_run", false)}
		})
		if err != nil {
			return err
		}
		if err := a.Require(auth.ProjectManage); err != nil {
			return err
		}
		ctx := r.Context()
		var out StoreCopyRead
		err = dryTx(ctx, b.DryRun, func(q db.Querier) error {
			s, err := projectStore(ctx, q, a, id)
			if err != nil {
				return err
			}
			target, err := copyTarget(r, q, a, b.Target)
			if err != nil {
				return err
			}
			if err := target.Require(auth.ProjectManage); err != nil {
				return err
			}
			copied, action, err := services.CopyStore(ctx, q, a, target, s, b.Secrets)
			if err != nil {
				return err
			}
			read, err := storeRead(ctx, q, copied)
			if err != nil {
				return err
			}
			out = StoreCopyRead{TargetProjectID: target.Project.ID, TargetProject: target.Project.Name, Action: action, Store: read}
			return nil
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
}
