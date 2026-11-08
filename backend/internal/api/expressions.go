package api

import (
	"errors"
	"net/http"
	"unicode/utf16"

	"github.com/go-chi/chi/v5"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/expr"
	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// Checking a Flink CDC expression as it is typed (ADR 0008 E6): its canonical text, kind and
// whether it is deterministic, or the refusal with where it is. Pure: it reads the pipeline's
// current spec (for masked and left-out columns) and the columns the client sends, never the
// source.

type expressionRequest struct {
	Use        expr.Use
	Table      string
	Expression string
	Columns    []domain.PreviewColumn
	HasColumns bool
}

func parseExpressionRequest(v *val.V, o *val.Object) expressionRequest {
	var req expressionRequest
	if f := o.Get("use"); f.Required() {
		s, _ := v.Literal(f.Loc, f.Raw, string(expr.UseColumn), string(expr.UseFilter))
		req.Use = expr.Use(s)
	}
	if f := o.Get("table"); f.Required() {
		req.Table, _ = v.Constrained(f.Loc, f.Raw, val.StrRules{Max: 300, Pattern: `^[A-Za-z0-9_$\-]+\.[A-Za-z0-9_$\-]+$`})
	}
	if f := o.Get("expression"); f.Required() {
		req.Expression, _ = v.Constrained(f.Loc, f.Raw, val.StrRules{Max: 4 * expr.MaxLength})
	}
	if f := o.Get("columns"); f.Present && !f.IsNull() {
		req.HasColumns = true
		if items, ok := v.List(f.Loc, f.Raw); ok && v.ListLen(f.Loc, len(items), 0, maxPreviewColumns) {
			for i, item := range items {
				co := v.Model(f.Loc.Add(i), item, "ExpressionColumn")
				if co.Failed {
					continue
				}
				var c domain.PreviewColumn
				if g := co.Get("name"); g.Required() {
					c.Name, _ = v.Constrained(g.Loc, g.Raw, val.StrRules{Min: 1, Max: 300})
				}
				if g := co.Get("type"); g.Required() {
					c.Type, _ = v.Constrained(g.Loc, g.Raw, val.StrRules{Max: 200})
				}
				c.Nullable = true // absent: it can be NULL
				if g := co.Get("nullable"); g.Present {
					c.Nullable, _ = v.Bool(g.Loc, g.Raw)
				}
				co.Forbid()
				req.Columns = append(req.Columns, c)
			}
		}
	}
	o.Forbid()
	return req
}

// utf16Offset is a byte offset of s as the browser counts characters (UTF-16 code units).
func utf16Offset(s string, b int) int {
	b = min(max(b, 0), len(s))
	return len(utf16.Encode([]rune(s[:b])))
}

func expressionRoutes(r chi.Router) {
	r.Post("/projects/{project_id}/pipelines/{pipeline_id}/expressions:check", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		req, err := body(r, parseExpressionRequest)
		if err != nil {
			if _, aerr := project(r); aerr != nil {
				return aerr
			}
			return err
		}
		_, p, err := viewPipeline(r, auth.PipelineView, false)
		if err != nil {
			return err
		}
		ctx := r.Context()
		q := pool(ctx)
		spec, err := services.SpecOf(ctx, q, p.CurrentVersionID)
		if err != nil {
			return err
		}
		if spec == nil {
			return problem.NotFound("Version")
		}
		source, err := services.GetConnection(ctx, q, p.SourceID)
		if err != nil {
			return err
		}
		env := domain.ExpressionEnv(source.Type, domain.TableColumns{Columns: req.Columns}, *spec, req.Table)
		if !req.HasColumns {
			env.Columns = nil // unknown: every column fits, preflight checks them
		}
		out := map[string]any{"error": nil}
		n, err := expr.Parse(req.Expression)
		var res expr.Result
		if err == nil {
			res, err = expr.Check(n, env, req.Use)
		}
		if err == nil {
			_, err = expr.FlinkChecked(n)
		}
		var e *expr.Error
		switch {
		case err == nil:
			cols := res.Columns
			if cols == nil {
				cols = []string{}
			}
			out = map[string]any{"canonical": expr.Canonical(n), "kind": string(res.Kind), "deterministic": res.Deterministic, "columns": cols, "error": nil}
			if req.Use == expr.UseColumn {
				// As a Flink CDC key column (ADR 0010): the rule preflight applies, or null when it fits.
				var why any
				if w := domain.ComputedKeyProblem(source.Type, *spec, req.Table, domain.Transform{Type: "computed", Table: req.Table, Expression: expr.Canonical(n)},
					domain.TableColumns{Columns: req.Columns}); w != "" {
					why = w
				}
				out["key_problem"] = why
			}
		case errors.As(err, &e):
			start := utf16Offset(req.Expression, e.Pos)
			out["error"] = map[string]any{"message": e.Msg, "offset": start, "length": utf16Offset(req.Expression, e.Pos+e.Len) - start}
		default:
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
}
