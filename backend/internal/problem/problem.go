// Package problem renders RFC 9457 problem details for every error the API returns.
package problem

import (
	"fmt"
	"maps"
)

var titles = map[int]string{
	400: "Bad request",
	401: "Not signed in",
	403: "Not allowed",
	404: "Not found",
	409: "Conflict",
	412: "Precondition failed",
	422: "Invalid input",
	428: "Precondition required",
	429: "Too many requests",
	500: "Internal error",
	503: "Unavailable",
}

// Title is the problem title for an HTTP status.
func Title(status int) string {
	if t, ok := titles[status]; ok {
		return t
	}
	return "Error"
}

// FieldError is one entry of a 422 validation response.
type FieldError struct {
	Loc     []string `json:"loc"`
	Message string   `json:"message"`
	Type    *string  `json:"type"`
}

// Problem is a user-facing error. Code is stable and documented; Detail is human text.
type Problem struct {
	Status int
	Code   string
	Detail string
	Errors []map[string]any
	Extra  map[string]any
}

func (p *Problem) Error() string { return fmt.Sprintf("%d %s: %s", p.Status, p.Code, p.Detail) }

// New creates a problem.
func New(status int, code, detail string) *Problem {
	return &Problem{Status: status, Code: code, Detail: detail}
}

// WithExtra adds top-level members to the body.
func (p *Problem) WithExtra(extra map[string]any) *Problem {
	if p.Extra == nil {
		p.Extra = map[string]any{}
	}
	maps.Copy(p.Extra, extra)
	return p
}

// WithErrors attaches an errors list.
func (p *Problem) WithErrors(errs []map[string]any) *Problem {
	p.Errors = errs
	return p
}

// NotFound is "<what> was not found." (404 not_found).
func NotFound(what string) *Problem {
	return New(404, "not_found", what+" was not found.")
}

// Conflict is a 409 with optional extra members.
func Conflict(code, detail string, extra map[string]any) *Problem {
	p := New(409, code, detail)
	if extra != nil {
		p.Extra = extra
	}
	return p
}

// Body renders the problem document.
func (p *Problem) Body(requestID string) map[string]any {
	body := map[string]any{
		"type":       "https://bezisync.io/problems/" + p.Code,
		"title":      Title(p.Status),
		"status":     p.Status,
		"code":       p.Code,
		"detail":     p.Detail,
		"request_id": nilIfEmpty(requestID),
	}
	maps.Copy(body, p.Extra)
	if len(p.Errors) > 0 {
		body["errors"] = p.Errors
	}
	return body
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// Validation builds the 422 validation_failed problem from field errors.
func Validation(errs []FieldError) *Problem {
	out := make([]map[string]any, len(errs))
	for i, e := range errs {
		var typ any
		if e.Type != nil {
			typ = *e.Type
		}
		out[i] = map[string]any{"loc": e.Loc, "message": e.Message, "type": typ}
	}
	return New(422, "validation_failed", "Some fields are invalid.").WithErrors(out)
}
