// Package val validates decoded JSON the way Pydantic v2 (lax mode) did, producing the
// same error locations, types and messages, so API clients see identical 422 responses.
//
// Inputs are generic values from pyjson.DecodeGeneric (objects, arrays, json.Number,
// strings, booleans, nil).
package val

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

// Loc is a Pydantic error location; elements are field names or list indexes.
type Loc []string

// Add returns loc extended with more elements.
func (l Loc) Add(parts ...any) Loc {
	out := make(Loc, 0, len(l)+len(parts))
	out = append(out, l...)
	for _, p := range parts {
		switch x := p.(type) {
		case string:
			out = append(out, x)
		case int:
			out = append(out, strconv.Itoa(x))
		default:
			out = append(out, fmt.Sprint(x))
		}
	}
	return out
}

// V collects errors.
type V struct {
	Errors []problem.FieldError
}

// Fail records an error.
func (v *V) Fail(loc Loc, typ, msg string) {
	t := typ
	v.Errors = append(v.Errors, problem.FieldError{Loc: append(Loc{}, loc...), Message: msg, Type: &t})
}

// ValueError records a custom validator failure ("Value error, …").
func (v *V) ValueError(loc Loc, msg string) {
	v.Fail(loc, "value_error", "Value error, "+msg)
}

// OK reports no errors so far.
func (v *V) OK() bool { return len(v.Errors) == 0 }

// Count is the number of errors (to detect failures within a section).
func (v *V) Count() int { return len(v.Errors) }

// Problem is the 422 for the collected errors (nil when valid).
func (v *V) Problem() *problem.Problem {
	if v.OK() {
		return nil
	}
	return problem.Validation(v.Errors)
}

// Object is a JSON object being validated as a model.
type Object struct {
	v      *V
	loc    Loc
	m      map[string]any
	used   map[string]bool
	Failed bool // the input was not an object
}

// Model starts validating raw as a model at loc. Non-objects fail with model_type
// (nested models) — use Body for request bodies.
func (v *V) Model(loc Loc, raw any, name string) *Object {
	m, ok := raw.(map[string]any)
	if !ok {
		v.Fail(loc, "model_type", "Input should be a valid dictionary or instance of "+name)
		return &Object{v: v, loc: loc, m: map[string]any{}, used: map[string]bool{}, Failed: true}
	}
	return &Object{v: v, loc: loc, m: m, used: map[string]bool{}}
}

// Body starts validating a request body (FastAPI wording for non-objects).
func (v *V) Body(raw any) *Object {
	m, ok := raw.(map[string]any)
	if !ok {
		v.Fail(Loc{"body"}, "model_attributes_type", "Input should be a valid dictionary or object to extract fields from")
		return &Object{v: v, loc: Loc{"body"}, m: map[string]any{}, used: map[string]bool{}, Failed: true}
	}
	return &Object{v: v, loc: Loc{"body"}, m: m, used: map[string]bool{}}
}

// Loc of the object.
func (o *Object) Loc() Loc { return o.loc }

// V returns the collector.
func (o *Object) V() *V { return o.v }

// Has reports whether the key was sent.
func (o *Object) Has(key string) bool {
	_, ok := o.m[key]
	return ok
}

// Raw returns the raw value of a key and marks it used.
func (o *Object) Raw(key string) (any, bool) {
	o.used[key] = true
	v, ok := o.m[key]
	return v, ok
}

// Forbid records extra_forbidden for unknown keys (models with extra="forbid"). Call
// after reading every field; Pydantic reports extras after field errors.
func (o *Object) Forbid() {
	if o.Failed {
		return
	}
	for _, k := range sortedKeys(o.m) {
		if !o.used[k] {
			o.v.Fail(o.loc.Add(k), "extra_forbidden", "Extra inputs are not permitted")
		}
	}
}

// Field is the value of one key with its location.
type Field struct {
	v       *V
	Loc     Loc
	Raw     any
	Present bool
}

// Get reads a field (marking it used).
func (o *Object) Get(key string) Field {
	raw, ok := o.Raw(key)
	return Field{v: o.v, Loc: o.loc.Add(key), Raw: raw, Present: ok}
}

// Required records "Field required" when absent and reports presence.
func (f Field) Required() bool {
	if !f.Present {
		f.v.Fail(f.Loc, "missing", "Field required")
		return false
	}
	return true
}

// IsNull reports an explicit JSON null.
func (f Field) IsNull() bool { return f.Present && f.Raw == nil }

// --- scalar conversions (lax mode) ---

// Str validates a string.
func (v *V) Str(loc Loc, raw any) (string, bool) {
	s, ok := raw.(string)
	if !ok {
		v.Fail(loc, "string_type", "Input should be a valid string")
		return "", false
	}
	return s, true
}

// StrRules are Pydantic string constraints.
type StrRules struct {
	Min, Max int // character counts; 0 = unset
	Pattern  string
}

// patternCache holds compiled patterns; requests validate concurrently.
var patternCache sync.Map // string → *regexp.Regexp

// Constrained validates a string with constraints (min, max, pattern; first failure wins).
func (v *V) Constrained(loc Loc, raw any, r StrRules) (string, bool) {
	s, ok := v.Str(loc, raw)
	if !ok {
		return "", false
	}
	n := utf8.RuneCountInString(s)
	if r.Min > 0 && n < r.Min {
		v.Fail(loc, "string_too_short", fmt.Sprintf("String should have at least %d character%s", r.Min, plural(r.Min)))
		return "", false
	}
	if r.Max > 0 && n > r.Max {
		v.Fail(loc, "string_too_long", fmt.Sprintf("String should have at most %d character%s", r.Max, plural(r.Max)))
		return "", false
	}
	if r.Pattern != "" {
		var re *regexp.Regexp
		if cached, ok := patternCache.Load(r.Pattern); ok {
			re = cached.(*regexp.Regexp)
		} else {
			re = regexp.MustCompile(r.Pattern)
			patternCache.Store(r.Pattern, re)
		}
		// Pydantic uses re.search semantics for patterns.
		if !re.MatchString(s) {
			v.Fail(loc, "string_pattern_mismatch", fmt.Sprintf("String should match pattern '%s'", r.Pattern))
			return "", false
		}
	}
	return s, true
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// goNumber converts Go numeric values (built in Go code rather than decoded from JSON) to
// json.Number so they validate like JSON input.
func goNumber(raw any) any {
	switch x := raw.(type) {
	case int:
		return json.Number(strconv.Itoa(x))
	case int32:
		return json.Number(strconv.FormatInt(int64(x), 10))
	case int64:
		return json.Number(strconv.FormatInt(x, 10))
	case float32:
		return json.Number(strconv.FormatFloat(float64(x), 'g', -1, 64))
	}
	return raw
}

// Int validates an integer in lax mode.
func (v *V) Int(loc Loc, raw any) (int, bool) {
	raw = goNumber(raw)
	if f, ok := raw.(float64); ok { // YAML .inf/.nan or Go callers
		raw = json.Number(strconv.FormatFloat(f, 'g', -1, 64))
		if math.IsNaN(f) || math.IsInf(f, 0) {
			v.Fail(loc, "finite_number", "Input should be a finite number")
			return 0, false
		}
	}
	switch x := raw.(type) {
	case bool:
		if x {
			return 1, true
		}
		return 0, true
	case json.Number:
		s := string(x)
		if !strings.ContainsAny(s, ".eE") {
			n, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				v.Fail(loc, "int_parsing_size", "Unable to parse input string as an integer, exceeded maximum size")
				return 0, false
			}
			return int(n), true
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil || math.IsInf(f, 0) {
			v.Fail(loc, "finite_number", "Input should be a finite number")
			return 0, false
		}
		if f != math.Trunc(f) {
			v.Fail(loc, "int_from_float", "Input should be a valid integer, got a number with a fractional part")
			return 0, false
		}
		return int(f), true
	case string:
		t := strings.TrimSpace(x)
		t = strings.ReplaceAll(t, "_", "") // Python int() accepts digit separators
		if n, err := strconv.ParseInt(t, 10, 64); err == nil && t != "" {
			return int(n), true
		}
		if f, err := strconv.ParseFloat(t, 64); err == nil && f == math.Trunc(f) && !math.IsInf(f, 0) && strings.ContainsAny(t, ".") && !strings.ContainsAny(t, "eE") {
			return int(f), true // "5.0"
		}
		v.Fail(loc, "int_parsing", "Input should be a valid integer, unable to parse string as an integer")
		return 0, false
	}
	v.Fail(loc, "int_type", "Input should be a valid integer")
	return 0, false
}

// Float validates a number in lax mode.
func (v *V) Float(loc Loc, raw any) (float64, bool) {
	raw = goNumber(raw)
	switch x := raw.(type) {
	case float64:
		return x, true
	case bool:
		if x {
			return 1, true
		}
		return 0, true
	case json.Number:
		f, err := strconv.ParseFloat(string(x), 64)
		if err != nil {
			v.Fail(loc, "float_parsing", "Input should be a valid number, unable to parse string as a number")
			return 0, false
		}
		return f, true
	case string:
		f, ok := pyFloat(strings.TrimSpace(x))
		if !ok {
			v.Fail(loc, "float_parsing", "Input should be a valid number, unable to parse string as a number")
			return 0, false
		}
		return f, true
	}
	v.Fail(loc, "float_type", "Input should be a valid number")
	return 0, false
}

func pyFloat(s string) (float64, bool) {
	switch strings.ToLower(strings.TrimLeft(s, "+-")) {
	case "nan":
		return math.NaN(), true
	case "inf", "infinity":
		if strings.HasPrefix(s, "-") {
			return math.Inf(-1), true
		}
		return math.Inf(1), true
	}
	if s == "" || strings.ContainsAny(strings.ToLower(s), "xp") {
		return 0, false
	}
	f, err := strconv.ParseFloat(strings.ReplaceAll(s, "_", ""), 64)
	return f, err == nil
}

// Bool validates a boolean in lax mode.
func (v *V) Bool(loc Loc, raw any) (bool, bool) {
	raw = goNumber(raw)
	switch x := raw.(type) {
	case bool:
		return x, true
	case json.Number:
		switch string(x) {
		case "0":
			return false, true
		case "1":
			return true, true
		}
		if f, err := strconv.ParseFloat(string(x), 64); err == nil && (f == 0 || f == 1) {
			return f == 1, true
		}
		v.Fail(loc, "bool_parsing", "Input should be a valid boolean, unable to interpret input")
		return false, false
	case string:
		switch strings.ToLower(strings.TrimSpace(x)) {
		case "0", "off", "f", "false", "n", "no":
			return false, true
		case "1", "on", "t", "true", "y", "yes":
			return true, true
		}
		v.Fail(loc, "bool_parsing", "Input should be a valid boolean, unable to interpret input")
		return false, false
	}
	v.Fail(loc, "bool_type", "Input should be a valid boolean")
	return false, false
}

// Literal validates one of the allowed strings.
func (v *V) Literal(loc Loc, raw any, allowed ...string) (string, bool) {
	if s, ok := raw.(string); ok {
		for _, a := range allowed {
			if s == a {
				return s, true
			}
		}
	}
	v.Fail(loc, "literal_error", "Input should be "+LiteralList(allowed))
	return "", false
}

// Enum validates one of an enum's values (Pydantic "enum" error wording).
func (v *V) Enum(loc Loc, raw any, allowed ...string) (string, bool) {
	if s, ok := raw.(string); ok {
		for _, a := range allowed {
			if s == a {
				return s, true
			}
		}
	}
	v.Fail(loc, "enum", "Input should be "+LiteralList(allowed))
	return "", false
}

// LiteralList renders "'a', 'b' or 'c'".
func LiteralList(allowed []string) string {
	q := make([]string, len(allowed))
	for i, a := range allowed {
		q[i] = "'" + a + "'"
	}
	if len(q) == 1 {
		return q[0]
	}
	return strings.Join(q[:len(q)-1], ", ") + " or " + q[len(q)-1]
}

// Num bounds for ge/le/gt/lt constraints on numbers.
type Num struct {
	GE, LE, GT, LT *float64
}

// F is a float pointer helper for bounds.
func F(x float64) *float64 { return &x }

func fmtBound(x float64) string {
	if x == math.Trunc(x) && math.Abs(x) < 1e15 {
		return strconv.FormatInt(int64(x), 10)
	}
	return strconv.FormatFloat(x, 'g', -1, 64)
}

// Bounds checks numeric constraints (Pydantic order: gt, ge, lt, le? — it reports the
// first violated in the order ge/gt then le/lt as declared; one error at most).
func (v *V) Bounds(loc Loc, x float64, b Num, isFloat bool) bool {
	// Pydantic prints the bound as declared; every bound in the product is declared as an
	// integer literal (gt=0, le=10_000), so integral bounds print without a fraction.
	_ = isFloat
	f := fmtBound
	switch {
	case b.GT != nil && !(x > *b.GT):
		v.Fail(loc, "greater_than", "Input should be greater than "+f(*b.GT))
	case b.GE != nil && !(x >= *b.GE):
		v.Fail(loc, "greater_than_equal", "Input should be greater than or equal to "+f(*b.GE))
	case b.LT != nil && !(x < *b.LT):
		v.Fail(loc, "less_than", "Input should be less than "+f(*b.LT))
	case b.LE != nil && !(x <= *b.LE):
		v.Fail(loc, "less_than_equal", "Input should be less than or equal to "+f(*b.LE))
	default:
		return true
	}
	return false
}

// IntIn validates an integer with bounds.
func (v *V) IntIn(loc Loc, raw any, b Num) (int, bool) {
	n, ok := v.Int(loc, raw)
	if !ok {
		return 0, false
	}
	return n, v.Bounds(loc, float64(n), b, false)
}

// FloatIn validates a float with bounds.
func (v *V) FloatIn(loc Loc, raw any, b Num) (float64, bool) {
	x, ok := v.Float(loc, raw)
	if !ok {
		return 0, false
	}
	return x, v.Bounds(loc, x, b, true)
}

// List validates an array; item is called per element with its location.
func (v *V) List(loc Loc, raw any) ([]any, bool) {
	l, ok := raw.([]any)
	if !ok {
		v.Fail(loc, "list_type", "Input should be a valid list")
		return nil, false
	}
	return l, true
}

// ListLen checks list length constraints after item validation.
func (v *V) ListLen(loc Loc, n, min, max int) bool {
	if min > 0 && n < min {
		v.Fail(loc, "too_short", fmt.Sprintf("List should have at least %d item%s after validation, not %d", min, plural(min), n))
		return false
	}
	if max > 0 && n > max {
		v.Fail(loc, "too_long", fmt.Sprintf("List should have at most %d item%s after validation, not %d", max, plural(max), n))
		return false
	}
	return true
}

// StrList validates list[str].
func (v *V) StrList(loc Loc, raw any) ([]string, bool) {
	l, ok := v.List(loc, raw)
	if !ok {
		return nil, false
	}
	out := make([]string, 0, len(l))
	good := true
	for i, item := range l {
		s, ok := v.Str(loc.Add(i), item)
		good = good && ok
		out = append(out, s)
	}
	return out, good
}

// Dict validates an object (dict[str, Any]).
func (v *V) Dict(loc Loc, raw any) (map[string]any, bool) {
	m, ok := raw.(map[string]any)
	if !ok {
		v.Fail(loc, "dict_type", "Input should be a valid dictionary")
		return nil, false
	}
	return m, true
}

// StrDict validates dict[str, str].
func (v *V) StrDict(loc Loc, raw any) (map[string]string, bool) {
	m, ok := v.Dict(loc, raw)
	if !ok {
		return nil, false
	}
	out := make(map[string]string, len(m))
	good := true
	for _, k := range sortedKeys(m) {
		s, ok := v.Str(loc.Add(k), m[k])
		good = good && ok
		out[k] = s
	}
	return out, good
}

// UUID validates a UUID the way Pydantic (Rust uuid crate) does.
func (v *V) UUID(loc Loc, raw any) (uuid.UUID, bool) {
	s, ok := raw.(string)
	if !ok {
		v.Fail(loc, "uuid_type", "UUID input should be a string, bytes or UUID object")
		return uuid.Nil, false
	}
	u, msg := ParseUUID(s)
	if msg != "" {
		v.Fail(loc, "uuid_parsing", "Input should be a valid UUID, "+msg)
		return uuid.Nil, false
	}
	return u, true
}

// ParseUUID parses like Pydantic (the Rust uuid crate); msg describes the error the way
// Pydantic words it.
func ParseUUID(s string) (uuid.UUID, string) {
	inner, offset, simple := s, 0, true
	switch {
	case len(s) >= 2 && s[0] == '{' && s[len(s)-1] == '}':
		inner, offset, simple = s[1:len(s)-1], 1, false
	case strings.HasPrefix(s, "urn:uuid:"):
		inner, offset, simple = s[len("urn:uuid:"):], len("urn:uuid:"), false
	}
	if (len(s) == 32 || len(s) == 36 || (!simple && (len(inner) == 36))) && validShape(inner) {
		if u, err := uuid.Parse(inner); err == nil {
			return u, ""
		}
	}
	hyphens := 0
	var bounds [4]int
	for i, r := range inner {
		switch {
		case r > 0x7f:
			return uuid.Nil, fmt.Sprintf("invalid character: found `%c` at %d", r, i+offset)
		case r == '-':
			if hyphens < 4 {
				bounds[hyphens] = i
			}
			hyphens++
		case !isHexDigit(byte(r)):
			return uuid.Nil, fmt.Sprintf("invalid character: found `%c` at %d", r, i+offset)
		}
	}
	if hyphens == 0 && simple {
		return uuid.Nil, fmt.Sprintf("invalid length: found %d", len(s))
	}
	if hyphens != 4 {
		return uuid.Nil, fmt.Sprintf("invalid group count: expected 5, found %d", hyphens+1)
	}
	starts := [5]int{0, 9, 14, 19, 24}
	want := [5]int{8, 4, 4, 4, 12}
	for i := 0; i < 4; i++ {
		if bounds[i] != starts[i+1]-1 {
			return uuid.Nil, fmt.Sprintf("invalid group length in group %d: expected %d, found %d", i, want[i], bounds[i]-starts[i])
		}
	}
	return uuid.Nil, fmt.Sprintf("invalid group length in group 4: expected 12, found %d", len(s)-starts[4])
}

func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// validShape: 32 hex digits, or 8-4-4-4-12 hex groups.
func validShape(s string) bool {
	if len(s) == 32 {
		for i := 0; i < 32; i++ {
			if !isHexDigit(s[i]) {
				return false
			}
		}
		return true
	}
	if len(s) != 36 {
		return false
	}
	for i := 0; i < 36; i++ {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if s[i] != '-' {
				return false
			}
		} else if !isHexDigit(s[i]) {
			return false
		}
	}
	return true
}

func sortedKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	// Pydantic reports extras in input order; JSON object order is lost after decoding,
	// so use a stable order.
	sortStrings(out)
	return out
}

func sortStrings(s []string) { sort.Strings(s) }
