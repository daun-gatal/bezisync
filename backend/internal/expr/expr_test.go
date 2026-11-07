package expr

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

// Each case: the text typed, its canonical form, and what compiles for Flink CDC.
var renderings = []struct{ in, canonical, flink string }{
	{"upper(name)", "UPPER(`name`)", "(CASE WHEN `name` IS NULL THEN NULL ELSE UPPER(`name`) END)"},
	{"`order` || '-' || `year`", "(`order` || '-') || `year`", "(CASE WHEN `order` IS NULL OR `year` IS NULL THEN NULL ELSE ((`order` || '-') || `year`) END)"},
	{"a + b * c", "`a` + `b` * `c`", "(CASE WHEN `a` IS NULL OR `b` IS NULL OR `c` IS NULL THEN NULL ELSE (TRY_CAST(`a` AS DOUBLE) + (TRY_CAST(`b` AS DOUBLE) * TRY_CAST(`c` AS DOUBLE))) END)"},
	{"(a + b) * c", "(`a` + `b`) * `c`", "(CASE WHEN `a` IS NULL OR `b` IS NULL OR `c` IS NULL THEN NULL ELSE ((TRY_CAST(`a` AS DOUBLE) + TRY_CAST(`b` AS DOUBLE)) * TRY_CAST(`c` AS DOUBLE)) END)"},
	{"a - (b - c)", "`a` - (`b` - `c`)", "(CASE WHEN `a` IS NULL OR `b` IS NULL OR `c` IS NULL THEN NULL ELSE (TRY_CAST(`a` AS DOUBLE) - (TRY_CAST(`b` AS DOUBLE) - TRY_CAST(`c` AS DOUBLE))) END)"},
	{"total / qty", "`total` / `qty`", "(CASE WHEN `total` IS NULL OR `qty` IS NULL OR TRY_CAST(`qty` AS DOUBLE) = TRY_CAST(0 AS DOUBLE) THEN NULL ELSE (TRY_CAST(`total` AS DOUBLE) / TRY_CAST(`qty` AS DOUBLE)) END)"},
	{"total % 0", "`total` % 0", "(CASE WHEN `total` IS NULL OR TRY_CAST(0 AS DOUBLE) = TRY_CAST(0 AS DOUBLE) THEN NULL ELSE (TRY_CAST(`total` AS BIGINT) % TRY_CAST(0 AS BIGINT)) END)"},
	{"total / 2.50", "`total` / 2.50", "(CASE WHEN `total` IS NULL THEN NULL ELSE (TRY_CAST(`total` AS DOUBLE) / TRY_CAST(2.50 AS DOUBLE)) END)"},
	{"- - x", "-(-`x`)", "(CASE WHEN `x` IS NULL THEN NULL ELSE (TRY_CAST(0 AS DOUBLE) - (TRY_CAST(0 AS DOUBLE) - TRY_CAST(`x` AS DOUBLE))) END)"},
	{"-x * 3", "-`x` * 3", "(CASE WHEN `x` IS NULL THEN NULL ELSE ((TRY_CAST(0 AS DOUBLE) - TRY_CAST(`x` AS DOUBLE)) * TRY_CAST(3 AS DOUBLE)) END)"},
	{"007.50", "7.50", "7.50"},
	{"'it''s' || name", "'it''s' || `name`", "(CASE WHEN `name` IS NULL THEN NULL ELSE ('it''s' || `name`) END)"},
	{"'US$5'", "'US$5'", "'US$5'"},
	{"'日本 ✓'", "'日本 ✓'", "'日本 ✓'"},
	{"cast(x as decimal(18,2))", "CAST(`x` AS DECIMAL(18, 2))", "TRY_CAST(`x` AS DECIMAL(18, 2))"},
	{"try_cast(x as varchar)", "TRY_CAST(`x` AS STRING)", "TRY_CAST(`x` AS VARCHAR)"},
	{"name like 'a!%'", "`name` LIKE 'a!%'", "(`name` LIKE 'a!!%' ESCAPE '!')"},
	{"name not like '%x_'", "`name` NOT LIKE '%x_'", "(`name` NOT LIKE '%x_' ESCAPE '!')"},
	{"id = 5", "`id` = 5", "(TRY_CAST(`id` AS DOUBLE) = TRY_CAST(5 AS DOUBLE))"},
	{"id != -5", "`id` <> -5", "(TRY_CAST(`id` AS DOUBLE) <> TRY_CAST(-5 AS DOUBLE))"},
	{"status = 'paid'", "`status` = 'paid'", "(`status` = 'paid')"},
	{"id in (1, 2)", "`id` IN (1, 2)", "(TRY_CAST(`id` AS DOUBLE) IN (TRY_CAST(1 AS DOUBLE), TRY_CAST(2 AS DOUBLE)))"},
	{"s not in ('a','b')", "`s` NOT IN ('a', 'b')", "(`s` NOT IN ('a', 'b'))"},
	{"x between 1 and 9", "`x` BETWEEN 1 AND 9", "(TRY_CAST(`x` AS DOUBLE) BETWEEN TRY_CAST(1 AS DOUBLE) AND TRY_CAST(9 AS DOUBLE))"},
	{"a is not distinct from b", "`a` IS NOT DISTINCT FROM `b`", "(`a` IS NOT DISTINCT FROM `b`)"},
	{"x is null or not y is true", "`x` IS NULL OR NOT `y` IS TRUE", "((`x` IS NULL) OR (NOT (`y` IS TRUE)))"},
	{"a and b or c and d", "`a` AND `b` OR `c` AND `d`", "((`a` AND `b`) OR (`c` AND `d`))"},
	{"a and (b or c)", "`a` AND (`b` OR `c`)", "(`a` AND (`b` OR `c`))"},
	{"case when a > 1 then 'x' else 'y' end", "CASE WHEN `a` > 1 THEN 'x' ELSE 'y' END", "(CASE WHEN (TRY_CAST(`a` AS DOUBLE) > TRY_CAST(1 AS DOUBLE)) THEN 'x' ELSE 'y' END)"},
	{"case s when 'a' then 1 end", "CASE `s` WHEN 'a' THEN 1 END", "(CASE `s` WHEN 'a' THEN 1 END)"},
	{"timestampdiff(day, a, b)", "TIMESTAMPDIFF(DAY, `a`, `b`)", "(CASE WHEN `a` IS NULL OR `b` IS NULL THEN NULL ELSE TIMESTAMPDIFF(DAY, `a`, `b`) END)"},
	{"date_format(created_at, 'yyyy-MM')", "DATE_FORMAT(`created_at`, 'yyyy-MM')", "(CASE WHEN `created_at` IS NULL THEN NULL ELSE DATE_FORMAT(`created_at`, 'yyyy-MM') END)"},
	{"localtimestamp", "LOCALTIMESTAMP", "LOCALTIMESTAMP"},
	{"now()", "NOW()", "NOW()"},
	{"__table_name__ || '.' || `a``b`", "(__table_name__ || '.') || `a``b`", "(CASE WHEN __table_name__ IS NULL OR `a``b` IS NULL THEN NULL ELSE ((__table_name__ || '.') || `a``b`) END)"},
	{"coalesce(a, b, 'none')", "COALESCE(`a`, `b`, 'none')", "COALESCE(`a`, `b`, 'none')"},
	{"(a || b) + 1", "(`a` || `b`) + 1", "(CASE WHEN `a` IS NULL OR `b` IS NULL THEN NULL ELSE (TRY_CAST((`a` || `b`) AS DOUBLE) + TRY_CAST(1 AS DOUBLE)) END)"},
}

func TestRenderings(t *testing.T) {
	for _, c := range renderings {
		n, err := Parse(c.in)
		if err != nil {
			t.Errorf("%q: %v", c.in, err)
			continue
		}
		if got := Canonical(n); got != c.canonical {
			t.Errorf("%q canonical:\n got %s\nwant %s", c.in, got, c.canonical)
		}
		if got := Flink(n); got != c.flink {
			t.Errorf("%q flink:\n got %s\nwant %s", c.in, got, c.flink)
		}
		// The canonical text is a fixed point: it parses back to the same tree.
		again, err := Parse(c.canonical)
		if err != nil || Canonical(again) != c.canonical {
			t.Errorf("%q: canonical %q doesn't round-trip: %v", c.in, c.canonical, err)
		}
	}
}

func TestRefusals(t *testing.T) {
	long := strings.Repeat("a+", 500) + "a"
	deep := strings.Repeat("(", 33) + "a" + strings.Repeat(")", 33)
	cases := map[string]string{
		"a; DROP TABLE t":                    `";"`,
		"a -- c":                             "Comments",
		"a /* c */":                          "Comments",
		"'${BZ_STATE_PASSWORD}'":             `"${"`,
		"${x}":                               `"${"`,
		"'a\\'":                              `"\"`,
		"\"a\"":                              "Double quotes",
		"'line\nbreak'":                      "Control",
		"SELECT a":                           "SELECT can't be used",
		"a FROM b":                           "Unexpected",
		"my_udf(a)":                          "isn't a function",
		"AI_CHAT(a)":                         "isn't a function",
		"regexp_replace(a, 'x', 'y')":        "Regular expressions",
		"day(a)":                             "DAYOFMONTH",
		"repeat(a, 9)":                       "isn't a function",
		"unix_timestamp(a)":                  "isn't a function",
		"a similar to 'x'":                   "SIMILAR TO",
		"a like b":                           "pattern in single quotes",
		"a like 'x' escape '#'":              "ESCAPE can't be chosen",
		"cast(a as decimal)":                 "precision and scale",
		"cast(a as decimal(40, 2))":          "precision",
		"cast(a as map)":                     "CAST to one of",
		"1e5":                                "in full",
		"'" + strings.Repeat("x", 201) + "'": "at most 200",
		long:                                 "at most 1000",
		deep:                                 "nests too deeply",
		"upper(a, b)":                        "takes one argument",
		"case end":                           "END can't be used",
		"timestampdiff(week, a, b)":          "starts with a unit",
		"":                                   "Write an expression",
		"a not b":                            "After NOT",
		"`a":                                 "closing back-quote",
		"'a":                                 "closing quote",
		"a \\ b":                             `"\"`,
	}
	for in, want := range cases {
		_, err := Parse(in)
		var e *Error
		if !errors.As(err, &e) || !strings.Contains(e.Msg, want) {
			t.Errorf("%q: got %v, want an error containing %q", in, err, want)
		}
	}
}

func TestCheck(t *testing.T) {
	env := Env{Columns: map[string]Kind{"tz": TimestampTZ, "now": Text, "name": Text, "AMOUNT": Decimal, "qty": Int, "created_at": Timestamp, "doc": Other, "paid": BoolK},
		Hidden: map[string]string{"card": "is left out of the pipeline"}}
	ok := []struct {
		in   string
		use  Use
		kind Kind
		det  bool
	}{
		{"upper(name)", UseColumn, Text, true},
		{"AMOUNT * qty", UseColumn, Float, true},
		{"qty + 1", UseColumn, Float, true}, {"qty % 2", UseColumn, Int, true}, {"-5", UseColumn, Int, true},
		{"AMOUNT / 2", UseColumn, Float, true},
		{"qty > 1 and paid", UseFilter, BoolK, true},
		{"year(created_at)", UseColumn, Int, true},
		{"coalesce(qty, 0)", UseColumn, Int, true}, {"cast(qty as double) = cast(AMOUNT as double)", UseFilter, BoolK, true}, {"qty = 5", UseFilter, BoolK, true},
		{"if(paid, 'yes', 'no')", UseColumn, Text, true},
		{"now()", UseColumn, TimestampTZ, false},
		{"doc is null", UseFilter, BoolK, true},
		{"cast(name as int) = 3", UseFilter, BoolK, true},
		{"__data_event_type__ = '-D'", UseColumn, BoolK, true},
	}
	for _, c := range ok {
		n, err := Parse(c.in)
		if err != nil {
			t.Fatalf("%q: %v", c.in, err)
		}
		r, err := Check(n, env, c.use)
		if err != nil || r.Kind != c.kind || r.Deterministic != c.det {
			t.Errorf("%q: %+v %v", c.in, r, err)
		}
	}
	bad := map[string]string{
		"upper(qty)":                       "needs text",
		"name > 3":                         "compares text with a whole number",
		"amount * 2":                       "Did you mean AMOUNT?",
		"upper(card)":                      "left out of the pipeline",
		"nope":                             "no column nope",
		"doc = 'x'":                        "can't compare binary",
		"if(paid, 1, 'x')":                 "mix",
		"date_format(created_at, 'dd.MM')": "pattern is one of",
		"qty + name":                       "needs a number",
		"NULL":                             "NULL alone",
	}
	for in, want := range bad {
		n, err := Parse(in)
		if err != nil {
			t.Fatalf("%q: %v", in, err)
		}
		if _, err := Check(n, env, UseColumn); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: got %v, want %q", in, err, want)
		}
	}
	// Filters: a condition, and deterministic.
	for in, want := range map[string]string{"__data_event_type__ <> '-D'": "can't read __data_event_type__", "created_at > localtimestamp": "can't use LOCALTIMESTAMP", "uuid() = name": "can't use UUID", "upper(name)": "is a condition"} {
		n, _ := Parse(in)
		if _, err := Check(n, env, UseFilter); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("filter %q: got %v, want %q", in, err, want)
		}
	}
	// Without discovery every column is Unknown and fits anything; the columns read are listed.
	n, _ := Parse("upper(a) || b || a")
	r, err := Check(n, Env{}, UseColumn)
	if err != nil || r.Kind != Text || !reflect.DeepEqual(r.Columns, []string{"a", "b"}) {
		t.Errorf("unknown columns: %+v %v", r, err)
	}
}

// No function BeziSync offers is one of Flink CDC's AI functions, which call a model provider.
func TestNoAIFunctions(t *testing.T) {
	for name := range Functions {
		if strings.HasPrefix(name, "AI_") || strings.Contains(name, "EMBEDDING") || strings.Contains(name, "MODEL") {
			t.Errorf("%s looks like an AI function", name)
		}
	}
}

// Errors carry a position inside the input.
func TestErrorPosition(t *testing.T) {
	_, err := Parse("upper(a) + nope(b)")
	var e *Error
	if !errors.As(err, &e) || e.Pos != 11 || e.Len != 4 {
		t.Fatalf("%+v", err)
	}
}

// Fuzz: never panics; whatever parses renders to a canonical fixed point that parses again, and
// the Flink rendering holds nothing the lexer refuses.
func FuzzParse(f *testing.F) {
	for _, c := range renderings {
		f.Add(c.in)
	}
	f.Add("'a''b' || `c``d` / (1 - -2)")
	f.Fuzz(func(t *testing.T, s string) {
		n, err := Parse(s)
		if err != nil {
			return
		}
		c := Canonical(n)
		again, err := Parse(c)
		if err != nil {
			t.Fatalf("canonical %q of %q doesn't parse: %v", c, s, err)
		}
		if c2 := Canonical(again); c2 != c {
			t.Fatalf("not a fixed point: %q -> %q", c, c2)
		}
		fl := Flink(n)
		for _, bad := range []string{"${", "--", "/*", ";", "\\", "\n"} {
			if strings.Contains(fl, bad) && !strings.Contains(c, bad) {
				t.Fatalf("Flink rendering %q of %q adds %q", fl, s, bad)
			}
		}
	})
}

// The Flink rendering never holds what would end the line or comment it out, and stays bounded.
func TestFlinkSafeText(t *testing.T) {
	for _, c := range renderings {
		n, _ := Parse(c.in)
		f := Flink(n)
		for _, bad := range []string{"--", "/*", ";", "${", "\\", "\n"} {
			if strings.Contains(f, bad) && !strings.Contains(c.in, bad) {
				t.Errorf("%q renders %q with %q", c.in, f, bad)
			}
		}
	}
	// NULL guards repeat their inputs: a deep nest of COALESCE inside text functions is refused.
	s := "a"
	for i := 0; i < 14; i++ {
		s = "upper(coalesce(" + s + ", 'x'))"
	}
	n, err := Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := FlinkChecked(n); err == nil {
		t.Errorf("an exponential rendering (%d characters) must be refused", len(Flink(n)))
	}
	if _, err := FlinkChecked(mustParse(t, "upper(coalesce(upper(coalesce(a, 'x')), 'y'))")); err != nil {
		t.Error(err)
	}
}

func mustParse(t *testing.T, s string) Node {
	t.Helper()
	n, err := Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return n
}
