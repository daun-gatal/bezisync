package expr

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// Writes name|flink rendering lines for the corpus harness (BZ_CORPUS=path); skipped otherwise.
func TestCorpusGen(t *testing.T) {
	out := os.Getenv("BZ_CORPUS")
	if out == "" {
		t.Skip()
	}
	cases := []string{
		"upper(s)", "lower(s) || '-' || s2", "trim(s)", "char_length(s)", "substr(s, 2, 2)", "concat(s, '-', cast(i as string))",
		"i + 1", "d * i", "d / z", "i % z", "-i", "i + -5", "b = 5", "d = 2.5", "i in (1, 5)", "b between 0 and 9000000000",
		"cast(i as double) = cast(b as double)", "coalesce(s, 'none')", "if(flag, s, s2)", "case when i > 0 then 'pos' when i < 0 then 'neg' else 'zero' end",
		"case s when 'abc' then 1 else 0 end", "s like 'a%'", "s2 like '%!%'", "year(ts) * 100 + month(ts)", "date_format(dt, 'yyyy-MM')",
		"timestampdiff(day, ts, try_cast('2027-01-01T00:00:00' as timestamp))", "to_date(s, 'yyyy-MM-dd')", "round(d, 1) + abs(i)",
		"lpad(s, 5, '*') || rpad(s2, 3, '.')", "replace(s, 'b', 'B')", "left(s, 1) || right(s, 1)", "locate('b', s)",
		"startswith(s, 'a') and not flag", "nullif(i, 5)", "ifnull(s2, s)", "cast(d as decimal(10, 1))", "try_cast(s as bigint)",
		"__table_name__ || '.' || s", "__data_event_type__", "upper(coalesce(s, 'x'))", "concat_ws('/', s, s2)", "`Amount` * 2",
		"cast(f as bigint)", "dayofweek(dt)", "date_add(dt, 2)", "i is null or s is not null", "s is distinct from 'abc'",
	}
	var b strings.Builder
	for i, c := range cases {
		n, err := Parse(c)
		if err != nil {
			t.Fatalf("%q: %v", c, err)
		}
		fmt.Fprintf(&b, "e%02d|%s\n", i, Flink(n))
	}
	if err := os.WriteFile(out, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
}
