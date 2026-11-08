package expr

import "testing"

// ADR 0010: whether a value is never empty, given which columns are NOT NULL (a and b are; n isn't).
func TestNeverNull(t *testing.T) {
	notNull := func(c string) bool { return c == "a" || c == "b" }
	for src, want := range map[string]bool{
		"'x'": true, "1": true, "NULL": false, "`a`": true, "`n`": false,
		"UPPER(`a`)": true, "UPPER(`n`)": false, "`a` || '-' || `b`": true, "`a` || `n`": false,
		"`a` + `b`": true, "`a` / 2": false, "`a` % 2": false, "-`a`": true,
		"IFNULL(`n`, '')": true, "IFNULL(`n`, `n`)": false, "COALESCE(`n`, `a`)": true, "NULLIF(`a`, 'x')": false,
		"CONCAT_WS('-', `n`, `a`)": true, "CONCAT_WS(`n`, `a`)": false,
		"TRY_CAST(`a` AS BIGINT)": false, "CAST(`a` AS STRING)": true, "TO_DATE(`a`, 'yyyy-MM-dd')": false,
		"CASE WHEN `a` = 'x' THEN 'y' END": false, "CASE WHEN `a` = 'x' THEN 'y' ELSE `b` END": true,
		"CASE WHEN `a` = 'x' THEN `n` ELSE 'z' END": false, "IF(`a` = 'x', 'y', 'z')": true, "IF(`a` = 'x', `n`, 'z')": false,
		"`n` IS NULL": true, "YEAR(`a`)": true, "DATE_FORMAT(`a`, 'yyyy')": true,
	} {
		n, err := Parse(src)
		if err != nil {
			t.Fatal(src, err)
		}
		if got := NeverNull(n, notNull); got != want {
			t.Errorf("%s: got %v, want %v", src, got, want)
		}
	}
}
