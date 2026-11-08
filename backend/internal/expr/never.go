package expr

// NeverNull reports whether a checked tree always has a value when the columns notNull says are
// NOT NULL have one (ADR 0010: a column in a Flink CDC primary key must never be empty). It is
// conservative: a form that can give NULL on values (a division, which gives NULL for a zero; a
// TRY_CAST or TO_DATE that can't read its input; NULLIF; a CASE without ELSE) is not.
func NeverNull(n Node, notNull func(col string) bool) bool {
	all := func(ns ...Node) bool {
		for _, x := range ns {
			if x != nil && !NeverNull(x, notNull) {
				return false
			}
		}
		return true
	}
	switch x := n.(type) {
	case *Str, *Num, *Bool:
		return true
	case *Null:
		return false
	case *Col:
		return notNull(x.Name)
	case *Unary:
		return all(x.X)
	case *Binary:
		if x.Op == "/" || x.Op == "%" {
			return false
		}
		return all(x.L, x.R)
	case *Is:
		return true
	case *Distinct:
		return true
	case *In:
		return all(append([]Node{x.X}, x.List...)...)
	case *Between:
		return all(x.X, x.Lo, x.Hi)
	case *Like:
		return all(x.X)
	case *Case:
		if x.Else == nil || !all(x.Else) {
			return false
		}
		for _, w := range x.Whens {
			if !all(w.Then) {
				return false
			}
		}
		return true
	case *Cast:
		return x.To.Name == "STRING" && all(x.X)
	case *Call:
		switch x.Fn {
		case "COALESCE", "IFNULL":
			for _, a := range x.Args {
				if NeverNull(a, notNull) {
					return true
				}
			}
			return false
		case "IF":
			return len(x.Args) == 3 && all(x.Args[1], x.Args[2])
		case "CONCAT_WS":
			// Flink CDC skips empty arguments; only an empty separator gives NULL.
			return len(x.Args) > 0 && all(x.Args[0])
		case "NULLIF", "TO_DATE":
			return false
		}
		return all(x.Args...)
	}
	return false
}
