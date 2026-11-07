package expr

import (
	"strconv"
	"strings"
)

// Kind is the kind of a value, as domain.ColumnKind names column kinds. Unknown is a value whose
// kind isn't known yet (a column without discovery): it fits any slot, and preflight decides.
type Kind string

const (
	Text        Kind = "text"
	Int         Kind = "int"
	Decimal     Kind = "decimal"
	Float       Kind = "float"
	BoolK       Kind = "bool"
	Date        Kind = "date"
	Time        Kind = "time"
	Timestamp   Kind = "timestamp"
	TimestampTZ Kind = "timestamptz"
	Unknown     Kind = "unknown"
	// Other is a value no function takes (binary, a document, a list): only IS NULL and COALESCE.
	Other Kind = "other"
	// NullK is the NULL literal: it fits any slot.
	NullK Kind = "null"
)

// Slot is what an argument of a function takes.
type Slot string

const (
	SText    Slot = "text"
	SNum     Slot = "number"  // int, decimal or float
	SWhole   Slot = "whole"   // a whole number
	STime    Slot = "time"    // date, timestamp or timestamptz
	SDate    Slot = "date"    // a date (or a timestamp, which Flink CDC turns into its date)
	SAny     Slot = "any"     // any value but Other
	SBool    Slot = "bool"    // a condition
	SPattern Slot = "pattern" // a text literal from DatePatterns
	SCols    Slot = "same"    // any value of one kind with the other arguments (COALESCE)
)

// Fn is one function of the language.
type Fn struct {
	Slots    []Slot // a variadic function repeats its last slot
	Min, Max int
	Out      Kind // the result's kind; "" means the kind of the arguments (numbers, COALESCE)
	Det      bool // the same arguments always give the same value
	Unit     bool // TIMESTAMPDIFF: a unit comes first
	Group    string
}

func (f Fn) arity() string {
	switch {
	case f.Min == f.Max && f.Max == 0:
		return "no arguments"
	case f.Min == f.Max && f.Max == 1:
		return "one argument"
	case f.Min == f.Max:
		return strconv.Itoa(f.Max) + " arguments"
	}
	return strconv.Itoa(f.Min) + " to " + strconv.Itoa(f.Max) + " arguments"
}

func (f Fn) slot(i int) Slot {
	if i < len(f.Slots) {
		return f.Slots[i]
	}
	return f.Slots[len(f.Slots)-1]
}

// DatePatterns are the date and time patterns TO_DATE, TO_TIMESTAMP and DATE_FORMAT accept (Java
// SimpleDateFormat).
var DatePatterns = []string{"yyyy", "yyyy-MM", "yyyy-MM-dd", "yyyy-MM-dd HH", "yyyy-MM-dd HH:mm", "yyyy-MM-dd HH:mm:ss", "yyyyMMdd", "HH:mm:ss", "dd/MM/yyyy", "MM/dd/yyyy"}

// TimeUnits are TIMESTAMPDIFF's units.
var TimeUnits = []string{"SECOND", "MINUTE", "HOUR", "DAY", "MONTH", "YEAR"}

func fn(group string, out Kind, det bool, min, max int, slots ...Slot) Fn {
	return Fn{Slots: slots, Min: min, Max: max, Out: out, Det: det, Group: group}
}

// Functions are the functions the language offers, by Flink CDC's name (ADR 0008 E2). A name not
// here is refused: UDFs, AI and model functions, regular expressions, struct functions.
var Functions = map[string]Fn{
	"UPPER":       fn("Text", Text, true, 1, 1, SText),
	"LOWER":       fn("Text", Text, true, 1, 1, SText),
	"TRIM":        fn("Text", Text, true, 1, 1, SText),
	"LTRIM":       fn("Text", Text, true, 1, 1, SText),
	"RTRIM":       fn("Text", Text, true, 1, 1, SText),
	"CHAR_LENGTH": fn("Text", Int, true, 1, 1, SText),
	"CONCAT":      fn("Text", Text, true, 1, 10, SText),
	"CONCAT_WS":   fn("Text", Text, true, 2, 11, SText),
	"SUBSTR":      fn("Text", Text, true, 2, 3, SText, SWhole, SWhole),
	"LEFT":        fn("Text", Text, true, 2, 2, SText, SWhole),
	"RIGHT":       fn("Text", Text, true, 2, 2, SText, SWhole),
	"REPLACE":     fn("Text", Text, true, 3, 3, SText, SText, SText),
	"LPAD":        fn("Text", Text, true, 3, 3, SText, SWhole, SText),
	"RPAD":        fn("Text", Text, true, 3, 3, SText, SWhole, SText),
	"LOCATE":      fn("Text", Int, true, 2, 3, SText, SText, SWhole),
	"STARTSWITH":  fn("Text", BoolK, true, 2, 2, SText, SText),
	"ENDSWITH":    fn("Text", BoolK, true, 2, 2, SText, SText),

	"ABS":   fn("Numbers", "", true, 1, 1, SNum),
	"CEIL":  fn("Numbers", "", true, 1, 1, SNum),
	"FLOOR": fn("Numbers", "", true, 1, 1, SNum),
	"ROUND": fn("Numbers", "", true, 2, 2, SNum, SWhole),

	"DATE_FORMAT":   fn("Dates", Text, true, 2, 2, STime, SPattern),
	"YEAR":          fn("Dates", Int, true, 1, 1, STime),
	"QUARTER":       fn("Dates", Int, true, 1, 1, STime),
	"MONTH":         fn("Dates", Int, true, 1, 1, STime),
	"WEEK":          fn("Dates", Int, true, 1, 1, STime),
	"DAYOFMONTH":    fn("Dates", Int, true, 1, 1, STime),
	"DAYOFWEEK":     fn("Dates", Int, true, 1, 1, STime),
	"DAYOFYEAR":     fn("Dates", Int, true, 1, 1, STime),
	"HOUR":          fn("Dates", Int, true, 1, 1, STime),
	"MINUTE":        fn("Dates", Int, true, 1, 1, STime),
	"SECOND":        fn("Dates", Int, true, 1, 1, STime),
	"TO_DATE":       fn("Dates", Date, true, 2, 2, SText, SPattern),
	"DATE_ADD":      fn("Dates", Text, true, 2, 2, SDate, SWhole),
	"TIMESTAMPDIFF": {Slots: []Slot{STime, STime}, Min: 2, Max: 2, Out: Int, Det: true, Unit: true, Group: "Dates"},

	// When the row is written: not deterministic (ADR 0008 E5).
	"NOW":               fn("Dates", TimestampTZ, false, 0, 0),
	"LOCALTIMESTAMP":    fn("Dates", Timestamp, false, 0, 0),
	"CURRENT_TIMESTAMP": fn("Dates", TimestampTZ, false, 0, 0),
	"CURRENT_DATE":      fn("Dates", Date, false, 0, 0),
	"UUID":              fn("Text", Text, false, 0, 0),

	"COALESCE": fn("Empty values", "", true, 2, 10, SCols),
	"IFNULL":   fn("Empty values", "", true, 2, 2, SCols),
	"NULLIF":   fn("Empty values", "", true, 2, 2, SCols),
	"IF":       {Slots: []Slot{SBool, SCols, SCols}, Min: 3, Max: 3, Det: true, Group: "Empty values"},
}

// Deterministic reports whether the function always gives the same value for the same arguments.
func Deterministic(name string) bool {
	f, ok := Functions[strings.ToUpper(name)]
	return ok && f.Det
}

// numeric reports a number kind.
func numeric(k Kind) bool { return k == Int || k == Decimal || k == Float }

func timeLike(k Kind) bool { return k == Date || k == Timestamp || k == TimestampTZ }

// Fits reports whether a value of kind k can go in slot s (Unknown and NULL fit anything).
func Fits(s Slot, k Kind) bool {
	if k == Unknown || k == NullK {
		return true
	}
	switch s {
	case SText, SPattern:
		return k == Text
	case SNum:
		return numeric(k)
	case SWhole:
		return k == Int
	case STime:
		return timeLike(k)
	case SDate:
		return k == Date || k == Timestamp || k == TimestampTZ
	case SBool:
		return k == BoolK
	case SAny, SCols:
		return k != Other
	}
	return false
}

// SlotWords is a slot in words, for errors.
func SlotWords(s Slot) string {
	switch s {
	case SText:
		return "text"
	case SNum:
		return "a number"
	case SWhole:
		return "a whole number"
	case STime:
		return "a date or a timestamp"
	case SDate:
		return "a date"
	case SBool:
		return "a condition (true or false)"
	case SPattern:
		return "a date pattern: " + strings.Join(DatePatterns, ", ")
	}
	return "a plain value (not binary, JSON or a nested document)"
}
