// Package expr is the expression language BeziSync accepts for Flink CDC transforms (ADR 0008): a
// whitelisted subset of Flink CDC's transform expressions, parsed into a tree and rendered back by
// BeziSync, so no text a user typed reaches a pipeline definition as typed.
//
// Pure: no I/O, no globals that change. Every refusal is an *Error with a byte position.
package expr

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Limits (ADR 0008 E2).
const (
	MaxLength  = 1000 // characters in an expression
	MaxLiteral = 200  // characters in a string literal
	MaxDepth   = 32   // nesting
	MaxNodes   = 200  // nodes in the tree
)

// Error is a refusal at a byte offset of the input (Len bytes long, at least 1 unless at the end).
type Error struct {
	Msg      string
	Pos, Len int
}

func (e *Error) Error() string { return e.Msg }

func errAt(pos, n int, format string, args ...any) *Error {
	return &Error{Msg: fmt.Sprintf(format, args...), Pos: pos, Len: n}
}

type tokKind int

const (
	tEOF tokKind = iota
	tIdent
	tQuoted // a back-quoted identifier
	tString
	tNumber
	tOp // + - * / % || = <> != < <= > >= ( ) ,
)

type token struct {
	kind tokKind
	text string // the identifier, the literal's value, the number as written, or the operator
	pos  int
	end  int
}

var twoCharOps = []string{"||", "<>", "!=", "<=", ">="}

// lex splits s into tokens, refusing what the grammar never allows anywhere: comments, ";",
// "${", "\", double quotes, control characters and unknown characters.
func lex(s string) ([]token, error) {
	if !utf8.ValidString(s) {
		return nil, errAt(0, len(s), "The expression isn't valid text.")
	}
	if n := utf8.RuneCountInString(s); n > MaxLength {
		return nil, errAt(0, len(s), "An expression is at most %d characters (this one has %d).", MaxLength, n)
	}
	var out []token
	i := 0
	for i < len(s) {
		r, w := utf8.DecodeRuneInString(s[i:])
		switch {
		case r == ' ' || r == '\t' || r == '\n' || r == '\r':
			i += w
			continue
		case unicode.IsControl(r):
			return nil, errAt(i, w, "Control characters can't be used.")
		case strings.HasPrefix(s[i:], "--") || strings.HasPrefix(s[i:], "/*"):
			return nil, errAt(i, 2, "Comments can't be used.")
		case strings.HasPrefix(s[i:], "${"):
			return nil, errAt(i, 2, "\"${\" can't be used.")
		case r == ';':
			return nil, errAt(i, 1, "\";\" can't be used: an expression is one value.")
		case r == '\\':
			return nil, errAt(i, 1, "\"\\\" can't be used.")
		case r == '"':
			return nil, errAt(i, 1, "Double quotes can't be used: text goes in single quotes ('…'), and column names in back-quotes (`…`).")
		case r == '\'':
			t, err := lexString(s, i)
			if err != nil {
				return nil, err
			}
			out = append(out, t)
			i = t.end
			continue
		case r == '`':
			t, err := lexQuoted(s, i)
			if err != nil {
				return nil, err
			}
			out = append(out, t)
			i = t.end
			continue
		case r >= '0' && r <= '9' || r == '.' && i+1 < len(s) && s[i+1] >= '0' && s[i+1] <= '9':
			t, err := lexNumber(s, i)
			if err != nil {
				return nil, err
			}
			out = append(out, t)
			i = t.end
			continue
		case r == '_' || r < utf8.RuneSelf && unicode.IsLetter(r):
			j := i
			for j < len(s) && (s[j] == '_' || s[j] >= 'a' && s[j] <= 'z' || s[j] >= 'A' && s[j] <= 'Z' || s[j] >= '0' && s[j] <= '9') {
				j++
			}
			out = append(out, token{kind: tIdent, text: s[i:j], pos: i, end: j})
			i = j
			continue
		}
		matched := false
		for _, op := range twoCharOps {
			if strings.HasPrefix(s[i:], op) {
				out = append(out, token{kind: tOp, text: op, pos: i, end: i + 2})
				i += 2
				matched = true
				break
			}
		}
		if matched {
			continue
		}
		if strings.ContainsRune("+-*/%=<>(),", r) {
			out = append(out, token{kind: tOp, text: string(r), pos: i, end: i + 1})
			i++
			continue
		}
		return nil, errAt(i, w, "%q can't be used here.", string(r))
	}
	return append(out, token{kind: tEOF, pos: len(s), end: len(s)}), nil
}

// lexString reads '…' with ” for a quote.
func lexString(s string, start int) (token, error) {
	var b strings.Builder
	i := start + 1
	for {
		if i >= len(s) {
			return token{}, errAt(start, len(s)-start, "This text has no closing quote (').")
		}
		r, w := utf8.DecodeRuneInString(s[i:])
		switch {
		case r == '\'':
			if i+1 < len(s) && s[i+1] == '\'' {
				b.WriteByte('\'')
				i += 2
				continue
			}
			v := b.String()
			if n := utf8.RuneCountInString(v); n > MaxLiteral {
				return token{}, errAt(start, i+1-start, "A text is at most %d characters (this one has %d).", MaxLiteral, n)
			}
			return token{kind: tString, text: v, pos: start, end: i + 1}, nil
		case r == '\\':
			return token{}, errAt(i, 1, "\"\\\" can't be used, in text either.")
		case unicode.IsControl(r):
			return token{}, errAt(i, w, "Control characters (line breaks, tabs) can't be used in text.")
		case strings.HasPrefix(s[i:], "${"):
			return token{}, errAt(i, 2, "\"${\" can't be used, in text either.")
		}
		b.WriteRune(r)
		i += w
	}
}

// lexQuoted reads `…` with “ for a back-quote.
func lexQuoted(s string, start int) (token, error) {
	var b strings.Builder
	i := start + 1
	for {
		if i >= len(s) {
			return token{}, errAt(start, len(s)-start, "This column name has no closing back-quote (`).")
		}
		r, w := utf8.DecodeRuneInString(s[i:])
		switch {
		case r == '`':
			if i+1 < len(s) && s[i+1] == '`' {
				b.WriteByte('`')
				i += 2
				continue
			}
			if b.Len() == 0 {
				return token{}, errAt(start, i+1-start, "A column name can't be empty.")
			}
			return token{kind: tQuoted, text: b.String(), pos: start, end: i + 1}, nil
		case r == '\\' || unicode.IsControl(r) || strings.HasPrefix(s[i:], "${"):
			return token{}, errAt(i, w, "This character can't be used in a column name.")
		}
		b.WriteRune(r)
		i += w
	}
}

// lexNumber reads digits with at most one decimal point; exponents are refused (write the number
// out).
func lexNumber(s string, start int) (token, error) {
	i, dot := start, false
	for i < len(s) && (s[i] >= '0' && s[i] <= '9' || s[i] == '.') {
		if s[i] == '.' {
			if dot {
				return token{}, errAt(start, i+1-start, "A number has at most one decimal point.")
			}
			dot = true
		}
		i++
	}
	if i < len(s) && (s[i] == 'e' || s[i] == 'E' || s[i] == '_' || unicode.IsLetter(rune(s[i]))) {
		return token{}, errAt(start, i+1-start, "Write numbers out in full (no exponent, no letters).")
	}
	if i-start > 40 {
		return token{}, errAt(start, i-start, "A number has at most 40 digits.")
	}
	return token{kind: tNumber, text: s[start:i], pos: start, end: i}, nil
}
