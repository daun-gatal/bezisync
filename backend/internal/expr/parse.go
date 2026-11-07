package expr

import (
	"slices"
	"strconv"
	"strings"
)

// Node is one node of a parsed expression.
type Node interface{ pos() int }

// The node types. Pos is the byte offset of the node in the parsed text.
type (
	// Col is a source column (case-sensitive), or one of MetadataColumns.
	Col struct {
		Name string
		Pos  int
	}
	// Str is a text literal.
	Str struct {
		V   string
		Pos int
	}
	// Num is a number as written, without leading zeros ("0.5", "12").
	Num struct {
		Text string
		Pos  int
	}
	// Bool is TRUE or FALSE.
	Bool struct {
		V   bool
		Pos int
	}
	// Null is NULL.
	Null struct{ Pos int }
	// Call is a function from Functions; Unit is TIMESTAMPDIFF's unit ("" otherwise).
	Call struct {
		Fn   string // upper case
		Unit string
		Args []Node
		Pos  int
	}
	// Unary is "-x" or "NOT x".
	Unary struct {
		Op  string
		X   Node
		Pos int
	}
	// Binary is an arithmetic, text, comparison or logical operator.
	Binary struct {
		Op   string // + - * / % || = <> < <= > >= AND OR
		L, R Node
		Pos  int
	}
	// Is is "x IS [NOT] NULL|TRUE|FALSE".
	Is struct {
		X    Node
		What string // NULL TRUE FALSE
		Not  bool
		Pos  int
	}
	// Distinct is "a IS [NOT] DISTINCT FROM b".
	Distinct struct {
		L, R Node
		Not  bool
		Pos  int
	}
	// In is "x [NOT] IN (…)".
	In struct {
		X    Node
		List []Node
		Not  bool
		Pos  int
	}
	// Between is "x [NOT] BETWEEN lo AND hi".
	Between struct {
		X, Lo, Hi Node
		Not       bool
		Pos       int
	}
	// Like is "x [NOT] LIKE 'pattern'" (SQL LIKE: % and _).
	Like struct {
		X       Node
		Pattern string
		Not     bool
		Pos     int
	}
	// Case is "CASE [subject] WHEN … THEN … [ELSE …] END".
	Case struct {
		Subject Node // nil for the searched form
		Whens   []When
		Else    Node // nil when absent
		Pos     int
	}
	// When is one WHEN … THEN … of a Case.
	When struct{ Cond, Then Node }
	// Cast is CAST or TRY_CAST (both render as TRY_CAST for Flink CDC, ADR 0008 E4).
	Cast struct {
		X   Node
		To  Type
		Try bool
		Pos int
	}
	// Type is a CAST target; P and S only for DECIMAL.
	Type struct {
		Name string // STRING BOOLEAN INT BIGINT DOUBLE DECIMAL DATE TIMESTAMP
		P, S int
	}
)

func (n *Col) pos() int      { return n.Pos }
func (n *Str) pos() int      { return n.Pos }
func (n *Num) pos() int      { return n.Pos }
func (n *Bool) pos() int     { return n.Pos }
func (n *Null) pos() int     { return n.Pos }
func (n *Call) pos() int     { return n.Pos }
func (n *Unary) pos() int    { return n.Pos }
func (n *Binary) pos() int   { return n.Pos }
func (n *Is) pos() int       { return n.Pos }
func (n *Distinct) pos() int { return n.Pos }
func (n *In) pos() int       { return n.Pos }
func (n *Between) pos() int  { return n.Pos }
func (n *Like) pos() int     { return n.Pos }
func (n *Case) pos() int     { return n.Pos }
func (n *Cast) pos() int     { return n.Pos }

// Position is where a node starts in the parsed text.
func Position(n Node) int { return n.pos() }

// MetadataColumns are Flink CDC's columns about the row (read-only, always text).
var MetadataColumns = []string{"__namespace_name__", "__schema_name__", "__table_name__", "__data_event_type__"}

// CastTypes are the CAST targets.
var CastTypes = []string{"STRING", "BOOLEAN", "INT", "BIGINT", "DOUBLE", "DECIMAL", "TIMESTAMP"}

// keywords are the bare words the grammar gives a meaning; any other bare word is a column, or a
// function when "(" follows.
var keywords = []string{"AND", "OR", "NOT", "IS", "NULL", "TRUE", "FALSE", "IN", "BETWEEN", "LIKE", "CASE", "WHEN", "THEN", "ELSE",
	"END", "CAST", "TRY_CAST", "AS", "DISTINCT", "FROM", "ESCAPE", "SIMILAR", "SELECT", "WHERE", "INTERVAL"}

type parser struct {
	toks  []token
	i     int
	nodes int
	depth int
}

// Parse parses s into a tree, or returns an *Error at the position of the first problem.
func Parse(s string) (Node, error) {
	toks, err := lex(s)
	if err != nil {
		return nil, err
	}
	if len(toks) == 1 {
		return nil, errAt(0, 0, "Write an expression.")
	}
	p := &parser{toks: toks}
	n, err := p.expr()
	if err != nil {
		return nil, err
	}
	if t := p.peek(); t.kind != tEOF {
		return nil, errAt(t.pos, t.end-t.pos, "Unexpected %s: the expression ends before it.", describe(t))
	}
	return n, nil
}

func describe(t token) string {
	switch t.kind {
	case tString:
		return "text"
	case tNumber:
		return "number " + t.text
	case tEOF:
		return "end"
	}
	return "\"" + t.text + "\""
}

func (p *parser) peek() token { return p.toks[p.i] }
func (p *parser) next() token {
	t := p.toks[p.i]
	if t.kind != tEOF {
		p.i++
	}
	return t
}

// kw reports whether the next token is the bare keyword k (any case).
func (p *parser) kw(k string) bool {
	t := p.peek()
	return t.kind == tIdent && strings.EqualFold(t.text, k)
}

func (p *parser) op(o string) bool {
	t := p.peek()
	return t.kind == tOp && t.text == o
}

func (p *parser) expect(o string) error {
	if p.op(o) {
		p.next()
		return nil
	}
	t := p.peek()
	return errAt(t.pos, max(t.end-t.pos, 1), "Expected %q, found %s.", o, describe(t))
}

func (p *parser) expectKw(k string) error {
	if p.kw(k) {
		p.next()
		return nil
	}
	t := p.peek()
	return errAt(t.pos, max(t.end-t.pos, 1), "Expected %s, found %s.", k, describe(t))
}

// node counts a node and refuses trees that are too large.
func (p *parser) node(at int) error {
	p.nodes++
	if p.nodes > MaxNodes {
		return errAt(at, 1, "The expression is too large (at most %d parts).", MaxNodes)
	}
	return nil
}

func (p *parser) enter(at int) error {
	p.depth++
	if p.depth > MaxDepth {
		return errAt(at, 1, "The expression nests too deeply (at most %d levels).", MaxDepth)
	}
	return nil
}

func (p *parser) leave() { p.depth-- }

// Precedence, lowest first: OR, AND, NOT, comparison (and IS, IN, BETWEEN, LIKE), + - ||, * / %,
// unary minus.
func (p *parser) expr() (Node, error) { return p.or() }

func (p *parser) or() (Node, error) {
	l, err := p.and()
	if err != nil {
		return nil, err
	}
	for p.kw("OR") {
		t := p.next()
		r, err := p.and()
		if err != nil {
			return nil, err
		}
		if err := p.node(t.pos); err != nil {
			return nil, err
		}
		l = &Binary{Op: "OR", L: l, R: r, Pos: t.pos}
	}
	return l, nil
}

func (p *parser) and() (Node, error) {
	l, err := p.not()
	if err != nil {
		return nil, err
	}
	for p.kw("AND") {
		t := p.next()
		r, err := p.not()
		if err != nil {
			return nil, err
		}
		if err := p.node(t.pos); err != nil {
			return nil, err
		}
		l = &Binary{Op: "AND", L: l, R: r, Pos: t.pos}
	}
	return l, nil
}

func (p *parser) not() (Node, error) {
	if p.kw("NOT") {
		t := p.next()
		if err := p.enter(t.pos); err != nil {
			return nil, err
		}
		defer p.leave()
		x, err := p.not()
		if err != nil {
			return nil, err
		}
		if err := p.node(t.pos); err != nil {
			return nil, err
		}
		return &Unary{Op: "NOT", X: x, Pos: t.pos}, nil
	}
	return p.comparison()
}

var comparisons = []string{"=", "<>", "!=", "<", "<=", ">", ">="}

func (p *parser) comparison() (Node, error) {
	l, err := p.additive()
	if err != nil {
		return nil, err
	}
	t := p.peek()
	switch {
	case t.kind == tOp && slices.Contains(comparisons, t.text):
		p.next()
		r, err := p.additive()
		if err != nil {
			return nil, err
		}
		op := t.text
		if op == "!=" {
			op = "<>"
		}
		if err := p.node(t.pos); err != nil {
			return nil, err
		}
		return &Binary{Op: op, L: l, R: r, Pos: t.pos}, nil
	case p.kw("IS"):
		p.next()
		not := false
		if p.kw("NOT") {
			p.next()
			not = true
		}
		if err := p.node(t.pos); err != nil {
			return nil, err
		}
		switch {
		case p.kw("NULL") || p.kw("TRUE") || p.kw("FALSE"):
			return &Is{X: l, What: strings.ToUpper(p.next().text), Not: not, Pos: t.pos}, nil
		case p.kw("DISTINCT"):
			p.next()
			if err := p.expectKw("FROM"); err != nil {
				return nil, err
			}
			r, err := p.additive()
			if err != nil {
				return nil, err
			}
			return &Distinct{L: l, R: r, Not: not, Pos: t.pos}, nil
		}
		n := p.peek()
		return nil, errAt(n.pos, max(n.end-n.pos, 1), "After IS comes NULL, TRUE, FALSE or DISTINCT FROM.")
	}
	not := false
	if p.kw("NOT") {
		p.next()
		not = true
	}
	switch {
	case p.kw("IN"):
		p.next()
		if err := p.expect("("); err != nil {
			return nil, err
		}
		var list []Node
		for {
			v, err := p.additive()
			if err != nil {
				return nil, err
			}
			list = append(list, v)
			if !p.op(",") {
				break
			}
			p.next()
		}
		if err := p.expect(")"); err != nil {
			return nil, err
		}
		if err := p.node(t.pos); err != nil {
			return nil, err
		}
		return &In{X: l, List: list, Not: not, Pos: t.pos}, nil
	case p.kw("BETWEEN"):
		p.next()
		lo, err := p.additive()
		if err != nil {
			return nil, err
		}
		if err := p.expectKw("AND"); err != nil {
			return nil, err
		}
		hi, err := p.additive()
		if err != nil {
			return nil, err
		}
		if err := p.node(t.pos); err != nil {
			return nil, err
		}
		return &Between{X: l, Lo: lo, Hi: hi, Not: not, Pos: t.pos}, nil
	case p.kw("LIKE"):
		p.next()
		pt := p.next()
		if pt.kind != tString {
			return nil, errAt(pt.pos, max(pt.end-pt.pos, 1), "LIKE takes a pattern in single quotes, e.g. 'a%%'.")
		}
		if p.kw("ESCAPE") {
			e := p.peek()
			return nil, errAt(e.pos, e.end-e.pos, "ESCAPE can't be chosen: BeziSync writes it, so %% and _ in a pattern always mean any text and any character.")
		}
		if err := p.node(t.pos); err != nil {
			return nil, err
		}
		return &Like{X: l, Pattern: pt.text, Not: not, Pos: t.pos}, nil
	case p.kw("SIMILAR"):
		s := p.peek()
		return nil, errAt(s.pos, s.end-s.pos, "SIMILAR TO isn't available: it runs a regular expression.")
	}
	if not {
		n := p.peek()
		return nil, errAt(n.pos, max(n.end-n.pos, 1), "After NOT here comes IN, BETWEEN or LIKE.")
	}
	return l, nil
}

func (p *parser) additive() (Node, error) {
	l, err := p.multiplicative()
	if err != nil {
		return nil, err
	}
	for p.op("+") || p.op("-") || p.op("||") {
		t := p.next()
		r, err := p.multiplicative()
		if err != nil {
			return nil, err
		}
		if err := p.node(t.pos); err != nil {
			return nil, err
		}
		l = &Binary{Op: t.text, L: l, R: r, Pos: t.pos}
	}
	return l, nil
}

func (p *parser) multiplicative() (Node, error) {
	l, err := p.unary()
	if err != nil {
		return nil, err
	}
	for p.op("*") || p.op("/") || p.op("%") {
		t := p.next()
		r, err := p.unary()
		if err != nil {
			return nil, err
		}
		if err := p.node(t.pos); err != nil {
			return nil, err
		}
		l = &Binary{Op: t.text, L: l, R: r, Pos: t.pos}
	}
	return l, nil
}

func (p *parser) unary() (Node, error) {
	if p.op("-") || p.op("+") {
		t := p.next()
		if err := p.enter(t.pos); err != nil {
			return nil, err
		}
		defer p.leave()
		x, err := p.unary()
		if err != nil {
			return nil, err
		}
		if t.text == "+" {
			return x, nil
		}
		if err := p.node(t.pos); err != nil {
			return nil, err
		}
		return &Unary{Op: "-", X: x, Pos: t.pos}, nil
	}
	return p.primary()
}

func (p *parser) primary() (Node, error) {
	t := p.peek()
	if err := p.node(t.pos); err != nil {
		return nil, err
	}
	switch t.kind {
	case tString:
		p.next()
		return &Str{V: t.text, Pos: t.pos}, nil
	case tNumber:
		p.next()
		return &Num{Text: canonicalNumber(t.text), Pos: t.pos}, nil
	case tQuoted:
		p.next()
		return &Col{Name: t.text, Pos: t.pos}, nil
	case tOp:
		if t.text == "(" {
			p.next()
			if err := p.enter(t.pos); err != nil {
				return nil, err
			}
			defer p.leave()
			x, err := p.expr()
			if err != nil {
				return nil, err
			}
			if err := p.expect(")"); err != nil {
				return nil, err
			}
			return x, nil
		}
	case tIdent:
		up := strings.ToUpper(t.text)
		switch up {
		case "NULL":
			p.next()
			return &Null{Pos: t.pos}, nil
		case "TRUE", "FALSE":
			p.next()
			return &Bool{V: up == "TRUE", Pos: t.pos}, nil
		case "CASE":
			return p.caseExpr()
		case "CAST", "TRY_CAST":
			return p.cast()
		}
		// A function when "(" follows; a column otherwise.
		if p.toks[p.i+1].kind == tOp && p.toks[p.i+1].text == "(" {
			return p.call()
		}
		if slices.Contains(keywords, up) {
			return nil, errAt(t.pos, t.end-t.pos, "%s can't be used here. A column with this name goes in back-quotes: `%s`.", up, t.text)
		}
		if bareFunctions[up] {
			p.next()
			return &Call{Fn: up, Pos: t.pos}, nil
		}
		p.next()
		return &Col{Name: t.text, Pos: t.pos}, nil
	}
	return nil, errAt(t.pos, max(t.end-t.pos, 1), "Expected a value, found %s.", describe(t))
}

// bareFunctions are called without parentheses in Flink CDC's SQL.
var bareFunctions = map[string]bool{"LOCALTIMESTAMP": true, "CURRENT_TIMESTAMP": true, "CURRENT_DATE": true}

func (p *parser) call() (Node, error) {
	t := p.next()
	name := strings.ToUpper(t.text)
	fn, ok := Functions[name]
	if !ok {
		return nil, errAt(t.pos, t.end-t.pos, "%s isn't a function BeziSync offers.%s", t.text, unknownHint(name))
	}
	if err := p.enter(t.pos); err != nil {
		return nil, err
	}
	defer p.leave()
	p.next() // (
	c := &Call{Fn: name, Pos: t.pos}
	if fn.Unit {
		u := p.next()
		up := strings.ToUpper(u.text)
		if u.kind != tIdent || !slices.Contains(TimeUnits, up) {
			return nil, errAt(u.pos, max(u.end-u.pos, 1), "%s starts with a unit: %s.", name, strings.Join(TimeUnits, ", "))
		}
		c.Unit = up
		if err := p.expect(","); err != nil {
			return nil, err
		}
	}
	if !p.op(")") {
		for {
			a, err := p.expr()
			if err != nil {
				return nil, err
			}
			c.Args = append(c.Args, a)
			if !p.op(",") {
				break
			}
			p.next()
		}
	}
	if err := p.expect(")"); err != nil {
		return nil, err
	}
	if n := len(c.Args); n < fn.Min || n > fn.Max {
		return nil, errAt(t.pos, t.end-t.pos, "%s takes %s.", name, fn.arity())
	}
	return c, nil
}

func unknownHint(name string) string {
	switch {
	case strings.HasPrefix(name, "REGEXP"):
		return " Regular expressions aren't available: they can stall the pipeline."
	case name == "DAY":
		return " Use DAYOFMONTH."
	case name == "SUBSTRING":
		return " Use SUBSTR(text, from, length)."
	case name == "TO_TIMESTAMP":
		return " It stops the pipeline on text it can't read: use TRY_CAST(text AS TIMESTAMP), which reads ISO text (2026-11-07T10:20:30) and gives NULL otherwise."
	}
	return ""
}

func (p *parser) caseExpr() (Node, error) {
	t := p.next()
	if err := p.enter(t.pos); err != nil {
		return nil, err
	}
	defer p.leave()
	c := &Case{Pos: t.pos}
	if !p.kw("WHEN") {
		s, err := p.expr()
		if err != nil {
			return nil, err
		}
		c.Subject = s
	}
	for p.kw("WHEN") {
		p.next()
		cond, err := p.expr()
		if err != nil {
			return nil, err
		}
		if err := p.expectKw("THEN"); err != nil {
			return nil, err
		}
		then, err := p.expr()
		if err != nil {
			return nil, err
		}
		c.Whens = append(c.Whens, When{Cond: cond, Then: then})
	}
	if len(c.Whens) == 0 {
		n := p.peek()
		return nil, errAt(n.pos, max(n.end-n.pos, 1), "CASE needs at least one WHEN … THEN ….")
	}
	if p.kw("ELSE") {
		p.next()
		e, err := p.expr()
		if err != nil {
			return nil, err
		}
		c.Else = e
	}
	if err := p.expectKw("END"); err != nil {
		return nil, err
	}
	return c, nil
}

func (p *parser) cast() (Node, error) {
	t := p.next()
	if err := p.enter(t.pos); err != nil {
		return nil, err
	}
	defer p.leave()
	if err := p.expect("("); err != nil {
		return nil, err
	}
	x, err := p.expr()
	if err != nil {
		return nil, err
	}
	if err := p.expectKw("AS"); err != nil {
		return nil, err
	}
	tt := p.next()
	name := strings.ToUpper(tt.text)
	if name == "VARCHAR" {
		name = "STRING"
	}
	if name == "DATE" {
		return nil, errAt(tt.pos, tt.end-tt.pos, "CAST to DATE doesn't run on Flink CDC: use TO_DATE(text, 'yyyy-MM-dd').")
	}
	if tt.kind != tIdent || !slices.Contains(CastTypes, name) {
		return nil, errAt(tt.pos, max(tt.end-tt.pos, 1), "CAST to one of %s.", strings.Join(CastTypes, ", "))
	}
	ty := Type{Name: name}
	if name == "DECIMAL" {
		if err := p.expect("("); err != nil {
			return nil, errAt(tt.pos, tt.end-tt.pos, "DECIMAL needs its precision and scale, e.g. DECIMAL(18, 2).")
		}
		pr, err := p.smallInt(1, 38, "precision")
		if err != nil {
			return nil, err
		}
		if err := p.expect(","); err != nil {
			return nil, errAt(tt.pos, tt.end-tt.pos, "DECIMAL needs its precision and scale, e.g. DECIMAL(18, 2).")
		}
		sc, err := p.smallInt(0, pr, "scale")
		if err != nil {
			return nil, err
		}
		if err := p.expect(")"); err != nil {
			return nil, err
		}
		ty.P, ty.S = pr, sc
	}
	if err := p.expect(")"); err != nil {
		return nil, err
	}
	return &Cast{X: x, To: ty, Try: strings.EqualFold(t.text, "TRY_CAST"), Pos: t.pos}, nil
}

func (p *parser) smallInt(lo, hi int, what string) (int, error) {
	t := p.next()
	n, err := strconv.Atoi(t.text)
	if t.kind != tNumber || err != nil || n < lo || n > hi {
		return 0, errAt(t.pos, max(t.end-t.pos, 1), "The %s is a whole number from %d to %d.", what, lo, hi)
	}
	return n, nil
}

// canonicalNumber drops leading zeros of the whole part (keeping one) and a trailing ".".
func canonicalNumber(s string) string {
	whole, frac, dot := strings.Cut(s, ".")
	whole = strings.TrimLeft(whole, "0")
	if whole == "" {
		whole = "0"
	}
	if !dot || frac == "" {
		return whole
	}
	return whole + "." + frac
}
