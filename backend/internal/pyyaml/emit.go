// Package pyyaml reads and writes YAML exactly like PyYAML's safe_load / safe_dump did,
// so exported manifests and spec diffs are byte-identical to the Python implementation.
//
// The emitter is a port of PyYAML's emitter for the block style safe_dump produces
// (default_flow_style=False, indent 2, width 80). Mappings are *om.Map (insertion order)
// or map[string]any (JSONB key order); sequences are []any.
package pyyaml

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
)

const (
	bestIndent = 2
	bestWidth  = 80
)

type emitter struct {
	out          strings.Builder
	allowUnicode bool
	indent       int // -1 = None
	indents      []int
	column       int
	whitespace   bool
	indention    bool
	flowLevel    int
}

// Dump is yaml.safe_dump(doc, sort_keys=False[, allow_unicode]).
func Dump(doc any, allowUnicode bool) (string, error) {
	return DumpAll([]any{doc}, allowUnicode)
}

// DumpAll is yaml.safe_dump_all(docs, sort_keys=False[, allow_unicode]).
func DumpAll(docs []any, allowUnicode bool) (string, error) {
	e := &emitter{allowUnicode: allowUnicode, indent: -1, whitespace: true, indention: true}
	for i, d := range docs {
		if i > 0 {
			e.writeIndent()
			e.writeIndicator("---", true, false, false)
		}
		if err := e.node(d, true, false, false, false); err != nil {
			return "", err
		}
		e.writeIndent() // document end
	}
	return e.out.String(), nil
}

func (e *emitter) increaseIndent(flow, indentless bool) {
	e.indents = append(e.indents, e.indent)
	if e.indent < 0 {
		if flow {
			e.indent = bestIndent
		} else {
			e.indent = 0
		}
	} else if !indentless {
		e.indent += bestIndent
	}
}

func (e *emitter) popIndent() {
	e.indent = e.indents[len(e.indents)-1]
	e.indents = e.indents[:len(e.indents)-1]
}

func (e *emitter) write(s string) {
	e.out.WriteString(s)
	e.column += utf8.RuneCountInString(s)
}

func (e *emitter) writeIndicator(ind string, needWhitespace, whitespace, indention bool) {
	data := ind
	if !e.whitespace && needWhitespace {
		data = " " + ind
	}
	e.whitespace = whitespace
	e.indention = e.indention && indention
	e.write(data)
}

func (e *emitter) writeLineBreak() {
	e.out.WriteByte('\n')
	e.whitespace = true
	e.indention = true
	e.column = 0
}

func (e *emitter) writeIndent() {
	indent := e.indent
	if indent < 0 {
		indent = 0
	}
	if !e.indention || e.column > indent || (e.column == indent && !e.whitespace) {
		e.writeLineBreak()
	}
	if e.column < indent {
		e.whitespace = true
		e.out.WriteString(strings.Repeat(" ", indent-e.column))
		e.column = indent
	}
}

// node emits one value. mappingCtx/simpleKey mirror PyYAML's expect_node flags.
func (e *emitter) node(v any, root, sequence, mappingCtx, simpleKey bool) error {
	switch x := v.(type) {
	case *om.Map:
		if x.Len() == 0 || e.flowLevel > 0 {
			e.writeIndicator("{", true, true, false)
			e.writeIndicator("}", false, false, false)
			return nil
		}
		e.increaseIndent(false, false)
		for _, k := range x.Keys() {
			val, _ := x.Get(k)
			if err := e.mappingItem(k, val); err != nil {
				return err
			}
		}
		e.popIndent()
		return nil
	case map[string]any:
		return e.node(om.FromJSONB(x), root, sequence, mappingCtx, simpleKey)
	case []any:
		if len(x) == 0 || e.flowLevel > 0 {
			e.writeIndicator("[", true, true, false)
			e.writeIndicator("]", false, false, false)
			return nil
		}
		indentless := mappingCtx && !e.indention
		e.increaseIndent(false, indentless)
		for _, item := range x {
			e.writeIndent()
			e.writeIndicator("-", true, false, true)
			if err := e.node(item, false, true, false, false); err != nil {
				return err
			}
		}
		e.popIndent()
		return nil
	case []string:
		l := make([]any, len(x))
		for i, s := range x {
			l[i] = s
		}
		return e.node(l, root, sequence, mappingCtx, simpleKey)
	}
	text, isStr, err := represent(v)
	if err != nil {
		return err
	}
	e.increaseIndent(true, false)
	e.scalar(text, isStr, simpleKey)
	e.popIndent()
	return nil
}

func (e *emitter) mappingItem(key string, value any) error {
	e.writeIndent()
	a := analyze(key, e.allowUnicode)
	// check_simple_key: len(scalar) + len("!!str") < 128, not empty, not multiline.
	simple := utf8.RuneCountInString(key)+5 < 128 && !a.empty && !a.multiline
	if simple {
		if err := e.node(key, false, false, true, true); err != nil {
			return err
		}
		e.writeIndicator(":", false, false, false)
		return e.node(value, false, false, true, false)
	}
	e.writeIndicator("?", true, false, true)
	if err := e.node(key, false, false, true, false); err != nil {
		return err
	}
	e.writeIndent()
	e.writeIndicator(":", true, false, true)
	return e.node(value, false, false, true, false)
}

// represent renders a non-collection value as PyYAML's SafeRepresenter would; isStr
// reports a str-tagged scalar (which may need quoting to keep its type).
func represent(v any) (string, bool, error) {
	switch x := v.(type) {
	case nil:
		return "null", false, nil
	case string:
		return x, true, nil
	case bool:
		if x {
			return "true", false, nil
		}
		return "false", false, nil
	case int:
		return strconv.Itoa(x), false, nil
	case int64:
		return strconv.FormatInt(x, 10), false, nil
	case float64:
		return reprFloat(x), false, nil
	case pyjson.Float:
		return reprFloat(float64(x)), false, nil
	case json.Number:
		s := string(x)
		if strings.ContainsAny(s, ".eE") {
			f, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return "", false, err
			}
			return reprFloat(f), false, nil
		}
		return s, false, nil
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10), false, nil
	case reflect.Float32, reflect.Float64:
		return reprFloat(rv.Float()), false, nil
	case reflect.String:
		return rv.String(), true, nil
	}
	return "", false, fmt.Errorf("pyyaml: cannot represent %T", v)
}

func reprFloat(f float64) string {
	switch {
	case math.IsNaN(f):
		return ".nan"
	case math.IsInf(f, 1):
		return ".inf"
	case math.IsInf(f, -1):
		return "-.inf"
	}
	s, _ := pyjson.FloatRepr(f)
	s = strings.ToLower(s)
	if !strings.Contains(s, ".") && strings.Contains(s, "e") {
		s = strings.Replace(s, "e", ".0e", 1)
	}
	return s
}

type analysis struct {
	empty, multiline                                         bool
	allowFlowPlain, allowBlockPlain, allowSingle, allowBlock bool
}

func isBreak(r rune) bool { return r == '\n' || r == '\u0085' || r == ' ' || r == ' ' }

func isWS(r rune) bool {
	return r == 0 || r == ' ' || r == '\t' || r == '\r' || isBreak(r)
}

func analyze(scalar string, allowUnicode bool) analysis {
	if scalar == "" {
		return analysis{empty: true, allowBlockPlain: true, allowSingle: true}
	}
	rs := []rune(scalar)
	var blockInd, flowInd, lineBreaks, special bool
	var leadingSpace, leadingBreak, trailingSpace, trailingBreak, breakSpace, spaceBreak bool
	if strings.HasPrefix(scalar, "---") || strings.HasPrefix(scalar, "...") {
		blockInd, flowInd = true, true
	}
	precededByWS := true
	followedByWS := len(rs) == 1 || isWS(rs[1])
	prevSpace, prevBreak := false, false
	for i, ch := range rs {
		if i == 0 {
			if strings.ContainsRune("#,[]{}&*!|>'\"%@`", ch) {
				flowInd, blockInd = true, true
			}
			if ch == '?' || ch == ':' {
				flowInd = true
				if followedByWS {
					blockInd = true
				}
			}
			if ch == '-' && followedByWS {
				flowInd, blockInd = true, true
			}
		} else {
			if strings.ContainsRune(",?[]{}", ch) {
				flowInd = true
			}
			if ch == ':' {
				flowInd = true
				if followedByWS {
					blockInd = true
				}
			}
			if ch == '#' && precededByWS {
				flowInd, blockInd = true, true
			}
		}
		if isBreak(ch) {
			lineBreaks = true
		}
		if !(ch == '\n' || (ch >= 0x20 && ch <= 0x7e)) {
			if (ch == 0x85 || (ch >= 0xa0 && ch <= 0xd7ff) || (ch >= 0xe000 && ch <= 0xfffd) ||
				(ch >= 0x10000 && ch < 0x10ffff)) && ch != 0xfeff {
				if !allowUnicode {
					special = true
				}
			} else {
				special = true
			}
		}
		switch {
		case ch == ' ':
			if i == 0 {
				leadingSpace = true
			}
			if i == len(rs)-1 {
				trailingSpace = true
			}
			if prevBreak {
				breakSpace = true
			}
			prevSpace, prevBreak = true, false
		case isBreak(ch):
			if i == 0 {
				leadingBreak = true
			}
			if i == len(rs)-1 {
				trailingBreak = true
			}
			if prevSpace {
				spaceBreak = true
			}
			prevSpace, prevBreak = false, true
		default:
			prevSpace, prevBreak = false, false
		}
		precededByWS = isWS(ch)
		followedByWS = i+2 >= len(rs) || isWS(rs[i+2])
	}
	a := analysis{multiline: lineBreaks, allowFlowPlain: true, allowBlockPlain: true, allowSingle: true, allowBlock: true}
	if leadingSpace || leadingBreak || trailingSpace || trailingBreak {
		a.allowFlowPlain, a.allowBlockPlain = false, false
	}
	if trailingSpace {
		a.allowBlock = false
	}
	if breakSpace {
		a.allowFlowPlain, a.allowBlockPlain, a.allowSingle = false, false, false
	}
	if spaceBreak || special {
		a.allowFlowPlain, a.allowBlockPlain, a.allowSingle, a.allowBlock = false, false, false, false
	}
	if lineBreaks {
		a.allowFlowPlain, a.allowBlockPlain = false, false
	}
	if flowInd {
		a.allowFlowPlain = false
	}
	if blockInd {
		a.allowBlockPlain = false
	}
	return a
}

func (e *emitter) scalar(text string, isStr, simpleKey bool) {
	a := analyze(text, e.allowUnicode)
	// implicit[0]: the plain form resolves back to the same tag.
	implicit := !isStr || Resolve(text) == TagStr
	style := byte('"')
	switch {
	case implicit && !(simpleKey && (a.empty || a.multiline)) &&
		((e.flowLevel > 0 && a.allowFlowPlain) || (e.flowLevel == 0 && a.allowBlockPlain)):
		style = 0
	case a.allowSingle && !(simpleKey && a.multiline):
		style = '\''
	}
	split := !simpleKey
	switch style {
	case 0:
		e.writePlain(text, split)
	case '\'':
		e.writeSingleQuoted(text, split)
	default:
		e.writeDoubleQuoted(text, split)
	}
}

func (e *emitter) writePlain(text string, split bool) {
	if text == "" {
		return
	}
	if !e.whitespace {
		e.write(" ")
	}
	e.whitespace = false
	e.indention = false
	rs := []rune(text)
	spaces := false
	start := 0
	for end := 0; end <= len(rs); end++ {
		var ch rune = -1
		if end < len(rs) {
			ch = rs[end]
		}
		if spaces {
			if ch != ' ' {
				if start+1 == end && e.column > bestWidth && split {
					e.writeIndent()
					e.whitespace = false
					e.indention = false
				} else {
					e.write(string(rs[start:end]))
				}
				start = end
			}
		} else if ch == -1 || ch == ' ' || isBreak(ch) {
			e.write(string(rs[start:end]))
			start = end
		}
		if ch != -1 {
			spaces = ch == ' '
		}
	}
}

func (e *emitter) writeSingleQuoted(text string, split bool) {
	e.writeIndicator("'", true, false, false)
	rs := []rune(text)
	spaces, breaks := false, false
	start := 0
	for end := 0; end <= len(rs); end++ {
		var ch rune = -1
		if end < len(rs) {
			ch = rs[end]
		}
		switch {
		case spaces:
			if ch == -1 || ch != ' ' {
				if start+1 == end && e.column > bestWidth && split && start != 0 && end != len(rs) {
					e.writeIndent()
				} else {
					e.write(string(rs[start:end]))
				}
				start = end
			}
		case breaks:
			if ch == -1 || !isBreak(ch) {
				if rs[start] == '\n' {
					e.writeLineBreak()
				}
				for _, br := range rs[start:end] {
					if br == '\n' {
						e.writeLineBreak()
					} else {
						e.out.WriteRune(br)
						e.whitespace, e.indention, e.column = true, true, 0
					}
				}
				e.writeIndent()
				start = end
			}
		default:
			if ch == -1 || ch == ' ' || isBreak(ch) || ch == '\'' {
				if start < end {
					e.write(string(rs[start:end]))
					start = end
				}
			}
		}
		if ch == '\'' {
			e.write("''")
			start = end + 1
		}
		if ch != -1 {
			spaces = ch == ' '
			breaks = isBreak(ch)
		}
	}
	e.writeIndicator("'", false, false, false)
}

var escapes = map[rune]string{
	0: "0", 0x07: "a", 0x08: "b", 0x09: "t", 0x0a: "n", 0x0b: "v", 0x0c: "f", 0x0d: "r", 0x1b: "e",
	'"': "\"", '\\': "\\", 0x85: "N", 0xa0: "_", 0x2028: "L", 0x2029: "P",
}

func (e *emitter) writeDoubleQuoted(text string, split bool) {
	e.writeIndicator("\"", true, false, false)
	rs := []rune(text)
	start := 0
	for end := 0; end <= len(rs); end++ {
		var ch rune = -1
		if end < len(rs) {
			ch = rs[end]
		}
		needsEscape := ch == -1 || ch == '"' || ch == '\\' || ch == 0x85 || ch == 0x2028 || ch == 0x2029 || ch == 0xfeff ||
			!((ch >= 0x20 && ch <= 0x7e) || (e.allowUnicode && ((ch >= 0xa0 && ch <= 0xd7ff) || (ch >= 0xe000 && ch <= 0xfffd))))
		if needsEscape {
			if start < end {
				e.write(string(rs[start:end]))
				start = end
			}
			if ch != -1 {
				var data string
				if r, ok := escapes[ch]; ok {
					data = "\\" + r
				} else if ch <= 0xff {
					data = fmt.Sprintf("\\x%02X", ch)
				} else if ch <= 0xffff {
					data = fmt.Sprintf("\\u%04X", ch)
				} else {
					data = fmt.Sprintf("\\U%08X", ch)
				}
				e.write(data)
				start = end + 1
			}
		}
		if end > 0 && end < len(rs)-1 && (ch == ' ' || start >= end) && e.column+(end-start) > bestWidth && split {
			data := string(rs[start:end]) + "\\"
			if start < end {
				start = end
			}
			e.write(data)
			e.writeIndent()
			e.whitespace = false
			e.indention = false
			if rs[start] == ' ' {
				e.write("\\")
			}
		}
	}
	e.writeIndicator("\"", false, false, false)
}
