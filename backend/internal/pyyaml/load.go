package pyyaml

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Tags PyYAML's implicit resolvers produce for plain scalars (YAML 1.1).
const (
	TagStr       = "str"
	TagBool      = "bool"
	TagInt       = "int"
	TagFloat     = "float"
	TagNull      = "null"
	TagTimestamp = "timestamp"
	TagMerge     = "merge"
	TagValue     = "value"
)

var (
	boolRe  = regexp.MustCompile(`^(?:yes|Yes|YES|no|No|NO|true|True|TRUE|false|False|FALSE|on|On|ON|off|Off|OFF)$`)
	floatRe = regexp.MustCompile(`^(?:[-+]?(?:[0-9][0-9_]*)\.[0-9_]*(?:[eE][-+][0-9]+)?` +
		`|\.[0-9][0-9_]*(?:[eE][-+][0-9]+)?` +
		`|[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+\.[0-9_]*` +
		`|[-+]?\.(?:inf|Inf|INF)` +
		`|\.(?:nan|NaN|NAN))$`)
	intRe = regexp.MustCompile(`^(?:[-+]?0b[0-1_]+` +
		`|[-+]?0[0-7_]+` +
		`|[-+]?(?:0|[1-9][0-9_]*)` +
		`|[-+]?0x[0-9a-fA-F_]+` +
		`|[-+]?[1-9][0-9_]*(?::[0-5]?[0-9])+)$`)
	mergeRe     = regexp.MustCompile(`^(?:<<)$`)
	nullRe      = regexp.MustCompile(`^(?:~|null|Null|NULL|)$`)
	timestampRe = regexp.MustCompile(`^(?:[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]` +
		`|[0-9][0-9][0-9][0-9]-[0-9][0-9]?-[0-9][0-9]?` +
		`(?:[Tt]|[ \t]+)[0-9][0-9]?` +
		`:[0-9][0-9]:[0-9][0-9](?:\.[0-9]*)?` +
		`(?:[ \t]*(?:Z|[-+][0-9][0-9]?(?::[0-9][0-9])?))?)$`)
	valueRe = regexp.MustCompile(`^(?:=)$`)
)

type resolver struct {
	tag    string
	re     *regexp.Regexp
	starts string
}

// Registration order matters: PyYAML tries resolvers for the first character in order.
var resolvers = []resolver{
	{TagBool, boolRe, "yYnNtTfFoO"},
	{TagFloat, floatRe, "-+0123456789."},
	{TagInt, intRe, "-+0123456789"},
	{TagMerge, mergeRe, "<"},
	{TagNull, nullRe, "~nN"},
	{TagTimestamp, timestampRe, "0123456789"},
	{TagValue, valueRe, "="},
}

// Resolve returns the tag a plain scalar resolves to.
func Resolve(value string) string {
	if value == "" {
		return TagNull // only the null resolver is registered for ''
	}
	first := []rune(value)[0]
	for _, r := range resolvers {
		if strings.ContainsRune(r.starts, first) && r.re.MatchString(value) {
			return r.tag
		}
	}
	return TagStr
}

// Timestamp is a YAML 1.1 timestamp (PyYAML builds a datetime/date, which validates as
// neither a string nor a number).
type Timestamp struct{ time.Time }

// LoadAll is yaml.safe_load_all: it returns every document (nil for empty documents).
// Mappings decode to map[string]any, numbers to json.Number (ints) or json.Number with a
// fraction (floats), so values validate like the Python objects PyYAML produced.
func LoadAll(text string) ([]any, error) {
	dec := yaml.NewDecoder(strings.NewReader(text))
	var docs []any
	for {
		var n yaml.Node
		err := dec.Decode(&n)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		v, err := construct(&n)
		if err != nil {
			return nil, err
		}
		docs = append(docs, v)
	}
	return docs, nil
}

func construct(n *yaml.Node) (any, error) {
	switch n.Kind {
	case yaml.DocumentNode:
		if len(n.Content) == 0 {
			return nil, nil
		}
		return construct(n.Content[0])
	case yaml.AliasNode:
		return construct(n.Alias)
	case yaml.SequenceNode:
		out := make([]any, 0, len(n.Content))
		for _, c := range n.Content {
			v, err := construct(c)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	case yaml.MappingNode:
		out := map[string]any{}
		var merges []map[string]any
		for i := 0; i+1 < len(n.Content); i += 2 {
			k, vn := n.Content[i], n.Content[i+1]
			if k.Kind == yaml.ScalarNode && (k.Tag == "!!merge" || (k.Style == 0 && k.Value == "<<")) {
				m, err := mergeSource(vn)
				if err != nil {
					return nil, err
				}
				merges = append(merges, m...)
				continue
			}
			kv, err := construct(k)
			if err != nil {
				return nil, err
			}
			key, err := keyString(kv)
			if err != nil {
				return nil, err
			}
			v, err := construct(vn)
			if err != nil {
				return nil, err
			}
			out[key] = v // duplicate keys: last wins, like PyYAML
		}
		for _, m := range merges { // explicit keys win over merged ones
			for k, v := range m {
				if _, ok := out[k]; !ok {
					out[k] = v
				}
			}
		}
		return out, nil
	case yaml.ScalarNode:
		return scalar(n)
	}
	return nil, fmt.Errorf("unsupported YAML node")
}

func mergeSource(n *yaml.Node) ([]map[string]any, error) {
	v, err := construct(n)
	if err != nil {
		return nil, err
	}
	switch x := v.(type) {
	case map[string]any:
		return []map[string]any{x}, nil
	case []any:
		var out []map[string]any
		for _, e := range x {
			m, ok := e.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("expected a mapping for merging")
			}
			out = append(out, m)
		}
		return out, nil
	}
	return nil, fmt.Errorf("expected a mapping or list of mappings for merging")
}

func keyString(k any) (string, error) {
	switch x := k.(type) {
	case string:
		return x, nil
	case nil:
		return "None", nil
	case bool:
		if x {
			return "True", nil
		}
		return "False", nil
	case json.Number:
		return string(x), nil
	}
	return fmt.Sprint(k), nil
}

func scalar(n *yaml.Node) (any, error) {
	explicit := n.Style&yaml.TaggedStyle != 0
	quoted := n.Style&(yaml.DoubleQuotedStyle|yaml.SingleQuotedStyle|yaml.LiteralStyle|yaml.FoldedStyle) != 0
	var tag string
	switch {
	case explicit:
		switch n.Tag {
		case "!!int":
			tag = TagInt
		case "!!float":
			tag = TagFloat
		case "!!bool":
			tag = TagBool
		case "!!null":
			tag = TagNull
		case "!!timestamp":
			tag = TagTimestamp
		default:
			tag = TagStr
		}
	case quoted:
		return n.Value, nil
	default:
		// yaml.v3 resolves plain scalars with YAML 1.2 rules; PyYAML used YAML 1.1.
		tag = Resolve(n.Value)
	}
	v := n.Value
	switch tag {
	case TagNull:
		return nil, nil
	case TagBool:
		switch strings.ToLower(v) {
		case "yes", "true", "on", "y":
			return true, nil
		}
		return false, nil
	case TagInt:
		return constructInt(v)
	case TagFloat:
		return constructFloat(v)
	case TagTimestamp:
		t, err := time.Parse("2006-1-2", strings.Fields(strings.ReplaceAll(strings.ReplaceAll(v, "T", " "), "t", " "))[0])
		if err != nil {
			t = time.Time{}
		}
		return Timestamp{t}, nil
	}
	return v, nil
}

func constructInt(v string) (any, error) {
	s := strings.ReplaceAll(v, "_", "")
	sign := int64(1)
	if strings.HasPrefix(s, "-") {
		sign = -1
		s = s[1:]
	} else if strings.HasPrefix(s, "+") {
		s = s[1:]
	}
	var n int64
	var err error
	switch {
	case s == "0":
		return json.Number("0"), nil
	case strings.HasPrefix(s, "0b"):
		n, err = strconv.ParseInt(s[2:], 2, 64)
	case strings.HasPrefix(s, "0x"):
		n, err = strconv.ParseInt(s[2:], 16, 64)
	case strings.HasPrefix(s, "0"):
		n, err = strconv.ParseInt(s[1:], 8, 64)
	case strings.Contains(s, ":"):
		parts := strings.Split(s, ":")
		base := int64(1)
		for i := len(parts) - 1; i >= 0; i-- {
			d, e := strconv.ParseInt(parts[i], 10, 64)
			if e != nil {
				return nil, e
			}
			n += d * base
			base *= 60
		}
	default:
		n, err = strconv.ParseInt(s, 10, 64)
	}
	if err != nil {
		return nil, err
	}
	return json.Number(strconv.FormatInt(sign*n, 10)), nil
}

func constructFloat(v string) (any, error) {
	s := strings.ToLower(strings.ReplaceAll(v, "_", ""))
	sign := 1.0
	if strings.HasPrefix(s, "-") {
		sign = -1
		s = s[1:]
	} else if strings.HasPrefix(s, "+") {
		s = s[1:]
	}
	var f float64
	switch {
	case s == ".inf":
		f = math.Inf(1)
	case s == ".nan":
		return math.NaN(), nil
	case strings.Contains(s, ":"):
		parts := strings.Split(s, ":")
		base := 1.0
		for i := len(parts) - 1; i >= 0; i-- {
			d, err := strconv.ParseFloat(parts[i], 64)
			if err != nil {
				return nil, err
			}
			f += d * base
			base *= 60
		}
	default:
		var err error
		f, err = strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, err
		}
	}
	f *= sign
	if math.IsInf(f, 0) {
		return f, nil
	}
	r, err := floatNumber(f)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func floatNumber(f float64) (json.Number, error) {
	s := strconv.FormatFloat(f, 'g', -1, 64)
	if !strings.ContainsAny(s, ".eE") {
		s += ".0"
	}
	return json.Number(s), nil
}
