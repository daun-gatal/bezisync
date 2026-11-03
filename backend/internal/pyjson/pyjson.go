// Package pyjson encodes values exactly like Python's json.dumps.
//
// Stored hashes (pipeline spec hashes, connector config hashes) were computed by the Python
// implementation with json.dumps, so they only stay stable if Go produces the same bytes:
// ensure_ascii escaping, Python's float repr, sorted keys and the chosen separators.
package pyjson

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Options mirror the json.dumps arguments that matter for hashing.
type Options struct {
	// SortKeys sorts object keys by code point (sort_keys=True).
	SortKeys bool
	// Compact uses separators (",", ":"); otherwise Python's defaults (", ", ": ").
	Compact bool
	// EnsureASCII escapes every non-ASCII character as \uXXXX (Python's default).
	EnsureASCII bool
}

// Dumps is json.dumps(value, sort_keys=True) with Python's default separators.
func Dumps(v any) (string, error) {
	return Encode(v, Options{SortKeys: true, EnsureASCII: true})
}

// DumpsCompact is json.dumps(value, sort_keys=True, separators=(",", ":")).
func DumpsCompact(v any) (string, error) {
	return Encode(v, Options{SortKeys: true, Compact: true, EnsureASCII: true})
}

// Float marks a value that Python holds as a float (repr "10.0", not "10").
type Float float64

// Encode renders v. Supported: nil, bool, strings, integers, floats (Float, float32/64),
// json.Number, maps with string keys, slices/arrays, structs (via their JSON form) and
// pointers to any of these.
func Encode(v any, opts Options) (string, error) {
	var b strings.Builder
	if err := encode(&b, reflect.ValueOf(v), opts); err != nil {
		return "", err
	}
	return b.String(), nil
}

var (
	numberType    = reflect.TypeFor[json.Number]()
	marshalerType = reflect.TypeFor[json.Marshaler]()
	orderedType   = reflect.TypeFor[OrderedMap]()
)

// OrderedMap is an object that keeps key order (om.Map); without SortKeys its keys are
// written in that order, like a Python dict.
type OrderedMap interface {
	Keys() []string
	Get(key string) (any, bool)
}

func encodeOrdered(b *strings.Builder, m OrderedMap, opts Options) error {
	keys := m.Keys()
	if opts.SortKeys {
		sort.Strings(keys)
	}
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteString(itemSep(opts))
		}
		writeString(b, k, opts.EnsureASCII)
		b.WriteString(keySep(opts))
		val, _ := m.Get(k)
		if err := encode(b, reflect.ValueOf(val), opts); err != nil {
			return err
		}
	}
	b.WriteByte('}')
	return nil
}

func encode(b *strings.Builder, v reflect.Value, opts Options) error {
	if !v.IsValid() {
		b.WriteString("null")
		return nil
	}
	if v.Type() == numberType {
		return encodeNumber(b, v.Interface().(json.Number))
	}
	if v.Kind() == reflect.Pointer && !v.IsNil() && v.Type().Implements(orderedType) {
		return encodeOrdered(b, v.Interface().(OrderedMap), opts)
	}
	if v.Kind() == reflect.Pointer && !v.IsNil() && v.Type().Implements(marshalerType) {
		generic, err := ToGeneric(v.Interface())
		if err != nil {
			return err
		}
		return encode(b, reflect.ValueOf(generic), opts)
	}
	switch v.Kind() {
	case reflect.Interface, reflect.Pointer:
		if v.IsNil() {
			b.WriteString("null")
			return nil
		}
		return encode(b, v.Elem(), opts)
	case reflect.Bool:
		if v.Bool() {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case reflect.String:
		writeString(b, v.String(), opts.EnsureASCII)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		b.WriteString(strconv.FormatInt(v.Int(), 10))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		b.WriteString(strconv.FormatUint(v.Uint(), 10))
	case reflect.Float32, reflect.Float64:
		s, err := FloatRepr(v.Float())
		if err != nil {
			return err
		}
		b.WriteString(s)
	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String {
			return fmt.Errorf("pyjson: map key type %s", v.Type().Key())
		}
		if v.IsNil() {
			b.WriteString("{}")
			return nil
		}
		keys := make([]string, 0, v.Len())
		for _, k := range v.MapKeys() {
			keys = append(keys, k.String())
		}
		if opts.SortKeys {
			sort.Strings(keys) // byte order of UTF-8 == code point order
		}
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteString(itemSep(opts))
			}
			writeString(b, k, opts.EnsureASCII)
			b.WriteString(keySep(opts))
			if err := encode(b, v.MapIndex(reflect.ValueOf(k).Convert(v.Type().Key())), opts); err != nil {
				return err
			}
		}
		b.WriteByte('}')
	case reflect.Slice, reflect.Array:
		if v.Kind() == reflect.Slice && v.IsNil() {
			b.WriteString("[]")
			return nil
		}
		b.WriteByte('[')
		for i := range v.Len() {
			if i > 0 {
				b.WriteString(itemSep(opts))
			}
			if err := encode(b, v.Index(i), opts); err != nil {
				return err
			}
		}
		b.WriteByte(']')
	case reflect.Struct:
		// Structs go through their JSON form, decoded with UseNumber so ints stay ints;
		// float fields must use pyjson.Float to keep their ".0".
		generic, err := ToGeneric(v.Interface())
		if err != nil {
			return err
		}
		return encode(b, reflect.ValueOf(generic), opts)
	default:
		return fmt.Errorf("pyjson: unsupported type %s", v.Type())
	}
	return nil
}

// ToGeneric converts a struct to maps/slices/json.Number via encoding/json.
func ToGeneric(v any) (any, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return DecodeGeneric(raw)
}

// DecodeGeneric decodes JSON keeping numbers as json.Number (ints stay ints).
func DecodeGeneric(raw []byte) (any, error) {
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	var out any
	if err := dec.Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

func itemSep(o Options) string {
	if o.Compact {
		return ","
	}
	return ", "
}

func keySep(o Options) string {
	if o.Compact {
		return ":"
	}
	return ": "
}

// encodeNumber renders a JSON number the way Python would after json.loads: integers
// verbatim, anything with a fraction or exponent as a float.
func encodeNumber(b *strings.Builder, n json.Number) error {
	s := string(n)
	if !strings.ContainsAny(s, ".eE") {
		b.WriteString(strings.TrimPrefix(s, "+"))
		return nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return err
	}
	r, err := FloatRepr(f)
	if err != nil {
		return err
	}
	b.WriteString(r)
	return nil
}

// FloatRepr is Python's repr(float) as json.dumps writes it.
func FloatRepr(f float64) (string, error) {
	switch {
	case math.IsNaN(f):
		return "NaN", nil
	case math.IsInf(f, 1):
		return "Infinity", nil
	case math.IsInf(f, -1):
		return "-Infinity", nil
	}
	if f == 0 {
		if math.Signbit(f) {
			return "-0.0", nil
		}
		return "0.0", nil
	}
	// Shortest round-trip digits, then Python's layout: fixed notation when the decimal
	// exponent is in [-4, 16), scientific otherwise ("1e+16", "1.5e-05").
	sci := strconv.FormatFloat(f, 'e', -1, 64) // d.ddde±XX
	neg := strings.HasPrefix(sci, "-")
	sci = strings.TrimPrefix(sci, "-")
	mant, expStr, _ := strings.Cut(sci, "e")
	exp, err := strconv.Atoi(expStr)
	if err != nil {
		return "", err
	}
	digits := strings.Replace(mant, ".", "", 1)
	var out string
	if exp >= -4 && exp < 16 {
		if exp >= 0 {
			if len(digits) <= exp+1 {
				out = digits + strings.Repeat("0", exp+1-len(digits)) + ".0"
			} else {
				out = digits[:exp+1] + "." + digits[exp+1:]
			}
		} else {
			out = "0." + strings.Repeat("0", -exp-1) + digits
		}
	} else {
		m := digits[:1]
		if len(digits) > 1 {
			m += "." + digits[1:]
		}
		sign := "+"
		if exp < 0 {
			sign = "-"
			exp = -exp
		}
		out = fmt.Sprintf("%se%s%02d", m, sign, exp)
	}
	if neg {
		out = "-" + out
	}
	return out, nil
}

const hexDigits = "0123456789abcdef"

func writeString(b *strings.Builder, s string, ensureASCII bool) {
	b.WriteByte('"')
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		i += size
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		default:
			switch {
			case r < 0x20 || (ensureASCII && r == 0x7f):
				writeU(b, r) // ensure_ascii escapes everything outside ' '..'~'
			case r < 0x80 || !ensureASCII:
				b.WriteRune(r)
			case r > 0xffff:
				r -= 0x10000
				writeU(b, 0xd800|(r>>10)&0x3ff)
				writeU(b, 0xdc00|r&0x3ff)
			default:
				writeU(b, r)
			}
		}
	}
	b.WriteByte('"')
}

func writeU(b *strings.Builder, r rune) {
	b.WriteString(`\u`)
	b.WriteByte(hexDigits[(r>>12)&0xf])
	b.WriteByte(hexDigits[(r>>8)&0xf])
	b.WriteByte(hexDigits[(r>>4)&0xf])
	b.WriteByte(hexDigits[r&0xf])
}

// MarshalJSON keeps the float form ("10.0") so it survives a JSON round trip as a float.
func (f Float) MarshalJSON() ([]byte, error) {
	s, err := FloatRepr(float64(f))
	if err != nil {
		return nil, err
	}
	if strings.ContainsAny(s, "IN") { // Infinity / NaN are not JSON
		return nil, fmt.Errorf("pyjson: %s is not valid JSON", s)
	}
	return []byte(s), nil
}

// UnmarshalJSON accepts any JSON number.
func (f *Float) UnmarshalJSON(raw []byte) error {
	var v float64
	if err := json.Unmarshal(raw, &v); err != nil {
		return err
	}
	*f = Float(v)
	return nil
}
