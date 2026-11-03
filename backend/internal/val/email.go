package val

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/net/idna"
	"golang.org/x/text/unicode/norm"
)

// Email validation as Pydantic's EmailStr did it (pydantic.networks.validate_email on top
// of email-validator 2.x with check_deliverability=False), returning the normalized
// address or the error reason.

const (
	maxEmailInput   = 2048
	emailMaxLength  = 254
	domainMaxLength = 253
	dnsLabelLimit   = 63
)

var (
	atext            = `a-zA-Z0-9_!#\$%&'\*\+\-/=\?\^` + "`" + `\{\|\}~`
	dotAtomText      = regexp.MustCompile(`^[` + atext + `]+(?:\.[` + atext + `]+)*$`)
	dotAtomTextIntl  = regexp.MustCompile(`^[` + atext + `\x{80}-\x{10FFFF}]+(?:\.[` + atext + `\x{80}-\x{10FFFF}]+)*$`)
	atextIntlDotRe   = regexp.MustCompile(`^[.` + atext + `\x{80}-\x{10FFFF}]$`)
	atextHostIntl    = regexp.MustCompile(`^[a-zA-Z0-9\-\.\x{80}-\x{10FFFF}]$`)
	hostnameLabel    = `(?:(?:[a-zA-Z0-9][a-zA-Z0-9\-]*)?[a-zA-Z0-9])`
	dotAtomHostname  = regexp.MustCompile(`^` + hostnameLabel + `(?:\.` + hostnameLabel + `)*$`)
	domainNameRegex  = regexp.MustCompile(`[A-Za-z]$`)
	qtextIntl        = regexp.MustCompile(`^[\x{20}-\x{7E}\x{80}-\x{10FFFF}]$`)
	prettyEmailRegex = regexp.MustCompile(`^\s*(?:((?:[\w.!#$%&'*+\-/=?^_` + "`" + `{|}~]+\s+)*[\w.!#$%&'*+\-/=?^_` + "`" + `{|}~]+)|"((?:[^"]|\")+)")?\s*<(.+)>\s*$`)
	specialUseDomain = []string{"arpa", "invalid", "local", "localhost", "onion", "test"}
	caseInsensitive  = map[string]bool{"info": true, "marketing": true, "sales": true, "support": true, "abuse": true, "noc": true,
		"security": true, "postmaster": true, "hostmaster": true, "usenet": true, "news": true, "webmaster": true, "www": true, "uucp": true, "ftp": true}
	uts46 = idna.New(idna.MapForLookup(), idna.Transitional(false), idna.StrictDomainName(false), idna.VerifyDNSLength(false), idna.BidiRule())
)

// Email validates an email address as EmailStr; the error message is Pydantic's.
func (v *V) Email(loc Loc, raw any) (string, bool) {
	s, ok := v.Str(loc, raw)
	if !ok {
		return "", false
	}
	out, reason := ValidateEmail(s)
	if reason != "" {
		v.Fail(loc, "value_error", "value is not a valid email address: "+reason)
		return "", false
	}
	return out, true
}

// ValidateEmail returns the normalized address, or "" and the reason.
func ValidateEmail(value string) (string, string) {
	if utf8.RuneCountInString(value) > maxEmailInput {
		return "", fmt.Sprintf("Length must not exceed %d characters", maxEmailInput)
	}
	if strings.ContainsAny(value, "\r\n") {
		return "", "Carriage return and line feed characters are not allowed"
	}
	if m := prettyEmailRegex.FindStringSubmatch(value); m != nil {
		value = m[3]
	}
	email := strings.TrimFunc(value, unicode.IsSpace)
	return validateEmailCore(email)
}

func safeChar(c rune) string {
	if c == '\\' {
		return `"\"`
	}
	if unicode.In(c, unicode.L, unicode.N, unicode.P, unicode.S) {
		return pyRepr(string(c))
	}
	if names, ok := unicodeNames[c]; ok {
		return names
	}
	if c < 0xFFFF {
		return fmt.Sprintf("U+%04X", c)
	}
	return fmt.Sprintf("U+%08X", c)
}

// Names Python's unicodedata gives the common invisible characters.
var unicodeNames = map[rune]string{
	' ': "SPACE", 0xa0: "NO-BREAK SPACE", 0x200b: "ZERO WIDTH SPACE", 0x2028: "LINE SEPARATOR", 0x2029: "PARAGRAPH SEPARATOR",
	0x3000: "IDEOGRAPHIC SPACE", 0x200e: "LEFT-TO-RIGHT MARK", 0x200f: "RIGHT-TO-LEFT MARK", 0xfeff: "ZERO WIDTH NO-BREAK SPACE",
}

func pyRepr(s string) string {
	if strings.Contains(s, "'") && !strings.Contains(s, `"`) {
		return `"` + s + `"`
	}
	return "'" + strings.ReplaceAll(s, "'", `\'`) + "'"
}

func badList(set map[string]bool) string {
	l := make([]string, 0, len(set))
	for k := range set {
		l = append(l, k)
	}
	sort.Strings(l)
	return strings.Join(l, ", ")
}

func splitEmail(email string) (local, domain string, quoted bool, reason string) {
	inQuote, escaped := false, false
	idx := -1
	for i, c := range email {
		switch {
		case inQuote:
			if c == '\\' && !escaped {
				escaped = true
			} else if c == '"' && !escaped {
				inQuote, escaped = false, false
			} else {
				escaped = false
			}
		case c == '"':
			inQuote = true
		case c == '@' || c == '<':
			idx = i
		}
		if idx >= 0 {
			break
		}
	}
	if idx < 0 {
		switch {
		case strings.ContainsRune(email, '＠'):
			return "", "", false, `The email address has the "full-width" at-sign (@) character instead of a regular at-sign.`
		case strings.ContainsRune(email, '﹫'):
			return "", "", false, `The email address has the "small commercial at" character instead of a regular at-sign.`
		}
		return "", "", false, "An email address must have an @-sign."
	}
	local, domain = email[:idx], strings.TrimPrefix(email[idx:], "@")
	if strings.HasPrefix(email[idx:], "<") {
		return "", "", false, "A display name and angle brackets around the email address are not permitted here."
	}
	// Unquote the local part.
	if strings.HasPrefix(local, `"`) {
		var b strings.Builder
		esc := false
		rs := []rune(local)
		for i := 1; i < len(rs); i++ {
			c := rs[i]
			switch {
			case esc:
				b.WriteRune(c)
				esc = false
			case c == '\\':
				esc = true
			case c == '"':
				if i != len(rs)-1 {
					var extra []string
					for _, e := range rs[i+1:] {
						extra = append(extra, safeChar(e))
					}
					return "", "", false, "Extra character(s) found after close quote: " + strings.Join(extra, ", ")
				}
				return b.String(), domain, true, ""
			default:
				b.WriteRune(c)
			}
		}
		return b.String(), domain, true, ""
	}
	return local, domain, false, ""
}

func checkUnsafe(s string, allowSpace bool) string {
	bad := map[rune]bool{}
	for i, c := range []rune(s) {
		switch {
		case unicode.In(c, unicode.L, unicode.N, unicode.P, unicode.S):
		case unicode.Is(unicode.M, c):
			if i == 0 {
				bad[c] = true
			}
		case unicode.Is(unicode.Zs, c):
			if !allowSpace {
				bad[c] = true
			}
		default:
			bad[c] = true
		}
	}
	if len(bad) == 0 {
		return ""
	}
	rs := make([]rune, 0, len(bad))
	for r := range bad {
		rs = append(rs, r)
	}
	sort.Slice(rs, func(i, j int) bool { return rs[i] < rs[j] })
	parts := make([]string, len(rs))
	for i, r := range rs {
		parts[i] = safeChar(r)
	}
	return "The email address contains unsafe characters: " + strings.Join(parts, ", ") + "."
}

func checkDotAtom(label, startDescr, endDescr string, hostname bool) string {
	switch {
	case strings.HasSuffix(label, "."):
		return fmt.Sprintf(endDescr, "period")
	case strings.HasPrefix(label, "."):
		return fmt.Sprintf(startDescr, "period")
	case strings.Contains(label, ".."):
		return "An email address cannot have two periods in a row."
	}
	if hostname {
		switch {
		case strings.HasSuffix(label, "-"):
			return fmt.Sprintf(endDescr, "hyphen")
		case strings.HasPrefix(label, "-"):
			return fmt.Sprintf(startDescr, "hyphen")
		case strings.Contains(label, ".-") || strings.Contains(label, "-."):
			return "An email address cannot have a period and a hyphen next to each other."
		}
	}
	return ""
}

func validateLocal(local string, quoted bool) (string, *string, bool, string) {
	if local == "" {
		return "", nil, false, "There must be something before the @-sign."
	}
	if dotAtomText.MatchString(local) {
		return local, &local, false, ""
	}
	valid := ""
	smtputf8 := false
	switch {
	case dotAtomTextIntl.MatchString(local):
		valid, smtputf8 = "dot-atom", true
	case quoted:
		bad := map[string]bool{}
		for _, c := range local {
			if !qtextIntl.MatchString(string(c)) {
				bad[safeChar(c)] = true
			}
		}
		if len(bad) > 0 {
			return "", nil, false, "The email address contains invalid characters in quotes before the @-sign: " + badList(bad) + "."
		}
		for _, c := range local {
			if c < 32 || c > 126 {
				smtputf8 = true
			}
		}
		valid = "quoted"
	}
	if valid != "" {
		if r := checkUnsafe(local, valid == "quoted"); r != "" {
			return "", nil, false, r
		}
		if valid == "quoted" {
			local = `"` + regexp.MustCompile(`(["\\])`).ReplaceAllString(local, `\$1`) + `"`
		}
		if smtputf8 {
			return local, nil, true, ""
		}
		return local, &local, false, ""
	}
	bad := map[string]bool{}
	for _, c := range local {
		if !atextIntlDotRe.MatchString(string(c)) {
			bad[safeChar(c)] = true
		}
	}
	if len(bad) > 0 {
		return "", nil, false, "The email address contains invalid characters before the @-sign: " + badList(bad) + "."
	}
	if r := checkDotAtom(local, "An email address cannot start with a %s.", "An email address cannot have a %s immediately before the @-sign.", false); r != "" {
		return "", nil, false, r
	}
	return "", nil, false, "The email address contains invalid characters before the @-sign."
}

func uts46ValidChar(c rune) bool {
	switch {
	case c >= 0x80 && c <= 0x9f:
		return false
	case (c >= 0x2010 && c <= 0x2060 && !(c >= 0x2024 && c <= 0x2026) && !(c >= 0x2028 && c <= 0x202e)) ||
		c == 0xad || c == 0x2064 || c == 0xff0e || (c >= 0x200b && c <= 0x200d) || (c >= 0x1bca0 && c <= 0x1bca3):
		return true
	case unicode.In(c, unicode.Cf, unicode.Co, unicode.Cs, unicode.Zs, unicode.Zl, unicode.Zp) || !unicode.In(c, unicode.L, unicode.M, unicode.N, unicode.P, unicode.S, unicode.Z, unicode.C):
		return false
	}
	return true
}

func validateDomain(domain string) (string, string, string) {
	bad := map[string]bool{}
	for _, c := range domain {
		if !atextHostIntl.MatchString(string(c)) {
			bad[safeChar(c)] = true
		}
	}
	if len(bad) > 0 {
		return "", "", "The part after the @-sign contains invalid characters: " + badList(bad) + "."
	}
	if r := checkUnsafe(domain, false); r != "" {
		return "", "", r
	}
	for _, c := range domain {
		if !uts46ValidChar(c) {
			bad[safeChar(c)] = true
		}
	}
	if len(bad) > 0 {
		return "", "", "The part after the @-sign contains invalid characters: " + badList(bad) + "."
	}
	original := domain
	// UTS-46 remap (lowercasing, NFC, label separators).
	mapped := norm.NFC.String(strings.ToLower(domain))
	for _, sep := range []string{"。", "．", "｡"} {
		mapped = strings.ReplaceAll(mapped, sep, ".")
	}
	domain = mapped
	for _, c := range domain {
		if !atextHostIntl.MatchString(string(c)) {
			bad[safeChar(c)] = true
		}
	}
	if len(bad) > 0 {
		return "", "", "The part after the @-sign contains invalid characters after Unicode normalization: " + badList(bad) + "."
	}
	if r := checkDotAtom(domain, "An email address cannot have a %s immediately after the @-sign.", "An email address cannot end with a %s.", true); r != "" {
		return "", "", r
	}
	for _, label := range strings.Split(domain, ".") {
		if len(label) >= 4 && label[2:4] == "--" && !strings.EqualFold(label[:2], "xn") {
			return "", "", "An email address cannot have two letters followed by two dashes immediately after the @-sign or after a period, except Punycode."
		}
	}
	ascii := domain
	if !dotAtomHostname.MatchString(domain) {
		var labels []string
		for _, label := range strings.Split(domain, ".") {
			a, err := uts46.ToASCII(label)
			if err != nil {
				return "", "", fmt.Sprintf("The part after the @-sign is invalid (%v).", err)
			}
			labels = append(labels, a)
		}
		ascii = strings.Join(labels, ".")
		if !dotAtomHostname.MatchString(ascii) {
			return "", "", "The email address contains invalid characters after the @-sign after IDNA encoding."
		}
	}
	if len(ascii) > domainMaxLength {
		diff := len(ascii) - domainMaxLength
		if ascii == original {
			return "", "", fmt.Sprintf("The email address is too long after the @-sign %s.", lengthReason(diff))
		}
		return "", "", fmt.Sprintf("The email address is too long after the @-sign (%d byte%s too many after IDNA encoding).", diff, plural(diff))
	}
	for _, label := range strings.Split(ascii, ".") {
		if len(label) > dnsLabelLimit {
			return "", "", fmt.Sprintf("After the @-sign, periods cannot be separated by so many characters %s.", lengthReason(len(label)-dnsLabelLimit))
		}
	}
	if !strings.Contains(ascii, ".") {
		return "", "", "The part after the @-sign is not valid. It should have a period."
	}
	if !domainNameRegex.MatchString(ascii) {
		return "", "", "The part after the @-sign is not valid. It is not within a valid top-level domain."
	}
	for _, d := range specialUseDomain {
		if ascii == d || strings.HasSuffix(ascii, "."+d) {
			return "", "", "The part after the @-sign is a special-use or reserved name that cannot be used with email."
		}
	}
	i18n, err := uts46.ToUnicode(ascii)
	if err != nil {
		return "", "", fmt.Sprintf("The part after the @-sign is not valid IDNA (%v).", err)
	}
	return ascii, i18n, ""
}

func lengthReason(diff int) string {
	s := ""
	if diff > 1 {
		s = "s"
	}
	return fmt.Sprintf("(%d character%s too many)", diff, s)
}

func validateEmailCore(email string) (string, string) {
	local, domain, quoted, reason := splitEmail(email)
	if reason != "" {
		return "", reason
	}
	original := local
	if quoted {
		original = `"` + local + `"`
	}
	original += "@" + domain
	localOut, asciiLocal, _, reason := validateLocal(local, quoted)
	if reason != "" {
		return "", reason
	}
	if n := norm.NFC.String(localOut); n != localOut {
		if _, _, _, r := validateLocal(n, quoted); r != "" {
			return "", "After Unicode normalization: " + r
		}
		localOut = n
	}
	if quoted {
		return "", "Quoting the part before the @-sign is not allowed here."
	}
	if asciiLocal != nil && caseInsensitive[strings.ToLower(*asciiLocal)] {
		l := strings.ToLower(*asciiLocal)
		asciiLocal = &l
		localOut = strings.ToLower(localOut)
	}
	if domain == "" {
		return "", "There must be something after the @-sign."
	}
	if strings.HasPrefix(domain, "[") && strings.HasSuffix(domain, "]") {
		return "", "A bracketed IP address after the @-sign is not allowed here."
	}
	asciiDomain, i18n, reason := validateDomain(domain)
	if reason != "" {
		return "", reason
	}
	normalized := localOut + "@" + i18n
	asciiForm := localOut
	if asciiLocal != nil {
		asciiForm = *asciiLocal
	}
	for _, c := range []struct{ addr, why string }{
		{original, ""}, {normalized, "after normalization"}, {asciiForm + "@" + asciiDomain, "when the part after the @-sign is converted to IDNA ASCII"},
	} {
		n := len(c.addr)
		diff := n - emailMaxLength
		if diff <= 0 {
			continue
		}
		chars := utf8.RuneCountInString(c.addr)
		switch {
		case c.why == "" && chars == n:
			return "", "The email address is too long " + lengthReason(diff) + "."
		case c.why == "":
			mbpc := 1
			for _, r := range c.addr {
				mbpc = max(mbpc, utf8.RuneLen(r))
			}
			mchars := max(1, diff/mbpc)
			s := plural(diff)
			if mchars == diff {
				return "", fmt.Sprintf("The email address is too long (%d character%s too many).", diff, s)
			}
			return "", fmt.Sprintf("The email address is too long (%d-%d character%s too many).", mchars, diff, s)
		default:
			return "", fmt.Sprintf("The email address is too long %s (%d byte%s too many).", c.why, diff, plural(diff))
		}
	}
	return normalized, ""
}
