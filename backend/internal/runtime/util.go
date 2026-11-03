package runtime

import (
	"sort"
	"strings"
	"unicode/utf8"
)

func sortStrings(s []string) { sort.Strings(s) }

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// decodeReplace is bytes.decode(errors="replace").
func decodeReplace(b []byte) string {
	s := string(b)
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "�")
	}
	return s
}
