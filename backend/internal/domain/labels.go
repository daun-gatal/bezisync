package domain

import "regexp"

var labelKeyRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// LabelsMessage is the rule pipeline labels follow when they are set or changed.
const LabelsMessage = "Value error, up to 20 labels; keys are slugs, values at most 63 characters"

// LabelsValid says whether labels follow the rule: at most 20, slug keys, values of at most 63
// characters.
func LabelsValid(labels map[string]string) bool {
	if len(labels) > 20 {
		return false
	}
	for k, v := range labels {
		if !labelKeyRe.MatchString(k) || len([]rune(v)) > 63 {
			return false
		}
	}
	return true
}
