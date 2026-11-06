package services

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

// A project's setting limits (ProjectPolicies.SettingLimits): a tighter range, fewer choices, a
// pinned value or a hidden setting, per registry key. Saving a pipeline refuses a value outside its
// limits; preflight blocks a deploy of one saved before them; a pin is written into the spec when
// the pipeline deploys and is never read by the compiler, so saving a policy restarts nothing (I2).

// settingAny is the registry setting with key, on any engine (nil: not a setting).
func settingAny(key string) *engine.Setting {
	for _, id := range engine.All {
		if s := engine.SettingOf(id, key); s != nil {
			return s
		}
	}
	return nil
}

// ordinal reads a value of a numeric setting as one number (durations in milliseconds).
func ordinal(s engine.Setting, value string) (float64, bool) {
	switch s.Type {
	case "int":
		n, err := strconv.Atoi(value)
		return float64(n), err == nil
	case "fraction":
		f, err := strconv.ParseFloat(value, 64)
		return f, err == nil
	case "duration":
		d, ok := engine.ParseFlinkDuration(value)
		return float64(d / time.Millisecond), ok
	}
	return 0, false
}

// ValidateSettingLimits checks limits against the registry when Admins save them (P6): each key a
// setting, a range only on a number and inside the setting's own, choices only from its own, a
// pin a value it accepts within the limit, and a hidden setting never pinned.
func ValidateSettingLimits(limits map[string]domain.SettingLimit) error {
	var errs []map[string]any
	keys := make([]string, 0, len(limits))
	for k := range limits {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		l := limits[k]
		bad := func(field, msg string) {
			errs = append(errs, map[string]any{"loc": []string{"setting_limits", k, field}, "message": msg})
		}
		s := settingAny(k)
		if s == nil {
			bad("", "is not a setting BeziSync offers")
			continue
		}
		numeric := s.Type == "int" || s.Type == "fraction" || s.Type == "duration"
		read := func(field, v string) (float64, bool) {
			if v == "" {
				return 0, false
			}
			if !numeric {
				bad(field, "only a number or a duration has a range")
				return 0, false
			}
			n, ok := ordinal(*s, v)
			if !ok || engine.CheckSetting(*s, v) != "" {
				bad(field, fmt.Sprintf("must be a value %s accepts (%s to %s)", s.Title, s.Min, s.Max))
				return 0, false
			}
			return n, true
		}
		min, hasMin := read("min", l.Min)
		max, hasMax := read("max", l.Max)
		if hasMin && hasMax && min > max {
			bad("min", "is above max")
		}
		if l.Choices != nil {
			switch {
			case s.Type != "enum":
				bad("choices", "only a setting with choices can be limited to some")
			case len(l.Choices) == 0:
				bad("choices", "leave at least one choice, or hide the setting")
			default:
				for _, c := range l.Choices {
					if !slices.Contains(s.Choices, c) {
						bad("choices", c+" is not one of "+strings.Join(s.Choices, ", "))
					}
				}
			}
		}
		if l.Pin != "" {
			switch {
			case l.Hide:
				bad("pin", "a hidden setting can't be pinned too")
			case engine.CheckSetting(*s, l.Pin) != "":
				bad("pin", engine.CheckSetting(*s, l.Pin))
			case engine.CanonicalSetting(*s, l.Pin) != l.Pin:
				bad("pin", "write it as "+engine.CanonicalSetting(*s, l.Pin))
			default:
				if why := limitViolation(*s, l, l.Pin); why != "" {
					bad("pin", why)
				}
			}
		}
	}
	if len(errs) > 0 {
		return problem.New(422, "setting_limits", "Some setting limits can't be used.").WithErrors(errs)
	}
	return nil
}

// limitViolation says why value breaks l ("" when it doesn't); pins and hiding aside.
func limitViolation(s engine.Setting, l domain.SettingLimit, value string) string {
	if n, ok := ordinal(s, value); ok {
		if l.Min != "" {
			if min, _ := ordinal(s, l.Min); n < min {
				return "this project allows at least " + l.Min
			}
		}
		if l.Max != "" {
			if max, _ := ordinal(s, l.Max); n > max {
				return "this project allows at most " + l.Max
			}
		}
	}
	if l.Choices != nil && !slices.Contains(l.Choices, value) {
		return "this project allows " + strings.Join(l.Choices, ", ")
	}
	return ""
}

// LimitsFor are the limits that apply to a pipeline on engine id (P10: a limit on another engine's
// setting doesn't apply; P9: a key the registry no longer has is ignored).
func LimitsFor(rules domain.ProjectPolicies, id engine.ID) map[string]domain.SettingLimit {
	out := map[string]domain.SettingLimit{}
	for k, l := range rules.SettingLimits {
		if engine.SettingOf(id, k) != nil {
			out[k] = l
		}
	}
	return out
}

// SettingLimitViolations are the tuning values that break the limits: hidden, outside a range or
// the choices, or other than the pin. With prev, only values new or changed since it count.
func SettingLimitViolations(rules domain.ProjectPolicies, id engine.ID, spec domain.PipelineSpec, prev *domain.PipelineSpec) map[string]string {
	out := map[string]string{}
	for k, l := range LimitsFor(rules, id) {
		value, set := spec.Tuning[k]
		if !set || (prev != nil && prev.Tuning[k] == value) {
			continue
		}
		s := engine.SettingOf(id, k)
		switch {
		case l.Hide:
			out[k] = "this project doesn't let pipelines set it"
		case l.Pin != "" && value != l.Pin:
			out[k] = "this project sets it to " + l.Pin
		default:
			if why := limitViolation(*s, l, value); why != "" {
				out[k] = why
			}
		}
	}
	return out
}

// RefuseSettingLimits is SettingLimitViolations as a 422 for a save.
func RefuseSettingLimits(rules domain.ProjectPolicies, id engine.ID, spec domain.PipelineSpec, prev *domain.PipelineSpec) error {
	v := SettingLimitViolations(rules, id, spec, prev)
	if len(v) == 0 {
		return nil
	}
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	errs := make([]map[string]any, len(keys))
	for i, k := range keys {
		errs[i] = map[string]any{"loc": []string{"tuning", k}, "message": v[k]}
	}
	return problem.New(422, "policy_violation", "Some settings are outside this project's limits.").WithErrors(errs)
}

// settingLimitCheck is preflight's policy_setting_limit: every value, saved before the limits too
// (P8). Pins aren't checked: preflight checks the spec with them, and deploy writes them.
func settingLimitCheck(rules domain.ProjectPolicies, id engine.ID, spec domain.PipelineSpec) *checks.Check {
	v := SettingLimitViolations(rules, id, spec, nil)
	if len(v) == 0 {
		return nil
	}
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + ": " + v[k]
	}
	return &checks.Check{Code: "policy_setting_limit", Title: "Setting limits", Category: checks.CatPolicy, Status: checks.Fail, Blocking: true,
		Message: "Settings outside this project's limits: " + strings.Join(parts, "; ") + ".",
		Fix:     &checks.Fix{Kind: "text", Who: "you", Code: "Change or remove them in Review → Engine settings."}}
}

// Pins are the pinned values a spec doesn't carry yet (key → value), for the settings this
// pipeline can use.
func Pins(rules domain.ProjectPolicies, id engine.ID, sourceType, sinkType string, spec domain.PipelineSpec) map[string]string {
	usable := map[string]bool{}
	for _, s := range engine.SettingsFor(id, sourceType, sinkType) {
		if slices.Contains(engine.TuningScopes(id), s.Scope) {
			usable[s.Key] = true
		}
	}
	out := map[string]string{}
	for k, l := range LimitsFor(rules, id) {
		if l.Pin != "" && usable[k] {
			if _, set := spec.Tuning[k]; !set {
				out[k] = l.Pin
			}
		}
	}
	return out
}

// withPins is spec with the pins written into its tuning (P7).
func withPins(spec domain.PipelineSpec, pins map[string]string) domain.PipelineSpec {
	if len(pins) == 0 {
		return spec
	}
	out := spec.Clone()
	if out.Tuning == nil {
		out.Tuning = map[string]string{}
	}
	for k, v := range pins {
		out.Tuning[k] = v
	}
	return out
}

// LimitImpact is how many pipelines a proposed limit would refuse, with a few of their names.
type LimitImpact struct {
	Key       string   `json:"key"`
	Pipelines int      `json:"pipelines"`
	Names     []string `json:"names"`
}

// SettingLimitImpact counts the project's pipelines whose current version breaks each proposed
// limit, reading them in keyset pages of impactPage (P12), so a large project stays cheap.
func SettingLimitImpact(ctx context.Context, q db.Querier, projectID uuid.UUID, limits map[string]domain.SettingLimit) ([]LimitImpact, error) {
	rules := domain.ProjectPolicies{SettingLimits: limits}
	byKey := map[string]*LimitImpact{}
	after := uuid.Nil
	for {
		rows, err := q.Query(ctx, `SELECT p.id, p.name, p.engine, v.spec FROM pipelines p JOIN pipeline_versions v ON v.id = p.current_version_id
			WHERE p.project_id = $1 AND p.deleted_at IS NULL AND p.id > $2 ORDER BY p.id LIMIT $3`, projectID, after, impactPage)
		if err != nil {
			return nil, err
		}
		n := 0
		for rows.Next() {
			var id uuid.UUID
			var name, eng string
			var raw db.Map
			if err := rows.Scan(&id, &name, &eng, &raw); err != nil {
				rows.Close()
				return nil, err
			}
			n++
			after = id
			for k := range SettingLimitViolations(rules, engine.ID(eng), domain.MustSpec(map[string]any(raw)), nil) {
				li := byKey[k]
				if li == nil {
					li = &LimitImpact{Key: k, Names: []string{}}
					byKey[k] = li
				}
				li.Pipelines++
				if len(li.Names) < 5 {
					li.Names = append(li.Names, name)
				}
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if n < impactPage {
			break
		}
	}
	out := []LimitImpact{}
	for _, li := range byKey {
		sort.Strings(li.Names)
		out = append(out, *li)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// impactPage is how many pipelines SettingLimitImpact reads at a time.
var impactPage = 200

// SetImpactPage changes the impact page size for a test; the returned func restores it.
func SetImpactPage(n int) func() {
	prev := impactPage
	impactPage = n
	return func() { impactPage = prev }
}
