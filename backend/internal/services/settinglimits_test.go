package services

import (
	"errors"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

func limitErrors(t *testing.T, err error) map[string]string {
	t.Helper()
	out := map[string]string{}
	if err == nil {
		return out
	}
	var p *problem.Problem
	if !errors.As(err, &p) {
		t.Fatal(err)
	}
	for _, e := range p.Errors {
		loc := e["loc"].([]string)
		out[strings.Join(loc[1:], ".")] = e["message"].(string)
	}
	return out
}

func TestValidateSettingLimits(t *testing.T) {
	ok := map[string]domain.SettingLimit{
		"debezium.source.snapshot.max.threads":         {Max: "4"},
		"quarkus.log.level":                            {Choices: []string{"WARN", "ERROR"}},
		"debezium.source.poll.interval.ms":             {Pin: "500"},
		"heartbeat.timeout":                            {Min: "30 s", Max: "2 min"},
		"debezium.source.skip.messages.without.change": {Hide: true},
	}
	if err := ValidateSettingLimits(ok); err != nil {
		t.Fatal(err)
	}
	// P6: a cap outside the setting's own bounds, a pin it refuses, and the rest.
	errs := limitErrors(t, ValidateSettingLimits(map[string]domain.SettingLimit{
		"debezium.source.snapshot.max.threads":         {Max: "0"},
		"debezium.source.poll.interval.ms":             {Pin: "5", Min: "100"},
		"quarkus.log.level":                            {Choices: []string{"DEBUG"}},
		"debezium.source.max.queue.size":               {Min: "8192", Max: "4096"},
		"debezium.source.skip.messages.without.change": {Hide: true, Pin: "true", Max: "1"},
		"made.up.key":                                  {Max: "1"},
	}))
	for k, want := range map[string]string{
		"debezium.source.snapshot.max.threads.max":         "1 to 16",
		"debezium.source.poll.interval.ms.pin":             "from 50 to 10000",
		"quarkus.log.level.choices":                        "DEBUG is not one of",
		"debezium.source.max.queue.size.min":               "above max",
		"debezium.source.skip.messages.without.change.pin": "can't be pinned",
		"debezium.source.skip.messages.without.change.max": "only a number",
		"made.up.key.": "not a setting",
	} {
		if !strings.Contains(errs[k], want) {
			t.Errorf("%s: %q", k, errs[k])
		}
	}
	// A pin outside the project's own range.
	if errs := limitErrors(t, ValidateSettingLimits(map[string]domain.SettingLimit{"debezium.source.poll.interval.ms": {Max: "200", Pin: "500"}})); !strings.Contains(errs["debezium.source.poll.interval.ms.pin"], "at most 200") {
		t.Fatal(errs)
	}
}

func TestSettingLimitViolationsAndPins(t *testing.T) {
	rules := domain.ProjectPolicies{SettingLimits: map[string]domain.SettingLimit{
		"debezium.source.snapshot.max.threads":         {Max: "4"},
		"debezium.source.skip.messages.without.change": {Hide: true},
		"debezium.source.poll.interval.ms":             {Pin: "500"},
		"heartbeat.timeout":                            {Max: "2 min"}, // a Flink setting
		"gone.from.the.registry":                       {Max: "1"},
	}}
	spec := domain.DefaultSpec()
	spec.Tuning = map[string]string{"debezium.source.snapshot.max.threads": "8", "debezium.source.skip.messages.without.change": "true"}
	v := SettingLimitViolations(rules, engine.Debezium, spec, nil)
	if !strings.Contains(v["debezium.source.snapshot.max.threads"], "at most 4") || !strings.Contains(v["debezium.source.skip.messages.without.change"], "doesn't let") || len(v) != 2 {
		t.Fatal(v)
	}
	// Only new or changed values count when saved.
	if v := SettingLimitViolations(rules, engine.Debezium, spec, &spec); len(v) != 0 {
		t.Fatal(v)
	}
	// P10: the Flink limit doesn't apply to Debezium; P9: an obsolete key is ignored.
	if _, ok := LimitsFor(rules, engine.Debezium)["heartbeat.timeout"]; ok {
		t.Fatal("wrong engine")
	}
	if _, ok := LimitsFor(rules, engine.FlinkCDC)["heartbeat.timeout"]; !ok {
		t.Fatal("its engine")
	}
	// A pin: written when the spec omits it; a different value is a violation.
	pins := Pins(rules, engine.Debezium, "postgresql", "kafka", spec)
	if pins["debezium.source.poll.interval.ms"] != "500" || len(pins) != 1 {
		t.Fatal(pins)
	}
	if w := withPins(spec, pins); w.Tuning["debezium.source.poll.interval.ms"] != "500" || spec.Tuning["debezium.source.poll.interval.ms"] != "" {
		t.Fatal("pins go into a copy")
	}
	spec.Tuning["debezium.source.poll.interval.ms"] = "250"
	if v := SettingLimitViolations(rules, engine.Debezium, spec, nil); !strings.Contains(v["debezium.source.poll.interval.ms"], "sets it to 500") {
		t.Fatal(v)
	}
	if c := settingLimitCheck(rules, engine.Debezium, spec); c == nil || !c.Blocking || !strings.Contains(c.Message, "debezium.source.snapshot.max.threads: this project allows at most 4") {
		t.Fatal(c)
	}
}
