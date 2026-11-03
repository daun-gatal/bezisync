package domain_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/domain"
)

func personal(t *testing.T, spec string, canMask bool) map[string]string {
	t.Helper()
	tables := map[string][]domain.DataColumn{
		"public.users": {
			{Name: "id", Type: "integer"},
			{Name: "email", Type: "text", PII: true, Maskable: canMask},
			{Name: "phone", Type: "character varying", PII: true, Maskable: canMask},
			{Name: "birth_date", Type: "date", PII: true},
		},
		"public.orders": {{Name: "id", Type: "integer"}, {Name: "note", Type: "text", Maskable: canMask}},
	}
	out := map[string]string{}
	for _, c := range domain.PersonalDataChecks(mustSpec(t, spec), tables, "MongoDB", canMask, nil) {
		out[c.Code] = c.Status + ": " + c.Message
		if c.Blocking != (c.Status == "fail") {
			t.Errorf("%s: blocking=%v status=%s", c.Code, c.Blocking, c.Status)
		}
	}
	return out
}

func TestPersonalDataChoicesPerPipeline(t *testing.T) {
	cases := []struct {
		name, spec string
		canMask    bool
		want       map[string]string // code → substring of the check
	}{
		{"nothing chosen warns about every personal-data column", `{"source": {"tables": {"include": ["public.users"]}}}`, true,
			map[string]string{"pii_unprotected": "warn: These columns look like personal data and are sent as they are: public.users.email, public.users.phone, public.users.birth_date."}},
		{"masked, removed and kept columns are all decided", `{"source": {"tables": {"include": ["public.users"]},
			"columns": {"public.users": {"mask": ["email", "phone"], "exclude": ["birth_date"]}}}}`, true,
			map[string]string{"pii_protected": "pass: 2 masked and 1 removed column(s)"}},
		{"keep as is silences the warning", `{"source": {"tables": {"include": ["public.users"]},
			"columns": {"public.users": {"mask": ["email"], "exclude": [], "keep": ["phone", "birth_date"]}}}}`, true,
			map[string]string{"pii_protected": "pass: 1 masked and 0 removed"}},
		{"a date can't be hashed: blocking, never a silent leak", `{"source": {"tables": {"include": ["public.users"]},
			"columns": {"public.users": {"mask": ["email", "phone", "birth_date"]}}}}`, true,
			map[string]string{"pii_mask_unsupported": "fail: These columns can't be masked, so they would reach the destination as they are: public.users.birth_date (date)."}},
		{"a source that can't hash at all", `{"source": {"tables": {"include": ["public.users"]},
			"columns": {"public.users": {"mask": ["email"], "exclude": ["phone", "birth_date"]}}}}`, false,
			map[string]string{"pii_mask_unsupported": "public.users.email (text)"}},
		{"removing wins over masking the same column", `{"source": {"tables": {"include": ["public.users"]},
			"columns": {"public.users": {"mask": ["birth_date", "email", "phone"], "exclude": ["birth_date"]}}}}`, true,
			map[string]string{"pii_protected": "2 masked and 1 removed"}},
		{"any column can be masked, not only detected ones", `{"source": {"tables": {"include": ["public.orders"]},
			"columns": {"public.orders": {"mask": ["note"]}}}}`, true,
			map[string]string{"pii_protected": "1 masked"}},
		{"a rule on a renamed column", `{"source": {"tables": {"include": ["public.orders"]},
			"columns": {"public.orders": {"mask": ["notes"]}}}}`, true,
			map[string]string{"pii_rule_column_missing": "warn: These columns have a masking rule but are not in their table, so the rule does nothing: public.orders.notes."}},
	}
	for _, c := range cases {
		got := personal(t, c.spec, c.canMask)
		if len(got) != len(c.want) {
			t.Errorf("%s: got %v", c.name, got)
			continue
		}
		for code, sub := range c.want {
			if !strings.Contains(got[code], sub) {
				t.Errorf("%s: %s = %q, want …%s…", c.name, code, got[code], sub)
			}
		}
	}
	if got := domain.PersonalDataChecks(mustSpec(t, `{"source": {"tables": {"include": ["public.users"]}}}`), nil, "PostgreSQL", true, errors.New("timeout")); len(got) != 1 || got[0].Code != "pii_unknown" || got[0].Blocking {
		t.Fatal(got)
	}
}

func TestKeepIsDumpedOnlyWhenSet(t *testing.T) {
	plain := mustSpec(t, `{"source": {"tables": {"include": ["public.users"]}, "columns": {"public.users": {"mask": ["email"]}}}}`)
	if raw, _ := plain.Dump().MarshalJSON(); strings.Contains(string(raw), "keep") {
		t.Fatal(string(raw))
	}
	kept := mustSpec(t, `{"source": {"tables": {"include": ["public.users"]}, "columns": {"public.users": {"mask": [], "exclude": [], "keep": ["phone"]}}}}`)
	raw, _ := kept.Dump().MarshalJSON()
	if !strings.Contains(string(raw), `"keep":["phone"]`) {
		t.Fatal(string(raw))
	}
	if again := mustSpec(t, string(raw)); !again.Equal(kept) || again.Clone().Source.Columns["public.users"].Keep[0] != "phone" {
		t.Fatal("keep did not round-trip")
	}
}
