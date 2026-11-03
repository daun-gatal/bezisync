package api_test

import (
	"strings"
	"testing"
)

// checkOf finds a check by code in a preflight report (nil when absent).
func checkOf(t *testing.T, report *resp, code string) any {
	t.Helper()
	return checkIn(t, report.JSON(), code)
}

func checkIn(t *testing.T, report any, code string) any {
	t.Helper()
	for _, s := range at(t, report, "sections").([]any) {
		for _, c := range at(t, s, "report", "checks").([]any) {
			if at(t, c, "code") == code {
				return c
			}
		}
	}
	return nil
}

func TestPersonalDataIsChosenPerPipeline(t *testing.T) {
	e := newPipelineEnv(t)
	exec(t, `CREATE TABLE shop.people (id serial primary key, email text, birth_date date, note text)`)
	exec(t, `GRANT SELECT ON shop.people TO PUBLIC`)
	pipe := createPipeline(t, e.admin, e.pid, e.ids, "shop.people")
	base := "/api/v1/projects/" + e.pid + "/pipelines/" + pipe.Str("id")
	spec := func(columns map[string]any) {
		cur := e.admin.get(base)
		e.admin.patch(base, map[string]any{"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.people"}},
			"columns": map[string]any{"shop.people": columns}}}}, ifMatch(cur.At("version"))...).expect(200)
	}

	// Nothing chosen (an API or YAML pipeline): a warning names the columns, the deploy may go on.
	pre := e.admin.post(base+"/preflight", nil).expect(200)
	unprotected := checkOf(t, pre, "pii_unprotected")
	if unprotected == nil || at(t, unprotected, "status") != "warn" || at(t, unprotected, "blocking") != false ||
		!strings.Contains(at(t, unprotected, "message").(string), "shop.people.email, shop.people.birth_date") {
		t.Fatal(string(pre.Raw))
	}
	if pre.At("ok") != true {
		t.Fatal("an unprotected column must not block:", string(pre.Raw))
	}

	// A date can't be hashed: masking it would leak, so preflight blocks the deploy.
	spec(map[string]any{"mask": []string{"email", "birth_date"}, "exclude": []string{}})
	if res := e.admin.post(base+"/deploy", map[string]any{}).expect(409); checkIn(t, res.At("preflight"), "pii_mask_unsupported") == nil {
		t.Fatal(string(res.Raw))
	}

	// Masked, removed and kept on purpose: protected, and the connector gets the masking.
	spec(map[string]any{"mask": []string{"email", "note"}, "exclude": []string{"birth_date"}})
	pre = e.admin.post(base+"/preflight", nil).expect(200)
	if c := checkOf(t, pre, "pii_protected"); c == nil || !strings.Contains(at(t, c, "message").(string), "2 masked and 1 removed") {
		t.Fatal(string(pre.Raw))
	}
	props := pre.Str("properties")
	if !strings.Contains(props, `column.mask.hash.v2.SHA-256.with.salt`) || !strings.Contains(props, `shop\\.people\\.birth_date`) {
		t.Fatal(props)
	}
	spec(map[string]any{"mask": []string{"email"}, "exclude": []string{}, "keep": []string{"birth_date"}})
	if got := e.admin.get(base).At("spec", "source", "columns", "shop.people", "keep"); got == nil || got.([]any)[0] != "birth_date" {
		t.Fatal(got)
	}
	if pre = e.admin.post(base+"/preflight", nil).expect(200); checkOf(t, pre, "pii_unprotected") != nil || checkOf(t, pre, "pii_protected") == nil {
		t.Fatal(string(pre.Raw))
	}

	// A new column that looks like personal data is flagged in the schema changes until chosen.
	exec(t, `INSERT INTO schema_changes (id, pipeline_id, "table", kind, "column", breaking, detected_at)
		VALUES (gen_random_uuid(), '`+pipe.Str("id")+`', 'shop.people', 'column_added', 'phone_number', false, now()),
		       (gen_random_uuid(), '`+pipe.Str("id")+`', 'shop.people', 'column_added', 'nickname', false, now())`)
	flagged := map[string]bool{}
	for _, c := range e.admin.get(base + "/schema").List("changes") {
		flagged[at(t, c, "column").(string)] = at(t, c, "personal_data").(bool)
	}
	if !flagged["phone_number"] || flagged["nickname"] {
		t.Fatal(flagged)
	}
	spec(map[string]any{"mask": []string{"email", "phone_number"}, "exclude": []string{}, "keep": []string{"birth_date"}})
	for _, c := range e.admin.get(base + "/schema").List("changes") {
		if at(t, c, "personal_data") == true {
			t.Fatal("a chosen column is still flagged")
		}
	}
}

func TestTheRetiredMaskingPolicyIsIgnored(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	base := "/api/v1/projects/" + defaultProject(t, admin)
	saved := admin.put(base+"/policies", map[string]any{"require_pii_masking": true, "require_approval": false}).expect(200)
	if strings.Contains(string(saved.Raw), "require_pii_masking") {
		t.Fatal(string(saved.Raw))
	}
	admin.put(base+"/policies", map[string]any{"require_pii_masking": "maybe"}).expect(422)
}
