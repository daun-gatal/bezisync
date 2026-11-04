package licenses

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const mitText = `MIT License

Copyright (c) 2020 Someone

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.`

func TestClassify(t *testing.T) {
	cases := []struct {
		name, text string
		want       []string
	}{
		{"mit", mitText, []string{"MIT"}},
		{"mit-0", "MIT No Attribution\n\nPermission is hereby granted, free of charge, to any person obtaining a copy of this software, to deal in the Software without restriction.", []string{"MIT-0"}},
		{"isc with and/or", "ISC License\n\nPermission to use, copy, modify, and/or distribute this software for any\npurpose with or without fee is hereby granted, provided that the above\ncopyright notice and this permission notice appear in all copies.", []string{"ISC"}},
		{"0bsd", "Permission to use, copy, modify, and/or distribute this software for any purpose with or without fee is hereby granted.\n\nTHE SOFTWARE IS PROVIDED \"AS IS\".", []string{"0BSD"}},
		{"apache", "                                 Apache License\n                           Version 2.0, January 2004", []string{"Apache-2.0"}},
		{"bsd-3", "Redistribution and use in source and binary forms, with or without\nmodification, are permitted provided that ...\n* Neither the name of Google Inc. nor the names of its contributors may be used", []string{"BSD-3-Clause"}},
		{"bsd-2", "Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:", []string{"BSD-2-Clause"}},
		{"bsd-4", "Redistribution and use in source and binary forms, with or without modification, are permitted. All advertising materials mentioning features", []string{"BSD-4-Clause"}},
		{"mpl mentions gpl", "Mozilla Public License Version 2.0\n\n1.12. \"Secondary License\" means either the GNU General Public License, Version 2.0, the GNU Lesser General Public License, Version 2.1", []string{"MPL-2.0"}},
		{"gpl", "GNU GENERAL PUBLIC LICENSE\nVersion 3, 29 June 2007\n\nPreamble", []string{"GPL"}},
		{"lgpl", "GNU LESSER GENERAL PUBLIC LICENSE\nVersion 3, 29 June 2007", []string{"LGPL"}},
		{"agpl", "GNU AFFERO GENERAL PUBLIC LICENSE\nVersion 3", []string{"AGPL"}},
		{"busl", "Business Source License 1.1\n\nParameters", []string{"BUSL-1.1"}},
		{"joined", "Apache License\nVersion 2.0\n\n---\n\n" + mitText, []string{"Apache-2.0", "MIT"}},
		{"ofl", "SIL OPEN FONT LICENSE Version 1.1", []string{"OFL-1.1"}},
		{"unlicense", "This is free and unencumbered software released into the public domain.", []string{"Unlicense"}},
		{"unknown", "All rights reserved.", nil},
	}
	for _, c := range cases {
		if got := Classify(c.text); !slices.Equal(got, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

func TestAllowedExpression(t *testing.T) {
	for expr, want := range map[string]bool{
		"MIT": true, "(MIT OR GPL-3.0)": true, "GPL-3.0 OR Apache-2.0": true, "MIT AND Apache-2.0": true,
		"MIT AND GPL-3.0": false, "GPL": false, "LGPL": false, "BUSL-1.1": false, "": false, "SEE LICENSE IN LICENSE": false,
	} {
		if got := AllowedExpression(expr); got != want {
			t.Errorf("%q: got %v, want %v", expr, got, want)
		}
	}
}

func TestVetNamesEveryPackageThatNeedsADecision(t *testing.T) {
	err := Vet([]Entry{
		{Name: "ok", Licenses: []string{"MIT"}},
		{Name: "copyleft", Licenses: []string{"MIT", "GPL"}},
		{Name: "mystery"},
	})
	if err == nil {
		t.Fatal("want an error")
	}
	for _, want := range []string{"copyleft: GPL is not an allowed licence", "mystery: no recognisable licence"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error lacks %q: %v", want, err)
		}
	}
	if strings.Contains(err.Error(), "ok:") {
		t.Error("an allowed package was reported")
	}
	Overrides["mystery"] = Override{SPDX: "MIT", Why: "test"}
	defer delete(Overrides, "mystery")
	if err := Vet([]Entry{{Name: "mystery"}}); err != nil {
		t.Errorf("an override must settle it: %v", err)
	}
}

func TestRenderPutsOwnLicenceFirstAndPrintsSharedTextsOnce(t *testing.T) {
	own := []File{{Name: "NOTICE", Text: "BeziSync notice"}, {Name: "LICENSE", Text: "Apache text"}}
	out := Render("web UI", own, []Entry{
		{Name: "zeta", Licenses: []string{"MIT"}, Texts: []File{{Name: "LICENSE", Text: "shared"}}},
		{Name: "Alpha", Licenses: []string{"MIT"}, Texts: []File{{Name: "LICENSE", Text: "shared"}}, Notices: []File{{Name: "NOTICE", Text: "alpha notice"}}},
	})
	order := []string{"BeziSync: NOTICE", "BeziSync: LICENSE", "Third-party software (2 packages)", "- Alpha (MIT)", "- zeta (MIT)", "--- NOTICE\n\nalpha notice"}
	last := -1
	for _, s := range order {
		i := strings.Index(out, s)
		if i <= last {
			t.Fatalf("%q missing or out of order in:\n%s", s, out)
		}
		last = i
	}
	if strings.Count(out, "\nshared\n") != 1 || !strings.Contains(out, "--- LICENSE: the same text as Alpha's above.") {
		t.Errorf("a shared text must print once:\n%s", out)
	}
}

func write(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestNpmEntriesFollowProductionDependenciesOnly(t *testing.T) {
	root := t.TempDir()
	nm := filepath.Join(root, "node_modules")
	write(t, filepath.Join(root, "package.json"), `{"name":"app","dependencies":{"a":"1","@s/b":"1"},"devDependencies":{"tool":"1"},"optionalDependencies":{"gone":"1"}}`)
	write(t, filepath.Join(nm, "a/package.json"), `{"name":"a","license":"MIT","dependencies":{"c":"2"}}`)
	write(t, filepath.Join(nm, "a/LICENSE"), mitText)
	write(t, filepath.Join(nm, "a/license.js"), "not a licence")
	// a needs c@2, nested under it; the hoisted c@1 belongs to @s/b.
	write(t, filepath.Join(nm, "a/node_modules/c/package.json"), `{"name":"c","license":{"type":"ISC"}}`)
	write(t, filepath.Join(nm, "@s/b/package.json"), `{"name":"@s/b","licenses":[{"type":"MIT"},{"type":"Apache-2.0"}],"dependencies":{"c":"1"}}`)
	write(t, filepath.Join(nm, "@s/b/NOTICE"), "b notice")
	write(t, filepath.Join(nm, "c/package.json"), `{"name":"c","license":"ISC"}`)
	write(t, filepath.Join(nm, "tool/package.json"), `{"name":"tool","license":"GPL-3.0"}`)

	entries, err := NpmEntries(root)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]Entry{}
	for _, e := range entries {
		got[e.Name] = e
	}
	if len(got) != 3 || got["tool"].Name != "" {
		t.Fatalf("want a, @s/b and c only (no devDependencies), got %v", entries)
	}
	if a := got["a"]; len(a.Texts) != 1 || a.Texts[0].Name != "LICENSE" || a.Note != "" {
		t.Errorf("a: %+v", a)
	}
	if b := got["@s/b"]; !slices.Equal(b.Licenses, []string{"MIT OR Apache-2.0"}) || len(b.Notices) != 1 || !strings.Contains(b.Note, "no licence file") {
		t.Errorf("@s/b: %+v", b)
	}
	if c := got["c"]; !slices.Equal(c.Licenses, []string{"ISC"}) {
		t.Errorf("c, from both places, must be one entry: %+v", c)
	}
	if err := Vet(entries); err != nil {
		t.Error(err)
	}

	write(t, filepath.Join(root, "package.json"), `{"name":"app","dependencies":{"missing":"1"}}`)
	if _, err := NpmEntries(root); err == nil || !strings.Contains(err.Error(), "missing is not installed") {
		t.Errorf("a missing dependency must fail: %v", err)
	}
}

func TestParseGoListKeepsNonMainModules(t *testing.T) {
	in := `{"Standard": true}
{"Module": {"Path": "example.com/app", "Main": true, "Dir": "/src"}}
{"Module": {"Path": "example.com/dep", "Dir": "/cache/dep@v1"}}
{"Module": {"Path": "example.com/forked", "Dir": "/cache/forked@v1", "Replace": {"Path": "../fork", "Dir": "/src/fork"}}}
`
	mods := map[string]string{}
	if err := ParseGoList(strings.NewReader(in), mods); err != nil {
		t.Fatal(err)
	}
	if len(mods) != 2 || mods["example.com/dep"] != "/cache/dep@v1" || mods["example.com/forked"] != "/src/fork" {
		t.Errorf("got %v", mods)
	}
	if err := ParseGoList(strings.NewReader(`{"Module": {"Path": "example.com/dep"}}`), map[string]string{}); err == nil {
		t.Error("a module that is not downloaded must fail")
	}
}
