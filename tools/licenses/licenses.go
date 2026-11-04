// Package licenses writes the licence files BeziSync ships: one for the server binary (API,
// controller, agent and CLI: every Go module it links, for every platform a release builds) and
// one for the web UI (every npm package its production dependencies pull in). Each starts with
// BeziSync's own NOTICE and LICENSE, then lists the third-party software with its licence texts
// and NOTICE files, as the Apache License (section 4) and the MIT, BSD and ISC licences ask of
// anyone who redistributes it. A licence outside Allowed fails the run. See cmd/licenses.
package licenses

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
)

// Output files, relative to the repository root.
const (
	ServerFile = "backend/internal/legal/licenses.txt"
	WebFile    = "frontend/public/licenses.txt"
)

// Allowed are the SPDX identifiers BeziSync may ship: permissive licences, MPL-2.0 (file-level
// copyleft: fine for unmodified dependencies) and the fonts' OFL. Anything else (GPL, LGPL, AGPL,
// SSPL, BUSL, licences we cannot recognise) needs a maintainer's decision: an entry in Overrides.
var Allowed = map[string]bool{
	"Apache-2.0": true, "MIT": true, "MIT-0": true, "BSD-2-Clause": true, "BSD-3-Clause": true, "ISC": true,
	"0BSD": true, "MPL-2.0": true, "CC0-1.0": true, "Unlicense": true, "OFL-1.1": true, "Zlib": true,
	"BlueOak-1.0.0": true,
}

// Override records a maintainer's review of a package whose licence files cannot be recognised.
type Override struct {
	SPDX string // the licence the package is under
	Why  string // where it says so
}

// Overrides by Go module path or npm package name.
var Overrides = map[string]Override{}

// File is a licence or notice file of a package.
type File struct {
	Name string
	Text string
}

// Entry is one third-party package.
type Entry struct {
	Name     string   // Go module path or npm package name
	Licenses []string // SPDX identifiers
	Texts    []File   // licence files
	Notices  []File   // NOTICE files: the Apache License asks to pass them on
	Note     string   // e.g. that a package ships no licence file
}

var (
	licenseName = regexp.MustCompile(`(?i)^(licen[cs]e|copying|unlicense)([._-].*)?$`)
	noticeName  = regexp.MustCompile(`(?i)^notice([._-].*)?$`)
	codeExt     = regexp.MustCompile(`(?i)\.(go|js|cjs|mjs|ts|tsx|json|map|html)$`)
)

// ReadFiles returns the licence and notice files at the top of a package directory, by name.
func ReadFiles(dir string) (texts, notices []File, err error) {
	items, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, err
	}
	for _, it := range items {
		name := it.Name()
		if it.IsDir() || codeExt.MatchString(name) {
			continue
		}
		isLicense, isNotice := licenseName.MatchString(name), noticeName.MatchString(name)
		if !isLicense && !isNotice {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, nil, err
		}
		f := File{Name: name, Text: tidy(string(b))}
		if isLicense {
			texts = append(texts, f)
		} else {
			notices = append(notices, f)
		}
	}
	return texts, notices, nil
}

func tidy(s string) string {
	s = strings.ReplaceAll(strings.ReplaceAll(s, "\r\n", "\n"), "\r", "\n")
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		lines[i] = strings.TrimRight(l, " \t")
	}
	return strings.Trim(strings.Join(lines, "\n"), "\n")
}

var space = regexp.MustCompile(`[\s*#>]+`)

func normalize(s string) string {
	s = strings.NewReplacer("‘", "'", "’", "'", "“", `"`, "”", `"`, "`", "'").Replace(strings.ToLower(s))
	return strings.TrimSpace(space.ReplaceAllString(s, " "))
}

// Classify names the licences a licence file contains (several when a file joins them, as some
// do for vendored code). Copyleft licences are recognised by their title at the top, or a GPL
// preamble anywhere, so the MPL's mentions of the GPL do not count.
func Classify(text string) []string {
	t := normalize(text)
	head := t[:min(len(t), 400)]
	has := func(parts ...string) bool {
		for _, p := range parts {
			if !strings.Contains(t, p) {
				return false
			}
		}
		return true
	}
	var ids []string
	add := func(id string) {
		if !slices.Contains(ids, id) {
			ids = append(ids, id)
		}
	}
	mpl := has("mozilla public license") && (has("version 2.0") || has("mozilla public license 2.0") || has("mozilla public license, v. 2.0"))
	switch {
	case mpl: // the MPL names the GPL family as "Secondary Licenses"
	case strings.Contains(head, "gnu affero general public license"):
		add("AGPL")
	case strings.Contains(head, "gnu lesser general public license"), strings.Contains(head, "gnu library general public license"):
		add("LGPL")
	case strings.Contains(head, "gnu general public license"), has("gnu general public license", "preamble"):
		add("GPL")
	}
	if strings.Contains(head, "business source license") {
		add("BUSL-1.1")
	}
	if strings.Contains(head, "server side public license") {
		add("SSPL-1.0")
	}
	if has("apache license", "version 2.0") {
		add("Apache-2.0")
	}
	if mpl {
		add("MPL-2.0")
	}
	if has("permission is hereby granted, free of charge, to any person obtaining a copy") {
		if has("the above copyright notice and this permission notice shall be included") {
			add("MIT")
		} else {
			add("MIT-0")
		}
	}
	if has("permission to use, copy, modify, and/or distribute this software for any purpose with or without fee is hereby granted") ||
		has("permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted") {
		if has("provided that the above copyright notice and this permission notice appear in all copies") {
			add("ISC")
		} else {
			add("0BSD")
		}
	}
	if has("redistribution and use in source and binary forms, with or without modification, are permitted") {
		switch {
		case has("all advertising materials mentioning features"):
			add("BSD-4-Clause")
		case has("neither the name") || has("may be used to endorse or promote products"):
			add("BSD-3-Clause")
		default:
			add("BSD-2-Clause")
		}
	}
	if has("this is free and unencumbered software released into the public domain") {
		add("Unlicense")
	}
	if has("cc0 1.0 universal") || has("creative commons legal code", "cc0") {
		add("CC0-1.0")
	}
	if has("sil open font license") {
		add("OFL-1.1")
	}
	if has("this software is provided 'as-is', without any express or implied warranty", "altered source versions must be plainly marked") {
		add("Zlib")
	}
	if has("blue oak model license") {
		add("BlueOak-1.0.0")
	}
	return ids
}

// AllowedExpression reports whether an npm "license" SPDX expression is allowed: one allowed
// alternative of an OR is enough; every part of an AND must be allowed.
func AllowedExpression(expr string) bool {
	expr = strings.TrimSpace(strings.NewReplacer("(", " ", ")", " ").Replace(expr))
	if expr == "" {
		return false
	}
	for _, alt := range strings.Split(expr, " OR ") {
		ok := true
		for _, part := range strings.Split(alt, " AND ") {
			if !Allowed[strings.TrimSpace(part)] {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

// Vet fails when a package's licence is not allowed or not known, naming every such package.
func Vet(entries []Entry) error {
	var bad []string
	for _, e := range entries {
		if o, ok := Overrides[e.Name]; ok && AllowedExpression(o.SPDX) {
			continue
		}
		if len(e.Licenses) == 0 {
			bad = append(bad, e.Name+": no recognisable licence (add an Override after reviewing it)")
			continue
		}
		for _, id := range e.Licenses {
			if !AllowedExpression(id) {
				bad = append(bad, fmt.Sprintf("%s: %s is not an allowed licence", e.Name, id))
			}
		}
	}
	if len(bad) > 0 {
		return errors.New("licences that need a decision:\n  " + strings.Join(bad, "\n  "))
	}
	return nil
}

func finish(e Entry) Entry {
	if o, ok := Overrides[e.Name]; ok {
		e.Licenses = []string{o.SPDX}
		e.Note = strings.TrimSpace(e.Note + " Licence reviewed by a maintainer: " + o.Why)
		return e
	}
	if len(e.Licenses) == 0 {
		for _, f := range e.Texts {
			for _, id := range Classify(f.Text) {
				if !slices.Contains(e.Licenses, id) {
					e.Licenses = append(e.Licenses, id)
				}
			}
		}
	}
	return e
}

// Render writes a licence file: the product's own NOTICE and LICENSE, an index of the
// third-party packages, then each package's licence texts and notices. A licence text
// identical to an earlier package's is printed once and referred to after that.
func Render(component string, own []File, entries []Entry) string {
	entries = slices.Clone(entries)
	sort.Slice(entries, func(i, j int) bool { return strings.ToLower(entries[i].Name) < strings.ToLower(entries[j].Name) })
	var b strings.Builder
	rule := strings.Repeat("=", 78)
	fmt.Fprintf(&b, "BeziSync %s: licences and notices\n%s\n\n", component, rule)
	b.WriteString("BeziSync is licensed under the Apache License, Version 2.0. Its NOTICE and licence\n" +
		"come first, then the third-party software it includes, each with its licence and notices.\n" +
		"Generated by `make licenses` (tools/cmd/licenses): do not edit by hand.\n\n")
	for _, f := range own {
		fmt.Fprintf(&b, "%s\nBeziSync: %s\n%s\n\n%s\n\n", rule, f.Name, rule, f.Text)
	}
	fmt.Fprintf(&b, "%s\nThird-party software (%d packages)\n%s\n\n", rule, len(entries), rule)
	for _, e := range entries {
		fmt.Fprintf(&b, "- %s (%s)\n", e.Name, licenseLabel(e))
	}
	seen := map[string]string{}
	for _, e := range entries {
		fmt.Fprintf(&b, "\n%s\n%s (%s)\n%s\n", rule, e.Name, licenseLabel(e), rule)
		if e.Note != "" {
			fmt.Fprintf(&b, "\n%s\n", e.Note)
		}
		for _, group := range [][]File{e.Texts, e.Notices} {
			for _, f := range group {
				if first, ok := seen[f.Text]; ok {
					fmt.Fprintf(&b, "\n--- %s: the same text as %s's above.\n", f.Name, first)
					continue
				}
				seen[f.Text] = e.Name
				fmt.Fprintf(&b, "\n--- %s\n\n%s\n", f.Name, f.Text)
			}
		}
	}
	return b.String()
}

func licenseLabel(e Entry) string {
	if len(e.Licenses) == 0 {
		return "unknown"
	}
	return strings.Join(e.Licenses, " AND ")
}

// OwnFiles are BeziSync's NOTICE and LICENSE from the repository root.
func OwnFiles(root string) ([]File, error) {
	var out []File
	for _, name := range []string{"NOTICE", "LICENSE"} {
		b, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			return nil, err
		}
		out = append(out, File{Name: name, Text: tidy(string(b))})
	}
	return out, nil
}

// Platforms the release builds the binary for (scripts/build-cli.sh and the images).
var Platforms = []string{"linux/amd64", "linux/arm64", "darwin/amd64", "darwin/arm64", "windows/amd64", "windows/arm64"}

type goPackage struct {
	Standard bool
	Module   *struct {
		Path    string
		Dir     string
		Main    bool
		Replace *struct{ Path, Dir string }
	}
}

// ParseGoList reads `go list -deps -json` output: the module path → directory of every
// non-main module a package comes from.
func ParseGoList(r io.Reader, into map[string]string) error {
	dec := json.NewDecoder(r)
	for {
		var p goPackage
		if err := dec.Decode(&p); errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return err
		}
		if p.Standard || p.Module == nil || p.Module.Main {
			continue
		}
		dir := p.Module.Dir
		if p.Module.Replace != nil && p.Module.Replace.Dir != "" {
			dir = p.Module.Replace.Dir
		}
		if dir == "" {
			return fmt.Errorf("module %s is not downloaded (run go mod download in backend/)", p.Module.Path)
		}
		into[p.Module.Path] = dir
	}
}

// GoEntries lists the modules ./cmd/bezisync links on any release platform, plus Go's own
// standard library and runtime, which every Go binary contains.
func GoEntries(backendDir string) ([]Entry, error) {
	mods := map[string]string{}
	for _, platform := range Platforms {
		goos, goarch, _ := strings.Cut(platform, "/")
		cmd := exec.Command("go", "list", "-deps", "-json=Standard,Module", "./cmd/bezisync") //nolint:noctx // a local build tool
		cmd.Dir = backendDir
		cmd.Env = append(os.Environ(), "GOOS="+goos, "GOARCH="+goarch, "CGO_ENABLED=0", "GOFLAGS=-mod=readonly")
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		if err != nil {
			return nil, fmt.Errorf("go list for %s: %w: %s", platform, err, stderr.String())
		}
		if err := ParseGoList(bytes.NewReader(out), mods); err != nil {
			return nil, err
		}
	}
	var entries []Entry
	for path, dir := range mods {
		texts, notices, err := ReadFiles(dir)
		if err != nil {
			return nil, err
		}
		e := Entry{Name: path, Texts: texts, Notices: notices}
		if len(texts) == 0 {
			e.Note = "The module ships no licence file."
		}
		entries = append(entries, finish(e))
	}
	goroot, err := exec.Command("go", "env", "GOROOT").Output() //nolint:noctx // a local build tool
	if err != nil {
		return nil, fmt.Errorf("go env GOROOT: %w", err)
	}
	b, err := os.ReadFile(filepath.Join(strings.TrimSpace(string(goroot)), "LICENSE"))
	if err != nil {
		return nil, err
	}
	entries = append(entries, finish(Entry{Name: "Go standard library and runtime (go.dev)", Texts: []File{{Name: "LICENSE", Text: tidy(string(b))}}}))
	return entries, nil
}

type packageJSON struct {
	Name                 string            `json:"name"`
	License              json.RawMessage   `json:"license"`
	Licenses             []json.RawMessage `json:"licenses"`
	Dependencies         map[string]string `json:"dependencies"`
	OptionalDependencies map[string]string `json:"optionalDependencies"`
}

func readPackage(dir string) (packageJSON, error) {
	var p packageJSON
	b, err := os.ReadFile(filepath.Join(dir, "package.json"))
	if err != nil {
		return p, err
	}
	return p, json.Unmarshal(b, &p)
}

// declared is the SPDX expression of an npm "license" field ("MIT", {"type": "MIT"}) or the
// legacy "licenses" list (joined with OR).
func declared(p packageJSON) string {
	one := func(raw json.RawMessage) string {
		var s string
		if json.Unmarshal(raw, &s) == nil {
			return s
		}
		var o struct{ Type string }
		if json.Unmarshal(raw, &o) == nil {
			return o.Type
		}
		return ""
	}
	if len(p.License) > 0 {
		return one(p.License)
	}
	var ids []string
	for _, raw := range p.Licenses {
		if id := one(raw); id != "" {
			ids = append(ids, id)
		}
	}
	return strings.Join(ids, " OR ")
}

// resolve finds an installed package the way Node does: in node_modules of the requiring
// package, then of each directory above it, up to the frontend directory.
func resolve(root, from, name string) (string, bool) {
	for dir := from; ; dir = filepath.Dir(dir) {
		candidate := filepath.Join(dir, "node_modules", filepath.FromSlash(name))
		if st, err := os.Stat(filepath.Join(candidate, "package.json")); err == nil && !st.IsDir() {
			return candidate, true
		}
		if dir == root || !strings.HasPrefix(dir, root) {
			return "", false
		}
	}
}

// NpmEntries lists every package the frontend's production dependencies pull in (its
// devDependencies, the build tools, are not shipped). It needs `bun install` to have run.
func NpmEntries(frontendDir string) ([]Entry, error) {
	root, err := filepath.Abs(frontendDir)
	if err != nil {
		return nil, err
	}
	top, err := readPackage(root)
	if err != nil {
		return nil, err
	}
	byName := map[string]*Entry{}
	visited := map[string]bool{}
	var walk func(from string, deps map[string]string, optional bool) error
	walk = func(from string, deps map[string]string, optional bool) error {
		names := make([]string, 0, len(deps))
		for n := range deps {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, name := range names {
			dir, ok := resolve(root, from, name)
			if !ok {
				if optional {
					continue
				}
				return fmt.Errorf("%s is not installed (run bun install in frontend/)", name)
			}
			if visited[dir] {
				continue
			}
			visited[dir] = true
			p, err := readPackage(dir)
			if err != nil {
				return err
			}
			texts, notices, err := ReadFiles(dir)
			if err != nil {
				return err
			}
			e, ok := byName[name]
			if !ok {
				e = &Entry{Name: name}
				byName[name] = e
			}
			if id := declared(p); id != "" && !slices.Contains(e.Licenses, id) {
				e.Licenses = append(e.Licenses, id)
			}
			for _, f := range texts {
				if !slices.ContainsFunc(e.Texts, func(g File) bool { return g.Text == f.Text }) {
					e.Texts = append(e.Texts, f)
				}
			}
			for _, f := range notices {
				if !slices.ContainsFunc(e.Notices, func(g File) bool { return g.Text == f.Text }) {
					e.Notices = append(e.Notices, f)
				}
			}
			if err := walk(dir, p.Dependencies, false); err != nil {
				return err
			}
			if err := walk(dir, p.OptionalDependencies, true); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(root, top.Dependencies, false); err != nil {
		return nil, err
	}
	if err := walk(root, top.OptionalDependencies, true); err != nil {
		return nil, err
	}
	entries := make([]Entry, 0, len(byName))
	for _, e := range byName {
		if len(e.Texts) == 0 {
			e.Note = "The package ships no licence file; its package.json declares " + licenseLabel(*e) + "."
		}
		entries = append(entries, finish(*e))
	}
	return entries, nil
}
