package contracts

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestParseIgnoreAndIgnored(t *testing.T) {
	prefixes := ParseIgnore("# header\n\ne2e/\n./backend/internal/testdb\n/deploy/  # charts\n")
	if want := []string{"e2e/", "backend/internal/testdb/", "deploy/"}; !reflect.DeepEqual(prefixes, want) {
		t.Fatalf("ParseIgnore = %v, want %v", prefixes, want)
	}
	for dir, want := range map[string]bool{
		"e2e": true, "e2e/tests": true, "backend/internal/testdb": true,
		"e2e-tools": false, "backend/internal/testdbx": false, "backend": false,
	} {
		if got := Ignored(dir, prefixes); got != want {
			t.Errorf("Ignored(%q) = %v, want %v", dir, got, want)
		}
	}
}

func TestIsSource(t *testing.T) {
	for name, want := range map[string]bool{
		"api.go": true, "Page.tsx": true, "001_init.sql": true, "e2e.sh": true,
		"api_test.go": false, "page.test.tsx": false, "x.spec.ts": false, "vite-env.d.ts": false,
		"README.md": false, "catalog.json": false,
	} {
		if got := IsSource(name); got != want {
			t.Errorf("IsSource(%q) = %v, want %v", name, got, want)
		}
	}
}

func TestLinks(t *testing.T) {
	md := strings.Join([]string{
		"[child](./child/README.md) and [up](../README.md#section)",
		"[site](https://bezisync.dev/docs/) [mail](mailto:a@b.c) [abs](/docs/) [anchor](#here)",
		"```md",
		"[example](./not-checked.md)",
		"```",
	}, "\n")
	if got, want := Links(md), []string{"./child/README.md", "../README.md"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Links = %v, want %v", got, want)
	}
}

func TestCitations(t *testing.T) {
	md := strings.Join([]string{
		"- `[api.go:Server.Handler]` routes requests",
		"- see [../db/db.go] for the pool",
		"- [store.go](./store.go) is a link, not a citation",
		"- [README.md:Overview] is not a citable file",
	}, "\n")
	want := []Citation{{Path: "api.go", Symbol: "Server.Handler"}, {Path: "../db/db.go"}}
	if got := Citations(md); !reflect.DeepEqual(got, want) {
		t.Fatalf("Citations = %+v, want %+v", got, want)
	}
}

func TestContainsIdentifier(t *testing.T) {
	src := "func runPanel() {}\nconst $store = 1"
	for id, want := range map[string]bool{"runPanel": true, "run": false, "Panel": false, "$store": true, "store": false} {
		if got := ContainsIdentifier(src, id); got != want {
			t.Errorf("ContainsIdentifier(%q) = %v, want %v", id, got, want)
		}
	}
}

func TestTestTargets(t *testing.T) {
	md := strings.Join([]string{
		"- Run: `make verify-quick GO_PKGS=\"./internal/api/ ./internal/db/...\"`",
		"- Run: `cd frontend && bun run test src/features/mcp`",
		"- Run: `cd tools && go test ./release/`",
		"- Watch: `cd frontend && bun run test src/lib --reporter dot`",
		"- Full: `cd frontend && bun run test`",
	}, "\n")
	want := []TestTarget{
		{Dir: "backend/internal/api"},
		{Dir: "backend/internal/db", Recursive: true},
		{Dir: "frontend/src/features/mcp", Recursive: true},
		{Dir: "tools/release"},
		{Dir: "frontend/src/lib", Recursive: true},
	}
	if got := TestTargets(md); !reflect.DeepEqual(got, want) {
		t.Fatalf("TestTargets = %+v, want %+v", got, want)
	}
}

// TestCheck builds a small repository and expects exactly the problems seeded into it.
func TestCheck(t *testing.T) {
	root := t.TempDir()
	write := func(rel, content string) {
		t.Helper()
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	good := "# api\n\n## Overview\nx\n\n## Interface\n- [api.go:Handler]\n\n## Invariants\n- None\n\n## Side Effects\n- None\n\n## Verification\n- Run: `cd backend && go test ./internal/api/`\n"
	write("backend/internal/api/api.go", "package api\nfunc Handler() {}\n")
	write("backend/internal/api/api_test.go", "package api\n")
	write("backend/internal/api/README.md", good+"- Single file: `cd backend && go test ./internal/api/api_test.go`\n")
	write("backend/internal/README.md", "# internal\n\n## Overview\nx\n\n## Subdirectories & Boundaries\n- [api](./api/README.md)\n")
	write("backend/README.md", "# backend\n\n## Overview\nx\n\n## Subdirectories & Boundaries\n- [internal](./internal/README.md)\n")
	// A broken contract: missing section, broken link, unknown symbol, a verification target without tests.
	write("backend/internal/db/db.go", "package db\nfunc Open() {}\n")
	write("backend/internal/db/README.md", "# db\n\n## Overview\n[gone](./gone.md)\n\n## Interface\n- [db.go:Close]\n\n## Invariants\n- None\n\n## Verification\n- Run: `cd backend && go test ./internal/db/`\n")
	// New code with no contract, and code the ignore file excludes.
	write("backend/internal/fresh/fresh.go", "package fresh\n")
	write("e2e/helpers.ts", "export const x = 1\n")
	write(IgnoreFile, "e2e/\n")
	write(Architecture, "# Architecture\n\n- [backend](backend/README.md)\n")
	git := exec.Command("git", "init", "-q")
	git.Dir = root
	if out, err := git.CombinedOutput(); err != nil {
		t.Fatalf("git init: %v %s", err, out)
	}

	problems, err := Check(root)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, len(problems))
	for i, p := range problems {
		got[i] = p.String()
	}
	want := []string{
		`backend/internal/db/README.md: missing section "## Side Effects"`,
		`backend/internal/db/README.md: broken link "./gone.md"`,
		`backend/internal/db/README.md: "Close" not found in backend/internal/db/db.go`,
		`backend/internal/db/README.md: verification runs tests in "backend/internal/db", which has none`,
		`backend/internal/fresh/README.md: missing contract (new code directory? see .rules/egm.md)`,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Check problems:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}
