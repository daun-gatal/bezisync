// Command licenses writes the licence files BeziSync ships, from the repository root:
//
//	go run ./cmd/licenses                      write backend/internal/legal/licenses.txt (embedded in
//	                                           the binary) and frontend/public/licenses.txt (served
//	                                           by the web image at /licenses.txt)
//	go run ./cmd/licenses --check              fail when either is stale
//	go run ./cmd/licenses --only server|web    just one of them (CI checks each in its own job)
//
// Every run fails when a dependency's licence is not allowed (see licenses.Allowed). The web
// file needs `bun install` in frontend/; the server file needs the Go modules downloaded.
// Run from tools/ (or set BEZISYNC_ROOT).
package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/daun-gatal/bezisync/tools/licenses"
)

func main() {
	check := flag.Bool("check", false, "fail when a file is stale instead of writing it")
	only := flag.String("only", "", "server or web (default both)")
	flag.Parse()
	if err := run(*check, *only); err != nil {
		fmt.Fprintln(os.Stderr, "licenses:", err)
		os.Exit(1)
	}
}

func repoRoot() (string, error) {
	if root := os.Getenv("BEZISYNC_ROOT"); root != "" {
		return root, nil
	}
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output() //nolint:noctx // a quick local command
	if err != nil {
		return "", fmt.Errorf("not in the repository (set BEZISYNC_ROOT): %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

func run(check bool, only string) error {
	if only != "" && only != "server" && only != "web" {
		return fmt.Errorf("--only is server or web, not %q", only)
	}
	root, err := repoRoot()
	if err != nil {
		return err
	}
	own, err := licenses.OwnFiles(root)
	if err != nil {
		return err
	}
	type target struct {
		name, component, file string
		list                  func() ([]licenses.Entry, error)
	}
	targets := []target{
		{"server", "server (API, controller, agent and CLI)", licenses.ServerFile, func() ([]licenses.Entry, error) {
			return licenses.GoEntries(filepath.Join(root, "backend"))
		}},
		{"web", "web UI", licenses.WebFile, func() ([]licenses.Entry, error) {
			return licenses.NpmEntries(filepath.Join(root, "frontend"))
		}},
	}
	for _, t := range targets {
		if only != "" && only != t.name {
			continue
		}
		entries, err := t.list()
		if err != nil {
			return fmt.Errorf("%s: %w", t.name, err)
		}
		if err := licenses.Vet(entries); err != nil {
			return fmt.Errorf("%s: %w", t.name, err)
		}
		content := licenses.Render(t.component, own, entries)
		path := filepath.Join(root, t.file)
		if check {
			current, err := os.ReadFile(path)
			if err != nil || string(current) != content {
				return fmt.Errorf("%s is stale: run make licenses", t.file)
			}
			fmt.Printf("%s is current (%d packages)\n", t.file, len(entries))
			continue
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			return err
		}
		fmt.Printf("wrote %s (%d packages)\n", t.file, len(entries))
	}
	return nil
}
