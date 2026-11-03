package checks

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var (
	literalCode = regexp.MustCompile(`(?:Check\{\s*Code:\s*|\.Passed\()"([a-z0-9_]+)"`)
	networkBase = regexp.MustCompile(`NetworkCheck\(ctx,[^"]*"([a-z0-9][a-z0-9_]*)"`)
)

// Every check code the backend emits has an entry in catalog.json (the generated reference
// `bezisync check-docs` and the UI's "learn more" links come from it).
func TestEveryEmittedCheckIsInTheCatalog(t *testing.T) {
	known := map[string]bool{}
	for _, e := range LoadCatalog().Entries {
		known[e.Code] = true
	}
	missing := map[string]string{}
	err := filepath.WalkDir("..", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		src, err := os.ReadFile(path) //nolint:gosec // our own source tree
		if err != nil {
			return err
		}
		for _, m := range literalCode.FindAllStringSubmatch(string(src), -1) {
			if !known[m[1]] {
				missing[m[1]] = path
			}
		}
		for _, m := range networkBase.FindAllStringSubmatch(string(src), -1) {
			for _, suffix := range []string{"_blocked", "_unreachable", "_reachable"} {
				if !known[m[1]+suffix] {
					missing[m[1]+suffix] = path
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for code, path := range missing {
		t.Errorf("%s (used in %s) has no entry in internal/checks/catalog.json", code, path)
	}
}
