// Package contracts checks the directory contracts: the README.md in every code directory and
// the root ARCHITECTURE.md (format and rules: .rules/egm.md). A contract must exist for every
// code directory outside .egmignore, carry its sections, link only to files that exist, cite
// only symbols that exist ([path:Symbol]), and name verification commands whose targets hold
// tests. See cmd/contracts.
package contracts

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
)

// Contract and blueprint file names, relative to a directory or the repository root.
const (
	Contract     = "README.md"
	Architecture = "ARCHITECTURE.md"
	IgnoreFile   = ".egmignore"
)

// Problem is one reason the check fails.
type Problem struct {
	File    string // repository-relative path of the contract
	Message string
}

func (p Problem) String() string { return p.File + ": " + p.Message }

var (
	sourceExtensions = map[string]bool{
		".go": true, ".ts": true, ".tsx": true, ".js": true, ".jsx": true, ".mjs": true, ".cjs": true,
		".sh": true, ".bash": true, ".sql": true, ".proto": true,
	}
	citable   = regexp.MustCompile(`\.(?:go|tsx?|jsx?|mjs|cjs|sh|sql|ya?ml|json)$`)
	testFile  = regexp.MustCompile(`(?:_test\.go$|\.test\.|\.spec\.)`)
	testDirs  = map[string]bool{"__tests__": true, "testdata": true, "test": true, "tests": true, "fixtures": true, "unittests": true}
	skipDirs  = map[string]bool{"node_modules": true, "dist": true, "build": true, "coverage": true}
	fence     = regexp.MustCompile("(?s)```.*?```")
	linkRe    = regexp.MustCompile(`\]\(([^)\s]+)\)`)
	schemeRe  = regexp.MustCompile(`^(?i:[a-z][a-z0-9+.-]*:|/)`)
	citeRe    = regexp.MustCompile(`\[([\w@./-]+\.[A-Za-z]+)(?::([A-Za-z_$][\w$.]*))?\]`)
	goPkgsRe  = regexp.MustCompile(`GO_PKGS="([^"]+)"`)
	cdRunRe   = regexp.MustCompile(`cd ([\w./-]+) && (?:bun run test|npx vitest run|go test)((?:\s+[\w./-]+)+)`)
	overview  = regexp.MustCompile(`^## (?:System )?Overview\b`)
	component = []section{
		{"## Overview", overview},
		{"## Interface", regexp.MustCompile(`^## Interface\b`)},
		{"## Invariants", regexp.MustCompile(`^## Invariants\b`)},
		{"## Side Effects", regexp.MustCompile(`^## Side Effects\b`)},
		{"## Verification", regexp.MustCompile(`^## Verification\b`)},
	}
	blueprint = []section{
		{"## Overview", overview},
		{"## Subdirectories", regexp.MustCompile(`^## Subdirectories\b`)},
	}
)

type section struct {
	heading string
	pattern *regexp.Regexp
}

// ParseIgnore reads .egmignore: one repository-relative directory prefix per line, # comments.
func ParseIgnore(text string) []string {
	var prefixes []string
	for _, line := range strings.Split(text, "\n") {
		if i := strings.Index(line, "#"); i >= 0 {
			line = line[:i]
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		line = strings.TrimPrefix(strings.TrimPrefix(line, "./"), "/")
		prefixes = append(prefixes, strings.TrimSuffix(line, "/")+"/")
	}
	return prefixes
}

// Ignored reports whether dir (repository-relative, "/"-separated) lies under an ignored prefix.
func Ignored(dir string, prefixes []string) bool {
	withSlash := dir + "/"
	for _, p := range prefixes {
		if strings.HasPrefix(withSlash, p) {
			return true
		}
	}
	return false
}

// IsSource reports whether a file name is non-test source code.
func IsSource(name string) bool {
	return sourceExtensions[path.Ext(name)] && !testFile.MatchString(name) && !strings.HasSuffix(name, ".d.ts")
}

// Links returns the relative link targets of a markdown text, without anchors, outside code fences.
func Links(markdown string) []string {
	var links []string
	for _, m := range linkRe.FindAllStringSubmatch(fence.ReplaceAllString(markdown, ""), -1) {
		target := strings.SplitN(strings.SplitN(m[1], "#", 2)[0], "?", 2)[0]
		if target == "" || schemeRe.MatchString(target) {
			continue
		}
		links = append(links, target)
	}
	return links
}

// Citation is a [path:Symbol] or bare [path] reference to code.
type Citation struct {
	Path   string
	Symbol string // empty for a bare file citation
}

// Citations returns the code citations of a markdown text; markdown link text ([x](y)) is not one.
func Citations(markdown string) []Citation {
	var out []Citation
	for _, idx := range citeRe.FindAllStringSubmatchIndex(markdown, -1) {
		if idx[1] < len(markdown) && markdown[idx[1]] == '(' {
			continue
		}
		p := markdown[idx[2]:idx[3]]
		if !citable.MatchString(p) {
			continue
		}
		c := Citation{Path: p}
		if idx[4] >= 0 {
			c.Symbol = markdown[idx[4]:idx[5]]
		}
		out = append(out, c)
	}
	return out
}

// ContainsIdentifier reports whether text holds identifier as a whole word (so `run` does not
// match inside `runPanel`).
func ContainsIdentifier(text, identifier string) bool {
	re := regexp.MustCompile(`(^|[^\w$])` + regexp.QuoteMeta(identifier) + `($|[^\w$])`)
	return re.MatchString(text)
}

// TestTarget is a path a verification command runs tests in, relative to the repository root.
type TestTarget struct {
	Dir       string
	Recursive bool
}

// TestTargets returns the test paths named by the verification commands of a contract:
// `make verify-quick GO_PKGS="./internal/x/ …"` (backend packages) and
// `cd <dir> && bun run test|npx vitest run|go test <paths…>`.
func TestTargets(markdown string) []TestTarget {
	var out []TestTarget
	add := func(base, p string) {
		recursive := strings.HasSuffix(p, "/...") || strings.HasSuffix(p, "...")
		p = strings.TrimSuffix(strings.TrimSuffix(p, "..."), "/")
		out = append(out, TestTarget{Dir: path.Clean(path.Join(base, p)), Recursive: recursive || !strings.HasPrefix(p, "./")})
	}
	for _, line := range strings.Split(markdown, "\n") {
		for _, m := range goPkgsRe.FindAllStringSubmatch(line, -1) {
			for _, p := range strings.Fields(m[1]) {
				add("backend", p)
			}
		}
		for _, m := range cdRunRe.FindAllStringSubmatch(line, -1) {
			for _, p := range strings.Fields(m[2]) {
				if strings.HasPrefix(p, "-") {
					break
				}
				add(m[1], p)
			}
		}
	}
	return out
}

// Check runs every check against the repository at root and returns the problems, in directory order.
func Check(root string) ([]Problem, error) {
	var ignore []string
	if b, err := os.ReadFile(filepath.Join(root, IgnoreFile)); err == nil {
		ignore = ParseIgnore(string(b))
	}
	codeDirs, err := codeDirectories(root, ignore)
	if err != nil {
		return nil, err
	}
	// A directory needs a contract when it holds code or sits above code.
	need := map[string]bool{}
	for dir := range codeDirs {
		parts := strings.Split(dir, "/")
		for i := 1; i <= len(parts); i++ {
			need[strings.Join(parts[:i], "/")] = true
		}
	}
	dirs := make([]string, 0, len(need))
	for d := range need {
		dirs = append(dirs, d)
	}
	sort.Strings(dirs)

	c := checker{root: root, cache: map[string]string{}}
	for _, dir := range dirs {
		file := path.Join(dir, Contract)
		b, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(file)))
		if err != nil {
			c.add(file, "missing contract (new code directory? see .rules/egm.md)")
			continue
		}
		text := string(b)
		c.sections(file, text, codeDirs[dir])
		c.links(file, text)
		c.citations(file, text)
		c.testTargets(file, text)
	}
	if b, err := os.ReadFile(filepath.Join(root, Architecture)); err != nil {
		c.add(Architecture, "missing root blueprint")
	} else {
		c.links(Architecture, string(b))
		c.citations(Architecture, string(b))
	}
	return c.problems, nil
}

type checker struct {
	root     string
	cache    map[string]string
	problems []Problem
}

func (c *checker) add(file, format string, args ...any) {
	p := Problem{File: file, Message: fmt.Sprintf(format, args...)}
	if !slices.Contains(c.problems, p) {
		c.problems = append(c.problems, p)
	}
}

func (c *checker) abs(rel string) string { return filepath.Join(c.root, filepath.FromSlash(rel)) }

func (c *checker) sections(file, text string, isComponent bool) {
	required := blueprint
	if isComponent {
		required = component
	}
	lines := strings.Split(text, "\n")
	for _, s := range required {
		if !slices.ContainsFunc(lines, s.pattern.MatchString) {
			c.add(file, "missing section %q", s.heading)
		}
	}
}

func (c *checker) links(file, text string) {
	base := path.Dir(file)
	for _, target := range Links(text) {
		if _, err := os.Stat(c.abs(path.Join(base, target))); err != nil {
			c.add(file, "broken link %q", target)
		}
	}
}

func (c *checker) citations(file, text string) {
	base := path.Dir(file)
	for _, cite := range Citations(text) {
		found := ""
		for _, candidate := range []string{path.Join(base, cite.Path), path.Clean(cite.Path)} {
			if info, err := os.Stat(c.abs(candidate)); err == nil && !info.IsDir() {
				found = candidate
				break
			}
		}
		if found == "" {
			c.add(file, "cited file not found %q", cite.Path)
			continue
		}
		if cite.Symbol == "" {
			continue
		}
		content, ok := c.cache[found]
		if !ok {
			b, err := os.ReadFile(c.abs(found))
			if err != nil {
				c.add(file, "cannot read %s: %v", found, err)
				continue
			}
			content = string(b)
			c.cache[found] = content
		}
		leaf := cite.Symbol[strings.LastIndex(cite.Symbol, ".")+1:]
		if !ContainsIdentifier(content, leaf) {
			c.add(file, "%q not found in %s", cite.Symbol, found)
		}
	}
}

func (c *checker) testTargets(file, text string) {
	for _, t := range TestTargets(text) {
		if !hasTests(c.abs(t.Dir), t.Recursive) {
			c.add(file, "verification runs tests in %q, which has none", t.Dir)
		}
	}
}

func hasTests(dir string, recursive bool) bool {
	if info, err := os.Stat(dir); err == nil && !info.IsDir() {
		return testFile.MatchString(info.Name()) // a single test file named directly
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if e.Type()&os.ModeSymlink != 0 {
			continue
		}
		if !e.IsDir() && testFile.MatchString(e.Name()) {
			return true
		}
		// Frontend tests live in a __tests__ folder beside the code.
		if e.IsDir() && !skipDirs[e.Name()] && (recursive || e.Name() == "__tests__") && hasTests(filepath.Join(dir, e.Name()), recursive) {
			return true
		}
	}
	return false
}

// codeDirectories maps every directory git knows (tracked or new, not ignored) that holds source
// code outside .egmignore, test folders and hidden or build folders.
func codeDirectories(root string, ignore []string) (map[string]bool, error) {
	cmd := exec.Command("git", "ls-files", "--cached", "--others", "--exclude-standard") //nolint:noctx // a quick local command
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git ls-files: %w", err)
	}
	dirs := map[string]bool{}
	for _, file := range strings.Split(string(out), "\n") {
		dir, name := path.Split(file)
		dir = strings.TrimSuffix(dir, "/")
		if dir == "" || !IsSource(name) {
			continue
		}
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(file))); err != nil {
			continue // deleted in the working tree
		}
		skip := false
		for _, part := range strings.Split(dir, "/") {
			if strings.HasPrefix(part, ".") || skipDirs[part] || testDirs[part] {
				skip = true
				break
			}
		}
		if !skip && !Ignored(dir, ignore) {
			dirs[dir] = true
		}
	}
	return dirs, nil
}
