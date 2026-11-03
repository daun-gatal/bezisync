package release

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

var today = time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)

const repo = "../.."

func copyFile(t *testing.T, from, to string) {
	t.Helper()
	raw, err := os.ReadFile(from)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(to), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(to, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

// fixture copies the real version files at 1.4.2 with a v1.4.2 changelog, no fragments yet.
func fixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for _, rel := range append(append([]string{}, VersionFiles...), "CHANGELOG.md") {
		copyFile(t, filepath.Join(repo, rel), filepath.Join(root, rel))
	}
	if err := os.MkdirAll(filepath.Join(root, "changelogs", "unreleased"), 0o750); err != nil {
		t.Fatal(err)
	}
	write(t, root, "changelogs/unreleased/README.md", "type: major\n")
	if err := SetVersion(root, "1.4.2"); err != nil {
		t.Fatal(err)
	}
	changelog, _ := os.ReadFile(filepath.Join(root, "CHANGELOG.md"))
	header := strings.TrimRight(strings.Split(string(changelog), "## [v")[0], " \n")
	write(t, root, "CHANGELOG.md", header+"\n\n## [v1.4.2] - 2026-01-01\n\n### Added\n- **old** - x\n")
	return root
}

func write(t *testing.T, root, rel, text string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(filepath.Join(root, rel)), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, rel), []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
}

func fragment(t *testing.T, root, name, bump, section string) {
	write(t, root, "changelogs/unreleased/"+name+".md", "type: "+bump+"\n\n### "+section+"\n- **"+name+"** - something "+bump+"\n")
}

func versions(t *testing.T, root string) map[string]bool {
	t.Helper()
	found := map[string]bool{}
	var pkg, doc struct {
		Version string `json:"version"`
		Info    struct {
			Version string `json:"version"`
		} `json:"info"`
	}
	raw, _ := os.ReadFile(filepath.Join(root, PackageJSON))
	_ = json.Unmarshal(raw, &pkg)
	found[pkg.Version] = true
	raw, _ = os.ReadFile(filepath.Join(root, OpenAPI))
	_ = json.Unmarshal(raw, &doc)
	found[doc.Info.Version] = true
	raw, _ = os.ReadFile(filepath.Join(root, GoVersion))
	found[regexp.MustCompile(`(?m)^var Version = "(.+)"`).FindStringSubmatch(string(raw))[1]] = true
	for _, chart := range Charts {
		raw, _ := os.ReadFile(filepath.Join(root, chart))
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.HasPrefix(line, "version:") || strings.HasPrefix(line, "appVersion:") {
				found[strings.Trim(strings.TrimSpace(strings.SplitN(line, ":", 2)[1]), `"`)] = true
			}
		}
	}
	return found
}

func target(t *testing.T, root, override string) string {
	t.Helper()
	cur, err := ReadVersion(root)
	if err != nil {
		t.Fatal(err)
	}
	fr, err := ParseFragments(root)
	if err != nil {
		t.Fatal(err)
	}
	v, err := TargetVersion(cur, fr, override)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestHighestFragmentTypeDecidesTheBump(t *testing.T) {
	root := fixture(t)
	steps := []struct{ name, bump, want string }{{"", "", "1.4.3"}, {"a-fix", "patch", "1.4.3"}, {"a-feature", "minor", "1.5.0"}, {"a-break", "major", "2.0.0"}}
	for _, s := range steps {
		if s.name != "" {
			fragment(t, root, s.name, s.bump, "Fixed")
		}
		if got := target(t, root, ""); got != s.want {
			t.Fatalf("after %s: %s, want %s", s.name, got, s.want)
		}
	}
	if got := target(t, root, "1.9.0"); got != "1.9.0" {
		t.Fatal(got)
	}
	if _, err := TargetVersion("1.4.2", nil, "v1.9"); err == nil {
		t.Fatal("accepted v1.9")
	}
}

func TestPre10BumpsStayBelow10(t *testing.T) {
	for _, c := range [][3]string{{"0.0.0", "minor", "0.0.1"}, {"0.0.0", "major", "0.1.0"}, {"0.1.0", "patch", "0.1.1"}, {"0.1.4", "minor", "0.1.5"}, {"0.1.4", "major", "0.2.0"}} {
		if got, _ := BumpVersion(c[0], c[1]); got != c[2] {
			t.Errorf("%s %s: %s, want %s", c[0], c[1], got, c[2])
		}
	}
}

func TestAssembleSetsEveryVersionAndMovesFragments(t *testing.T) {
	root := fixture(t)
	fragment(t, root, "a-fix", "patch", "Fixed")
	fragment(t, root, "a-feature", "minor", "Added")
	v, err := Assemble(root, "", today)
	if err != nil || v != "1.5.0" {
		t.Fatal(v, err)
	}
	if got := versions(t, root); len(got) != 1 || !got["1.5.0"] {
		t.Fatal(got)
	}
	left, _ := os.ReadDir(filepath.Join(root, "changelogs", "unreleased"))
	if len(left) != 1 || left[0].Name() != "README.md" {
		t.Fatal(left)
	}
	changelog, _ := os.ReadFile(filepath.Join(root, "CHANGELOG.md"))
	if strings.Index(string(changelog), "## [v1.5.0] - 2026-10-27") > strings.Index(string(changelog), "## [v1.4.2]") {
		t.Fatal(string(changelog))
	}
	notes, err := Notes(root, "1.5.0")
	if err != nil || notes != "### Added\n- **a-feature** - something minor\n\n### Fixed\n- **a-fix** - something patch\n" {
		t.Fatalf("%q %v", notes, err)
	}
	// The OpenAPI document is written byte-for-byte like `bezisync openapi` writes it: the
	// untouched parts of the real document survive a rewrite unchanged.
	real, _ := os.ReadFile(filepath.Join(repo, OpenAPI))
	got, _ := os.ReadFile(filepath.Join(root, OpenAPI))
	var r, g map[string]any
	_ = json.Unmarshal(real, &r)
	_ = json.Unmarshal(got, &g)
	if !strings.Contains(string(got), `"version": "1.5.0"`) || len(got) != len(real)-len(r["info"].(map[string]any)["version"].(string))+len("1.5.0") {
		t.Fatal("openapi.json was reformatted")
	}
}

func TestAssembleRefusesNothingOrGoingBackwards(t *testing.T) {
	root := fixture(t)
	if _, err := Assemble(root, "", today); err == nil || !strings.Contains(err.Error(), "no changelog fragments") {
		t.Fatal(err)
	}
	fragment(t, root, "a-fix", "patch", "Fixed")
	if _, err := Assemble(root, "1.4.2", today); err == nil || !strings.Contains(err.Error(), "not newer") {
		t.Fatal(err)
	}
	if got := versions(t, root); len(got) != 1 || !got["1.4.2"] {
		t.Fatal(got)
	}
}

func TestInvalidFragmentAndMismatchedVersions(t *testing.T) {
	root := fixture(t)
	write(t, root, "changelogs/unreleased/bad.md", "type: huge\n### Fixed\n- x\n")
	if _, err := ParseFragments(root); err == nil || !strings.Contains(err.Error(), "bad.md") {
		t.Fatal(err)
	}
	pkg := filepath.Join(root, PackageJSON)
	raw, _ := os.ReadFile(pkg)
	_ = os.WriteFile(pkg, []byte(strings.Replace(string(raw), `"version": "1.4.2"`, `"version": "9.9.9"`, 1)), 0o600)
	if _, err := ReadVersion(root); err == nil || !strings.Contains(err.Error(), "mismatch") {
		t.Fatal(err)
	}
	_ = os.WriteFile(pkg, raw, 0o600)
	chart := filepath.Join(root, Charts[1])
	raw, _ = os.ReadFile(chart)
	_ = os.WriteFile(chart, []byte(strings.Replace(string(raw), "version: 1.4.2", "version: 1.4.1", 1)), 0o600)
	if _, err := ReadVersion(root); err == nil || !strings.Contains(err.Error(), "Chart.yaml version") {
		t.Fatal(err)
	}
}

func TestNotesForAMissingVersion(t *testing.T) {
	root := fixture(t)
	if n, err := Notes(root, "1.4.2"); err != nil || !strings.HasPrefix(n, "### Added\n") {
		t.Fatal(n, err)
	}
	if _, err := Notes(root, "7.0.0"); err == nil {
		t.Fatal("found v7.0.0")
	}
}

func TestOnlyBuildContextsAndChartsReachAnArtifact(t *testing.T) {
	reaches := []string{"backend/internal/api/api.go", "backend/cmd/bezisync/main.go", "backend/go.mod", "backend/go.sum", "backend/Dockerfile",
		"frontend/src/App.tsx", "frontend/nginx.conf.template", "frontend/package.json", "frontend/.dockerignore",
		"deploy/helm/bezisync/values.yaml", "deploy/helm/bezisync-agent/templates/deployment.yaml"}
	stays := []string{"backend/internal/api/api_test.go", "backend/testdata/contract/compile.json", "backend/.golangci.yml",
		"frontend/src/lib/__tests__/format.test.ts", "frontend/src/test/setup.ts", "frontend/openapi.json", "docs/README.md", "e2e/Dockerfile",
		"k8s/values.yaml", "deploy/compose/docker-compose.yml", "deploy/e2e/values.yaml", ".github/workflows/ci.yml", "tools/release/release.go", "CHANGELOG.md"}
	for _, p := range reaches {
		if ok, err := ReachesArtifact(repo, p); err != nil || !ok {
			t.Errorf("%s should reach an artifact (%v)", p, err)
		}
	}
	for _, p := range stays {
		if ok, err := ReachesArtifact(repo, p); err != nil || ok {
			t.Errorf("%s should not reach an artifact (%v)", p, err)
		}
	}
}

func TestAFragmentExactlyWhenAnArtifactChanges(t *testing.T) {
	root := fixture(t)
	write(t, root, "backend/.dockerignore", "testdata\n**/*_test.go\n")
	write(t, root, "frontend/.dockerignore", "**/__tests__\n")
	fragment(t, root, "a-fix", "patch", "Fixed")
	code, added := "backend/internal/x.go", "changelogs/unreleased/a-fix.md"
	classify := func(paths ...string) Changes {
		c, err := Classify(root, paths)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	if p := classify(code, added).Problems(); len(p) != 0 {
		t.Fatal(p)
	}
	if p := classify(code).Problems(); len(p) != 1 || !strings.Contains(p[0], "need a changelog fragment") {
		t.Fatal(p)
	}
	if p := classify(added, "docs/a.md").Problems(); len(p) != 1 || !strings.Contains(p[0], "nothing to release") {
		t.Fatal(p)
	}
	if p := classify("changelogs/unreleased/gone.md", "changelogs/unreleased/README.md", "docs/a.md").Problems(); len(p) != 0 {
		t.Fatal(p)
	}
	quiet := classify("backend/internal/x_test.go", "frontend/src/__tests__/a.test.ts")
	if len(quiet.Artifacts) != 0 || quiet.Demo || len(quiet.Problems()) != 0 {
		t.Fatalf("%+v", quiet)
	}
	if !classify("deploy/compose/docker-compose.yml").Demo {
		t.Fatal("compose change does not run the demo")
	}
}

func TestDockerignoreExceptionsAreRefused(t *testing.T) {
	root := fixture(t)
	write(t, root, "frontend/.dockerignore", "src\n!src/main.tsx\n")
	if _, err := ReachesArtifact(root, "frontend/src/main.tsx"); err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatal(err)
	}
}

func TestChangesComparesAgainstTheMergeBase(t *testing.T) {
	paths, err := ChangedPaths(repo, "HEAD")
	if err != nil || len(paths) != 0 {
		t.Fatal(paths, err)
	}
	if _, err := ChangedPaths(repo, "no-such-ref"); err == nil || !strings.Contains(err.Error(), "git diff") {
		t.Fatal(err)
	}
}

func TestFnmatchLikePython(t *testing.T) {
	for _, c := range []struct {
		name, pattern string
		want          bool
	}{{"a/b/c_test.go", "*_test.go", true}, {"x.go", "*_test.go", false}, {"testdata", "testdata", true}, {"a", "[ab]", true}, {"c", "[!ab]", true}, {"a", "[!ab]", false}, {"a?", "a\\?", false}} {
		if got := fnmatch(c.name, c.pattern); got != c.want {
			t.Errorf("fnmatch(%q, %q) = %v", c.name, c.pattern, got)
		}
	}
}
