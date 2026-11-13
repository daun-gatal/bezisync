package release

import (
	"encoding/json"
	"os"
	"os/exec"
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

// fixture copies the real version files at app version 1.4.2 (charts at their own 0.3.0) with a
// v1.4.2 changelog and no fragments, committed in a fresh git repository.
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
	for _, c := range charts(t, root) {
		if err := SetChartVersion(root, c, "0.3.0"); err != nil {
			t.Fatal(err)
		}
	}
	changelog, _ := os.ReadFile(filepath.Join(root, "CHANGELOG.md"))
	header := strings.TrimRight(strings.Split(string(changelog), "## [v")[0], " \n")
	write(t, root, "CHANGELOG.md", header+"\n\n## [v1.4.2] - 2026-01-01\n\n### Added\n- **old** - x\n")
	run(t, root, "init", "-q")
	commit(t, root, "fixture")
	return root
}

func run(t *testing.T, root string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false"}, args...)...) //nolint:noctx // a test
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return string(out)
}

func commit(t *testing.T, root, message string) {
	t.Helper()
	run(t, root, "add", "-A")
	run(t, root, "commit", "-q", "--allow-empty", "-m", message)
}

func charts(t *testing.T, root string) []Chart {
	t.Helper()
	c, err := ReadCharts(root)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func chart(t *testing.T, root, name string) Chart {
	t.Helper()
	for _, c := range charts(t, root) {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("no chart %s", name)
	return Chart{}
}

// publish tags a chart's current version as released.
func publish(t *testing.T, root, name string) {
	t.Helper()
	c := chart(t, root, name)
	run(t, root, "tag", c.Tag(c.Version))
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

// appVersions is every app version found (the files and every chart's appVersion).
func appVersions(t *testing.T, root string) map[string]bool {
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
	for _, c := range charts(t, root) {
		found[c.AppVersion] = true
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
	steps := []struct{ name, bump, want string }{{"", "", "1.4.3"}, {"a-fix", "patch", "1.4.3"}, {"a-feature", "minor", "1.5.0"}, {"another-fix", "patch", "1.5.0"}, {"a-break", "major", "2.0.0"}}
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

// Plain semver below 1.0 too: the first release is 0.0.0 with a minor fragment, so v0.1.0.
func TestPlainSemverBelow10(t *testing.T) {
	for _, c := range [][3]string{{"0.0.0", "minor", "0.1.0"}, {"0.0.0", "patch", "0.0.1"}, {"0.0.0", "major", "1.0.0"},
		{"0.1.0", "patch", "0.1.1"}, {"0.1.4", "minor", "0.2.0"}, {"0.1.4", "major", "1.0.0"}, {"1.4.2", "minor", "1.5.0"}} {
		if got, _ := BumpVersion(c[0], c[1]); got != c[2] {
			t.Errorf("%s %s: %s, want %s", c[0], c[1], got, c[2])
		}
	}
}

func TestAssembleSetsTheAppVersionAndBumpsOnlyPublishedCharts(t *testing.T) {
	root := fixture(t)
	publish(t, root, "bezisync-agent") // released, so only its appVersion would change: patch bump
	fragment(t, root, "a-fix", "patch", "Fixed")
	fragment(t, root, "a-feature", "minor", "Added")
	v, err := Assemble(root, "", today)
	if err != nil || v != "1.5.0" {
		t.Fatal(v, err)
	}
	if got := appVersions(t, root); len(got) != 1 || !got["1.5.0"] {
		t.Fatal(got)
	}
	if got := chart(t, root, "bezisync").Version; got != "0.3.0" { // unpublished: ships as is
		t.Fatal(got)
	}
	if got := chart(t, root, "bezisync-agent").Version; got != "0.3.1" {
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
	if got := appVersions(t, root); len(got) != 1 || !got["1.4.2"] {
		t.Fatal(got)
	}
}

func TestPlanDecidesWhatAReleasePublishes(t *testing.T) {
	root := fixture(t)
	plan := func(override string) Plan {
		t.Helper()
		p, err := MakePlan(root, override)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	// Nothing published yet: both charts' versions are new, a chart-only release.
	if p := plan(""); p.Kind != "charts" || p.Version != "1.4.3" || len(p.Charts) != 2 || p.AgentChart != "0.3.0" {
		t.Fatalf("%+v", p)
	}
	publish(t, root, "bezisync")
	publish(t, root, "bezisync-agent")
	if p := plan(""); p.Kind != "none" || len(p.Charts) != 0 || p.PreCharts[0] != (ChartRelease{"bezisync", "0.3.1"}) {
		t.Fatalf("%+v", p)
	}
	// A chart change bumped one chart: only that chart ships, and it pins the published agent chart.
	if err := SetChartVersion(root, chart(t, root, "bezisync"), "0.4.0"); err != nil {
		t.Fatal(err)
	}
	if p := plan(""); p.Kind != "charts" || len(p.Charts) != 1 || p.Charts[0] != (ChartRelease{"bezisync", "0.4.0"}) || p.AgentChart != "0.3.0" {
		t.Fatalf("%+v", p)
	}
	// Fragments make it an app release, with every chart at the version it ships as.
	fragment(t, root, "a-feature", "minor", "Added")
	p := plan("")
	want := []ChartRelease{{"bezisync", "0.4.0"}, {"bezisync-agent", "0.3.1"}}
	if p.Kind != "app" || p.Version != "1.5.0" || len(p.Charts) != 2 || p.Charts[0] != want[0] || p.Charts[1] != want[1] || p.AgentChart != "0.3.1" {
		t.Fatalf("%+v", p)
	}
	if p := plan("2.0.0"); p.Version != "2.0.0" {
		t.Fatalf("%+v", p)
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
	file := filepath.Join(root, Charts[1])
	raw, _ = os.ReadFile(file)
	_ = os.WriteFile(file, []byte(strings.Replace(string(raw), `appVersion: "1.4.2"`, `appVersion: "1.4.1"`, 1)), 0o600)
	if _, err := ReadVersion(root); err == nil || !strings.Contains(err.Error(), "Chart.yaml appVersion") {
		t.Fatal(err)
	}
	_ = os.WriteFile(file, []byte(strings.Replace(string(raw), "version: 0.3.0", "version: 0.3", 1)), 0o600)
	if _, err := ReadCharts(root); err == nil || !strings.Contains(err.Error(), "must look like") {
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

func TestOnlyBuildContextsReachAnImage(t *testing.T) {
	reaches := []string{"backend/internal/api/api.go", "backend/cmd/bezisync/main.go", "backend/go.mod", "backend/go.sum", "backend/Dockerfile",
		"frontend/src/App.tsx", "frontend/nginx.conf.template", "frontend/package.json", "frontend/.dockerignore"}
	stays := []string{"backend/internal/api/api_test.go", "backend/testdata/contract/compile.json", "backend/.golangci.yml",
		"frontend/src/lib/__tests__/format.test.ts", "frontend/src/test/setup.ts", "frontend/openapi.json", "docs/README.md", "e2e/Dockerfile",
		"deploy/helm/bezisync/values.yaml", "deploy/helm/bezisync-agent/templates/deployment.yaml", "deploy/apparmor/bezisync-default",
		"deploy/compose/docker-compose.yml", "deploy/e2e/values.yaml", ".github/workflows/ci.yml", "tools/release/release.go", "CHANGELOG.md"}
	for _, p := range reaches {
		if ok, err := ReachesImage(repo, p); err != nil || !ok {
			t.Errorf("%s should reach an image (%v)", p, err)
		}
	}
	for _, p := range stays {
		if ok, err := ReachesImage(repo, p); err != nil || ok {
			t.Errorf("%s should not reach an image (%v)", p, err)
		}
	}
}

func TestAFragmentExactlyWhenAnImageChanges(t *testing.T) {
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
	// A chart change takes a version bump, not a fragment.
	if p := classify(added, "deploy/helm/bezisync/values.yaml").Problems(); len(p) != 1 || !strings.Contains(p[0], "nothing to release") {
		t.Fatal(p)
	}
	if p := classify("changelogs/unreleased/gone.md", "changelogs/unreleased/README.md", "docs/a.md").Problems(); len(p) != 0 {
		t.Fatal(p)
	}
	quiet := classify("backend/internal/x_test.go", "frontend/src/__tests__/a.test.ts")
	if len(quiet.Images) != 0 || quiet.Demo || len(quiet.Problems()) != 0 {
		t.Fatalf("%+v", quiet)
	}
	if !classify(".devcontainer/lab/compose.yml").Demo || !classify("scripts/lab-smoke.sh").Demo {
		t.Fatal("a lab change must run the demo smoke tests")
	}
	if !classify("deploy/compose/docker-compose.yml").Demo {
		t.Fatal("compose change does not run the demo")
	}
}

func TestAChangedChartNeedsANewVersion(t *testing.T) {
	root := fixture(t)
	classify := func(paths ...string) Changes {
		c, err := Classify(root, paths)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	values := "deploy/helm/bezisync/values.yaml"
	// Never published: any version is new.
	if c := classify(values); len(c.Problems()) != 0 || len(c.Charts) != 1 || c.Charts[0] != "bezisync" {
		t.Fatalf("%+v", c)
	}
	publish(t, root, "bezisync")
	if p := classify(values).Problems(); len(p) != 1 || !strings.Contains(p[0], "bump version in deploy/helm/bezisync/Chart.yaml") {
		t.Fatal(p)
	}
	// The other chart is not affected by this one's tag.
	if p := classify("deploy/helm/bezisync-agent/values.yaml").Problems(); len(p) != 0 {
		t.Fatal(p)
	}
	if err := SetChartVersion(root, chart(t, root, "bezisync"), "0.2.9"); err != nil {
		t.Fatal(err)
	}
	if p := classify(values).Problems(); len(p) != 1 || !strings.Contains(p[0], "older than the published 0.3.0") {
		t.Fatal(p)
	}
	// Two changes before a release share one bump: 0.3.1 stays unpublished until released.
	if err := SetChartVersion(root, chart(t, root, "bezisync"), "0.3.1"); err != nil {
		t.Fatal(err)
	}
	if p := classify(values).Problems(); len(p) != 0 {
		t.Fatal(p)
	}
}

func TestChartNotesListThePullRequestsSinceTheLastRelease(t *testing.T) {
	root := fixture(t)
	publish(t, root, "bezisync")
	write(t, root, "deploy/helm/bezisync/templates/a.yaml", "a: 1\n")
	commit(t, root, "Merge pull request #12 from someone/branch\n\nfeat(chart): add pod labels")
	write(t, root, "deploy/helm/bezisync/templates/b.yaml", "b: 1\n")
	commit(t, root, "fix(chart): probe timeout (#13)")
	write(t, root, "deploy/helm/bezisync-agent/templates/c.yaml", "c: 1\n")
	commit(t, root, "fix(agent chart): not this chart (#14)")
	commit(t, root, "chore: release v1.4.3")
	if err := SetChartVersion(root, chart(t, root, "bezisync"), "0.3.1"); err != nil {
		t.Fatal(err)
	}
	notes, err := ChartNotes(root, chart(t, root, "bezisync"))
	if err != nil {
		t.Fatal(err)
	}
	want := "Helm chart **bezisync 0.3.1** (installs BeziSync 1.4.2).\n\n### Changes\n- fix(chart): probe timeout (#13)\n- feat(chart): add pod labels (#12)\n"
	if notes != want {
		t.Fatalf("%q", notes)
	}
}

func TestDockerignoreExceptionsAreRefused(t *testing.T) {
	root := fixture(t)
	write(t, root, "frontend/.dockerignore", "src\n!src/main.tsx\n")
	if _, err := ReachesImage(root, "frontend/src/main.tsx"); err == nil || !strings.Contains(err.Error(), "not supported") {
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
