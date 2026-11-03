// Package release plans and assembles BeziSync releases. The app version (images, CLI) comes
// from the changelog fragments in changelogs/unreleased/: a change carries a fragment exactly
// when it reaches an image's build context. Each Helm chart has its own version: a change to a
// chart bumps it by hand, and a version counts as published once its chart-<name>-v<version>
// tag exists. See cmd/release.
package release

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Every file that carries the app version, relative to the repository root.
const (
	GoVersion   = "backend/internal/version/version.go"
	PackageJSON = "frontend/package.json"
	OpenAPI     = "frontend/openapi.json"
	FragmentDir = "changelogs/unreleased/"
	ChartDir    = "deploy/helm/"
)

var (
	// Charts are the published Helm charts (each Chart.yaml carries its own version and the
	// app version as appVersion).
	Charts       = []string{"deploy/helm/bezisync/Chart.yaml", "deploy/helm/bezisync-agent/Chart.yaml"}
	AppFiles     = []string{GoVersion, PackageJSON, OpenAPI}
	VersionFiles = append(append([]string{}, AppFiles...), Charts...)
	// Each image is built from its directory (Dockerfile, .dockerignore and every file the
	// .dockerignore keeps). The demo smoke test also builds both images, and runs when the
	// compose stack or its tool changes.
	ImageContexts = []string{"backend", "frontend"}
	DemoPaths     = []string{"deploy/compose/", "scripts/demo-smoke.sh", "tools/cmd/demo-smoke/"}

	skipFiles     = map[string]bool{"README.md": true, ".gitkeep": true}
	categories    = []string{"Added", "Changed", "Fixed", "Removed"}
	bumpOrder     = map[string]int{"patch": 0, "minor": 1, "major": 2}
	semver        = regexp.MustCompile(`^(\d+)\.(\d+)\.(\d+)$`)
	goVersionLine = regexp.MustCompile(`(?m)^(var Version = ")[^"]+(")`)
	// Chart.yaml fields: *Set patterns keep groups 1 and 2 around the value (see substitute).
	chartName       = regexp.MustCompile(`(?m)^name:\s*(\S+)\s*$`)
	chartVersionGet = regexp.MustCompile(`(?m)^version:\s*"?([^"\s]+)"?\s*$`)
	chartVersionSet = regexp.MustCompile(`(?m)^(version:\s*)\S+()`)
	chartAppGet     = regexp.MustCompile(`(?m)^appVersion:\s*"?([^"\s]+)"?\s*$`)
	chartAppSet     = regexp.MustCompile(`(?m)^(appVersion:\s*")[^"]+(")`)
)

// Error is a release problem shown to the person running the tool.
type Error struct{ Msg string }

func (e *Error) Error() string { return e.Msg }

func fail(format string, a ...any) error { return &Error{fmt.Sprintf(format, a...)} }

// Fragment is one changelog fragment.
type Fragment struct {
	Path     string
	BumpType string
	Sections map[string][]string
}

func read(root, relative string) (string, error) {
	raw, err := os.ReadFile(filepath.Join(root, relative))
	return string(raw), err
}

// Chart is a Helm chart in the repository.
type Chart struct {
	Name       string // the chart's name (Chart.yaml name)
	File       string // its Chart.yaml, relative to the root
	Version    string // the chart's own version
	AppVersion string // the app version it installs
}

// Dir is the chart's directory, relative to the root, with a trailing slash.
func (c Chart) Dir() string { return filepath.ToSlash(filepath.Dir(c.File)) + "/" }

// Tag is the git tag that marks a published version of the chart.
func (c Chart) Tag(version string) string { return ChartTag(c.Name, version) }

// ChartTag is the git tag of a published chart version: chart-<name>-v<version>.
func ChartTag(name, version string) string { return "chart-" + name + "-v" + version }

// ReadCharts reads every chart's name and versions.
func ReadCharts(root string) ([]Chart, error) {
	var out []Chart
	for _, file := range Charts {
		text, err := read(root, file)
		if err != nil {
			return nil, err
		}
		c := Chart{File: file}
		for _, f := range []struct {
			pattern *regexp.Regexp
			field   *string
			key     string
		}{{chartName, &c.Name, "name"}, {chartVersionGet, &c.Version, "version"}, {chartAppGet, &c.AppVersion, "appVersion"}} {
			m := f.pattern.FindStringSubmatch(text)
			if m == nil {
				return nil, fail("%s has no %s", file, f.key)
			}
			*f.field = m[1]
		}
		if !semver.MatchString(c.Version) {
			return nil, fail("%s: version must look like 1.2.3, got: %s", file, c.Version)
		}
		out = append(out, c)
	}
	return out, nil
}

// ReadVersion is the current app version; the backend, frontend and every chart's appVersion
// must agree.
func ReadVersion(root string) (string, error) {
	src, err := read(root, GoVersion)
	if err != nil {
		return "", err
	}
	m := goVersionLine.FindStringSubmatchIndex(src)
	if m == nil {
		return "", fail("could not find the version in %s", GoVersion)
	}
	version := src[m[3]:m[4]]
	pkg, err := read(root, PackageJSON)
	if err != nil {
		return "", err
	}
	var front struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal([]byte(pkg), &front); err != nil {
		return "", err
	}
	if front.Version != version {
		return "", fail("version mismatch: %s=%s, %s=%s", GoVersion, version, PackageJSON, front.Version)
	}
	charts, err := ReadCharts(root)
	if err != nil {
		return "", err
	}
	for _, c := range charts {
		if c.AppVersion != version {
			return "", fail("version mismatch: %s appVersion=%s, %s=%s", c.File, c.AppVersion, GoVersion, version)
		}
	}
	return version, nil
}

// BumpVersion applies a fragment type, plain semver: major → X+1.0.0, minor → X.Y+1.0,
// patch → X.Y.Z+1 (also below 1.0, where a breaking change takes a minor fragment).
func BumpVersion(current, bumpType string) (string, error) {
	m := semver.FindStringSubmatch(current)
	if m == nil {
		return "", fail("unsupported version format: %s", current)
	}
	major, _ := strconv.Atoi(m[1])
	minor, _ := strconv.Atoi(m[2])
	patch, _ := strconv.Atoi(m[3])
	switch bumpType {
	case "major":
		return fmt.Sprintf("%d.0.0", major+1), nil
	case "minor":
		return fmt.Sprintf("%d.%d.0", major, minor+1), nil
	}
	return fmt.Sprintf("%d.%d.%d", major, minor, patch+1), nil
}

var (
	typeLine    = regexp.MustCompile(`^type:\s*(major|minor|patch)\s*$`)
	sectionLine = regexp.MustCompile(`^###\s+(Added|Changed|Fixed|Removed)\s*$`)
)

// ParseFragments reads changelogs/unreleased/*.md (except README.md) in name order.
func ParseFragments(root string) ([]Fragment, error) {
	paths, err := filepath.Glob(filepath.Join(root, "changelogs", "unreleased", "*.md"))
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	var out []Fragment
	for _, path := range paths {
		name := filepath.Base(path)
		if skipFiles[name] {
			continue
		}
		raw, err := os.ReadFile(path) //nolint:gosec // a fragment in the repository
		if err != nil {
			return nil, err
		}
		lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
		if strings.TrimSpace(string(raw)) == "" {
			return nil, fail("%s: file is empty", name)
		}
		m := typeLine.FindStringSubmatch(lines[0])
		if m == nil {
			return nil, fail(`%s: first line must be "type: major|minor|patch"`, name)
		}
		f := Fragment{Path: path, BumpType: m[1], Sections: map[string][]string{}}
		category := ""
		for _, line := range lines[1:] {
			line = strings.TrimRight(line, "\r")
			if h := sectionLine.FindStringSubmatch(line); h != nil {
				category = h[1]
				if _, ok := f.Sections[category]; !ok {
					f.Sections[category] = []string{}
				}
			} else if category != "" && strings.HasPrefix(strings.TrimLeft(line, " \t"), "-") {
				f.Sections[category] = append(f.Sections[category], line)
			}
		}
		bullets := 0
		for _, b := range f.Sections {
			bullets += len(b)
		}
		if bullets == 0 {
			return nil, fail("%s: add at least one bullet under a valid section", name)
		}
		out = append(out, f)
	}
	return out, nil
}

// TargetVersion is the version the fragments (or an explicit override) give.
func TargetVersion(current string, fragments []Fragment, override string) (string, error) {
	if override != "" {
		if !semver.MatchString(override) {
			return "", fail("version override must look like 1.2.3, got: %s", override)
		}
		return override, nil
	}
	bump := "patch"
	for _, f := range fragments {
		if bumpOrder[f.BumpType] > bumpOrder[bump] {
			bump = f.BumpType
		}
	}
	return BumpVersion(current, bump)
}

// ChangelogBlock is the CHANGELOG.md section of a release.
func ChangelogBlock(version string, fragments []Fragment, today time.Time) string {
	block := fmt.Sprintf("## [v%s] - %s\n", version, today.Format("2006-01-02"))
	for _, category := range categories {
		var bullets []string
		for _, f := range fragments {
			bullets = append(bullets, f.Sections[category]...)
		}
		if len(bullets) > 0 {
			block += "\n### " + category + "\n" + strings.Join(bullets, "\n") + "\n"
		}
	}
	return block
}

var firstRelease = regexp.MustCompile(`(?m)^## \[v\d`)

func updateChangelog(root, version string, fragments []Fragment, today time.Time) error {
	changelog, err := read(root, "CHANGELOG.md")
	if err != nil {
		return err
	}
	block := ChangelogBlock(version, fragments, today)
	var updated string
	if loc := firstRelease.FindStringIndex(changelog); loc == nil {
		updated = strings.TrimRight(changelog, " \t\n\r") + "\n\n" + block
	} else {
		updated = changelog[:loc[0]] + block + "\n" + changelog[loc[0]:]
	}
	return os.WriteFile(filepath.Join(root, "CHANGELOG.md"), []byte(strings.TrimRight(updated, " \t\n\r")+"\n"), 0o644) //nolint:gosec // a tracked text file
}

// Notes is the body of the CHANGELOG.md section of a version (without its heading).
func Notes(root, version string) (string, error) {
	changelog, err := read(root, "CHANGELOG.md")
	if err != nil {
		return "", err
	}
	heading := regexp.MustCompile(`(?m)^## \[v` + regexp.QuoteMeta(version) + `\][^\n]*\n`).FindStringIndex(changelog)
	if heading == nil {
		return "", fail("CHANGELOG.md has no section for v%s", version)
	}
	rest := changelog[heading[1]:]
	end := len(rest)
	if next := regexp.MustCompile(`(?m)^## \[`).FindStringIndex(rest); next != nil {
		end = next[0]
	}
	return strings.TrimSpace(rest[:end]) + "\n", nil
}

func substitute(root, relative string, pattern *regexp.Regexp, version string) error {
	text, err := read(root, relative)
	if err != nil {
		return err
	}
	loc := pattern.FindStringSubmatchIndex(text)
	if loc == nil {
		return fail("could not find the version in %s", relative)
	}
	// Keep groups 1 and 2, replace what lies between them (the first match only).
	updated := text[:loc[3]] + version + text[loc[4]:]
	return os.WriteFile(filepath.Join(root, relative), []byte(updated), 0o644) //nolint:gosec // a tracked text file
}

// SetChartVersion writes a chart's own version.
func SetChartVersion(root string, c Chart, version string) error {
	return substitute(root, c.File, chartVersionSet, version)
}

// SetVersion writes the app version everywhere it lives: the backend, the frontend, the
// OpenAPI document and every chart's appVersion (not the charts' own versions).
func SetVersion(root, version string) error {
	if err := substitute(root, GoVersion, goVersionLine, version); err != nil {
		return err
	}
	if err := substitute(root, PackageJSON, regexp.MustCompile(`(?m)^(  "version"\s*:\s*")[^"]+(")`), version); err != nil {
		return err
	}
	for _, chart := range Charts {
		if err := substitute(root, chart, chartAppSet, version); err != nil {
			return err
		}
	}
	// Written exactly like `bezisync openapi` does, so CI's drift check stays green.
	raw, err := read(root, OpenAPI)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var doc map[string]any
	if err := dec.Decode(&doc); err != nil {
		return err
	}
	info, ok := doc["info"].(map[string]any)
	if !ok {
		return fail("%s has no info object", OpenAPI)
	}
	info["version"] = version
	var b strings.Builder
	if err := writeIndented(&b, doc, 0); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(root, OpenAPI), []byte(b.String()+"\n"), 0o644) //nolint:gosec // a tracked text file
}

func key(version string) [3]int {
	var out [3]int
	for i, p := range strings.SplitN(version, ".", 3) {
		out[i], _ = strconv.Atoi(p)
	}
	return out
}

func newer(a, b string) bool {
	ka, kb := key(a), key(b)
	for i := range ka {
		if ka[i] != kb[i] {
			return ka[i] > kb[i]
		}
	}
	return false
}

func git(root string, args ...string) (string, error) {
	cmd := exec.Command("git", args...) //nolint:gosec,noctx // a fixed git argv, no shell
	cmd.Dir = root
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return "", fail("git %s failed: %s", strings.Join(args, " "), strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

// PublishedVersions are the chart's versions that have a chart-<name>-v<version> tag.
func PublishedVersions(root string, c Chart) ([]string, error) {
	out, err := git(root, "tag", "--list", c.Tag("*"))
	if err != nil {
		return nil, err
	}
	var versions []string
	for _, line := range strings.Split(out, "\n") {
		if v := strings.TrimPrefix(strings.TrimSpace(line), c.Tag("")); v != strings.TrimSpace(line) && semver.MatchString(v) {
			versions = append(versions, v)
		}
	}
	sort.Slice(versions, func(i, j int) bool { return newer(versions[j], versions[i]) })
	return versions, nil
}

// Published reports whether a version of the chart was released (its tag exists).
func Published(root string, c Chart, version string) (bool, error) {
	versions, err := PublishedVersions(root, c)
	if err != nil {
		return false, err
	}
	for _, v := range versions {
		if v == version {
			return true, nil
		}
	}
	return false, nil
}

// NextChartVersion is the version the chart ships as in its next release: its current version
// while that is unpublished (a chart change bumped it), else a patch bump (only appVersion
// changed).
func NextChartVersion(root string, c Chart) (string, error) {
	published, err := Published(root, c, c.Version)
	if err != nil || !published {
		return c.Version, err
	}
	return BumpVersion(c.Version, "patch")
}

// ChartRelease is a chart at the version a release publishes.
type ChartRelease struct{ Name, Version string }

// Plan is what a release from the current tree publishes.
type Plan struct {
	// Kind is "app" (changelog fragments: images, CLI and every chart), "charts" (no fragments,
	// but a chart's version is unpublished: only those charts) or "none".
	Kind string
	// Version is the app version: the release's for "app", the next patch otherwise (what a
	// pre-release build of the images carries).
	Version string
	// Charts are the charts this release publishes, at their versions.
	Charts []ChartRelease
	// PreCharts are every chart at the version it would ship as (pre-release builds).
	PreCharts []ChartRelease
	// AgentChart is the bezisync-agent version the control-plane chart of this release pins for
	// agent installs: the one released with it, else the newest published one ("" when none).
	AgentChart string
}

// AgentChartName is the agent chart, whose version the control-plane chart pins.
const AgentChartName = "bezisync-agent"

// MakePlan decides what a release publishes. An override sets the app version.
func MakePlan(root, override string) (Plan, error) {
	fragments, err := ParseFragments(root)
	if err != nil {
		return Plan{}, err
	}
	current, err := ReadVersion(root)
	if err != nil {
		return Plan{}, err
	}
	charts, err := ReadCharts(root)
	if err != nil {
		return Plan{}, err
	}
	p := Plan{Kind: "none"}
	var unpublished []ChartRelease
	for _, c := range charts {
		next, err := NextChartVersion(root, c)
		if err != nil {
			return Plan{}, err
		}
		p.PreCharts = append(p.PreCharts, ChartRelease{c.Name, next})
		if next == c.Version {
			unpublished = append(unpublished, ChartRelease{c.Name, next})
		}
	}
	switch {
	case len(fragments) > 0:
		p.Kind, p.Charts = "app", p.PreCharts
	case len(unpublished) > 0:
		p.Kind, p.Charts = "charts", unpublished
	}
	if p.Kind == "app" || override != "" {
		p.Version, err = TargetVersion(current, fragments, override)
	} else {
		p.Version, err = BumpVersion(current, "patch")
	}
	if err != nil {
		return p, err
	}
	for _, c := range p.Charts {
		if c.Name == AgentChartName {
			p.AgentChart = c.Version
		}
	}
	for _, c := range charts {
		if c.Name == AgentChartName && p.AgentChart == "" {
			published, err := PublishedVersions(root, c)
			if err != nil {
				return p, err
			}
			if len(published) > 0 {
				p.AgentChart = published[0]
			}
		}
	}
	return p, nil
}

// Assemble moves the fragments into a new CHANGELOG.md section, deletes them, sets the app
// version everywhere (every chart's appVersion included) and bumps the patch of every chart
// whose version is already published; it returns the app version.
func Assemble(root, override string, today time.Time) (string, error) {
	fragments, err := ParseFragments(root)
	if err != nil {
		return "", err
	}
	if len(fragments) == 0 {
		return "", fail("no changelog fragments - nothing to release")
	}
	current, err := ReadVersion(root)
	if err != nil {
		return "", err
	}
	version, err := TargetVersion(current, fragments, override)
	if err != nil {
		return "", err
	}
	if !newer(version, current) {
		return "", fail("v%s is not newer than the current v%s", version, current)
	}
	charts, err := ReadCharts(root)
	if err != nil {
		return "", err
	}
	for _, c := range charts {
		next, err := NextChartVersion(root, c)
		if err != nil {
			return "", err
		}
		if next != c.Version {
			if err := SetChartVersion(root, c, next); err != nil {
				return "", err
			}
		}
	}
	if err := updateChangelog(root, version, fragments, today); err != nil {
		return "", err
	}
	if err := SetVersion(root, version); err != nil {
		return "", err
	}
	for _, f := range fragments {
		if err := os.Remove(f.Path); err != nil {
			return "", err
		}
	}
	return version, nil
}

func dockerignore(root, context string) ([]string, error) {
	raw, err := os.ReadFile(filepath.Join(root, context, ".dockerignore")) //nolint:gosec // a tracked file
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var patterns []string
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "!") {
			return nil, fail("%s/.dockerignore: exceptions (!) are not supported here", context)
		}
		patterns = append(patterns, strings.Trim(line, "/"))
	}
	return patterns, nil
}

// fnmatch is Python's fnmatch.fnmatchcase: * and ? also match "/".
func fnmatch(name, pattern string) bool {
	var re strings.Builder
	re.WriteString("^")
	for i := 0; i < len(pattern); i++ {
		c := pattern[i]
		switch c {
		case '*':
			re.WriteString(".*")
		case '?':
			re.WriteString(".")
		case '[':
			j := i + 1
			if j < len(pattern) && pattern[j] == '!' {
				j++
			}
			if j < len(pattern) && pattern[j] == ']' {
				j++
			}
			for j < len(pattern) && pattern[j] != ']' {
				j++
			}
			if j >= len(pattern) {
				re.WriteString(`\[`)
				continue
			}
			class := pattern[i+1 : j]
			if strings.HasPrefix(class, "!") {
				class = "^" + class[1:]
			}
			re.WriteString("[" + strings.ReplaceAll(class, `\`, `\\`) + "]")
			i = j
		default:
			re.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	re.WriteString("$")
	ok, _ := regexp.MatchString(re.String(), name)
	return ok
}

// ignored reports whether Docker leaves relative out of the context: a pattern matches it
// or a parent.
func ignored(relative string, patterns []string) bool {
	parts := strings.Split(relative, "/")
	for _, pattern := range patterns {
		candidates := []string{pattern, strings.TrimPrefix(pattern, "**/")} // ** also matches no directory
		for i := range parts {
			prefix := strings.Join(parts[:i+1], "/")
			for _, c := range candidates {
				if fnmatch(prefix, c) {
					return true
				}
			}
		}
	}
	return false
}

// ReachesImage reports whether a change to path changes a published image.
func ReachesImage(root, path string) (bool, error) {
	for _, context := range ImageContexts {
		if !strings.HasPrefix(path, context+"/") {
			continue
		}
		relative := strings.TrimPrefix(path, context+"/")
		if relative == "Dockerfile" || relative == ".dockerignore" {
			return true, nil
		}
		patterns, err := dockerignore(root, context)
		if err != nil {
			return false, err
		}
		return !ignored(relative, patterns), nil
	}
	return false, nil
}

// Changes is what a set of changed paths means for a release.
type Changes struct {
	Images    []string // changed paths that reach an image
	Fragments []string // fragments added or edited
	Charts    []string // charts with changes (their names)
	Demo      bool     // the demo smoke test has something new to prove
	problems  []string // charts changed without a new version
}

// Problems are the reasons CI fails the change.
func (c Changes) Problems() []string {
	problems := append([]string{}, c.problems...)
	if len(c.Images) > 0 && len(c.Fragments) == 0 {
		shown := c.Images
		more := ""
		if len(shown) > 5 {
			shown, more = shown[:5], ", ..."
		}
		problems = append(problems, fmt.Sprintf("these changes reach a published image, so they need a changelog fragment in %s (the next release ships them): %s%s",
			FragmentDir, strings.Join(shown, ", "), more))
	}
	if len(c.Fragments) > 0 && len(c.Images) == 0 {
		problems = append(problems, fmt.Sprintf("%s: nothing here reaches an image, so there is nothing to release - remove the fragment (charts take a version bump instead; docs, tests and CI ship without one)",
			strings.Join(c.Fragments, ", ")))
	}
	return problems
}

// Classify sorts changed paths. A changed chart must carry a version that is not published
// yet and is newer than every published one.
func Classify(root string, paths []string) (Changes, error) {
	var c Changes
	charts, err := ReadCharts(root)
	if err != nil {
		return c, err
	}
	changed := map[string]bool{}
	for _, p := range paths {
		reaches, err := ReachesImage(root, p)
		if err != nil {
			return c, err
		}
		if reaches {
			c.Images = append(c.Images, p)
		}
		for _, chart := range charts {
			if strings.HasPrefix(p, chart.Dir()) && !changed[chart.Name] {
				changed[chart.Name] = true
				c.Charts = append(c.Charts, chart.Name)
			}
		}
		rest := strings.TrimPrefix(p, FragmentDir)
		if strings.HasPrefix(p, FragmentDir) && !strings.Contains(rest, "/") && strings.HasSuffix(p, ".md") && !skipFiles[filepath.Base(p)] {
			if _, err := os.Stat(filepath.Join(root, p)); err == nil { // a deleted fragment releases nothing
				c.Fragments = append(c.Fragments, p)
			}
		}
		for _, d := range DemoPaths {
			if strings.HasPrefix(p, d) {
				c.Demo = true
			}
		}
	}
	for _, chart := range charts {
		if !changed[chart.Name] {
			continue
		}
		published, err := PublishedVersions(root, chart)
		if err != nil {
			return c, err
		}
		if len(published) > 0 && !newer(chart.Version, published[0]) {
			c.problems = append(c.problems, fmt.Sprintf("%s changed, but its version %s is already published or older than the published %s: bump version in %s (a breaking values change bumps the minor below 1.0, the major from 1.0)",
				chart.Name, chart.Version, published[0], chart.File))
		}
	}
	c.Demo = c.Demo || len(c.Images) > 0
	return c, nil
}

// ChangedPaths lists BASE...HEAD.
func ChangedPaths(root, base string) ([]string, error) {
	out, err := git(root, "diff", "--name-only", "--no-renames", base+"...HEAD")
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, line := range strings.Split(out, "\n") {
		if line != "" {
			paths = append(paths, line)
		}
	}
	return paths, nil
}

// ChartNotes are the release notes of a chart-only release: the subjects of the pull requests
// that changed the chart since its last published version (merge commits name the pull request
// in their body; squash merges carry it in the subject).
func ChartNotes(root string, c Chart) (string, error) {
	published, err := PublishedVersions(root, c)
	if err != nil {
		return "", err
	}
	args := []string{"log", "--first-parent", "--format=%s%x1f%b%x1e"}
	if len(published) > 0 {
		args = append(args, c.Tag(published[0])+"..HEAD")
	} else {
		args = append(args, "HEAD")
	}
	out, err := git(root, append(args, "--", strings.TrimSuffix(c.Dir(), "/"))...)
	if err != nil {
		return "", err
	}
	merge := regexp.MustCompile(`^Merge pull request (#\d+) from `)
	var lines []string
	for _, entry := range strings.Split(out, "\x1e") {
		subject, body, _ := strings.Cut(strings.TrimSpace(entry), "\x1f")
		if subject == "" || strings.HasPrefix(subject, "chore: release ") {
			continue
		}
		if m := merge.FindStringSubmatch(subject); m != nil {
			title, _, _ := strings.Cut(strings.TrimSpace(body), "\n")
			if title = strings.TrimSpace(title); title != "" {
				subject = title + " (" + m[1] + ")"
			}
		}
		lines = append(lines, "- "+subject)
	}
	if len(lines) == 0 {
		lines = []string{"- Chart maintenance"}
	}
	return fmt.Sprintf("Helm chart **%s %s** (installs BeziSync %s).\n\n### Changes\n%s\n", c.Name, c.Version, c.AppVersion, strings.Join(lines, "\n")), nil
}

// writeIndented is Python's json.dumps(value, indent=2, sort_keys=True) (ensure_ascii,
// separators "," and ": ") for decoded JSON values.
func writeIndented(b *strings.Builder, v any, depth int) error {
	pad := func(d int) string { return "\n" + strings.Repeat("  ", d) }
	switch x := v.(type) {
	case map[string]any:
		if len(x) == 0 {
			b.WriteString("{}")
			return nil
		}
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b.WriteString("{")
		for i, k := range keys {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString(pad(depth + 1))
			writeString(b, k)
			b.WriteString(": ")
			if err := writeIndented(b, x[k], depth+1); err != nil {
				return err
			}
		}
		b.WriteString(pad(depth) + "}")
	case []any:
		if len(x) == 0 {
			b.WriteString("[]")
			return nil
		}
		b.WriteString("[")
		for i, item := range x {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString(pad(depth + 1))
			if err := writeIndented(b, item, depth+1); err != nil {
				return err
			}
		}
		b.WriteString(pad(depth) + "]")
	case string:
		writeString(b, x)
	case json.Number:
		b.WriteString(x.String())
	case bool:
		b.WriteString(strconv.FormatBool(x))
	case nil:
		b.WriteString("null")
	default:
		return fail("unsupported JSON value %T", v)
	}
	return nil
}

func writeString(b *strings.Builder, s string) {
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		default:
			switch {
			case r < 0x20 || (r >= 0x7f && r <= 0xffff):
				fmt.Fprintf(b, `\u%04x`, r)
			case r > 0xffff:
				r -= 0x10000
				fmt.Fprintf(b, `\u%04x\u%04x`, 0xd800+(r>>10), 0xdc00+(r&0x3ff))
			default:
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
}
