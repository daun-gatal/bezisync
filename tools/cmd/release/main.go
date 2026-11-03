// Command release plans and assembles BeziSync releases.
//
//	go run ./cmd/release                    assemble an app release: next version from the fragments
//	go run ./cmd/release 2.0.0              assemble with an explicit app version
//	go run ./cmd/release --plan [2.0.0]     what a release publishes now (GITHUB_OUTPUT lines):
//	                                        kind=app|charts|none, version, charts, pre_charts
//	go run ./cmd/release --dry-run          print the app version a release would get (fails without fragments)
//	go run ./cmd/release --next             print the next app version; a patch bump without fragments
//	go run ./cmd/release --current          print the current app version (backend, frontend and charts agree)
//	go run ./cmd/release --notes 1.2.0      print the CHANGELOG.md section of v1.2.0 (GitHub Release notes)
//	go run ./cmd/release --chart-notes NAME print the notes of a chart-only release of chart NAME
//	go run ./cmd/release --changes BASE     check BASE...HEAD: a fragment exactly when an image changes,
//	                                        and a new version for every changed chart
//
// The app version (images, CLI, every chart's appVersion) comes from the changelog fragments,
// plain semver, highest type wins. Each chart has its own version, bumped by the change that
// edits the chart; a version counts as published once its chart-<name>-v<version> tag exists.
// Run from tools/ (or set BEZISYNC_ROOT); it works on the repository root.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/daun-gatal/bezisync/tools/release"
)

func repoRoot() (string, error) {
	if root := os.Getenv("BEZISYNC_ROOT"); root != "" {
		return root, nil
	}
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output() //nolint:noctx // a quick local command
	if err != nil {
		return "", errors.New("not inside the repository (set BEZISYNC_ROOT)")
	}
	return strings.TrimSpace(string(out)), nil
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

type options struct {
	plan, dryRun, next, current bool
	notes, chartNotes, changes  string
	override                    string
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("release", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var o options
	fs.BoolVar(&o.plan, "plan", false, "print what a release publishes now")
	fs.BoolVar(&o.dryRun, "dry-run", false, "print the release version only")
	fs.BoolVar(&o.next, "next", false, "print the next version, even unreleased")
	fs.BoolVar(&o.current, "current", false, "print the current version")
	fs.StringVar(&o.notes, "notes", "", "print the changelog section of `VERSION`")
	fs.StringVar(&o.chartNotes, "chart-notes", "", "print the notes of a chart-only release of chart `NAME`")
	fs.StringVar(&o.changes, "changes", "", "check the changes since `BASE`")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	modes := 0
	for _, on := range []bool{o.plan, o.dryRun, o.next, o.current, o.notes != "", o.chartNotes != "", o.changes != ""} {
		if on {
			modes++
		}
	}
	if modes > 1 || fs.NArg() > 1 {
		fmt.Fprintln(stderr, "release: choose one of --plan, --dry-run, --next, --current, --notes, --chart-notes, --changes")
		return 2
	}
	o.override = fs.Arg(0)
	root, err := repoRoot()
	if err == nil {
		root, err = filepath.Abs(root)
	}
	if err != nil {
		fmt.Fprintf(stderr, "release: %v\n", err)
		return 1
	}
	code, err := dispatch(root, o, stdout, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "release: %v\n", err)
		return 1
	}
	return code
}

func list(charts []release.ChartRelease) string {
	parts := make([]string, 0, len(charts))
	for _, c := range charts {
		parts = append(parts, c.Name+"="+c.Version)
	}
	return strings.Join(parts, " ")
}

func dispatch(root string, o options, stdout, stderr io.Writer) (int, error) {
	switch {
	case o.changes != "":
		return changes(root, o.changes, stdout, stderr)
	case o.plan:
		p, err := release.MakePlan(root, o.override)
		if err != nil {
			return 0, err
		}
		fmt.Fprintf(stdout, "kind=%s\nversion=%s\ncharts=%s\npre_charts=%s\nagent_chart=%s\n", p.Kind, p.Version, list(p.Charts), list(p.PreCharts), p.AgentChart)
		return 0, nil
	case o.notes != "":
		text, err := release.Notes(root, o.notes)
		if err != nil {
			return 0, err
		}
		fmt.Fprint(stdout, text)
		return 0, nil
	case o.chartNotes != "":
		charts, err := release.ReadCharts(root)
		if err != nil {
			return 0, err
		}
		for _, c := range charts {
			if c.Name == o.chartNotes {
				text, err := release.ChartNotes(root, c)
				if err != nil {
					return 0, err
				}
				fmt.Fprint(stdout, text)
				return 0, nil
			}
		}
		return 0, &release.Error{Msg: "no chart named " + o.chartNotes}
	case o.current:
		v, err := release.ReadVersion(root)
		if err != nil {
			return 0, err
		}
		fmt.Fprintln(stdout, v)
		return 0, nil
	case o.dryRun || o.next:
		fragments, err := release.ParseFragments(root)
		if err != nil {
			return 0, err
		}
		if o.dryRun && len(fragments) == 0 {
			return 0, &release.Error{Msg: "no changelog fragments - nothing to release"}
		}
		cur, err := release.ReadVersion(root)
		if err != nil {
			return 0, err
		}
		v, err := release.TargetVersion(cur, fragments, o.override)
		if err != nil {
			return 0, err
		}
		fmt.Fprintln(stdout, v)
		return 0, nil
	}
	v, err := release.Assemble(root, o.override, time.Now())
	if err != nil {
		return 0, err
	}
	fmt.Fprintf(stdout, "Release v%s assembled: %s, CHANGELOG.md\n", v, strings.Join(release.VersionFiles, ", "))
	return 0, nil
}

func changes(root, base string, stdout, stderr io.Writer) (int, error) {
	paths, err := release.ChangedPaths(root, base)
	if err != nil {
		return 0, err
	}
	c, err := release.Classify(root, paths)
	if err != nil {
		return 0, err
	}
	fmt.Fprintf(stdout, "build=%t\ndemo=%t\ncharts=%t\n", len(c.Images) > 0, c.Demo, len(c.Charts) > 0)
	prefix := "release: "
	if os.Getenv("GITHUB_ACTIONS") == "true" {
		prefix = "::error::"
	}
	for _, p := range c.Problems() {
		fmt.Fprintln(stderr, prefix+p)
	}
	if len(c.Problems()) > 0 {
		return 1, nil
	}
	return 0, nil
}
