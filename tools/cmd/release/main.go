// Command release assembles a BeziSync release from the changelog fragments.
//
//	go run ./cmd/release                 assemble: next version from the fragments (highest type wins)
//	go run ./cmd/release 2.0.0           assemble with an explicit version
//	go run ./cmd/release --dry-run       print the version a release would get (fails without fragments)
//	go run ./cmd/release --next          print the next version; a patch bump when there are no fragments
//	go run ./cmd/release --current       print the current version (backend, frontend and charts agree)
//	go run ./cmd/release --notes 1.2.0   print the CHANGELOG.md section of v1.2.0 (GitHub Release notes)
//	go run ./cmd/release --changes BASE  check BASE...HEAD: a fragment exactly when an image or chart changes
//
// Only changes that reach a published artifact release: an image's build context (its
// directory minus its .dockerignore) or a Helm chart. Such a change must carry a fragment and
// anything else must not, so every merge to main either releases or leaves the published
// artifacts untouched. Run from tools/ (or set BEZISYNC_ROOT); it works on the repository root.
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

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("release", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dryRun := fs.Bool("dry-run", false, "print the release version only")
	next := fs.Bool("next", false, "print the next version, even unreleased")
	current := fs.Bool("current", false, "print the current version")
	notes := fs.String("notes", "", "print the changelog section of `VERSION`")
	changes := fs.String("changes", "", "check the changes since `BASE`")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	modes := 0
	for _, on := range []bool{*dryRun, *next, *current, *notes != "", *changes != ""} {
		if on {
			modes++
		}
	}
	if modes > 1 || fs.NArg() > 1 {
		fmt.Fprintln(stderr, "release: choose one of --dry-run, --next, --current, --notes, --changes")
		return 2
	}
	override := fs.Arg(0)
	root, err := repoRoot()
	if err == nil {
		root, err = filepath.Abs(root)
	}
	if err != nil {
		fmt.Fprintf(stderr, "release: %v\n", err)
		return 1
	}
	code, err := dispatch(root, override, *dryRun, *next, *current, *notes, *changes, stdout, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "release: %v\n", err)
		return 1
	}
	return code
}

func dispatch(root, override string, dryRun, next, current bool, notes, changes string, stdout, stderr io.Writer) (int, error) {
	switch {
	case changes != "":
		paths, err := release.ChangedPaths(root, changes)
		if err != nil {
			return 0, err
		}
		c, err := release.Classify(root, paths)
		if err != nil {
			return 0, err
		}
		fmt.Fprintf(stdout, "build=%t\ndemo=%t\n", len(c.Artifacts) > 0, c.Demo)
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
	case notes != "":
		text, err := release.Notes(root, notes)
		if err != nil {
			return 0, err
		}
		fmt.Fprint(stdout, text)
		return 0, nil
	case current:
		v, err := release.ReadVersion(root)
		if err != nil {
			return 0, err
		}
		fmt.Fprintln(stdout, v)
		return 0, nil
	case dryRun || next:
		fragments, err := release.ParseFragments(root)
		if err != nil {
			return 0, err
		}
		if dryRun && len(fragments) == 0 {
			return 0, &release.Error{Msg: "no changelog fragments - nothing to release"}
		}
		cur, err := release.ReadVersion(root)
		if err != nil {
			return 0, err
		}
		v, err := release.TargetVersion(cur, fragments, override)
		if err != nil {
			return 0, err
		}
		fmt.Fprintln(stdout, v)
		return 0, nil
	}
	v, err := release.Assemble(root, override, time.Now())
	if err != nil {
		return 0, err
	}
	fmt.Fprintf(stdout, "Release v%s assembled: %s, CHANGELOG.md\n", v, strings.Join(release.VersionFiles, ", "))
	return 0, nil
}
