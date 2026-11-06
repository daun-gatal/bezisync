// Command contracts checks the directory contracts (README.md per code directory and the root
// ARCHITECTURE.md) against the code; rules in .rules/egm.md:
//
//	go run ./cmd/contracts      exit 1 and list every problem, or print "contracts are current"
//
// Run from tools/ (or set BEZISYNC_ROOT). It needs git (it lists the repository's files).
package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/daun-gatal/bezisync/tools/contracts"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "contracts:", err)
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

func run() error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	problems, err := contracts.Check(root)
	if err != nil {
		return err
	}
	if len(problems) == 0 {
		fmt.Println("contracts are current")
		return nil
	}
	for _, p := range problems {
		fmt.Fprintln(os.Stderr, p)
	}
	return fmt.Errorf("%d problem(s); see .rules/egm.md (Directory contracts)", len(problems))
}
