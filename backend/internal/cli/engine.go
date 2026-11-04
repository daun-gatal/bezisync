package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/daun-gatal/bezisync/backend/internal/flinkhelper"
)

// engineCmd is the in-container side of the engines: `bezisync engine flink-cdc jobmanager|taskmanager`.
func engineCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "engine", Short: "Run inside an engine's containers (BeziSync starts it; not for people).", Hidden: true}
	flink := &cobra.Command{Use: "flink-cdc ROLE", Short: "Run a Flink CDC pipeline's JobManager or a TaskManager.",
		Args: cobra.ExactArgs(1), ValidArgs: []string{"jobmanager", "taskmanager"},
		RunE: func(_ *cobra.Command, args []string) error {
			layout := flinkhelper.DefaultLayout()
			switch args[0] {
			case "jobmanager":
				return flinkhelper.RunJobManager(layout)
			case "taskmanager":
				return flinkhelper.RunTaskManager(layout)
			}
			return fmt.Errorf("unknown role %q: jobmanager or taskmanager", args[0])
		}}
	cmd.AddCommand(flink)
	return cmd
}

// installSelfCmd copies this binary into a directory (init containers inject the helper this way).
func installSelfCmd() *cobra.Command {
	return &cobra.Command{Use: "install-self DIR", Short: "Copy this binary to DIR/bezisync (engine containers use it).", Hidden: true,
		Args: cobra.ExactArgs(1), RunE: func(_ *cobra.Command, args []string) error { return flinkhelper.InstallSelf(args[0]) }}
}
