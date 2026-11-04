package cli

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/spf13/cobra"
)

// Saved positions and Start over from the command line. Restore and start-over change where a
// pipeline resumes, so they need --yes (the API's typed confirmation is the pipeline's name).

func projectFlag(cmd *cobra.Command, project *string) {
	cmd.Flags().StringVarP(project, "project", "p", "", "Project slug (default: from login or BEZISYNC_PROJECT).")
}

// pipelinePath is the API path of the named pipeline.
func pipelinePath(project, name string, stderr io.Writer) (*Client, string, error) {
	c, err := remoteClient(project, stderr)
	if err != nil {
		return nil, "", err
	}
	base, err := c.Base()
	if err != nil {
		return nil, "", err
	}
	id, err := c.PipelineID(name)
	if err != nil {
		return nil, "", err
	}
	return c, base + "/pipelines/" + id, nil
}

func positionsCmd(project *string, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{Use: "positions NAME", Short: "List a Flink CDC pipeline's saved positions, newest first.", Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			c, path, err := pipelinePath(*project, args[0], stderr)
			if err != nil {
				return err
			}
			out, err := c.Call(http.MethodGet, path+"/positions", nil, nil)
			if err != nil {
				return err
			}
			m, _ := out.(map[string]any)
			list, _ := m["positions"].([]any)
			if len(list) == 0 {
				fmt.Fprintln(stdout, "No saved positions.")
				return nil
			}
			fmt.Fprintf(stdout, "%-8s %-11s %-25s %-21s %s\n", "ID", "KIND", "TAKEN", "REASON", "CURRENT")
			for _, x := range list {
				p, _ := x.(map[string]any)
				current := "set aside"
				if b, _ := p["current"].(bool); b {
					current = "yes"
				}
				fmt.Fprintf(stdout, "%-8s %-11s %-25s %-21s %s\n", str(p["id"]), str(p["kind"]), str(p["taken_at"]), str(p["reason"]), current)
			}
			return nil
		}}
	projectFlag(cmd, project)
	return cmd
}

func restoreCmd(project *string, stdout, stderr io.Writer) *cobra.Command {
	var yes bool
	cmd := &cobra.Command{Use: "restore NAME POSITION_ID", Short: "Make a paused Flink CDC pipeline resume from a saved position.", Args: cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			id, err := strconv.ParseInt(args[1], 10, 64)
			if err != nil || id <= 0 {
				return fmt.Errorf("POSITION_ID is a number (see `bezisync remote pipelines positions %s`)", args[0])
			}
			if !yes {
				return errors.New("changes after that position are read again: run again with --yes to restore it")
			}
			c, path, err := pipelinePath(*project, args[0], stderr)
			if err != nil {
				return err
			}
			out, err := c.Call(http.MethodPost, path+"/positions/"+args[1]+"/restore", nil, map[string]any{"confirm": args[0]})
			if err != nil {
				return err
			}
			m, _ := out.(map[string]any)
			p, _ := m["position"].(map[string]any)
			fmt.Fprintf(stdout, "%s resumes from position #%s (a copy of #%d) when you resume it.\n", args[0], str(p["id"]), id)
			return nil
		}}
	cmd.Flags().BoolVar(&yes, "yes", false, "Confirm: changes after the position are read again.")
	projectFlag(cmd, project)
	return cmd
}

func startOverCmd(project *string, stdout, stderr io.Writer) *cobra.Command {
	var yes, scratch, now bool
	var since string
	cmd := &cobra.Command{Use: "start-over NAME", Short: "Start a paused pipeline over: copy every table again, read from now, or read since a time.",
		Long: "Start a paused pipeline over. --from-scratch copies every table again; --from-now reads changes from now (changes in between are\n" +
			"never delivered); --since TIME (RFC 3339) reads changes since then (Flink CDC on MySQL or SQL Server). Needs --yes.",
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			chosen := 0
			body := map[string]any{"confirm": args[0]}
			if scratch {
				chosen++
				body["mode"] = "resnapshot"
			}
			if now {
				chosen++
				body["mode"] = "skip_to_now"
			}
			if since != "" {
				chosen++
				t, err := time.Parse(time.RFC3339, since)
				if err != nil {
					return fmt.Errorf("--since is an RFC 3339 time, e.g. 2026-11-04T08:00:00Z")
				}
				body["mode"], body["from_time"] = "from_time", t.UTC().Format(time.RFC3339)
			}
			if chosen != 1 {
				return errors.New("choose one of --from-scratch, --from-now or --since TIME")
			}
			if !yes {
				return errors.New("this changes where the pipeline resumes: run again with --yes")
			}
			c, path, err := pipelinePath(*project, args[0], stderr)
			if err != nil {
				return err
			}
			if _, err := c.Call(http.MethodPost, path+"/offsets/reset", nil, body); err != nil {
				return err
			}
			fmt.Fprintf(stdout, "%s starts over when you resume it.\n", args[0])
			return nil
		}}
	cmd.Flags().BoolVar(&scratch, "from-scratch", false, "Copy every table again.")
	cmd.Flags().BoolVar(&now, "from-now", false, "Read changes from now.")
	cmd.Flags().StringVar(&since, "since", "", "Read changes since this time (RFC 3339).")
	cmd.Flags().BoolVar(&yes, "yes", false, "Confirm.")
	projectFlag(cmd, project)
	return cmd
}
