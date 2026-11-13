// Command demo-smoke drives the demo stack's sandbox through the API (used by
// scripts/demo-smoke.sh): create a pipeline from the sandbox template, wait until it streams,
// read live orders back from Kafka, then delete it. With -lab it checks the Codespaces lab
// instead (scripts/lab-smoke.sh, ADR 0018): the Sandbox extras, the teammates' roles,
// preflight on the source that isn't ready, and streams into Kafka and the warehouse.
//
//	go run ./cmd/demo-smoke http://localhost:28080
//	BEZISYNC_SMOKE_ADMIN_PASSWORD=… BEZISYNC_SMOKE_TEAM_PASSWORD=… go run ./cmd/demo-smoke -lab http://localhost:28080
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/daun-gatal/bezisync/tools/apiclient"
)

type pipeline struct {
	Lifecycle string          `json:"lifecycle"`
	Health    string          `json:"health"`
	Status    json.RawMessage `json:"status"`
}

func main() {
	lab := flag.Bool("lab", false, "check the Codespaces lab instead of the demo")
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: demo-smoke [-lab] URL")
		os.Exit(2)
	}
	var err error
	if *lab {
		err = runLab(context.Background(), flag.Arg(0), os.Getenv("BEZISYNC_SMOKE_ADMIN_PASSWORD"), os.Getenv("BEZISYNC_SMOKE_TEAM_PASSWORD"))
	} else {
		err = run(context.Background(), flag.Arg(0))
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "[demo] %v\n", err)
		os.Exit(1)
	}
}

// wait polls fn every 3 s until it reports done or the timeout passes.
func wait(what string, timeout time.Duration, fn func() (bool, error), describe func() string) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		done, err := fn()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		time.Sleep(3 * time.Second)
	}
	detail := ""
	if describe != nil {
		detail = ": " + describe()
	}
	return fmt.Errorf("timed out waiting for %s%s", what, detail)
}

func run(ctx context.Context, base string) error {
	api, err := apiclient.New(base)
	if err != nil {
		return err
	}
	if err := api.Login(ctx, "admin@bezisync.local", "bezisync-demo"); err != nil {
		return err
	}
	var me struct {
		Projects []struct {
			ID        string `json:"id"`
			Slug      string `json:"slug"`
			IsSandbox bool   `json:"is_sandbox"`
		} `json:"projects"`
	}
	if err := api.Call(ctx, http.MethodGet, "/auth/me", nil, &me); err != nil {
		return err
	}
	projectID := ""
	for _, p := range me.Projects {
		if p.Slug == "sandbox" && p.IsSandbox {
			projectID = p.ID
		}
	}
	if projectID == "" {
		return errors.New("no sandbox project")
	}
	prefix := "/projects/" + projectID
	var templates []struct{ ID string }
	if err := api.Call(ctx, http.MethodGet, prefix+"/templates", nil, &templates); err != nil {
		return err
	}
	if len(templates) != 1 {
		return fmt.Errorf("expected one sandbox template, found %d", len(templates))
	}
	var conns []struct{ ID, Name string }
	if err := api.Call(ctx, http.MethodGet, prefix+"/connections", nil, &conns); err != nil {
		return err
	}
	source := ""
	for _, c := range conns {
		if c.Name == "demo-shop" {
			source = c.ID
		}
	}
	var created struct{ ID string }
	if err := api.Call(ctx, http.MethodPost, prefix+"/pipelines", map[string]any{"name": "orders-demo", "source_id": source, "template_id": templates[0].ID,
		"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders"}}}}}, &created); err != nil {
		return err
	}
	u := prefix + "/pipelines/" + created.ID
	if err := api.Call(ctx, http.MethodPost, u+"/deploy", map[string]any{}, nil); err != nil {
		return fmt.Errorf("deploy refused: %w", err)
	}
	fmt.Println("[demo] deployed; waiting for streaming (the first run pulls Debezium Server)")
	var cur pipeline
	err = wait("streaming", 15*time.Minute, func() (bool, error) {
		if err := api.Call(ctx, http.MethodGet, u, nil, &cur); err != nil {
			return false, err
		}
		if cur.Lifecycle == "failed" {
			return false, fmt.Errorf("pipeline failed: %s", cur.Status)
		}
		return cur.Lifecycle == "streaming" && cur.Health == "healthy", nil
	}, func() string {
		return fmt.Sprintf("lifecycle %s, health %s, status %s", cur.Lifecycle, cur.Health, cur.Status)
	})
	if err != nil {
		return err
	}
	count := 0
	query := url.Values{"table": {"shop.orders"}, "limit": {"50"}}.Encode()
	err = wait("order events in Kafka", 3*time.Minute, func() (bool, error) {
		var events struct {
			Records []json.RawMessage `json:"records"`
		}
		if err := api.Call(ctx, http.MethodGet, u+"/events?"+query, nil, &events); err != nil {
			return false, nil //nolint:nilerr // the destination may not be readable yet: keep polling
		}
		count = len(events.Records)
		return count > 0, nil
	}, nil)
	if err != nil {
		return err
	}
	fmt.Printf("[demo] %d order events readable from Kafka\n", count)
	if err := api.Call(ctx, http.MethodDelete, u, nil, nil); err != nil {
		return err
	}
	return wait("cleanup", 4*time.Minute, func() (bool, error) {
		err := api.Call(ctx, http.MethodGet, u, nil, &cur)
		return err == nil && cur.Lifecycle == "deleted", err
	}, nil)
}
