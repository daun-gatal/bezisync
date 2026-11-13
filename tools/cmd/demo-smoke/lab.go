package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/daun-gatal/bezisync/tools/apiclient"
)

// labConnections are the Sandbox's connections in the lab (ADR 0018), by name.
var labConnections = []string{"demo-http", "demo-kafka", "demo-mysql", "demo-shop", "demo-warehouse", "legacy-postgres"}

// labTeam are the lab's teammates and the role each holds in the Sandbox.
var labTeam = map[string]string{"editor@bezisync.local": "editor", "operator@bezisync.local": "operator", "viewer@bezisync.local": "viewer"}

type named struct{ ID, Name string }

// runName makes a pipeline name unique to this run, so a re-run never collides with a pipeline an
// interrupted run left behind.
func runName(name string) string { return fmt.Sprintf("%s-%d", name, time.Now().Unix()%100000) }

// sandbox signs in and returns the client and the Sandbox project's API prefix.
func sandbox(ctx context.Context, base, email, password string) (*apiclient.Client, string, string, error) {
	api, err := apiclient.New(base)
	if err != nil {
		return nil, "", "", err
	}
	if err := api.Login(ctx, email, password); err != nil {
		return nil, "", "", fmt.Errorf("%s can't sign in: %w", email, err)
	}
	var me struct {
		Projects []struct {
			ID        string `json:"id"`
			Slug      string `json:"slug"`
			IsSandbox bool   `json:"is_sandbox"`
			Role      string `json:"role"`
		} `json:"projects"`
	}
	if err := api.Call(ctx, http.MethodGet, "/auth/me", nil, &me); err != nil {
		return nil, "", "", err
	}
	for _, p := range me.Projects {
		if p.Slug == "sandbox" && p.IsSandbox {
			return api, "/projects/" + p.ID, p.Role, nil
		}
	}
	return nil, "", "", fmt.Errorf("%s sees no sandbox project", email)
}

func idOf(items []named, name string) string {
	for _, it := range items {
		if it.Name == name {
			return it.ID
		}
	}
	return ""
}

// runLab checks the Codespaces lab: every Sandbox extra, the teammates' roles, preflight on the
// source that isn't ready, and pipelines that stream into Kafka and into the warehouse.
func runLab(ctx context.Context, base, adminPassword, teamPassword string) error {
	if adminPassword == "" || teamPassword == "" {
		return errors.New("set BEZISYNC_SMOKE_ADMIN_PASSWORD and BEZISYNC_SMOKE_TEAM_PASSWORD")
	}
	admin, prefix, _, err := sandbox(ctx, base, "admin@bezisync.local", adminPassword)
	if err != nil {
		return err
	}
	var conns []named
	if err := admin.Call(ctx, http.MethodGet, prefix+"/connections", nil, &conns); err != nil {
		return err
	}
	var names []string
	for _, c := range conns {
		names = append(names, c.Name)
	}
	slices.Sort(names)
	if !slices.Equal(names, labConnections) {
		return fmt.Errorf("sandbox connections %v, want %v", names, labConnections)
	}
	var templates []named
	if err := admin.Call(ctx, http.MethodGet, prefix+"/templates", nil, &templates); err != nil {
		return err
	}
	if len(templates) != 3 {
		return fmt.Errorf("expected 3 sandbox templates, found %d", len(templates))
	}
	fmt.Println("[lab] sandbox has its 6 connections and 3 templates")

	editor, err := checkTeam(ctx, base, prefix, teamPassword, conns, templates)
	if err != nil {
		return err
	}
	if err := checkLegacyPreflight(ctx, admin, prefix, conns, templates); err != nil {
		return err
	}
	// The editor deploys both pipelines: the role that is meant to.
	if err := streamTo(ctx, editor, prefix, "lab-orders-kafka", idOf(conns, "demo-shop"), idOf(templates, "demo-shop-to-kafka"), "shop.orders", true); err != nil {
		return err
	}
	return streamTo(ctx, editor, prefix, "lab-orders-warehouse", idOf(conns, "demo-mysql"), idOf(templates, "demo-shop-to-warehouse"), "shop.orders", false)
}

// checkTeam signs each teammate in and checks its role; a viewer may not create a pipeline.
func checkTeam(ctx context.Context, base, prefix, password string, conns, templates []named) (*apiclient.Client, error) {
	var editor *apiclient.Client
	for email, want := range labTeam {
		c, _, role, err := sandbox(ctx, base, email, password)
		if err != nil {
			return nil, err
		}
		if role != want {
			return nil, fmt.Errorf("%s has role %q, want %q", email, role, want)
		}
		if want == "editor" {
			editor = c
		}
		if want == "viewer" {
			err := c.Call(ctx, http.MethodPost, prefix+"/pipelines", map[string]any{"name": "viewer-try", "source_id": idOf(conns, "demo-shop"),
				"template_id": idOf(templates, "demo-shop-to-kafka")}, nil)
			var apiErr *apiclient.Error
			if !errors.As(err, &apiErr) || apiErr.Status != http.StatusForbidden {
				return nil, fmt.Errorf("a viewer creating a pipeline: got %w, want 403", err)
			}
		}
	}
	fmt.Println("[lab] editor, operator and viewer sign in with their roles; the viewer can't create a pipeline")
	return editor, nil
}

// checkLegacyPreflight: preflight on legacy-postgres fails and names wal_level as the fix.
func checkLegacyPreflight(ctx context.Context, api *apiclient.Client, prefix string, conns, templates []named) error {
	var created struct{ ID string }
	if err := api.Call(ctx, http.MethodPost, prefix+"/pipelines", map[string]any{"name": runName("lab-legacy-check"), "source_id": idOf(conns, "legacy-postgres"),
		"template_id": idOf(templates, "demo-shop-to-kafka"),
		"spec":        map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"billing.invoices"}}}}}, &created); err != nil {
		return fmt.Errorf("create a pipeline from legacy-postgres: %w", err)
	}
	u := prefix + "/pipelines/" + created.ID
	defer func() { _ = api.Call(context.WithoutCancel(ctx), http.MethodDelete, u, nil, nil) }()
	var report struct {
		OK       bool `json:"ok"`
		Sections []struct {
			Report struct {
				Checks []struct {
					Code, Status, Technical string
					Fix                     any `json:"fix"`
				} `json:"checks"`
			} `json:"report"`
		} `json:"sections"`
	}
	if err := api.Call(ctx, http.MethodPost, u+"/preflight", map[string]any{}, &report); err != nil {
		return fmt.Errorf("preflight on legacy-postgres: %w", err)
	}
	found := false
	var failed []string
	for _, s := range report.Sections {
		for _, c := range s.Report.Checks {
			if c.Status != "fail" {
				continue
			}
			failed = append(failed, c.Code)
			if c.Code == "pg_wal_level" && c.Fix != nil && strings.Contains(fmt.Sprint(c.Fix), "wal_level = logical") {
				found = true
			}
		}
	}
	if report.OK || !found {
		return fmt.Errorf("preflight on legacy-postgres: ok=%v, no failed check with a wal_level fix among %v", report.OK, failed)
	}
	fmt.Println("[lab] preflight on legacy-postgres fails with the wal_level fix")
	return nil
}

// streamTo deploys a pipeline from a template and waits until it streams healthily; for Kafka it
// also reads order events back. It deletes the pipeline afterwards.
func streamTo(ctx context.Context, api *apiclient.Client, prefix, name, sourceID, templateID, table string, readBack bool) error {
	if sourceID == "" || templateID == "" {
		return fmt.Errorf("%s: source or template missing", name)
	}
	var created struct{ ID string }
	name = runName(name)
	if err := api.Call(ctx, http.MethodPost, prefix+"/pipelines", map[string]any{"name": name, "source_id": sourceID, "template_id": templateID,
		"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{table}}}}}, &created); err != nil {
		return fmt.Errorf("%s: create: %w", name, err)
	}
	u := prefix + "/pipelines/" + created.ID
	deleted := false
	defer func() {
		if !deleted { // a failed check still leaves no pipeline behind
			_ = api.Call(context.WithoutCancel(ctx), http.MethodDelete, u, nil, nil)
		}
	}()
	if err := api.Call(ctx, http.MethodPost, u+"/deploy", map[string]any{}, nil); err != nil {
		return fmt.Errorf("%s: deploy refused: %w", name, err)
	}
	var cur pipeline
	err := wait(name+" streaming", 15*time.Minute, func() (bool, error) {
		if err := api.Call(ctx, http.MethodGet, u, nil, &cur); err != nil {
			return false, err
		}
		if cur.Lifecycle == "failed" {
			return false, fmt.Errorf("%s failed: %s", name, cur.Status)
		}
		return cur.Lifecycle == "streaming" && cur.Health == "healthy", nil
	}, func() string {
		return fmt.Sprintf("lifecycle %s, health %s, status %s", cur.Lifecycle, cur.Health, cur.Status)
	})
	if err != nil {
		return err
	}
	if readBack {
		query := url.Values{"table": {table}, "limit": {"20"}}.Encode()
		err = wait(name+" events", 3*time.Minute, func() (bool, error) {
			var events struct {
				Records []any `json:"records"`
			}
			if err := api.Call(ctx, http.MethodGet, u+"/events?"+query, nil, &events); err != nil {
				return false, nil //nolint:nilerr // the destination may not be readable yet: keep polling
			}
			return len(events.Records) > 0, nil
		}, nil)
		if err != nil {
			return err
		}
	}
	fmt.Printf("[lab] %s streams healthily\n", name)
	if err := api.Call(ctx, http.MethodDelete, u, nil, nil); err != nil {
		return err
	}
	deleted = true
	return wait(name+" cleanup", 4*time.Minute, func() (bool, error) {
		err := api.Call(ctx, http.MethodGet, u, nil, &cur)
		return err == nil && cur.Lifecycle == "deleted", err
	}, nil)
}
