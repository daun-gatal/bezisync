package services

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/version"
)

// Opt-in, anonymous usage statistics: aggregate counts only, at most weekly.

const telemetryInterval = 7 * 24 * time.Hour

func instanceState(ctx context.Context, q db.Querier, key string) (*models.InstanceState, error) {
	row, err := db.Find[models.InstanceState](ctx, q, `SELECT * FROM instance_state WHERE key = $1`, key)
	if err != nil || row != nil {
		return row, err
	}
	row = &models.InstanceState{Key: key, Value: db.Map{}, UpdatedAt: db.Now()}
	return row, db.Insert(ctx, q, "instance_state", row)
}

func saveInstanceState(ctx context.Context, q db.Querier, row *models.InstanceState) error {
	row.UpdatedAt = db.Now()
	return db.Update(ctx, q, "instance_state", row, "key")
}

// InstallID is the random id of this install (created on first use).
func InstallID(ctx context.Context, q db.Querier) (string, error) {
	row, err := instanceState(ctx, q, "install")
	if err != nil {
		return "", err
	}
	if _, ok := row.Value["id"]; !ok {
		row.Value["id"] = db.NewID().String()
		if err := saveInstanceState(ctx, q, row); err != nil {
			return "", err
		}
	}
	return fmt.Sprint(row.Value["id"]), nil
}

func counter() map[string]int { return map[string]int{} }

// TelemetryPayload is exactly what would be sent.
func TelemetryPayload(ctx context.Context, q db.Querier) (map[string]any, error) {
	live, err := db.Select[models.Pipeline](ctx, q, `SELECT * FROM pipelines WHERE deleted_at IS NULL`)
	if err != nil {
		return nil, err
	}
	conns, err := db.Select[models.Connection](ctx, q, `SELECT * FROM connections`)
	if err != nil {
		return nil, err
	}
	typeOf := map[string]string{}
	for _, c := range conns {
		typeOf[c.ID.String()] = c.Type
	}
	failing := counter()
	for _, c := range conns {
		list, _ := c.LastCheck["checks"].([]any)
		for _, chk := range list {
			m, _ := chk.(map[string]any)
			if m["status"] == "fail" {
				failing[fmt.Sprint(m["code"])]++
			}
		}
	}
	projects, err := db.Select[models.Project](ctx, q, `SELECT * FROM projects`)
	if err != nil {
		return nil, err
	}
	users, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM users`)
	if err != nil {
		return nil, err
	}
	installID, err := InstallID(ctx, q)
	if err != nil {
		return nil, err
	}
	byLifecycle, bySource, bySink := counter(), counter(), counter()
	for _, p := range live {
		byLifecycle[p.Lifecycle]++
		bySource[typeOf[p.SourceID.String()]]++
		if p.SinkID != nil {
			bySink[typeOf[p.SinkID.String()]]++
		}
	}
	rts, err := db.Select[models.Runtime](ctx, q, `SELECT * FROM runtimes`)
	if err != nil {
		return nil, err
	}
	runtimes := counter()
	for _, r := range rts {
		runtimes[r.Kind+"/"+r.Mode]++
	}
	connections := counter()
	for _, c := range conns {
		connections[c.Kind+"/"+c.Type]++
	}
	approvals, policies, sandbox := 0, 0, false
	for _, p := range projects {
		if truthyAny(p.Policies["require_approval"]) {
			approvals++
		}
		if len(p.Policies) > 0 {
			policies++
		}
		sandbox = sandbox || p.IsSandbox
	}
	templates, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM pipeline_templates`)
	if err != nil {
		return nil, err
	}
	stores, err := db.Select[models.SecretStore](ctx, q, `SELECT * FROM secret_stores`)
	if err != nil {
		return nil, err
	}
	storeKinds := counter()
	for _, s := range stores {
		storeKinds[s.Kind]++
	}
	sso, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM sso_providers WHERE enabled`)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"schema": 1, "install_id": installID, "version": version.Version, "projects": len(projects), "users": users,
		"pipelines": map[string]any{"total": len(live), "by_lifecycle": byLifecycle, "by_source": bySource, "by_sink": bySink},
		"runtimes":  runtimes, "connections": connections, "failing_checks": failing,
		"features": map[string]any{"sso": sso > 0, "approvals": approvals, "policies": policies, "templates": templates,
			"secret_stores": storeKinds, "sandbox": sandbox},
	}, nil
}

// TelemetryLastSent is when the last report was sent.
func TelemetryLastSent(ctx context.Context, q db.Querier) (*time.Time, error) {
	row, err := db.Find[models.InstanceState](ctx, q, `SELECT * FROM instance_state WHERE key = 'telemetry'`)
	if err != nil || row == nil {
		return nil, err
	}
	s, ok := row.Value["last_sent_at"].(string)
	if !ok || s == "" {
		return nil, nil
	}
	t, err := pytime.Parse(s)
	if err != nil {
		return nil, nil //nolint:nilerr // an unreadable timestamp counts as never sent
	}
	return &t, nil
}

// MaybeSendTelemetry sends the weekly report when enabled.
func MaybeSendTelemetry(ctx context.Context, q db.Querier, now time.Time) (bool, error) {
	s := config.Get()
	if !s.TelemetryEnabled || s.TelemetryURL == "" {
		return false, nil
	}
	previous, err := TelemetryLastSent(ctx, q)
	if err != nil {
		return false, err
	}
	if previous != nil && now.Sub(*previous) < telemetryInterval {
		return false, nil
	}
	payload, err := TelemetryPayload(ctx, q)
	if err != nil {
		return false, err
	}
	body, err := jsonMarshal(payload)
	if err != nil {
		return false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.TelemetryURL, bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err == nil {
		_ = resp.Body.Close()
		if resp.StatusCode >= 400 {
			err = fmt.Errorf("HTTP %d", resp.StatusCode)
		}
	}
	if err != nil {
		slog.Info("telemetry not sent: "+err.Error(), "logger", "bezisync.services.telemetry")
		return false, nil //nolint:nilerr // a failed delivery is logged and retried next week
	}
	row, err := instanceState(ctx, q, "telemetry")
	if err != nil {
		return false, err
	}
	row.Value["last_sent_at"] = pytime.ISOFormat(now)
	return true, saveInstanceState(ctx, q, row)
}
