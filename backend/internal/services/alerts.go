package services

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/smtp"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/security"
	"github.com/daun-gatal/bezisync/backend/internal/ssrf"
)

// Alert rules: evaluation (pending → firing → resolved), delivery and the inbox.

// Condition is the result of checking one rule against one pipeline.
type Condition struct {
	Active              bool
	Title, Message, Fix string
	Severity            string // overrides the rule's severity when set
}

// RuleKind describes an alert rule kind.
type RuleKind struct {
	Kind              string         `json:"kind"`
	Title             string         `json:"title"`
	Description       string         `json:"description"`
	DefaultSeverity   string         `json:"default_severity"`
	DefaultForSeconds int            `json:"default_for_seconds"`
	Params            map[string]any `json:"params"`
}

// RuleKinds in display order.
var RuleKinds = []RuleKind{
	{"pipeline_down", "Pipeline down", "The connector failed or keeps restarting.", "critical", 120, map[string]any{}},
	{"latency", "Latency above target", "Changes arrive later than the pipeline's latency target.", "warning", 300, map[string]any{"threshold_seconds": nil}},
	{"source_at_risk", "Source at risk", "The source retains too much log for the pipeline, or its slot is about to be invalidated.", "critical", 60, map[string]any{}},
	{"status_unknown", "Status unknown", "BeziSync has no fresh evidence about a pipeline that should be running.", "warning", 300, map[string]any{}},
	{"position_regressed", "Position moved backwards", "The stored position went backwards without a reset: events may be delivered twice.", "warning", 0, map[string]any{}},
	{"delivery_unverified", "Delivery not verified", "A delivery canary written to the source did not reach the destination in time.", "critical", 0, map[string]any{}},
	{"schema_breaking", "Breaking schema change", "A captured table dropped a column, changed a column's type, or disappeared.", "warning", 0, map[string]any{}},
	{"transform_drift", "Transform needs attention", "After a schema change, a transform step refers to a column that is gone, a filter rule can no longer match, or two fields would get one name.", "warning", 0, map[string]any{}},
	{"cluster_shrink_failed", "Cluster stopped shrinking", "A custom Flink CDC cluster failed on its smaller streaming shape several times, so BeziSync keeps it at its copy size.", "warning", 0, map[string]any{}},
}

// RuleKindByName finds a rule kind.
func RuleKindByName(kind string) (RuleKind, bool) {
	for _, k := range RuleKinds {
		if k.Kind == kind {
			return k, true
		}
	}
	return RuleKind{}, false
}

func mapOf(v any) map[string]any {
	m, _ := v.(map[string]any)
	if m == nil {
		return map[string]any{}
	}
	return m
}

func strOf(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	}
	return pyFormat(v)
}

func orStr(a any, b string) string {
	if truthyAny(a) {
		return strOf(a)
	}
	return b
}

// Evaluate checks one rule kind against a pipeline's observed status.
func Evaluate(kind string, p *models.Pipeline, params map[string]any, now time.Time) Condition {
	status := map[string]any(p.Status)
	if status == nil {
		status = map[string]any{}
	}
	running := p.DesiredState == "running" && p.DeployedVersionID != nil
	reasons, _ := status["reasons"].([]any)
	first := map[string]any{}
	if len(reasons) > 0 {
		first = mapOf(reasons[0])
	}
	summary := ""
	if v, ok := status["summary"]; ok {
		summary = strOf(v)
	}
	switch kind {
	case "pipeline_down":
		if running && (p.Lifecycle == "failed" || p.Health == "down") {
			return Condition{true, p.Name + " is down", orStr(first["message"], summary), strOf(first["fix"]), ""}
		}
	case "latency":
		lagV, ok := mapOf(status["metrics"])["ms_behind_source"]
		threshold := params["threshold_seconds"]
		if threshold == nil {
			threshold = status["latency_target_seconds"]
			if !truthyAny(threshold) {
				threshold = 60
			}
		}
		if running && ok && lagV != nil {
			lag := toFloat(lagV)
			th := toFloat(threshold)
			if lag/1000 > th {
				return Condition{true, fmt.Sprintf("%s is %s s behind the source", p.Name, domain.FormatFixed(lag/1000, 0)),
					fmt.Sprintf("Latency target is %s s.", domain.FormatFixed(th, 0)),
					"Check the destination's throughput and the connector's queue, or choose a larger size.", ""}
			}
		}
	case "source_at_risk":
		impact := mapOf(status["source_impact"])
		if p.DeployedVersionID != nil && truthyAny(impact["risk"]) {
			sev := ""
			if truthyAny(impact["lost"]) {
				sev = "critical"
			}
			return Condition{true, p.Name + " puts its source at risk", strOf(impact["risk"]), strOf(impact["risk_fix"]), sev}
		}
	case "status_unknown":
		asOf, _ := status["as_of"].(string)
		stale := asOf == ""
		if asOf != "" {
			if t, err := pytime.Parse(asOf); err == nil {
				stale = now.Sub(t) > 3*time.Minute
			}
		}
		if running && ((p.Health == "unknown" && p.Lifecycle != "deploying") || stale) {
			return Condition{true, "No fresh status for " + p.Name,
				orStr(first["message"], "BeziSync has not observed the connector recently."),
				orStr(first["fix"], "Check the runtime and the BeziSync controller."), ""}
		}
	case "delivery_unverified":
		canary := mapOf(status["canary"])
		if running && canary["ok"] == false {
			// The lost canary's time (a newer one may already be pending).
			sent := "unknown time"
			if v, ok := canary["lost_sent_at"]; ok && v != nil {
				sent = strOf(v)
			} else if v, ok := mapOf(canary["pending"])["sent_at"]; ok {
				sent = strOf(v)
			}
			return Condition{true, p.Name + ": delivery not verified",
				fmt.Sprintf("A canary written at %s has not reached the destination.", sent),
				"Check the destination and the connector logs; changes may be stuck or lost.", ""}
		}
	case "schema_breaking":
		pending := int(toFloat(status["schema_pending_breaking"]))
		if pending != 0 && p.DeployedVersionID != nil {
			return Condition{true, "Breaking schema change on " + p.Name,
				fmt.Sprintf("%d unreviewed breaking change(s) to captured tables.", pending),
				"Check consumers of the affected tables, then mark the changes as reviewed in the Schema tab.", ""}
		}
	case "transform_drift":
		drift, _ := status["transform_drift"].([]any)
		if len(drift) > 0 && p.DeployedVersionID != nil {
			first := mapOf(drift[0])
			message := strOf(first["message"])
			if len(drift) > 1 {
				message += fmt.Sprintf(" (and %d more)", len(drift)-1)
			}
			severity := ""
			for _, d := range drift {
				if truthyAny(mapOf(d)["fail"]) {
					severity = "critical"
				}
			}
			return Condition{true, "Transform needs attention on " + p.Name, message,
				"Open the pipeline's Transform step: pick the new column or remove the step, then deploy.", severity}
		}
	case "cluster_shrink_failed":
		if shape, _ := p.Artifacts[ArtifactFlinkShape].(map[string]any); p.DeployedVersionID != nil && shape["shrink_off"] == true {
			return Condition{true, p.Name + "'s cluster stopped shrinking", fmt.Sprintf("Its streaming shape failed %d times before saving a position; it runs at its copy size.",
				int(toFloat(shape["shrink_failures"]))), "Give each reader more CPU or memory while streaming (Review → Size), then deploy.", ""}
		}
	case "position_regressed":
		if r := status["position_regressed"]; truthyAny(r) {
			return Condition{true, p.Name + "'s position moved backwards", strOf(r),
				"Consumers may receive duplicates. Check for a restored state database or a manual offset change.", ""}
		}
	}
	return Condition{}
}

// SeedDefaultRules gives a project its default rules on first use.
func SeedDefaultRules(ctx context.Context, q db.Querier, project *models.Project) ([]models.AlertRule, error) {
	existing, err := db.Select[models.AlertRule](ctx, q, `SELECT * FROM alert_rules WHERE project_id = $1 ORDER BY created_at, id`, project.ID)
	if err != nil || len(existing) > 0 {
		return existing, err
	}
	now := db.Now()
	var out []models.AlertRule
	for _, k := range RuleKinds {
		params := db.Map{}
		for pk, pv := range k.Params {
			params[pk] = pv
		}
		r := models.AlertRule{ID: db.NewID(), ProjectID: project.ID, Name: k.Title, Kind: k.Kind, Severity: k.DefaultSeverity,
			ForSeconds: k.DefaultForSeconds, Params: params, MatchLabels: db.Map{}, ChannelIDs: db.List{}, NotifyOwner: true, Enabled: true,
			CreatedAt: now, UpdatedAt: now}
		if err := db.Insert(ctx, q, "alert_rules", &r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

func labelsMatch(p *models.Pipeline, wanted map[string]any) bool {
	for k, v := range wanted {
		if have, ok := p.Labels[k]; !ok || strOf(have) != strOf(v) {
			return false
		}
	}
	return true
}

// EvaluateProject advances every (rule, pipeline) pair; returns alerts that fired or resolved.
func EvaluateProject(ctx context.Context, q db.Querier, projectID uuid.UUID, now time.Time) ([]*models.Alert, error) {
	project, err := db.Find[models.Project](ctx, q, `SELECT * FROM projects WHERE id = $1`, projectID)
	if err != nil || project == nil {
		return nil, err
	}
	all, err := SeedDefaultRules(ctx, q, project)
	if err != nil {
		return nil, err
	}
	var rules []models.AlertRule
	for _, r := range all {
		if r.Enabled {
			rules = append(rules, r)
		}
	}
	pipelines, err := db.Select[models.Pipeline](ctx, q, `SELECT * FROM pipelines WHERE project_id = $1`, projectID)
	if err != nil {
		return nil, err
	}
	openList, err := db.Select[models.Alert](ctx, q, `SELECT * FROM alerts WHERE project_id = $1 AND state <> 'resolved'`, projectID)
	if err != nil {
		return nil, err
	}
	type key struct{ rule, pipeline uuid.UUID }
	open := map[key]*models.Alert{}
	for i := range openList {
		open[key{openList[i].RuleID, openList[i].PipelineID}] = &openList[i]
	}
	var changed []*models.Alert
	for i := range pipelines {
		p := &pipelines[i]
		for _, rule := range rules {
			alert := open[key{rule.ID, p.ID}]
			cond := Condition{}
			if p.DeletedAt == nil && labelsMatch(p, rule.MatchLabels) {
				cond = Evaluate(rule.Kind, p, rule.Params, now)
			}
			switch {
			case cond.Active:
				isNew := alert == nil
				if isNew {
					sev := rule.Severity
					if cond.Severity != "" {
						sev = cond.Severity
					}
					alert = &models.Alert{ID: db.NewID(), ProjectID: projectID, RuleID: rule.ID, PipelineID: p.ID, State: "pending",
						Severity: sev, StartedAt: now, LastSeenAt: now, Deliveries: db.List{}}
					open[key{rule.ID, p.ID}] = alert
				}
				alert.LastSeenAt = now
				alert.Title, alert.Message, alert.Fix = cond.Title, cond.Message, cond.Fix
				if cond.Severity != "" {
					alert.Severity = cond.Severity
				}
				if alert.State == "pending" && now.Sub(alert.StartedAt) >= time.Duration(rule.ForSeconds)*time.Second {
					alert.State, alert.FiredAt = "firing", &now
					changed = append(changed, alert)
				}
				if isNew {
					err = db.Insert(ctx, q, "alerts", alert)
				} else {
					err = db.Update(ctx, q, "alerts", alert)
				}
				if err != nil {
					return nil, err
				}
			case alert != nil:
				notified := alert.State == "firing" || alert.State == "acknowledged"
				alert.State, alert.ResolvedAt = "resolved", &now
				if err := db.Update(ctx, q, "alerts", alert); err != nil {
					return nil, err
				}
				delete(open, key{rule.ID, p.ID})
				if notified {
					changed = append(changed, alert)
				}
			}
		}
	}
	for _, a := range changed {
		if err := announce(ctx, q, a, project); err != nil {
			return nil, err
		}
	}
	return changed, nil
}

func announce(ctx context.Context, q db.Querier, a *models.Alert, project *models.Project) error {
	p, err := db.Find[models.Pipeline](ctx, q, `SELECT * FROM pipelines WHERE id = $1`, a.PipelineID)
	if err != nil {
		return err
	}
	resolved := a.State == "resolved"
	kind, summary, inboxKind, title := "alert.fired", "Alert: "+a.Title, "alert_fired", a.Title
	if resolved {
		kind, summary, inboxKind, title = "alert.resolved", "Resolved: "+a.Title, "alert_resolved", "Resolved: "+a.Title
	}
	if _, err := Record(ctx, q, AuditEvent{Kind: kind, ProjectID: &project.ID, NotAudit: true, Summary: summary, ResourceType: "pipeline",
		ResourceID: &a.PipelineID, Details: map[string]any{"alert_id": a.ID.String(), "severity": a.Severity}}); err != nil {
		return err
	}
	audience, err := ProjectAudience(ctx, q, project.ID, false)
	if err != nil {
		return err
	}
	if _, err := Notify(ctx, q, audience, Notification{ProjectID: project.ID, Kind: inboxKind, Title: title, Body: a.Message,
		Link: fmt.Sprintf("/p/%s/pipelines/%s", project.Slug, a.PipelineID)}); err != nil {
		return err
	}
	rule, err := db.Find[models.AlertRule](ctx, q, `SELECT * FROM alert_rules WHERE id = $1`, a.RuleID)
	if err != nil || rule == nil {
		return err
	}
	var targets []any
	for _, c := range rule.ChannelIDs {
		targets = append(targets, strOf(c))
	}
	if rule.NotifyOwner && p != nil && strings.Contains(p.Owner, "@") {
		targets = append(targets, "owner:"+p.Owner)
	}
	if len(targets) == 0 {
		return nil
	}
	pid, rid := project.ID, a.ID
	return db.Insert(ctx, q, "jobs", &models.Job{ID: db.NewID(), Kind: "notify", ProjectID: &pid, ResourceID: &rid,
		Payload: db.Map{"targets": targets}, State: "queued", MaxAttempts: 5, RunAfter: db.Now(), CreatedAt: db.Now()})
}

// AlertPayload is what channels receive (ordered like the Python dict).
func AlertPayload(a *models.Alert, pipelineName string, project *models.Project) *om.Map {
	link := fmt.Sprintf("%s/p/%s/pipelines/%s", strings.TrimRight(config.Get().PublicURL, "/"), project.Slug, a.PipelineID)
	opt := func(t *time.Time) any {
		if t == nil {
			return nil
		}
		return pytime.ISOFormat(*t)
	}
	return om.New().Set("id", a.ID.String()).Set("state", a.State).Set("severity", a.Severity).Set("title", a.Title).
		Set("message", a.Message).Set("fix", a.Fix).Set("pipeline", pipelineName).Set("project", project.Slug).
		Set("started_at", pytime.ISOFormat(a.StartedAt)).Set("fired_at", opt(a.FiredAt)).Set("resolved_at", opt(a.ResolvedAt)).Set("url", link)
}

func pyDumps(v any) ([]byte, error) {
	s, err := pyjson.Encode(v, pyjson.Options{EnsureASCII: true})
	return []byte(s), err
}

func postJSON(ctx context.Context, rawURL string, body []byte, headers map[string]string) error {
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return errors.New("only http(s) URLs are allowed")
	}
	port := 80
	if u.Scheme == "https" {
		port = 443
	}
	if p := u.Port(); p != "" {
		port, _ = strconv.Atoi(p)
	}
	if _, err := ssrf.ResolveChecked(ctx, strings.ToLower(u.Hostname()), port); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	// The check above gives a clear message; the guarded client is what holds: every dial,
	// redirects included, resolves and checks again (ADR 0004).
	resp, err := ssrf.HTTPClient(10 * time.Second).Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		text, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, domain.Truncate(string(text), 200))
	}
	return nil
}

func payloadStr(p *om.Map, k string) string {
	v, _ := p.Get(k)
	return strOf(v)
}

// Deliver sends an alert to one channel.
func Deliver(ctx context.Context, ch *models.AlertChannel, payload *om.Map) error {
	sec := map[string]any{}
	if ch.SecretsEnc != nil && *ch.SecretsEnc != "" {
		var err error
		if sec, err = security.DecryptSecret(*ch.SecretsEnc); err != nil {
			return err
		}
	}
	resolved := payloadStr(payload, "state") == "resolved"
	switch ch.Kind {
	case "slack":
		icon := "🟠"
		switch {
		case resolved:
			icon = "✅"
		case payloadStr(payload, "severity") == "critical":
			icon = "🔴"
		}
		text := fmt.Sprintf("%s *%s*\n%s", icon, payloadStr(payload, "title"), payloadStr(payload, "message"))
		if fix := payloadStr(payload, "fix"); fix != "" && !resolved {
			text += "\n_Fix:_ " + fix
		}
		body, _ := pyDumps(om.New().Set("text", fmt.Sprintf("%s\n<%s|Open in BeziSync>", text, payloadStr(payload, "url"))))
		return postJSON(ctx, strOf(sec["webhook_url"]), body, nil)
	case "webhook":
		body, err := pyDumps(payload)
		if err != nil {
			return err
		}
		headers := map[string]string{}
		if s := strOf(sec["signing_secret"]); s != "" {
			mac := hmac.New(sha256.New, []byte(s))
			mac.Write(body)
			headers["x-bezisync-signature"] = "sha256=" + hex.EncodeToString(mac.Sum(nil))
		}
		return postJSON(ctx, strOf(sec["url"]), body, headers)
	case "pagerduty":
		target := "https://events.pagerduty.com/v2/enqueue"
		if v := strOf(ch.Config["events_url"]); v != "" {
			target = v
		}
		action, sev := "trigger", "warning"
		if resolved {
			action = "resolve"
		}
		if payloadStr(payload, "severity") == "critical" {
			sev = "critical"
		}
		body, _ := pyDumps(om.New().Set("routing_key", strOf(sec["routing_key"])).Set("dedup_key", payloadStr(payload, "id")).
			Set("event_action", action).Set("payload", om.New().Set("summary", payloadStr(payload, "title")).
			Set("source", fmt.Sprintf("bezisync/%s/%s", payloadStr(payload, "project"), payloadStr(payload, "pipeline"))).
			Set("severity", sev).Set("custom_details", om.New().Set("message", payloadStr(payload, "message")).Set("fix", payloadStr(payload, "fix")))).
			Set("links", []any{om.New().Set("href", payloadStr(payload, "url")).Set("text", "Open in BeziSync")}))
		return postJSON(ctx, target, body, nil)
	case "email":
		var recipients []string
		if l, ok := ch.Config["recipients"].([]any); ok {
			for _, r := range l {
				recipients = append(recipients, strOf(r))
			}
		}
		return SendEmail(recipients, payload)
	}
	return fmt.Errorf("unknown channel kind %s", ch.Kind)
}

// SendEmail sends an alert email through the configured SMTP server.
func SendEmail(recipients []string, payload *om.Map) error {
	s := config.Get()
	if s.SMTPHost == "" {
		return errors.New("Email is not configured (BEZISYNC_SMTP_HOST).")
	}
	if len(recipients) == 0 {
		return nil
	}
	prefix := "[" + payloadStr(payload, "severity") + "] "
	if payloadStr(payload, "state") == "resolved" {
		prefix = "[resolved] "
	}
	fix := ""
	if f := payloadStr(payload, "fix"); f != "" {
		fix = "Fix: " + f
	}
	var msg bytes.Buffer
	fmt.Fprintf(&msg, "Subject: %s\r\n", mime.QEncoding.Encode("utf-8", prefix+payloadStr(payload, "title")))
	fmt.Fprintf(&msg, "From: %s\r\n", s.SMTPFrom)
	fmt.Fprintf(&msg, "To: %s\r\n", strings.Join(recipients, ", "))
	msg.WriteString("Content-Type: text/plain; charset=\"utf-8\"\r\nContent-Transfer-Encoding: 8bit\r\nMIME-Version: 1.0\r\n\r\n")
	fmt.Fprintf(&msg, "%s\n\n%s\n\n%s\n", payloadStr(payload, "message"), fix, payloadStr(payload, "url"))
	addr := net.JoinHostPort(s.SMTPHost, strconv.Itoa(s.SMTPPort))
	d := net.Dialer{Timeout: 15 * time.Second}
	conn, err := d.DialContext(context.Background(), "tcp", addr)
	if err != nil {
		return err
	}
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
	c, err := smtp.NewClient(conn, s.SMTPHost)
	if err != nil {
		_ = conn.Close()
		return err
	}
	defer func() { _ = c.Close() }()
	if s.SMTPStartTLS {
		if err := c.StartTLS(&tls.Config{ServerName: s.SMTPHost, MinVersion: tls.VersionTLS12}); err != nil {
			return err
		}
	}
	if s.SMTPUsername != "" {
		if err := c.Auth(smtp.PlainAuth("", s.SMTPUsername, s.SMTPPassword, s.SMTPHost)); err != nil {
			return err
		}
	}
	if err := c.Mail(s.SMTPFrom); err != nil {
		return err
	}
	for _, r := range recipients {
		if err := c.Rcpt(r); err != nil {
			return err
		}
	}
	w, err := c.Data()
	if err != nil {
		return err
	}
	if _, err := w.Write(msg.Bytes()); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return c.Quit()
}

// Retry asks the job runner to try again later (not counted as a failure reason).
type Retry struct{ Msg string }

func (r *Retry) Error() string { return r.Msg }

// RunNotifyJob delivers an alert to its targets (idempotent per target and state).
func RunNotifyJob(ctx context.Context, q db.Querier, alertID uuid.UUID, targets []string) (map[string]any, error) {
	a, err := db.Find[models.Alert](ctx, q, `SELECT * FROM alerts WHERE id = $1`, alertID)
	if err != nil {
		return nil, err
	}
	if a == nil {
		return map[string]any{"skipped": "alert gone"}, nil
	}
	project, err := db.Find[models.Project](ctx, q, `SELECT * FROM projects WHERE id = $1`, a.ProjectID)
	if err != nil {
		return nil, err
	}
	p, err := db.Find[models.Pipeline](ctx, q, `SELECT * FROM pipelines WHERE id = $1`, a.PipelineID)
	if err != nil {
		return nil, err
	}
	name := "?"
	if p != nil {
		name = p.Name
	}
	payload := AlertPayload(a, name, project)
	already := map[[2]string]bool{}
	for _, d := range a.Deliveries {
		m := mapOf(d)
		if m["ok"] == true {
			already[[2]string{strOf(m["target"]), strOf(m["state"])}] = true
		}
	}
	var results []any
	var errs []string
	for _, target := range targets {
		if already[[2]string{target, a.State}] {
			continue
		}
		err := func() error {
			if strings.HasPrefix(target, "owner:") {
				return SendEmail([]string{strings.TrimPrefix(target, "owner:")}, payload)
			}
			id, perr := uuid.Parse(target)
			if perr != nil {
				return perr
			}
			ch, err := db.Find[models.AlertChannel](ctx, q, `SELECT * FROM alert_channels WHERE id = $1`, id)
			if err != nil {
				return err
			}
			if ch == nil || !ch.Enabled {
				return errSkip
			}
			if err := Deliver(ctx, ch, payload); err != nil {
				msg := domain.Truncate(err.Error(), 500)
				ch.LastError = &msg
				_ = db.Update(ctx, q, "alert_channels", ch)
				return err
			}
			now := db.Now()
			ch.LastDeliveryAt, ch.LastError = &now, nil
			return db.Update(ctx, q, "alert_channels", ch)
		}()
		if errors.Is(err, errSkip) {
			continue
		}
		at := pytime.ISOFormat(time.Now())
		if err == nil {
			results = append(results, map[string]any{"target": target, "state": a.State, "ok": true, "at": at})
		} else {
			results = append(results, map[string]any{"target": target, "state": a.State, "ok": false, "error": domain.Truncate(err.Error(), 300), "at": at})
			errs = append(errs, fmt.Sprintf("%s: %v", target, err))
		}
	}
	a.Deliveries = append(a.Deliveries, results...)
	if err := db.Update(ctx, q, "alerts", a); err != nil {
		return nil, err
	}
	if len(errs) > 0 {
		return nil, &Retry{domain.Truncate(strings.Join(errs, "; "), 900)}
	}
	return map[string]any{"delivered": len(results)}, nil
}

var errSkip = errors.New("skip")
