package api

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/schemas"
	"github.com/daun-gatal/bezisync/backend/internal/security"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// ChannelRead is an alert delivery channel; its secrets are never returned.
type ChannelRead struct {
	ID              uuid.UUID    `json:"id"`
	Name            string       `json:"name"`
	Kind            string       `json:"kind"`
	Config          db.Map       `json:"config"`
	SecretFieldsSet []string     `json:"secret_fields_set"`
	Enabled         bool         `json:"enabled"`
	LastDeliveryAt  *pytime.Time `json:"last_delivery_at"`
	LastError       *string      `json:"last_error"`
}

// RuleRead is an alert rule.
type RuleRead struct {
	ID          uuid.UUID         `json:"id"`
	Name        string            `json:"name"`
	Kind        string            `json:"kind"`
	Description string            `json:"description"`
	Severity    string            `json:"severity"`
	ForSeconds  int               `json:"for_seconds"`
	Params      db.Map            `json:"params"`
	MatchLabels map[string]string `json:"match_labels"`
	ChannelIDs  []uuid.UUID       `json:"channel_ids"`
	NotifyOwner bool              `json:"notify_owner"`
	Enabled     bool              `json:"enabled"`
}

func channelRead(c *models.AlertChannel) (ChannelRead, error) {
	fields := []string{}
	if c.SecretsEnc != nil && *c.SecretsEnc != "" {
		sec, err := security.DecryptSecret(*c.SecretsEnc)
		if err != nil {
			return ChannelRead{}, err
		}
		for k := range sec {
			fields = append(fields, k)
		}
		sort.Strings(fields)
	}
	cfg := c.Config
	if cfg == nil {
		cfg = db.Map{}
	}
	return ChannelRead{ID: c.ID, Name: c.Name, Kind: c.Kind, Config: cfg, SecretFieldsSet: fields, Enabled: c.Enabled,
		LastDeliveryAt: pytime.P(c.LastDeliveryAt), LastError: c.LastError}, nil
}

func ruleRead(r *models.AlertRule) RuleRead {
	out := RuleRead{ID: r.ID, Name: r.Name, Kind: r.Kind, Severity: r.Severity, ForSeconds: r.ForSeconds, Params: r.Params,
		MatchLabels: map[string]string{}, ChannelIDs: []uuid.UUID{}, NotifyOwner: r.NotifyOwner, Enabled: r.Enabled}
	if k, ok := services.RuleKindByName(r.Kind); ok {
		out.Description = k.Description
	}
	if out.Params == nil {
		out.Params = db.Map{}
	}
	for k, v := range r.MatchLabels {
		out.MatchLabels[k] = pyStr(v)
	}
	for _, c := range r.ChannelIDs {
		if id, err := uuid.Parse(pyStr(c)); err == nil {
			out.ChannelIDs = append(out.ChannelIDs, id)
		}
	}
	return out
}

func projectRow[T any](ctx context.Context, q db.Querier, a *auth.ProjectAccess, table string, id uuid.UUID, what string, projectOf func(*T) uuid.UUID) (*T, error) {
	row, err := db.Find[T](ctx, q, `SELECT * FROM `+table+` WHERE id = $1`, id)
	if err != nil {
		return nil, err
	}
	if row == nil || projectOf(row) != a.Project.ID {
		return nil, problem.NotFound(what)
	}
	return row, nil
}

func getChannel(ctx context.Context, q db.Querier, a *auth.ProjectAccess, id uuid.UUID) (*models.AlertChannel, error) {
	return projectRow(ctx, q, a, "alert_channels", id, "Channel", func(c *models.AlertChannel) uuid.UUID { return c.ProjectID })
}

func getRule(ctx context.Context, q db.Querier, a *auth.ProjectAccess, id uuid.UUID) (*models.AlertRule, error) {
	return projectRow(ctx, q, a, "alert_rules", id, "Rule", func(r *models.AlertRule) uuid.UUID { return r.ProjectID })
}

func checkChannels(ctx context.Context, q db.Querier, a *auth.ProjectAccess, ids []uuid.UUID) (db.List, error) {
	out := db.List{}
	for _, id := range ids {
		if _, err := getChannel(ctx, q, a, id); err != nil {
			return nil, err
		}
		out = append(out, id.String())
	}
	return out, nil
}

func validateChannel(kind string, cfg, sec map[string]any) (map[string]any, map[string]any, error) {
	k, ok := schemas.Channel(kind)
	if !ok {
		return nil, nil, problem.New(422, "unknown_channel_kind", fmt.Sprintf("Unknown channel kind '%s'.", kind))
	}
	return validateKind(k, cfg, sec)
}

func severityOf(v *val.V, o *val.Object, key string) *string {
	f := o.Get(key)
	if !f.Present || f.IsNull() {
		return nil
	}
	s, ok := v.Literal(f.Loc, f.Raw, "critical", "warning")
	if !ok {
		return nil
	}
	return &s
}

func optUUIDList(v *val.V, o *val.Object, key string) ([]uuid.UUID, bool) {
	f := o.Get(key)
	if !f.Present || f.IsNull() {
		return nil, false
	}
	items, ok := v.List(f.Loc, f.Raw)
	if !ok {
		return nil, false
	}
	out := []uuid.UUID{}
	for i, item := range items {
		if id, ok := v.UUID(f.Loc.Add(i), item); ok {
			out = append(out, id)
		}
	}
	return out, true
}

func optIntIn(v *val.V, o *val.Object, key string, b val.Num) *int {
	f := o.Get(key)
	if !f.Present || f.IsNull() {
		return nil
	}
	n, ok := v.IntIn(f.Loc, f.Raw, b)
	if !ok {
		return nil
	}
	return &n
}

func strMap(m map[string]string) db.Map {
	out := db.Map{}
	for k, v := range m {
		out[k] = v
	}
	return out
}

func truncate(s string, n int) string {
	if utf8.RuneCountInString(s) > n {
		return string([]rune(s)[:n])
	}
	return s
}

func alertRoutes(r chi.Router) {
	p := "/projects/{project_id}"
	r.Get(p+"/alert-kinds", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		if _, err := project(r); err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, services.RuleKinds)
		return nil
	}))
	r.Get(p+"/alerts", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		qs := httpapi.Q(r)
		state := qs.Literal("state", "open", "open", "resolved", "all")
		pipelineID := qs.UUID("pipeline_id")
		limit := qs.Int("limit", 200, val.Num{GE: val.F(1), LE: val.F(500)})
		if err := qs.Err(); err != nil {
			return err
		}
		if err := a.Require(auth.AlertView); err != nil {
			return err
		}
		sql := `SELECT a.*, r.name AS rule_name, p.name AS pipeline_name, p.owner AS pipeline_owner FROM alerts a
			JOIN alert_rules r ON a.rule_id = r.id JOIN pipelines p ON a.pipeline_id = p.id WHERE a.project_id = $1`
		args := []any{a.Project.ID}
		switch state {
		case "open":
			sql += ` AND a.state IN ('firing', 'acknowledged')`
		case "resolved":
			sql += ` AND a.state = 'resolved' AND a.fired_at IS NOT NULL`
		default:
			sql += ` AND a.fired_at IS NOT NULL`
		}
		if pipelineID != nil {
			args = append(args, *pipelineID)
			sql += fmt.Sprintf(` AND a.pipeline_id = $%d`, len(args))
		}
		args = append(args, limit)
		sql += fmt.Sprintf(` ORDER BY a.severity ASC, a.started_at DESC LIMIT $%d`, len(args))
		ctx := r.Context()
		rows, err := db.Select[struct {
			models.Alert
			RuleName      string `db:"rule_name"`
			PipelineName  string `db:"pipeline_name"`
			PipelineOwner string `db:"pipeline_owner"`
		}](ctx, pool(ctx), sql, args...)
		if err != nil {
			return err
		}
		out := make([]*om.Map, len(rows))
		for i, row := range rows {
			deliveries := row.Deliveries
			if deliveries == nil {
				deliveries = db.List{}
			}
			out[i] = om.New().Set("id", row.ID).Set("rule_id", row.RuleID).Set("rule_name", row.RuleName).Set("pipeline_id", row.PipelineID).
				Set("pipeline_name", row.PipelineName).Set("pipeline_owner", row.PipelineOwner).Set("state", row.State).Set("severity", row.Severity).
				Set("title", row.Title).Set("message", row.Message).Set("fix", row.Fix).Set("started_at", pytime.T(row.StartedAt)).
				Set("fired_at", pytime.P(row.FiredAt)).Set("resolved_at", pytime.P(row.ResolvedAt)).Set("acknowledged_at", pytime.P(row.AcknowledgedAt)).
				Set("acknowledged_by", row.AcknowledgedBy).Set("deliveries", deliveries)
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Post(p+"/alerts/{alert_id}/acknowledge", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "alert_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		if err := a.Require(auth.AlertAck); err != nil {
			return err
		}
		ctx := r.Context()
		err = tx(ctx, func(q db.Querier) error {
			alert, err := projectRow(ctx, q, a, "alerts", id, "Alert", func(x *models.Alert) uuid.UUID { return x.ProjectID })
			if err != nil {
				return err
			}
			if alert.State != "firing" {
				return problem.New(409, "not_firing", "Only firing alerts can be acknowledged.")
			}
			label := a.Principal.Label()
			if _, err := q.Exec(ctx, `UPDATE alerts SET state = 'acknowledged', acknowledged_at = $1, acknowledged_by = $2 WHERE id = $3`,
				db.Now(), label, alert.ID); err != nil {
				return err
			}
			_, err = services.Record(ctx, q, services.AuditEvent{Kind: "alert.acknowledged", Summary: "Acknowledged: " + alert.Title,
				ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "pipeline", ResourceID: &alert.PipelineID})
			return err
		})
		if err != nil {
			return err
		}
		httpapi.NoContent(w)
		return nil
	}))
	r.Get(p+"/alert-rules", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		if err := a.Require(auth.AlertView); err != nil {
			return err
		}
		ctx := r.Context()
		var rules []models.AlertRule
		err = tx(ctx, func(q db.Querier) error {
			var err error
			rules, err = services.SeedDefaultRules(ctx, q, a.Project)
			return err
		})
		if err != nil {
			return err
		}
		sort.SliceStable(rules, func(i, j int) bool { return rules[i].Name < rules[j].Name })
		out := make([]RuleRead, len(rules))
		for i := range rules {
			out[i] = ruleRead(&rules[i])
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Post(p+"/alert-rules", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		type write struct {
			Name, Kind, Severity string
			ForSeconds           int
			Params               map[string]any
			MatchLabels          map[string]string
			ChannelIDs           []uuid.UUID
			NotifyOwner, Enabled bool
		}
		b, err := body(r, func(v *val.V, o *val.Object) write {
			wr := write{Name: str(v, o, "name", "", val.StrRules{Min: 1, Max: 200}, true), Kind: str(v, o, "kind", "", val.StrRules{}, true), Severity: "warning",
				ForSeconds: 120, NotifyOwner: boolean(v, o, "notify_owner", true), Enabled: boolean(v, o, "enabled", true)}
			if s := severityOf(v, o, "severity"); s != nil {
				wr.Severity = *s
			}
			if n := optIntIn(v, o, "for_seconds", val.Num{GE: val.F(0), LE: val.F(86_400)}); n != nil {
				wr.ForSeconds = *n
			}
			if wr.Params = dict(v, o, "params", false); wr.Params == nil {
				wr.Params = map[string]any{}
			}
			wr.MatchLabels, _ = labelsOf(v, o, "match_labels", false)
			wr.ChannelIDs, _ = optUUIDList(v, o, "channel_ids")
			return wr
		})
		if err != nil {
			return err
		}
		if err := a.Require(auth.AlertManage); err != nil {
			return err
		}
		if _, ok := services.RuleKindByName(b.Kind); !ok {
			return problem.New(422, "unknown_rule_kind", fmt.Sprintf("Unknown rule kind '%s'.", b.Kind))
		}
		ctx := r.Context()
		var out RuleRead
		err = tx(ctx, func(q db.Querier) error {
			channels, err := checkChannels(ctx, q, a, b.ChannelIDs)
			if err != nil {
				return err
			}
			now := db.Now()
			rule := &models.AlertRule{ID: db.NewID(), ProjectID: a.Project.ID, Name: b.Name, Kind: b.Kind, Severity: b.Severity, ForSeconds: b.ForSeconds,
				Params: db.Map(b.Params), MatchLabels: strMap(b.MatchLabels), ChannelIDs: channels, NotifyOwner: b.NotifyOwner, Enabled: b.Enabled,
				CreatedAt: now, UpdatedAt: now}
			if err := db.Insert(ctx, q, "alert_rules", rule); err != nil {
				return err
			}
			if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "alert_rule.created", Summary: "Added alert rule " + b.Name,
				ProjectID: &a.Project.ID, Principal: a.Principal}); err != nil {
				return err
			}
			out = ruleRead(rule)
			return nil
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusCreated, out)
		return nil
	}))
	r.Patch(p+"/alert-rules/{rule_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "rule_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		type update struct {
			Name, Severity       *string
			ForSeconds           *int
			Params               map[string]any
			HasParams            bool
			MatchLabels          map[string]string
			HasLabels            bool
			ChannelIDs           []uuid.UUID
			HasChannels          bool
			NotifyOwner, Enabled *bool
		}
		b, err := body(r, func(v *val.V, o *val.Object) update {
			u := update{Name: optStr(v, o, "name", val.StrRules{}), Severity: severityOf(v, o, "severity"),
				ForSeconds:  optIntIn(v, o, "for_seconds", val.Num{GE: val.F(0), LE: val.F(86_400)}),
				NotifyOwner: optBool(v, o, "notify_owner"), Enabled: optBool(v, o, "enabled")}
			u.Params, u.HasParams = optDict(v, o, "params")
			u.MatchLabels, u.HasLabels = labelsOf(v, o, "match_labels", false)
			u.ChannelIDs, u.HasChannels = optUUIDList(v, o, "channel_ids")
			return u
		})
		if err != nil {
			return err
		}
		if err := a.Require(auth.AlertManage); err != nil {
			return err
		}
		ctx := r.Context()
		var out RuleRead
		err = tx(ctx, func(q db.Querier) error {
			rule, err := getRule(ctx, q, a, id)
			if err != nil {
				return err
			}
			if b.HasChannels {
				if rule.ChannelIDs, err = checkChannels(ctx, q, a, b.ChannelIDs); err != nil {
					return err
				}
			}
			if b.Name != nil {
				rule.Name = *b.Name
			}
			if b.Severity != nil {
				rule.Severity = *b.Severity
			}
			if b.ForSeconds != nil {
				rule.ForSeconds = *b.ForSeconds
			}
			if b.HasParams {
				rule.Params = db.Map(b.Params)
			}
			if b.HasLabels {
				rule.MatchLabels = strMap(b.MatchLabels)
			}
			if b.NotifyOwner != nil {
				rule.NotifyOwner = *b.NotifyOwner
			}
			if b.Enabled != nil {
				rule.Enabled = *b.Enabled
			}
			rule.UpdatedAt = db.Now()
			if err := db.Update(ctx, q, "alert_rules", rule); err != nil {
				return err
			}
			if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "alert_rule.updated", Summary: "Updated alert rule " + rule.Name,
				ProjectID: &a.Project.ID, Principal: a.Principal}); err != nil {
				return err
			}
			out = ruleRead(rule)
			return nil
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Delete(p+"/alert-rules/{rule_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "rule_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		if err := a.Require(auth.AlertManage); err != nil {
			return err
		}
		ctx := r.Context()
		err = tx(ctx, func(q db.Querier) error {
			rule, err := getRule(ctx, q, a, id)
			if err != nil {
				return err
			}
			if err := db.Delete(ctx, q, "alert_rules", rule.ID); err != nil {
				return err
			}
			_, err = services.Record(ctx, q, services.AuditEvent{Kind: "alert_rule.deleted", Summary: "Deleted alert rule " + rule.Name,
				ProjectID: &a.Project.ID, Principal: a.Principal})
			return err
		})
		if err != nil {
			return err
		}
		httpapi.NoContent(w)
		return nil
	}))
	r.Get(p+"/alert-channel-kinds", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		if _, err := project(r); err != nil {
			return err
		}
		out := []any{}
		for _, k := range schemas.Channels() {
			out = append(out, obj("kind", k.Kind, "title", k.Title, "config_schema", k.ConfigSchema, "secret_schema", k.SecretSchema))
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Get(p+"/alert-channels", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		if err := a.Require(auth.AlertView); err != nil {
			return err
		}
		ctx := r.Context()
		rows, err := db.Select[models.AlertChannel](ctx, pool(ctx), `SELECT * FROM alert_channels WHERE project_id = $1 ORDER BY name`, a.Project.ID)
		if err != nil {
			return err
		}
		out := make([]ChannelRead, len(rows))
		for i := range rows {
			if out[i], err = channelRead(&rows[i]); err != nil {
				return err
			}
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Post(p+"/alert-channels", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		type write struct {
			Name, Kind      string
			Config, Secrets map[string]any
			Enabled         bool
		}
		b, err := body(r, func(v *val.V, o *val.Object) write {
			wr := write{Name: str(v, o, "name", "", val.StrRules{Min: 1, Max: 100}, true), Enabled: boolean(v, o, "enabled", true)}
			if f := o.Get("kind"); f.Required() {
				wr.Kind, _ = v.Literal(f.Loc, f.Raw, "slack", "webhook", "pagerduty", "email")
			}
			if wr.Config = dict(v, o, "config", false); wr.Config == nil {
				wr.Config = map[string]any{}
			}
			if wr.Secrets = dict(v, o, "secrets", false); wr.Secrets == nil {
				wr.Secrets = map[string]any{}
			}
			return wr
		})
		if err != nil {
			return err
		}
		if err := a.Require(auth.AlertManage); err != nil {
			return err
		}
		cfg, sec, err := validateChannel(b.Kind, b.Config, b.Secrets)
		if err != nil {
			return err
		}
		ctx := r.Context()
		var out ChannelRead
		err = tx(ctx, func(q db.Querier) error {
			enc, err := services.EncryptOrNil(sec)
			if err != nil {
				return err
			}
			now := db.Now()
			c := &models.AlertChannel{ID: db.NewID(), ProjectID: a.Project.ID, Name: b.Name, Kind: b.Kind, Config: db.Map(cfg), SecretsEnc: enc,
				Enabled: b.Enabled, CreatedAt: now, UpdatedAt: now}
			if err := db.Insert(ctx, q, "alert_channels", c); err != nil {
				return err
			}
			if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "alert_channel.created", Summary: fmt.Sprintf("Added %s channel %s", b.Kind, b.Name),
				ProjectID: &a.Project.ID, Principal: a.Principal}); err != nil {
				return err
			}
			out, err = channelRead(c)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusCreated, out)
		return nil
	}))
	r.Patch(p+"/alert-channels/{channel_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "channel_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		type update struct {
			Name            *string
			Config, Secrets map[string]any
			HasConfig       bool
			Enabled         *bool
		}
		b, err := body(r, func(v *val.V, o *val.Object) update {
			u := update{Name: optStr(v, o, "name", val.StrRules{Min: 1, Max: 100}), Enabled: optBool(v, o, "enabled")}
			u.Config, u.HasConfig = optDict(v, o, "config")
			u.Secrets, _ = optDict(v, o, "secrets")
			return u
		})
		if err != nil {
			return err
		}
		if err := a.Require(auth.AlertManage); err != nil {
			return err
		}
		ctx := r.Context()
		var out ChannelRead
		err = tx(ctx, func(q db.Querier) error {
			c, err := getChannel(ctx, q, a, id)
			if err != nil {
				return err
			}
			current := map[string]any{}
			if c.SecretsEnc != nil && *c.SecretsEnc != "" {
				if current, err = security.DecryptSecret(*c.SecretsEnc); err != nil {
					return err
				}
			}
			cfgIn := map[string]any(c.Config)
			if b.HasConfig {
				cfgIn = b.Config
			}
			cfg, sec, err := validateChannel(c.Kind, cfgIn, services.MergeSecrets(current, b.Secrets))
			if err != nil {
				return err
			}
			enc, err := services.EncryptOrNil(sec)
			if err != nil {
				return err
			}
			c.Config, c.SecretsEnc = db.Map(cfg), enc
			if b.Name != nil {
				c.Name = *b.Name
			}
			if b.Enabled != nil {
				c.Enabled = *b.Enabled
			}
			c.UpdatedAt = db.Now()
			if err := db.Update(ctx, q, "alert_channels", c); err != nil {
				return err
			}
			if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "alert_channel.updated", Summary: "Updated channel " + c.Name,
				ProjectID: &a.Project.ID, Principal: a.Principal}); err != nil {
				return err
			}
			out, err = channelRead(c)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Delete(p+"/alert-channels/{channel_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "channel_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		if err := a.Require(auth.AlertManage); err != nil {
			return err
		}
		ctx := r.Context()
		err = tx(ctx, func(q db.Querier) error {
			c, err := getChannel(ctx, q, a, id)
			if err != nil {
				return err
			}
			rules, err := db.Select[models.AlertRule](ctx, q, `SELECT * FROM alert_rules WHERE project_id = $1`, a.Project.ID)
			if err != nil {
				return err
			}
			for i := range rules {
				kept := db.List{}
				found := false
				for _, cid := range rules[i].ChannelIDs {
					if pyStr(cid) == c.ID.String() {
						found = true
					} else {
						kept = append(kept, cid)
					}
				}
				if found {
					if _, err := q.Exec(ctx, `UPDATE alert_rules SET channel_ids = $1, updated_at = $2 WHERE id = $3`, kept, db.Now(), rules[i].ID); err != nil {
						return err
					}
				}
			}
			if err := db.Delete(ctx, q, "alert_channels", c.ID); err != nil {
				return err
			}
			_, err = services.Record(ctx, q, services.AuditEvent{Kind: "alert_channel.deleted", Summary: "Deleted channel " + c.Name,
				ProjectID: &a.Project.ID, Principal: a.Principal})
			return err
		})
		if err != nil {
			return err
		}
		httpapi.NoContent(w)
		return nil
	}))
	r.Post(p+"/alert-channels/{channel_id}/test", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "channel_id")
		if err != nil {
			return err
		}
		a, err := project(r)
		if err != nil {
			return err
		}
		if err := a.Require(auth.AlertManage); err != nil {
			return err
		}
		ctx := r.Context()
		c, err := getChannel(ctx, pool(ctx), a, id)
		if err != nil {
			return err
		}
		now := pytime.ISOFormat(db.Now())
		payload := om.New().Set("id", "test-"+strings.ReplaceAll(uuid.NewString(), "-", "")[:8]).Set("state", "firing").Set("severity", "warning").
			Set("title", "Test notification from BeziSync").Set("message", fmt.Sprintf("Channel %s works.", c.Name)).Set("fix", "").
			Set("pipeline", "test").Set("project", a.Project.Slug).Set("started_at", now).Set("fired_at", now).Set("resolved_at", nil).
			Set("url", "").Set("test", true)
		if derr := services.Deliver(ctx, c, payload); derr != nil {
			msg := truncate(derr.Error(), 500)
			if _, err := pool(ctx).Exec(ctx, `UPDATE alert_channels SET last_error = $1 WHERE id = $2`, msg, c.ID); err != nil {
				return err
			}
			httpapi.JSON(w, http.StatusOK, obj("ok", false, "message", msg))
			return nil
		}
		if _, err := pool(ctx).Exec(ctx, `UPDATE alert_channels SET last_delivery_at = $1, last_error = NULL WHERE id = $2`, db.Now(), c.ID); err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, obj("ok", true, "message", "Delivered."))
		return nil
	}))
	r.Get(p+"/notifications", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		qs := httpapi.Q(r)
		limit := qs.Int("limit", 30, val.Num{GE: val.F(1), LE: val.F(200)})
		if err := qs.Err(); err != nil {
			return err
		}
		ctx := r.Context()
		q := pool(ctx)
		user := a.Principal.User.ID
		unread, err := db.Scalar[int64](ctx, q, `SELECT count(*) FROM notifications WHERE user_id = $1 AND project_id = $2 AND read_at IS NULL`, user, a.Project.ID)
		if err != nil {
			return err
		}
		rows, err := db.Select[models.Notification](ctx, q, `SELECT * FROM notifications WHERE user_id = $1 AND project_id = $2 ORDER BY created_at DESC LIMIT $3`,
			user, a.Project.ID, limit)
		if err != nil {
			return err
		}
		type notificationRead struct {
			ID        uuid.UUID   `json:"id"`
			Kind      string      `json:"kind"`
			Title     string      `json:"title"`
			Body      string      `json:"body"`
			Link      string      `json:"link"`
			CreatedAt pytime.Time `json:"created_at"`
			Read      bool        `json:"read"`
		}
		items := make([]notificationRead, len(rows))
		for i, n := range rows {
			items[i] = notificationRead{ID: n.ID, Kind: n.Kind, Title: n.Title, Body: n.Body, Link: n.Link, CreatedAt: pytime.T(n.CreatedAt), Read: n.ReadAt != nil}
		}
		httpapi.JSON(w, http.StatusOK, obj("unread", unread, "items", items))
		return nil
	}))
	r.Post(p+"/notifications/read-all", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		a, err := project(r)
		if err != nil {
			return err
		}
		ctx := r.Context()
		if _, err := pool(ctx).Exec(ctx, `UPDATE notifications SET read_at = $1 WHERE user_id = $2 AND project_id = $3 AND read_at IS NULL`,
			db.Now(), a.Principal.User.ID, a.Project.ID); err != nil {
			return err
		}
		httpapi.NoContent(w)
		return nil
	}))
}
