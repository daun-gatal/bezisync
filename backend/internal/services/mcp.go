package services

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/models"
)

// MCP settings (Settings → MCP): one document in instance_state, so every API replica reads the
// same state and a change applies without a restart.

const mcpStateKey = "mcp"

// MCP OAuth registration modes.
const (
	RegistrationOpen     = "open"
	RegistrationApproval = "approval"
)

// MCPSettings is what an Admin chose for the MCP endpoint.
type MCPSettings struct {
	// Enabled: off by default; the endpoint answers 404 until an Admin turns it on.
	Enabled bool `json:"enabled"`
	// AllowAPITokens: personal API tokens (bzt_) work on MCP.
	AllowAPITokens bool `json:"allow_api_tokens"`
	// OAuthEnabled: agents may sign in with OAuth (BeziSync is the authorization server).
	OAuthEnabled bool `json:"oauth_enabled"`
	// OAuthRegistration: new agents work at once ("open") or wait for an Admin ("approval").
	OAuthRegistration string `json:"oauth_registration"`
	// OAuthRedirectHosts limit where agents may be sent back to (empty: any https host or loopback).
	OAuthRedirectHosts []string `json:"oauth_redirect_hosts"`
	// OAuthMaxGrantDays is the longest an agent's access lasts before signing in again.
	OAuthMaxGrantDays int `json:"oauth_max_grant_days"`
	// Tools turns single tools on or off; a tool without an entry uses its default (reads and
	// writes on, destructive tools off), so tools added later arrive in their safe state.
	Tools map[string]bool `json:"tools"`
	// AllowedOrigins: browser origins allowed to call the endpoint (requests without an Origin
	// header, such as CLI and desktop agents, always pass).
	AllowedOrigins []string `json:"allowed_origins"`
}

// DefaultMCPSettings: everything off.
func DefaultMCPSettings() MCPSettings {
	return MCPSettings{AllowAPITokens: true, OAuthRegistration: RegistrationOpen, OAuthRedirectHosts: []string{},
		OAuthMaxGrantDays: 90, Tools: map[string]bool{}, AllowedOrigins: []string{}}
}

// GetMCPSettings reads the settings (defaults for anything not stored).
func GetMCPSettings(ctx context.Context, q db.Querier) (MCPSettings, error) {
	s := DefaultMCPSettings()
	row, err := db.Find[models.InstanceState](ctx, q, `SELECT * FROM instance_state WHERE key = $1`, mcpStateKey)
	if err != nil || row == nil {
		return s, err
	}
	raw, err := json.Marshal(row.Value)
	if err != nil {
		return s, err
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		return DefaultMCPSettings(), nil //nolint:nilerr // a corrupt document must not open the endpoint
	}
	if s.Tools == nil {
		s.Tools = map[string]bool{}
	}
	if s.OAuthRedirectHosts == nil {
		s.OAuthRedirectHosts = []string{}
	}
	if s.AllowedOrigins == nil {
		s.AllowedOrigins = []string{}
	}
	if s.OAuthRegistration != RegistrationApproval {
		s.OAuthRegistration = RegistrationOpen
	}
	if s.OAuthMaxGrantDays < 1 || s.OAuthMaxGrantDays > 365 {
		s.OAuthMaxGrantDays = 90
	}
	return s, nil
}

// SaveMCPSettings stores the settings and audits what changed.
func SaveMCPSettings(ctx context.Context, q db.Querier, next MCPSettings, p *auth.Principal) error {
	before, err := GetMCPSettings(ctx, q)
	if err != nil {
		return err
	}
	row, err := instanceState(ctx, q, mcpStateKey)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(next)
	if err != nil {
		return err
	}
	var value db.Map
	if err := value.Scan(raw); err != nil {
		return err
	}
	row.Value = value
	if err := saveInstanceState(ctx, q, row); err != nil {
		return err
	}
	changes := mcpChanges(before, next)
	if len(changes) == 0 {
		return nil
	}
	_, err = Record(ctx, q, AuditEvent{Kind: "mcp.settings_updated", Summary: "Changed MCP settings: " + strings.Join(changes, "; "), Principal: p,
		Details: map[string]any{"changes": changes}})
	return err
}

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

func mcpChanges(a, b MCPSettings) []string {
	var out []string
	if a.Enabled != b.Enabled {
		out = append(out, "MCP "+onOff(b.Enabled))
	}
	if a.AllowAPITokens != b.AllowAPITokens {
		out = append(out, "API tokens "+onOff(b.AllowAPITokens))
	}
	if a.OAuthEnabled != b.OAuthEnabled {
		out = append(out, "OAuth sign-in "+onOff(b.OAuthEnabled))
	}
	if a.OAuthRegistration != b.OAuthRegistration {
		out = append(out, "new agents: "+b.OAuthRegistration)
	}
	if strings.Join(a.OAuthRedirectHosts, ",") != strings.Join(b.OAuthRedirectHosts, ",") {
		out = append(out, "redirect hosts: "+strings.Join(b.OAuthRedirectHosts, ", "))
	}
	if a.OAuthMaxGrantDays != b.OAuthMaxGrantDays {
		out = append(out, fmt.Sprintf("access lasts %d days", b.OAuthMaxGrantDays))
	}
	if strings.Join(a.AllowedOrigins, ",") != strings.Join(b.AllowedOrigins, ",") {
		out = append(out, "allowed origins: "+strings.Join(b.AllowedOrigins, ", "))
	}
	names := map[string]bool{}
	for k := range a.Tools {
		names[k] = true
	}
	for k := range b.Tools {
		names[k] = true
	}
	keys := make([]string, 0, len(names))
	for k := range names {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		av, aok := a.Tools[k]
		bv, bok := b.Tools[k]
		if aok != bok || av != bv {
			state := "default"
			if bok {
				state = onOff(bv)
			}
			out = append(out, "tool "+k+" "+state)
		}
	}
	return out
}
