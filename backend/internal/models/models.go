// Package models mirrors the database schema (one struct per table, columns by `db` tag).
package models

import (
	"time"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/db"
)

// Roles in one project. Admin is account-wide (User.IsSuperuser), not a project role.
const (
	RoleEditor   = "editor"
	RoleOperator = "operator"
	RoleViewer   = "viewer"
)

type User struct {
	ID                 uuid.UUID  `db:"id"`
	Email              string     `db:"email"`
	Name               string     `db:"name"`
	PasswordHash       *string    `db:"password_hash"`
	IsSuperuser        bool       `db:"is_superuser"`
	IsActive           bool       `db:"is_active"`
	UIMode             string     `db:"ui_mode"`
	MutedNotifications db.List    `db:"muted_notifications"`
	SSOSubject         *string    `db:"sso_subject"`
	LastLoginAt        *time.Time `db:"last_login_at"`
	CreatedAt          time.Time  `db:"created_at"`
	UpdatedAt          time.Time  `db:"updated_at"`
}

// Muted returns the muted notification kinds as strings.
func (u *User) Muted() []string {
	out := make([]string, 0, len(u.MutedNotifications))
	for _, v := range u.MutedNotifications {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

type UserSession struct {
	ID        uuid.UUID `db:"id"`
	UserID    uuid.UUID `db:"user_id"`
	TokenHash string    `db:"token_hash"`
	CSRFToken string    `db:"csrf_token"`
	ExpiresAt time.Time `db:"expires_at"`
	UserAgent string    `db:"user_agent"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

type APIToken struct {
	ID         uuid.UUID  `db:"id"`
	UserID     uuid.UUID  `db:"user_id"`
	Name       string     `db:"name"`
	TokenHash  string     `db:"token_hash"`
	Prefix     string     `db:"prefix"`
	ProjectID  *uuid.UUID `db:"project_id"`
	ReadOnly   bool       `db:"read_only"`
	ExpiresAt  *time.Time `db:"expires_at"`
	LastUsedAt *time.Time `db:"last_used_at"`
	CreatedAt  time.Time  `db:"created_at"`
	UpdatedAt  time.Time  `db:"updated_at"`
}

type Project struct {
	ID          uuid.UUID `db:"id"`
	Slug        string    `db:"slug"`
	Name        string    `db:"name"`
	Description string    `db:"description"`
	IsSandbox   bool      `db:"is_sandbox"`
	Policies    db.Map    `db:"policies"`
	CreatedAt   time.Time `db:"created_at"`
	UpdatedAt   time.Time `db:"updated_at"`
}

type Membership struct {
	ID           uuid.UUID `db:"id"`
	ProjectID    uuid.UUID `db:"project_id"`
	UserID       uuid.UUID `db:"user_id"`
	Role         string    `db:"role"`
	CanApprove   bool      `db:"can_approve"`
	GrantedBySSO bool      `db:"granted_by_sso"`
	CreatedAt    time.Time `db:"created_at"`
	UpdatedAt    time.Time `db:"updated_at"`
}

type Connection struct {
	ID            uuid.UUID  `db:"id"`
	ProjectID     uuid.UUID  `db:"project_id"`
	Kind          string     `db:"kind"`
	Type          string     `db:"type"`
	Name          string     `db:"name"`
	Description   string     `db:"description"`
	Provider      string     `db:"provider"`
	Config        db.Map     `db:"config"`
	SecretsEnc    *string    `db:"secrets_enc"`
	Managed       bool       `db:"managed"`
	LastCheck     db.Map     `db:"last_check"`
	LastCheckOK   *bool      `db:"last_check_ok"`
	LastCheckedAt *time.Time `db:"last_checked_at"`
	Version       int        `db:"version"`
	CreatedByID   *uuid.UUID `db:"created_by_id"`
	CreatedAt     time.Time  `db:"created_at"`
	UpdatedAt     time.Time  `db:"updated_at"`
}

type SecretStore struct {
	ID         uuid.UUID `db:"id"`
	ProjectID  uuid.UUID `db:"project_id"`
	Name       string    `db:"name"`
	Kind       string    `db:"kind"`
	Config     db.Map    `db:"config"`
	SecretsEnc *string   `db:"secrets_enc"`
	// When the credentials were last set or replaced (nil: never).
	SecretsUpdatedAt *time.Time `db:"secrets_updated_at"`
	LastTest         db.Map     `db:"last_test"`
	Version          int        `db:"version"`
	CreatedAt        time.Time  `db:"created_at"`
	UpdatedAt        time.Time  `db:"updated_at"`
}

type SSOProvider struct {
	ID                   uuid.UUID `db:"id"`
	Label                string    `db:"label"`
	Issuer               string    `db:"issuer"`
	ClientID             string    `db:"client_id"`
	ClientSecretEnc      *string   `db:"client_secret_enc"`
	Scopes               db.List   `db:"scopes"`
	GroupsClaim          string    `db:"groups_claim"`
	AllowUnverifiedEmail bool      `db:"allow_unverified_email"`
	RoleRules            db.List   `db:"role_rules"`
	DefaultRole          *string   `db:"default_role"`
	DefaultProjects      db.List   `db:"default_projects"`
	Enabled              bool      `db:"enabled"`
	LastTest             db.Map    `db:"last_test"`
	Version              int       `db:"version"`
	CreatedAt            time.Time `db:"created_at"`
	UpdatedAt            time.Time `db:"updated_at"`
	Kind                 string    `db:"kind"`
	Config               db.Map    `db:"config"`
}

type SSORequest struct {
	ID         string    `db:"id"`
	ProviderID uuid.UUID `db:"provider_id"`
	RelayHash  string    `db:"relay_hash"`
	NextPath   string    `db:"next_path"`
	ExpiresAt  time.Time `db:"expires_at"`
}

type OAuthClient struct {
	ID           string     `db:"id"`
	Name         string     `db:"name"`
	RedirectURIs db.List    `db:"redirect_uris"`
	ClientURI    *string    `db:"client_uri"`
	Status       string     `db:"status"`
	CreatedAt    time.Time  `db:"created_at"`
	LastUsedAt   *time.Time `db:"last_used_at"`
	DecidedByID  *uuid.UUID `db:"decided_by_id"`
	DecidedAt    *time.Time `db:"decided_at"`
}

type OAuthRequest struct {
	ID            uuid.UUID  `db:"id"`
	ClientID      string     `db:"client_id"`
	RedirectURI   string     `db:"redirect_uri"`
	State         *string    `db:"state"`
	CodeChallenge string     `db:"code_challenge"`
	Resource      *string    `db:"resource"`
	Status        string     `db:"status"`
	UserID        *uuid.UUID `db:"user_id"`
	ProjectID     *uuid.UUID `db:"project_id"`
	ReadOnly      bool       `db:"read_only"`
	GrantDays     int        `db:"grant_days"`
	CodeHash      *string    `db:"code_hash"`
	ExpiresAt     time.Time  `db:"expires_at"`
	CreatedAt     time.Time  `db:"created_at"`
}

type OAuthGrant struct {
	ID                  uuid.UUID  `db:"id"`
	ClientID            string     `db:"client_id"`
	UserID              uuid.UUID  `db:"user_id"`
	ProjectID           *uuid.UUID `db:"project_id"`
	ReadOnly            bool       `db:"read_only"`
	AccessHash          string     `db:"access_hash"`
	AccessExpiresAt     time.Time  `db:"access_expires_at"`
	RefreshHash         string     `db:"refresh_hash"`
	PreviousRefreshHash *string    `db:"previous_refresh_hash"`
	ExpiresAt           time.Time  `db:"expires_at"`
	CreatedAt           time.Time  `db:"created_at"`
	LastUsedAt          *time.Time `db:"last_used_at"`
	RevokedAt           *time.Time `db:"revoked_at"`
}

type Event struct {
	ID           uuid.UUID  `db:"id"`
	OccurredAt   time.Time  `db:"occurred_at"`
	ProjectID    *uuid.UUID `db:"project_id"`
	ActorID      *uuid.UUID `db:"actor_id"`
	ActorLabel   string     `db:"actor_label"`
	Kind         string     `db:"kind"`
	ResourceType string     `db:"resource_type"`
	ResourceID   *uuid.UUID `db:"resource_id"`
	Summary      string     `db:"summary"`
	Details      db.Map     `db:"details"`
	Audit        bool       `db:"audit"`
	RequestID    *string    `db:"request_id"`
}

type InstanceState struct {
	Key       string    `db:"key"`
	Value     db.Map    `db:"value"`
	UpdatedAt time.Time `db:"updated_at"`
}

type Runtime struct {
	ID             uuid.UUID  `db:"id"`
	ProjectID      uuid.UUID  `db:"project_id"`
	Name           string     `db:"name"`
	Kind           string     `db:"kind"`
	Mode           string     `db:"mode"`
	Config         db.Map     `db:"config"`
	SecretsEnc     *string    `db:"secrets_enc"`
	DefaultImage   *string    `db:"default_image"`
	Enabled        bool       `db:"enabled"`
	LastTest       db.Map     `db:"last_test"`
	LastSeenAt     *time.Time `db:"last_seen_at"`
	Capacity       db.Map     `db:"capacity"`
	Version        int        `db:"version"`
	AgentTokenHash *string    `db:"agent_token_hash"`
	AgentVersion   *string    `db:"agent_version"`
	AgentState     db.Map     `db:"agent_state"`
	CreatedAt      time.Time  `db:"created_at"`
	UpdatedAt      time.Time  `db:"updated_at"`
}

type Pipeline struct {
	ID                uuid.UUID  `db:"id"`
	ProjectID         uuid.UUID  `db:"project_id"`
	Name              string     `db:"name"`
	Description       string     `db:"description"`
	Owner             string     `db:"owner"`
	Labels            db.Map     `db:"labels"`
	SourceID          uuid.UUID  `db:"source_id"`
	SinkID            *uuid.UUID `db:"sink_id"`
	RuntimeID         *uuid.UUID `db:"runtime_id"`
	DesiredState      string     `db:"desired_state"`
	CurrentVersionID  *uuid.UUID `db:"current_version_id"`
	DeployedVersionID *uuid.UUID `db:"deployed_version_id"`
	DeployedAt        *time.Time `db:"deployed_at"`
	DeployedImage     *string    `db:"deployed_image"`
	Status            db.Map     `db:"status"`
	Lifecycle         string     `db:"lifecycle"`
	Health            string     `db:"health"`
	Artifacts         db.Map     `db:"artifacts"`
	DeletedAt         *time.Time `db:"deleted_at"`
	Version           int        `db:"version"`
	CreatedByID       *uuid.UUID `db:"created_by_id"`
	CreatedAt         time.Time  `db:"created_at"`
	UpdatedAt         time.Time  `db:"updated_at"`
}

// ShortID is the first 12 hex characters of the id (workload names).
func (p *Pipeline) ShortID() string {
	return ShortID(p.ID)
}

// ShortID is id.hex[:12].
func ShortID(id uuid.UUID) string {
	s := id.String()
	return s[0:8] + s[9:13]
}

// LabelMap returns labels as strings.
func (p *Pipeline) LabelMap() map[string]string {
	out := make(map[string]string, len(p.Labels))
	for k, v := range p.Labels {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

type PipelineVersion struct {
	ID             uuid.UUID  `db:"id"`
	PipelineID     uuid.UUID  `db:"pipeline_id"`
	Number         int        `db:"number"`
	Spec           db.Map     `db:"spec"`
	SpecHash       string     `db:"spec_hash"`
	Note           string     `db:"note"`
	CreatedAt      time.Time  `db:"created_at"`
	CreatedByID    *uuid.UUID `db:"created_by_id"`
	CreatedByLabel string     `db:"created_by_label"`
	// SealedAt is when something came to depend on the version (deployed, sent for approval);
	// until then its author's saves update it in place.
	SealedAt *time.Time `db:"sealed_at"`
}

type Job struct {
	ID          uuid.UUID  `db:"id"`
	Kind        string     `db:"kind"`
	ProjectID   *uuid.UUID `db:"project_id"`
	ResourceID  *uuid.UUID `db:"resource_id"`
	Payload     db.Map     `db:"payload"`
	State       string     `db:"state"`
	Attempts    int        `db:"attempts"`
	MaxAttempts int        `db:"max_attempts"`
	RunAfter    time.Time  `db:"run_after"`
	LockedBy    *string    `db:"locked_by"`
	LockedAt    *time.Time `db:"locked_at"`
	Result      db.Map     `db:"result"`
	Error       *string    `db:"error"`
	CreatedAt   time.Time  `db:"created_at"`
	FinishedAt  *time.Time `db:"finished_at"`
}

type MetricSample struct {
	ID                  int       `db:"id"`
	PipelineID          uuid.UUID `db:"pipeline_id"`
	At                  time.Time `db:"at"`
	Resolution          int       `db:"resolution"`
	EventsPerSecond     *float64  `db:"events_per_second"`
	LagMS               *float64  `db:"lag_ms"`
	EventsTotal         *float64  `db:"events_total"`
	QueueUsedRatio      *float64  `db:"queue_used_ratio"`
	SourceRetainedBytes *float64  `db:"source_retained_bytes"`
	Healthy             *bool     `db:"healthy"`
}

type AgentWorkload struct {
	ID         uuid.UUID `db:"id"`
	RuntimeID  uuid.UUID `db:"runtime_id"`
	ShortID    string    `db:"short_id"`
	ConfigHash string    `db:"config_hash"`
	SpecEnc    string    `db:"spec_enc"`
	UpdatedAt  time.Time `db:"updated_at"`
}

type AgentRequest struct {
	ID        uuid.UUID `db:"id"`
	RuntimeID uuid.UUID `db:"runtime_id"`
	Op        string    `db:"op"`
	Args      db.Map    `db:"args"`
	State     string    `db:"state"`
	Result    db.Map    `db:"result"`
	CreatedAt time.Time `db:"created_at"`
}

type ChangeRequest struct {
	ID            uuid.UUID  `db:"id"`
	ProjectID     uuid.UUID  `db:"project_id"`
	PipelineID    uuid.UUID  `db:"pipeline_id"`
	VersionID     uuid.UUID  `db:"version_id"`
	State         string     `db:"state"`
	RequestedByID *uuid.UUID `db:"requested_by_id"`
	ForceReason   *string    `db:"force_reason"`
	Note          string     `db:"note"`
	Preflight     db.Map     `db:"preflight"`
	DecidedByID   *uuid.UUID `db:"decided_by_id"`
	DecidedAt     *time.Time `db:"decided_at"`
	Comment       string     `db:"comment"`
	CreatedAt     time.Time  `db:"created_at"`
}

type PipelineTemplate struct {
	ID          uuid.UUID  `db:"id"`
	ProjectID   uuid.UUID  `db:"project_id"`
	Name        string     `db:"name"`
	Title       string     `db:"title"`
	Description string     `db:"description"`
	Spec        db.Map     `db:"spec"`
	SinkID      *uuid.UUID `db:"sink_id"`
	RuntimeID   *uuid.UUID `db:"runtime_id"`
	CreatedByID *uuid.UUID `db:"created_by_id"`
	Version     int        `db:"version"`
	CreatedAt   time.Time  `db:"created_at"`
	UpdatedAt   time.Time  `db:"updated_at"`
}

type SchemaChange struct {
	ID         uuid.UUID  `db:"id"`
	PipelineID uuid.UUID  `db:"pipeline_id"`
	Table      string     `db:"table"`
	Kind       string     `db:"kind"`
	Column     *string    `db:"column"`
	Before     *string    `db:"before"`
	After      *string    `db:"after"`
	Breaking   bool       `db:"breaking"`
	DetectedAt time.Time  `db:"detected_at"`
	ReviewedAt *time.Time `db:"reviewed_at"`
}

type AlertChannel struct {
	ID             uuid.UUID  `db:"id"`
	ProjectID      uuid.UUID  `db:"project_id"`
	Name           string     `db:"name"`
	Kind           string     `db:"kind"`
	Config         db.Map     `db:"config"`
	SecretsEnc     *string    `db:"secrets_enc"`
	Enabled        bool       `db:"enabled"`
	LastDeliveryAt *time.Time `db:"last_delivery_at"`
	LastError      *string    `db:"last_error"`
	CreatedAt      time.Time  `db:"created_at"`
	UpdatedAt      time.Time  `db:"updated_at"`
}

type AlertRule struct {
	ID          uuid.UUID `db:"id"`
	ProjectID   uuid.UUID `db:"project_id"`
	Name        string    `db:"name"`
	Kind        string    `db:"kind"`
	Severity    string    `db:"severity"`
	ForSeconds  int       `db:"for_seconds"`
	Params      db.Map    `db:"params"`
	MatchLabels db.Map    `db:"match_labels"`
	ChannelIDs  db.List   `db:"channel_ids"`
	NotifyOwner bool      `db:"notify_owner"`
	Enabled     bool      `db:"enabled"`
	CreatedAt   time.Time `db:"created_at"`
	UpdatedAt   time.Time `db:"updated_at"`
}

type Alert struct {
	ID             uuid.UUID  `db:"id"`
	ProjectID      uuid.UUID  `db:"project_id"`
	RuleID         uuid.UUID  `db:"rule_id"`
	PipelineID     uuid.UUID  `db:"pipeline_id"`
	State          string     `db:"state"`
	Severity       string     `db:"severity"`
	Title          string     `db:"title"`
	Message        string     `db:"message"`
	Fix            string     `db:"fix"`
	StartedAt      time.Time  `db:"started_at"`
	FiredAt        *time.Time `db:"fired_at"`
	LastSeenAt     time.Time  `db:"last_seen_at"`
	ResolvedAt     *time.Time `db:"resolved_at"`
	AcknowledgedAt *time.Time `db:"acknowledged_at"`
	AcknowledgedBy *string    `db:"acknowledged_by"`
	Deliveries     db.List    `db:"deliveries"`
}

type Notification struct {
	ID        uuid.UUID  `db:"id"`
	UserID    uuid.UUID  `db:"user_id"`
	ProjectID uuid.UUID  `db:"project_id"`
	Kind      string     `db:"kind"`
	Title     string     `db:"title"`
	Body      string     `db:"body"`
	Link      string     `db:"link"`
	CreatedAt time.Time  `db:"created_at"`
	ReadAt    *time.Time `db:"read_at"`
}

type Feedback struct {
	ID         uuid.UUID  `db:"id"`
	ProjectID  uuid.UUID  `db:"project_id"`
	UserID     *uuid.UUID `db:"user_id"`
	Email      string     `db:"email"`
	Kind       string     `db:"kind"`
	Message    string     `db:"message"`
	Page       string     `db:"page"`
	UserAgent  string     `db:"user_agent"`
	CreatedAt  time.Time  `db:"created_at"`
	ResolvedAt *time.Time `db:"resolved_at"`
	ResolvedBy *string    `db:"resolved_by"`
}

type DebeziumImage struct {
	ID          uuid.UUID  `db:"id"`
	Image       string     `db:"image"`
	Label       string     `db:"label"`
	Notes       string     `db:"notes"`
	IsDefault   bool       `db:"is_default"`
	Deprecated  bool       `db:"deprecated"`
	CreatedByID *uuid.UUID `db:"created_by_id"`
	CreatedAt   time.Time  `db:"created_at"`
	UpdatedAt   time.Time  `db:"updated_at"`
}
