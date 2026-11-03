// Package connectors is the connector protocol: everything BeziSync knows about one source
// or sink type, plus the registry.
//
// Config and secret models (and the UI forms built from them) are the JSON Schemas in
// descriptors.json, captured from the Python implementation; schemaval validates against
// them with Pydantic's semantics.
package connectors

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/ssrf"
)

// Resolved is a connection with decrypted secrets, handed to connector code only.
type Resolved struct {
	Type     string
	Provider string
	Config   map[string]any
	Secrets  map[string]any
	Name     string
	// Linked are the connections a config field points at (descriptor "ui.connection"), by field:
	// a Spanner source coordinates through a Kafka connection of its project.
	Linked map[string]Resolved
}

// CompileContext identifies the pipeline being compiled.
type CompileContext struct {
	PipelineID string // uuid string
	ShortID    string
	Name       string
	Spec       domain.PipelineSpec
}

// Prefix is Debezium's topic.prefix / logical server name, stable for the pipeline's life.
func (c CompileContext) Prefix() string { return "bz_" + c.ShortID }

// Destination maps a captured table to its destination name.
func (c CompileContext) Destination(schema, table string) string {
	n := strings.ReplaceAll(c.Spec.Sink.Naming, "{schema}", schema)
	n = strings.ReplaceAll(n, "{table}", table)
	return strings.ReplaceAll(n, "{pipeline}", c.Name)
}

// TopicSafe is a name as Debezium writes it into a topic: every character other than letters,
// digits, '.', '_' and '-' becomes '_' (an Oracle common user C##CDC is C__CDC).
func TopicSafe(name string) string {
	return strings.Map(func(r rune) rune {
		if r == '.' || r == '_' || r == '-' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			return r
		}
		return '_'
	}, name)
}

// JavaRegexLiteral escapes regex metacharacters for Java regular expressions.
func JavaRegexLiteral(text string) string {
	var b strings.Builder
	for _, r := range text {
		if strings.ContainsRune(`.\+*?[^]$(){}=!<>|:-`, r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// ArtifactResult is the outcome of creating or removing one thing BeziSync owns.
type ArtifactResult struct {
	Kind    string `json:"kind"`
	Name    string `json:"name"`
	Outcome string `json:"outcome"` // created | exists | removed | not_found | retained | failed
	Detail  string `json:"detail"`
}

// SourceImpact is what the pipeline costs the source right now. Fields a source cannot measure
// stay nil: the UI shows them as unknown, never as fine.
type SourceImpact struct {
	RetainedBytes *int64  `json:"retained_bytes"`
	SlotActive    *bool   `json:"slot_active"`
	LogStatus     *string `json:"log_status"`
	HeadroomBytes *int64  `json:"headroom_bytes"`
	// LimitBytes is the pipeline's source_retained_limit_gb policy that RetainedBytes is checked
	// against (PostgreSQL, MySQL).
	LimitBytes *int64 `json:"limit_bytes"`
	// NoCap: PostgreSQL 13+ reports no safe_wal_size because max_slot_wal_keep_size is -1, so the
	// slot is never invalidated (WAL grows until the disk is full instead).
	NoCap bool `json:"no_cap"`
	// Slot is the PostgreSQL replication slot; ReadingFile the MySQL binlog file at the stored
	// position; WindowSeconds how far back a full MongoDB oplog reaches.
	Slot          *string `json:"slot"`
	ReadingFile   *string `json:"reading_file"`
	WindowSeconds *int64  `json:"window_seconds"`
	// PositionKept (SQL Server): whether the changes after the stored position are still in the
	// CDC change tables; false means the cleanup job removed some the pipeline had not read.
	PositionKept *bool `json:"position_kept"`
	// LogUsedBytes and LogSizeBytes: the source's own transaction log (SQL Server), shared by
	// everything on the database, so not "for this pipeline".
	LogUsedBytes *int64 `json:"log_used_bytes"`
	LogSizeBytes *int64 `json:"log_size_bytes"`
	// BehindSeconds is how far the stored position trails the source's clock, and
	// RetentionSeconds how far back the source can still be read from (CockroachDB's GC TTL,
	// Spanner's change stream retention, Oracle's oldest archived log): once BehindSeconds passes
	// RetentionSeconds the pipeline cannot resume.
	BehindSeconds    *int64  `json:"behind_seconds"`
	RetentionSeconds *int64  `json:"retention_seconds"`
	Summary          string  `json:"summary"`
	Risk             *string `json:"risk"`
	RiskFix          string  `json:"risk_fix"`
	Lost             bool    `json:"lost"`
}

// TimeWindowImpact reports a source whose history is kept for a time: retention is how far
// back it reaches, behind how far the stored position trails (nil when unknown); what names the
// history in lower case ("the change stream"). The pipeline is at risk once it uses 80% of the
// window, and lost past it.
func TimeWindowImpact(behind, retention *int64, what, fix string) *SourceImpact {
	impact := &SourceImpact{BehindSeconds: behind, RetentionSeconds: retention}
	switch {
	case behind == nil && retention == nil:
		impact.Summary = "Nothing measured yet."
	case behind == nil:
		impact.Summary = fmt.Sprintf("No stored position yet; %s keeps changes for %s.", what, HumanSeconds(*retention))
	case retention == nil:
		impact.Summary = fmt.Sprintf("The stored position is %s behind; how long %s keeps changes is unknown.", HumanSeconds(*behind), what)
	default:
		impact.Summary = fmt.Sprintf("The stored position is %s behind; %s keeps changes for %s.", HumanSeconds(*behind), what, HumanSeconds(*retention))
		switch {
		case *behind >= *retention:
			impact.Lost = true
			r := fmt.Sprintf("The pipeline is %s behind, past the %s the source keeps: changes it had not read are gone.", HumanSeconds(*behind), HumanSeconds(*retention))
			impact.Risk = &r
			impact.RiskFix = "Reset the pipeline's position and copy the tables again. " + fix
		case *behind*5 >= *retention*4:
			r := fmt.Sprintf("The pipeline is %s behind and the source keeps changes for %s.", HumanSeconds(*behind), HumanSeconds(*retention))
			impact.Risk = &r
			impact.RiskFix = "Resume the pipeline or fix its destination before the window runs out. " + fix
		}
	}
	return impact
}

// HumanSeconds renders a duration as "3 d 4 h", "5 h 10 min", "45 min" or "20 s".
func HumanSeconds(s int64) string {
	switch {
	case s < 60:
		return fmt.Sprintf("%d s", max(s, 0))
	case s < 3600:
		return fmt.Sprintf("%d min", s/60)
	case s < 48*3600:
		if m := s % 3600 / 60; m != 0 {
			return fmt.Sprintf("%d h %d min", s/3600, m)
		}
		return fmt.Sprintf("%d h", s/3600)
	}
	if h := s % 86400 / 3600; h != 0 {
		return fmt.Sprintf("%d d %d h", s/86400, h)
	}
	return fmt.Sprintf("%d d", s/86400)
}

// Column of a discovered table.
type Column struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
	PII      bool   `json:"pii"`
}

// TableInfo is one discovered table.
type TableInfo struct {
	Schema          string   `json:"schema"`
	Name            string   `json:"name"`
	RowsEstimate    *int64   `json:"rows_estimate"`
	BytesEstimate   *int64   `json:"bytes_estimate"`
	PrimaryKey      []string `json:"primary_key"`
	ReplicaIdentity *string  `json:"replica_identity"`
	Columns         []Column `json:"columns"`
	Warnings        []string `json:"warnings"`
	Selectable      bool     `json:"selectable"`
}

// Identifier is schema.table.
func (t TableInfo) Identifier() string { return t.Schema + "." + t.Name }

// Discovery is the table list of a source.
type Discovery struct {
	Tables       []TableInfo `json:"tables"`
	Truncated    bool        `json:"truncated"`
	DiscoveredAt string      `json:"discovered_at"`
}

// NewDiscovery stamps the discovery time.
func NewDiscovery(tables []TableInfo, truncated bool) Discovery {
	if tables == nil {
		tables = []TableInfo{}
	}
	for i := range tables {
		if tables[i].PrimaryKey == nil {
			tables[i].PrimaryKey = []string{}
		}
		if tables[i].Columns == nil {
			tables[i].Columns = []Column{}
		}
		if tables[i].Warnings == nil {
			tables[i].Warnings = []string{}
		}
	}
	return Discovery{Tables: tables, Truncated: truncated, DiscoveredAt: pytime.ISOFormat(time.Now())}
}

// TailRecord is one event read back from a destination for preview.
type TailRecord struct {
	Destination string  `json:"destination"`
	Partition   *int    `json:"partition"`
	Offset      *int64  `json:"offset"`
	Timestamp   *string `json:"timestamp"`
	Key         any     `json:"key"`
	Value       any     `json:"value"`
}

// Connector is one source or sink type.
type Connector interface {
	Kind() string
	Type() string
	// Check runs the connection checks. An error means an unexpected failure (the Python
	// implementation raised), not a failed check.
	Check(ctx context.Context, conn Resolved, tables []string) (checks.Report, error)
}

// Source connectors.
type Source interface {
	Connector
	Discover(ctx context.Context, conn Resolved) (Discovery, error)
	// CompileSource returns properties, secret env and whether schema history is needed.
	CompileSource(conn Resolved, c CompileContext) (map[string]string, map[string]string, bool, error)
	RoutePattern(c CompileContext) (string, string)
	// TopicPattern is a Java regex matching the Debezium topic of one captured table
	// ("schema.table"), before BeziSync renames it.
	TopicPattern(c CompileContext, table string) string
	Prepare(ctx context.Context, conn Resolved, c CompileContext) ([]ArtifactResult, error)
	Cleanup(ctx context.Context, conn Resolved, c CompileContext) ([]ArtifactResult, error)
	ForgetPosition(ctx context.Context, conn Resolved, c CompileContext) ([]ArtifactResult, error)
	// Probe measures the impact on the source (nil when unsupported).
	Probe(ctx context.Context, conn Resolved, c CompileContext, retainedLimitBytes int64) (*SourceImpact, error)
	Signal(ctx context.Context, conn Resolved, c CompileContext, signalType string, data map[string]any) (string, error)
	// CanaryTable is the "schema.table" of the delivery canary, as it appears in topic names.
	CanaryTable(conn Resolved, c CompileContext) (string, error)
	WriteCanary(ctx context.Context, conn Resolved, c CompileContext, token string) error
}

// Sink connectors.
type Sink interface {
	Connector
	CompileSink(conn Resolved, c CompileContext) (map[string]string, map[string]string, error)
	PrepareSink(ctx context.Context, conn Resolved, c CompileContext, destinations map[string]string) ([]ArtifactResult, error)
	SecretFiles(conn Resolved) (map[string]string, error)
	CleanupSink(ctx context.Context, conn Resolved, c CompileContext, destinations map[string]string) ([]ArtifactResult, error)
	Tail(ctx context.Context, conn Resolved, destination string, limit int, timeout time.Duration) ([]TailRecord, error)
}

// ErrNotSupported is returned by optional operations a connector does not implement.
type ErrNotSupported struct{ Msg string }

func (e *ErrNotSupported) Error() string { return e.Msg }

// IsNotSupported reports an unsupported optional operation.
func IsNotSupported(err error) bool {
	var e *ErrNotSupported
	return errors.As(err, &e)
}

// BaseSource provides the defaults of optional source operations.
type BaseSource struct{ Title string }

// RoutePattern maps Debezium topics "<prefix>.<schema>.<table>" to destination names.
func (BaseSource) RoutePattern(c CompileContext) (string, string) {
	naming := strings.ReplaceAll(c.Spec.Sink.Naming, "{schema}", "$1")
	naming = strings.ReplaceAll(naming, "{table}", "$2")
	naming = strings.ReplaceAll(naming, "{pipeline}", c.Name)
	return JavaRegexLiteral(c.Prefix()) + `\.([^.]+)\.([^.]+)`, naming
}

// TopicPattern matches "<prefix>.<schema>.<table>".
func (BaseSource) TopicPattern(c CompileContext, table string) string {
	return JavaRegexLiteral(c.Prefix() + "." + table)
}

// Prepare does nothing by default.
func (BaseSource) Prepare(context.Context, Resolved, CompileContext) ([]ArtifactResult, error) {
	return []ArtifactResult{}, nil
}

// Cleanup does nothing by default.
func (BaseSource) Cleanup(context.Context, Resolved, CompileContext) ([]ArtifactResult, error) {
	return []ArtifactResult{}, nil
}

// ForgetPosition does nothing by default.
func (BaseSource) ForgetPosition(context.Context, Resolved, CompileContext) ([]ArtifactResult, error) {
	return []ArtifactResult{}, nil
}

// Probe is unsupported by default.
func (BaseSource) Probe(context.Context, Resolved, CompileContext, int64) (*SourceImpact, error) {
	return nil, nil
}

// Signal is unsupported by default.
func (b BaseSource) Signal(context.Context, Resolved, CompileContext, string, map[string]any) (string, error) {
	return "", &ErrNotSupported{b.Title + " does not support signals yet"}
}

// CanaryTable is unsupported by default.
func (b BaseSource) CanaryTable(Resolved, CompileContext) (string, error) {
	return "", &ErrNotSupported{b.Title + " does not support a delivery canary"}
}

// WriteCanary is unsupported by default.
func (b BaseSource) WriteCanary(context.Context, Resolved, CompileContext, string) error {
	return &ErrNotSupported{b.Title + " does not support a delivery canary"}
}

// BaseSink provides the defaults of optional sink operations.
type BaseSink struct{ Title string }

// PrepareSink does nothing by default.
func (BaseSink) PrepareSink(context.Context, Resolved, CompileContext, map[string]string) ([]ArtifactResult, error) {
	return []ArtifactResult{}, nil
}

// SecretFiles: none by default.
func (BaseSink) SecretFiles(Resolved) (map[string]string, error) { return map[string]string{}, nil }

// CleanupSink does nothing by default.
func (BaseSink) CleanupSink(context.Context, Resolved, CompileContext, map[string]string) ([]ArtifactResult, error) {
	return []ArtifactResult{}, nil
}

// Tail is unsupported by default.
func (b BaseSink) Tail(context.Context, Resolved, string, int, time.Duration) ([]TailRecord, error) {
	return nil, &ErrNotSupported{b.Title + " destinations cannot be read back"}
}

// PIIHints are column-name fragments that suggest personal data.
var PIIHints = []string{"email", "phone", "ssn", "passport", "birth", "dob", "address", "card", "iban", "tax_id", "national_id"}

// LooksLikePII reports whether a column name suggests personal data.
func LooksLikePII(column string) bool {
	lower := strings.ToLower(column)
	for _, h := range PIIHints {
		if strings.Contains(lower, h) {
			return true
		}
	}
	return false
}

// NetworkCheck probes TCP reachability through the SSRF guard. Returns false (and records
// a failure) if unreachable.
func NetworkCheck(ctx context.Context, run *checks.Run, host string, port int, code, what string, timeout time.Duration) bool {
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	latency, err := ssrf.TCPProbe(ctx, host, port, timeout)
	var blocked *ssrf.BlockedAddress
	switch {
	case errors.As(err, &blocked):
		run.Add(checks.Check{
			Code: code + "_blocked", Title: "Reach " + what, Status: checks.Fail, Category: checks.CatNetwork,
			Message: blocked.Error(), Blocking: true,
			Why: "Connection checks may not contact loopback, link-local or cloud metadata addresses.",
		})
		return false
	case err != nil:
		run.Add(checks.Check{
			Code: code + "_unreachable", Title: "Reach " + what, Status: checks.Fail, Category: checks.CatNetwork,
			Message:   fmt.Sprintf("Cannot open a connection to %s:%d.", host, port),
			Technical: err.Error(), Blocking: true,
			Why: "Connectors run where the runtime runs; if BeziSync cannot reach the host, neither can they.",
			Fix: &checks.Fix{Kind: "text", Who: "network admin",
				Code: fmt.Sprintf("Allow TCP %d from the runtime to %s (security group, firewall, or VPC peering).", port, host)},
		})
		return false
	}
	run.Passed(code+"_reachable", "Reach "+what, checks.CatNetwork,
		fmt.Sprintf("Connected to %s:%d in %s ms.", host, port, domain.FormatFixed(latency, 0)), "")
	return true
}

// --- descriptors and the registry ---

//go:embed descriptors.json
var descriptorsJSON []byte

// Descriptor is what the UI knows about a connector type (plus behaviour flags).
type Descriptor struct {
	Descriptor         *om.Map // kind, type, title, description, presets, schemas, supports_masking
	DebeziumName       string
	NeedsSchemaHistory bool
	TracksSchema       bool
	SupportsCanary     bool
	UnwrapTransform    string
	Readable           bool
	// Canary: the delivery canary can be found by reading back (default: Readable). SQS is
	// readable, but its consumers may take the canary first.
	Canary          bool
	ConfigSchema    *om.Map
	SecretSchema    *om.Map
	Title           string
	Kind, Type      string
	SupportsMasking bool
	Presets         []any
	Transforms      TransformCaps // sources only
	Capture         CaptureCaps   // sources only
	Sink            SinkCaps      // sinks only
	// Links maps config fields that hold another connection of the project (ui.connection) to
	// that connection's "kind/type".
	Links map[string]string
}

// CaptureCaps is how a source reads existing rows and changes (descriptor "capture").
type CaptureCaps struct {
	// SnapshotModes are the pipeline snapshot modes the source offers, first the default; empty
	// when it cannot copy existing rows at all (it streams from the moment it starts).
	SnapshotModes []string `json:"snapshot_modes"`
	// ModeNames maps a BeziSync mode to the connector's own name for it (Vitess: no_data → never).
	ModeNames map[string]string `json:"mode_names,omitempty"`
	// Incremental: tables can be copied again while streaming (signals).
	Incremental bool `json:"incremental"`
	// IncrementalNote is a known limitation of re-copies, shown where they are started.
	IncrementalNote string `json:"incremental_note,omitempty"`
	// MinDebezium is the oldest Debezium Server release ("3.7.0") whose connector does what
	// BeziSync configures (CockroachDB's sinkless changefeed); "" when any 3.x will do.
	MinDebezium string `json:"min_debezium,omitempty"`
	// Maturity is Debezium's own status of the connector: "stable" or "incubating".
	Maturity string `json:"maturity"`
}

// DebeziumMode is the connector's name for a pipeline snapshot mode ("" when it has none).
func (c CaptureCaps) DebeziumMode(mode string) string {
	if !slices.Contains(c.SnapshotModes, mode) {
		return ""
	}
	if name, ok := c.ModeNames[mode]; ok {
		return name
	}
	return mode
}

// SourceFiler is a source that needs files (keys, wallets) mounted under SecretDir.
type SourceFiler interface {
	SourceFiles(conn Resolved) (map[string]string, error)
}

// ManagedTopics is a source whose own tables (signals, heartbeats) are not under a "bezisync"
// schema: the pattern matches their topics, which never reach the destination.
type ManagedTopics interface {
	ManagedTopicPattern(c CompileContext) string
}

// Namer is a source whose topics are named after Debezium's connector name instead of the
// topic prefix (Spanner): the compiler sets debezium.source.name to the prefix.
type Namer interface {
	NamesTopicsByConnector() bool
}

// TransformCaps is what a source supports beyond the generic steps (descriptor "transforms").
type TransformCaps struct {
	ValueFormats map[string][]string // decimals / timestamps / binary → allowed modes
	KeyColumns   bool                // message.key.columns
	RowFilters   bool                // filter rules on columns (events carry typed rows)
	// MaskableTypes are the column types (as discovery reports them) Debezium can hash:
	// column.mask.hash only applies to character columns and leaves any other value as it is.
	MaskableTypes []string
}

// CanMask reports whether a column of this type can be masked (hashed) by this source.
func (t TransformCaps) CanMask(columnType string) bool {
	return slices.Contains(t.MaskableTypes, strings.ToLower(columnType))
}

var descriptors = map[string]*Descriptor{}
var descriptorOrder []string

func init() {
	raw, err := om.DecodeOrdered(descriptorsJSON)
	if err != nil {
		panic(fmt.Sprintf("connectors: descriptors.json: %v", err))
	}
	list, _ := raw.(*om.Map).Get("connectors")
	for _, e := range list.([]any) {
		entry := e.(*om.Map)
		d := &Descriptor{}
		dv, _ := entry.Get("descriptor")
		d.Descriptor = dv.(*om.Map)
		d.DebeziumName = getStr(entry, "debezium_name")
		d.NeedsSchemaHistory = getBool(entry, "needs_schema_history")
		d.TracksSchema = getBool(entry, "tracks_schema")
		d.SupportsCanary = getBool(entry, "supports_canary")
		d.UnwrapTransform = getStr(entry, "unwrap_transform")
		d.Readable = getBool(entry, "readable")
		d.Canary = d.Readable
		if v, ok := entry.Get("canary"); ok {
			d.Canary, _ = v.(bool)
		}
		cs, _ := d.Descriptor.Get("config_schema")
		ss, _ := d.Descriptor.Get("secret_schema")
		d.ConfigSchema, d.SecretSchema = cs.(*om.Map), ss.(*om.Map)
		d.Title = getStr(d.Descriptor, "title")
		d.Kind, d.Type = getStr(d.Descriptor, "kind"), getStr(d.Descriptor, "type")
		d.SupportsMasking = getBool(d.Descriptor, "supports_masking")
		p, _ := d.Descriptor.Get("presets")
		d.Presets, _ = p.([]any)
		if tv, ok := d.Descriptor.Get("transforms"); ok {
			tm := tv.(*om.Map)
			d.Transforms.KeyColumns = getBool(tm, "key_columns")
			d.Transforms.RowFilters = getBool(tm, "row_filters")
			if mt, ok := tm.Get("maskable_types"); ok {
				for _, m := range mt.([]any) {
					d.Transforms.MaskableTypes = append(d.Transforms.MaskableTypes, m.(string))
				}
			}
			d.Transforms.ValueFormats = map[string][]string{}
			if vf, ok := tm.Get("value_formats"); ok {
				for _, k := range vf.(*om.Map).Keys() {
					modes, _ := vf.(*om.Map).Get(k)
					for _, m := range modes.([]any) {
						d.Transforms.ValueFormats[k] = append(d.Transforms.ValueFormats[k], m.(string))
					}
				}
			}
		}
		if cv, ok := d.Descriptor.Get("capture"); ok {
			cm := cv.(*om.Map)
			if modes, ok := cm.Get("snapshot_modes"); ok {
				d.Capture.SnapshotModes = []string{}
				for _, m := range modes.([]any) {
					d.Capture.SnapshotModes = append(d.Capture.SnapshotModes, m.(string))
				}
			}
			if names, ok := cm.Get("mode_names"); ok {
				d.Capture.ModeNames = map[string]string{}
				for _, k := range names.(*om.Map).Keys() {
					v, _ := names.(*om.Map).Get(k)
					d.Capture.ModeNames[k] = v.(string)
				}
			}
			d.Capture.Incremental = getBool(cm, "incremental")
			d.Capture.IncrementalNote = getStr(cm, "incremental_note")
			d.Capture.MinDebezium = getStr(cm, "min_debezium")
			d.Capture.Maturity = getStr(cm, "maturity")
			// Served to the UI next to the rest: the canary flag itself lives on the entry.
			cm.Set("canary", d.SupportsCanary)
		}
		props, _ := d.ConfigSchema.Get("properties")
		pm, _ := props.(*om.Map)
		for _, field := range keysOf(pm) {
			pv, _ := pm.Get(field)
			if ui, ok := pv.(*om.Map).Get("ui"); ok {
				if target := getStr(ui.(*om.Map), "connection"); target != "" {
					if d.Links == nil {
						d.Links = map[string]string{}
					}
					d.Links[field] = target
				}
			}
		}
		if d.Kind == "sink" {
			d.Sink = parseSinkCaps(d.Descriptor)
		}
		key := d.Kind + "/" + d.Type
		descriptors[key] = d
		descriptorOrder = append(descriptorOrder, key)
	}
}

func keysOf(m *om.Map) []string {
	if m == nil {
		return nil
	}
	return m.Keys()
}

func getStr(m *om.Map, k string) string {
	v, _ := m.Get(k)
	s, _ := v.(string)
	return s
}

func getBool(m *om.Map, k string) bool {
	v, _ := m.Get(k)
	b, _ := v.(bool)
	return b
}

// Describe returns the descriptor of a connector type.
func Describe(kind, typ string) (*Descriptor, error) {
	if d, ok := descriptors[kind+"/"+typ]; ok {
		return d, nil
	}
	return nil, problem.New(422, "unknown_connector", fmt.Sprintf("Unknown %s type '%s'.", kind, typ))
}

// Descriptors returns every connector in registry order.
func Descriptors() []*Descriptor {
	out := make([]*Descriptor, 0, len(descriptorOrder))
	for _, k := range descriptorOrder {
		out = append(out, descriptors[k])
	}
	return out
}

var registry = map[string]Connector{}

// Register adds a connector implementation (called from init of each connector file).
func Register(c Connector) { registry[c.Kind()+"/"+c.Type()] = c }

// Get returns a connector implementation.
func Get(kind, typ string) (Connector, error) {
	if c, ok := registry[kind+"/"+typ]; ok {
		return c, nil
	}
	return nil, problem.New(422, "unknown_connector", fmt.Sprintf("Unknown %s type '%s'.", kind, typ))
}

// GetSource returns a source connector.
func GetSource(typ string) (Source, error) {
	c, err := Get("source", typ)
	if err != nil {
		return nil, err
	}
	return c.(Source), nil
}

// GetSink returns a sink connector.
func GetSink(typ string) (Sink, error) {
	c, err := Get("sink", typ)
	if err != nil {
		return nil, err
	}
	return c.(Sink), nil
}

// --- small helpers for reading validated config ---

// Str reads a string config value ("" when absent or null).
func Str(m map[string]any, k string) string {
	switch v := m[k].(type) {
	case string:
		return v
	case nil:
		return ""
	default:
		return fmt.Sprint(v)
	}
}

// OptStr reads an optional string (nil when absent or null).
func OptStr(m map[string]any, k string) *string {
	v, ok := m[k]
	if !ok || v == nil {
		return nil
	}
	s := Str(m, k)
	return &s
}

// Int reads an integer config value.
func Int(m map[string]any, k string) int {
	switch v := m[k].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case fmt.Stringer:
		n, _ := strconv.Atoi(v.String())
		return n
	case string:
		n, _ := strconv.Atoi(v)
		return n
	}
	return 0
}

// Bool reads a boolean config value.
func Bool(m map[string]any, k string) bool {
	b, _ := m[k].(bool)
	return b
}

// PyStr is Python's str() for config values (booleans as True/False, None as None).
func PyStr(v any) string {
	switch x := v.(type) {
	case nil:
		return "None"
	case bool:
		if x {
			return "True"
		}
		return "False"
	case string:
		return x
	}
	return fmt.Sprint(v)
}

// CanarySources names the sources that support the delivery canary ("PostgreSQL, MySQL / MariaDB
// or Oracle").
func CanarySources() string {
	var titles []string
	for _, d := range Descriptors() {
		if d.Kind == "source" && d.SupportsCanary {
			titles = append(titles, d.Title)
		}
	}
	switch len(titles) {
	case 0:
		return "none"
	case 1:
		return titles[0]
	}
	return strings.Join(titles[:len(titles)-1], ", ") + " or " + titles[len(titles)-1]
}
