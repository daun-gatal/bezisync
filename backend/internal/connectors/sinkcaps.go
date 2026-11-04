package connectors

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/schemaval"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// Sink capabilities: what a sink type needs from the pipeline (descriptor "sink"). The
// compiler applies them, saving a pipeline validates against them and the wizard reads them
// from connector-types, so a sink that cannot take deletes as empty events, or that flattens
// events itself, never gets a configuration Debezium would fail on.

// SinkCaps is a sink's descriptor "sink" object.
type SinkCaps struct {
	Naming NamingRules
	// Formats are the event formats it accepts (spec.sink.format.type); a single entry is
	// forced, whatever the spec says (Iceberg reads Kafka Connect structs: "connect").
	Formats []string
	// Unwrap is how the "row after change" step must look; nil leaves it to the user.
	Unwrap *UnwrapRule
	// Outbox: whether the outbox router step is allowed.
	Outbox bool
	// NoTombstones: the sink fails on the empty event Debezium sends after a delete.
	NoTombstones bool
	// TimePrecision: the sink cannot read Debezium's default (adaptive) temporal types.
	TimePrecision bool
	// OptionsSchema is the JSON Schema of spec.sink.options (nil: no options).
	OptionsSchema *om.Map
	// MinResources is the smallest connector size it runs in ("" = any).
	MinResources string
	// Image is the Debezium Server build the sink needs (nil: the official images).
	Image *SinkImage
}

// SinkImage is a Debezium Server build that ships a sink the official images lack. Pipelines
// to that sink always run it; runtime and catalog defaults apply to official images only.
type SinkImage struct {
	Repository string
	Tag        string
	Digest     string
	// Debezium is the Debezium Server version the build is based on (e.g. "3.6").
	Debezium string
	// Platforms it is published for (e.g. ["amd64"]).
	Platforms []string
	// HealthPort serves /q/health when the build moves it off 8080 (0: 8080).
	HealthPort int
}

// Ref is the pinned reference: repository:tag@digest.
func (i SinkImage) Ref() string {
	ref := i.Repository + ":" + i.Tag
	if i.Digest != "" {
		ref += "@" + i.Digest
	}
	return ref
}

// NamingRules constrain destination names.
type NamingRules struct {
	Default   string
	Pattern   *regexp.Regexp
	MaxLength int
	Message   string
	// FlattenDots: the sink turns dots into underscores (table names).
	FlattenDots bool
	// Single: every table goes to one destination named by the connection (a queue, a hub).
	Single bool
	// Explicit: the descriptor states its rules, so rendered names are checked too (the first
	// sinks only ever had the template checked).
	Explicit bool
}

// UnwrapRule is the required shape of the unwrap step.
type UnwrapRule struct {
	// Required: the step is always present (inserted when the spec has none).
	Required bool
	// AddFields are always added (merged with the user's).
	AddFields []string
	// DeleteHandling lists the allowed modes; the first is used when BeziSync inserts the step.
	DeleteHandling []string
	// DeleteOption names the sink option that decides the delete handling instead of the user
	// (JDBC: "deletes"), through DeleteMap (option value → handling).
	DeleteOption string
	DeleteMap    map[string]string
}

// DefaultNaming is the template a new pipeline starts with.
const DefaultNaming = "cdc.{schema}.{table}"

var defaultNamingPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

func defaultSinkCaps() SinkCaps {
	return SinkCaps{
		Naming: NamingRules{Default: DefaultNaming, Pattern: defaultNamingPattern, MaxLength: 249,
			Message: "Use letters, digits, '.', '_' and '-'."},
		Formats: []string{"json", "avro", "protobuf", "cloudevents"},
		Outbox:  true,
	}
}

// parseSinkCaps reads descriptor "sink" over the defaults, and writes the result back so
// connector-types always shows every capability.
func parseSinkCaps(d *om.Map) SinkCaps {
	c := defaultSinkCaps()
	raw, _ := d.Get("sink")
	m, _ := raw.(*om.Map)
	if m == nil {
		m = om.New()
	}
	if nv, ok := m.Get("naming"); ok {
		n := nv.(*om.Map)
		c.Naming.Explicit = true
		if s := getStr(n, "default"); s != "" {
			c.Naming.Default = s
		}
		if s := getStr(n, "pattern"); s != "" {
			c.Naming.Pattern = regexp.MustCompile(s)
		}
		if v, ok := n.Get("max_length"); ok {
			c.Naming.MaxLength = Int(map[string]any{"v": v}, "v")
		}
		if s := getStr(n, "message"); s != "" {
			c.Naming.Message = s
		}
		c.Naming.FlattenDots = getBool(n, "flatten_dots")
		c.Naming.Single = getBool(n, "single")
	}
	if fv, ok := m.Get("formats"); ok {
		c.Formats = nil
		for _, f := range fv.([]any) {
			c.Formats = append(c.Formats, f.(string))
		}
	}
	if uv, ok := m.Get("unwrap"); ok && uv != nil {
		u := uv.(*om.Map)
		rule := &UnwrapRule{Required: getBool(u, "required")}
		if af, ok := u.Get("add_fields"); ok {
			for _, f := range af.([]any) {
				rule.AddFields = append(rule.AddFields, f.(string))
			}
		}
		if dh, ok := u.Get("delete_handling"); ok {
			for _, f := range dh.([]any) {
				rule.DeleteHandling = append(rule.DeleteHandling, f.(string))
			}
		}
		if fv, ok := u.Get("delete_handling_from"); ok && fv != nil {
			from := fv.(*om.Map)
			rule.DeleteOption = getStr(from, "option")
			rule.DeleteMap = map[string]string{}
			if mv, ok := from.Get("map"); ok {
				for _, k := range mv.(*om.Map).Keys() {
					v, _ := mv.(*om.Map).Get(k)
					rule.DeleteMap[k], _ = v.(string)
				}
			}
		}
		c.Unwrap = rule
	}
	if v, ok := m.Get("outbox"); ok {
		c.Outbox, _ = v.(bool)
	}
	c.NoTombstones = getBool(m, "no_tombstones")
	c.TimePrecision = getBool(m, "time_precision")
	if ov, ok := m.Get("options_schema"); ok {
		c.OptionsSchema, _ = ov.(*om.Map)
	}
	c.MinResources = getStr(m, "min_resources")
	if iv, ok := m.Get("image"); ok && iv != nil {
		im := iv.(*om.Map)
		img := &SinkImage{Repository: getStr(im, "repository"), Tag: getStr(im, "tag"), Digest: getStr(im, "digest"), Debezium: getStr(im, "debezium")}
		if v, ok := im.Get("health_port"); ok {
			img.HealthPort = Int(map[string]any{"v": v}, "v")
		}
		if pv, ok := im.Get("platforms"); ok {
			for _, p := range pv.([]any) {
				img.Platforms = append(img.Platforms, p.(string))
			}
		}
		c.Image = img
	}

	naming := om.New().Set("default", c.Naming.Default).Set("pattern", c.Naming.Pattern.String()).
		Set("max_length", c.Naming.MaxLength).Set("message", c.Naming.Message).
		Set("flatten_dots", c.Naming.FlattenDots).Set("single", c.Naming.Single)
	formats := make([]any, len(c.Formats))
	for i, f := range c.Formats {
		formats[i] = f
	}
	var unwrap any
	if c.Unwrap != nil {
		var from any
		if c.Unwrap.DeleteOption != "" {
			m := om.New()
			for _, k := range sortedMapKeys(c.Unwrap.DeleteMap) {
				m.Set(k, c.Unwrap.DeleteMap[k])
			}
			from = om.New().Set("option", c.Unwrap.DeleteOption).Set("map", m)
		}
		unwrap = om.New().Set("required", c.Unwrap.Required).Set("add_fields", anyList(c.Unwrap.AddFields)).
			Set("delete_handling", anyList(c.Unwrap.DeleteHandling)).Set("delete_handling_from", from)
	}
	var options any
	if c.OptionsSchema != nil {
		options = c.OptionsSchema
	}
	var minRes any
	if c.MinResources != "" {
		minRes = c.MinResources
	}
	var image any
	if c.Image != nil {
		image = om.New().Set("repository", c.Image.Repository).Set("tag", c.Image.Tag).Set("digest", c.Image.Digest).
			Set("debezium", c.Image.Debezium).Set("platforms", anyList(c.Image.Platforms)).Set("health_port", c.Image.HealthPort)
	}
	d.Set("sink", om.New().Set("naming", naming).Set("formats", formats).Set("unwrap", unwrap).Set("outbox", c.Outbox).
		Set("no_tombstones", c.NoTombstones).Set("time_precision", c.TimePrecision).Set("options_schema", options).
		Set("min_resources", minRes).Set("image", image))
	return c
}

func anyList(s []string) []any {
	out := make([]any, len(s))
	for i, v := range s {
		out[i] = v
	}
	return out
}

// CheckNaming validates a naming template against the sink's rules: 422 naming_invalid.
func (c SinkCaps) CheckNaming(title, naming string) error {
	if c.Naming.Single {
		return nil
	}
	literal := naming
	for _, p := range []string{"{schema}", "{table}", "{pipeline}"} {
		literal = strings.ReplaceAll(literal, p, "x")
	}
	if c.Naming.FlattenDots {
		literal = strings.ReplaceAll(literal, ".", "_")
	}
	if literal == "" || !c.Naming.Pattern.MatchString(literal) {
		return namingProblem(fmt.Sprintf("%s destination names: %s", title, c.Naming.Message))
	}
	return nil
}

// CheckDestinations validates the rendered names of the captured tables.
func (c SinkCaps) CheckDestinations(title string, destinations map[string]string) error {
	if c.Naming.Single || !c.Naming.Explicit {
		return nil
	}
	tables := make([]string, 0, len(destinations))
	for t := range destinations {
		tables = append(tables, t)
	}
	sort.Strings(tables)
	for _, table := range tables {
		name := destinations[table]
		if c.Naming.FlattenDots {
			name = strings.ReplaceAll(name, ".", "_")
		}
		if !c.Naming.Pattern.MatchString(name) {
			return namingProblem(fmt.Sprintf("%s cannot use the name '%s' for %s. %s", title, name, table, c.Naming.Message))
		}
		if c.Naming.MaxLength > 0 && len(name) > c.Naming.MaxLength {
			return namingProblem(fmt.Sprintf("'%s' (for %s) is longer than the %d characters %s allows.", name, table, c.Naming.MaxLength, title))
		}
	}
	return nil
}

func namingProblem(msg string) error {
	return problem.New(422, "naming_invalid", msg).WithErrors([]map[string]any{{"loc": []string{"sink", "naming"}, "message": msg}})
}

// CheckTransforms refuses transform steps the sink cannot take.
func (c SinkCaps) CheckTransforms(title string, steps []domain.Transform) error {
	for _, t := range steps {
		switch t.Type {
		case "outbox":
			if !c.Outbox {
				return problem.New(422, "transform_unsupported", fmt.Sprintf("%s does not take outbox events: remove the outbox step.", title))
			}
		case "unwrap":
			if c.Unwrap != nil && c.Unwrap.DeleteOption == "" && len(c.Unwrap.DeleteHandling) > 0 && !containsStr(c.Unwrap.DeleteHandling, t.DeleteHandling) {
				return problem.New(422, "transform_unsupported", fmt.Sprintf("%s cannot take deletes as '%s' in the row-after-change step: use %s.",
					title, t.DeleteHandling, strings.Join(c.Unwrap.DeleteHandling, " or ")))
			}
		}
	}
	return nil
}

// Options validates spec.sink.options against the sink's options schema and fills defaults
// (422 with loc sink.options.<field>). Sinks without options ignore them: the Python releases
// stored the field as a free-form object, so old versions may carry keys nothing reads.
func (c SinkCaps) Options(options map[string]any) (map[string]any, error) {
	if c.OptionsSchema == nil {
		return map[string]any{}, nil
	}
	if options == nil {
		options = map[string]any{}
	}
	v := &val.V{}
	out, ok := schemaval.Model(v, val.Loc{"sink", "options"}, c.OptionsSchema, options)
	if !ok {
		return nil, problem.Validation(v.Errors)
	}
	return out, nil
}

// SinkOptions validates a pipeline's options for its sink type.
func SinkOptions(sinkType string, spec domain.PipelineSpec) (map[string]any, error) {
	d, err := Describe("sink", sinkType)
	if err != nil {
		return nil, err
	}
	return d.Sink.Options(spec.Sink.Options)
}

// ResourceRank orders connector sizes.
func ResourceRank(size string) int {
	switch size {
	case "small":
		return 1
	case "medium":
		return 2
	case "large":
		return 3
	}
	return 0
}

// DestinationNamer is implemented by sinks whose destination is not the routed name itself
// (a namespace in front, a single queue for every table). Prepare, cleanup and the Events tab
// use what it returns.
type DestinationNamer interface {
	Destination(conn Resolved, c CompileContext, routed string) string
}

// PipelineFacts is what a pipeline-level sink check knows.
type PipelineFacts struct {
	// Destinations maps captured tables to destinations (as Compile names them).
	Destinations map[string]string
	// Tables are the discovered captured tables; nil when discovery failed.
	Tables []TableInfo
	// Options are the validated sink options.
	Options map[string]any
	// FlinkCDC: the pipeline runs on Flink CDC (Debezium's sizes, keys and partitioning don't apply).
	FlinkCDC bool
}

// PipelineChecker is implemented by sinks with checks that depend on the pipeline (its tables,
// keys and options). They run in the Configuration section of preflight.
type PipelineChecker interface {
	CheckPipeline(ctx context.Context, conn Resolved, c CompileContext, facts PipelineFacts) []checks.Check
}

// GetSinkPipelineChecker returns the sink's pipeline-level checks, if it has any.
func GetSinkPipelineChecker(typ string) (PipelineChecker, bool) {
	s, err := GetSink(typ)
	if err != nil {
		return nil, false
	}
	pc, ok := s.(PipelineChecker)
	return pc, ok
}

// EventFormat is the format the workload uses: the sink's only format, or the requested one
// when the sink accepts it (422 format_unsupported otherwise).
func (c SinkCaps) EventFormat(title, requested string) (string, error) {
	if len(c.Formats) == 1 {
		return c.Formats[0], nil
	}
	if !containsStr(c.Formats, requested) {
		msg := fmt.Sprintf("%s takes %s events.", title, strings.Join(c.Formats, ", "))
		return "", problem.New(422, "format_unsupported", msg).WithErrors([]map[string]any{{"loc": []string{"sink", "format", "type"}, "message": msg}})
	}
	return requested, nil
}

// SinkImageFor is the image pipelines to this sink type run ("" for official images): the
// install's override (a mirror) or the pinned build.
func SinkImageFor(sinkType string) string {
	d, err := Describe("sink", sinkType)
	if err != nil || d.Sink.Image == nil {
		return ""
	}
	if override := sinkImageOverride(sinkType); override != "" {
		return override
	}
	return d.Sink.Image.Ref()
}

func sinkImageOverride(sinkType string) string {
	if sinkType == "iceberg" {
		return config.Get().IcebergImage
	}
	return ""
}

// ImageRepository is the repository of an image reference (no tag, no digest).
func ImageRepository(image string) string {
	if i := strings.Index(image, "@"); i >= 0 {
		image = image[:i]
	}
	if i := strings.LastIndex(image, ":"); i >= 0 && !strings.Contains(image[i+1:], "/") {
		image = image[:i]
	}
	return image
}

// InSinkImageFamily reports whether an image is the sink's own build (its pinned repository or
// the install's mirror).
func InSinkImageFamily(sinkType, image string) bool {
	d, err := Describe("sink", sinkType)
	if err != nil || d.Sink.Image == nil {
		return false
	}
	repo := ImageRepository(image)
	if repo == d.Sink.Image.Repository {
		return true
	}
	override := sinkImageOverride(sinkType)
	return override != "" && repo == ImageRepository(override)
}

// DeliveryDelayer is implemented by sinks that hold changes back on purpose (Iceberg commits at
// an interval): health treats that delay as expected, not as lag.
type DeliveryDelayer interface {
	DeliveryDelay(options map[string]any) time.Duration
}

// DeliveryDelay is how long a pipeline's sink holds changes back (0 for most sinks).
func DeliveryDelay(sinkType string, spec domain.PipelineSpec) time.Duration {
	s, err := GetSink(sinkType)
	if err != nil {
		return 0
	}
	dd, ok := s.(DeliveryDelayer)
	if !ok {
		return 0
	}
	opts, err := SinkOptions(sinkType, spec)
	if err != nil {
		return 0
	}
	return dd.DeliveryDelay(opts)
}

// DefaultHealthPort serves Debezium Server's /q/health.
const DefaultHealthPort = 8080

// HealthPort is where a workload of this sink, running this image, serves /q/health: the sink's
// own build may move it (the Iceberg build serves health on its management port, 9000).
func HealthPort(sinkType, image string) int {
	d, err := Describe("sink", sinkType)
	if err != nil || d.Sink.Image == nil || d.Sink.Image.HealthPort == 0 || !InSinkImageFamily(sinkType, image) {
		return DefaultHealthPort
	}
	return d.Sink.Image.HealthPort
}

// SinkDestination is the destination a sink really writes for a routed name (see
// DestinationNamer).
func SinkDestination(s Sink, conn Resolved, c CompileContext, routed string) string {
	if namer, ok := s.(DestinationNamer); ok {
		return namer.Destination(conn, c, routed)
	}
	return routed
}

func sortedMapKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// SinkReleaser is implemented by sinks that keep helpers of their own next to the destinations
// (SNS: the preview queue the Events tab reads). Deleting a pipeline always releases them,
// whether or not its destinations are dropped.
type SinkReleaser interface {
	ReleaseSink(ctx context.Context, conn Resolved, c CompileContext, destinations map[string]string) ([]ArtifactResult, error)
}
