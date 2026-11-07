// Package compiler compiles a pipeline version into a Debezium Server workload.
//
// Pure: given the spec and resolved connections it returns application.properties (never
// containing a secret value — only ${ENV} references) and the environment that carries the
// secrets. The same function backs deploys, the Review step and exports.
package compiler

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/runtime"
)

// Resource presets.
type Resource struct {
	CPU, Memory, Heap string
	Batch, Queue      int
}

var Resources = map[string]Resource{
	"small":  {"500m", "768Mi", "512m", 2048, 8192},
	"medium": {"1", "1536Mi", "1g", 4096, 16384},
	"large":  {"2", "3Gi", "2g", 8192, 32768},
}

// ManagedPatterns are keys an Expert may not override: BeziSync owns identity, state,
// credentials and routing.
var ManagedPatterns = []*regexp.Regexp{
	regexp.MustCompile(`^debezium\.source\.(offset\.storage.*|schema\.history\.internal.*|topic\.prefix|slot\.name|publication\.name)$`),
	regexp.MustCompile(`^debezium\.source\.database\.(hostname|port|user|password|dbname|names|server\.id)$`),
	regexp.MustCompile(`^debezium\.source\.(connector\.class|signal\..*|heartbeat\.action\.query)$`),
	regexp.MustCompile(`^debezium\.sink\.type$`),
	regexp.MustCompile(`^debezium\.sink\.[a-z]+\.producer\.(bootstrap\.servers|sasl\..*|security\.protocol)$`),
	regexp.MustCompile(`^quarkus\.http\.port$`),
	// The transform chain carries BeziSync's own filter (bzmeta) and routing (bzroute); steps
	// live in spec.transforms, where they are validated, previewed and policy-checked.
	regexp.MustCompile(`^debezium\.(transforms|predicates)(\..*)?$`),
}

// ResourceFor is what a Debezium Server container asks for: a preset, or a custom size with its
// heap worked out from its memory (domain.HeapMiB) and the batch and queue of the preset its
// memory reaches.
func ResourceFor(p domain.Policies) Resource {
	if p.Resources != "custom" || p.Custom == nil {
		return Resources[p.Resources]
	}
	a := p.Amount()
	band := Resources["small"]
	switch {
	case a.MemoryMiB >= domain.ResourcePresets["large"].MemoryMiB:
		band = Resources["large"]
	case a.MemoryMiB >= domain.ResourcePresets["medium"].MemoryMiB:
		band = Resources["medium"]
	}
	return Resource{CPU: p.Custom.CPU, Memory: p.Custom.Memory, Heap: fmt.Sprintf("%dm", domain.HeapMiB(a.MemoryMiB)), Batch: band.Batch, Queue: band.Queue}
}

// Constants shared with the runtime drivers.
const (
	StateSchema = "debezium_state"
	MetricsPort = 9404
	SecretDir   = connectors.SecretDir
	HTTPPort    = 8080
)

// Compiled is a workload ready for a runtime.
type Compiled struct {
	Properties   map[string]string
	Env          map[string]string
	Image        string
	Resources    map[string]any
	Destinations map[string]string
	Artifacts    map[string]any
	// Files are secret files mounted under SecretDir; name → content.
	Files map[string]string
	// Tunnel is the SSH tunnel sidecar (key in env BZ_SSH_KEY), or nil.
	Tunnel map[string]any
	// HealthPort serves /q/health (8080, unless the image moves it).
	HealthPort int
	// Flink is set for a pipeline that runs on Flink CDC (Properties is then empty).
	Flink *FlinkCompiled
	// Managed are the properties BeziSync set before the spec's advanced ones: no advanced
	// property may change them (ClassifyAdvanced). Not part of the config hash.
	Managed map[string]bool
	// Placement is the placement profile the pods run with (nil: none). Part of the config hash
	// only when set, so pipelines without one keep theirs.
	Placement *runtime.Placement
}

// Render is application.properties, or a Flink CDC pipeline's pipeline.yaml.
func (c *Compiled) Render() string {
	if c.Flink != nil {
		return c.Flink.Pipeline
	}
	return RenderProperties(c.Properties)
}

// FromFlink wraps a Flink CDC compile for the callers both engines share.
func FromFlink(f *FlinkCompiled) *Compiled {
	return &Compiled{Env: f.Env, Image: f.Image, Destinations: f.Destinations, Artifacts: map[string]any{}, Files: f.Files, Tunnel: f.Tunnel,
		HealthPort: HTTPPort, Flink: f, Resources: map[string]any{}}
}

// ConfigHash identifies what the workload runs. It must stay byte-identical to the Python
// implementation: json.dumps(…, sort_keys=True) with default separators.
func (c *Compiled) ConfigHash() string {
	base := c.baseHash()
	if c.Placement == nil {
		return base
	}
	// Through encoding/json: the profile's own field names, sorted by Dumps.
	raw, err := json.Marshal(c.Placement)
	if err != nil {
		panic(err)
	}
	var placement any
	if err := json.Unmarshal(raw, &placement); err != nil {
		panic(err)
	}
	s, err := pyjson.Dumps(map[string]any{"h": base, "placement": placement})
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:16]
}

func (c *Compiled) baseHash() string {
	if c.Flink != nil {
		return c.Flink.ConfigHash()
	}
	files := c.Files
	if files == nil {
		files = map[string]string{}
	}
	var tunnel any
	if c.Tunnel != nil {
		tunnel = c.Tunnel
	}
	payload := map[string]any{
		"p": c.Properties, "e": c.Env, "i": c.Image, "r": c.Resources, "f": files, "t": tunnel,
	}
	s, err := pyjson.Dumps(payload)
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:16]
}

// RedactedEnv hides every value.
func (c *Compiled) RedactedEnv() map[string]string {
	out := make(map[string]string, len(c.Env))
	for k := range c.Env {
		out[k] = "•••"
	}
	return out
}

func escape(value string, key bool) string {
	out := strings.ReplaceAll(value, `\`, `\\`)
	out = strings.ReplaceAll(out, "\n", `\n`)
	out = strings.ReplaceAll(out, "\r", `\r`)
	out = strings.ReplaceAll(out, "\t", `\t`)
	if key {
		out = strings.ReplaceAll(out, " ", `\ `)
		out = strings.ReplaceAll(out, "=", `\=`)
		out = strings.ReplaceAll(out, ":", `\:`)
	} else if strings.HasPrefix(out, " ") {
		out = `\` + out
	}
	return out
}

// RenderProperties renders a Java properties file (sorted keys).
func RenderProperties(props map[string]string) string {
	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	lines := []string{"# Generated by BeziSync. Do not edit: changes are overwritten on deploy."}
	for _, k := range keys {
		lines = append(lines, escape(k, true)+"="+escape(props[k], false))
	}
	return strings.Join(lines, "\n") + "\n"
}

// StateStore returns the JDBC offset (and schema-history) store properties and env.
func StateStore(c connectors.CompileContext, needsHistory bool) (map[string]string, map[string]string, error) {
	u, err := url.Parse(db.NormalizeURL(config.Get().ConnectorStateDatabaseURL()))
	if err != nil {
		return nil, nil, err
	}
	port := u.Port()
	if port == "" {
		port = "5432"
	}
	database := strings.TrimPrefix(u.Path, "/")
	jdbc := fmt.Sprintf("jdbc:postgresql://%s:%s/%s?currentSchema=%s", u.Hostname(), port, database, StateSchema)
	props := map[string]string{
		"debezium.source.offset.storage":                          "io.debezium.storage.jdbc.offset.JdbcOffsetBackingStore",
		"debezium.source.offset.storage.jdbc.connection.url":      jdbc,
		"debezium.source.offset.storage.jdbc.connection.user":     "${BZ_STATE_USER}",
		"debezium.source.offset.storage.jdbc.connection.password": "${BZ_STATE_PASSWORD}",
		"debezium.source.offset.storage.jdbc.table.name":          "offsets_" + c.ShortID,
		"debezium.source.offset.flush.interval.ms":                "5000",
	}
	if needsHistory {
		props["debezium.source.schema.history.internal"] = "io.debezium.storage.jdbc.history.JdbcSchemaHistory"
		props["debezium.source.schema.history.internal.jdbc.connection.url"] = jdbc
		props["debezium.source.schema.history.internal.jdbc.connection.user"] = "${BZ_STATE_USER}"
		props["debezium.source.schema.history.internal.jdbc.connection.password"] = "${BZ_STATE_PASSWORD}"
		props["debezium.source.schema.history.internal.jdbc.table.name"] = "history_" + c.ShortID
	}
	user := u.User.Username()
	password, _ := u.User.Password()
	return props, map[string]string{"BZ_STATE_USER": user, "BZ_STATE_PASSWORD": password}, nil
}

func validateAdvanced(advanced map[string]string) error {
	keys := make([]string, 0, len(advanced))
	for k := range advanced {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var errs []map[string]any
	for _, key := range keys {
		if !strings.HasPrefix(key, "debezium.") && !strings.HasPrefix(key, "quarkus.") {
			errs = append(errs, map[string]any{"loc": []string{"advanced", key}, "message": "must start with debezium. or quarkus."})
			continue
		}
		for _, p := range ManagedPatterns {
			if p.MatchString(key) {
				errs = append(errs, map[string]any{"loc": []string{"advanced", key}, "message": "managed by BeziSync and cannot be overridden"})
				break
			}
		}
	}
	if len(errs) > 0 {
		return problem.New(422, "managed_property", "Some advanced properties cannot be set.").WithErrors(errs)
	}
	return nil
}

func merge(dst, src map[string]string) {
	for k, v := range src {
		dst[k] = v
	}
}

// Compile compiles a pipeline; image "" means the install default.
func Compile(c connectors.CompileContext, source, sink connectors.Resolved, image string) (*Compiled, error) {
	if err := noConfigExpressions(source, sink); err != nil {
		return nil, err
	}
	src, err := connectors.GetSource(source.Type)
	if err != nil {
		return nil, err
	}
	snk, err := connectors.GetSink(sink.Type)
	if err != nil {
		return nil, err
	}
	srcDesc, _ := connectors.Describe("source", source.Type)
	snkDesc, err := connectors.Describe("sink", sink.Type)
	if err != nil {
		return nil, err
	}
	caps := snkDesc.Sink
	if len(c.Spec.Source.Tables.Include) == 0 {
		return nil, problem.New(422, "no_tables", "Select at least one table.")
	}
	if err := validateAdvanced(c.Spec.Advanced); err != nil {
		return nil, err
	}
	sinkOptions, err := caps.Options(c.Spec.Sink.Options)
	if err != nil {
		return nil, err
	}
	if err := caps.CheckNaming(snkDesc.Title, c.Spec.Sink.Naming); err != nil {
		return nil, err
	}
	if err := caps.CheckTransforms(snkDesc.Title, c.Spec.Transforms); err != nil {
		return nil, err
	}
	format, err := caps.EventFormat(snkDesc.Title, c.Spec.Sink.Format.Type)
	if err != nil {
		return nil, err
	}
	c.Spec.Transforms = withSoftDelete(withSinkUnwrap(c.Spec.Transforms, caps.Unwrap, sinkOptions))
	spec := c.Spec
	res := ResourceFor(spec.Policies)
	props := map[string]string{
		"quarkus.http.port":                                      strconv.Itoa(HTTPPort),
		"quarkus.log.console.json":                               "false",
		"quarkus.log.level":                                      "INFO",
		"debezium.source.topic.prefix":                           c.Prefix(),
		"debezium.source.max.batch.size":                         strconv.Itoa(res.Batch),
		"debezium.source.max.queue.size":                         strconv.Itoa(res.Queue),
		"debezium.source.errors.max.retries":                     strconv.Itoa(spec.Policies.Errors.Retries),
		"debezium.source.event.processing.failure.handling.mode": spec.Policies.Errors.OnPoisonEvent,
		"debezium.source.notification.enabled.channels":          "log",
	}
	snapshotProps, err := SnapshotProps(srcDesc, spec)
	if err != nil {
		return nil, err
	}
	merge(props, snapshotProps)
	if n, ok := src.(connectors.Namer); ok && n.NamesTopicsByConnector() {
		props["debezium.source.name"] = c.Prefix()
	}
	env := map[string]string{}
	sourceProps, sourceEnv, history, err := src.CompileSource(source, c)
	if err != nil {
		return nil, err
	}
	merge(props, sourceProps)
	merge(env, sourceEnv)
	var tunnel map[string]any
	sshCfg, key, err := connectors.SSHOf(source.Config, source.Secrets)
	if err != nil {
		return nil, problem.New(422, "ssh_incomplete", err.Error())
	}
	if sshCfg != nil {
		// Debezium talks to the tunnel sidecar in its pod / network namespace.
		props["debezium.source.database.hostname"] = "127.0.0.1"
		props["debezium.source.database.port"] = strconv.Itoa(connectors.TunnelPort)
		env["BZ_SSH_KEY"] = key
		tunnel = map[string]any{
			"host":         connectors.PyStr(source.Config["host"]),
			"port":         connectors.Int(source.Config, "port"),
			"ssh_host":     sshCfg.Host,
			"ssh_port":     sshCfg.Port,
			"ssh_user":     sshCfg.User,
			"ssh_host_key": sshCfg.HostKey,
			"listen_port":  connectors.TunnelPort,
			"image":        config.Get().AgentImage,
		}
	}
	stateProps, stateEnv, err := StateStore(c, history)
	if err != nil {
		return nil, err
	}
	merge(props, stateProps)
	merge(env, stateEnv)
	sinkProps, sinkEnv, err := snk.CompileSink(sink, c)
	if err != nil {
		return nil, err
	}
	merge(props, sinkProps)
	merge(env, sinkEnv)

	f := spec.Sink.Format
	props["debezium.format.value"] = format
	if format == "cloudevents" {
		props["debezium.format.key"] = "json"
	} else {
		props["debezium.format.key"] = format
	}
	switch format {
	case "json":
		v := "false"
		if f.IncludeSchema {
			v = "true"
		}
		props["debezium.format.value.schemas.enable"] = v
		props["debezium.format.key.schemas.enable"] = v
	case "avro", "protobuf":
		if f.RegistryURL == nil || *f.RegistryURL == "" {
			return nil, problem.New(422, "registry_required", fmt.Sprintf("%s needs a schema registry URL.", format))
		}
		for _, part := range []string{"key", "value"} {
			if f.RegistryKind == "apicurio" {
				props["debezium.format."+part+".apicurio.registry.url"] = *f.RegistryURL
				props["debezium.format."+part+".apicurio.registry.auto-register"] = "true"
				props["debezium.format."+part+".apicurio.registry.find-latest"] = "true"
			} else {
				props["debezium.format."+part+".schema.registry.url"] = *f.RegistryURL
			}
		}
	}
	// Transforms: drop BeziSync's own heartbeat/signal records, then shape, then route.
	chain := []string{"bzmeta"}
	// BeziSync's own tables never reach the destination, except the delivery canary.
	meta := `__debezium-heartbeat\..*|` + connectors.JavaRegexLiteral(c.Prefix()) + `\.([^.]+\.)?bezisync\.(?!canary_).*`
	if m, ok := src.(connectors.ManagedTopics); ok {
		meta += "|" + m.ManagedTopicPattern(c)
	}
	merge(props, map[string]string{
		"debezium.transforms.bzmeta.type":      "org.apache.kafka.connect.transforms.Filter",
		"debezium.transforms.bzmeta.predicate": "bzmeta",
		"debezium.predicates":                  "bzmeta",
		"debezium.predicates.bzmeta.type":      "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
		"debezium.predicates.bzmeta.pattern":   meta,
	})
	steps, names, predicates, err := stepProps(c, src, srcDesc)
	if err != nil {
		return nil, err
	}
	merge(props, steps)
	chain = append(chain, names...)
	if len(predicates) > 0 {
		props["debezium.predicates"] = strings.Join(append([]string{"bzmeta"}, predicates...), ",")
	}
	options, err := sourceOptions(c, srcDesc)
	if err != nil {
		return nil, err
	}
	merge(props, options)
	merge(props, sinkSourceProps(caps, spec, srcDesc))
	regex, replacement := src.RoutePattern(c)
	props["debezium.transforms.bzroute.type"] = "org.apache.kafka.connect.transforms.RegexRouter"
	props["debezium.transforms.bzroute.regex"] = regex
	props["debezium.transforms.bzroute.replacement"] = replacement
	chain = append(chain, "bzroute")
	merged, mergeChain := routeProps(c, spec)
	merge(props, merged)
	chain = append(chain, mergeChain...)
	props["debezium.transforms"] = strings.Join(chain, ",")
	tuned, err := tuningSettings(engine.Debezium, source.Type, sink.Type, spec.Tuning)
	if err != nil {
		return nil, err
	}
	for _, s := range tuned {
		_, set := props[s.Key]
		if err := ownedByBeziSync(s, set); err != nil {
			return nil, err
		}
		props[s.Key] = spec.Tuning[s.Key]
	}
	managed := make(map[string]bool, len(props)+1)
	for k := range props {
		managed[k] = true
	}
	merge(props, spec.Advanced)

	if caps.NoTombstones {
		managed["debezium.source.tombstones.on.delete"] = true
		// The sink fails on empty events; an advanced override must not bring them back.
		props["debezium.source.tombstones.on.delete"] = "false"
	}

	destinations := map[string]string{}
	routed := domain.RouteTargets(spec)
	natural := map[string]string{}
	for _, t := range spec.Source.Tables.Include {
		schema, table, _ := strings.Cut(t, ".")
		destinations[t] = c.Destination(schema, table)
		if to, ok := routed[t]; ok {
			destinations[t] = to
		} else {
			natural[destinations[t]] = t
		}
	}
	// A route meets an unrouted table's own destination: its events would mix in unasked.
	for _, t := range spec.Source.Tables.Include {
		if to, ok := routed[t]; ok {
			if other, taken := natural[to]; taken {
				return nil, problem.New(422, "destination_collision", fmt.Sprintf("%s and %s would both be written to %s. Route %s too, or send the route elsewhere.", t, other, to, other))
			}
		}
	}
	if spec.Policies.DeliveryCanary && srcDesc.SupportsCanary {
		canary, err := src.CanaryTable(source, c)
		if err != nil {
			return nil, err
		}
		schema, table, _ := strings.Cut(canary, ".")
		destinations[canary] = c.Destination(schema, table)
	}
	if err := caps.CheckDestinations(snkDesc.Title, destinations); err != nil {
		return nil, err
	}
	if namer, ok := snk.(connectors.DestinationNamer); ok {
		for t, routed := range destinations {
			destinations[t] = namer.Destination(sink, c, routed)
		}
	}
	if image == "" {
		image = config.Get().DebeziumDefaultImage
	}
	files, err := snk.SecretFiles(sink)
	if err != nil {
		return nil, err
	}
	if files == nil {
		files = map[string]string{}
	}
	if sf, ok := src.(connectors.SourceFiler); ok {
		sourceFiles, err := sf.SourceFiles(source)
		if err != nil {
			return nil, err
		}
		for name, content := range sourceFiles {
			if _, taken := files[name]; taken {
				return nil, fmt.Errorf("compiler: source and sink both mount %s", name)
			}
			files[name] = content
		}
	}
	return &Compiled{
		Properties:   props,
		Env:          env,
		Image:        image,
		Resources:    map[string]any{"cpu": res.CPU, "memory": res.Memory, "heap": res.Heap},
		Destinations: destinations,
		Artifacts:    map[string]any{},
		Files:        files,
		Tunnel:       tunnel,
		HealthPort:   connectors.HealthPort(sink.Type, image),
		Managed:      managed,
	}, nil
}

// noConfigExpressions refuses a connection (or one it links to) whose settings hold "${"
// (connectors.ConfigExpressions): stored before the rule, it must not reach a runtime.
func noConfigExpressions(conns ...connectors.Resolved) error {
	for _, r := range conns {
		if err := connectors.RefuseConfigExpressions(r.Name, r.Config); err != nil {
			return err
		}
		for _, field := range slices.Sorted(maps.Keys(r.Linked)) {
			if err := noConfigExpressions(r.Linked[field]); err != nil {
				return err
			}
		}
	}
	return nil
}
